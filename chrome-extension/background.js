// ─── Virola Background Service Worker v17.2 ────────────────────────────────
// Real-time streaming with proper action execution pipeline
//
// v17.2 fixes (bg process feedback):
// - command_result handler now detects background:true payloads and injects a
//   proper tool_result with label, pid, startup_output so the AI knows the
//   process registered successfully (previously injected an empty output).
// - Added bg_process_output SSE listener: live stdout/stderr from background
//   processes is now forwarded to the AI as tool_result events.
// - Added bg_process_exited SSE listener: AI is notified when a bg process stops
//   or crashes, with exit code included so it can react/retry.
//
// v16.0 changes:
// - Removed cli_message handler (chat architecture removed from server)
// - No /inject, /chat-events, /extension-status endpoints exist anymore
// - OpenAI-compatible endpoint added server-side: POST /v1/chat/completions
// - File-write command results (cat heredoc, sed) inject a short confirmation
//   instead of echoing the full file content back to the AI.
// - file_done SSE event injects "✓ Written: <path> (<bytes> bytes)" tool_result.
// - command_result for non-write commands unchanged (full output injected).

const DEFAULT_PORT = 3172;
const BRIDGE_URL = () => `http://localhost:${state.port}`;
const SSE_URL = () => `http://localhost:${state.port}/stream`;

let state = {
  port: DEFAULT_PORT,
  connected: false,
  tool: 'virola-v16.0',
  autoApprove: false,
  activeTab: null,
  streamActive: false,
  currentStreamId: null,
  stats: { dispatched: 0, succeeded: 0, failed: 0, chunksReceived: 0, partialBlocks: 0 },
};

// ── File-write command detection ───────────────────────────────────────────────
// Commands that write file content should return a short confirmation, not the
// echoed file body. Matches cat heredoc (> or >>), sed -i, and tee.
const FILE_WRITE_CMD_RE = /^\s*(cat\s+[>]{1,2}|sed\s+-i|tee\s+)/i;

function isFileWriteCommand(cmd) {
  if (!cmd) return false;
  // Check the first non-empty line of a potentially multi-line command
  const firstLine = cmd.trim().split('\n')[0];
  return FILE_WRITE_CMD_RE.test(firstLine);
}

// Extract a short filename from a file-write command for the confirmation message
function extractFilePath(cmd) {
  if (!cmd) return 'file';
  const firstLine = cmd.trim().split('\n')[0];
  // cat > path or cat >> path
  const catMatch = firstLine.match(/cat\s+>{1,2}\s*(\S+)/i);
  if (catMatch) return catMatch[1];
  // sed -i ... path
  const sedMatch = firstLine.match(/sed\s+-i\S*\s+.+?\s+(\S+)$/i);
  if (sedMatch) return sedMatch[1];
  // tee path
  const teeMatch = firstLine.match(/tee\s+(\S+)/i);
  if (teeMatch) return teeMatch[1];
  return 'file';
}

// ── Storage ────────────────────────────────────────────────────────────────────
chrome.storage.local.get(['port', 'autoApprove', 'tool'], (saved) => {
  if (saved.port) state.port = saved.port;
  if (saved.tool) state.tool = saved.tool;
  if (saved.autoApprove !== undefined) state.autoApprove = saved.autoApprove;
  initHealthCheck();
  initSSEConnection();
});

// ── Health Check ────────────────────────────────────────────────────────────────
function initHealthCheck() {
  setInterval(ping, 5000);
  ping();
}

async function ping() {
  try {
    const res = await fetch(`${BRIDGE_URL()}/health`, {
      signal: AbortSignal.timeout(2000)
    });
    const data = await res.json();
    setConnected(true, data.tool || state.tool);
  } catch {
    setConnected(false);
  }
}

function setConnected(connected, toolName) {
  const changed = state.connected !== connected;
  state.connected = connected;
  if (toolName) state.activeTool = toolName;
  if (changed) {
    broadcastStatus();
    updateBadge();
  }
}

function updateBadge() {
  const text = state.connected ? (state.streamActive ? '●' : 'ON') : '';
  const color = state.connected ? (state.streamActive ? '#00ff00' : '#00C896') : '#E24B4A';
  chrome.action.setBadgeText({ text });
  chrome.action.setBadgeBackgroundColor({ color });
}

// ── SSE Connection ─────────────────────────────────────────────────────────────
let eventSource = null;
let reconnectAttempts = 0;
let reconnectTimer = null;

function scheduleReconnect() {
  if (reconnectTimer) return; // already scheduled
  // Cap backoff at 5s — retry forever, never give up
  const delay = Math.min(1000 * Math.pow(1.5, Math.min(reconnectAttempts, 6)), 5000);
  reconnectAttempts++;
  console.log(`[Virola] SSE reconnect in ${Math.round(delay)}ms (attempt ${reconnectAttempts})`);
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    initSSEConnection();
  }, delay);
}

function initSSEConnection() {
  if (eventSource) {
    eventSource.close();
    eventSource = null;
  }

  if (!state.connected) {
    scheduleReconnect();
    return;
  }

  try {
    eventSource = new EventSource(SSE_URL());

    eventSource.addEventListener('connected', (e) => {
      const data = JSON.parse(e.data);
      console.log('[Virola] SSE connected, workspace:', data.workspace);
      reconnectAttempts = 0; // reset backoff on successful connect
    });

    // File chunk - update live
    eventSource.addEventListener('file_chunk', (e) => {
      const data = JSON.parse(e.data);
      state.stats.chunksReceived++;
      broadcastToTabs({
        type: 'VIROLA_FILE_CHUNK',
        path: data.path,
        chunk: data.chunk,
        offset: data.offset
      });
    });

    // ── file_done: inject a short write confirmation ───────────────────────────
    // Previously this was silent. Now we inject a tool_result so the AI knows
    // the file landed without seeing the full file content echoed back.
    eventSource.addEventListener('file_done', (e) => {
      const data = JSON.parse(e.data);
      console.log('[Virola] File written:', data.path, data.bytes, 'bytes');

      const confirmText = buildWriteConfirmation(data.path, data.bytes);
      broadcastToTabs({
        type: 'VIROLA_INJECT',
        text: confirmText,
        autoSend: true
      });
    });

    // ── command_result: suppress file content, inject short confirmation ───────
    eventSource.addEventListener('command_result', (e) => {
      const data = JSON.parse(e.data);
      const cmd = data.command || '';
      const exitCode = data.exitCode ?? 0;

      let resultText;

      if (data.background) {
        // Background process launched — inject startup output + registration info
        // so the AI knows the process is running and can reference it by label/pid.
        const startupSnippet = data.output
          ? `\n<startup_output>${escapeHtml(data.output)}</startup_output>` : '';
        const terminalNote = data.terminal
          ? ' (opened in new terminal tab)' : ` (detached, pid ${data.pid})`;
        resultText = `<tool_result>\n<command>${escapeHtml(cmd)}</command>\n<status>background process started${terminalNote}</status>\n<label>${escapeHtml(data.label || '')}</label>\n<pid>${data.pid || ''}</pid>${startupSnippet}\n<note>${escapeHtml(data.message || 'Use Kill Server to stop.')}</note>\n</tool_result>`;
        broadcastToTabs({ type: 'VIROLA_INJECT', text: resultText, autoSend: true });
        console.log('[Virola] Background process started:', data.label, 'pid:', data.pid);
        return;
      }

      if (isFileWriteCommand(cmd)) {
        // File-write command — never echo the file body back to the AI.
        // Return a short confirmation (or error if it failed).
        if (exitCode === 0) {
          const filePath = extractFilePath(cmd);
          resultText = buildWriteConfirmation(filePath, null, cmd);
        } else {
          // Write failed — DO inject the error so the AI can retry
          resultText = buildErrorResult(cmd, data.output || '', exitCode);
        }
      } else {
        // Normal command — inject full output as before
        resultText = buildCommandResult(cmd, data.output || '', exitCode);
      }

      broadcastToTabs({
        type: 'VIROLA_INJECT',
        text: resultText,
        autoSend: true
      });

      console.log('[Virola] Command result injected:', cmd?.slice(0, 50), 'exit:', exitCode,
        isFileWriteCommand(cmd) ? '(write — condensed)' : '');
    });

    // Stream complete
    eventSource.addEventListener('stream_complete', (e) => {
      const data = JSON.parse(e.data);
      console.log('[Virola] Stream complete:', data.actionCount, 'actions,', data.partialBlocks || 0, 'partial blocks');
    });

    // ── bg_process_output: stream live logs from background processes back to AI ─
    eventSource.addEventListener('bg_process_output', (e) => {
      const data = JSON.parse(e.data);
      const resultText = `<tool_result>\n<event>bg_process_output</event>\n<label>${escapeHtml(data.label || '')}</label>\n<output>${escapeHtml(data.output || '')}</output>\n</tool_result>`;
      broadcastToTabs({ type: 'VIROLA_INJECT', text: resultText, autoSend: true });
      console.log('[Virola] BG process output injected:', data.label, data.output?.slice(0, 80));
    });

    // ── bg_process_exited: notify AI when a background process stops ───────────
    eventSource.addEventListener('bg_process_exited', (e) => {
      const data = JSON.parse(e.data);
      const resultText = `<tool_result>\n<event>bg_process_exited</event>\n<label>${escapeHtml(data.label || '')}</label>\n<pid>${data.pid || ''}</pid>\n<exit_code>${data.exitCode != null ? data.exitCode : 'unknown'}</exit_code>\n<note>Background process has stopped.</note>\n</tool_result>`;
      broadcastToTabs({ type: 'VIROLA_INJECT', text: resultText, autoSend: true });
      console.log('[Virola] BG process exited:', data.label, 'exit code:', data.exitCode);
    });

    // Action result (write_file actions via dispatch, not shell commands)
    eventSource.addEventListener('action_result', (e) => {
      const data = JSON.parse(e.data);
      if (data.type === 'write_file') {
        // Already covered by file_done — skip to avoid double injection
        return;
      }
      const resultText = `<tool_result>\n<action>${data.type}</action>\n<status>${data.result?.ok ? 'success' : 'failed'}</status>\n</tool_result>`;
      broadcastToTabs({
        type: 'VIROLA_INJECT',
        text: resultText,
        autoSend: true
      });
    });

    eventSource.onerror = () => {
      console.warn('[Virola] SSE error/disconnect — will reconnect...');
      eventSource.close();
      eventSource = null;
      scheduleReconnect();
    };

  } catch (err) {
    console.error('[Virola] SSE init failed:', err);
    setTimeout(initSSEConnection, 5000);
  }
}

// ── Result builders ────────────────────────────────────────────────────────────

function buildWriteConfirmation(filePath, bytes, cmd) {
  // Determine if this was a create (>) or append (>>) from the command
  let action = 'written';
  if (cmd) {
    if (/cat\s+>>/.test(cmd)) action = 'appended';
    else if (/sed\s+-i/.test(cmd)) action = 'edited';
    else if (/tee\s+/.test(cmd)) action = 'written';
  }
  const sizeStr = bytes != null ? ` (${bytes} bytes)` : '';
  return `<tool_result>\n<status>write successful</status>\n<file>${escapeHtml(filePath)}</file>\n<action>${action}${sizeStr}</action>\n</tool_result>`;
}

function buildCommandResult(cmd, output, exitCode) {
  return `<tool_result>\n<command>${escapeHtml(cmd)}</command>\n<output>${escapeHtml(output)}</output>\n<exit_code>${exitCode}</exit_code>\n</tool_result>`;
}

function buildErrorResult(cmd, output, exitCode) {
  return `<tool_result>\n<command>${escapeHtml(cmd)}</command>\n<error>${escapeHtml(output)}</error>\n<exit_code>${exitCode}</exit_code>\n</tool_result>`;
}

function escapeHtml(str) {
  if (!str) return '';
  return str
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

// ── Broadcast to all tabs ──────────────────────────────────────────────────────
function broadcastToTabs(message) {
  chrome.tabs.query({}, (tabs) => {
    tabs.forEach(tab => {
      chrome.tabs.sendMessage(tab.id, message).catch(() => {});
    });
  });
}

// ── Message Bus ────────────────────────────────────────────────────────────────
chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
  if (msg.type === 'VIROLA_STREAM_START') {
    console.log('[Virola] Stream started:', msg.streamId, 'from:', msg.platform);
    state.streamActive = true;
    state.currentStreamId = msg.streamId;
    state.stats.dispatched++;
    updateBadge();
    broadcastStatus();
    sendResponse({ ok: true });
    return;
  }

  if (msg.type === 'VIROLA_STREAM_CHUNK') {
    handleStreamChunk(msg.data)
      .then(() => sendResponse({ ok: true }))
      .catch(err => {
        console.warn('[Virola] Chunk forward failed:', err.message);
        sendResponse({ ok: false, error: err.message });
      });
    return true;
  }

  if (msg.type === 'VIROLA_STREAM_END') {
    console.log('[Virola] Stream end received, processing...');
    handleStreamEnd(msg)
      .then(() => sendResponse({ ok: true }))
      .catch(err => {
        console.error('[Virola] Stream end error:', err.message);
        sendResponse({ ok: false, error: err.message });
      });
    return true;
  }

  if (msg.type === 'VIROLA_INJECT') {
    const tabId = sender.tab?.id;
    if (tabId) {
      chrome.tabs.sendMessage(tabId, {
        type: 'VIROLA_INJECT',
        text: msg.text,
        autoSend: msg.autoSend !== false
      });
    }
    sendResponse({ ok: true });
    return;
  }

  if (msg.type === 'VIROLA_GET_STATUS') {
    sendResponse({
      connected: state.connected,
      tool: state.activeTool,
      stats: state.stats,
      port: state.port,
      streamActive: state.streamActive,
      platform: sender.tab?.url ? detectPlatform(sender.tab.url) : 'unknown'
    });
    return;
  }

  if (msg.type === 'VIROLA_SETTINGS_UPDATE') {
    Object.assign(state, msg.settings);
    chrome.storage.local.set(msg.settings);
    ping();
    sendResponse({ ok: true });
    return;
  }
});

// ── Stream Chunk Handler ──────────────────────────────────────────────────────
async function handleStreamChunk(data) {
  if (!state.connected) throw new Error('Virola not connected');

  try {
    await fetch(`${BRIDGE_URL()}/stream-chunk`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        streamId: state.currentStreamId,
        chunk: data.text,
        fullText: data.fullText,
        accumulatedText: data.accumulatedText,
        type: data.type,
        language: data.language,
        timestamp: Date.now()
      }),
      signal: AbortSignal.timeout(5000)
    });
  } catch (err) {
    console.warn('[Virola] Chunk forward failed:', err.message);
    // Don't throw — chunk loss is recoverable
  }
}

// ── Stream End Handler ────────────────────────────────────────────────────────
async function handleStreamEnd(msg) {
  state.streamActive = false;
  updateBadge();

  const { streamId, fullText, source, platform } = msg;
  console.log('[Virola] Processing stream end:', streamId, 'text length:', fullText?.length);

  if (!state.connected) {
    console.log('[Virola] Server not connected, skipping action execution');
    return;
  }

  try {
    const res = await fetch(`${BRIDGE_URL()}/stream-end`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        streamId: streamId || state.currentStreamId,
        fullText,
        platform,
        source,
        autoApprove: state.autoApprove
      }),
      signal: AbortSignal.timeout(60000)
    });

    if (!res.ok) throw new Error(`Server error ${res.status}`);

    const result = await res.json();
    console.log('[Virola] Server response:', result.actions?.length || 0, 'actions,', result.partialBlocks || 0, 'partial blocks');

    if (result.partialBlocks > 0) state.stats.partialBlocks += result.partialBlocks;
    if (result.actions?.length > 0) state.stats.succeeded += result.actions.length;

    broadcastStatus();

  } catch (err) {
    state.stats.failed++;
    console.error('[Virola] Stream end failed:', err.message);
    throw err;
  }
}

// ── Platform Detection ────────────────────────────────────────────────────────
function detectPlatform(url) {
  if (!url) return 'unknown';
  if (url.includes('chatgpt.com') || url.includes('openai.com')) return 'chatgpt';
  if (url.includes('anthropic.com') || url.includes('claude.ai')) return 'claude';
  if (url.includes('deepseek.com')) return 'deepseek';
  if (url.includes('gemini') || url.includes('aistudio.google')) return 'gemini';
  if (url.includes('qwen.ai') || url.includes('tongyi')) return 'qwen';
  if (url.includes('moonshot.cn')) return 'moonshot';
  if (url.includes('groq.com')) return 'groq';
  if (url.includes('perplexity.ai')) return 'perplexity';
  return 'unknown';
}

// ── Broadcast Status ──────────────────────────────────────────────────────────
function broadcastStatus() {
  chrome.runtime.sendMessage({
    type: 'VIROLA_STATUS',
    connected: state.connected,
    tool: state.activeTool,
    stats: state.stats,
    port: state.port,
    streamActive: state.streamActive
  }).catch(() => {});
}

// ── Tab tracking ─────────────────────────────────────────────────────────────
chrome.tabs.onActivated.addListener(({ tabId }) => { state.activeTab = tabId; });
chrome.tabs.onUpdated.addListener((tabId, change, tab) => {
  if (change.status === 'complete' && tab.active) state.activeTab = tabId;
});

updateBadge();
