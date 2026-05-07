// ─── Virola Popup v10.1 ─────────────────────────────────────────────────────
// Fixed system prompt with proper injection handling and AGGRESSIVE prompt structure

const $ = id => document.getElementById(id);

let log = [];
let serverStatus = null;

// ── Init ─────────────────────────────────────────────────────────────────────
chrome.runtime.sendMessage({ type: 'VIROLA_GET_STATUS' }, (status) => {
  if (status) applyStatus(status);
});

chrome.storage.local.get(['port', 'autoApprove'], (saved) => {
  if (saved.port) $('portInput').value = saved.port;
  if (saved.autoApprove) $('autoApprove').checked = saved.autoApprove;
});

chrome.storage.local.get('activityLog', (s) => {
  if (s.activityLog) { log = s.activityLog; renderLog(); }
});

// ── Status updates ────────────────────────────────────────────────────────────
chrome.runtime.onMessage.addListener((msg) => {
  if (msg.type === 'VIROLA_STATUS') applyStatus(msg);
  if (msg.type === 'VIROLA_LOG') addLogEntry(msg.entry);
  if (msg.type === 'VIROLA_STREAM_UPDATE') updateStreamStatus(msg);
});

function applyStatus({ connected, tool, stats, port, streamActive, platform }) {
  const pill = $('statusPill');
  const txt = $('statusText');
  const hint = $('offlineHint');

  pill.className = 'status-pill ' + (connected ? 'connected' : 'disconnected');
  txt.textContent = connected ? (tool || 'Connected') : 'Offline';
  hint.classList.toggle('hidden', connected);

  if (streamActive) {
    txt.textContent = 'Streaming...';
    pill.className = 'status-pill streaming';
  }

  if (stats) {
    $('statDispatched').textContent = stats.dispatched || 0;
    $('statSuccess').textContent = stats.succeeded || 0;
    $('statFailed').textContent = stats.failed || 0;
    $('statChunks').textContent = stats.chunksReceived || 0;
  }

  if (port) $('portInput').value = port;

  serverStatus = { connected, streamActive };
}

function updateStreamStatus({ active, chunks, text }) {
  const streamEl = $('streamStatus');
  if (streamEl) {
    if (active) {
      streamEl.textContent = `Streaming: ${chunks || 0} chunks`;
      streamEl.className = 'stream-status active';
    } else {
      streamEl.textContent = 'Idle';
      streamEl.className = 'stream-status';
    }
  }
}

// ── System Prompt (v13.2-WSL) ──────────────────────────────────────────────────
// Updated for WSL Ubuntu: cat heredoc support, multi-line bash scripts.


// ── System Prompt — loaded from prompt.md in extension folder ─────────────────
// The prompt is no longer hardcoded. It is fetched from prompt.md at runtime.
// This means you can update prompt.md without rebuilding the extension.

let VIROLA_SYSTEM_PROMPT = null;

async function loadSystemPrompt() {
  if (VIROLA_SYSTEM_PROMPT) return VIROLA_SYSTEM_PROMPT;
  try {
    const url = chrome.runtime.getURL('prompt.md');
    const res = await fetch(url);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    VIROLA_SYSTEM_PROMPT = await res.text();
    return VIROLA_SYSTEM_PROMPT;
  } catch (err) {
    console.error('[Virola] Failed to load prompt.md:', err);
    throw new Error('Could not load prompt.md from extension folder: ' + err.message);
  }
}

// ── System Prompt Button ─────────────────────────────────────────────────────
const sysPromptBtn = $('injectSysPromptBtn');
const sysPromptStatus = $('sysPromptStatus');

sysPromptBtn.addEventListener('click', async () => {
  sysPromptBtn.disabled = true;
  sysPromptStatus.textContent = 'Loading prompt.md...';
  sysPromptStatus.className = 'sys-prompt-status';

  try {
    // Load prompt from prompt.md in extension folder (not hardcoded)
    const promptText = await loadSystemPrompt();

    sysPromptStatus.textContent = 'Injecting...';
    const [activeTab] = await chrome.tabs.query({ active: true, currentWindow: true });
    if (!activeTab?.id) throw new Error('No active tab');

    chrome.tabs.sendMessage(
      activeTab.id,
      { type: 'VIROLA_INJECT', text: promptText, autoSend: false },
      (response) => {
        if (chrome.runtime.lastError) {
          updateSysPromptStatus({ success: false, message: 'Not a supported AI chat page' });
        } else {
          updateSysPromptStatus({ success: true, message: 'System prompt injected!' });
        }
      }
    );
  } catch (err) {
    updateSysPromptStatus({ success: false, message: err.message });
  }
});

function updateSysPromptStatus({ success, message }) {
  sysPromptBtn.disabled = false;
  sysPromptStatus.textContent = message || (success ? 'Injected!' : 'Failed');
  sysPromptStatus.className = 'sys-prompt-status ' + (success ? 'success' : 'failed');
  setTimeout(() => {
    sysPromptStatus.textContent = 'Ready';
    sysPromptStatus.className = 'sys-prompt-status';
  }, 3000);
}

// ── Port & Connect ────────────────────────────────────────────────────────────
$('connectBtn').addEventListener('click', () => {
  const port = parseInt($('portInput').value, 10);
  if (isNaN(port) || port < 1024 || port > 65535) return;
  saveSettings({ port });
  $('connectBtn').textContent = '…';
  setTimeout(() => { $('connectBtn').textContent = 'Ping'; }, 1500);
});

$('portInput').addEventListener('change', () => {
  const port = parseInt($('portInput').value, 10);
  if (!isNaN(port)) saveSettings({ port });
});

// ── Auto-approve ─────────────────────────────────────────────────────────────
$('autoApprove').addEventListener('change', (e) => {
  saveSettings({ autoApprove: e.target.checked });
});

// ── Settings ─────────────────────────────────────────────────────────────────
function saveSettings(patch) {
  chrome.storage.local.set(patch);
  chrome.runtime.sendMessage({ type: 'VIROLA_SETTINGS_UPDATE', settings: patch });
}

// ── Activity Log ─────────────────────────────────────────────────────────────
function addLogEntry(entry) {
  log.unshift(entry);
  if (log.length > 100) log.pop();
  chrome.storage.local.set({ activityLog: log });
  renderLog();
}

function renderLog() {
  const container = $('logContainer');
  const empty = $('logEmpty');

  if (!log.length) {
    empty.style.display = '';
    return;
  }

  empty.style.display = 'none';
  const lines = log.slice(0, 30).map(e => {
    const time = new Date(e.ts).toLocaleTimeString('en', { hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit' });
    const cls = e.ok === false ? 'error' : e.ok === true ? 'success' : '';
    return `<div class="log-line ${cls}">
      <span class="log-time">${time}</span>
      <span class="log-source">${escHtml(e.source || '?')}</span>
      <span class="log-action">${e.type || '?'}</span>
      <span class="log-detail">${escHtml(e.detail || '')}</span>
    </div>`;
  }).join('');

  container.innerHTML = lines;
}

function escHtml(s) {
  return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

$('clearLog').addEventListener('click', () => {
  log = [];
  chrome.storage.local.set({ activityLog: [] });
  renderLog();
  $('logEmpty').style.display = '';
});

$('openDocs').addEventListener('click', () => {
  chrome.tabs.create({ url: 'https://github.com/virola/virola#readme' });
});

// ── Kill Latest Server button ─────────────────────────────────────────────────
function getPort() {
  return parseInt($('portInput').value, 10) || 3172;
}

async function refreshBgProcesses() {
  try {
    const res = await fetch(`http://localhost:${getPort()}/processes`);
    if (!res.ok) return;
    const { processes } = await res.json();
    const section = $('bgProcessSection');
    const list    = $('bgProcessList');
    const btn     = $('killLastBtn');
    section.style.display = '';
    if (!processes || processes.length === 0) {
      list.innerHTML = '<span style="color:var(--muted);font-size:11px;">No background processes running</span>';
      btn.textContent = '⏹ Kill Latest Server';
      btn.disabled = true;
    } else {
      list.innerHTML = processes.map(p =>
        `<div style="display:flex;justify-content:space-between;padding:2px 0;">
          <span style="color:var(--accent)">⟳ ${p.label.slice(0, 38)}</span>
          <span style="color:var(--muted)">${Math.round(p.uptimeMs / 1000)}s</span>
        </div>`
      ).join('');
      btn.textContent = processes.length === 1
        ? `⏹ Kill: ${processes[0].label.slice(0, 28)}`
        : `⏹ Kill Latest (${processes.length} running)`;
      btn.disabled = false;
    }
  } catch { /* server not reachable */ }
}

$('killLastBtn').addEventListener('click', async () => {
  const btn    = $('killLastBtn');
  const status = $('killStatus');
  btn.disabled = true;
  btn.textContent = 'Killing…';
  status.style.color = 'var(--muted)';
  status.textContent = '';
  try {
    const res = await fetch(`http://localhost:${getPort()}/kill-last`, { method: 'POST' });
    const data = await res.json();
    if (data.ok) {
      status.style.color = 'var(--accent)';
      status.textContent = `✓ Killed: ${data.label || 'process'}`;
    } else {
      status.style.color = 'var(--warn)';
      status.textContent = `✗ ${data.error || 'Failed'}`;
    }
  } catch (err) {
    status.style.color = 'var(--warn)';
    status.textContent = `✗ ${err.message}`;
  }
  setTimeout(() => {
    status.textContent = '';
    refreshBgProcesses();
  }, 3000);
});

// Poll for running bg processes every 2s
setInterval(refreshBgProcesses, 2000);
refreshBgProcesses();

// ── Status poll ──────────────────────────────────────────────────────────────
setInterval(() => {
  chrome.runtime.sendMessage({ type: 'VIROLA_GET_STATUS' }, (s) => { if (s) applyStatus(s); });
}, 2000);
