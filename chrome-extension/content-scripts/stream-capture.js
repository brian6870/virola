// ─── Virola Stream Capture v17.1 ──────────────────────────────────────────
//
// FIXES v17.1 (bug: command text contaminated by injected tool_result prose):
// - processTextChange() now detects when new content is a <tool_result> injection
//   and skips it even after isInjecting lock expires (guards the 500ms poll path).
// Real-time streaming response capture for AI chat interfaces
//
// FIXES v13.1 (bug: // FILENAME: blocks write empty files):
// Mirrored server-side parser fixes into parseActions() (used for local logging).
// - `while (content.startsWith('\n'))` instead of single `if` check.
// - `if (filePath && content.trim())` instead of `if (filePath && content)`.
//
// FIXES v12.1 (bug: files always empty / FILENAME blocks not detected):
// ROOT CAUSE: AI chat UIs render markdown — the DOM never contains ``` fences
// or // FILENAME: directives. accumulatedText was built from rendered DOM text,
// so the server's regex found no code blocks and wrote nothing.
//
// FIX 1: fetch() interception — intercepts AI streaming API responses and
//   accumulates RAW markdown (with fences) into rawMarkdownBuffer BEFORE the
//   UI renders it. fireStreamEnd() sends rawMarkdownBuffer as fullText.
// FIX 2: parseActions() rewritten to match server.js logic exactly — now
//   correctly enforces // FILENAME: directive (old regex captured any comment).
// FIX 3 (server.js): sanitizeContent() no longer calls .trim() — trimming
//   strips intentional leading/trailing whitespace from file contents.
//
// How it works:
// 1. MutationObserver watches for AI response elements
// 2. Intercepts streaming text updates character-by-character
// 3. Sends chunks to background service via chrome.runtime
// 4. Background streams to local server via HTTP
// 5. Server parses actions and executes commands
// 6. Results are injected back into the AI chat

(function () {
  'use strict';

  const SOURCE = 'stream-capture';

  // ── Stream State Variables ───────────────────────────────────────────────────
  let currentStreamId = null;
  let accumulatedText = '';
  let lastSeenText = '';          // CRITICAL: Cleared on stream end
  let isStreaming = false;
  let streamStartTime = null;
  let tokenCount = 0;
  let streamEndFired = false;     // CRITICAL: Hard guard against double-firing
  let isInjecting = false;        // CRITICAL: Block observer during inject to prevent loop
  let lastStreamEndTime = 0;      // CRITICAL: Cooldown after stream-end to block re-trigger
  const STREAM_COOLDOWN_MS = 2500; // ms to ignore new streams after firing stream-end

  // ── Timers ───────────────────────────────────────────────────────────────────
  let debounceTimer = null;
  let stabilityCheckTimer = null;
  let stabilityRound = 0;
  let lastStableText = '';        // Text at start of stability check

  // ── Platform Detection ───────────────────────────────────────────────────────
  function detectPlatform() {
    const hostname = window.location.hostname;

    if (hostname.includes('chatgpt.com') || hostname.includes('openai.com')) return 'chatgpt';
    if (hostname.includes('claude.ai') || hostname.includes('anthropic.com')) return 'claude';
    if (hostname.includes('deepseek.com')) return 'deepseek';
    if (hostname.includes('gemini') || hostname.includes('aistudio.google')) return 'gemini';
    if (hostname.includes('qwen.ai') || hostname.includes('tongyi') || hostname.includes('aliyun')) return 'qwen';
    if (hostname.includes('moonshot.cn')) return 'moonshot';
    if (hostname.includes('groq.com')) return 'groq';
    if (hostname.includes('perplexity.ai')) return 'perplexity';

    return 'unknown';
  }

  const PLATFORM = detectPlatform();

  // ── Platform-Specific Selectors ──────────────────────────────────────────────
  const PLATFORM_CONFIG = {
    claude: {
      isGenerating: () => !!document.querySelector('.inline-loading-indicator, [class*="generating"], [class*="typing"]'),
      messageContainer: '[data-testid="conversation-turn-annotated"], .conversation-turn-annotated, [class*="message"]',
      inputField: 'textarea[data-testid="chat-input"], textarea[name="prompt"]'
    },
    chatgpt: {
      isGenerating: () => !!document.querySelector('[data-testid="turn-loader"], .result-streaming, [class*="generating"]'),
      messageContainer: '[data-message-author-role="assistant"], [class*="message"]',
      inputField: 'textarea[data-testid="chat-input"]'
    },
    deepseek: {
      isGenerating: () => !!document.querySelector('.generating, [class*="streaming"], .typing'),
      messageContainer: '.markdown-body, [class*="message"]',
      inputField: 'textarea'
    },
    gemini: {
      isGenerating: () => !!document.querySelector('.gemini-thinking, [aria-busy="true"], [class*="generating"]'),
      messageContainer: '[class*="message"], [class*="response"], .gemini-response',
      inputField: 'textarea, [contenteditable="true"]'
    },
    qwen: {
      isGenerating: () => !!document.querySelector('.generating, [class*="streaming"], .thinking'),
      messageContainer: '.markdown-body, [class*="message"]',
      inputField: 'textarea'
    },
    moonshot: {
      isGenerating: () => !!document.querySelector('.generating, [class*="streaming"]'),
      messageContainer: '.markdown-body, [class*="message"]',
      inputField: 'textarea'
    },
    groq: {
      isGenerating: () => !!document.querySelector('.generating, [class*="streaming"]'),
      messageContainer: '[class*="message"]',
      inputField: 'textarea'
    },
    perplexity: {
      isGenerating: () => !!document.querySelector('.generating, [class*="streaming"]'),
      messageContainer: '[class*="answer"], [class*="message"]',
      inputField: 'textarea'
    },
    unknown: {
      isGenerating: () => !!document.querySelector('.generating, [class*="streaming"], [aria-busy="true"]'),
      messageContainer: '[role="assistant"], [class*="message"], article, main',
      inputField: 'textarea, [contenteditable="true"]'
    }
  };

  const CONFIG = PLATFORM_CONFIG[PLATFORM] || PLATFORM_CONFIG.unknown;

  // ── Stream ID generator ──────────────────────────────────────────────────────
  function generateStreamId() {
    return `stream_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
  }

  // ── Input field detection ────────────────────────────────────────────────────
  function getInputField() {
    return (
      document.querySelector(CONFIG.inputField) ||
      document.querySelector('textarea#chat-input') ||
      document.querySelector('textarea') ||
      document.querySelector('[contenteditable="true"]')
    );
  }

  // ── Send button detection ─────────────────────────────────────────────────────
  function getSendButton() {
    return (
      document.querySelector('button[type="submit"]') ||
      document.querySelector('button[aria-label*="send" i]') ||
      document.querySelector('.send-button') ||
      document.querySelector('[data-testid="send-button"]')
    );
  }

  function clickSend() {
    let tries = 0;
    const trySend = () => {
      const btn = getSendButton();
      if (btn && !btn.disabled) { btn.click(); return; }
      if (++tries < 5) setTimeout(trySend, 150);
    };
    trySend();
  }

  // ── Text injection ────────────────────────────────────────────────────────────
  function injectText(text, autoSend = false) {
    const input = getInputField();
    if (!input) {
      console.log('[Virola] No input field found');
      return;
    }

    // Lock: prevent MutationObserver from treating injected text as AI output
    isInjecting = true;

    const nativeSetter = Object.getOwnPropertyDescriptor(
      input.tagName === 'TEXTAREA' ? window.HTMLTextAreaElement.prototype : window.HTMLInputElement.prototype,
      'value'
    );

    if (nativeSetter && input.tagName === 'TEXTAREA') {
      nativeSetter.set.call(input, text);
    } else if (input.contentEditable === 'true') {
      input.focus();
      document.execCommand('selectAll');
      document.execCommand('insertText', false, text);
    }

    input.dispatchEvent(new Event('input', { bubbles: true }));
    input.dispatchEvent(new Event('change', { bubbles: true }));

    if (autoSend) setTimeout(clickSend, 200);

    // Hold the inject lock for the full stream-cooldown window so the 500ms
    // interval poll and MutationObserver cannot pick up the injected
    // <tool_result> text and start a new fake stream before the cooldown
    // expires.  Previously 800ms — shorter than STREAM_COOLDOWN_MS (2500ms) —
    // which left a gap where the poll fired and relaunched a duplicate stream.
    setTimeout(() => { isInjecting = false; }, STREAM_COOLDOWN_MS + 200);
  }

  // ── Raw markdown capture via fetch interception ─────────────────────────────
  let rawMarkdownBuffer = '';
  let rawMarkdownStreamId = null;
  // Track fetch streams so we only capture the CURRENT AI response, not injections
  let activeFetchCapture = false;

  (function interceptFetch() {
    const origFetch = window.fetch;
    window.fetch = async function (...args) {
      const response = await origFetch.apply(this, args);
      const url = typeof args[0] === 'string' ? args[0] : args[0]?.url || '';

      // TIGHT filter — only intercept known AI streaming API paths
      // Deliberately exclude anything that could be our own tool_result POST
      const isAIEndpoint = (
        /\/(v1\/)?chat\/completions/i.test(url) ||
        /\/api\/(generate|stream|completion|message)/i.test(url) ||
        /deepseek\.com\/(api\/chat|chat\/completions|v1\/chat)/i.test(url) ||
        /chat\.deepseek\.com\/api\//i.test(url) ||
        /anthropic\.com.*\/messages/i.test(url) ||
        /openai\.com.*\/chat\/completions/i.test(url) ||
        /gemini.*generateContent/i.test(url) ||
        /generativelanguage\.googleapis\.com/i.test(url)
      ) && !url.includes('localhost') && !url.includes('127.0.0.1');

      if (!isAIEndpoint) return response;

      // Don't double-capture if already capturing
      if (activeFetchCapture) return response;

      const clone = response.clone();
      activeFetchCapture = true;

      // Each fetch gets its OWN isolated buffer.
      // We REPLACE rawMarkdownBuffer with this fetch's content rather than
      // appending — this prevents cross-stream contamination where the previous
      // stream's text bleeds into the next stream's buffer.
      let thisFetchText = '';
      // Reset the shared buffer immediately so stale text from the previous
      // stream is gone before any new chunks arrive.
      rawMarkdownBuffer = '';

      (async () => {
        try {
          const reader = clone.body?.getReader();
          if (!reader) { activeFetchCapture = false; return; }
          const decoder = new TextDecoder();
          let buf = '';

          while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            const chunk = decoder.decode(value, { stream: true });
            buf += chunk;

            const lines = buf.split('\n');
            buf = lines.pop();

            for (const line of lines) {
              if (!line.startsWith('data:')) continue;
              const raw = line.slice(5).trim();
              if (raw === '[DONE]') continue;
              try {
                const obj = JSON.parse(raw);
                const anthText = obj?.delta?.text || obj?.completion || '';
                const oaiText = obj?.choices?.[0]?.delta?.content || obj?.choices?.[0]?.text || '';
                const genText = obj?.content || obj?.text || obj?.message?.content || '';
                const text = anthText || oaiText || genText;
                if (text) {
                  thisFetchText += text;
                  // Always keep rawMarkdownBuffer in sync with THIS fetch only.
                  // Never += here — the buffer belongs to one fetch at a time.
                  rawMarkdownBuffer = thisFetchText;
                }
              } catch { /* not JSON */ }
            }
          }
        } catch { /* ignore */ } finally {
          activeFetchCapture = false;
        }
      })();

      return response;
    };
  })();

  // ── Raw DOM text extraction (fallback when fetch interception misses content) ─
  function extractRawText(el) {
    if (!el) return '';

    const tag = el.tagName && el.tagName.toLowerCase();

    // Skip DeepSeek / QwQ thinking blocks entirely — they have class "think" or "ds-think"
    if (tag && el.classList) {
      for (const cls of el.classList) {
        if (/think|reasoning|chain.of.thought/i.test(cls)) return '';
      }
    }
    // Also skip elements with data attributes marking them as thinking
    if (el.dataset && (el.dataset.think || el.dataset.thinking)) return '';

    // Code/pre blocks: return raw textContent (rendered code, not fenced markdown)
    if (tag === 'pre' || tag === 'code') {
      return el.textContent || '';
    }

    let result = '';
    for (const node of el.childNodes) {
      if (node.nodeType === Node.TEXT_NODE) {
        result += node.textContent;
      } else if (node.nodeType === Node.ELEMENT_NODE) {
        const ct = node.tagName.toLowerCase();
        // Skip thinking nodes at any depth
        if (node.classList) {
          let isThink = false;
          for (const cls of node.classList) {
            if (/think|reasoning|chain.of.thought/i.test(cls)) { isThink = true; break; }
          }
          if (isThink) continue;
        }
        if (ct === 'pre' || ct === 'code') {
          result += node.textContent;
        } else if (ct === 'br') {
          result += '\n';
        } else if (ct === 'p' || ct === 'div' || ct === 'li') {
          const inner = extractRawText(node);
          result += inner;
          if (inner && !inner.endsWith('\n')) result += '\n';
        } else {
          result += extractRawText(node);
        }
      }
    }
    return result;
  }

  // ── Strip <think> XML blocks from text (DeepSeek API-style output) ────────────
  function stripThinkingText(text) {
    return text
      .replace(/<think>[\s\S]*?<\/think>/gi, '')
      .replace(/<thinking>[\s\S]*?<\/thinking>/gi, '')
      .trim();
  }

  // ── Pinned stream element ─────────────────────────────────────────────────────
  // At stream-start we record WHICH message container element is being written.
  // All subsequent DOM reads during that stream use ONLY that element, so a new
  // AI response element appearing after tool_result injection never contaminates
  // the current stream's accumulatedText.
  let pinnedStreamElement = null;

  // ── Get latest assistant message ─────────────────────────────────────────────
  function getLatestAssistantMessage() {
    try {
      const containers = document.querySelectorAll(CONFIG.messageContainer);
      if (!containers.length) return '';

      // If we have a pinned element for the current stream, use ONLY that.
      // Verify it's still in the DOM first.
      if (pinnedStreamElement && document.contains(pinnedStreamElement)) {
        const text = extractRawText(pinnedStreamElement).trim();
        if (text && text.length > 10) return text;
      }

      // No pin yet (or pin gone) — use the last element and set pin on first real content.
      const el = containers[containers.length - 1];
      const text = extractRawText(el).trim();

      if (text && text.length > 10) {
        return text;
      }
    } catch (e) {
      console.warn('[Virola] Error getting message:', e.message);
    }

    return '';
  }

  // ── Token detection for streaming ─────────────────────────────────────────────
  function detectTokenType(text) {
    if (text.match(/```\w*\s*$/)) return 'code_start';
    if (text.includes('\n```') || text.match(/\n```\s*$/)) return 'code_end';
    return 'text';
  }

  // ── Clear all timers ────────────────────────────────────────────────────────
  function clearAllTimers() {
    if (debounceTimer) {
      clearTimeout(debounceTimer);
      debounceTimer = null;
    }
    if (stabilityCheckTimer) {
      clearInterval(stabilityCheckTimer);
      stabilityCheckTimer = null;
    }
    stabilityRound = 0;
  }

  // ── Schedule stability check ──────────────────────────────────────────────────
  // Only called after 800ms of NO text changes
  function scheduleStabilityCheck() {
    stabilityRound = 0;
    lastStableText = lastSeenText;

    // 2 rounds at 300ms = 600ms total stability required (fast!)
    stabilityCheckTimer = setInterval(() => {
      const currentText = getLatestAssistantMessage();

      if (currentText === lastStableText) {
        stabilityRound++;

        if (stabilityRound >= 2) {
          clearAllTimers();
          if (!CONFIG.isGenerating()) {
            fireStreamEnd();
          }
        }
      } else {
        clearAllTimers();
        lastStableText = currentText;
        lastSeenText = currentText;
        scheduleDebounce();
      }
    }, 300);
  }

  // ── Schedule debounce ───────────────────────────────────────────────────────
  // 800ms debounce - resets on EVERY text change (was 3000ms — too slow)
  function scheduleDebounce() {
    clearAllTimers();

    debounceTimer = setTimeout(() => {
      scheduleStabilityCheck();
    }, 800);
  }

  // ── FIRE STREAM END (only once!) ─────────────────────────────────────────────
  function fireStreamEnd() {
    // CRITICAL: Hard guard - can only fire once per stream
    if (streamEndFired) {
      console.log('[Virola] Stream-end already fired, ignoring');
      return;
    }

    if (!isStreaming) {
      console.log('[Virola] Not streaming, ignoring stream-end');
      return;
    }

    streamEndFired = true;
    lastStreamEndTime = Date.now();
    console.log('[Virola] >>> FIRING STREAM END <<<');

    isStreaming = false;
    clearAllTimers();

    // Prefer rawMarkdownBuffer (raw fences intact) over DOM-accumulated text.
    // The fetch intercept captures the true SSE stream — no prose contamination.
    // Only fall back to accumulatedText if rawMarkdownBuffer is empty or contains
    // a <tool_result> injection marker.
    //
    // CRITICAL: Do NOT compare lengths. accumulatedText is built from rendered DOM
    // text which can be LONGER than raw markdown because the DOM accumulates
    // injected tool_result prose appended onto the previous message's code block.
    // Always trust rawMarkdownBuffer when it has real content.
    const rawIsClean = rawMarkdownBuffer &&
      rawMarkdownBuffer.length > 20 &&
      !/<tool_result>/i.test(rawMarkdownBuffer);
    const rawBest = rawIsClean ? rawMarkdownBuffer : accumulatedText;

    // Strip any injected <tool_result> blocks and agent headers from whatever
    // text we're about to send. The server does the same, but cleaning here
    // means the logged action preview is also accurate.
    const bestFullText = rawBest
      .replace(/<tool_result>[\s\S]*?<\/tool_result>/gi, '')
      .replace(/<tool_result>[\s\S]*/gi, '')
      .replace(/^#\s*\[AGENT:[^\]]*\]\s*$/gim, '')
      .trim();

    console.log(`[Virola] fullText source: ${rawMarkdownBuffer.length > accumulatedText.length ? 'raw fetch intercept' : 'DOM accumulation'} (${bestFullText.length} chars)`);

    // Extract actions from best text (for local logging only)
    const actions = parseActions(bestFullText);

    if (actions.length > 0) {
      console.log('[Virola] Found', actions.length, 'actions to execute');
      actions.forEach((action, i) => {
        console.log(`  Action ${i + 1}: ${action.type} - ${action.params?.path || action.params?.command?.slice(0, 50) || ''}`);
      });
    }

    // Send stream-end message with full accumulated text
    chrome.runtime.sendMessage({
      type: 'VIROLA_STREAM_END',
      streamId: currentStreamId,
      fullText: bestFullText,
      tokenCount: tokenCount,
      duration: Date.now() - streamStartTime,
      source: SOURCE,
      platform: PLATFORM
    });

    // Reset state for next stream - CRITICAL: clear lastSeenText AND rawMarkdownBuffer
    accumulatedText = '';
    rawMarkdownBuffer = '';     // CRITICAL: clear for next stream
    currentStreamId = null;
    tokenCount = 0;
    lastSeenText = '';          // CRITICAL: clear for next stream
    pinnedStreamElement = null; // CRITICAL: release element pin for next stream

    console.log('[Virola] Stream reset complete, lastSeenText and rawMarkdownBuffer cleared');
  }

  // ── Parse actions from text ──────────────────────────────────────────────────
  // NOTE: This is used only for local logging/preview in the content script.
  // The authoritative parsing happens server-side in parseActionsWithPartial().
  // Keep this in sync with server.js logic.
  const CMD_LANGS_CS = new Set(['bash', 'sh', 'shell', 'zsh', 'ps1', 'powershell', 'cmd', 'bat']);

  function parseActions(text) {
    const actions = [];

    // Strip <think>/<thinking> blocks
    const cleanText = text
      .replace(/<think>[\s\S]*?<\/think>/gi, '')
      .replace(/<thinking>[\s\S]*?<\/thinking>/gi, '')
      .trim();

    // Pattern 1: Complete fenced code blocks  ```lang\n// FILENAME: path\ncontent\n```
    const completeFenceRe = /```(\w*)\s*\n([\s\S]*?)```/g;
    for (const match of cleanText.matchAll(completeFenceRe)) {
      const lang = (match[1] || '').toLowerCase().trim();
      const blockContent = match[2];

      // Shell fence → execute entire block as command
      if (CMD_LANGS_CS.has(lang)) {
        const cmd = blockContent.trim();
        if (cmd) actions.push({ type: 'execute_command', params: { command: cmd } });
        continue;
      }

      const lines = blockContent.split('\n');
      const firstLine = lines[0].trim();

      // // FILENAME: directive
      const filenameMatch = firstLine.match(/^\/\/\s*FILENAME:\s*(.+)$/i);
      if (filenameMatch) {
        const filePath = filenameMatch[1].trim();
        let content = lines.slice(1).join('\n');
        // Strip ALL leading blank lines (AI sometimes inserts blank line after // FILENAME:)
        while (content.startsWith('\n')) content = content.slice(1);
        if (content.endsWith('\n')) content = content.slice(0, -1);
        if (filePath && content.trim()) {
          actions.push({ type: 'write_file', params: { path: filePath, content, language: lang } });
        }
        continue;
      }

      // // COMMAND: directive
      const cmdDirective = firstLine.match(/^\/\/\s*COMMAND:\s*(.+)$/i);
      if (cmdDirective) {
        const cmd = cmdDirective[1].trim();
        if (cmd) actions.push({ type: 'execute_command', params: { command: cmd } });
        continue;
      }

      // Generic path comment: // src/file.ext  or  # src/file.ext
      const pathComment = firstLine.match(/^(?:\/\/|#)\s*([\w./-]+\.\w+)\s*$/);
      if (pathComment) {
        const filePath = pathComment[1].trim();
        let content = lines.slice(1).join('\n');
        // Strip ALL leading blank lines
        while (content.startsWith('\n')) content = content.slice(1);
        if (content.endsWith('\n')) content = content.slice(0, -1);
        if (filePath && content.trim() && content.length > 5) {
          actions.push({ type: 'write_file', params: { path: filePath, content, language: lang } });
        }
      }
    }

    // Pattern 2: Standalone // COMMAND: lines outside code blocks
    const noCodeBlocks = cleanText.replace(/```[\s\S]*?```/g, '');
    const cmdLineRe = /^[ \t]*\/\/\s*COMMAND:\s*(.+)$/gim;
    for (const match of noCodeBlocks.matchAll(cmdLineRe)) {
      const cmd = match[1].trim();
      if (cmd) actions.push({ type: 'execute_command', params: { command: cmd } });
    }

    return actions;
  }

  // ── Process text change ───────────────────────────────────────────────────────
  function processTextChange(rawText) {
    if (!rawText) return;
    // CRITICAL: ignore DOM mutations caused by our own injectText calls
    if (isInjecting) return;

    // Strip <think>/<thinking> blocks from DeepSeek R1 and similar models.
    // These are reasoning/chain-of-thought tokens — never action content.
    const newText = stripThinkingText(rawText);
    if (!newText) return;

    // Check if this is genuinely new text
    if (newText === lastSeenText) return;

    // CRITICAL: if the NEW content appended since last seen is a <tool_result>
    // block, this is our own injected feedback being picked up by the observer.
    // Block it even if isInjecting has already expired.
    const newContent = newText.slice(lastSeenText.length);
    if (/<tool_result>/i.test(newContent) || /^[\s]*<tool_result>/i.test(newText.trimStart())) {
      console.log('[Virola] Skipping tool_result injection detected in new content');
      return;
    }

    // ── Stream START detection ──────────────────────────────────────────────
    if (!isStreaming && newText.length > 0) {
      // Cooldown: don't start a new stream immediately after one just fired
      // This blocks injected tool_results from being treated as AI output
      if ((Date.now() - lastStreamEndTime) < STREAM_COOLDOWN_MS) {
        return;
      }
      console.log('[Virola] >>> STREAM START <<<');
      isStreaming = true;
      streamEndFired = false;  // Reset guard
      streamStartTime = Date.now();
      currentStreamId = generateStreamId();
      tokenCount = 0;
      accumulatedText = '';
      rawMarkdownBuffer = ''; // Reset here too — belt-and-suspenders against stale fetch data

      // PIN the current last message element so DOM reads for this stream
      // never bleed into a different element that appears later (e.g. after
      // tool_result injection triggers a new AI response on DeepSeek).
      try {
        const containers = document.querySelectorAll(CONFIG.messageContainer);
        pinnedStreamElement = containers.length ? containers[containers.length - 1] : null;
        console.log('[Virola] Pinned stream element:', pinnedStreamElement?.className?.slice(0, 60));
      } catch { pinnedStreamElement = null; }

      chrome.runtime.sendMessage({
        type: 'VIROLA_STREAM_START',
        streamId: currentStreamId,
        source: SOURCE,
        platform: PLATFORM
      });
    }

    // ── Accumulate and send chunk ────────────────────────────────────────────
    if (newContent) {
      tokenCount += newContent.length;
      accumulatedText += newContent;

      chrome.runtime.sendMessage({
        type: 'VIROLA_STREAM_CHUNK',
        data: {
          text: newContent,
          fullText: newText,
          accumulatedText: accumulatedText,
          type: detectTokenType(newText),
          timestamp: Date.now(),
          streamId: currentStreamId
        },
        source: SOURCE
      });

      console.log(`[Virola] Chunk: +${newContent.length} chars, total: ${accumulatedText.length}`);
    }

    lastSeenText = newText;

    // ── Reset debounce on EVERY text change ──────────────────────────────────
    // This is the key fix - any pause < 3s just restarts the timer
    if (isStreaming) {
      scheduleDebounce();
    }
  }

  // ── Check for stream completion ───────────────────────────────────────────────
  function checkStreamComplete() {
    if (!isStreaming || streamEndFired) return;

    // Check if AI is still generating
    if (CONFIG.isGenerating()) {
      return; // Still generating, don't fire
    }

    // AI stopped generating - trigger the flow
    console.log('[Virola] AI stopped generating, initiating debounce');
    scheduleDebounce();
  }

  // ── Message listener (from background) ─────────────────────────────────────
  chrome.runtime.onMessage.addListener((msg) => {
    if (msg.type === 'VIROLA_INJECT') {
      console.log('[Virola] Injecting text into chat');
      injectText(msg.text, msg.autoSend);
    }

    if (msg.type === 'VIROLA_FILE_CHUNK') {
      console.debug('[Virola] File chunk:', msg.path);
    }

    if (msg.type === 'VIROLA_FILE_COMPLETE') {
      console.log('[Virola] File written:', msg.path, msg.bytes, 'bytes');
    }

    if (msg.type === 'VIROLA_COMMAND_RESULT') {
      console.log('[Virola] Command result:', msg.exitCode, msg.output?.slice(0, 100));
      // Inject command result back into chat
      if (msg.output) {
        const resultText = `<tool_result>\n<output>${msg.output}</output>\n<exit_code>${msg.exitCode}</exit_code>\n</tool_result>`;
        injectText(resultText, true);
      }
    }

    if (msg.type === 'VIROLA_ACTION_RESULT') {
      console.log('[Virola] Action result:', msg.action?.type, msg.result);
    }
  });

  // ── Mutation Observer Setup ──────────────────────────────────────────────────
  const observer = new MutationObserver((mutations) => {
    if (isInjecting) return; // ignore our own DOM writes
    const text = getLatestAssistantMessage();

    // Always process text changes
    if (text && text !== lastSeenText) {
      processTextChange(text);
    }

    // Check if streaming just completed
    if (isStreaming && !streamEndFired) {
      checkStreamComplete();
    }
  });

  observer.observe(document.body, {
    childList: true,
    subtree: true,
    characterData: true,
    characterDataOldValue: true
  });

  // ── Periodic check for slow updates ─────────────────────────────────────────
  setInterval(() => {
    if (isInjecting) return;
    const text = getLatestAssistantMessage();
    if (text && text !== lastSeenText) {
      processTextChange(text);
    }
  }, 500);

  console.log(`[Virola v16.0] Stream capture active for ${PLATFORM} - debounce: 800ms, stability: 2×300ms`);
  console.log('[Virola] fetch() intercepted for raw markdown capture');
  console.log('[Virola] inject-loop guard: isInjecting lock active');
  console.log('[Virola] Bash fences auto-execute — no // COMMAND: prefix needed');
  console.log('[Virola] Platform config:', JSON.stringify({ generating: CONFIG.isGenerating.toString(), container: CONFIG.messageContainer }));

})();
