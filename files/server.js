#!/usr/bin/env node
// ─── Virola Server v17.1 ────────────────────────────────────────────────────
//
// FIXES v17.1 (bug: command text contaminated by injected tool_result prose):
// - parseActionsWithPartial() now trims non-shell prose that leaked into bash
//   fences (e.g. "python3 -m venv venvVirtual environment created...").
//   Each bash block's lines are scanned; only leading shell-like lines are kept.
// Real-time streaming server with complete action execution
//
// CHANGES v17.0:
// - EXPANDED isLongRunningCommand: now detects node, npm start/run, npx (vite/next/nodemon/etc),
//   bun, and deno servers — not just Python
// - ADDED continuous bg_process_output SSE events: stdout/stderr of background processes
//   are streamed live to the chat after the 3s startup window closes
//
// CHANGES v16.0 (merged from v14.1 + v15, with new OpenAI endpoint):
// - REMOVED chat architecture entirely: no /inject, /chat-events, /extension-status,
//   no chatClients, no broadcastChat, no injectQueue, no injectWaiters
// - ADDED OpenAI-compatible endpoint: POST /v1/chat/completions
//   → accepts { model, messages, stream } from any third-party chat UI
//   → detects bash fences in the assistant's reply and executes them immediately
//   → streams back Server-Sent Events (stream: true) or returns JSON (stream: false)
// - KEPT all v15 bash fence detection (no // COMMAND: prefix needed ever)
// - RESTORED v14's richer conversational guard (prose-ratio check)
// - Bash command execution is direct spawnSync — swift and efficient
// - All timeouts removed; spawnSync waits for natural process exit
// - Pip/python/general dedup: only blocks retries after SUCCESS

const http = require('http');
const https = require('https');
const fs = require('fs');
const path = require('path');
const { execSync, spawnSync, spawn } = require('child_process');
const readline = require('readline');

// ── CLI ───────────────────────────────────────────────────────────────────────
const args = process.argv.slice(2);
const getArg = (flag, def) => { const i = args.indexOf(flag); return i !== -1 && args[i + 1] ? args[i + 1] : def; };
const hasFlag = flag => args.includes(flag);

const PORT = parseInt(getArg('--port', '3172'), 10);
const BASE_DIR = path.resolve(getArg('--dir', path.join(__dirname, '..')));
const AUTO_APPROVE = hasFlag('--auto-approve');
const VERSION = '17.2.0';

// ── Root Workspace ────────────────────────────────────────────────────────────
const WORKSPACE = BASE_DIR;

const GITIGNORE_ENTRIES = [
  'server.js', 'package.json', 'package-lock.json', 'node_modules/',
  '.vibeignore', 'README.md', 'CODE_EXPLANATION.md', 'chrome-extension/',
  'background.js', 'manifest.json', 'popup.html', 'popup.js',
  'core.js', 'content-scripts/', 'prompt.md', 'VIROLA_SYSTEM_PROMPT.md',
];

function ensureGitignore() {
  const gitignorePath = path.join(WORKSPACE, '.gitignore');
  const marker = '# virola-managed';
  if (fs.existsSync(gitignorePath)) {
    const existing = fs.readFileSync(gitignorePath, 'utf8');
    if (existing.includes(marker)) return;
    fs.appendFileSync(gitignorePath, '\n' + marker + '\n' + GITIGNORE_ENTRIES.join('\n') + '\n');
  } else {
    fs.writeFileSync(gitignorePath, marker + '\n' + GITIGNORE_ENTRIES.join('\n') + '\n');
  }
  log('.gitignore updated with Virola entries', C.green);
}

const SERVER_IGNORES = new Set([
  'server.js', 'package.json', 'package-lock.json', 'node_modules',
  '.vibeignore', 'README.md', 'CODE_EXPLANATION.md',
  'chrome-extension', 'background.js', 'manifest.json',
  'popup.html', 'popup.js', 'core.js', 'content-scripts',
  'prompt.md', 'VIROLA_SYSTEM_PROMPT.md',
]);

// ── Colors ───────────────────────────────────────────────────────────────────
const C = {
  reset: '\x1b[0m', dim: '\x1b[2m', green: '\x1b[32m',
  yellow: '\x1b[33m', red: '\x1b[31m', cyan: '\x1b[36m',
  bold: '\x1b[1m', magenta: '\x1b[35m'
};

function ts() { return new Date().toISOString().substr(11, 8); }
function log(msg, color = C.reset) {
  process.stdout.write(`${C.dim}[${ts()}]${C.reset} ${color}${msg}${C.reset}\n`);
}

// ── Stats ─────────────────────────────────────────────────────────────────────
const stats = {
  dispatched: 0, succeeded: 0, failed: 0,
  streamsReceived: 0, chunksReceived: 0, partialBlocks: 0,
  skippedDuplicates: 0, skippedPipDuplicates: 0, skippedPythonDuplicates: 0,
  openaiRequests: 0,
};

// ── Background Process Registry ───────────────────────────────────────────────
const bgProcesses = new Map();

// ── Stream-level dedup ────────────────────────────────────────────────────────
// Prevents the same AI response from being executed twice when the extension
// fires stream-end multiple times (e.g. once for the injected tool_result and
// once for the actual response). We hash the fullText and reject repeats within
// a short window.
const recentStreamHashes = new Map(); // hash → timestamp
const STREAM_HASH_TTL_MS = 8000;

function simpleHash(str) {
  let h = 0;
  for (let i = 0; i < Math.min(str.length, 500); i++) {
    h = Math.imul(31, h) + str.charCodeAt(i) | 0;
  }
  return h.toString(36) + str.length;
}

function isDuplicateStream(fullText) {
  const hash = simpleHash(fullText);
  const now = Date.now();
  // Evict expired entries
  for (const [k, ts] of recentStreamHashes) {
    if (now - ts > STREAM_HASH_TTL_MS) recentStreamHashes.delete(k);
  }
  if (recentStreamHashes.has(hash)) return true;
  recentStreamHashes.set(hash, now);
  return false;
}

// ── Unified Command Dedup ─────────────────────────────────────────────────────
// Strategy: in-flight lock + post-completion cooldown.
//
// Time-based windows (3s, 15s, 60s) all fail for long-running commands like
// `pip install` or `python3 -m venv` that take minutes — duplicate stream-end
// events arriving mid-execution simply fall outside any fixed window.
//
// Instead we track three states per command key:
//   RUNNING  — command is currently executing → block all duplicates
//   COOLING  — command finished, cooling down for POST_RUN_COOLDOWN_MS → block
//   IDLE     — cooldown expired → allow a fresh run
//
// This means a 7-minute pip install blocks duplicates for the full 7 minutes
// plus the cooldown, regardless of how many stream-end events arrive.

const POST_RUN_COOLDOWN_MS = 60_000; // 60s cooldown after a command completes

// Map<cmdKey, { state: 'running'|'cooling'|'idle', startedAt, finishedAt, count }>
const cmdLockMap = new Map();

function normKey(cmd) {
  return cmd.trim().replace(/\s+/g, ' ').toLowerCase();
}

// Returns true if the command should be blocked (running or cooling).
// Side-effect: registers the command as running if it was IDLE (first call).
function isDuplicateCommand(cmd) {
  const key = normKey(cmd);
  const now = Date.now();
  const entry = cmdLockMap.get(key);

  if (entry) {
    if (entry.state === 'running') {
      entry.count++;
      log(`  ⛔ dedup (in-flight, ${Math.round((now - entry.startedAt) / 1000)}s running): ${cmd.slice(0, 60)}`, C.yellow);
      stats.skippedDuplicates++;
      return true;
    }
    if (entry.state === 'cooling') {
      const elapsed = now - entry.finishedAt;
      if (elapsed < POST_RUN_COOLDOWN_MS) {
        entry.count++;
        log(`  ⛔ dedup (cooldown, ${Math.round(elapsed / 1000)}s / ${POST_RUN_COOLDOWN_MS / 1000}s): ${cmd.slice(0, 60)}`, C.yellow);
        stats.skippedDuplicates++;
        return true;
      }
      // Cooldown expired — fall through and allow re-run
    }
  }

  // Register as running
  cmdLockMap.set(key, { state: 'running', startedAt: now, finishedAt: null, count: 1 });
  // Prune stale idle entries
  for (const [k, v] of cmdLockMap) {
    if (v.state === 'cooling' && (now - v.finishedAt) > POST_RUN_COOLDOWN_MS * 2) cmdLockMap.delete(k);
  }
  return false;
}

// Call after a command finishes (success or failure) to start the cooldown.
function markCommandFinished(cmd) {
  const key = normKey(cmd);
  const entry = cmdLockMap.get(key);
  if (entry && entry.state === 'running') {
    entry.state = 'cooling';
    entry.finishedAt = Date.now();
  }
}

// Legacy aliases used elsewhere in the file — all delegate to the unified system.
function isDuplicatePip(cmd) {
  if (!/^\s*pip3?\s+install\b/i.test(cmd.trim().split('\n')[0])) return false;
  return isDuplicateCommand(cmd);
}
function isDuplicatePython(cmd) {
  if (!/^\s*python3?\s+/i.test(cmd.trim().split('\n')[0])) return false;
  return isDuplicateCommand(cmd);
}
function markPipSucceeded(cmd) { markCommandFinished(cmd); }
function markPythonSucceeded(cmd) { markCommandFinished(cmd); }
function markCommandSucceeded(cmd) { markCommandFinished(cmd); }

// ── Long-Running Process Detection ───────────────────────────────────────────
const LONG_RUNNING_PYTHON_RE = /^(\s*python3?\s+(?!-c\s)(?:-m\s+(uvicorn|flask|gunicorn|http\.server|django|manage)|(?!-)\S+\.py\b))/i;

// Direct invocation: uvicorn, gunicorn, flask run, etc. (without python -m prefix)
const LONG_RUNNING_DIRECT_RE = /^(\s*(uvicorn|gunicorn|flask)\s)/i;

// Node/JS servers: `node server.js`, `node .`, `node src/index.js`
const LONG_RUNNING_NODE_RE   = /^\s*node\s+(?!-e\s)(?!--eval\s)\S/i;

// npm/yarn/pnpm lifecycle scripts that start servers
const LONG_RUNNING_NPM_RE    = /^\s*(npm|yarn|pnpm)\s+(start|run\s+(start|serve|dev|watch|preview))\b/i;

// npx-launched servers: next dev, vite, ts-node, nodemon, etc.
const LONG_RUNNING_NPX_RE    = /^\s*npx\s+(next|vite|ts-node|nodemon|tsx|serve|http-server|lite-server|json-server|fastify|nest)\b/i;

// bun / deno servers
const LONG_RUNNING_BUN_RE    = /^\s*bun\s+(run\s+)?\S+\.(ts|js)\b/i;
const LONG_RUNNING_DENO_RE   = /^\s*deno\s+(run|serve)\b/i;

function isLongRunningCommand(cmd) {
  // Check each segment of a chained command (cd foo && uvicorn ...)
  // so that "cd dir && uvicorn ..." is correctly identified as long-running.
  const segments = cmd.split(/&&|\|/).map(s => s.trim());
  return segments.some(first =>
    LONG_RUNNING_PYTHON_RE.test(first) ||
    LONG_RUNNING_DIRECT_RE.test(first) ||
    LONG_RUNNING_NODE_RE.test(first)   ||
    LONG_RUNNING_NPM_RE.test(first)    ||
    LONG_RUNNING_NPX_RE.test(first)    ||
    LONG_RUNNING_BUN_RE.test(first)    ||
    LONG_RUNNING_DENO_RE.test(first)
  );
}

function processLabel(cmd) {
  return cmd.trim().split('\n')[0].replace(/\s+/g, ' ').slice(0, 60);
}

const STARTUP_COLLECT_MS = 4000;

// ── Terminal launcher ─────────────────────────────────────────────────────────
// Detects the best available terminal emulator on the current OS and opens the
// server command in a NEW TAB / WINDOW so the user can see output directly.
// A companion relay process writes all stdout/stderr to a temp log file, which
// the main Virola process tails and streams back as SSE events.
function detectTerminalLauncher() {
  const isWin  = process.platform === 'win32';
  const isMac  = process.platform === 'darwin';

  if (isWin) {
    // Windows Terminal (wt), then plain cmd
    try { execSync('where wt', { stdio: 'ignore' }); return 'wt'; } catch {}
    return 'cmd';
  }
  if (isMac) {
    return 'osascript'; // always available on macOS
  }
  // Linux — try common terminals in preference order
  const linuxTerms = ['gnome-terminal', 'xfce4-terminal', 'konsole', 'xterm', 'lxterminal', 'tilix', 'alacritty', 'kitty'];
  for (const t of linuxTerms) {
    try { execSync(`which ${t}`, { stdio: 'ignore' }); return t; } catch {}
  }
  return null; // fallback to hidden spawn
}

const TERMINAL = detectTerminalLauncher();
log(`  Terminal launcher: ${TERMINAL || 'none (hidden spawn fallback)'}`, C.dim);

function buildTerminalCommand(shellCmd, logFile) {
  // We wrap the user's command so that:
  //  1. It runs in a NEW visible terminal tab/window
  //  2. All output is tee'd to logFile so Virola can tail it
  //  3. The window stays open after the process exits (so user sees errors)
  const wrapped = `bash -c '(${shellCmd}) 2>&1 | tee "${logFile}"; echo; echo "─── process exited ───"; read -p "Press Enter to close..."'`;

  switch (TERMINAL) {
    case 'osascript': {
      // macOS Terminal.app — open a new tab in the frontmost window
      const escaped = wrapped.replace(/\\/g, '\\\\').replace(/"/g, '\\"');
      return [
        'osascript', [
          '-e', 'tell application "Terminal"',
          '-e', `do script "${escaped}"`,
          '-e', 'end tell',
        ]
      ];
    }
    case 'wt': {
      // Windows Terminal — new tab
      const ps = `powershell -NoExit -Command "${shellCmd.replace(/"/g, '`"')} 2>&1 | Tee-Object -FilePath '${logFile}'"`;
      return ['wt', ['--', 'cmd', '/c', ps]];
    }
    case 'cmd': {
      return ['cmd', ['/c', 'start', 'cmd', '/k', shellCmd]];
    }
    case 'gnome-terminal':
      return [TERMINAL, ['--', 'bash', '-c', wrapped]];
    case 'xfce4-terminal':
      return [TERMINAL, ['--command', wrapped, '--hold']];
    case 'konsole':
      return [TERMINAL, ['--noclose', '-e', 'bash', '-c', wrapped]];
    case 'tilix':
      return [TERMINAL, ['-e', wrapped]];
    case 'alacritty':
    case 'kitty':
      return [TERMINAL, ['-e', 'bash', '-c', wrapped]];
    default: // xterm and unknown
      return [TERMINAL || 'xterm', ['-hold', '-e', wrapped]];
  }
}

function spawnBackground(command) {
  return new Promise((resolve) => {
    const label = processLabel(command);

    // Kill any existing process with the same label
    if (bgProcesses.has(label)) {
      const prev = bgProcesses.get(label);
      try { prev.proc?.kill('SIGTERM'); } catch {}
      try { if (prev.tailer) prev.tailer.kill(); } catch {}
      bgProcesses.delete(label);
      log(`  ↳ killed previous bg process: ${label}`, C.yellow);
    }

    const startTime = Date.now();

    // Temp log file — the terminal tees output here; we tail it for SSE
    const logFile = path.join(require('os').tmpdir(), `virola-bg-${Date.now()}.log`);
    // Touch the file so the tailer can open it immediately
    fs.writeFileSync(logFile, '');

    // ── Open a new terminal window/tab ────────────────────────────────────────
    if (TERMINAL) {
      const [termBin, termArgs] = buildTerminalCommand(command, logFile);
      // The terminal launcher itself exits immediately after opening the window
      const launcher = spawn(termBin, termArgs, {
        cwd: WORKSPACE, stdio: 'ignore', detached: true,
      });
      launcher.unref();
      // SAFETY: a spawn error on the launcher must never crash the Node server.
      launcher.on('error', (err) => {
        log(`  ⚠ terminal launcher error (${label}): ${err.message}`, C.yellow);
      });

      log(`  ↗ opened in new terminal tab: ${label}`, C.cyan);

      // ── Tail the log file and stream output back via SSE ────────────────────
      let startupOutput = '';
      let settled = false;
      let tailProc = null;
      // Never store -1 or 0 as a PID — process.kill(0,...) sends the signal to
      // the entire process group which includes the Node server itself.
      let fakePid = null;

      function settle() {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        // Only record a positive PID; otherwise leave null so killBgProcess
        // skips the process.kill() call and stays safe.
        fakePid = (launcher.pid && launcher.pid > 0) ? launcher.pid : null;
        // Store a sentinel entry — no real proc to kill, but logFile tailer tracked
        bgProcesses.set(label, { pid: fakePid, proc: null, tailer: tailProc, logFile, command, startTime });
        log(`  ✓ bg terminal started: ${label}`, C.green);
        broadcastSSE('bg_process_started', { label, pid: fakePid, command, startupOutput, terminal: true });
        resolve({
          ok: true, background: true, pid: fakePid, label, startupOutput, terminal: true,
          message: `Opened in new terminal tab. Click "⏹ Kill Server" in the Virola popup or POST /kill-last to stop.`
        });
      }

      // Tail the log file for live output
      tailProc = spawn('tail', ['-f', logFile], { stdio: ['ignore', 'pipe', 'ignore'] });
      // SAFETY: tailer errors must never propagate as unhandled 'error' events.
      tailProc.on('error', (err) => {
        log(`  ⚠ log tailer error (${label}): ${err.message}`, C.yellow);
      });
      tailProc.stdout.on('error', (err) => {
        log(`  ⚠ log tailer stdout error (${label}): ${err.message}`, C.yellow);
      });
      tailProc.stdout.on('data', chunk => {
        try {
          const text = chunk.toString();
          if (!settled) {
            startupOutput += text;
          } else {
            log(`  │ [${label}] ${text.trimEnd()}`, C.dim);
            broadcastSSE('bg_process_output', { label, output: text });
          }
        } catch (err) {
          log(`  ⚠ log tailer data handler error (${label}): ${err.message}`, C.yellow);
        }
      });
      tailProc.on('exit', () => {
        bgProcesses.delete(label);
        broadcastSSE('bg_process_exited', { label, pid: fakePid, exitCode: null });
        try { fs.unlinkSync(logFile); } catch {}
      });

      const timer = setTimeout(() => settle(), STARTUP_COLLECT_MS);
      return;
    }

    // ── Fallback: hidden child process (no terminal available) ───────────────
    // CRITICAL: detached:true so the child process is in its own process group
    // and survives if the node server is restarted or stopped (WSL/Linux).
    // We tee output to a log file so we can still tail it for SSE and kill it.
    log(`  ⚠ No terminal found — running ${label} as detached hidden process`, C.yellow);

    // Log file for output capture
    const hiddenLogFile = path.join(require('os').tmpdir(), `virola-bg-${Date.now()}.log`);
    fs.writeFileSync(hiddenLogFile, '');

    const proc = spawn('bash', ['-c', `(${command}) 2>&1 | tee "${hiddenLogFile}"`], {
      cwd: WORKSPACE,
      stdio: ['ignore', 'ignore', 'ignore'],
      detached: true,   // ← KEY: own process group, survives node server death
    });
    proc.unref();       // ← node server won't wait for this child to exit
    // SAFETY: a spawn error must never propagate as an unhandled 'error' event
    // and crash the Node server.  Resolve as a failed result so the caller always
    // gets a response and the promise never hangs.
    proc.on('error', (err) => {
      log(`  ⚠ bg proc spawn error (${label}): ${err.message}`, C.yellow);
      resolve({ ok: false, background: false, output: `spawn error: ${err.message}`, exitCode: 1 });
    });

    let startupOutput = '';
    let settled = false;
    // SAFETY: declare hiddenTailer BEFORE anything that can call settle(), because
    // proc 'exit' can fire synchronously (e.g. command not found) before we reach
    // the hiddenTailer = spawn(...) line below.  The closure must always see a
    // defined reference even if still null.
    let hiddenTailer = null;

    function settle(exitCode) {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      const pid = proc.pid;
      const alive = exitCode === null;
      if (alive) {
        bgProcesses.set(label, { pid, proc, tailer: hiddenTailer, logFile: hiddenLogFile, command, startTime });
        log(`  ✓ bg process started (detached): ${label} (pid ${pid})`, C.green);
        broadcastSSE('bg_process_started', { label, pid, command, startupOutput, terminal: false });
        resolve({
          ok: true, background: true, pid, label, startupOutput, terminal: false,
          message: `Process started detached. pid ${pid}. Click "⏹ Kill Server" in the Virola popup or POST /kill-last to stop.`
        });
      } else {
        // Process already gone — stop the tailer so it doesn't leak.
        if (hiddenTailer) { try { hiddenTailer.kill(); } catch {} }
        log(`  ↳ bg candidate exited early (code ${exitCode}): ${label}`, C.yellow);
        resolve({ ok: exitCode === 0, background: false, output: startupOutput, exitCode });
      }
    }

    // Tail the log file for output (proc stdio is ignore since detached)
    hiddenTailer = spawn('tail', ['-f', hiddenLogFile], { stdio: ['ignore', 'pipe', 'ignore'] });
    // SAFETY: tailer errors must never propagate as unhandled 'error' events.
    hiddenTailer.on('error', (err) => {
      log(`  ⚠ hidden tailer error (${label}): ${err.message}`, C.yellow);
    });
    hiddenTailer.stdout.on('error', (err) => {
      log(`  ⚠ hidden tailer stdout error (${label}): ${err.message}`, C.yellow);
    });
    hiddenTailer.stdout.on('data', chunk => {
      try {
        const text = chunk.toString();
        if (!settled) { startupOutput += text; }
        else {
          log(`  │ [${label}] ${text.trimEnd()}`, C.dim);
          broadcastSSE('bg_process_output', { label, pid: proc.pid, output: text });
        }
      } catch (err) {
        log(`  ⚠ hidden tailer data handler error (${label}): ${err.message}`, C.yellow);
      }
    });

    // Since proc is detached+unref'd, we can't rely on proc 'exit'.
    // The tailer exits when the process dies and closes the log file.
    hiddenTailer.on('exit', () => {
      bgProcesses.delete(label);
      broadcastSSE('bg_process_exited', { label, pid: proc.pid, exitCode: null });
      try { fs.unlinkSync(hiddenLogFile); } catch {}
    });

    const timer = setTimeout(() => settle(null), STARTUP_COLLECT_MS);
  });
}

function killBgProcess(label) {
  const entry = bgProcesses.get(label);
  if (!entry) return { ok: false, error: `No background process with label: ${label}` };
  try {
    // Kill by PID/process group — NEVER use pkill -f (it pattern-matches and
    // can accidentally kill the node server process itself).
    // SAFETY: only send signals for valid positive PIDs.
    // pid=null means we had no reliable PID (terminal mode); pid=0 would send
    // the signal to OUR OWN process group — both are skipped.
    if (entry.pid && entry.pid > 0) {
      // Kill the entire process group spawned by the detached bash wrapper.
      // Negative PID = kill process group.
      try { process.kill(-entry.pid, 'SIGTERM'); } catch {}
      // Also try direct PID in case it's not a group leader
      try { process.kill(entry.pid, 'SIGTERM'); } catch {}
    }
    if (entry.proc) {
      try { entry.proc.kill('SIGTERM'); } catch {}
    }
    // Kill the log tailer
    if (entry.tailer) {
      try { entry.tailer.kill(); } catch {}
    }
    // Clean up log file
    if (entry.logFile) {
      try { fs.unlinkSync(entry.logFile); } catch {}
    }
    bgProcesses.delete(label);
    broadcastSSE('bg_process_killed', { label, pid: entry.pid });
    return { ok: true, label, pid: entry.pid };
  } catch (err) { return { ok: false, error: err.message }; }
}

// ── Active Streams & SSE ──────────────────────────────────────────────────────
const activeStreams = new Map();
const sseClients = new Set();

function broadcastSSE(event, data) {
  const payload = `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
  for (const res of sseClients) {
    try { res.write(payload); } catch { sseClients.delete(res); }
  }
}

// ── HTTP Server ───────────────────────────────────────────────────────────────
const server = http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }
  const url = new URL(req.url, `http://localhost:${PORT}`);
  route(url.pathname, req, res);
});

function route(pathname, req, res) {
  if (pathname === '/health' && req.method === 'GET') return handleHealth(res);
  if (pathname === '/execute' && req.method === 'POST') return handleExecute(req, res);
  if (pathname === '/stream-chunk' && req.method === 'POST') return handleStreamChunk(req, res);
  if (pathname === '/stream-end' && req.method === 'POST') return handleStreamEnd(req, res);
  if (pathname === '/stream' && req.method === 'GET') return handleSSE(req, res);
  if (pathname === '/files' && req.method === 'GET') return handleListWorkspace(res);
  if (pathname === '/paste' && req.method === 'POST') return handlePaste(req, res);
  if (pathname === '/kill-process' && req.method === 'POST') return handleKillProcess(req, res);
  if (pathname === '/kill-last'    && req.method === 'POST') return handleKillLast(res);
  if (pathname === '/processes' && req.method === 'GET') return handleListProcesses(res);
  // ── OpenAI-compatible endpoint ─────────────────────────────────────────────
  if (pathname === '/v1/chat/completions' && req.method === 'POST') return handleOpenAI(req, res);
  if (pathname === '/v1/models' && req.method === 'GET') return handleOpenAIModels(res);
  sendJSON(res, 404, { error: 'Not found' });
}

function handleHealth(res) {
  const processes = [];
  for (const [label, entry] of bgProcesses) {
    processes.push({ label, pid: entry.pid, uptimeMs: Date.now() - entry.startTime });
  }
  sendJSON(res, 200, {
    ok: true, tool: 'virola-v17', version: VERSION, stats,
    workspace: WORKSPACE, baseDir: BASE_DIR, autoApprove: AUTO_APPROVE,
    extensionClients: sseClients.size,
    backgroundProcesses: processes
  });
}

// ── OpenAI-Compatible Endpoint ────────────────────────────────────────────────
// POST /v1/chat/completions
// Accepts: { model, messages, stream, ... }
// The body's last assistant message (or all messages) is scanned for bash
// fences and any commands are executed immediately.
// Responds with a fake OpenAI-format completion so any third-party chat UI works.
function handleOpenAIModels(res) {
  sendJSON(res, 200, {
    object: 'list',
    data: [
      { id: 'virola-executor', object: 'model', created: 1700000000, owned_by: 'virola' },
    ]
  });
}

async function handleOpenAI(req, res) {
  let body = '';
  req.on('data', chunk => { body += chunk; });
  req.on('end', async () => {
    let payload;
    try { payload = JSON.parse(body); } catch { return sendJSON(res, 400, { error: { message: 'Invalid JSON', type: 'invalid_request_error' } }); }

    stats.openaiRequests++;
    const { messages = [], stream = false, model = 'virola-executor' } = payload;

    // Gather all assistant content to scan for commands.
    // Also include the last user message in case the UI puts commands there.
    const textToScan = messages
      .filter(m => m.role === 'assistant' || m.role === 'user')
      .map(m => (typeof m.content === 'string' ? m.content : (m.content || []).map(c => c.text || '').join('')))
      .join('\n\n');

    log(`← OpenAI request: ${messages.length} messages, stream=${stream}, model=${model}`, C.cyan);

    const { actions, partialBlocks } = parseActionsWithPartial(textToScan);
    log(`  Found ${actions.length} executable action(s)`, actions.length > 0 ? C.green : C.dim);

    // Execute all detected commands
    const commandOutputs = [];
    for (const action of actions) {
      if (action.type !== 'execute_command') continue;
      const cmd = action.params?.command || '';
      if (isDuplicatePip(cmd)) { commandOutputs.push({ cmd, output: '[pip dedup]', exitCode: 0 }); continue; }
      if (isDuplicatePython(cmd)) { commandOutputs.push({ cmd, output: '[python dedup]', exitCode: 0 }); continue; }
      if (isDuplicateCommand(cmd)) { commandOutputs.push({ cmd, output: '', exitCode: 0 }); continue; }

      log(`  $ ${cmd.slice(0, 100)}`, C.dim);
      try {
        const result = await dispatch(action);
        const exitCode = result.exitCode ?? 0;
        if (exitCode === 0) { markPipSucceeded(cmd); markPythonSucceeded(cmd); markCommandSucceeded(cmd); }
        commandOutputs.push({ cmd, output: result.output || '', exitCode, background: result.background || false });
        broadcastSSE('command_result', { command: cmd, output: result.output || '', exitCode });
        stats.succeeded++;
      } catch (err) {
        commandOutputs.push({ cmd, output: err.message, exitCode: 1 });
        broadcastSSE('command_result', { command: cmd, output: err.message, exitCode: 1 });
        stats.failed++;
      }
    }

    // Build a summary reply that includes command outputs
    let replyContent = '';
    if (commandOutputs.length > 0) {
      replyContent = commandOutputs.map(({ cmd, output, exitCode, background }) => {
        const label = background ? '⟳ background' : (exitCode === 0 ? '✓' : '✗');
        const out = output.trim();
        return `${label} \`${cmd.split('\n')[0].slice(0, 80)}\`${out ? `\n\`\`\`\n${out}\n\`\`\`` : ''}`;
      }).join('\n\n');
    } else {
      replyContent = '(no executable commands detected in message)';
    }

    const created = Math.floor(Date.now() / 1000);
    const completionId = 'chatcmpl-vb' + Date.now().toString(36);

    if (stream) {
      // Stream SSE format
      res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Access-Control-Allow-Origin': '*',
      });

      // Chunk the reply so streaming UIs see it flowing in
      const chunkSize = 40;
      for (let i = 0; i < replyContent.length; i += chunkSize) {
        const delta = replyContent.slice(i, i + chunkSize);
        const chunk = {
          id: completionId,
          object: 'chat.completion.chunk',
          created,
          model,
          choices: [{ index: 0, delta: { role: 'assistant', content: delta }, finish_reason: null }]
        };
        res.write(`data: ${JSON.stringify(chunk)}\n\n`);
      }

      // Final chunk
      const doneChunk = {
        id: completionId,
        object: 'chat.completion.chunk',
        created,
        model,
        choices: [{ index: 0, delta: {}, finish_reason: 'stop' }]
      };
      res.write(`data: ${JSON.stringify(doneChunk)}\n\n`);
      res.write('data: [DONE]\n\n');
      res.end();
    } else {
      // Regular JSON response
      sendJSON(res, 200, {
        id: completionId,
        object: 'chat.completion',
        created,
        model,
        choices: [{
          index: 0,
          message: { role: 'assistant', content: replyContent },
          finish_reason: 'stop'
        }],
        usage: { prompt_tokens: 0, completion_tokens: replyContent.length, total_tokens: replyContent.length },
        // Also include raw execution results for programmatic use
        _virola: { actions: actions.length, commandOutputs, partialBlocks }
      });
    }
  });
}

// ── /stream-chunk ─────────────────────────────────────────────────────────────
function handleStreamChunk(req, res) {
  let body = '';
  req.on('data', chunk => { body += chunk; });
  req.on('end', () => {
    let payload;
    try { payload = JSON.parse(body); } catch { return sendJSON(res, 400, { error: 'Invalid JSON' }); }

    const { streamId, chunk: text, accumulatedText, fullText: chunkFullText, type, language, timestamp } = payload;
    if (!streamId || !text) return sendJSON(res, 400, { error: 'Missing streamId or chunk' });

    stats.chunksReceived++;
    let stream = activeStreams.get(streamId);
    if (!stream) {
      stream = { chunks: [], latestFullText: '', startTime: Date.now(), language };
      activeStreams.set(streamId, stream);
    }

    stream.chunks.push({ text, type, language, timestamp });
    if (accumulatedText && accumulatedText.length > (stream.latestFullText || '').length) {
      stream.latestFullText = accumulatedText;
    }
    if (chunkFullText && chunkFullText.length > (stream.latestFullText || '').length) {
      stream.latestFullText = chunkFullText;
    }

    broadcastSSE('stream_chunk', { streamId, chunk: text, type, language, chunkIndex: stream.chunks.length });
    sendJSON(res, 200, { ok: true, chunkIndex: stream.chunks.length });
  });
}

// ── /stream-end ───────────────────────────────────────────────────────────────
async function handleStreamEnd(req, res) {
  let body = '';
  req.on('data', chunk => { body += chunk; });
  req.on('end', async () => {
    let payload;
    try { payload = JSON.parse(body); } catch { return sendJSON(res, 400, { error: 'Invalid JSON' }); }

    const { streamId, fullText, autoApprove: clientAuto } = payload;
    stats.streamsReceived++;

    const stream = activeStreams.get(streamId);
    let reconstructedText;

    const chunkDeltaText = stream && stream.chunks.length > 0
      ? stream.chunks.map(c => c.text).join('') : '';
    const snapshotText = stream?.latestFullText || '';

    if (fullText && fullText.length > 0) {
      reconstructedText = fullText;
      log(`${C.green}stream complete${C.reset} (fullText: ${fullText.length} chars)`);
    } else if (snapshotText.length > 0) {
      reconstructedText = snapshotText;
      log(`${C.green}stream complete${C.reset} (snapshot: ${snapshotText.length} chars)`);
    } else if (chunkDeltaText.length > 0) {
      reconstructedText = chunkDeltaText;
      log(`${C.yellow}stream complete${C.reset} (delta: ${stream.chunks.length} chunks)`);
    } else {
      log(`${C.yellow}stream-end: no text found${C.reset}`);
      return sendJSON(res, 200, { ok: true, actions: [], fullText: '' });
    }

    if (stream) activeStreams.delete(streamId);

    // Dedup: if this exact text was already processed within the last 8s, skip.
    // This prevents double-execution when the extension fires stream-end twice
    // (once for injected tool_result feedback, once for the real AI response).
    if (isDuplicateStream(reconstructedText)) {
      log(`  ⛔ duplicate stream-end (same text, <8s) — skipping execution`, C.yellow);
      return sendJSON(res, 200, { ok: true, actions: [], results: [], partialBlocks: 0, fullText: reconstructedText, dedup: true });
    }

    const { actions, partialBlocks } = parseActionsWithPartial(reconstructedText);
    if (partialBlocks > 0) { stats.partialBlocks += partialBlocks; }
    if (actions.length > 0) {
      log(`${C.green}  Found ${actions.length} actions${C.reset}`);
      actions.forEach((a, i) => log(`    ${i + 1}. ${a.type}: ${(a.params?.path || a.params?.command || '').slice(0, 50)}`));
    }

    broadcastSSE('stream_complete', { streamId: streamId || 'unknown', actionCount: actions.length, partialBlocks });

    const results = [];
    const shouldAuto = AUTO_APPROVE || clientAuto || global._sessionAutoApprove;

    for (const action of actions) {
      stats.dispatched++;

      if (action.type === 'execute_command') {
        const cmd = action.params?.command || '';
        if (isDuplicateCommand(cmd)) {
          stats.skippedDuplicates++;
          results.push({ action, result: { ok: true, output: '[dedup]', exitCode: 0, skipped: true } }); continue;
        }
      }

      const isFileWrite = action.type === 'write_file' ||
        (action.type === 'execute_command' && isFileWriteCommand(action.params?.command || ''));

      if (!shouldAuto && !isFileWrite) {
        const allowed = await promptConfirm(action);
        if (!allowed) { stats.failed++; results.push({ action, error: 'User cancelled' }); continue; }
      }

      try {
        const result = await dispatch(action);
        stats.succeeded++;
        results.push({ action, result });

        if (action.type === 'execute_command') {
          const cmd = action.params?.command || '';
          markCommandFinished(cmd);
          if (result.background) {
            // Background process: broadcast with startupOutput and background metadata
            // so the extension can inject a proper tool_result back to the AI.
            broadcastSSE('command_result', {
              command: cmd,
              output: result.startupOutput || '',
              exitCode: 0,
              background: true,
              pid: result.pid,
              label: result.label,
              terminal: result.terminal || false,
              message: result.message || '',
            });
          } else {
            broadcastSSE('command_result', { command: cmd, output: result.output || '', exitCode: result.exitCode ?? 0 });
          }
        } else if (action.type === 'write_file') {
          broadcastSSE('action_result', { type: action.type, params: action.params, result });
        }
      } catch (err) {
        stats.failed++;
        results.push({ action, error: err.message });
        log(`  ✗ ${action.type}: ${err.message}`, C.red);
        // Release the lock on failure so the AI can retry the command
        if (action.type === 'execute_command') {
          markCommandFinished(action.params?.command || '');
          broadcastSSE('command_result', { command: action.params?.command, output: err.message, exitCode: 1 });
        }
      }
    }

    sendJSON(res, 200, { ok: true, actions, results, partialBlocks, fullText: reconstructedText });
  });
}

// ── /stream (SSE) ─────────────────────────────────────────────────────────────
function handleSSE(req, res) {
  // Disable socket idle timeout for SSE connections — they must stay open forever.
  req.socket.setTimeout(0);
  req.socket.setKeepAlive(true, 1000);

  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache, no-transform',
    'Connection': 'keep-alive',
    'Access-Control-Allow-Origin': '*',
    'X-Accel-Buffering': 'no',  // disable nginx buffering if behind a proxy
  });

  // Tell the browser to reconnect after 2s if the connection drops
  res.write('retry: 2000\n\n');
  res.write(`event: connected\ndata: ${JSON.stringify({ workspace: WORKSPACE, version: VERSION })}\n\n`);
  sseClients.add(res);
  log(`SSE client connected (${sseClients.size} total)`, C.cyan);

  // Heartbeat every 5s — keep connection alive through proxies and idle timeouts
  const heartbeat = setInterval(() => {
    if (sseClients.has(res)) {
      try { res.write(': heartbeat\n\n'); } catch { sseClients.delete(res); clearInterval(heartbeat); }
    } else clearInterval(heartbeat);
  }, 5000);

  req.on('close', () => {
    sseClients.delete(res);
    clearInterval(heartbeat);
    log(`SSE client disconnected (${sseClients.size} remaining)`, C.dim);
  });
}

// ── /execute ──────────────────────────────────────────────────────────────────
async function handleExecute(req, res) {
  let body = '';
  req.on('data', chunk => { body += chunk; });
  req.on('end', async () => {
    let payload;
    try { payload = JSON.parse(body); } catch { return sendJSON(res, 400, { error: 'Invalid JSON' }); }
    const { action, source, autoApprove: clientAuto } = payload;
    if (!action?.type) return sendJSON(res, 400, { error: 'Missing action.type' });

    stats.dispatched++;
    log(`← ${source || '?'} | ${C.cyan}${action.type}${C.reset}`);
    const shouldAuto = AUTO_APPROVE || clientAuto || global._sessionAutoApprove;

    if (action.type === 'execute_command') {
      const cmd = action.params?.command || '';
      if (isDuplicateCommand(cmd)) {
        stats.skippedDuplicates++;
        return sendJSON(res, 200, { ok: true, output: '[dedup]', exitCode: 0, skipped: true });
      }
    }

    const isFileWrite = action.type === 'write_file' ||
      (action.type === 'execute_command' && isFileWriteCommand(action.params?.command || ''));
    if (!shouldAuto && !isFileWrite) {
      const allowed = await promptConfirm(action);
      if (!allowed) {
        if (action.type === 'execute_command') markCommandFinished(action.params?.command || '');
        stats.failed++; return sendJSON(res, 200, { error: 'User cancelled' });
      }
    }

    try {
      const result = await dispatch(action);
      stats.succeeded++;
      if (action.type === 'execute_command') {
        markCommandFinished(action.params?.command || '');
      }
      broadcastSSE('action_result', { type: action.type, params: action.params, result });
      sendJSON(res, 200, result);
    } catch (err) {
      stats.failed++;
      if (action.type === 'execute_command') markCommandFinished(action.params?.command || '');
      sendJSON(res, 200, { error: err.message });
    }
  });
}

// ── Action Dispatcher ─────────────────────────────────────────────────────────
async function dispatch(action) {
  switch (action.type) {
    case 'write_file': return writeFileLive(action.params);
    case 'read_file': return readFile(action.params);
    case 'execute_command': return executeCommand(action.params);
    case 'apply_diff': return applyDiff(action.params);
    case 'list_files': return listFiles(action.params);
    case 'search_files': return searchFiles(action.params);
    case 'attempt_completion': return showCompletion(action.params);
    default: throw new Error(`Unsupported action: ${action.type}`);
  }
}

// ── Path Resolution ────────────────────────────────────────────────────────────
function resolvePath(filePath) {
  if (!filePath) throw new Error('Missing path');
  const topLevel = filePath.replace(/^\//, '').split('/')[0].split('\\')[0];
  if (SERVER_IGNORES.has(topLevel)) throw new Error(`Path is protected: ${filePath}`);
  const abs = path.resolve(WORKSPACE, filePath);
  if (!abs.startsWith(WORKSPACE)) throw new Error(`Path escapes workspace: ${filePath}`);
  return abs;
}

// ── Python Syntax Validation ───────────────────────────────────────────────────
function validatePythonSyntax(code) {
  try {
    const tmpFile = path.join('/tmp', `vb_syntax_${Date.now()}.py`);
    fs.writeFileSync(tmpFile, code, 'utf8');
    const result = spawnSync('python3', ['-m', 'py_compile', tmpFile], { encoding: 'utf8' });
    try { fs.unlinkSync(tmpFile); } catch { }
    if (result.status === 0) return { ok: true };
    return { ok: false, error: (result.stderr || result.stdout || '').trim() || 'Syntax error' };
  } catch (err) {
    return { ok: true }; // python3 not available — skip
  }
}

// ── Write File ────────────────────────────────────────────────────────────────
async function writeFileLive({ path: filePath, content }) {
  const abs = resolvePath(filePath);
  const rel = path.relative(WORKSPACE, abs);
  fs.mkdirSync(path.dirname(abs), { recursive: true });

  broadcastSSE('file_open', { path: rel, fullPath: abs });
  log(`  → ${rel}`, C.magenta);

  const text = sanitizeContent(content ?? '');

  if (filePath.endsWith('.py')) {
    const syntaxResult = validatePythonSyntax(text);
    if (!syntaxResult.ok) {
      const errMsg = syntaxResult.error;
      log(`  ✗ Python syntax error in ${rel}: ${errMsg}`, C.red);
      broadcastSSE('file_syntax_error', { path: rel, error: errMsg });
      return { ok: false, path: filePath, syntaxError: errMsg, hint: 'File NOT written — fix syntax error and retry' };
    }
  }

  fs.writeFileSync(abs, text);
  const bytes = Buffer.byteLength(text);
  broadcastSSE('file_done', { path: rel, bytes });
  log(`  ✓ ${rel} (${bytes} bytes)`, C.green);
  return { ok: true, path: filePath, bytes };
}

// ── File-Write Command Detection ──────────────────────────────────────────────
const FILE_WRITE_CMD_RE = /^\s*cat\s+[>|]+/i;
function isFileWriteCommand(cmd) { return FILE_WRITE_CMD_RE.test(cmd.trim().split('\n')[0]); }

// ── Read File ─────────────────────────────────────────────────────────────────
function readFile({ path: filePath }) {
  const abs = resolvePath(filePath);
  if (filePath === '.' || !filePath.includes('.') || fs.statSync(abs).isDirectory()) {
    const entries = fs.readdirSync(abs, { withFileTypes: true });
    return { ok: true, path: filePath, files: entries.map(e => ({ name: e.name, type: e.isDirectory() ? 'directory' : 'file' })) };
  }
  if (!fs.existsSync(abs)) throw new Error(`File not found: ${filePath}`);
  return { ok: true, content: fs.readFileSync(abs, 'utf8') };
}

// ── Heredoc / Multi-line Command Normaliser ───────────────────────────────────
const HEREDOC_RE = /<<\s*['"']?\w+['"']?/;

function normaliseCommand(raw) {
  // Do NOT replace literal \n escape sequences here.
  // Commands coming from the AI already contain real newlines (the parser
  // extracts them verbatim from fenced blocks).  Replacing \\n → \n would
  // corrupt heredoc content that was intentionally written as a two-character
  // backslash-n (e.g. inside a Python print("…\n…") string).
  // The only thing we need to decide is *how* to invoke bash for this command.
  const expanded = raw;
  return { expanded, needsBashStdin: expanded.includes('\n') || HEREDOC_RE.test(expanded) };
}

// ── Execute Command ───────────────────────────────────────────────────────────
// Swift and direct: no timeout, spawnSync waits for natural exit.
// Multi-line / heredoc commands go via bash stdin for correct parsing.
function executeCommand({ command }) {
  if (!command) throw new Error('Missing command');
  log(`  $ ${command.slice(0, 120)}`, C.dim);

  const isWin = process.platform === 'win32';
  const { expanded, needsBashStdin } = normaliseCommand(command);

  if (!isWin && isLongRunningCommand(expanded)) {
    log(`  [long-running → background]`, C.dim);
    return spawnBackground(expanded);
  }

  let result;
  if (!isWin && needsBashStdin) {
    result = spawnSync('bash', ['-s'], { cwd: WORKSPACE, encoding: 'utf8', input: expanded, maxBuffer: 100 * 1024 * 1024 });
  } else if (isWin) {
    result = spawnSync('cmd', ['/c', command], { cwd: WORKSPACE, encoding: 'utf8', maxBuffer: 100 * 1024 * 1024 });
  } else {
    result = spawnSync('bash', ['-c', expanded], { cwd: WORKSPACE, encoding: 'utf8', maxBuffer: 100 * 1024 * 1024 });
  }

  const output = ((result.stdout || '') + (result.stderr || '')).trim();
  const exitCode = result.status ?? (result.error ? 1 : 0);

  if (result.error) log(`  ✗ spawn error: ${result.error.message}`, C.red);
  if (output) output.split('\n').forEach(l => log(`  │ ${l}`, C.dim));
  // Do NOT broadcastSSE here — handleStreamEnd, handleOpenAI, and handleExecute
  // each call broadcastSSE after dispatch() returns, so broadcasting here too
  // would fire the event a second (or third) time for every command.
  return { ok: exitCode === 0, output, exitCode };
}

// ── Apply Diff ────────────────────────────────────────────────────────────────
function applyDiff({ path: filePath, diff }) {
  const abs = resolvePath(filePath);
  const original = fs.existsSync(abs) ? fs.readFileSync(abs, 'utf8') : '';
  return writeFileLive({ path: filePath, content: patchDiff(diff, original) });
}

function listFiles({ path: dirPath = '.' }) {
  const abs = resolvePath(dirPath);
  if (!fs.existsSync(abs)) throw new Error(`Directory not found: ${dirPath}`);
  const entries = fs.readdirSync(abs, { withFileTypes: true });
  return { ok: true, path: dirPath, files: entries.map(e => ({ name: e.name, type: e.isDirectory() ? 'directory' : 'file' })) };
}

function searchFiles({ pattern, path: dirPath = '.', file_pattern: fp }) {
  if (!pattern) throw new Error('Missing pattern');
  const abs = resolvePath(dirPath);
  let output = '';
  try {
    output = execSync(`grep -rn --include="${fp || '*'}" "${pattern.replace(/"/g, '\\"')}" .`, {
      cwd: abs, encoding: 'utf8', timeout: 10_000,
    });
  } catch { }
  return { ok: true, pattern, matches: output.trim().split('\n').filter(Boolean) };
}

function showCompletion({ result }) {
  const msg = (result || '').slice(0, 300);
  log(`\n${C.bold}${C.green}✅ ${msg}${C.reset}\n`);
  broadcastSSE('completion', { result: msg });
  return { ok: true };
}

async function handleKillProcess(req, res) {
  let body = '';
  req.on('data', chunk => { body += chunk; });
  req.on('end', () => {
    let payload;
    try { payload = JSON.parse(body); } catch { return sendJSON(res, 400, { error: 'Invalid JSON' }); }
    const { label, pid } = payload;
    if (label) return sendJSON(res, 200, killBgProcess(label));
    if (pid) {
      for (const [lbl, entry] of bgProcesses) {
        if (entry.pid === pid) return sendJSON(res, 200, killBgProcess(lbl));
      }
      return sendJSON(res, 404, { ok: false, error: `No bg process with pid: ${pid}` });
    }
    return sendJSON(res, 400, { error: 'Provide label or pid' });
  });
}

// Kill the most recently started background process (used by Ctrl+K and /kill-last)
function killLastProcess() {
  if (bgProcesses.size === 0) return { ok: false, error: 'No background processes running' };
  // bgProcesses is a Map — last inserted key = most recently started
  const entries = [...bgProcesses.entries()];
  const [label] = entries[entries.length - 1];
  return killBgProcess(label);
}

function handleKillLast(res) {
  sendJSON(res, 200, killLastProcess());
}

function handleListProcesses(res) {
  const list = [];
  for (const [label, entry] of bgProcesses) {
    list.push({ label, pid: entry.pid, command: entry.command, uptimeMs: Date.now() - entry.startTime });
  }
  sendJSON(res, 200, { ok: true, processes: list });
}

function handleListWorkspace(res) {
  sendJSON(res, 200, { ok: true, workspace: WORKSPACE, files: walkDir(WORKSPACE, WORKSPACE) });
}

async function handlePaste(req, res) {
  let body = '';
  req.on('data', chunk => { body += chunk; });
  req.on('end', async () => {
    let payload;
    try { payload = JSON.parse(body); } catch { return sendJSON(res, 400, { error: 'Invalid JSON' }); }
    const { filePath, content, autoApprove: clientAuto } = payload;
    if (!filePath) return sendJSON(res, 400, { error: 'Missing filePath' });
    if (content === undefined || content === null) return sendJSON(res, 400, { error: 'Missing content' });

    stats.dispatched++;
    const shouldAuto = AUTO_APPROVE || clientAuto || global._sessionAutoApprove;
    if (!shouldAuto) {
      const allowed = await promptConfirm({ type: 'paste_file', params: { path: filePath } });
      if (!allowed) { stats.failed++; return sendJSON(res, 200, { error: 'User cancelled' }); }
    }
    try {
      const result = await writeFileLive({ path: filePath, content });
      stats.succeeded++;
      sendJSON(res, 200, result);
    } catch (err) {
      stats.failed++;
      sendJSON(res, 200, { error: err.message });
    }
  });
}

// ── Utilities ─────────────────────────────────────────────────────────────────
function sanitizeContent(content) {
  if (!content) return content;
  return content.replace(/\r\n/g, '\n');
}

function patchDiff(diff, original) {
  const lines = diff.split('\n'), result = [], origLines = original.split('\n');
  let idx = 0;
  for (const line of lines) {
    if (line.startsWith('---') || line.startsWith('+++') || line.startsWith('@@')) continue;
    if (line.startsWith('+')) result.push(line.slice(1));
    else if (line.startsWith('-')) idx++;
    else result.push(origLines[idx++] ?? line.slice(1));
  }
  return result.join('\n');
}

function walkDir(dir, root) {
  const entries = [];
  try {
    for (const e of fs.readdirSync(dir, { withFileTypes: true })) {
      if (e.name.startsWith('.') && e.name !== '.gitignore') continue;
      const abs = path.join(dir, e.name);
      const rel = path.relative(root, abs);
      if (SERVER_IGNORES.has(e.name)) continue;
      if (e.isDirectory()) entries.push({ name: e.name, type: 'directory', path: rel, children: walkDir(abs, root) });
      else entries.push({ name: e.name, type: 'file', path: rel, size: fs.statSync(abs).size });
    }
  } catch { }
  return entries;
}

// ── Action Parser v17 ─────────────────────────────────────────────────────────
//
// Detection logic (merged best of v14 + v15):
// 1. Strip <think>/<thinking>/[THINK] blocks (incl. unclosed)
// 2. Conversational guard:
//    a. If NO code fences AND strong conversational opener → skip (v15 method)
//    b. If prose ratio > 80% + conversational signals + < 2 fences → skip (v14 method)
// 3. Commands from: bash/sh/shell/zsh fences, unlabelled fences starting with shell cmds,
//    bare heredoc blocks outside fences
// 4. // COMMAND: directive is NOT parsed (removed in v15, kept removed here)
// 5. Partial block fallback for cut-off streams

const CMD_LANGS = new Set(['bash', 'sh', 'shell', 'zsh', 'ps1', 'powershell', 'cmd', 'bat']);

const REAL_COMMAND_RE = /^(cat\s|sed\s|echo\s|printf\s|mkdir|rm\s|cp\s|mv\s|cd\s|ls|touch\s|chmod\s|pip\s|pip3\s|python\s|python3\s|node\s|npm\s|yarn\s|git\s|curl\s|wget\s|apt|sudo\s|export\s|source\s|bash\s|sh\s|find\s|grep\s|awk\s|tar\s|zip\s|unzip\s|\.\/|\$\s+\S)/im;

function isRealCommandBlock(blockContent) {
  const lines = blockContent.split('\n').map(l => l.trim()).filter(Boolean);
  if (!lines.length) return false;
  const firstLine = lines[0];
  // Reject if starts with a plain sentence (prose disguised as code)
  const looksLikeProse = /^[A-Z][a-z]/.test(firstLine) && !/[|>&$#]/.test(firstLine);
  const hasRealCommand = REAL_COMMAND_RE.test(blockContent);
  if (looksLikeProse && !hasRealCommand) return false;
  return hasRealCommand;
}

// Strong conversational starters (v15 approach — tight)
const STRONG_CONVERSATIONAL_STARTERS = [
  /^(sure[,!.]?|okay[,!.]?|ok[,!.]?|got it[,!.]?|of course[,!.]?|absolutely[,!.]?|certainly[,!.]?|happy to help[,!.]?|hello[,!.]?|hi there[,!.]?|no problem[,!.]?|understood[,!.]?)\s*$/i,
  /^i (can|will|am going to|would be happy to) help/i,
  /^let me (help|assist|know if)/i,
  /^(great question|good question|that's a great|here's how|here is how|to answer your question)/i,
  /^(sorry|i apologize|unfortunately|i (can't|cannot) (do|help|assist) with that)/i,
];

// Broader conversational patterns (v14 approach — for prose-ratio check)
const CONVERSATIONAL_PATTERNS = [
  /^(sure|okay|ok|got it|of course|absolutely|certainly|happy to|great|hello|hi there|no problem|understood|i understand|i see|i'll|i will|i can|let me|let's|here('s| is)|i've|i have|i'm|i am|to do this|in order to|first,|firstly|next,|then,|finally,|step \d)/i,
  /^(sorry|apolog|unfortunately|i (can't|cannot|don't|do not)|that's not|this (isn't|is not)|please note|note that|be aware|keep in mind)/i,
  /\?$/, // ends with question mark
];

function isConversationalResponse(stripped, fenceCount) {
  // If text contains // COMMAND: directives — never conversational
  if (/\/\/\s*COMMAND:/i.test(stripped)) return false;

  // Gate 1 (v15): no fences + strong opener
  if (fenceCount === 0) {
    if (/<<\s*['"']?\w+['"']?/.test(stripped)) return false; // heredoc = not conversational
    const lines = stripped.split('\n').map(l => l.trim()).filter(Boolean).slice(0, 5);
    const hasStrongOpener = lines.some(line => STRONG_CONVERSATIONAL_STARTERS.some(p => p.test(line)));
    if (hasStrongOpener && !REAL_COMMAND_RE.test(stripped)) return true;
  }

  // Gate 2 (v14): prose-ratio check
  const prose = stripped.replace(/```[\s\S]*?```/g, '').trim();
  const proseLines = prose.split('\n').map(l => l.trim()).filter(Boolean);
  const totalLines = stripped.split('\n').length;
  const proseRatio = totalLines > 0 ? proseLines.length / totalLines : 0;

  let conversationalHits = 0;
  for (const line of proseLines.slice(0, 10)) {
    if (CONVERSATIONAL_PATTERNS.some(p => p.test(line))) conversationalHits++;
  }

  if (conversationalHits > 0 && proseRatio > 0.8 && fenceCount < 2) {
    log(`  ⛔ prose-dominant response (${Math.round(proseRatio * 100)}% prose, ${conversationalHits} signals) — skip`, C.yellow);
    return true;
  }

  return false;
}

function dedupeActions(actions) {
  const seen = new Set();
  return actions.filter(a => {
    const key = a.type + ':' + (a.params?.path || a.params?.command || '');
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function parseActionsWithPartial(text) {
  const actions = [];
  let partialBlocks = 0;

  // Strip think/reasoning blocks (incl. unclosed at end of stream)
  // Also strip <tool_result> blocks — these are command results that were
  // injected BACK into the chat as feedback. If the stream captures both the
  // injected result AND the AI's follow-up prose, the tool_result block must
  // be removed before parsing or the old command gets re-extracted and the
  // AI's prose text gets appended to it (producing "cmd &'s Cube is running…").
  let stripped = text
    .replace(/<think>[\s\S]*?<\/think>/gi, '')
    .replace(/<thinking>[\s\S]*?<\/thinking>/gi, '')
    .replace(/<think>[\s\S]*/gi, '')
    .replace(/<thinking>[\s\S]*/gi, '')
    .replace(/\[THINK\][\s\S]*?\[\/THINK\]/gi, '')
    .replace(/\[THINK\][\s\S]*/gi, '')
    // Remove complete <tool_result>…</tool_result> blocks (injected feedback)
    .replace(/<tool_result>[\s\S]*?<\/tool_result>/gi, '')
    // Remove any unclosed <tool_result> block that reaches end of text
    .replace(/<tool_result>[\s\S]*/gi, '')
    // Remove agent header lines like "# [AGENT: tester]"
    .replace(/^#\s*\[AGENT:[^\]]*\]\s*$/gim, '')
    .trim();

  const fenceCount = Math.floor(((stripped.match(/```/g) || []).length) / 2);

  if (isConversationalResponse(stripped, fenceCount)) {
    log(`  ⛔ conversational reply — skip execution`, C.yellow);
    return { actions: [], partialBlocks: 0, blocked: true, reason: 'conversational' };
  }

  const cleanText = stripped;

  // ── Pattern 1: Complete fenced code blocks ────────────────────────────────
  const completeFenceRe = /```(\w*)\s*\n([\s\S]*?)```/g;

  for (const match of cleanText.matchAll(completeFenceRe)) {
    const lang = (match[1] || '').toLowerCase().trim();
    const blockContent = match[2];

    if (CMD_LANGS.has(lang)) {
      // Strip prose that leaked into the block from the injected tool_result feedback.
      // Contamination happens in two ways:
      //   A) Same-line: "pip install -r requirements.txt environment. Let me create one."
      //      — AI prose appended directly after the command on the same line.
      //   B) Multi-line: command on line 0, prose on subsequent lines.
      const rawCmd = blockContent.trim();
      if (!rawCmd) continue;

      // ── A) Same-line prose truncation ─────────────────────────────────────
      // Detect the boundary where a valid shell token sequence ends and a
      // natural-language sentence begins on the same line.
      // Two contamination patterns:
      //   1. With space:    "pip install -r requirements.txt environment. Let me..."
      //   2. No-space join: "python3 -m venv venvVirtual environment created..."
      function truncateInlineProse(line) {
        let best = line;

        // Pattern 1: space + known prose word
        const proseBreak = line.search(/\s+(environment|error|warning|note|this|now|let|please|make|already|done|created|installed|running|starting|complete|finished|failed|success|sorry|i\s|virtual\s)/i);
        if (proseBreak > 0) {
          const cmdPart = line.slice(0, proseBreak).trim();
          if (REAL_COMMAND_RE.test(cmdPart) && cmdPart.length < best.length) best = cmdPart;
        }

        // Pattern 2: no-space camelCase boundary (e.g. "venvVirtual" → "venv")
        // Run independently and take result if shorter than pattern 1 result.
        const m = line.match(/^(.*?[a-z0-9._\-\/])([A-Z][a-z].*)$/);
        if (m) {
          const cmdPart = m[1].trim();
          if (cmdPart.length > 3 && REAL_COMMAND_RE.test(cmdPart) && cmdPart.length < best.length) best = cmdPart;
        }

        return best;
      }

      // ── B) Multi-line: keep only shell-like lines ─────────────────────────
      const rawLines = rawCmd.split('\n');
      const cleanedLines = [];
      for (let li = 0; li < rawLines.length; li++) {
        const l = rawLines[li].trim();
        if (!l) { cleanedLines.push(''); continue; }
        const isShellLike = /^(#|\$|export|source|cd|ls|mkdir|rm|cp|mv|cat|echo|printf|pip|pip3|python|python3|node|npm|yarn|git|curl|wget|apt|sudo|bash|sh|find|grep|awk|tar|zip|unzip|touch|chmod|chown|\.|\.\/|venv|\w+=|if |for |while |do$|done$|fi$|else|elif|then|{|}|\[|\]|&&|\|\||>>?|<|;|-)/i.test(l);
        if (isShellLike) {
          cleanedLines.push(truncateInlineProse(rawLines[li]));
        } else if (li === 0) {
          // First line determines if this is a real command block at all
          if (!isRealCommandBlock(rawCmd)) break;
          cleanedLines.push(truncateInlineProse(rawLines[li]));
        } else {
          break; // Stop at first non-shell line
        }
      }

      const cmd = cleanedLines.join('\n').trim() || rawCmd;

      if (cmd && isRealCommandBlock(cmd)) {
        actions.push({ type: 'execute_command', params: { command: cmd } });
      } else if (rawCmd) {
        log(`  ⛔ bash block rejected (prose): ${rawCmd.slice(0, 60).replace(/\n/g, ' ')}`, C.yellow);
      }
      continue;
    }

    // Unlabelled blocks starting with shell commands
    if (lang === '' || lang === 'txt' || lang === 'text') {
      const firstLine = blockContent.split('\n')[0].trim();
      const SHELL_START_RE = /^(cat\s|sed\s|echo\s|printf\s|mkdir\s|rm\s|cp\s|mv\s|cd\s|ls\s|touch\s|chmod\s|chown\s|pip\s|pip3\s|python\s|python3\s|node\s|npm\s|yarn\s|git\s|curl\s|wget\s|apt\s|apt-get\s|sudo\s|export\s|source\s|bash\s|sh\s|find\s|grep\s|awk\s|tar\s|zip\s|unzip\s|\.\/)/i;
      if (SHELL_START_RE.test(firstLine)) {
        const cmd = blockContent.trim();
        if (cmd) actions.push({ type: 'execute_command', params: { command: cmd } });
        continue;
      }
    }

    // // COMMAND: directive inside any fence — for all commands EXCEPT cat (cat uses heredoc)
    {
      const firstLine = blockContent.split('\n')[0].trim();
      const cmdDirective = firstLine.match(/^\/\/\s*COMMAND:\s*(.+)$/i);
      if (cmdDirective) {
        const directiveCmd = cmdDirective[1].trim();
        // cat commands must use heredoc — skip // COMMAND: cat
        if (!/^\s*cat\s/i.test(directiveCmd)) {
          const bodyLines = blockContent.split('\n').slice(1);
          const body = bodyLines.join('\n').trim();
          const cmd = body ? directiveCmd + '\n' + body : directiveCmd;
          if (cmd) actions.push({ type: 'execute_command', params: { command: cmd } });
        }
      }
    }
  }

  const noCodeBlocks = cleanText.replace(/```[\s\S]*?```/g, '');

  // ── Pattern 2: Standalone // COMMAND: lines outside fences ──────────────
  // Matches: // COMMAND: <anything except cat>
  const cmdLineRe = /^[ \t]*\/\/\s*COMMAND:\s*(.+)$/gim;
  for (const match of noCodeBlocks.matchAll(cmdLineRe)) {
    const cmd = match[1].trim();
    // cat commands must use heredoc syntax — skip // COMMAND: cat
    if (cmd && !/^\s*cat\s/i.test(cmd)) {
      actions.push({ type: 'execute_command', params: { command: cmd } });
    }
  }

  // ── Pattern 3: Bare heredoc commands outside fences ──────────────────────
  const HEREDOC_OPEN_RE = /^(cat\s+[>|]+\s*\S+\s*<<\s*['"']?(\w+)['"']?)\s*$/gm;
  for (const match of noCodeBlocks.matchAll(HEREDOC_OPEN_RE)) {
    const delimiter = match[2];
    const startIdx = match.index;
    const afterOpen = noCodeBlocks.slice(startIdx);
    const closeRe = new RegExp(`^${delimiter}\\s*$`, 'm');
    const closeMatch = closeRe.exec(afterOpen);
    if (closeMatch) {
      const fullCmd = afterOpen.slice(0, closeMatch.index + closeMatch[0].length).trim();
      if (fullCmd) actions.push({ type: 'execute_command', params: { command: fullCmd } });
    }
  }

  // ── Pattern 4: Partial blocks (streaming cut-off fallback) ────────────────
  if (!actions.length) {
    const partialRe = /```(\w*)\s*\n([\s\S]+)$/gm;
    for (const match of cleanText.matchAll(partialRe)) {
      const lang = (match[1] || '').toLowerCase().trim();
      const blockContent = match[2];
      if (CMD_LANGS.has(lang)) {
        const cmd = blockContent.trim();
        if (cmd && cmd.length > 2 && isRealCommandBlock(cmd)) {
          actions.push({ type: 'execute_command', params: { command: cmd } });
          partialBlocks++;
        }
      }
    }
  }

  return { actions: dedupeActions(actions), partialBlocks };
}

function promptConfirm(action) {
  return new Promise(resolve => {
    const label = `${action.type}: ${JSON.stringify(action.params || {}).slice(0, 80)}`;
    process.stdout.write(`\n${C.yellow}⚡ ${label}${C.reset}\nAllow? [y]es / [n]o / [a]lways: `);
    const rl = readline.createInterface({ input: process.stdin, terminal: false });
    rl.once('line', line => {
      rl.close();
      const ans = line.trim().toLowerCase();
      if (ans === 'a' || ans === 'always') { global._sessionAutoApprove = true; resolve(true); }
      else resolve(ans === 'y' || ans === 'yes');
    });
    rl.once('close', () => resolve(false));
  });
}

function sendJSON(res, status, data) {
  const body = JSON.stringify(data);
  res.writeHead(status, { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) });
  res.end(body);
}

// ── Boot ─────────────────────────────────────────────────────────────────────
ensureGitignore();

server.listen(PORT, '127.0.0.1', () => {
  // Disable server-level timeouts — SSE clients must stay connected indefinitely.
  server.timeout = 0;
  server.keepAliveTimeout = 0;
  server.headersTimeout = 0;
  console.log(
    `\n${C.bold}${C.cyan}⚡ Virola v17.0${C.reset}\n` +
    `${C.green}✓ Server running${C.reset}  →  http://localhost:${PORT}\n` +
    `${C.dim}  Root dir      : ${WORKSPACE}${C.reset}\n` +
    `${C.dim}  Auto-approve  : ${AUTO_APPROVE ? 'yes (--auto-approve)' : 'no'}${C.reset}\n` +
    `${C.dim}  Bash fences   : bash/sh/shell/zsh execute automatically${C.reset}\n` +
    `${C.dim}  // COMMAND:   : supported for all commands except cat${C.reset}\n` +
    `${C.dim}  cat files     : use heredoc syntax only${C.reset}\n` +
    `${C.dim}  OpenAI compat : POST /v1/chat/completions${C.reset}\n` +
    `${C.dim}                  GET  /v1/models${C.reset}\n` +
    `${C.dim}  Execution     : spawnSync (no timeout), heredoc → bash stdin${C.reset}\n` +
    `${C.dim}  Python files  : syntax-validated before write${C.reset}\n` +
    `${C.dim}  Dedup         : blocks only after SUCCESS${C.reset}\n` +
    `${C.dim}  Long-running  : background spawn${C.reset}\n` +
    `${C.dim}  Kill server   : Click "⏹ Kill Latest Server" in the extension popup${C.reset}\\n` +
    `${C.dim}               : or POST /kill-last  |  GET /processes to list${C.reset}\\n` +
    `${C.dim}  New tab       : long-running servers open in a new terminal window${C.reset}\\n\\n` +
    `Press Ctrl+C to stop Virola.\\n`
  );
});

server.on('error', err => {
  if (err.code === 'EADDRINUSE') console.error(`${C.red}✗ Port ${PORT} in use${C.reset}`);
  else console.error(`${C.red}✗ ${err.message}${C.reset}`);
  process.exit(1);
});

process.on('SIGINT', () => { console.log(`\nVirola stopped.`); process.exit(0); });
process.on('SIGTERM', () => process.exit(0));

// ── SAFETY NETS: keep the Node server alive no matter what ───────────────────
// Any unhandled error from a bg process child (missing 'error' listener, rejected
// promise in an event handler, etc.) would otherwise crash the server.
process.on('uncaughtException', (err) => {
  console.error(`[Virola] uncaughtException (server kept alive): ${err?.stack || err}`);
});
process.on('unhandledRejection', (reason) => {
  console.error(`[Virola] unhandledRejection (server kept alive): ${reason?.stack || reason}`);
});
// To kill a background server: click "⏹ Kill Latest Server" in the Virola Chrome extension popup,
// or POST http://localhost:PORT/kill-last from any HTTP client.
