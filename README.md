# Virola v17.0

Real-time AI coding bridge — detects bash commands in AI responses and executes them instantly.

## What's new in v17

- **Node.js server detection** — `node server.js`, `npm start`, `npm run dev`, `npx vite`, `npx next dev`, `nodemon`, `bun run`, `deno serve`, and more now automatically spawn in the background instead of blocking the main server
- **Live output streaming** — after a background process starts, all subsequent stdout/stderr is forwarded as `bg_process_output` SSE events back to the chat in real time, so you see errors and health messages as they happen

## What's new in v16

- **Chat architecture removed** — no more `/inject`, `/chat-events`, `/extension-status`, `chat.js`
- **OpenAI-compatible endpoint** — point any third-party chat UI at `http://localhost:3172/v1/chat/completions`
- **Bash auto-detection** — commands in ```bash``` / ```sh``` fences execute automatically, no `// COMMAND:` prefix needed
- **Fast execution** — direct `spawnSync` with no timeouts; long-running servers spawn in background
- **Python syntax check** — `.py` files validated before write; corrupt files are rejected
- **Dedup guardrail** — pip/python/general commands only skipped after a prior SUCCESS

## Quick start

```bash
cd files
node server.js --auto-approve
```

Flags:
- `--port 3172`     Port to listen on (default 3172)
- `--dir /path`     Workspace root (default: parent of server.js)
- `--auto-approve`  Execute all commands without prompting

## Background process detection

The following command patterns automatically run in a background tab and stream their output back live:

| Runtime | Detected patterns |
|---------|-------------------|
| Python  | `python app.py`, `python -m uvicorn`, `python -m flask`, `python -m gunicorn`, `python -m http.server`, Django `manage.py` |
| Node.js | `node server.js`, `node .`, `node src/index.js` (any `.js` target) |
| npm / yarn / pnpm | `npm start`, `npm run start`, `npm run dev`, `npm run serve`, `yarn dev`, etc. |
| npx     | `npx vite`, `npx next dev`, `npx ts-node`, `npx nodemon`, `npx tsx`, `npx serve`, `npx http-server`, `npx json-server`, `npx fastify`, `npx nest` |
| Bun     | `bun run server.ts`, `bun run index.js` |
| Deno    | `deno run`, `deno serve` |

## SSE events

| Event | Payload | Description |
|-------|---------|-------------|
| `bg_process_started` | `{ label, pid, command, startupOutput }` | Fires after 3s startup window; includes initial stdout/stderr |
| `bg_process_output`  | `{ label, pid, output }` | **New in v17** — fires for every stdout/stderr chunk after startup |
| `bg_process_exited`  | `{ label, pid, exitCode }` | Process died or was killed |
| `bg_process_killed`  | `{ label, pid }` | Killed via `/kill-process` |

## OpenAI-compatible endpoint

Set any third-party chat UI base URL to: `http://localhost:3172`

- `GET  /v1/models`
- `POST /v1/chat/completions`  (stream: true/false supported)

API Key: anything (ignored)
Model: `virola-executor`

## Other endpoints

- `GET  /health`           Server status + stats
- `GET  /stream`           SSE event stream (Chrome extension)
- `POST /stream-chunk`     Forward streaming chunk
- `POST /stream-end`       End of stream, parse + execute actions
- `POST /execute`          Execute a single action
- `GET  /files`            List workspace files
- `POST /paste`            Write file by content
- `POST /kill-process`     Kill background process by label or PID
- `GET  /processes`        List running background processes

## Chrome extension

Load `chrome-extension/` as an unpacked extension to use with browser-based AI chat UIs.
Supported: DeepSeek, ChatGPT, Claude, Gemini, Qwen, Groq, Perplexity, Moonshot.
