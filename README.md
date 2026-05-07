# Virola

Virola is a local AI execution bridge for developer chat UIs. It detects bash/sh commands in AI responses, executes them against your local workspace, and streams live output back to the chat.

The project includes:

- `files/server.js` — the main Node.js server and OpenAI-compatible chat endpoint
- `files/package.json` — install and run scripts
- `chrome-extension/` — browser extension for forwarding AI responses and receiving execution results

## Features

- Executes shell commands automatically from AI assistant responses
- Detects long-running developer servers and streams stdout/stderr as live SSE events
- Supports OpenAI-compatible `POST /v1/chat/completions`
- Works with local chat UIs via the included Chrome extension
- Supports `npm`, `npx`, `node`, `bun`, `deno`, and Python server detection

## Requirements

- Node.js 16 or newer
- Git (optional, only for repo workflows)

## Installation

```bash
cd files
npm install
```

## Running Virola

Start the server from the `files` folder:

```bash
cd files
npm run dev
```

Or run the production start command:

```bash
cd files
npm start
```

### Optional flags

- `--port 3172` — server port (default: `3172`)
- `--dir /path/to/workspace` — workspace root directory (default: parent of `server.js`)
- `--auto-approve` — execute commands immediately without confirmation prompts

Example:

```bash
cd files
node server.js --auto-approve --port 3172 --dir ..
```

## Usage

1. Install dependencies in `files/`
2. Start Virola with `npm run dev` or `node server.js`
3. Point your AI chat UI to `http://localhost:3172`
4. Use the Chrome extension in `chrome-extension/` to forward AI responses and receive execution updates

## Endpoints

- `GET  /health` — server health and status
- `GET  /stream` — SSE event stream for browser extension
- `POST /stream-chunk` — forward a streaming chunk
- `POST /stream-end` — end stream and execute parsed actions
- `POST /execute` — execute a single action
- `GET  /files` — list workspace files
- `POST /paste` — write file content
- `POST /kill-process` — kill a background process
- `GET  /processes` — list running background processes

## OpenAI-compatible API

Base URL: `http://localhost:3172`

- `GET  /v1/models`
- `POST /v1/chat/completions`

The server accepts any API key value and uses model `virola-executor`.

## Chrome extension

Load the `chrome-extension/` folder as an unpacked extension in Chrome/Edge.
It supports browser-based AI chat UIs such as DeepSeek, ChatGPT, Claude, Gemini, Qwen, Groq, Perplexity, and Moonshot.
