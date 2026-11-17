#!/usr/bin/env node
/**
 * Deterministic OpenAI-compatible provider used by the full-system harness.
 *
 * This is an EXTERNAL test double for the provider boundary only: the real
 * RunEngine, planner, tool loop, approvals, persistence and UI are exercised.
 * No product code branches on this server.
 *
 * Planner requests are non-streaming JSON; task turns are SSE. The script is
 * deliberately small for the first smoke pass and is extended by the fixture
 * driver for tool/approval scenarios.
 */
import { createServer } from 'node:http';
import { appendFileSync, mkdirSync, writeFileSync } from 'node:fs';
import { dirname } from 'node:path';

const port = Number(process.env.MOCK_PROVIDER_PORT || 0);
const logPath = process.env.MOCK_PROVIDER_LOG || '';
const mode = process.env.MOCK_PROVIDER_MODE || 'single-leaf';

if (logPath) mkdirSync(dirname(logPath), { recursive: true });

function writeState(value) {
  if (logPath) writeFileSync(`${logPath}.state.json`, `${JSON.stringify(value, null, 2)}\n`);
}

function log(value) {
  if (logPath) appendFileSync(logPath, `${JSON.stringify(value)}\n`);
}

const PLAN = mode === 'tools'
  ? {
      nodes: [
        {
          id: 'n1',
          role: 'superagent',
          deps: [],
          depth: 1,
          parent: null,
          objective: 'Coordinate the write branch and report its result.',
        },
        {
          id: 'n2',
          role: 'agent',
          deps: [],
          depth: 2,
          parent: 'n1',
          objective: 'Write golden/out.txt with the scripted acceptance content.',
        },
      ],
    }
  : {
      nodes: [
        {
          id: 'n1',
          role: 'superagent',
          deps: [],
          depth: 1,
          parent: null,
          objective: 'Read the workspace and report what the golden project contains.',
        },
      ],
    };

function usage(prompt = 11, completion = 7) {
  return {
    prompt_tokens: prompt,
    completion_tokens: completion,
    total_tokens: prompt + completion,
    prompt_cache_hit_tokens: 0,
    prompt_cache_miss_tokens: prompt,
    prompt_tokens_details: { cached_tokens: 0 },
  };
}

function jsonPlan() {
  return JSON.stringify(PLAN);
}

function sseText(text) {
  const first = {
    id: 'chatcmpl-scripted',
    object: 'chat.completion.chunk',
    model: 'scripted-model',
    choices: [{ index: 0, delta: { content: text }, finish_reason: null }],
  };
  const last = {
    id: 'chatcmpl-scripted',
    object: 'chat.completion.chunk',
    model: 'scripted-model',
    choices: [{ index: 0, delta: {}, finish_reason: 'stop' }],
    usage: usage(),
  };
  return `data: ${JSON.stringify(first)}\n\ndata: ${JSON.stringify(last)}\n\ndata: [DONE]\n\n`;
}

function sseToolCall(name, args) {
  const first = {
    id: 'chatcmpl-scripted',
    object: 'chat.completion.chunk',
    model: 'scripted-model',
    choices: [{
      index: 0,
      delta: {
        tool_calls: [{
          index: 0,
          id: 'call-scripted-1',
          type: 'function',
          function: { name, arguments: JSON.stringify(args) },
        }],
      },
      finish_reason: null,
    }],
  };
  const last = {
    id: 'chatcmpl-scripted',
    object: 'chat.completion.chunk',
    model: 'scripted-model',
    choices: [{ index: 0, delta: {}, finish_reason: 'tool_calls' }],
    usage: usage(),
  };
  return `data: ${JSON.stringify(first)}\n\ndata: ${JSON.stringify(last)}\n\ndata: [DONE]\n\n`;
}

function hasToolResult(messages) {
  return Array.isArray(messages) && messages.some((m) => m.role === 'tool');
}

const server = createServer((request, response) => {
  if (request.method === 'GET' && request.url === '/health') {
    response.writeHead(200, { 'content-type': 'application/json' });
    response.end(JSON.stringify({ status: 'ok', model: 'scripted-model', mode }));
    return;
  }
  if (request.method !== 'POST' || !request.url?.endsWith('/chat/completions')) {
    response.writeHead(404, { 'content-type': 'application/json' });
    response.end(JSON.stringify({ error: { message: 'not found' } }));
    return;
  }

  let raw = '';
  request.on('data', (chunk) => { raw += chunk; });
  request.on('end', () => {
    let body;
    try { body = JSON.parse(raw); } catch {
      response.writeHead(400, { 'content-type': 'application/json' });
      response.end(JSON.stringify({ error: { message: 'invalid json' } }));
      return;
    }
    log({ at: new Date().toISOString(), stream: body.stream === true, url: request.url, body });
    if (mode === 'fault') {
      response.writeHead(500, { 'content-type': 'application/json' });
      response.end(JSON.stringify({ error: { message: 'scripted provider failure', code: 'E_LLM_SERVER' } }));
      return;
    }
    if (body.stream !== true) {
      const content = jsonPlan();
      response.writeHead(200, { 'content-type': 'application/json' });
      response.end(JSON.stringify({
        id: 'chatcmpl-scripted-plan',
        object: 'chat.completion',
        model: 'scripted-model',
        choices: [{ index: 0, message: { role: 'assistant', content }, finish_reason: 'stop' }],
        usage: usage(23, 19),
      }));
      return;
    }
    response.writeHead(200, {
      'content-type': 'text/event-stream',
      'cache-control': 'no-cache',
      connection: 'keep-alive',
    });
    const system = String(body.messages?.[0]?.content || '');
    const isRootControl = system.includes('HyperAgent controller');
    const isWorkerAgent = system.includes('HiveMind worker agent') && !system.includes('SuperAgent');
    if (mode === 'tools' && !hasToolResult(body.messages) && !isRootControl && isWorkerAgent) {
      response.end(sseToolCall('file_write', {
        path: 'golden/out.txt',
        content: 'written by the scripted acceptance run\n',
      }));
    } else {
      response.end(sseText('Scripted leaf result: the workspace was inspected; no mutation was requested.'));
    }
  });
});

server.listen(port, '127.0.0.1', () => {
  const address = server.address();
  const actualPort = typeof address === 'object' && address !== null ? address.port : port;
  writeState({ port: actualPort, pid: process.pid, mode });
  process.stdout.write(`${JSON.stringify({ ready: true, port: actualPort, pid: process.pid })}\n`);
});

for (const signal of ['SIGINT', 'SIGTERM']) {
  process.on(signal, () => server.close(() => process.exit(0)));
}
