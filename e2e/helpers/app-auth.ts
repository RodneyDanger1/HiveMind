import { createHash } from 'node:crypto';
import { existsSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { expect, type APIRequestContext, type Page, type Response } from '@playwright/test';

/**
 * Launch-token-aware auth for the *live* HiveMind app.
 *
 * WHY THIS EXISTS
 * ---------------
 * The launcher starts `hiverouter.exe` with `HIVEROUTER_LAUNCH_TOKEN=<secret>` and opens
 * the browser at `http://127.0.0.1:<port>/?launch=<secret>`. The server then refuses
 * every unauthenticated `/api/*` GET with HTTP 403 `E_ORIGIN_REJECTED`
 * (`backend/crates/hiverouter-core/src/http.rs`):
 *   - `/api/session`  -> "the launcher token is required to read the session"
 *   - any other read  -> "this read route requires the launcher token"
 * The web app reads `?launch=` from `window.location` (`web/src/main.tsx`,
 * `web/src/api/client.ts`) and re-sends it as `x-hivemind-launch` on same-origin reads.
 *
 * A Playwright test opens a FRESH browser context, so it never inherits the launcher's
 * window state: without this helper every authed GET (and therefore the WS handshake,
 * which needs the session token from `/api/session`) fails with 403. That is a HARNESS
 * gap, not a product defect. This module closes it and pins the negative direction too.
 *
 * TOKEN SOURCES (in order, no secret is ever logged or written to evidence)
 *   1. `E2E_LAUNCH_TOKEN` environment variable
 *   2. `launch.token` inside the data dir named by `E2E_DATA_DIR`
 *   3. `launch.token` inside the data dir named by `HIVEROUTER_DATA_DIR`
 *   4. `launch.token` inside `%LOCALAPPDATA%\HiveMind` (the Windows launcher default)
 *
 * The WebSocket itself (`/ws?token=<session>&last_seq=<n>`) is authenticated by the
 * per-boot session token, NOT by the launch token; the launch token only gates the HTTP
 * read that yields that session token. A spec that skips the HTTP step gets no socket.
 */

export const LAUNCH_TOKEN_ENV = 'E2E_LAUNCH_TOKEN';
export const LAUNCH_HEADER = 'x-hivemind-launch';
export const LAUNCH_QUERY = 'launch';
export const SESSION_PATH = '/api/session';
export const CONTRACT_SCHEMA = 'hiverouter.v1';

/** Server refusal code used for both the session gate and the read-route gate. */
export const LAUNCH_REFUSAL_CODE = 'E_ORIGIN_REJECTED';

/** Copy the live server returns for a missing/invalid launcher token. */
export const LAUNCH_GATE_COPY = /launcher token/i;

/** Read-only route used to prove the `/api/*` read gate (not only `/api/session`). */
export const GATED_READ_PATH = '/api/logs?source=stdout&tail=1';

export type TokenSource =
  | `env:${typeof LAUNCH_TOKEN_ENV}`
  | 'file:launch.token'
  | 'none';

export type DataDirSource = 'E2E_DATA_DIR' | 'HIVEROUTER_DATA_DIR' | 'LOCALAPPDATA' | 'none';

export interface LaunchTokenResolution {
  /** Raw token for request headers. NEVER persist, log, or attach this value. */
  readonly token: string | null;
  readonly tokenSource: TokenSource;
  /** SHA-256 prefix, safe to put in evidence. Null when no token was resolved. */
  readonly tokenFingerprint: string | null;
  readonly dataDir: string | null;
  readonly dataDirSource: DataDirSource;
  /** Probe notes that never contain the token. */
  readonly notes: readonly string[];
}

export interface AppAuth {
  readonly baseURL: string;
  readonly token: string | null;
  readonly tokenSource: TokenSource;
  readonly tokenFingerprint: string | null;
  readonly dataDir: string | null;
  readonly dataDirSource: DataDirSource;
  /**
   * True when an unauthenticated `GET /api/session` was refused with 403, i.e. the
   * server really is launch-gated. False means the server was started without
   * `HIVEROUTER_LAUNCH_TOKEN` and the gate is off.
   */
  readonly gated: boolean;
  /** Status of the unauthenticated `/api/session` probe (evidence, no body). */
  readonly unauthStatus: number;
}

export interface ProbeResult {
  readonly status: number;
  readonly code: string | null;
  readonly message: string | null;
  /** Present only on HTTP 200; the raw session token is deliberately dropped. */
  readonly schema?: string;
  readonly lastSeq?: number;
  readonly ws?: string;
  readonly tokenPresent?: boolean;
}

function fingerprint(token: string): string {
  return createHash('sha256').update(token).digest('hex').slice(0, 12);
}

/** Validates a resolved token without ever echoing its value. */
function normalizeToken(raw: string, source: string): string {
  const token = raw.trim();
  if (token.length === 0) {
    throw new Error(`LIVE_LAUNCH_TOKEN_EMPTY: ${source} contained only whitespace`);
  }
  if (/\s/.test(token)) {
    throw new Error(`LIVE_LAUNCH_TOKEN_INVALID: ${source} contains embedded whitespace`);
  }
  if (token.length > 512) {
    throw new Error(`LIVE_LAUNCH_TOKEN_INVALID: ${source} is longer than 512 characters`);
  }
  return token;
}

export function resolveDataDir(): { dir: string; source: DataDirSource } {
  for (const name of ['E2E_DATA_DIR', 'HIVEROUTER_DATA_DIR'] as const) {
    const value = process.env[name];
    if (typeof value === 'string' && value.trim().length > 0) {
      return { dir: value.trim(), source: name };
    }
  }
  const localAppData = process.env.LOCALAPPDATA;
  if (typeof localAppData === 'string' && localAppData.trim().length > 0) {
    return { dir: join(localAppData.trim(), 'HiveMind'), source: 'LOCALAPPDATA' };
  }
  return { dir: '', source: 'none' };
}

/**
 * Resolves the launch token from the environment or the launcher's `launch.token`.
 *
 * A malformed token file is a named failure, never a silent "no token": a harness that
 * quietly falls back to unauthenticated navigation is exactly the bug this fixes.
 */
export function resolveLaunchToken(): LaunchTokenResolution {
  const notes: string[] = [];
  const envToken = process.env[LAUNCH_TOKEN_ENV];
  if (typeof envToken === 'string' && envToken.trim().length > 0) {
    const token = normalizeToken(envToken, `${LAUNCH_TOKEN_ENV}`);
    const dataDirInfo = resolveDataDir();
    notes.push(`token read from ${LAUNCH_TOKEN_ENV}`);
    return {
      token,
      tokenSource: `env:${LAUNCH_TOKEN_ENV}`,
      tokenFingerprint: fingerprint(token),
      dataDir: dataDirInfo.source === 'none' ? null : dataDirInfo.dir,
      dataDirSource: dataDirInfo.source,
      notes,
    };
  }
  if (typeof envToken === 'string') {
    notes.push(`${LAUNCH_TOKEN_ENV} is set but empty; falling back to launch.token`);
  }

  const dataDirInfo = resolveDataDir();
  if (dataDirInfo.source === 'none') {
    notes.push('no data dir: neither E2E_DATA_DIR, HIVEROUTER_DATA_DIR nor LOCALAPPDATA is set');
    return {
      token: null,
      tokenSource: 'none',
      tokenFingerprint: null,
      dataDir: null,
      dataDirSource: 'none',
      notes,
    };
  }

  const tokenPath = join(dataDirInfo.dir, 'launch.token');
  if (!existsSync(tokenPath)) {
    notes.push(`no launch.token under the ${dataDirInfo.source} data dir`);
    return {
      token: null,
      tokenSource: 'none',
      tokenFingerprint: null,
      dataDir: dataDirInfo.dir,
      dataDirSource: dataDirInfo.source,
      notes,
    };
  }

  let raw: string;
  try {
    raw = readFileSync(tokenPath, 'utf8');
  } catch (error) {
    throw new Error(
      `LIVE_LAUNCH_TOKEN_UNREADABLE: could not read launch.token from the ${dataDirInfo.source} ` +
        `data dir (${error instanceof Error ? error.message : String(error)})`,
    );
  }
  const token = normalizeToken(raw, 'launch.token');
  notes.push(`token read from launch.token in the ${dataDirInfo.source} data dir`);
  return {
    token,
    tokenSource: 'file:launch.token',
    tokenFingerprint: fingerprint(token),
    dataDir: dataDirInfo.dir,
    dataDirSource: dataDirInfo.source,
    notes,
  };
}

/** Headers for a same-origin authed read. Empty when the server is ungated. */
export function launchHeaders(auth: AppAuth): Record<string, string> {
  return auth.token === null ? {} : { [LAUNCH_HEADER]: auth.token };
}

/**
 * Builds the launcher URL for `path`, preserving any existing query and hash.
 * The token ends up in the address bar by design: the app reads it from there.
 */
export function launchUrl(baseURL: string, path: string, token: string): string {
  const url = new URL(path, baseURL.endsWith('/') ? baseURL : `${baseURL}/`);
  url.searchParams.set(LAUNCH_QUERY, token);
  return url.toString();
}

/** Replaces the launch token (query value or header value) before anything is stored. */
export function redactLaunchToken(text: string, token: string | null): string {
  if (token === null || token.length === 0) {
    return text;
  }
  return text
    .split(token)
    .join('[redacted-launch-token]')
    .replace(
      new RegExp(`([?&]${LAUNCH_QUERY}=)[^&\\s"']+`, 'g'),
      '$1[redacted-launch-token]',
    );
}

/** Evidence-safe page URL (never carries the launch token). */
export function safeUrl(url: string, token: string | null): string {
  return redactLaunchToken(url, token);
}

interface RawProbe {
  readonly status: number;
  readonly body: unknown;
  readonly text: string;
}

async function rawGet(url: string, headers: Record<string, string> = {}): Promise<RawProbe> {
  const response = await fetch(url, {
    headers: { accept: 'application/json', ...headers },
    signal: AbortSignal.timeout(5_000),
    redirect: 'manual',
  });
  const text = await response.text();
  let body: unknown = null;
  try {
    body = JSON.parse(text) as unknown;
  } catch {
    body = null;
  }
  return { status: response.status, body, text };
}

function refusalFields(body: unknown): { code: string | null; message: string | null } {
  if (typeof body !== 'object' || body === null) {
    return { code: null, message: null };
  }
  const record = body as Record<string, unknown>;
  const nested =
    typeof record.error === 'object' && record.error !== null
      ? (record.error as Record<string, unknown>)
      : record;
  return {
    code: typeof nested.code === 'string' ? nested.code : null,
    message: typeof nested.message === 'string' ? nested.message : null,
  };
}

/**
 * Probes `GET /api/session`. The raw session token is never returned to the caller or
 * written to evidence — only its presence and the frozen fields are surfaced.
 */
export async function probeSession(
  baseURL: string,
  headers: Record<string, string> = {},
): Promise<ProbeResult> {
  const probe = await rawGet(`${baseURL}${SESSION_PATH}`, headers);
  const refusal = refusalFields(probe.body);
  if (probe.status !== 200 || typeof probe.body !== 'object' || probe.body === null) {
    return { status: probe.status, code: refusal.code, message: refusal.message };
  }
  const body = probe.body as Record<string, unknown>;
  return {
    status: probe.status,
    code: null,
    message: null,
    schema: typeof body.schema === 'string' ? body.schema : undefined,
    lastSeq: typeof body.last_seq === 'number' ? body.last_seq : undefined,
    ws: typeof body.ws === 'string' ? body.ws : undefined,
    tokenPresent: typeof body.token === 'string' && body.token.length > 0,
  };
}

/**
 * Resolves the token and classifies the running server:
 *   - gated (403 unauthenticated) with a working token -> usable AppAuth
 *   - gated without a token -> named failure (never a silent unauthenticated run)
 *   - not gated (200 unauthenticated) -> AppAuth with token = null, gated = false
 */
export async function resolveAppAuth(baseURL: string): Promise<AppAuth> {
  const base = baseURL.replace(/\/+$/, '');
  const resolution = resolveLaunchToken();
  let unauth: ProbeResult;
  try {
    unauth = await probeSession(base);
  } catch (error) {
    throw new Error(
      `LIVE_SERVER_UNREACHABLE: unauthenticated GET ${SESSION_PATH} at ${base} failed ` +
        `(${error instanceof Error ? error.message : String(error)}); is the live app running?`,
    );
  }

  if (unauth.status !== 200 && unauth.status !== 403) {
    throw new Error(
      `LIVE_SESSION_PROBE_UNEXPECTED: unauthenticated GET ${SESSION_PATH} returned HTTP ${unauth.status}` +
        (unauth.code === null ? '' : ` (${unauth.code})`) +
        `; is ${base} the live HiveMind server?`,
    );
  }

  const gated = unauth.status === 403;

  if (resolution.token === null) {
    if (gated) {
      throw new Error(
        'LIVE_LAUNCH_TOKEN_MISSING: the live server refused an unauthenticated GET ' +
          `${SESSION_PATH} (HTTP 403 ${unauth.code ?? 'unknown code'}) and no launch token was found. ` +
          `Set ${LAUNCH_TOKEN_ENV}, or E2E_DATA_DIR/HIVEROUTER_DATA_DIR to the launcher data dir ` +
          'that holds launch.token. Refusing to run the live spec unauthenticated.',
      );
    }
    return {
      baseURL: base,
      token: null,
      tokenSource: 'none',
      tokenFingerprint: null,
      dataDir: resolution.dataDir,
      dataDirSource: resolution.dataDirSource,
      gated: false,
      unauthStatus: unauth.status,
    };
  }

  const authed = await probeSession(base, { [LAUNCH_HEADER]: resolution.token });

  if (authed.status !== 200) {
    throw new Error(
      `LIVE_LAUNCH_TOKEN_REFUSED: GET ${SESSION_PATH} with the resolved token ` +
        `(source=${resolution.tokenSource}, fingerprint=${String(resolution.tokenFingerprint)}) ` +
        `returned HTTP ${authed.status}` +
        (authed.code === null ? '' : ` (${authed.code}: ${String(authed.message)})`) +
        '. The token belongs to a different server boot (launch.token rotated): re-resolve it.',
    );
  }
  if (authed.schema !== CONTRACT_SCHEMA) {
    throw new Error(
      `LIVE_SCHEMA_MISMATCH: ${SESSION_PATH} schema=${String(authed.schema)}, expected ${CONTRACT_SCHEMA}`,
    );
  }
  if (authed.tokenPresent !== true || typeof authed.lastSeq !== 'number' || authed.ws !== '/ws') {
    throw new Error(
      `LIVE_SESSION_SHAPE: ${SESSION_PATH} did not carry {token,schema,last_seq,ws} on an authed read`,
    );
  }

  return {
    baseURL: base,
    token: resolution.token,
    tokenSource: resolution.tokenSource,
    tokenFingerprint: resolution.tokenFingerprint,
    dataDir: resolution.dataDir,
    dataDirSource: resolution.dataDirSource,
    gated,
    unauthStatus: unauth.status,
  };
}

/** Evidence-safe description of the resolved auth context (no token, no username path). */
export function describeAuth(auth: AppAuth): Record<string, unknown> {
  return {
    baseURL: auth.baseURL,
    gated: auth.gated,
    unauthSessionStatus: auth.unauthStatus,
    tokenPresent: auth.token !== null,
    tokenSource: auth.tokenSource,
    tokenFingerprint: auth.tokenFingerprint,
    dataDirConfigured: auth.dataDir !== null,
    dataDirSource: auth.dataDirSource,
  };
}

/**
 * Navigates with the launch token in the URL (the exact launcher path) and waits for
 * the app's boot network to settle. Throws when the server is gated and no token exists.
 */
export async function gotoWithLaunch(
  page: Page,
  auth: AppAuth,
  path = '/',
): Promise<Response | null> {
  if (auth.gated && auth.token === null) {
    throw new Error(
      'LIVE_LAUNCH_TOKEN_REQUIRED: the server is launch-gated; authed navigation needs a token',
    );
  }
  const target =
    auth.token === null ? new URL(path, `${auth.baseURL}/`).toString() : launchUrl(auth.baseURL, path, auth.token);
  const response = await page.goto(target, { waitUntil: 'networkidle', timeout: 30_000 });
  await page.waitForTimeout(600);
  return response;
}

export interface StrictAuthEvidence {
  readonly auth: Record<string, unknown>;
  readonly sessionWithoutToken: { status: number; code: string | null };
  readonly sessionWithInvalidToken: { status: number; code: string | null };
  readonly sessionWithToken: {
    status: number;
    schema: string | null;
    tokenPresent: boolean;
    lastSeq: number | null;
    ws: string | null;
  };
  readonly gatedReadWithoutToken: { path: string; status: number; code: string | null };
  readonly gatedReadWithToken: { path: string; status: number };
  readonly note: string;
}

/** Deliberately wrong token, structurally plausible but never a real secret. */
const INVALID_TOKEN = 'e2e-invalid-launch-token-000000000000';

/**
 * Asserts the strict launch gate over real HTTP with Playwright's request context:
 * session path, a second read route, missing header and wrong header.
 *
 * When the server is NOT gated (started without `HIVEROUTER_LAUNCH_TOKEN`) this records
 * that fact and asserts only the unauthenticated-200 direction — it never pretends the
 * gate was proven.
 */
export async function assertStrictLaunchAuth(
  request: APIRequestContext,
  auth: AppAuth,
): Promise<StrictAuthEvidence> {
  const url = (path: string): string => `${auth.baseURL}${path}`;
  const headers = launchHeaders(auth);

  const sessionWithout = await request.get(url(SESSION_PATH), {
    headers: { accept: 'application/json' },
    failOnStatusCode: false,
  });
  const sessionWithoutBody = (await sessionWithout.json().catch(() => null)) as unknown;
  const sessionWithoutRefusal = refusalFields(sessionWithoutBody);

  const sessionInvalid = await request.get(url(SESSION_PATH), {
    headers: { accept: 'application/json', [LAUNCH_HEADER]: INVALID_TOKEN },
    failOnStatusCode: false,
  });
  const sessionInvalidBody = (await sessionInvalid.json().catch(() => null)) as unknown;
  const sessionInvalidRefusal = refusalFields(sessionInvalidBody);

  const sessionAuthed = await request.get(url(SESSION_PATH), {
    headers: { accept: 'application/json', ...headers },
    failOnStatusCode: false,
  });
  const sessionAuthedBody = (await sessionAuthed.json().catch(() => null)) as
    | Record<string, unknown>
    | null;

  const readWithout = await request.get(url(GATED_READ_PATH), {
    headers: { accept: 'application/json' },
    failOnStatusCode: false,
  });
  const readWithoutBody = (await readWithout.json().catch(() => null)) as unknown;
  const readWithoutRefusal = refusalFields(readWithoutBody);

  const readAuthed = await request.get(url(GATED_READ_PATH), {
    headers: { accept: 'application/json', ...headers },
    failOnStatusCode: false,
  });

  const evidence: StrictAuthEvidence = {
    auth: describeAuth(auth),
    sessionWithoutToken: { status: sessionWithout.status(), code: sessionWithoutRefusal.code },
    sessionWithInvalidToken: { status: sessionInvalid.status(), code: sessionInvalidRefusal.code },
    sessionWithToken: {
      status: sessionAuthed.status(),
      schema: typeof sessionAuthedBody?.schema === 'string' ? sessionAuthedBody.schema : null,
      tokenPresent: typeof sessionAuthedBody?.token === 'string' && sessionAuthedBody.token.length > 0,
      lastSeq: typeof sessionAuthedBody?.last_seq === 'number' ? sessionAuthedBody.last_seq : null,
      ws: typeof sessionAuthedBody?.ws === 'string' ? sessionAuthedBody.ws : null,
    },
    gatedReadWithoutToken: {
      path: GATED_READ_PATH,
      status: readWithout.status(),
      code: readWithoutRefusal.code,
    },
    gatedReadWithToken: { path: GATED_READ_PATH, status: readAuthed.status() },
    note: auth.gated
      ? 'server is launch-gated (HIVEROUTER_LAUNCH_TOKEN configured)'
      : 'server is NOT launch-gated: HIVEROUTER_LAUNCH_TOKEN was not configured for this boot',
  };

  if (auth.token === null) {
    // Ungated server: an anonymous read is legitimately 200.
    expect(sessionWithout.status(), 'ungated server: anonymous /api/session must be 200').toBe(200);
    expect(readWithout.status(), 'ungated server: anonymous read route must be 200').toBe(200);
    return evidence;
  }

  if (!auth.gated) {
    // A token was supplied but the server ignores it; the authed direction must still pass.
    expect(sessionAuthed.status(), 'authed /api/session must be 200').toBe(200);
    return evidence;
  }

  expect(sessionWithout.status(), 'missing launch token: /api/session must be refused').toBe(403);
  expect(
    sessionWithoutRefusal.code,
    'missing launch token: /api/session refusal code',
  ).toBe(LAUNCH_REFUSAL_CODE);
  expect(
    sessionInvalid.status(),
    'invalid launch token: /api/session must be refused',
  ).toBe(403);
  expect(
    sessionInvalidRefusal.code,
    'invalid launch token: /api/session refusal code',
  ).toBe(LAUNCH_REFUSAL_CODE);
  expect(
    sessionAuthed.status(),
    'valid launch token: /api/session must be accepted',
  ).toBe(200);
  expect(sessionAuthedBody?.schema, 'authed /api/session schema').toBe(CONTRACT_SCHEMA);
  expect(readWithout.status(), 'missing launch token: read route must be refused').toBe(403);
  expect(
    readWithoutRefusal.code,
    'missing launch token: read-route refusal code',
  ).toBe(LAUNCH_REFUSAL_CODE);
  expect(readAuthed.status(), 'valid launch token: read route must be accepted').toBe(200);

  const rawSession = JSON.stringify(sessionAuthedBody ?? {});
  expect(
    rawSession.includes(String(auth.token)),
    'the launch token must never be echoed by the server (session includes launch token)',
  ).toBe(false);
  expect(
    JSON.stringify(sessionWithoutBody ?? {}).includes(String(auth.token)),
    'a refusal body must never echo the launch token',
  ).toBe(false);

  return evidence;
}
