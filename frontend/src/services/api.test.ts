import { afterEach, describe, expect, it, vi } from 'vitest';
import { api, ApiError, buildQuery, marketHistoryCsvUrl, request } from './api';

function mockFetch(status: number, body?: unknown, headers: Record<string, string> = {}) {
  const fn = vi.fn(
    async (_url: string, _init?: RequestInit) =>
      new Response(body === undefined ? null : typeof body === 'string' ? body : JSON.stringify(body), {
        status,
        headers: { 'Content-Type': 'application/json', ...headers },
      }),
  );
  vi.stubGlobal('fetch', fn);
  return fn;
}

afterEach(() => vi.unstubAllGlobals());

describe('request', () => {
  it('returns parsed JSON on 2xx', async () => {
    mockFetch(200, { items: [], page: 1, page_size: 25, total: 0 });
    await expect(request('/api/instruments')).resolves.toEqual({ items: [], page: 1, page_size: 25, total: 0 });
  });

  it('returns undefined for 204', async () => {
    mockFetch(204);
    await expect(api.deleteWatchlist(3)).resolves.toBeUndefined();
  });

  it('sends JSON bodies with the right method and headers', async () => {
    const fetch = mockFetch(201, { id: 1, name: 'Tech', created_at: 'x', instruments: [] });
    await api.createWatchlist('Tech');
    const [url, init] = fetch.mock.calls[0];
    expect(url).toBe('/api/watchlists');
    expect(init?.method).toBe('POST');
    expect(init?.body).toBe('{"name":"Tech"}');
    expect((init?.headers as Record<string, string>)['Content-Type']).toBe('application/json');
  });

  it('throws ApiError carrying the string detail', async () => {
    mockFetch(409, { detail: 'watchlist name already exists' });
    const err = await api.createWatchlist('Tech').catch((e: unknown) => e);
    expect(err).toBeInstanceOf(ApiError);
    expect(err).toMatchObject({ status: 409, detail: 'watchlist name already exists', message: 'watchlist name already exists' });
  });

  it('flattens FastAPI 422 validation details', async () => {
    mockFetch(422, { detail: [{ loc: ['query', 'range'], msg: 'Input should be 1D, 1W' }, { msg: 'second' }] });
    await expect(api.history('AAPL', '1D')).rejects.toMatchObject({ status: 422, detail: 'Input should be 1D, 1W; second' });
  });

  it('adds Retry-After to 429 errors', async () => {
    mockFetch(429, { detail: 'rate limited' }, { 'Retry-After': '7' });
    await expect(api.overview()).rejects.toMatchObject({ status: 429, retryAfter: 7, detail: 'rate limited (retry in 7s)' });
  });

  it('falls back to the status line when the body has no detail', async () => {
    mockFetch(502, '<html>bad gateway</html>');
    await expect(api.overview()).rejects.toMatchObject({ status: 502, detail: 'HTTP 502' });
  });

  it('wraps network failures', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => Promise.reject(new TypeError('Failed to fetch'))));
    await expect(api.overview()).rejects.toMatchObject({ status: 0, message: expect.stringContaining('unreachable') });
  });
});

describe('health component endpoints', () => {
  it('treat 503 with a JSON body as a "down" response, not an error', async () => {
    const body = { status: 'down', latency_ms: null, detail: 'connection refused', pool: { size: 0, checked_out: 0, overflow: 0 } };
    mockFetch(503, body);
    await expect(api.healthDatabase()).resolves.toEqual(body);
  });
  it('still throw on other failures', async () => {
    mockFetch(500, { detail: 'boom' });
    await expect(api.healthRedis()).rejects.toMatchObject({ status: 500, detail: 'boom' });
  });
});

describe('query building', () => {
  it('drops empty params and encodes the rest', () => {
    expect(buildQuery({ a: 1, b: '', c: undefined, d: null, e: 'x y' })).toBe('?a=1&e=x+y');
    expect(buildQuery({})).toBe('');
  });
  it('builds analytics and CSV URLs from the contract params', async () => {
    const fetch = mockFetch(200, {});
    await api.returns({ symbols: ['AAPL', 'MSFT'], range: '3M' }, 'log');
    expect(fetch.mock.calls[0][0]).toBe('/api/analytics/returns?symbols=AAPL%2CMSFT&range=3M&method=log');
    expect(marketHistoryCsvUrl({ symbol: 'AAPL', status: 'valid', page: 3, page_size: 50 })).toBe('/api/market/history?symbol=AAPL&status=valid&format=csv');
  });
});
