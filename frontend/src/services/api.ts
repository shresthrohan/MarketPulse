import type {
  AnalyticsRange,
  ComparisonResponse,
  CorrelationResponse,
  CursorPage,
  DatabaseHealth,
  DrawdownResponse,
  Health,
  HistoryRange,
  HistoryResponse,
  InstrumentRow,
  InstrumentsQuery,
  KafkaHealth,
  MarketEvent,
  MarketHistoryQuery,
  MarketOverview,
  MovingAveragesResponse,
  Page,
  PipelineEvent,
  PipelineEventsQuery,
  PipelineMetrics,
  PipelineOverview,
  RedisHealth,
  ReturnMethod,
  ReturnsResponse,
  ServicesResponse,
  Statistics,
  SystemHealth,
  Throughput,
  VolatilityResponse,
  VolumeResponse,
  Watchlist,
} from '@/types/api';

export class ApiError extends Error {
  readonly status: number;
  readonly detail: string;
  readonly body: unknown;
  readonly retryAfter?: number;

  constructor(status: number, detail: string, body: unknown = null, retryAfter?: number) {
    super(detail);
    this.name = 'ApiError';
    this.status = status;
    this.detail = detail;
    this.body = body;
    this.retryAfter = retryAfter;
  }
}

type Params = Record<string, string | number | null | undefined>;

export function buildQuery(params: Params = {}): string {
  const sp = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) if (v !== undefined && v !== null && v !== '') sp.set(k, String(v));
  const s = sp.toString();
  return s ? `?${s}` : '';
}

/** FastAPI sends `detail` as a string, or as a list of validation errors for 422. */
function detailOf(body: unknown, fallback: string): string {
  const d = body && typeof body === 'object' ? (body as { detail?: unknown }).detail : undefined;
  if (typeof d === 'string' && d) return d;
  if (Array.isArray(d) && d.length)
    return d.map((e) => (e && typeof e === 'object' && 'msg' in e ? String((e as { msg: unknown }).msg) : JSON.stringify(e))).join('; ');
  return fallback;
}

export async function request<T>(path: string, init: RequestInit & { json?: unknown } = {}): Promise<T> {
  const { json, headers, ...rest } = init;
  let res: Response;
  try {
    res = await fetch(path, {
      ...rest,
      headers: { Accept: 'application/json', ...(json !== undefined && { 'Content-Type': 'application/json' }), ...headers },
      body: json !== undefined ? JSON.stringify(json) : rest.body,
    });
  } catch {
    throw new ApiError(0, 'Backend unreachable (network error)');
  }
  if (res.status === 204) return undefined as T;
  const text = await res.text();
  let body: unknown = null;
  if (text) {
    try {
      body = JSON.parse(text);
    } catch {
      body = text;
    }
  }
  if (!res.ok) {
    const retryAfter = Number(res.headers.get('Retry-After')) || undefined;
    let detail = detailOf(body, `HTTP ${res.status}${res.statusText ? ` ${res.statusText}` : ''}`);
    if (res.status === 429 && retryAfter) detail += ` (retry in ${retryAfter}s)`;
    throw new ApiError(res.status, detail, body, retryAfter);
  }
  return body as T;
}

/** Health component endpoints answer 503 when the component is down but still send the JSON status body. */
async function allowDown<T>(p: Promise<T>): Promise<T> {
  try {
    return await p;
  } catch (e) {
    if (e instanceof ApiError && e.status === 503 && e.body && typeof e.body === 'object') return e.body as T;
    throw e;
  }
}

const sym = (s: string) => encodeURIComponent(s.toUpperCase());

export interface AnalyticsQuery {
  symbols: string[];
  range: AnalyticsRange;
}

const analytics = <T>(path: string, q: AnalyticsQuery, extra: Params = {}) =>
  request<T>(`/api/analytics/${path}${buildQuery({ symbols: q.symbols.join(','), range: q.range, ...extra })}`);

export const marketHistoryCsvUrl = (q: MarketHistoryQuery) =>
  `/api/market/history${buildQuery({ ...q, page: undefined, page_size: undefined, format: 'csv' } as Params)}`;

export const api = {
  instruments: (q: InstrumentsQuery) => request<Page<InstrumentRow>>(`/api/instruments${buildQuery(q as Params)}`),
  instrument: (s: string) => request<InstrumentRow>(`/api/instruments/${sym(s)}`),
  history: (s: string, range: HistoryRange) => request<HistoryResponse>(`/api/instruments/${sym(s)}/history${buildQuery({ range })}`),
  statistics: (s: string) => request<Statistics>(`/api/instruments/${sym(s)}/statistics`),

  overview: () => request<MarketOverview>('/api/market/overview'),
  marketHistory: (q: MarketHistoryQuery) => request<CursorPage<MarketEvent>>(`/api/market/history${buildQuery(q as Params)}`),

  returns: (q: AnalyticsQuery, method: ReturnMethod) => analytics<ReturnsResponse>('returns', q, { method }),
  volatility: (q: AnalyticsQuery, window: number) => analytics<VolatilityResponse>('volatility', q, { window }),
  correlation: (q: AnalyticsQuery, method: ReturnMethod) => analytics<CorrelationResponse>('correlation', q, { method }),
  drawdown: (q: AnalyticsQuery) => analytics<DrawdownResponse>('drawdown', q),
  comparison: (q: AnalyticsQuery) => analytics<ComparisonResponse>('comparison', q),
  volume: (q: AnalyticsQuery, window: number) => analytics<VolumeResponse>('volume', q, { window }),
  movingAverages: (symbol: string, range: AnalyticsRange, sma?: number, ema?: number) =>
    request<MovingAveragesResponse>(`/api/analytics/moving-averages${buildQuery({ symbol, range, sma, ema })}`),

  watchlists: () => request<{ items: Watchlist[] }>('/api/watchlists'),
  createWatchlist: (name: string) => request<Watchlist>('/api/watchlists', { method: 'POST', json: { name } }),
  renameWatchlist: (id: number, name: string) => request<Watchlist>(`/api/watchlists/${id}`, { method: 'PUT', json: { name } }),
  deleteWatchlist: (id: number) => request<void>(`/api/watchlists/${id}`, { method: 'DELETE' }),
  addToWatchlist: (id: number, symbol: string) =>
    request<Watchlist>(`/api/watchlists/${id}/instruments`, { method: 'POST', json: { symbol } }),
  removeFromWatchlist: (id: number, symbol: string) =>
    request<void>(`/api/watchlists/${id}/instruments/${sym(symbol)}`, { method: 'DELETE' }),

  pipelineOverview: () => request<PipelineOverview>('/api/pipeline/overview'),
  pipelineServices: () => request<ServicesResponse>('/api/pipeline/services'),
  pipelineMetrics: (minutes: number, bucket_s: number) =>
    request<PipelineMetrics>(`/api/pipeline/metrics${buildQuery({ minutes, bucket_s })}`),
  pipelineThroughput: (minutes: number, bucket_s: number) =>
    request<Throughput>(`/api/pipeline/throughput${buildQuery({ minutes, bucket_s })}`),
  pipelineEvents: (q: PipelineEventsQuery) => request<CursorPage<PipelineEvent>>(`/api/pipeline/events${buildQuery(q as Params)}`),

  health: () => request<Health>('/api/health'),
  healthDatabase: () => allowDown(request<DatabaseHealth>('/api/health/database')),
  healthRedis: () => allowDown(request<RedisHealth>('/api/health/redis')),
  healthKafka: () => allowDown(request<KafkaHealth>('/api/health/kafka')),
  system: () => request<SystemHealth>('/api/health/system'),
};
