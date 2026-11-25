// Mirrors docs/api.md. Timestamps are ISO-8601 UTC strings; chart points use Unix seconds.

export type IsoTime = string;
export type UnixSeconds = number;

export interface Point {
  time: UnixSeconds;
  value: number;
}

export interface Page<T> {
  items: T[];
  page: number;
  page_size: number;
  total: number;
}

export interface CursorPage<T> {
  items: T[];
  page: number;
  page_size: number;
  has_more: boolean;
}

// ---------- Objects ----------

export interface Quote {
  symbol: string;
  price: number;
  open: number | null;
  high: number | null;
  low: number | null;
  volume: number | null;
  change: number | null;
  /** Percent units (0.6696 = 0.67%), versus the UTC-day open. */
  change_pct: number | null;
  /** Percent units, versus the previous processed price. */
  tick_change_pct: number | null;
  rolling_avg: number | null;
  rolling_volume: number | null;
  rolling_window: number;
  event_id: string;
  event_timestamp: IsoTime;
  processed_at: IsoTime;
  source: string;
  data_mode: string;
}

export type AssetType = 'equity' | 'etf' | 'crypto';

export interface InstrumentRow {
  id: number;
  symbol: string;
  company_name: string;
  exchange: string;
  asset_type: AssetType;
  currency: string;
  /** Where this instrument's data comes from (simulated / live / delayed); null until established. */
  data_mode: string | null;
  quote: Quote | null;
}

// ---------- Instruments ----------

export type InstrumentSort = 'symbol' | 'company_name' | 'price' | 'change' | 'change_pct' | 'volume' | 'updated';
export type SortOrder = 'asc' | 'desc';

export interface InstrumentsQuery {
  search?: string;
  asset_type?: AssetType;
  exchange?: string;
  sort?: InstrumentSort;
  order?: SortOrder;
  page?: number;
  page_size?: number;
}

export type HistoryRange = '1D' | '1W' | '1M' | '3M' | '1Y';

export interface Bar {
  time: UnixSeconds;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

export interface HistoryResponse {
  symbol: string;
  range: HistoryRange;
  interval: '1m' | '15m' | '1d';
  bars: Bar[];
}

export interface MaxDrawdown {
  value: number;
  peak_at: IsoTime;
  trough_at: IsoTime;
  recovered_at: IsoTime | null;
}

/** Fractions: returns/volatility 0.012 = 1.2%. */
export interface Statistics {
  symbol: string;
  as_of: IsoTime;
  last_price: number | null;
  observations: number;
  high_52w: number | null;
  low_52w: number | null;
  avg_volume_30d: number | null;
  volatility_30d: number | null;
  returns: { '1W': number | null; '1M': number | null; '3M': number | null; '1Y': number | null };
  max_drawdown_1y: MaxDrawdown | null;
  sma_20: number | null;
  sma_50: number | null;
  ema_20: number | null;
}

// ---------- Market ----------

export interface Breadth {
  advancers: number;
  decliners: number;
  unchanged: number;
}

export interface MarketOverview {
  data_modes: string[];
  sources: string[];
  last_update: IsoTime | null;
  instruments_tracked: number;
  quotes_available: number;
  indices: InstrumentRow[];
  majors: InstrumentRow[];
  gainers: InstrumentRow[];
  losers: InstrumentRow[];
  active: InstrumentRow[];
  breadth: Breadth;
}

export interface RankingResponse {
  items: InstrumentRow[];
}

export type MarketEventStatus = 'valid' | 'retried';

export interface MarketEvent {
  event_id: string;
  symbol: string;
  event_timestamp: IsoTime;
  price: number;
  volume: number;
  open: number | null;
  high: number | null;
  low: number | null;
  close: number | null;
  source: string;
  data_mode: string;
  processing_status: MarketEventStatus;
  attempts: number;
  ingested_at: IsoTime | null;
  processed_at: IsoTime;
  latency_ms: number | null;
}

export interface MarketHistoryQuery {
  symbol?: string;
  source?: string;
  status?: MarketEventStatus;
  start?: IsoTime;
  end?: IsoTime;
  min_price?: number;
  max_price?: number;
  search?: string;
  page?: number;
  page_size?: number;
}

// ---------- Analytics ----------

export type AnalyticsRange = '1M' | '3M' | '6M' | '1Y';
export type ReturnMethod = 'simple' | 'log';
export type SymbolSeries = Record<string, Point[]>;

export interface ReturnsResponse {
  method: ReturnMethod;
  range: AnalyticsRange;
  series: SymbolSeries;
  summary: Record<string, { total_return: number | null; mean_return: number | null; observations: number }>;
}

export interface VolatilityResponse {
  window: number;
  annualization: number;
  series: SymbolSeries;
  latest: Record<string, number | null>;
  full_period: Record<string, number | null>;
}

export interface CorrelationResponse {
  symbols: string[];
  matrix: (number | null)[][];
  observations: number[][];
}

export interface DrawdownResponse {
  series: SymbolSeries;
  max_drawdown: Record<string, MaxDrawdown | null>;
}

export interface ComparisonResponse {
  base: number;
  series: SymbolSeries;
  total_return: Record<string, number | null>;
}

export interface MovingAveragesResponse {
  symbol: string;
  sma_window: number;
  ema_span: number;
  close: Point[];
  sma: Point[];
  ema: Point[];
}

export interface VolumeResponse {
  window: number;
  series: SymbolSeries;
  average: SymbolSeries;
}

// ---------- Watchlists ----------

export interface Watchlist {
  id: number;
  name: string;
  created_at: IsoTime;
  instruments: InstrumentRow[];
}

// ---------- Pipeline ----------

export interface PipelineOverview {
  window_hours: number;
  instruments_tracked: number;
  events_received: number;
  events_processed: number;
  events_failed: number;
  events_duplicate: number;
  events_retried: number;
  events_per_second: number;
  avg_latency_ms: number | null;
  max_latency_ms: number | null;
  db_write_ms: number | null;
  consumer_lag: number | null;
  last_successful_event_at: IsoTime | null;
  data_quality: { total: number; valid: number; invalid: number; duplicate: number; rejected: number; valid_pct: number | null };
  error_breakdown: { error_type: string; count: number }[];
}

export type ServiceKey = 'ingestion' | 'kafka' | 'processor' | 'postgres' | 'redis' | 'api' | 'websocket';
export type ServiceStatus = 'up' | 'degraded' | 'down';

export interface Service {
  key: ServiceKey;
  name: string;
  status: ServiceStatus;
  latency_ms: number | null;
  detail: string;
  metrics: Record<string, number | string | boolean | null>;
  last_seen: IsoTime | null;
}

export interface ServicesResponse {
  checked_at: IsoTime;
  items: Service[];
}

export interface PipelineMetrics {
  bucket_s: number;
  series: {
    events_per_second: Point[];
    avg_latency_ms: Point[];
    error_rate: Point[];
    consumer_lag: Point[];
    db_write_ms: Point[];
  };
}

export interface Throughput {
  bucket_s: number;
  received: Point[];
  processed: Point[];
  failed: Point[];
}

export type Severity = 'info' | 'warning' | 'error';

export interface PipelineError {
  id: number;
  event_id: string | null;
  symbol: string | null;
  error_type: string;
  error_message: string;
  severity: Severity;
  source: string | null;
  raw_payload: unknown;
  timestamp: IsoTime;
}

export type EventsView = 'all' | 'success' | 'failed' | 'retried';

export interface PipelineEvent {
  timestamp: IsoTime;
  event_id: string | null;
  source: string | null;
  symbol: string | null;
  event_type: string;
  severity: Severity;
  processing_status: 'valid' | 'retried' | 'rejected';
  error_details: string | null;
}

export interface PipelineEventsQuery {
  view?: EventsView;
  search?: string;
  symbol?: string;
  severity?: Severity;
  page?: number;
  page_size?: number;
}

// ---------- Health ----------

export interface ComponentHealth {
  status: string;
  latency_ms: number | null;
  detail: string;
}

export interface Health {
  status: 'ok' | 'degraded' | 'down';
  version: string;
  uptime_s: number;
  components: Record<'database' | 'redis' | 'kafka' | 'ingestion' | 'processor' | 'websocket', ComponentHealth>;
}

export interface DatabaseHealth extends ComponentHealth {
  pool: { size: number; checked_out: number; overflow: number };
}

export interface RedisHealth extends ComponentHealth {
  used_memory_bytes: number | null;
  connected_clients: number | null;
}

export interface KafkaHealth extends ComponentHealth {
  /** Not pinned down by the contract: a count or a list of broker addresses. */
  brokers: number | string[] | null;
  topics: Record<string, number>;
  missing_topics: string[];
  consumer_lag: number | null;
}

export interface SystemHealth {
  started_at: IsoTime;
  uptime_s: number;
  process: { cpu_percent: number | null; rss_bytes: number | null; threads: number | null };
  host: {
    cpu_percent: number | null;
    cpu_count: number | null;
    memory_total_bytes: number | null;
    memory_used_bytes: number | null;
    memory_percent: number | null;
    load_avg: number[] | null;
  };
  api: {
    requests: number;
    p50_ms: number | null;
    p95_ms: number | null;
    p99_ms: number | null;
    routes: { route: string; count: number; p50_ms: number | null; p95_ms: number | null }[];
  };
  websocket: { connections: number; messages_sent: number; dropped_clients: number; listener_connected: boolean };
  services: {
    key: string;
    host: string | null;
    status: string;
    cpu_percent: number | null;
    rss_bytes: number | null;
    uptime_s: number | null;
    heartbeat_at: IsoTime | null;
  }[];
}

// ---------- WebSocket /ws/market ----------

export type ClientMessage = { action: 'subscribe' | 'unsubscribe'; symbols: string[] };

export type QuoteMessage = { type: 'quote' } & Quote;

export interface BarMessage extends Bar {
  type: 'bar';
  symbol: string;
  interval: '1m';
}

export type ServerMessage =
  | { type: 'subscribed'; symbols: string[] }
  | QuoteMessage
  | BarMessage
  | { type: 'error'; message: string };
