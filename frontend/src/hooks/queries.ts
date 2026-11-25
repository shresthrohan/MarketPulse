import { keepPreviousData, QueryClient, useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { toast } from 'sonner';
import { api, ApiError, type AnalyticsQuery } from '@/services/api';
import type {
  AnalyticsRange,
  HistoryRange,
  InstrumentsQuery,
  MarketHistoryQuery,
  PipelineEventsQuery,
  ReturnMethod,
} from '@/types/api';

export const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 10_000,
      refetchOnWindowFocus: false,
      // Client errors won't fix themselves; retry only network/5xx/429 a couple of times.
      retry: (count, err) => count < 2 && !(err instanceof ApiError && err.status >= 400 && err.status < 500 && err.status !== 429),
    },
  },
});

export const RANKING_REFETCH_MS = 15_000;
export const MONITOR_REFETCH_MS = 5_000;

// ---------- Instruments & market ----------

export const useInstruments = (q: InstrumentsQuery, refetch = true) =>
  useQuery({
    queryKey: ['instruments', q],
    queryFn: () => api.instruments(q),
    placeholderData: keepPreviousData,
    refetchInterval: refetch ? RANKING_REFETCH_MS : false,
  });

/** Every instrument (contract caps page_size at 100), for selectors. */
export const useInstrumentUniverse = () =>
  useQuery({ queryKey: ['instruments', 'universe'], queryFn: () => api.instruments({ sort: 'symbol', order: 'asc', page_size: 100 }), staleTime: 300_000 });

export const useInstrumentSearch = (term: string) =>
  useQuery({
    queryKey: ['instruments', 'search', term],
    queryFn: () => api.instruments({ search: term, sort: 'symbol', order: 'asc', page_size: 8 }),
    enabled: term.length > 0,
    staleTime: 30_000,
  });

export const useInstrument = (symbol: string) => useQuery({ queryKey: ['instrument', symbol], queryFn: () => api.instrument(symbol) });

export const useHistory = (symbol: string, range: HistoryRange) =>
  useQuery({ queryKey: ['history', symbol, range], queryFn: () => api.history(symbol, range), enabled: !!symbol });

export const useStatistics = (symbol: string) =>
  useQuery({ queryKey: ['statistics', symbol], queryFn: () => api.statistics(symbol), refetchInterval: 60_000 });

export const useMarketOverview = () =>
  useQuery({ queryKey: ['market', 'overview'], queryFn: api.overview, refetchInterval: RANKING_REFETCH_MS });

export const useMarketHistory = (q: MarketHistoryQuery, refetchMs: number | false = false) =>
  useQuery({
    queryKey: ['market', 'history', q],
    queryFn: () => api.marketHistory(q),
    placeholderData: keepPreviousData,
    refetchInterval: refetchMs,
  });

// ---------- Analytics ----------

const analyticsOpts = { placeholderData: keepPreviousData, staleTime: 60_000 } as const;
const enabledFor = (q: AnalyticsQuery) => q.symbols.length > 0;

export const useReturns = (q: AnalyticsQuery, method: ReturnMethod) =>
  useQuery({ queryKey: ['analytics', 'returns', q, method], queryFn: () => api.returns(q, method), enabled: enabledFor(q), ...analyticsOpts });
export const useVolatility = (q: AnalyticsQuery, window: number) =>
  useQuery({ queryKey: ['analytics', 'volatility', q, window], queryFn: () => api.volatility(q, window), enabled: enabledFor(q), ...analyticsOpts });
export const useCorrelation = (q: AnalyticsQuery, method: ReturnMethod) =>
  useQuery({ queryKey: ['analytics', 'correlation', q, method], queryFn: () => api.correlation(q, method), enabled: enabledFor(q), ...analyticsOpts });
export const useDrawdown = (q: AnalyticsQuery) =>
  useQuery({ queryKey: ['analytics', 'drawdown', q], queryFn: () => api.drawdown(q), enabled: enabledFor(q), ...analyticsOpts });
export const useComparison = (q: AnalyticsQuery) =>
  useQuery({ queryKey: ['analytics', 'comparison', q], queryFn: () => api.comparison(q), enabled: enabledFor(q), ...analyticsOpts });
export const useVolumeTrend = (q: AnalyticsQuery, window: number) =>
  useQuery({ queryKey: ['analytics', 'volume', q, window], queryFn: () => api.volume(q, window), enabled: enabledFor(q), ...analyticsOpts });
export const useMovingAverages = (symbol: string, range: AnalyticsRange, sma?: number, ema?: number, enabled = true) =>
  useQuery({
    queryKey: ['analytics', 'ma', symbol, range, sma, ema],
    queryFn: () => api.movingAverages(symbol, range, sma, ema),
    enabled: enabled && !!symbol,
    ...analyticsOpts,
  });

// ---------- Watchlists ----------

export const useWatchlists = () => useQuery({ queryKey: ['watchlists'], queryFn: api.watchlists });

export function errorText(e: unknown): string {
  if (!(e instanceof ApiError)) return e instanceof Error ? e.message : 'Unexpected error';
  if (e.status === 409) return `Conflict: ${e.detail}`;
  if (e.status === 404) return `Not found: ${e.detail}`;
  if (e.status === 422) return `Invalid input: ${e.detail}`;
  return e.detail;
}

/** Watchlist mutation: toast the outcome and refresh every watchlist query. */
export function useWatchlistMutation<V, R>(fn: (v: V) => Promise<R>, success: (v: V, r: R) => string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: fn,
    onSuccess: (r, v) => toast.success(success(v, r)),
    onError: (e) => toast.error(errorText(e)),
    onSettled: () => qc.invalidateQueries({ queryKey: ['watchlists'] }),
  });
}

// ---------- Pipeline ----------

export const usePipelineOverview = () =>
  useQuery({ queryKey: ['pipeline', 'overview'], queryFn: api.pipelineOverview, refetchInterval: MONITOR_REFETCH_MS });
export const usePipelineServices = () =>
  useQuery({ queryKey: ['pipeline', 'services'], queryFn: api.pipelineServices, refetchInterval: MONITOR_REFETCH_MS });
export const usePipelineMetrics = (minutes: number, bucket: number) =>
  useQuery({
    queryKey: ['pipeline', 'metrics', minutes, bucket],
    queryFn: () => api.pipelineMetrics(minutes, bucket),
    refetchInterval: MONITOR_REFETCH_MS,
    placeholderData: keepPreviousData,
  });
export const usePipelineThroughput = (minutes: number, bucket: number) =>
  useQuery({
    queryKey: ['pipeline', 'throughput', minutes, bucket],
    queryFn: () => api.pipelineThroughput(minutes, bucket),
    refetchInterval: MONITOR_REFETCH_MS,
    placeholderData: keepPreviousData,
  });
export const usePipelineEvents = (q: PipelineEventsQuery) =>
  useQuery({
    queryKey: ['pipeline', 'events', q],
    queryFn: () => api.pipelineEvents(q),
    refetchInterval: MONITOR_REFETCH_MS,
    placeholderData: keepPreviousData,
  });

// ---------- Health ----------

const monitor = { refetchInterval: MONITOR_REFETCH_MS } as const;
export const useHealth = () => useQuery({ queryKey: ['health'], queryFn: api.health, ...monitor });
export const useHealthDatabase = () => useQuery({ queryKey: ['health', 'database'], queryFn: api.healthDatabase, ...monitor });
export const useHealthRedis = () => useQuery({ queryKey: ['health', 'redis'], queryFn: api.healthRedis, ...monitor });
export const useHealthKafka = () => useQuery({ queryKey: ['health', 'kafka'], queryFn: api.healthKafka, ...monitor });
export const useSystemHealth = () => useQuery({ queryKey: ['health', 'system'], queryFn: api.system, ...monitor });
