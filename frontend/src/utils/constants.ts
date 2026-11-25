import type { AnalyticsRange, HistoryRange } from '@/types/api';

/** Instrument the overview chart opens on (the spec's default); the nav's Instrument entry falls back to the first instrument if absent. */
export const DEFAULT_SYMBOL = 'SPY';

export const HISTORY_RANGES = ['1D', '1W', '1M', '3M', '1Y'] as const satisfies readonly HistoryRange[];
export const ANALYTICS_RANGES = ['1M', '3M', '6M', '1Y'] as const satisfies readonly AnalyticsRange[];
