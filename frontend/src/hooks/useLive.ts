import { useSyncExternalStore } from 'react';
import { latestQuote, liveStore, type LiveState } from '@/services/liveStore';
import type { InstrumentRow, Quote } from '@/types/api';

function useLive<T>(select: (s: LiveState) => T): T {
  return useSyncExternalStore(liveStore.subscribe, () => select(liveStore.getState()));
}

export const useLiveQuote = (symbol: string | undefined) => useLive((s) => (symbol ? s.quotes[symbol] : undefined));
export const useLiveQuotes = () => useLive((s) => s.quotes);
export const useLiveRecent = () => useLive((s) => s.recent);
export const useWsStatus = () => useLive((s) => s.status);
export const useLastMessageAt = () => useLive((s) => s.lastMessageAt);
export const useMessageCount = () => useLive((s) => s.messages);

/** Live quote when one has arrived (and is newer), else the REST snapshot. */
export function useQuote(row: Pick<InstrumentRow, 'symbol' | 'quote'>): { quote: Quote | null; live: boolean } {
  const live = useLiveQuote(row.symbol);
  const quote = latestQuote(live, row.quote);
  return { quote, live: !!live && quote === live };
}
