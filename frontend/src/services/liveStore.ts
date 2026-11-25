import type { BarMessage, Quote, ServerMessage } from '@/types/api';
import type { WsStatus } from './ws';

export interface LiveQuote extends Quote {
  /** Client receipt time (ms epoch). */
  received_at: number;
}

export interface LiveState {
  status: WsStatus;
  quotes: Record<string, LiveQuote>;
  /** Most recent quote updates, newest first. */
  recent: LiveQuote[];
  lastMessageAt: number | null;
  messages: number;
}

export const RECENT_MAX = 25;

export function createLiveStore(now: () => number = Date.now) {
  let state: LiveState = {
    status: { state: 'connecting', attempt: 0, retryInMs: null, lastCloseCode: null },
    quotes: {},
    recent: [],
    lastMessageAt: null,
    messages: 0,
  };
  const listeners = new Set<() => void>();
  const barListeners = new Set<(bar: BarMessage) => void>();

  const set = (patch: Partial<LiveState>) => {
    state = { ...state, ...patch };
    listeners.forEach((l) => l());
  };

  return {
    getState: () => state,
    subscribe(listener: () => void) {
      listeners.add(listener);
      return () => void listeners.delete(listener);
    },
    /** Bars bypass React state: charts push them straight into their series. */
    onBar(listener: (bar: BarMessage) => void) {
      barListeners.add(listener);
      return () => void barListeners.delete(listener);
    },
    setStatus(status: WsStatus) {
      set({ status });
    },
    handleMessage(msg: ServerMessage) {
      const t = now();
      if (msg.type === 'quote') {
        const { type: _type, ...quote } = msg;
        const merged: LiveQuote = { ...state.quotes[quote.symbol], ...quote, received_at: t };
        set({
          quotes: { ...state.quotes, [quote.symbol]: merged },
          recent: [merged, ...state.recent].slice(0, RECENT_MAX),
          lastMessageAt: t,
          messages: state.messages + 1,
        });
      } else if (msg.type === 'bar') {
        set({ lastMessageAt: t, messages: state.messages + 1 });
        barListeners.forEach((l) => l(msg));
      }
    },
  };
}

/** The app-wide store, fed by the single socket owned by <LiveProvider>. */
export const liveStore = createLiveStore();

/** Prefer whichever quote was processed last: a stale live quote must not hide a fresher REST snapshot. */
export function latestQuote(live: Quote | undefined, rest: Quote | null | undefined): Quote | null {
  if (!live) return rest ?? null;
  if (!rest) return live;
  return Date.parse(live.processed_at) >= Date.parse(rest.processed_at) ? live : rest;
}
