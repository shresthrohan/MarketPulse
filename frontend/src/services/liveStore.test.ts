import { describe, expect, it, vi } from 'vitest';
import type { BarMessage, Quote, QuoteMessage } from '@/types/api';
import { createLiveStore, latestQuote, RECENT_MAX } from './liveStore';

const quote = (over: Partial<Quote> = {}): Quote => ({
  symbol: 'AAPL',
  price: 225.5,
  open: 224,
  high: 226,
  low: 223.5,
  volume: 1000,
  change: 1.5,
  change_pct: 0.6696,
  tick_change_pct: 0.01,
  rolling_avg: 225.4,
  rolling_volume: 100,
  rolling_window: 20,
  event_id: 'e1',
  event_timestamp: '2026-11-25T14:30:00.000Z',
  processed_at: '2026-11-25T14:30:00.040Z',
  source: 'simulator',
  data_mode: 'simulated',
  ...over,
});
const msg = (over: Partial<Quote> = {}): QuoteMessage => ({ type: 'quote', ...quote(over) });

describe('live quote store', () => {
  it('notifies subscribers and stops after unsubscribe', () => {
    const store = createLiveStore(() => 1000);
    const listener = vi.fn();
    const unsubscribe = store.subscribe(listener);
    store.handleMessage(msg());
    expect(listener).toHaveBeenCalledTimes(1);
    unsubscribe();
    store.handleMessage(msg({ price: 226 }));
    expect(listener).toHaveBeenCalledTimes(1);
  });

  it('merges quotes per symbol and keeps snapshots referentially stable for untouched symbols', () => {
    let t = 1000;
    const store = createLiveStore(() => t);
    store.handleMessage(msg());
    store.handleMessage(msg({ symbol: 'MSFT', price: 420, event_id: 'm1' }));
    const aaplBefore = store.getState().quotes.AAPL;

    t = 2000;
    // a partial update must not wipe fields it doesn't carry
    store.handleMessage({ type: 'quote', symbol: 'MSFT', price: 421, event_id: 'm2' } as QuoteMessage);
    const s = store.getState();
    expect(s.quotes.AAPL).toBe(aaplBefore); // unchanged symbol -> same object, so useSyncExternalStore won't re-render it
    expect(s.quotes.MSFT).toMatchObject({ price: 421, event_id: 'm2', data_mode: 'simulated', received_at: 2000 });
    expect(s.lastMessageAt).toBe(2000);
    expect(s.messages).toBe(3);
    expect(s.quotes.MSFT).not.toHaveProperty('type');
  });

  it('keeps a bounded newest-first recent list', () => {
    const store = createLiveStore();
    for (let i = 0; i < RECENT_MAX + 5; i++) store.handleMessage(msg({ event_id: `e${i}`, price: 200 + i }));
    const { recent } = store.getState();
    expect(recent).toHaveLength(RECENT_MAX);
    expect(recent[0].event_id).toBe(`e${RECENT_MAX + 4}`);
  });

  it('routes bars to bar listeners without touching quotes', () => {
    const store = createLiveStore(() => 5);
    const onBar = vi.fn();
    const off = store.onBar(onBar);
    const bar: BarMessage = { type: 'bar', symbol: 'AAPL', interval: '1m', time: 1764081000, open: 1, high: 2, low: 0.5, close: 1.5, volume: 10 };
    store.handleMessage(bar);
    expect(onBar).toHaveBeenCalledWith(bar);
    expect(store.getState().quotes).toEqual({});
    expect(store.getState().lastMessageAt).toBe(5);
    off();
    store.handleMessage(bar);
    expect(onBar).toHaveBeenCalledTimes(1);
  });

  it('tracks connection status', () => {
    const store = createLiveStore();
    store.setStatus({ state: 'open', attempt: 0, retryInMs: null, lastCloseCode: null });
    expect(store.getState().status.state).toBe('open');
  });
});

describe('latestQuote', () => {
  it('prefers the most recently processed snapshot', () => {
    const live = quote({ processed_at: '2026-11-25T14:30:05Z', price: 1 });
    const rest = quote({ processed_at: '2026-11-25T14:30:10Z', price: 2 });
    expect(latestQuote(live, rest)).toBe(rest);
    expect(latestQuote(rest, live)).toBe(rest);
    expect(latestQuote(undefined, rest)).toBe(rest);
    expect(latestQuote(live, null)).toBe(live);
    expect(latestQuote(undefined, null)).toBeNull();
  });
});
