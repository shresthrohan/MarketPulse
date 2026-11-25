import { afterEach, describe, expect, it, vi } from 'vitest';
import { BACKOFF_CAP_MS, backoffDelay, MarketSocket, wsUrl, type WsStatus } from './ws';

describe('backoffDelay', () => {
  it('grows exponentially with equal jitter', () => {
    // random=0 -> lower bound (half the exponential step), random=1 -> full step
    expect(backoffDelay(0, () => 0)).toBe(250);
    expect(backoffDelay(0, () => 1)).toBe(500);
    expect(backoffDelay(1, () => 1)).toBe(1000);
    expect(backoffDelay(3, () => 1)).toBe(4000);
    expect(backoffDelay(3, () => 0.5)).toBe(3000);
  });
  it('never exceeds the 30s cap, even for huge attempt counts', () => {
    for (const attempt of [6, 10, 50, 1000]) {
      expect(backoffDelay(attempt, () => 1)).toBe(BACKOFF_CAP_MS);
      expect(backoffDelay(attempt, () => 0)).toBe(BACKOFF_CAP_MS / 2);
    }
  });
  it('stays within [step/2, step] for random jitter', () => {
    for (let i = 0; i < 200; i++) {
      const d = backoffDelay(4);
      expect(d).toBeGreaterThanOrEqual(4000);
      expect(d).toBeLessThanOrEqual(8000);
    }
  });
  it('treats negative attempts as the first attempt', () => {
    expect(backoffDelay(-3, () => 1)).toBe(500);
  });
});

describe('wsUrl', () => {
  it('follows the page protocol', () => {
    expect(wsUrl('/ws/market', { protocol: 'http:', host: 'localhost:5173' })).toBe('ws://localhost:5173/ws/market');
    expect(wsUrl('/ws/market', { protocol: 'https:', host: 'mp.example' })).toBe('wss://mp.example/ws/market');
  });
});

class FakeSocket {
  static all: FakeSocket[] = [];
  readyState = 0;
  sent: string[] = [];
  onopen: ((ev: Event) => void) | null = null;
  onclose: ((ev: CloseEvent) => void) | null = null;
  onmessage: ((ev: MessageEvent) => void) | null = null;
  onerror: ((ev: Event) => void) | null = null;
  constructor(readonly url: string) {
    FakeSocket.all.push(this);
  }
  send(data: string) {
    this.sent.push(data);
  }
  close() {
    this.readyState = 3;
  }
  open() {
    this.readyState = 1;
    this.onopen?.(new Event('open'));
  }
  drop(code = 1006) {
    this.readyState = 3;
    this.onclose?.({ code } as CloseEvent);
  }
  receive(msg: unknown) {
    this.onmessage?.({ data: typeof msg === 'string' ? msg : JSON.stringify(msg) } as MessageEvent);
  }
}

describe('MarketSocket', () => {
  afterEach(() => {
    vi.useRealTimers();
    FakeSocket.all = [];
  });

  it('re-sends subscriptions after reconnecting with backoff', () => {
    vi.useFakeTimers();
    const statuses: WsStatus[] = [];
    const messages: unknown[] = [];
    const sock = new MarketSocket(
      'ws://x/ws/market',
      { onMessage: (m) => messages.push(m), onStatus: (s) => statuses.push(s) },
      (u) => new FakeSocket(u),
    );
    sock.subscribe(['AAPL', 'MSFT']); // before open: remembered, not sent
    sock.start();
    const first = FakeSocket.all[0];
    expect(first.sent).toEqual([]);
    first.open();
    expect(JSON.parse(first.sent[0])).toEqual({ action: 'subscribe', symbols: ['AAPL', 'MSFT'] });

    sock.unsubscribe(['MSFT']);
    first.receive({ type: 'subscribed', symbols: ['AAPL'] });
    first.receive('not json'); // ignored
    expect(messages).toEqual([{ type: 'subscribed', symbols: ['AAPL'] }]);

    first.drop(1013);
    const reconnecting = statuses.at(-1)!;
    expect(reconnecting).toMatchObject({ state: 'reconnecting', attempt: 1, lastCloseCode: 1013 });
    expect(reconnecting.retryInMs).toBeLessThanOrEqual(500);
    expect(FakeSocket.all).toHaveLength(1);

    vi.advanceTimersByTime(500);
    const second = FakeSocket.all[1];
    expect(second).toBeDefined();
    second.open();
    expect(JSON.parse(second.sent[0])).toEqual({ action: 'subscribe', symbols: ['AAPL'] });
    expect(statuses.at(-1)).toMatchObject({ state: 'open', attempt: 0 });
  });

  it('does not reconnect after stop()', () => {
    vi.useFakeTimers();
    const statuses: WsStatus[] = [];
    const sock = new MarketSocket('ws://x', { onMessage: () => {}, onStatus: (s) => statuses.push(s) }, (u) => new FakeSocket(u));
    sock.start();
    FakeSocket.all[0].open();
    sock.stop();
    FakeSocket.all[0].drop();
    vi.advanceTimersByTime(60_000);
    expect(FakeSocket.all).toHaveLength(1);
    expect(statuses.at(-1)?.state).toBe('closed');
  });
});
