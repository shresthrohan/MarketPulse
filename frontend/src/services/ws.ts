import type { ClientMessage, ServerMessage } from '@/types/api';

export type WsState = 'connecting' | 'open' | 'reconnecting' | 'closed';

export interface WsStatus {
  state: WsState;
  /** Consecutive failed attempts since the last successful open. */
  attempt: number;
  /** Delay before the next reconnect attempt, when reconnecting. */
  retryInMs: number | null;
  /** Close code of the last disconnect (1013 = server at capacity / slow client). */
  lastCloseCode: number | null;
}

export const BACKOFF_BASE_MS = 500;
export const BACKOFF_CAP_MS = 30_000;

/** Exponential backoff with "equal jitter": half fixed, half random, never above the cap. */
export function backoffDelay(attempt: number, random: () => number = Math.random): number {
  const exp = Math.min(BACKOFF_CAP_MS, BACKOFF_BASE_MS * 2 ** Math.max(0, attempt));
  return Math.round(exp / 2 + random() * (exp / 2));
}

export function wsUrl(path: string, loc: Pick<Location, 'protocol' | 'host'> = window.location): string {
  return `${loc.protocol === 'https:' ? 'wss:' : 'ws:'}//${loc.host}${path}`;
}

type SocketLike = Pick<WebSocket, 'send' | 'close' | 'readyState'> & {
  onopen: ((ev: Event) => void) | null;
  onclose: ((ev: CloseEvent) => void) | null;
  onmessage: ((ev: MessageEvent) => void) | null;
  onerror: ((ev: Event) => void) | null;
};

interface Handlers {
  onMessage: (msg: ServerMessage) => void;
  onStatus: (status: WsStatus) => void;
}

/** Reconnecting market-data socket. Subscriptions are remembered and re-sent after every reconnect. */
export class MarketSocket {
  private ws: SocketLike | null = null;
  private timer: ReturnType<typeof setTimeout> | undefined;
  private attempt = 0;
  private stopped = true;
  private readonly subs = new Set<string>();
  private readonly url: string;
  private readonly handlers: Handlers;
  private readonly create: (url: string) => SocketLike;

  constructor(url: string, handlers: Handlers, create: (url: string) => SocketLike = (u) => new WebSocket(u)) {
    this.url = url;
    this.handlers = handlers;
    this.create = create;
  }

  start(): void {
    if (!this.stopped) return;
    this.stopped = false;
    this.open();
  }

  stop(): void {
    this.stopped = true;
    clearTimeout(this.timer);
    const ws = this.ws;
    this.ws = null;
    ws?.close(1000);
    this.emit('closed', null, null);
  }

  subscribe(symbols: string[]): void {
    symbols.forEach((s) => this.subs.add(s));
    this.send({ action: 'subscribe', symbols });
  }

  unsubscribe(symbols: string[]): void {
    symbols.forEach((s) => this.subs.delete(s));
    this.send({ action: 'unsubscribe', symbols });
  }

  private send(msg: ClientMessage): void {
    if (this.ws?.readyState === 1 /* OPEN */) this.ws.send(JSON.stringify(msg));
  }

  private emit(state: WsState, retryInMs: number | null, lastCloseCode: number | null): void {
    this.handlers.onStatus({ state, attempt: this.attempt, retryInMs, lastCloseCode });
  }

  private open(): void {
    this.emit(this.attempt ? 'reconnecting' : 'connecting', null, null);
    const ws = this.create(this.url);
    this.ws = ws;
    ws.onopen = () => {
      this.attempt = 0;
      this.emit('open', null, null);
      if (this.subs.size) this.send({ action: 'subscribe', symbols: [...this.subs] });
    };
    ws.onmessage = (ev) => {
      let msg: ServerMessage;
      try {
        msg = JSON.parse(String(ev.data));
      } catch {
        return; // ignore malformed frames
      }
      this.handlers.onMessage(msg);
    };
    ws.onerror = () => {}; // a close event always follows; reconnect is handled there
    ws.onclose = (ev) => {
      if (this.ws !== ws) return; // superseded or stopped
      this.ws = null;
      if (this.stopped) return;
      const delay = backoffDelay(this.attempt);
      this.attempt += 1;
      this.emit('reconnecting', delay, ev.code);
      this.timer = setTimeout(() => this.open(), delay);
    };
  }
}

export function wsStatusText(s: WsStatus): string {
  switch (s.state) {
    case 'open':
      return 'Market stream connected';
    case 'connecting':
      return 'Connecting to market stream…';
    case 'closed':
      return 'Market stream closed';
    case 'reconnecting': {
      const when = s.retryInMs !== null ? ` in ${(s.retryInMs / 1000).toFixed(1)}s` : '…';
      const code = s.lastCloseCode ? `, last close code ${s.lastCloseCode}${s.lastCloseCode === 1013 ? ' (server busy)' : ''}` : '';
      return `Disconnected — reconnecting${when} (attempt ${s.attempt}${code})`;
    }
  }
}
