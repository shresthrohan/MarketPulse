// Fixed locale + UTC so the terminal renders identically everywhere (the venue's trading day is the UTC day).
const LOCALE = 'en-US';
export const DASH = '—';

type Num = number | null | undefined;
const ok = (v: Num): v is number => typeof v === 'number' && Number.isFinite(v);

const nfCache = new Map<string, Intl.NumberFormat>();
function nf(min: number, max: number): Intl.NumberFormat {
  const key = `${min}:${max}`;
  let f = nfCache.get(key);
  if (!f) nfCache.set(key, (f = new Intl.NumberFormat(LOCALE, { minimumFractionDigits: min, maximumFractionDigits: max })));
  return f;
}
const price2 = nf(2, 2);
const price4 = nf(4, 4);
const fixed2 = nf(2, 2);
const int = nf(0, 0);
const compact = new Intl.NumberFormat(LOCALE, { notation: 'compact', maximumFractionDigits: 2 });

/** 2 decimals, 4 below 1 (sub-dollar prices need the extra precision). */
export function fmtPrice(v: Num): string {
  if (!ok(v)) return DASH;
  return (Math.abs(v) < 1 && v !== 0 ? price4 : price2).format(v);
}

/** Signed absolute change: +1.50 / -0.25 (4 decimals only for sub-cent moves). */
export function fmtChange(v: Num): string {
  if (!ok(v)) return DASH;
  return `${v > 0 ? '+' : ''}${(Math.abs(v) < 0.01 && v !== 0 ? price4 : price2).format(v)}`;
}

/** Value already in percent units (0.67 -> "+0.67%"). */
export function fmtPct(v: Num, { signed = true, digits = 2 } = {}): string {
  if (!ok(v)) return DASH;
  const r = Number(v.toFixed(digits)) || 0; // no "-0.00%" for moves that round to zero
  return `${signed && r > 0 ? '+' : ''}${nf(digits, digits).format(r)}%`;
}

/** Value as a fraction (0.012 -> "+1.20%"). */
export const fmtFrac = (v: Num, opts?: { signed?: boolean; digits?: number }) => fmtPct(ok(v) ? v * 100 : v, opts);

/** Compact: 1834200 -> "1.83M". */
export const fmtCompact = (v: Num) => (ok(v) ? compact.format(v) : DASH);

const qty = nf(0, 4);
/** Volume: exact with grouping below 100k (up to 4 decimals for fractional crypto quantities), compact above. */
export const fmtVolume = (v: Num) =>
  !ok(v) ? DASH : Math.abs(v) >= 100_000 ? compact.format(v) : (Number.isInteger(v) ? int : qty).format(v);

export const fmtInt = (v: Num) => (ok(v) ? int.format(v) : DASH);
export const fmtNum = (v: Num, digits = 2) => (ok(v) ? nf(digits, digits).format(v) : DASH);

export const fmtMs = (v: Num) => (!ok(v) ? DASH : v < 10 ? `${fixed2.format(v)} ms` : `${nf(1, 1).format(v)} ms`);

export function fmtBytes(v: Num): string {
  if (!ok(v)) return DASH;
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let i = 0;
  let n = v;
  while (n >= 1024 && i < units.length - 1) {
    n /= 1024;
    i += 1;
  }
  return `${nf(0, i ? 1 : 0).format(n)} ${units[i]}`;
}

/** Seconds -> "3d 4h", "2h 5m", "4m 12s", "9s". */
export function fmtDuration(seconds: Num): string {
  if (!ok(seconds) || seconds < 0) return DASH;
  const s = Math.floor(seconds);
  const d = Math.floor(s / 86400);
  const h = Math.floor((s % 86400) / 3600);
  const m = Math.floor((s % 3600) / 60);
  if (d) return `${d}d ${h}h`;
  if (h) return `${h}h ${m}m`;
  if (m) return `${m}m ${s % 60}s`;
  return `${s}s`;
}

type TimeInput = string | number | null | undefined;
const toDate = (t: TimeInput) => {
  if (t === null || t === undefined || t === '') return null;
  const d = new Date(t);
  return Number.isNaN(d.getTime()) ? null : d;
};
const timeFmt = new Intl.DateTimeFormat(LOCALE, { timeZone: 'UTC', hour: '2-digit', minute: '2-digit', second: '2-digit', hourCycle: 'h23' });
const dateTimeFmt = new Intl.DateTimeFormat(LOCALE, {
  timeZone: 'UTC',
  month: 'short',
  day: '2-digit',
  hour: '2-digit',
  minute: '2-digit',
  second: '2-digit',
  hourCycle: 'h23',
});
const dateFmt = new Intl.DateTimeFormat(LOCALE, { timeZone: 'UTC', year: 'numeric', month: 'short', day: '2-digit' });

const withFmt = (f: Intl.DateTimeFormat) => (t: TimeInput) => {
  const d = toDate(t);
  return d ? f.format(d) : DASH;
};
/** "14:30:05" (UTC). Accepts ISO strings or ms epochs. */
export const fmtTime = withFmt(timeFmt);
/** "Nov 25, 14:30:05" (UTC). */
export const fmtDateTime = withFmt(dateTimeFmt);
/** "Nov 25, 2026" (UTC). */
export const fmtDate = withFmt(dateFmt);

/** "12s ago" relative to `now` (ms). */
export function fmtAgo(t: TimeInput, now = Date.now()): string {
  const d = toDate(t);
  if (!d) return DASH;
  const s = Math.max(0, Math.round((now - d.getTime()) / 1000));
  return s < 1 ? 'just now' : `${fmtDuration(s)} ago`;
}

/** `<input type="datetime-local">` value, read as UTC (the whole UI is UTC) -> ISO string. */
export function localInputToIso(v: string): string | undefined {
  if (!v) return undefined;
  const d = new Date(`${v}Z`);
  return Number.isNaN(d.getTime()) ? undefined : d.toISOString();
}

/** Sign of a number as a tone. */
export const tone = (v: Num): 'positive' | 'negative' | 'neutral' => (!ok(v) || v === 0 ? 'neutral' : v > 0 ? 'positive' : 'negative');
