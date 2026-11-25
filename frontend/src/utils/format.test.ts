import { describe, expect, it } from 'vitest';
import {
  DASH,
  fmtAgo,
  fmtBytes,
  fmtChange,
  fmtCompact,
  fmtDateTime,
  fmtDuration,
  fmtFrac,
  fmtInt,
  fmtMs,
  fmtPct,
  fmtPrice,
  fmtTime,
  fmtVolume,
  localInputToIso,
  tone,
} from './format';

describe('price formatting', () => {
  it('uses 2 decimals with grouping, 4 below 1', () => {
    expect(fmtPrice(225.5)).toBe('225.50');
    expect(fmtPrice(12345.678)).toBe('12,345.68');
    expect(fmtPrice(0.12345)).toBe('0.1235');
    expect(fmtPrice(0)).toBe('0.00');
  });
  it('renders a dash for missing or non-finite values', () => {
    for (const v of [null, undefined, NaN, Infinity]) expect(fmtPrice(v)).toBe(DASH);
  });
  it('signs absolute changes', () => {
    expect(fmtChange(1.5)).toBe('+1.50');
    expect(fmtChange(-0.25)).toBe('-0.25');
    expect(fmtChange(0)).toBe('0.00');
    expect(fmtChange(0.0042)).toBe('+0.0042');
  });
});

describe('percent formatting', () => {
  it('treats fmtPct input as percent units (API change_pct)', () => {
    expect(fmtPct(0.6696)).toBe('+0.67%');
    expect(fmtPct(-2)).toBe('-2.00%');
    expect(fmtPct(0)).toBe('0.00%');
    expect(fmtPct(-0.0001)).toBe('0.00%');
    expect(fmtPct(99, { signed: false, digits: 1 })).toBe('99.0%');
  });
  it('treats fmtFrac input as a fraction (statistics/analytics)', () => {
    expect(fmtFrac(0.012)).toBe('+1.20%');
    expect(fmtFrac(-0.18)).toBe('-18.00%');
    expect(fmtFrac(0.27, { signed: false })).toBe('27.00%');
    expect(fmtFrac(null)).toBe(DASH);
  });
});

describe('volume / compact formatting', () => {
  it('compacts large numbers', () => {
    expect(fmtCompact(1_834_200)).toBe('1.83M');
    expect(fmtCompact(2.1e6)).toBe('2.1M');
    expect(fmtCompact(950)).toBe('950');
    expect(fmtCompact(undefined)).toBe(DASH);
  });
  it('keeps exact volumes below 100k and compacts above', () => {
    expect(fmtVolume(18_342)).toBe('18,342');
    expect(fmtVolume(1_834_200)).toBe('1.83M');
    expect(fmtVolume(0.00012)).toBe('0.0001'); // fractional crypto quantity is not rounded to 0
    expect(fmtVolume(12.34567)).toBe('12.3457');
    expect(fmtVolume(null)).toBe(DASH);
  });
  it('formats integers, latency and bytes', () => {
    expect(fmtInt(1234567)).toBe('1,234,567');
    expect(fmtMs(1.234)).toBe('1.23 ms');
    expect(fmtMs(38.24)).toBe('38.2 ms');
    expect(fmtBytes(512)).toBe('512 B');
    expect(fmtBytes(1536)).toBe('1.5 KB');
    expect(fmtBytes(3 * 1024 ** 3)).toBe('3 GB');
  });
});

describe('time formatting (UTC)', () => {
  const iso = '2026-11-25T14:30:05.500+00:00';
  it('formats ISO strings and epochs in UTC', () => {
    expect(fmtTime(iso)).toBe('14:30:05');
    expect(fmtTime(Date.parse(iso))).toBe('14:30:05');
    expect(fmtDateTime(iso)).toBe('Nov 25, 14:30:05');
    expect(fmtTime('not a date')).toBe(DASH);
    expect(fmtTime(null)).toBe(DASH);
  });
  it('formats durations and relative times', () => {
    expect(fmtDuration(9)).toBe('9s');
    expect(fmtDuration(252)).toBe('4m 12s');
    expect(fmtDuration(7500)).toBe('2h 5m');
    expect(fmtDuration(3 * 86400 + 4 * 3600)).toBe('3d 4h');
    expect(fmtAgo(Date.parse(iso) - 12_000, Date.parse(iso))).toBe('12s ago');
  });
  it('reads datetime-local values as UTC', () => {
    expect(localInputToIso('2026-11-25T14:30')).toBe('2026-11-25T14:30:00.000Z');
    expect(localInputToIso('')).toBeUndefined();
    expect(localInputToIso('garbage')).toBeUndefined();
  });
});

describe('tone', () => {
  it('maps sign to tone', () => {
    expect(tone(1)).toBe('positive');
    expect(tone(-1)).toBe('negative');
    expect(tone(0)).toBe('neutral');
    expect(tone(null)).toBe('neutral');
  });
});
