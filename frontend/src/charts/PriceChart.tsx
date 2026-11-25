import type { CandlestickData, HistogramData, ISeriesApi, LineData, MouseEventParams, UTCTimestamp } from 'lightweight-charts';
import { useEffect, useRef, useState } from 'react';
import { liveStore } from '@/services/liveStore';
import type { Bar, HistoryResponse, Point } from '@/types/api';
import { cn } from '@/utils/cn';
import { fmtPrice, fmtVolume } from '@/utils/format';
import { cssVar, withAlpha } from './colors';
import { useChart } from './useChart';

export interface Overlay {
  id: string;
  label: string;
  color: string;
  data: Point[];
}

interface Legend {
  bar: Bar | null;
  overlays: Record<string, number | undefined>;
}

const NO_OVERLAYS: Overlay[] = [];
const t = (time: number) => time as UTCTimestamp;
const toCandle = (b: Bar): CandlestickData => ({ time: t(b.time), open: b.open, high: b.high, low: b.low, close: b.close });

/**
 * Candles + volume histogram (own price scale) + optional SMA/EMA overlays, with an OHLCV legend under the crosshair.
 * With `live`, the backend's `bar` messages for `symbol` are pushed in via series.update — no client-side aggregation.
 */
export function PriceChart({
  symbol,
  bars,
  interval,
  live = false,
  overlays = NO_OVERLAYS,
  className,
}: {
  symbol: string;
  bars: Bar[];
  interval: HistoryResponse['interval'];
  live?: boolean;
  overlays?: Overlay[];
  className?: string;
}) {
  const { ref, chart, theme } = useChart();
  const candles = useRef<ISeriesApi<'Candlestick'> | null>(null);
  const volume = useRef<ISeriesApi<'Histogram'> | null>(null);
  const lines = useRef(new Map<string, ISeriesApi<'Line'>>());
  const last = useRef<Bar | null>(null);
  const lastOverlay = useRef<Legend['overlays']>({});
  const colors = useRef({ up: '#22c55e', down: '#ef4444' });
  const hovering = useRef(false);
  const fitted = useRef<Bar[] | null>(null);
  const [legend, setLegend] = useState<Legend>({ bar: null, overlays: {} });

  const toVolume = (b: Bar): HistogramData => ({
    time: t(b.time),
    value: b.volume,
    color: withAlpha(b.close >= b.open ? colors.current.up : colors.current.down, 0.45),
  });
  const showLast = () => {
    if (!hovering.current) setLegend({ bar: last.current, overlays: lastOverlay.current });
  };

  // Series + crosshair legend. No cleanup calls into the chart: useChart's chart.remove() disposes everything.
  useEffect(() => {
    if (!chart) return;
    const c = chart.addCandlestickSeries({ borderVisible: false });
    c.priceScale().applyOptions({ scaleMargins: { top: 0.08, bottom: 0.26 } });
    const v = chart.addHistogramSeries({ priceScaleId: 'volume', priceFormat: { type: 'volume' }, lastValueVisible: false, priceLineVisible: false });
    chart.priceScale('volume').applyOptions({ scaleMargins: { top: 0.8, bottom: 0 } });
    candles.current = c;
    volume.current = v;
    const map = lines.current;
    chart.subscribeCrosshairMove((p: MouseEventParams) => {
      const cd = p.seriesData.get(c) as CandlestickData | undefined;
      if (!p.time || !cd) {
        hovering.current = false;
        setLegend({ bar: last.current, overlays: lastOverlay.current });
        return;
      }
      hovering.current = true;
      const vol = (p.seriesData.get(v) as HistogramData | undefined)?.value ?? 0;
      const ov: Legend['overlays'] = {};
      for (const [id, s] of map) ov[id] = (p.seriesData.get(s) as LineData | undefined)?.value;
      setLegend({ bar: { time: Number(p.time), open: cd.open, high: cd.high, low: cd.low, close: cd.close, volume: vol }, overlays: ov });
    });
    return () => {
      candles.current = null;
      volume.current = null;
      map.clear();
    };
  }, [chart]);

  useEffect(() => {
    if (!chart || !candles.current || !volume.current) return;
    colors.current = { up: cssVar('--positive'), down: cssVar('--negative') };
    const { up, down } = colors.current;
    candles.current.applyOptions({ upColor: up, downColor: down, wickUpColor: up, wickDownColor: down });
    candles.current.setData(bars.map(toCandle));
    volume.current.setData(bars.map(toVolume));
    chart.applyOptions({ timeScale: { timeVisible: interval !== '1d' } });
    last.current = bars.at(-1) ?? null;
    if (fitted.current !== bars) {
      chart.timeScale().fitContent();
      fitted.current = bars;
    }
    showLast();
  }, [chart, bars, interval, theme]);

  useEffect(() => {
    if (!chart) return;
    const map = lines.current;
    for (const [id, s] of map)
      if (!overlays.some((o) => o.id === id)) {
        chart.removeSeries(s);
        map.delete(id);
      }
    for (const o of overlays) {
      let s = map.get(o.id);
      if (!s) {
        s = chart.addLineSeries({ lineWidth: 2, priceLineVisible: false, lastValueVisible: false, crosshairMarkerVisible: false });
        map.set(o.id, s);
      }
      s.applyOptions({ color: o.color });
      s.setData(o.data.map((p) => ({ time: t(p.time), value: p.value })));
    }
    lastOverlay.current = Object.fromEntries(overlays.map((o) => [o.id, o.data.at(-1)?.value]));
    showLast();
  }, [chart, overlays]);

  useEffect(() => {
    if (!chart || !live) return;
    return liveStore.onBar((b) => {
      if (b.symbol !== symbol || !candles.current || !volume.current) return;
      // series.update() throws for bars older than the last one (e.g. a late message after a history refetch).
      if (last.current && b.time < last.current.time) return;
      candles.current.update(toCandle(b));
      volume.current.update(toVolume(b));
      last.current = { time: b.time, open: b.open, high: b.high, low: b.low, close: b.close, volume: b.volume };
      showLast();
    });
  }, [chart, live, symbol]);

  const b = legend.bar;
  const dir = b ? (b.close >= b.open ? 'text-positive' : 'text-negative') : '';
  return (
    <div className={cn('relative', className)}>
      <div ref={ref} className="absolute inset-0" />
      <div className="pointer-events-none absolute left-2 top-1.5 z-10 flex max-w-[calc(100%-5rem)] flex-wrap items-center gap-x-3 gap-y-0.5 text-[11px]">
        <span className="font-semibold">{symbol}</span>
        {live && <span className="text-[10px] font-semibold uppercase tracking-wider text-positive">● streaming 1m</span>}
        {b && (
          <>
            {(['open', 'high', 'low', 'close'] as const).map((k) => (
              <span key={k} className="text-muted-foreground">
                {k[0].toUpperCase()} <span className={dir}>{fmtPrice(b[k])}</span>
              </span>
            ))}
            <span className="text-muted-foreground">
              V <span className="text-foreground">{fmtVolume(b.volume)}</span>
            </span>
          </>
        )}
        {overlays.map((o) => (
          <span key={o.id} style={{ color: o.color }}>
            {o.label} {fmtPrice(legend.overlays[o.id])}
          </span>
        ))}
      </div>
      {!b && (
        <div className="pointer-events-none absolute inset-0 z-10 flex items-center justify-center text-xs text-muted-foreground">
          No bars in this range yet{live && ' — waiting for live bars'}
        </div>
      )}
    </div>
  );
}
