import type { IChartApi, ISeriesApi, MouseEventParams, SeriesType, UTCTimestamp } from 'lightweight-charts';
import { useEffect, useRef, useState } from 'react';
import type { Point } from '@/types/api';
import { cn } from '@/utils/cn';
import { fmtNum } from '@/utils/format';
import { withAlpha } from './colors';
import { useChart } from './useChart';

export interface SeriesSpec {
  id: string;
  label: string;
  color: string;
  data: Point[];
  kind?: 'line' | 'area' | 'histogram';
}

const defaultFormat = (v: number) => fmtNum(v);

const colorOptions = (s: SeriesSpec) =>
  s.kind === 'histogram'
    ? { color: withAlpha(s.color, 0.55) }
    : s.kind === 'area'
      ? { lineColor: s.color, topColor: withAlpha(s.color, 0.28), bottomColor: withAlpha(s.color, 0) }
      : { color: s.color };

function addSeries(chart: IChartApi, s: SeriesSpec): ISeriesApi<SeriesType> {
  const common = { priceLineVisible: false, lastValueVisible: true } as const;
  if (s.kind === 'histogram') return chart.addHistogramSeries(common);
  if (s.kind === 'area') return chart.addAreaSeries({ ...common, lineWidth: 2 });
  return chart.addLineSeries({ ...common, lineWidth: 2 });
}

/**
 * Multi-series time chart (line / area / histogram) with a legend showing values under the crosshair.
 * `format` must be a stable function (module-level); `fitKey` refits the time axis when it changes.
 */
export function LineChart({
  series,
  format = defaultFormat,
  timeVisible = true,
  fitKey,
  className,
}: {
  series: SeriesSpec[];
  format?: (v: number) => string;
  timeVisible?: boolean;
  fitKey?: string | number;
  className?: string;
}) {
  const { ref, chart } = useChart();
  const map = useRef(new Map<string, ISeriesApi<SeriesType>>());
  const fitted = useRef<string | null>(null);
  const [hover, setHover] = useState<Record<string, number> | null>(null);

  useEffect(() => {
    chart?.applyOptions({ localization: { priceFormatter: format }, timeScale: { timeVisible } });
  }, [chart, format, timeVisible]);

  useEffect(() => {
    if (!chart) return;
    const m = map.current;
    chart.subscribeCrosshairMove((p: MouseEventParams) => {
      if (!p.time) return setHover(null);
      const vals: Record<string, number> = {};
      for (const [id, s] of m) {
        const d = p.seriesData.get(s) as { value?: number } | undefined;
        if (typeof d?.value === 'number') vals[id] = d.value;
      }
      setHover(vals);
    });
    return () => m.clear(); // chart.remove() (useChart) disposes the series
  }, [chart]);

  useEffect(() => {
    if (!chart) return;
    const m = map.current;
    for (const [id, s] of m)
      if (!series.some((x) => x.id === id)) {
        chart.removeSeries(s);
        m.delete(id);
      }
    for (const spec of series) {
      const s = m.get(spec.id) ?? addSeries(chart, spec);
      m.set(spec.id, s);
      s.applyOptions(colorOptions(spec));
      s.setData(spec.data.map((p) => ({ time: p.time as UTCTimestamp, value: p.value })));
    }
    const key = `${fitKey ?? ''}|${series.map((s) => s.id).join(',')}|${series.some((s) => s.data.length) ? 1 : 0}`;
    if (fitted.current !== key) {
      chart.timeScale().fitContent();
      fitted.current = key;
    }
  }, [chart, series, fitKey]);

  return (
    <div className={cn('flex flex-col', className)}>
      <div className="flex min-h-5 flex-wrap gap-x-3 gap-y-0.5 px-1 pb-1 text-[11px]">
        {series.map((s) => {
          const v = hover ? hover[s.id] : s.data.at(-1)?.value;
          return (
            <span key={s.id} className="inline-flex items-center gap-1.5 text-muted-foreground">
              <span className="size-2 rounded-sm" style={{ background: s.color }} aria-hidden />
              {s.label}
              <span className="text-foreground">{v === undefined ? '—' : format(v)}</span>
            </span>
          );
        })}
      </div>
      <div className="relative min-h-0 flex-1">
        <div ref={ref} className="absolute inset-0" />
        {series.every((s) => s.data.length === 0) && (
          <div className="pointer-events-none absolute inset-0 flex items-center justify-center text-xs text-muted-foreground">
            No data points in this window
          </div>
        )}
      </div>
    </div>
  );
}
