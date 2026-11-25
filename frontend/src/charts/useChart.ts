import { ColorType, createChart, CrosshairMode, LineStyle, type IChartApi } from 'lightweight-charts';
import { useEffect, useRef, useState, type RefObject } from 'react';
import { useTheme, type Theme } from '@/hooks/useTheme';
import { cssVar } from './colors';

/**
 * Creates a lightweight-charts instance in `ref`, sizes it with a ResizeObserver (the container's CSS decides the size),
 * re-themes it when the theme toggles and removes it on unmount. Times render in UTC like the rest of the UI.
 */
export function useChart(): { ref: RefObject<HTMLDivElement>; chart: IChartApi | null; theme: Theme } {
  const ref = useRef<HTMLDivElement>(null);
  const [chart, setChart] = useState<IChartApi | null>(null);
  const theme = useTheme();

  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    const c = createChart(el, {
      width: el.clientWidth,
      height: el.clientHeight,
      crosshair: { mode: CrosshairMode.Normal },
      rightPriceScale: { borderVisible: false },
      timeScale: { borderVisible: false, timeVisible: true, secondsVisible: false },
      handleScale: { axisPressedMouseMove: { time: true, price: false } },
    });
    const ro = new ResizeObserver(([entry]) => {
      const { width, height } = entry.contentRect;
      if (width > 0 && height > 0) c.applyOptions({ width, height });
    });
    ro.observe(el);
    setChart(c);
    return () => {
      ro.disconnect();
      c.remove();
      setChart(null);
    };
  }, []);

  useEffect(() => {
    if (!chart) return;
    const grid = cssVar('--chart-grid');
    const muted = cssVar('--muted-foreground');
    chart.applyOptions({
      layout: { background: { type: ColorType.Solid, color: cssVar('--card') }, textColor: muted, fontFamily: 'Inter, ui-sans-serif, sans-serif', fontSize: 11 },
      grid: { vertLines: { color: grid }, horzLines: { color: grid } },
      crosshair: {
        vertLine: { color: muted, style: LineStyle.Dashed, labelBackgroundColor: cssVar('--panel') },
        horzLine: { color: muted, style: LineStyle.Dashed, labelBackgroundColor: cssVar('--panel') },
      },
    });
  }, [chart, theme]);

  return { ref, chart, theme };
}
