// Series palette avoids pure green/red so it never reads as up/down.
const SERIES_COLORS = ['#3B82F6', '#F59E0B', '#A855F7', '#14B8A6', '#EC4899', '#F97316', '#06B6D4', '#EAB308', '#8B5CF6', '#64748B'];

export const seriesColor = (i: number) => SERIES_COLORS[((i % SERIES_COLORS.length) + SERIES_COLORS.length) % SERIES_COLORS.length];

/** Theme token value (hex/rgba) for canvas-drawn charts, which can't use CSS variables directly. */
export const cssVar = (name: string) => getComputedStyle(document.documentElement).getPropertyValue(name).trim();

export function withAlpha(hex: string, alpha: number): string {
  const h = hex.replace('#', '');
  const n = parseInt(h.length === 3 ? [...h].map((c) => c + c).join('') : h, 16);
  return `rgba(${(n >> 16) & 255}, ${(n >> 8) & 255}, ${n & 255}, ${alpha})`;
}
