import { ArrowDownRight, ArrowUpRight } from 'lucide-react';
import { cn } from '@/utils/cn';
import { fmtChange, fmtFrac, fmtPct, fmtPrice, tone } from '@/utils/format';
import type { Quote } from '@/types/api';

const toneClass = { positive: 'text-positive', negative: 'text-negative', neutral: 'text-muted-foreground' } as const;

/** Signed, colored number. kind: "pct" = percent units, "frac" = fraction, "abs" = price change. */
export function ChangeText({
  value,
  kind = 'pct',
  arrow,
  className,
}: {
  value: number | null | undefined;
  kind?: 'pct' | 'frac' | 'abs';
  arrow?: boolean;
  className?: string;
}) {
  const t = tone(value);
  const text = kind === 'abs' ? fmtChange(value) : kind === 'frac' ? fmtFrac(value) : fmtPct(value);
  const Icon = t === 'positive' ? ArrowUpRight : t === 'negative' ? ArrowDownRight : null;
  return (
    <span className={cn('inline-flex items-center gap-0.5', toneClass[t], className)}>
      {arrow && Icon && <Icon className="size-3.5" aria-hidden />}
      {text}
    </span>
  );
}

/** Last price; briefly flashes green/red on each live tick (direction from the backend's tick_change_pct). */
export function FlashPrice({ quote, live, className }: { quote: Quote | null; live: boolean; className?: string }) {
  const dir = quote?.tick_change_pct ?? 0;
  return (
    <span
      key={live ? quote?.event_id : undefined}
      className={cn('-mx-0.5 rounded px-0.5', live && (dir > 0 ? 'animate-flash-up' : dir < 0 ? 'animate-flash-down' : ''), className)}
    >
      {fmtPrice(quote?.price)}
    </span>
  );
}
