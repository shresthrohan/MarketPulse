import { Skeleton } from '@/components/ui/skeleton';
import { ErrorState } from '@/components/States';
import { useInstrumentUniverse } from '@/hooks/queries';
import { cn } from '@/utils/cn';
import { seriesColor } from '@/charts/colors';

/** Toggle chips over the instrument universe; keeps between `min` and `max` selected. */
// ponytail: chip grid suits a tens-of-symbols universe; swap for a searchable list if it grows past ~100.
export function SymbolMultiSelect({
  value,
  onChange,
  min = 1,
  max = 10,
}: {
  value: string[];
  onChange: (symbols: string[]) => void;
  min?: number;
  max?: number;
}) {
  const universe = useInstrumentUniverse();
  if (universe.isPending) return <Skeleton className="h-7 w-full" />;
  if (universe.isError) return <ErrorState error={universe.error} onRetry={() => void universe.refetch()} className="py-3" />;

  const toggle = (s: string) => {
    if (value.includes(s)) {
      if (value.length > min) onChange(value.filter((v) => v !== s));
    } else if (value.length < max) onChange([...value, s]);
  };

  return (
    <div role="group" aria-label={`Select ${min}–${max} instruments`} className="flex flex-wrap gap-1">
      {universe.data.items.map((r) => {
        const idx = value.indexOf(r.symbol);
        const on = idx >= 0;
        return (
          <button
            key={r.id}
            type="button"
            aria-pressed={on}
            title={r.company_name}
            disabled={(!on && value.length >= max) || (on && value.length <= min)}
            onClick={() => toggle(r.symbol)}
            className={cn(
              'inline-flex h-6 cursor-pointer items-center gap-1.5 rounded border px-2 text-[11px] font-medium transition-colors disabled:cursor-not-allowed',
              on ? 'border-secondary/50 bg-secondary/10 text-foreground' : 'text-muted-foreground hover:text-foreground disabled:opacity-40',
            )}
          >
            {on && <span className="size-1.5 rounded-full" style={{ background: seriesColor(idx) }} />}
            {r.symbol}
          </button>
        );
      })}
    </div>
  );
}
