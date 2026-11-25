import type { LucideIcon } from 'lucide-react';
import type { ReactNode } from 'react';
import { Skeleton } from '@/components/ui/skeleton';
import { Hint } from '@/components/ui/tooltip';
import { cn } from '@/utils/cn';

export function StatCard({
  label,
  value,
  sub,
  icon: Icon,
  hint,
  loading,
  tone,
  className,
}: {
  label: string;
  value: ReactNode;
  sub?: ReactNode;
  icon?: LucideIcon;
  hint?: string;
  loading?: boolean;
  tone?: 'positive' | 'negative' | 'warning';
  className?: string;
}) {
  return (
    <div className={cn('min-w-0 rounded-lg border bg-card px-3 py-2.5', className)}>
      <div className="flex items-center justify-between gap-2 text-[10px] font-semibold uppercase tracking-[0.08em] text-muted-foreground">
        <Hint content={hint} asChild>
          <span className="min-w-0 truncate" tabIndex={hint ? 0 : undefined}>
            {label}
          </span>
        </Hint>
        {Icon && <Icon className="size-3.5 shrink-0" aria-hidden />}
      </div>
      {loading ? (
        <Skeleton className="mt-2 h-6 w-24" />
      ) : (
        <div
          className={cn(
            'mt-1 truncate text-lg font-semibold leading-7',
            tone === 'positive' && 'text-positive',
            tone === 'negative' && 'text-negative',
            tone === 'warning' && 'text-warning',
          )}
        >
          {value}
        </div>
      )}
      {sub && !loading && <div className="mt-0.5 truncate text-[11px] text-muted-foreground">{sub}</div>}
    </div>
  );
}
