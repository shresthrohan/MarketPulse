import type { UseQueryResult } from '@tanstack/react-query';
import { AlertTriangle, Inbox, RotateCw } from 'lucide-react';
import type { ReactNode } from 'react';
import { Button } from '@/components/ui/button';
import { Card, CardHeader, CardTitle } from '@/components/ui/card';
import { Skeleton } from '@/components/ui/skeleton';
import { cn } from '@/utils/cn';
import { fmtTime } from '@/utils/format';

export function EmptyState({
  title = 'No data',
  description,
  action,
  className,
}: {
  title?: string;
  description?: ReactNode;
  action?: ReactNode;
  className?: string;
}) {
  return (
    <div className={cn('flex flex-col items-center justify-center gap-1.5 px-4 py-8 text-center', className)}>
      <Inbox className="size-5 text-muted-foreground" aria-hidden />
      <p className="text-xs font-medium">{title}</p>
      {description && <p className="max-w-sm text-[11px] text-muted-foreground">{description}</p>}
      {action}
    </div>
  );
}

export function ErrorState({ error, onRetry, className }: { error: unknown; onRetry?: () => void; className?: string }) {
  return (
    <div role="alert" className={cn('flex flex-col items-center justify-center gap-2 px-4 py-8 text-center', className)}>
      <AlertTriangle className="size-5 text-negative" aria-hidden />
      <p className="text-xs font-medium">Failed to load</p>
      <p className="max-w-md break-words text-[11px] text-muted-foreground">{error instanceof Error ? error.message : String(error)}</p>
      {onRetry && (
        <Button variant="secondary" size="sm" onClick={onRetry}>
          <RotateCw /> Retry
        </Button>
      )}
    </div>
  );
}

/** Loading skeleton / error with retry / empty / data — the four states every query renders. */
export function QueryState<T>({
  query,
  children,
  skeleton,
  isEmpty,
  empty,
}: {
  query: UseQueryResult<T>;
  children: (data: T) => ReactNode;
  skeleton?: ReactNode;
  isEmpty?: (data: T) => boolean;
  empty?: ReactNode;
}) {
  if (query.isPending) return <>{skeleton ?? <Skeleton className="m-3 h-24" />}</>;
  if (query.isError && query.data === undefined) return <ErrorState error={query.error} onRetry={() => void query.refetch()} />;
  const data = query.data as T;
  return (
    <>
      {query.isError && (
        <div role="status" className="flex items-center gap-1.5 border-b border-warning/30 bg-warning/10 px-3 py-1 text-[11px] text-warning">
          <AlertTriangle className="size-3" aria-hidden />
          Refresh failed ({query.error instanceof Error ? query.error.message : 'error'}) — showing data from {fmtTime(query.dataUpdatedAt)} UTC
        </div>
      )}
      {isEmpty?.(data) ? (empty ?? <EmptyState />) : children(data)}
    </>
  );
}

/** Card with a titled header whose body follows the query's state. */
export function QueryCard<T>({
  title,
  actions,
  query,
  children,
  isEmpty,
  empty,
  skeletonClass = 'h-64',
  className,
}: {
  title: ReactNode;
  actions?: ReactNode;
  query: UseQueryResult<T>;
  children: (data: T) => ReactNode;
  isEmpty?: (data: T) => boolean;
  empty?: ReactNode;
  skeletonClass?: string;
  className?: string;
}) {
  return (
    <Card className={className}>
      <CardHeader>
        <CardTitle>{title}</CardTitle>
        {actions}
      </CardHeader>
      <QueryState query={query} skeleton={<Skeleton className={cn('m-3', skeletonClass)} />} isEmpty={isEmpty} empty={empty}>
        {children}
      </QueryState>
    </Card>
  );
}
