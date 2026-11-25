import type { UseQueryResult } from '@tanstack/react-query';
import { ArrowDown, ArrowUp, ChevronLeft, ChevronRight, ChevronsUpDown } from 'lucide-react';
import type { ReactNode } from 'react';
import { Button } from '@/components/ui/button';
import { Skeleton } from '@/components/ui/skeleton';
import { TableCell, TableHead, TableRow } from '@/components/ui/table';
import { Hint } from '@/components/ui/tooltip';
import { EmptyState, ErrorState } from '@/components/States';
import { cn } from '@/utils/cn';
import { fmtInt } from '@/utils/format';

/** Header cell that drives server-side sorting. */
export function SortHead<K extends string>({
  label,
  field,
  sort,
  order,
  onSort,
  className,
}: {
  label: string;
  field: K;
  sort: K;
  order: 'asc' | 'desc';
  onSort: (field: K) => void;
  className?: string;
}) {
  const active = sort === field;
  const Icon = !active ? ChevronsUpDown : order === 'asc' ? ArrowUp : ArrowDown;
  return (
    <TableHead className={className} aria-sort={active ? (order === 'asc' ? 'ascending' : 'descending') : 'none'}>
      <button
        type="button"
        onClick={() => onSort(field)}
        className={cn('inline-flex cursor-pointer items-center gap-1 uppercase hover:text-foreground', active && 'text-foreground')}
      >
        {label}
        <Icon className="size-3" aria-hidden />
      </button>
    </TableHead>
  );
}

export function TableSkeleton({ rows = 8, cols }: { rows?: number; cols: number }) {
  return (
    <>
      {Array.from({ length: rows }, (_, r) => (
        <TableRow key={r} className="hover:bg-transparent">
          {Array.from({ length: cols }, (_, c) => (
            <TableCell key={c}>
              <Skeleton className="h-3.5 w-full max-w-24" />
            </TableCell>
          ))}
        </TableRow>
      ))}
    </>
  );
}

/** Full-width row for empty/error states inside a table body. */
export function TableMessage({ cols, children }: { cols: number; children: ReactNode }) {
  return (
    <TableRow className="hover:bg-transparent">
      <TableCell colSpan={cols} className="h-auto whitespace-normal p-0">
        {children}
      </TableCell>
    </TableRow>
  );
}

/** Table-body flavour of QueryState: skeleton rows / error with retry / empty message / rows. */
export function QueryRows<T>({
  query,
  cols,
  rows = 8,
  isEmpty,
  empty,
  children,
}: {
  query: UseQueryResult<T>;
  cols: number;
  rows?: number;
  isEmpty: (data: T) => boolean;
  empty: string | ReactNode;
  children: (data: T) => ReactNode;
}) {
  if (query.isPending) return <TableSkeleton cols={cols} rows={rows} />;
  if (query.isError && query.data === undefined)
    return (
      <TableMessage cols={cols}>
        <ErrorState error={query.error} onRetry={() => void query.refetch()} />
      </TableMessage>
    );
  const data = query.data as T;
  if (isEmpty(data)) return <TableMessage cols={cols}>{typeof empty === 'string' ? <EmptyState title={empty} /> : empty}</TableMessage>;
  return <>{children(data)}</>;
}

/** UUIDs shortened to 8 chars, full value on hover. */
export function ShortId({ id }: { id: string | null | undefined }) {
  if (!id) return <span className="text-muted-foreground">—</span>;
  return (
    <Hint content={<span className="font-mono">{id}</span>}>
      <span className="font-mono text-[11px] text-muted-foreground">{id.slice(0, 8)}</span>
    </Hint>
  );
}

/** Works with `total` (numbered) or `has_more` (cursor-style) pages. */
export function Pagination({
  page,
  pageSize,
  count,
  total,
  hasMore,
  onPage,
  loading,
}: {
  page: number;
  pageSize: number;
  count: number;
  total?: number;
  hasMore?: boolean;
  onPage: (page: number) => void;
  loading?: boolean;
}) {
  const from = count ? (page - 1) * pageSize + 1 : 0;
  const to = (page - 1) * pageSize + count;
  const pages = total !== undefined ? Math.max(1, Math.ceil(total / pageSize)) : undefined;
  const canNext = pages !== undefined ? page < pages : !!hasMore;
  return (
    <div className="flex items-center justify-between gap-2 border-t px-3 py-2 text-[11px] text-muted-foreground">
      <span>
        {fmtInt(from)}–{fmtInt(to)}
        {total !== undefined && <> of {fmtInt(total)}</>}
        {pages !== undefined && <span className="hidden sm:inline"> · page {page} / {pages}</span>}
      </span>
      <div className="flex items-center gap-1">
        <Button variant="outline" size="sm" disabled={page <= 1 || loading} onClick={() => onPage(page - 1)} aria-label="Previous page">
          <ChevronLeft /> Prev
        </Button>
        <Button variant="outline" size="sm" disabled={!canNext || loading} onClick={() => onPage(page + 1)} aria-label="Next page">
          Next <ChevronRight />
        </Button>
      </div>
    </div>
  );
}
