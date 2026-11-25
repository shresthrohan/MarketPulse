import { ChevronDown, ChevronRight, Search } from 'lucide-react';
import { Fragment, useState } from 'react';
import { Pagination, QueryRows, ShortId } from '@/components/DataTable';
import { PageHeader } from '@/components/PageHeader';
import { RangeTabs } from '@/components/RangeTabs';
import { StatusBadge } from '@/components/StatusBadge';
import { Card, CardHeader } from '@/components/ui/card';
import { Input, NativeSelect } from '@/components/ui/input';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { Hint } from '@/components/ui/tooltip';
import { useInstrumentUniverse, usePipelineEvents } from '@/hooks/queries';
import { useDebounced } from '@/hooks/useDebounced';
import type { EventsView, PipelineEvent, Severity } from '@/types/api';
import { cn } from '@/utils/cn';
import { fmtDateTime } from '@/utils/format';

const VIEWS = ['all', 'success', 'failed', 'retried'] as const satisfies readonly EventsView[];
const VIEW_LABEL: Record<EventsView, string> = { all: 'All', success: 'Successful', failed: 'Failed', retried: 'Retried' };
const PAGE_SIZE = 50;
const COLS = 8;

// Stable across 5 s refetches (no index), so an expanded row stays expanded as new events arrive.
const rowKey = (e: PipelineEvent) => `${e.event_id ?? 'none'}-${e.timestamp}-${e.event_type}`;

export function LogsPage() {
  const [view, setView] = useState<EventsView>('all');
  const [search, setSearch] = useState('');
  const [symbol, setSymbol] = useState('');
  const [severity, setSeverity] = useState<Severity | ''>('');
  const [page, setPage] = useState(1);
  const [expanded, setExpanded] = useState<Set<string>>(new Set());
  const term = useDebounced(search.trim(), 300);
  const universe = useInstrumentUniverse();

  const q = usePipelineEvents({ view, search: term || undefined, symbol: symbol || undefined, severity: severity || undefined, page, page_size: PAGE_SIZE });

  const reset = <T,>(setter: (v: T) => void) => (v: T) => {
    setter(v);
    setPage(1);
    setExpanded(new Set());
  };
  const toggle = (k: string) =>
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(k)) next.delete(k);
      else next.add(k);
      return next;
    });

  return (
    <>
      <PageHeader title="Event logs" description="Stored ticks and rejected events from the processor. Refreshes every 5 s." />
      <Card>
        <CardHeader className="justify-start">
          <RangeTabs label="Event view" value={view} options={VIEWS} onChange={reset(setView)} format={(v) => VIEW_LABEL[v]} />
          <div className="relative w-full sm:w-64">
            <Search className="pointer-events-none absolute left-2.5 top-1/2 size-3.5 -translate-y-1/2 text-muted-foreground" aria-hidden />
            <Input
              value={search}
              onChange={(e) => reset(setSearch)(e.target.value)}
              placeholder="Event ID or message"
              aria-label="Search events"
              className="pl-8"
            />
          </div>
          <NativeSelect value={symbol} onChange={(e) => reset(setSymbol)(e.target.value)} aria-label="Symbol">
            <option value="">All symbols</option>
            {(universe.data?.items ?? []).map((r) => (
              <option key={r.id}>{r.symbol}</option>
            ))}
          </NativeSelect>
          <NativeSelect value={severity} onChange={(e) => reset(setSeverity)(e.target.value as Severity | '')} aria-label="Severity">
            <option value="">All severities</option>
            <option value="info">Info</option>
            <option value="warning">Warning</option>
            <option value="error">Error</option>
          </NativeSelect>
        </CardHeader>
        <Table className={cn(q.isPlaceholderData && 'opacity-60 transition-opacity')}>
          <TableHeader>
            <TableRow className="hover:bg-transparent">
              <TableHead>Timestamp (UTC)</TableHead>
              <TableHead>Event ID</TableHead>
              <TableHead>Source</TableHead>
              <TableHead>Symbol</TableHead>
              <TableHead>Event type</TableHead>
              <TableHead>Severity</TableHead>
              <TableHead>Processing</TableHead>
              <TableHead>Error details</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            <QueryRows query={q} cols={COLS} rows={12} isEmpty={(d) => !d.items.length} empty="No events match">
              {(d) =>
                d.items.map((e) => {
                  const k = rowKey(e);
                  const open = expanded.has(k);
                  return (
                    <Fragment key={k}>
                      <TableRow className={cn(e.severity === 'error' && 'bg-negative/[0.04]')}>
                        <TableCell className="text-muted-foreground">{fmtDateTime(e.timestamp)}</TableCell>
                        <TableCell>
                          <ShortId id={e.event_id} />
                        </TableCell>
                        <TableCell className="text-muted-foreground">{e.source ?? '—'}</TableCell>
                        <TableCell className="font-semibold">{e.symbol ?? '—'}</TableCell>
                        <TableCell className="font-mono text-[11px]">{e.event_type}</TableCell>
                        <TableCell>
                          <StatusBadge status={e.severity} />
                        </TableCell>
                        <TableCell>
                          <StatusBadge status={e.processing_status} />
                        </TableCell>
                        <TableCell className="max-w-80">
                          {e.error_details ? (
                            <button
                              type="button"
                              onClick={() => toggle(k)}
                              aria-expanded={open}
                              className="flex w-full min-w-0 cursor-pointer items-center gap-1 text-left text-muted-foreground hover:text-foreground"
                            >
                              {open ? <ChevronDown className="size-3 shrink-0" /> : <ChevronRight className="size-3 shrink-0" />}
                              <Hint asChild content={open ? undefined : e.error_details}>
                                <span className="block truncate">{e.error_details}</span>
                              </Hint>
                            </button>
                          ) : (
                            <span className="text-muted-foreground">—</span>
                          )}
                        </TableCell>
                      </TableRow>
                      {open && (
                        <TableRow className="hover:bg-transparent">
                          <TableCell colSpan={COLS} className="h-auto whitespace-pre-wrap break-words bg-panel/60 py-2 font-mono text-[11px]">
                            {e.error_details}
                          </TableCell>
                        </TableRow>
                      )}
                    </Fragment>
                  );
                })
              }
            </QueryRows>
          </TableBody>
        </Table>
        {q.data && (
          <Pagination page={page} pageSize={PAGE_SIZE} count={q.data.items.length} hasMore={q.data.has_more} onPage={(p) => {
              setPage(p);
              setExpanded(new Set());
            }} loading={q.isPlaceholderData} />
        )}
      </Card>
    </>
  );
}
