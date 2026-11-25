import { Download, FilterX, RotateCw } from 'lucide-react';
import { useState, type ReactNode } from 'react';
import { useSearchParams } from 'react-router-dom';
import { Pagination, QueryRows, ShortId } from '@/components/DataTable';
import { PageHeader } from '@/components/PageHeader';
import { StatusBadge } from '@/components/StatusBadge';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card, CardContent } from '@/components/ui/card';
import { Input, NativeSelect } from '@/components/ui/input';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { Hint } from '@/components/ui/tooltip';
import { useInstrumentUniverse, useMarketHistory, useMarketOverview } from '@/hooks/queries';
import { useDebounced } from '@/hooks/useDebounced';
import { marketHistoryCsvUrl } from '@/services/api';
import type { MarketEventStatus, MarketHistoryQuery } from '@/types/api';
import { cn } from '@/utils/cn';
import { fmtDateTime, fmtMs, fmtPrice, fmtVolume, localInputToIso } from '@/utils/format';

const PAGE_SIZE = 50;
const COLS = 9;
const EMPTY = { symbol: '', source: '', status: '', start: '', end: '', min: '', max: '', search: '' };
type Filters = typeof EMPTY;

const num = (v: string) => (v.trim() === '' || !Number.isFinite(Number(v)) ? undefined : Number(v));

function Field({ label, children, className }: { label: string; children: ReactNode; className?: string }) {
  return (
    <label className={cn('grid gap-1 text-[10px] font-semibold uppercase tracking-wider text-muted-foreground', className)}>
      {label}
      {children}
    </label>
  );
}

export function ExplorerPage() {
  const [params] = useSearchParams();
  const [filters, setFilters] = useState<Filters>(() => ({ ...EMPTY, symbol: params.get('symbol')?.toUpperCase() ?? '' }));
  const [page, setPage] = useState(1);
  const f = useDebounced(filters, 350);
  const universe = useInstrumentUniverse();
  const overview = useMarketOverview();

  const set = (k: keyof Filters) => (e: { target: { value: string } }) => {
    setFilters((prev) => ({ ...prev, [k]: e.target.value }));
    setPage(1);
  };

  const query: MarketHistoryQuery = {
    symbol: f.symbol || undefined,
    source: f.source || undefined,
    status: (f.status || undefined) as MarketEventStatus | undefined,
    start: localInputToIso(f.start),
    end: localInputToIso(f.end),
    min_price: num(f.min),
    max_price: num(f.max),
    search: f.search.trim() || undefined,
  };
  const q = useMarketHistory({ ...query, page, page_size: PAGE_SIZE });
  const badRange = query.min_price !== undefined && query.max_price !== undefined && query.min_price > query.max_price;

  return (
    <>
      <PageHeader
        title="Data explorer"
        description="Stored market events, newest first. Date inputs are UTC."
        actions={
          <>
            <Button variant="outline" size="sm" onClick={() => void q.refetch()} disabled={q.isFetching}>
              <RotateCw className={cn(q.isFetching && 'animate-spin')} /> Refresh
            </Button>
            <Button asChild size="sm" variant="secondary">
              <a href={marketHistoryCsvUrl(query)} download>
                <Download /> Export CSV
              </a>
            </Button>
          </>
        }
      />
      <Card>
        <CardContent className="grid grid-cols-2 gap-3 border-b sm:grid-cols-4">
          <Field label="Symbol">
            <NativeSelect value={filters.symbol} onChange={set('symbol')}>
              <option value="">All</option>
              {(universe.data?.items ?? []).map((r) => (
                <option key={r.id}>{r.symbol}</option>
              ))}
              {filters.symbol && !universe.data?.items.some((r) => r.symbol === filters.symbol) && <option>{filters.symbol}</option>}
            </NativeSelect>
          </Field>
          <Field label="Source">
            <NativeSelect value={filters.source} onChange={set('source')}>
              <option value="">All</option>
              {(overview.data?.sources ?? []).map((s) => (
                <option key={s}>{s}</option>
              ))}
            </NativeSelect>
          </Field>
          <Field label="Status">
            <NativeSelect value={filters.status} onChange={set('status')}>
              <option value="">All</option>
              <option value="valid">Valid</option>
              <option value="retried">Retried</option>
            </NativeSelect>
          </Field>
          <Field label="Event ID prefix">
            <Input value={filters.search} onChange={set('search')} placeholder="e.g. 3f2a" className="font-mono" />
          </Field>
          <Field label="From (UTC)">
            <Input type="datetime-local" value={filters.start} onChange={set('start')} />
          </Field>
          <Field label="To (UTC)">
            <Input type="datetime-local" value={filters.end} onChange={set('end')} />
          </Field>
          <Field label="Min price">
            <Input type="number" inputMode="decimal" min={0} step="any" value={filters.min} onChange={set('min')} aria-invalid={badRange} />
          </Field>
          <Field label="Max price">
            <Input type="number" inputMode="decimal" min={0} step="any" value={filters.max} onChange={set('max')} aria-invalid={badRange} />
          </Field>
        </CardContent>
        <div className="flex flex-wrap items-center gap-2 border-b px-3 py-1.5 text-[11px] text-muted-foreground">
          {badRange ? <span className="text-warning">Min price is above max price.</span> : <span>CSV export uses the current filters (max 50,000 rows).</span>}
          <Button variant="ghost" size="sm" className="ml-auto" onClick={() => {
              setFilters(EMPTY);
              setPage(1);
            }} disabled={JSON.stringify(filters) === JSON.stringify(EMPTY)}>
            <FilterX /> Clear filters
          </Button>
        </div>
        <Table className={cn(q.isPlaceholderData && 'opacity-60 transition-opacity')}>
          <TableHeader>
            <TableRow className="hover:bg-transparent">
              <TableHead>Event ID</TableHead>
              <TableHead>Symbol</TableHead>
              <TableHead>Event time (UTC)</TableHead>
              <TableHead className="text-right">Price</TableHead>
              <TableHead className="text-right">Volume</TableHead>
              <TableHead>Source</TableHead>
              <TableHead>Data mode</TableHead>
              <TableHead>Validation</TableHead>
              <TableHead className="text-right">Latency</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            <QueryRows query={q} cols={COLS} rows={12} isEmpty={(d) => !d.items.length} empty="No events match these filters">
              {(d) =>
                d.items.map((e) => (
                  <TableRow key={e.event_id}>
                    <TableCell>
                      <ShortId id={e.event_id} />
                    </TableCell>
                    <TableCell className="font-semibold">{e.symbol}</TableCell>
                    <TableCell className="text-muted-foreground">{fmtDateTime(e.event_timestamp)}</TableCell>
                    <TableCell className="text-right font-medium">{fmtPrice(e.price)}</TableCell>
                    <TableCell className="text-right">{fmtVolume(e.volume)}</TableCell>
                    <TableCell className="text-muted-foreground">{e.source}</TableCell>
                    <TableCell>
                      <Badge variant={e.data_mode === 'live' ? 'positive' : 'warning'}>{e.data_mode}</Badge>
                    </TableCell>
                    <TableCell>
                      <Hint content={`${e.attempts} attempt(s) · ingested ${fmtDateTime(e.ingested_at)} · processed ${fmtDateTime(e.processed_at)}`}>
                        <StatusBadge status={e.processing_status} />
                      </Hint>
                    </TableCell>
                    <TableCell className="text-right">{fmtMs(e.latency_ms)}</TableCell>
                  </TableRow>
                ))
              }
            </QueryRows>
          </TableBody>
        </Table>
        {q.data && (
          <Pagination page={page} pageSize={PAGE_SIZE} count={q.data.items.length} hasMore={q.data.has_more} onPage={setPage} loading={q.isPlaceholderData} />
        )}
      </Card>
    </>
  );
}
