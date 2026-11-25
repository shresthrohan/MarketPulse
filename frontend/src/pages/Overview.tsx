import { Activity, ArrowRight, Boxes, Gauge, Inbox, Layers } from 'lucide-react';
import { useState } from 'react';
import { Link } from 'react-router-dom';
import { PriceChart } from '@/charts/PriceChart';
import { ModeTag, rowMode } from '@/components/ModeTag';
import { ChangeText, FlashPrice } from '@/components/ChangeText';
import { QueryRows, ShortId } from '@/components/DataTable';
import { PageHeader } from '@/components/PageHeader';
import { RangeTabs } from '@/components/RangeTabs';
import { StatCard } from '@/components/StatCard';
import { StatusBadge } from '@/components/StatusBadge';
import { EmptyState, ErrorState, QueryState } from '@/components/States';
import { Card, CardHeader, CardTitle } from '@/components/ui/card';
import { NativeSelect } from '@/components/ui/input';
import { Skeleton } from '@/components/ui/skeleton';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { Hint } from '@/components/ui/tooltip';
import { MONITOR_REFETCH_MS, useHistory, useInstrumentUniverse, useMarketHistory, useMarketOverview, usePipelineOverview } from '@/hooks/queries';
import { useLiveRecent, useQuote, useWsStatus } from '@/hooks/useLive';
import { wsStatusText } from '@/services/ws';
import type { HistoryRange, InstrumentRow } from '@/types/api';
import { fmtCompact, fmtInt, fmtMs, fmtNum, fmtPrice, fmtTime, fmtVolume } from '@/utils/format';
import { DEFAULT_SYMBOL, HISTORY_RANGES } from '@/utils/constants';

export function OverviewPage() {
  return (
    <>
      <PageHeader title="Market overview" description="Pipeline throughput, market summary and the live tape. Times in UTC." />
      <InfraCards />
      <div className="grid gap-3 xl:grid-cols-[minmax(0,1fr)_360px]">
        <MainChart />
        <MarketSummary />
      </div>
      <Rankings />
      <div className="grid gap-3 2xl:grid-cols-2">
        <Majors />
        <LiveTape />
      </div>
      <RecentEvents />
    </>
  );
}

function InfraCards() {
  const q = usePipelineOverview();
  const overview = useMarketOverview();
  if (q.isError && !q.data)
    return (
      <Card>
        <ErrorState error={q.error} onRetry={() => void q.refetch()} className="py-4" />
      </Card>
    );
  const p = q.data;
  const loading = q.isPending;
  return (
    <div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
      <StatCard
        label="Instruments tracked"
        icon={Boxes}
        loading={loading}
        value={fmtInt(p?.instruments_tracked)}
        sub={overview.data ? `${fmtInt(overview.data.quotes_available)} with quotes` : undefined}
      />
      <StatCard
        label="Events received"
        icon={Inbox}
        loading={loading}
        hint="Sum over the pipeline overview window"
        value={fmtCompact(p?.events_received)}
        sub={p && `last ${p.window_hours}h · ${fmtNum(p.events_per_second, 1)} ev/s`}
      />
      <StatCard
        label="Events processed"
        icon={Layers}
        loading={loading}
        value={fmtCompact(p?.events_processed)}
        sub={p && `${fmtInt(p.events_failed)} failed · ${fmtInt(p.events_duplicate)} duplicate`}
      />
      <StatCard
        label="Avg processing latency"
        icon={Gauge}
        loading={loading}
        hint="Ingestion → committed, last 5 minutes"
        value={fmtMs(p?.avg_latency_ms)}
        sub={p && `max ${fmtMs(p.max_latency_ms)} · db write ${fmtMs(p.db_write_ms)}`}
      />
    </div>
  );
}

function MainChart() {
  const [symbol, setSymbol] = useState(DEFAULT_SYMBOL);
  const [range, setRange] = useState<HistoryRange>('1D');
  const universe = useInstrumentUniverse();
  const history = useHistory(symbol, range);
  const options = universe.data?.items.map((r) => r.symbol) ?? [symbol];
  return (
    <Card>
      <CardHeader>
        <div className="flex items-center gap-2">
          <CardTitle>Price · volume</CardTitle>
          <NativeSelect value={symbol} onChange={(e) => setSymbol(e.target.value)} aria-label="Chart instrument" className="h-7 w-24">
            {(options.includes(symbol) ? options : [symbol, ...options]).map((s) => (
              <option key={s}>{s}</option>
            ))}
          </NativeSelect>
          <Link to={`/instruments/${symbol}`} className="inline-flex items-center gap-1 text-[11px] text-secondary hover:underline">
            Details <ArrowRight className="size-3" />
          </Link>
        </div>
        <RangeTabs label="Chart range" value={range} options={HISTORY_RANGES} onChange={setRange} />
      </CardHeader>
      <QueryState query={history} skeleton={<Skeleton className="m-3 h-[300px] md:h-[380px]" />}>
        {(h) => <PriceChart symbol={symbol} bars={h.bars} interval={h.interval} live={range === '1D'} className="h-[300px] md:h-[380px]" />}
      </QueryState>
    </Card>
  );
}

function QuoteListRow({ row, show = 'pct' }: { row: InstrumentRow; show?: 'pct' | 'volume' }) {
  const { quote, live } = useQuote(row);
  return (
    <li>
      <Link to={`/instruments/${row.symbol}`} className="flex items-center gap-2 px-3 py-1.5 transition-colors hover:bg-panel/60">
        <span className="w-12 shrink-0 font-semibold">{row.symbol}</span>
        <ModeTag mode={rowMode(row)} />
        <span className="min-w-0 flex-1 truncate text-muted-foreground">{row.company_name}</span>
        <FlashPrice quote={quote} live={live} className="w-20 shrink-0 text-right" />
        {show === 'volume' ? (
          <span className="w-16 shrink-0 text-right">{fmtVolume(quote?.volume)}</span>
        ) : (
          <ChangeText value={quote?.change_pct} className="w-16 shrink-0 justify-end" />
        )}
      </Link>
    </li>
  );
}

function QuoteList({ rows, show, empty }: { rows: InstrumentRow[]; show?: 'pct' | 'volume'; empty: string }) {
  if (!rows.length) return <EmptyState title={empty} className="py-5" />;
  return (
    <ul className="divide-y">
      {rows.map((r) => (
        <QuoteListRow key={r.id} row={r} show={show} />
      ))}
    </ul>
  );
}

const listSkeleton = (
  <div className="space-y-2 p-3">
    {Array.from({ length: 5 }, (_, i) => (
      <Skeleton key={i} className="h-5" />
    ))}
  </div>
);

function MarketSummary() {
  const q = useMarketOverview();
  return (
    <Card className="flex flex-col">
      <CardHeader>
        <CardTitle>Indices (ETFs)</CardTitle>
      </CardHeader>
      <QueryState query={q} skeleton={listSkeleton}>
        {(o) => {
          const { advancers, decliners, unchanged } = o.breadth;
          const total = advancers + decliners + unchanged || 1;
          return (
            <>
              <QuoteList rows={o.indices} empty="No ETF instruments" />
              <div className="mt-auto border-t px-3 py-2.5">
                <div className="mb-1.5 flex justify-between text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
                  <span>Breadth</span>
                  <span>
                    <span className="text-positive">{advancers} adv</span> · <span className="text-negative">{decliners} dec</span> · {unchanged} unch
                  </span>
                </div>
                <div className="flex h-1.5 overflow-hidden rounded-full bg-panel" role="img" aria-label={`${advancers} advancers, ${decliners} decliners, ${unchanged} unchanged`}>
                  <div className="bg-positive" style={{ width: `${(advancers / total) * 100}%` }} />
                  <div className="bg-muted-foreground/40" style={{ width: `${(unchanged / total) * 100}%` }} />
                  <div className="bg-negative" style={{ width: `${(decliners / total) * 100}%` }} />
                </div>
              </div>
            </>
          );
        }}
      </QueryState>
    </Card>
  );
}

function Rankings() {
  const q = useMarketOverview();
  const lists = [
    { key: 'gainers', title: 'Top gainers', show: 'pct' },
    { key: 'losers', title: 'Top losers', show: 'pct' },
    { key: 'active', title: 'Most active', show: 'volume' },
  ] as const;
  return (
    <div className="grid gap-3 md:grid-cols-3">
      {lists.map((l) => (
        <Card key={l.key}>
          <CardHeader>
            <CardTitle>{l.title}</CardTitle>
            <span className="text-[10px] text-muted-foreground">{l.show === 'volume' ? 'day volume' : '% vs UTC-day open'}</span>
          </CardHeader>
          <QueryState query={q} skeleton={listSkeleton}>
            {(o) => <QuoteList rows={o[l.key]} show={l.show} empty="No quotes yet" />}
          </QueryState>
        </Card>
      ))}
    </div>
  );
}

function MajorRow({ row }: { row: InstrumentRow }) {
  const { quote, live } = useQuote(row);
  return (
    <TableRow>
      <TableCell className="font-semibold">
        <Link to={`/instruments/${row.symbol}`} className="hover:text-secondary">
          {row.symbol}
        </Link>
        <ModeTag mode={rowMode(row)} />
      </TableCell>
      <TableCell className="max-w-40 truncate text-muted-foreground">{row.company_name}</TableCell>
      <TableCell className="text-right">
        <FlashPrice quote={quote} live={live} />
      </TableCell>
      <TableCell className="text-right">
        <ChangeText value={quote?.change} kind="abs" />
      </TableCell>
      <TableCell className="text-right">
        <ChangeText value={quote?.change_pct} />
      </TableCell>
      <TableCell className="text-right text-muted-foreground">
        {fmtPrice(quote?.low)} – {fmtPrice(quote?.high)}
      </TableCell>
      <TableCell className="text-right">{fmtVolume(quote?.volume)}</TableCell>
    </TableRow>
  );
}

function Majors() {
  const q = useMarketOverview();
  const cols = 7;
  return (
    <Card>
      <CardHeader>
        <CardTitle>Major instruments (equities)</CardTitle>
      </CardHeader>
      <Table>
        <TableHeader>
          <TableRow className="hover:bg-transparent">
            <TableHead>Symbol</TableHead>
            <TableHead>Company</TableHead>
            <TableHead className="text-right">Last</TableHead>
            <TableHead className="text-right">Chg</TableHead>
            <TableHead className="text-right">% Chg</TableHead>
            <TableHead className="text-right">Day range</TableHead>
            <TableHead className="text-right">Volume</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          <QueryRows query={q} cols={cols} rows={6} isEmpty={(d) => !d.majors.length} empty="No equities tracked">
            {(d) => d.majors.map((r) => <MajorRow key={r.id} row={r} />)}
          </QueryRows>
        </TableBody>
      </Table>
    </Card>
  );
}

function LiveTape() {
  const recent = useLiveRecent();
  const ws = useWsStatus();
  return (
    <Card>
      <CardHeader>
        <CardTitle>Recent instrument updates</CardTitle>
        <span className="inline-flex items-center gap-1.5 text-[10px] text-muted-foreground">
          <Activity className="size-3" /> WebSocket quote stream
        </span>
      </CardHeader>
      {recent.length === 0 ? (
        <EmptyState title="Waiting for live quotes" description={wsStatusText(ws)} />
      ) : (
        <Table>
          <TableHeader>
            <TableRow className="hover:bg-transparent">
              <TableHead>Processed</TableHead>
              <TableHead>Symbol</TableHead>
              <TableHead className="text-right">Price</TableHead>
              <TableHead className="text-right">Tick</TableHead>
              <TableHead className="text-right">% Day</TableHead>
              <TableHead>Mode</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {recent.slice(0, 12).map((q) => (
              <TableRow key={`${q.event_id}-${q.received_at}`} className="animate-fade-in">
                <TableCell className="text-muted-foreground">{fmtTime(q.processed_at)}</TableCell>
                <TableCell className="font-semibold">
                  <Link to={`/instruments/${q.symbol}`} className="hover:text-secondary">
                    {q.symbol}
                  </Link>
                </TableCell>
                <TableCell className="text-right">{fmtPrice(q.price)}</TableCell>
                <TableCell className="text-right">
                  <ChangeText value={q.tick_change_pct} />
                </TableCell>
                <TableCell className="text-right">
                  <ChangeText value={q.change_pct} />
                </TableCell>
                <TableCell>
                  <span className="text-[10px] uppercase text-muted-foreground">{q.data_mode}</span>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      )}
    </Card>
  );
}

function RecentEvents() {
  const q = useMarketHistory({ page_size: 15 }, MONITOR_REFETCH_MS);
  const cols = 8;
  return (
    <Card>
      <CardHeader>
        <CardTitle>Recent market events</CardTitle>
        <Link to="/explorer" className="inline-flex items-center gap-1 text-[11px] text-secondary hover:underline">
          Data explorer <ArrowRight className="size-3" />
        </Link>
      </CardHeader>
      <Table>
        <TableHeader>
          <TableRow className="hover:bg-transparent">
            <TableHead>Event</TableHead>
            <TableHead>Symbol</TableHead>
            <TableHead>Event time</TableHead>
            <TableHead className="text-right">Price</TableHead>
            <TableHead className="text-right">Volume</TableHead>
            <TableHead>Source</TableHead>
            <TableHead>Status</TableHead>
            <TableHead className="text-right">Latency</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          <QueryRows query={q} cols={cols} rows={6} isEmpty={(d) => !d.items.length} empty="No market events stored yet">
            {(d) =>
              d.items.map((e) => (
              <TableRow key={e.event_id}>
                <TableCell>
                  <ShortId id={e.event_id} />
                </TableCell>
                <TableCell className="font-semibold">{e.symbol}</TableCell>
                <TableCell className="text-muted-foreground">{fmtTime(e.event_timestamp)}</TableCell>
                <TableCell className="text-right">{fmtPrice(e.price)}</TableCell>
                <TableCell className="text-right">{fmtVolume(e.volume)}</TableCell>
                <TableCell className="text-muted-foreground">{e.source}</TableCell>
                <TableCell>
                  <Hint content={`${e.attempts} attempt(s)`}>
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
    </Card>
  );
}
