import { ArrowRight } from 'lucide-react';
import { useMemo, useState, type ReactNode } from 'react';
import { Link, Navigate, useNavigate, useParams } from 'react-router-dom';
import { seriesColor } from '@/charts/colors';
import { PriceChart, type Overlay } from '@/charts/PriceChart';
import { ModeTag, rowMode } from '@/components/ModeTag';
import { AddToWatchlistButton } from '@/components/AddToWatchlist';
import { ChangeText, FlashPrice } from '@/components/ChangeText';
import { QueryRows, ShortId } from '@/components/DataTable';
import { InstrumentSearch } from '@/components/InstrumentSearch';
import { RangeTabs } from '@/components/RangeTabs';
import { StatusBadge } from '@/components/StatusBadge';
import { EmptyState, ErrorState, QueryState } from '@/components/States';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Skeleton } from '@/components/ui/skeleton';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { Hint } from '@/components/ui/tooltip';
import { MONITOR_REFETCH_MS, useHistory, useInstrument, useInstrumentUniverse, useMarketHistory, useMovingAverages, useStatistics } from '@/hooks/queries';
import { useQuote } from '@/hooks/useLive';
import { ApiError } from '@/services/api';
import type { AnalyticsRange, HistoryRange, InstrumentRow, Statistics } from '@/types/api';
import { DEFAULT_SYMBOL, HISTORY_RANGES } from '@/utils/constants';
import { fmtDate, fmtDateTime, fmtFrac, fmtInt, fmtMs, fmtPrice, fmtTime, fmtVolume } from '@/utils/format';

/** /instruments with no symbol: open the default instrument, or the first one if it isn't tracked. */
export function InstrumentIndex() {
  const universe = useInstrumentUniverse();
  if (universe.isPending) return <Skeleton className="h-40" />;
  if (universe.isError)
    return (
      <Card>
        <ErrorState error={universe.error} onRetry={() => void universe.refetch()} />
      </Card>
    );
  const items = universe.data.items;
  if (!items.length) return <EmptyState title="No instruments tracked yet" />;
  const target = items.some((i) => i.symbol === DEFAULT_SYMBOL) ? DEFAULT_SYMBOL : items[0].symbol;
  return <Navigate to={`/instruments/${target}`} replace />;
}

export function InstrumentPage() {
  const symbol = (useParams().symbol ?? '').toUpperCase();
  const navigate = useNavigate();
  const inst = useInstrument(symbol);

  if (inst.isPending)
    return (
      <div className="space-y-3">
        <Skeleton className="h-20" />
        <Skeleton className="h-[420px]" />
      </div>
    );
  if (inst.isError && !inst.data) {
    if (inst.error instanceof ApiError && inst.error.status === 404)
      return (
        <Card>
          <EmptyState
            title={`Unknown instrument “${symbol}”`}
            description={inst.error.detail}
            action={<InstrumentSearch className="mt-2 w-72" onSelect={(s) => navigate(`/instruments/${s}`)} />}
          />
        </Card>
      );
    return (
      <Card>
        <ErrorState error={inst.error} onRetry={() => void inst.refetch()} />
      </Card>
    );
  }

  return (
    <>
      <InstrumentHeader row={inst.data} />
      <div className="grid gap-3 xl:grid-cols-[minmax(0,1fr)_320px]">
        <ChartPanel symbol={symbol} />
        <StatsPanel symbol={symbol} />
      </div>
      <EventsPanel symbol={symbol} />
    </>
  );
}

function Fact({ label, children }: { label: string; children: ReactNode }) {
  return (
    <div className="min-w-0">
      <div className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">{label}</div>
      <div className="truncate">{children}</div>
    </div>
  );
}

function InstrumentHeader({ row }: { row: InstrumentRow }) {
  const { quote, live } = useQuote(row);
  return (
    <Card>
      <CardContent className="flex flex-wrap items-center gap-x-8 gap-y-3">
        <div className="min-w-0">
          <div className="flex flex-wrap items-center gap-2">
            <h1 className="text-xl font-semibold tracking-tight">{row.symbol}</h1>
            <Badge variant="outline">{row.exchange}</Badge>
            <Badge variant={row.asset_type === 'etf' ? 'info' : 'default'}>{row.asset_type}</Badge>
            <Badge variant="outline">{row.currency}</Badge>
            <ModeTag mode={rowMode({ data_mode: row.data_mode, quote })} always />
          </div>
          <p className="mt-0.5 truncate text-xs text-muted-foreground">{row.company_name}</p>
        </div>
        <div className="flex items-baseline gap-3">
          <FlashPrice quote={quote} live={live} className="text-2xl font-semibold" />
          <ChangeText value={quote?.change} kind="abs" className="text-sm" />
          <ChangeText value={quote?.change_pct} arrow className="text-sm font-medium" />
        </div>
        <div className="grid grid-cols-3 gap-x-6 gap-y-2 sm:grid-cols-6">
          <Fact label="Open">{fmtPrice(quote?.open)}</Fact>
          <Fact label="High">{fmtPrice(quote?.high)}</Fact>
          <Fact label="Low">{fmtPrice(quote?.low)}</Fact>
          <Fact label="Day vol">{fmtVolume(quote?.volume)}</Fact>
          <Fact label={`Avg ${quote?.rolling_window ?? ''} ticks`}>{fmtPrice(quote?.rolling_avg)}</Fact>
          <Fact label="Processed">
            <Hint content={quote ? `Event ${quote.event_id} · source ${quote.source}` : undefined}>{fmtTime(quote?.processed_at)}</Hint>
          </Fact>
        </div>
        <div className="ml-auto">
          <AddToWatchlistButton symbol={row.symbol} label />
        </div>
      </CardContent>
    </Card>
  );
}

/** Moving-average overlay ranges: analytics accepts 1M/3M/6M/1Y, so only 1M/3M/1Y map across. */
const MA_RANGE: Partial<Record<HistoryRange, AnalyticsRange>> = { '1M': '1M', '3M': '3M', '1Y': '1Y' };

function ChartPanel({ symbol }: { symbol: string }) {
  const [range, setRange] = useState<HistoryRange>('1D');
  const [showMa, setShowMa] = useState(true);
  const history = useHistory(symbol, range);
  const maRange = MA_RANGE[range];
  const ma = useMovingAverages(symbol, maRange ?? '1M', undefined, undefined, !!maRange && showMa);
  const overlays = useMemo<Overlay[] | undefined>(() => {
    if (!maRange || !showMa || !ma.data) return undefined;
    return [
      { id: 'sma', label: `SMA ${ma.data.sma_window}`, color: seriesColor(0), data: ma.data.sma },
      { id: 'ema', label: `EMA ${ma.data.ema_span}`, color: seriesColor(1), data: ma.data.ema },
    ];
  }, [maRange, showMa, ma.data]);

  return (
    <Card>
      <CardHeader>
        <div className="flex items-center gap-2">
          <CardTitle>OHLCV</CardTitle>
          {history.data && <span className="text-[10px] text-muted-foreground">{history.data.interval} bars · UTC</span>}
        </div>
        <div className="flex items-center gap-2">
          {maRange && (
            <Button size="sm" variant={showMa ? 'secondary' : 'outline'} aria-pressed={showMa} onClick={() => setShowMa((v) => !v)}>
              SMA / EMA
            </Button>
          )}
          <RangeTabs label="Chart range" value={range} options={HISTORY_RANGES} onChange={setRange} />
        </div>
      </CardHeader>
      {ma.isError && maRange && showMa && (
        <p className="border-b px-3 py-1 text-[11px] text-warning">Moving averages unavailable: {ma.error.message}</p>
      )}
      <QueryState query={history} skeleton={<Skeleton className="m-3 h-[320px] md:h-[420px]" />}>
        {(h) => (
          <PriceChart symbol={symbol} bars={h.bars} interval={h.interval} live={range === '1D'} overlays={overlays} className="h-[320px] md:h-[420px]" />
        )}
      </QueryState>
    </Card>
  );
}

function StatRow({ label, children, hint }: { label: string; children: ReactNode; hint?: string }) {
  return (
    <div className="flex items-center justify-between gap-3 py-1.5">
      <Hint content={hint}>
        <span className="text-muted-foreground">{label}</span>
      </Hint>
      <span className="text-right font-medium">{children}</span>
    </div>
  );
}

function StatsBody({ s }: { s: Statistics }) {
  const dd = s.max_drawdown_1y;
  return (
    <div className="divide-y px-3 py-1">
      <StatRow label="Last price">{fmtPrice(s.last_price)}</StatRow>
      <StatRow label="52W high">{fmtPrice(s.high_52w)}</StatRow>
      <StatRow label="52W low">{fmtPrice(s.low_52w)}</StatRow>
      <StatRow label="Volatility 30D" hint="Annualized sample std of daily returns × √365">
        {fmtFrac(s.volatility_30d, { signed: false })}
      </StatRow>
      {(['1W', '1M', '3M', '1Y'] as const).map((k) => (
        <StatRow key={k} label={`Return ${k}`}>
          <ChangeText value={s.returns[k]} kind="frac" />
        </StatRow>
      ))}
      <StatRow
        label="Max drawdown 1Y"
        hint={dd ? `Peak ${fmtDate(dd.peak_at)} → trough ${fmtDate(dd.trough_at)} · ${dd.recovered_at ? `recovered ${fmtDate(dd.recovered_at)}` : 'not recovered'}` : undefined}
      >
        <ChangeText value={dd?.value} kind="frac" />
      </StatRow>
      <StatRow label="SMA 20">{fmtPrice(s.sma_20)}</StatRow>
      <StatRow label="SMA 50">{fmtPrice(s.sma_50)}</StatRow>
      <StatRow label="EMA 20">{fmtPrice(s.ema_20)}</StatRow>
      <StatRow label="Avg volume 30D">{fmtVolume(s.avg_volume_30d)}</StatRow>
      <StatRow label="Observations" hint="Daily closes used">
        {fmtInt(s.observations)}
      </StatRow>
    </div>
  );
}

function StatsPanel({ symbol }: { symbol: string }) {
  const q = useStatistics(symbol);
  return (
    <Card>
      <CardHeader>
        <CardTitle>Statistics</CardTitle>
        {q.data && <span className="text-[10px] text-muted-foreground">as of {fmtDateTime(q.data.as_of)}</span>}
      </CardHeader>
      <QueryState
        query={q}
        skeleton={
          <div className="space-y-2.5 p-3">
            {Array.from({ length: 12 }, (_, i) => (
              <Skeleton key={i} className="h-4" />
            ))}
          </div>
        }
      >
        {(s) => <StatsBody s={s} />}
      </QueryState>
    </Card>
  );
}

function EventsPanel({ symbol }: { symbol: string }) {
  const q = useMarketHistory({ symbol, page_size: 20 }, MONITOR_REFETCH_MS);
  const cols = 9;
  return (
    <Card>
      <CardHeader>
        <CardTitle>Latest events · {symbol}</CardTitle>
        <Link to={`/explorer?symbol=${symbol}`} className="inline-flex items-center gap-1 text-[11px] text-secondary hover:underline">
          Open in Data Explorer <ArrowRight className="size-3" />
        </Link>
      </CardHeader>
      <Table>
        <TableHeader>
          <TableRow className="hover:bg-transparent">
            <TableHead>Event</TableHead>
            <TableHead>Event time</TableHead>
            <TableHead className="text-right">Price</TableHead>
            <TableHead className="text-right">Volume</TableHead>
            <TableHead className="text-right">Open</TableHead>
            <TableHead className="text-right">High</TableHead>
            <TableHead className="text-right">Low</TableHead>
            <TableHead>Status</TableHead>
            <TableHead className="text-right">Latency</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          <QueryRows query={q} cols={cols} rows={6} isEmpty={(d) => !d.items.length} empty={`No stored events for ${symbol} yet`}>
            {(d) =>
              d.items.map((e) => (
              <TableRow key={e.event_id}>
                <TableCell>
                  <ShortId id={e.event_id} />
                </TableCell>
                <TableCell className="text-muted-foreground">{fmtDateTime(e.event_timestamp)}</TableCell>
                <TableCell className="text-right font-medium">{fmtPrice(e.price)}</TableCell>
                <TableCell className="text-right">{fmtVolume(e.volume)}</TableCell>
                <TableCell className="text-right text-muted-foreground">{fmtPrice(e.open)}</TableCell>
                <TableCell className="text-right text-muted-foreground">{fmtPrice(e.high)}</TableCell>
                <TableCell className="text-right text-muted-foreground">{fmtPrice(e.low)}</TableCell>
                <TableCell>
                  <StatusBadge status={e.processing_status} />
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
