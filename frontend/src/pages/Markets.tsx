import { Search } from 'lucide-react';
import { useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { AddToWatchlistButton } from '@/components/AddToWatchlist';
import { ChangeText, FlashPrice } from '@/components/ChangeText';
import { Pagination, QueryRows, SortHead } from '@/components/DataTable';
import { PageHeader } from '@/components/PageHeader';
import { ModeTag, rowMode } from '@/components/ModeTag';
import { EmptyState } from '@/components/States';
import { Badge } from '@/components/ui/badge';
import { Card, CardHeader } from '@/components/ui/card';
import { Input, NativeSelect } from '@/components/ui/input';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { useInstrumentUniverse, useInstruments } from '@/hooks/queries';
import { useDebounced } from '@/hooks/useDebounced';
import { useQuote } from '@/hooks/useLive';
import type { AssetType, InstrumentRow, InstrumentSort, SortOrder } from '@/types/api';
import { cn } from '@/utils/cn';
import { fmtDateTime, fmtVolume } from '@/utils/format';

const PAGE_SIZE = 25;
const COLS = 9;

function MarketRow({ row }: { row: InstrumentRow }) {
  const navigate = useNavigate();
  const { quote, live } = useQuote(row);
  return (
    <TableRow className="cursor-pointer" onClick={() => navigate(`/instruments/${row.symbol}`)}>
      <TableCell>
        <Link to={`/instruments/${row.symbol}`} onClick={(e) => e.stopPropagation()} className="font-semibold hover:text-secondary">
          {row.symbol}
        </Link>
        <ModeTag mode={rowMode(row)} />
        {row.asset_type === 'etf' && (
          <Badge variant="info" className="ml-1.5">
            ETF
          </Badge>
        )}
      </TableCell>
      <TableCell className="max-w-56 truncate text-muted-foreground">{row.company_name}</TableCell>
      <TableCell className="text-right font-medium">
        <FlashPrice quote={quote} live={live} />
      </TableCell>
      <TableCell className="text-right">
        <ChangeText value={quote?.change} kind="abs" />
      </TableCell>
      <TableCell className="text-right">
        <ChangeText value={quote?.change_pct} arrow />
      </TableCell>
      <TableCell className="text-right">{fmtVolume(quote?.volume)}</TableCell>
      <TableCell className="text-muted-foreground">{row.exchange}</TableCell>
      <TableCell className="text-muted-foreground">{quote ? fmtDateTime(quote.processed_at) : 'no quote yet'}</TableCell>
      <TableCell className="w-10 px-1 text-right">
        <AddToWatchlistButton symbol={row.symbol} />
      </TableCell>
    </TableRow>
  );
}

export function MarketsPage() {
  const [search, setSearch] = useState('');
  const [assetType, setAssetType] = useState<AssetType | ''>('');
  const [exchange, setExchange] = useState('');
  const [sort, setSort] = useState<InstrumentSort>('symbol');
  const [order, setOrder] = useState<SortOrder>('asc');
  const [page, setPage] = useState(1);
  const term = useDebounced(search.trim(), 300);

  const q = useInstruments({
    search: term || undefined,
    asset_type: assetType || undefined,
    exchange: exchange || undefined,
    sort,
    order,
    page,
    page_size: PAGE_SIZE,
  });
  const universe = useInstrumentUniverse();
  const exchanges = [...new Set(universe.data?.items.map((r) => r.exchange) ?? [])].sort();

  const onSort = (field: InstrumentSort) => {
    if (field === sort) setOrder((o) => (o === 'asc' ? 'desc' : 'asc'));
    else {
      setSort(field);
      setOrder(field === 'symbol' || field === 'company_name' ? 'asc' : 'desc');
    }
    setPage(1);
  };
  const head = (label: string, field: InstrumentSort, className?: string) => (
    <SortHead label={label} field={field} sort={sort} order={order} onSort={onSort} className={className} />
  );

  return (
    <>
      <PageHeader title="Markets" description="Instrument screener. Sorting, filtering and paging run server-side; prices update live over the WebSocket." />
      <Card>
        <CardHeader className="justify-start">
          <div className="relative w-full sm:w-64">
            <Search className="pointer-events-none absolute left-2.5 top-1/2 size-3.5 -translate-y-1/2 text-muted-foreground" aria-hidden />
            <Input
              value={search}
              onChange={(e) => {
                setSearch(e.target.value);
                setPage(1);
              }}
              placeholder="Filter symbol or company"
              aria-label="Filter instruments"
              className="pl-8"
            />
          </div>
          <NativeSelect
            value={assetType}
            onChange={(e) => {
              setAssetType(e.target.value as AssetType | '');
              setPage(1);
            }}
            aria-label="Asset type"
          >
            <option value="">All asset types</option>
            <option value="equity">Equity</option>
            <option value="etf">ETF</option>
            <option value="crypto">Crypto</option>
          </NativeSelect>
          <NativeSelect
            value={exchange}
            onChange={(e) => {
              setExchange(e.target.value);
              setPage(1);
            }}
            aria-label="Exchange"
          >
            <option value="">All exchanges</option>
            {exchanges.map((x) => (
              <option key={x}>{x}</option>
            ))}
          </NativeSelect>
        </CardHeader>
        <Table className={cn(q.isPlaceholderData && 'opacity-60 transition-opacity')}>
          <TableHeader>
            <TableRow className="hover:bg-transparent">
              {head('Symbol', 'symbol')}
              {head('Company', 'company_name')}
              {head('Last', 'price', 'text-right')}
              {head('Chg', 'change', 'text-right')}
              {head('% Chg', 'change_pct', 'text-right')}
              {head('Volume', 'volume', 'text-right')}
              <TableHead>Exchange</TableHead>
              {head('Last updated', 'updated')}
              <TableHead>
                <span className="sr-only">Watchlist</span>
              </TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            <QueryRows
              query={q}
              cols={COLS}
              rows={10}
              isEmpty={(d) => !d.items.length}
              empty={<EmptyState title="No instruments match" description="Try a different search or clear the filters." />}
            >
              {(d) => d.items.map((r) => <MarketRow key={r.id} row={r} />)}
            </QueryRows>
          </TableBody>
        </Table>
        {q.data && (
          <Pagination page={page} pageSize={PAGE_SIZE} count={q.data.items.length} total={q.data.total} onPage={setPage} loading={q.isPlaceholderData} />
        )}
      </Card>
    </>
  );
}
