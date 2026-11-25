import { useEffect, useMemo, useState, type ReactNode } from 'react';
import { seriesColor } from '@/charts/colors';
import { LineChart, type SeriesSpec } from '@/charts/LineChart';
import { ChangeText } from '@/components/ChangeText';
import { PageHeader } from '@/components/PageHeader';
import { RangeTabs } from '@/components/RangeTabs';
import { QueryCard } from '@/components/States';
import { SymbolMultiSelect } from '@/components/SymbolMultiSelect';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Input, NativeSelect } from '@/components/ui/input';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { Hint } from '@/components/ui/tooltip';
import {
  useComparison,
  useCorrelation,
  useDrawdown,
  useInstrumentUniverse,
  useMovingAverages,
  useReturns,
  useVolatility,
  useVolumeTrend,
} from '@/hooks/queries';
import { useDebounced } from '@/hooks/useDebounced';
import type { AnalyticsRange, CorrelationResponse, ReturnMethod, SymbolSeries } from '@/types/api';
import { ANALYTICS_RANGES } from '@/utils/constants';
import { fmtCompact, fmtDate, fmtFrac, fmtInt, fmtNum } from '@/utils/format';

const METHODS = ['simple', 'log'] as const satisfies readonly ReturnMethod[];
const CHART = 'h-[280px]';

// Stable formatters (LineChart requires stable references).
const fracSigned = (v: number) => fmtFrac(v);
const fracPlain = (v: number) => fmtFrac(v, { signed: false });
const level = (v: number) => fmtNum(v, 2);
const compact = (v: number) => fmtCompact(v);

function toSpecs(series: SymbolSeries, order: string[], kind?: SeriesSpec['kind']): SeriesSpec[] {
  return order.filter((s) => series[s]).map((s) => ({ id: s, label: s, color: seriesColor(order.indexOf(s)), data: series[s], kind }));
}

/** Integer input that only commits values inside [min, max]. */
function WindowField({ label, value, min, max, onChange }: { label: string; value: number; min: number; max: number; onChange: (v: number) => void }) {
  const [text, setText] = useState(String(value));
  const n = Number(text);
  const valid = Number.isInteger(n) && n >= min && n <= max;
  return (
    <label className="inline-flex items-center gap-1.5 text-[11px] text-muted-foreground">
      {label}
      <Input
        type="number"
        inputMode="numeric"
        min={min}
        max={max}
        value={text}
        aria-invalid={!valid}
        title={`${min}–${max}`}
        className={`h-7 w-16 ${valid ? '' : 'border-negative'}`}
        onChange={(e) => {
          setText(e.target.value);
          const v = Number(e.target.value);
          if (Number.isInteger(v) && v >= min && v <= max) onChange(v);
        }}
      />
    </label>
  );
}

function SymbolTable({ head, rows }: { head: string[]; rows: { key: string; cells: ReactNode[] }[] }) {
  return (
    <Table>
      <TableHeader>
        <TableRow className="hover:bg-transparent">
          <TableHead>Symbol</TableHead>
          {head.map((h) => (
            <TableHead key={h} className="text-right">
              {h}
            </TableHead>
          ))}
        </TableRow>
      </TableHeader>
      <TableBody>
        {rows.map((r, i) => (
          <TableRow key={r.key}>
            <TableCell>
              <span className="inline-flex items-center gap-1.5 font-semibold">
                <span className="size-2 rounded-sm" style={{ background: seriesColor(i) }} aria-hidden />
                {r.key}
              </span>
            </TableCell>
            {r.cells.map((c, j) => (
              <TableCell key={j} className="text-right">
                {c}
              </TableCell>
            ))}
          </TableRow>
        ))}
      </TableBody>
    </Table>
  );
}

function CorrelationMatrix({ data }: { data: CorrelationResponse }) {
  const { symbols, matrix, observations } = data;
  return (
    <div className="overflow-x-auto p-3">
      <table className="mx-auto border-separate border-spacing-0.5 text-[11px]">
        <thead>
          <tr>
            <th />
            {symbols.map((s) => (
              <th key={s} className="px-1 pb-1 font-semibold text-muted-foreground">
                {s}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {symbols.map((a, i) => (
            <tr key={a}>
              <th className="pr-2 text-right font-semibold text-muted-foreground">{a}</th>
              {symbols.map((b, j) => {
                const v = matrix[i]?.[j] ?? null;
                const n = observations[i]?.[j] ?? 0;
                const color = v === null ? undefined : v >= 0 ? 'var(--secondary)' : 'var(--negative)';
                return (
                  <td key={b} className="p-0">
                    <Hint content={`${a} × ${b}: ${fmtInt(n)} paired observations${v === null ? ' (fewer than 10 — not computed)' : ''}`}>
                      <span
                        className="flex h-9 w-14 items-center justify-center rounded-sm border border-transparent font-medium"
                        style={color ? { background: `color-mix(in srgb, ${color} ${Math.round(Math.abs(v!) * 70)}%, transparent)` } : undefined}
                      >
                        {v === null ? <span className="text-muted-foreground">n/a</span> : fmtNum(v, 2)}
                      </span>
                    </Hint>
                  </td>
                );
              })}
            </tr>
          ))}
        </tbody>
      </table>
      <div className="mt-2 flex items-center justify-center gap-2 text-[10px] text-muted-foreground">
        <span>−1</span>
        <span className="h-1.5 w-32 rounded-full" style={{ background: 'linear-gradient(90deg, var(--negative), transparent, var(--secondary))' }} />
        <span>+1</span>
      </div>
    </div>
  );
}

export function AnalyticsPage() {
  const universe = useInstrumentUniverse();
  const [picked, setPicked] = useState<string[] | null>(null);
  const [range, setRange] = useState<AnalyticsRange>('3M');
  const [method, setMethod] = useState<ReturnMethod>('simple');
  const [volWindow, setVolWindow] = useState(20);
  const [volumeWindow, setVolumeWindow] = useState(20);
  const [sma, setSma] = useState(20);
  const [ema, setEma] = useState(50);
  const [focus, setFocus] = useState('');

  // Until the user picks, analyse the first four instruments of the universe.
  const symbols = useMemo(() => picked ?? universe.data?.items.slice(0, 4).map((r) => r.symbol) ?? [], [picked, universe.data]);
  const symbolsKey = symbols.join(',');
  const q = useMemo(() => ({ symbols: symbolsKey ? symbolsKey.split(',') : [], range }), [symbolsKey, range]);
  useEffect(() => {
    if (!symbols.includes(focus)) setFocus(symbols[0] ?? '');
  }, [symbols, focus]);

  const w = { volWindow: useDebounced(volWindow, 400), volumeWindow: useDebounced(volumeWindow, 400), sma: useDebounced(sma, 400), ema: useDebounced(ema, 400) };
  const returns = useReturns(q, method);
  const vol = useVolatility(q, w.volWindow);
  const corr = useCorrelation(q, method);
  const dd = useDrawdown(q);
  const cmp = useComparison(q);
  const ma = useMovingAverages(focus, range, w.sma, w.ema);
  const volume = useVolumeTrend({ symbols: focus ? [focus] : [], range }, w.volumeWindow);

  const order = q.symbols;
  const cmpSeries = useMemo(() => (cmp.data ? toSpecs(cmp.data.series, order) : []), [cmp.data, order]);
  const retSeries = useMemo(() => (returns.data ? toSpecs(returns.data.series, order) : []), [returns.data, order]);
  const volSeries = useMemo(() => (vol.data ? toSpecs(vol.data.series, order) : []), [vol.data, order]);
  const ddSeries = useMemo(() => (dd.data ? toSpecs(dd.data.series, order, 'area') : []), [dd.data, order]);
  const maSeries = useMemo<SeriesSpec[]>(
    () =>
      ma.data
        ? [
            { id: 'close', label: 'Close', color: seriesColor(9), data: ma.data.close },
            { id: 'sma', label: `SMA ${ma.data.sma_window}`, color: seriesColor(0), data: ma.data.sma },
            { id: 'ema', label: `EMA ${ma.data.ema_span}`, color: seriesColor(1), data: ma.data.ema },
          ]
        : [],
    [ma.data],
  );
  const volumeSeries = useMemo<SeriesSpec[]>(
    () =>
      volume.data && focus
        ? [
            { id: 'volume', label: 'Daily volume', color: seriesColor(0), data: volume.data.series[focus] ?? [], kind: 'histogram' },
            { id: 'avg', label: `${volume.data.window}D average`, color: seriesColor(1), data: volume.data.average[focus] ?? [] },
          ]
        : [],
    [volume.data, focus],
  );

  const focusPicker = (
    <NativeSelect value={focus} onChange={(e) => setFocus(e.target.value)} aria-label="Focus instrument" className="h-7">
      {symbols.map((s) => (
        <option key={s}>{s}</option>
      ))}
    </NativeSelect>
  );

  return (
    <>
      <PageHeader
        title="Analytics"
        description="Computed server-side from daily closes. Up to 10 instruments."
        actions={
          <>
            <RangeTabs label="Analysis range" value={range} options={ANALYTICS_RANGES} onChange={setRange} />
            <RangeTabs label="Return method" value={method} options={METHODS} onChange={setMethod} />
          </>
        }
      />
      <Card>
        <CardHeader>
          <CardTitle>Instruments ({symbols.length}/10)</CardTitle>
        </CardHeader>
        <CardContent>
          <SymbolMultiSelect value={symbols} onChange={setPicked} />
        </CardContent>
      </Card>

      <QueryCard title="Comparative performance · normalized to 100" query={cmp} skeletonClass={CHART}>
        {(d) => (
          <div className="grid gap-3 p-3 lg:grid-cols-[minmax(0,1fr)_260px]">
            <LineChart series={cmpSeries} format={level} fitKey={range} timeVisible={false} className={CHART} />
            <SymbolTable head={['Total return']} rows={order.map((s) => ({ key: s, cells: [<ChangeText value={d.total_return[s]} kind="frac" />] }))} />
          </div>
        )}
      </QueryCard>

      <div className="grid gap-3 xl:grid-cols-2">
        <QueryCard title={`Historical returns · daily ${method}`} query={returns} skeletonClass={CHART}>
          {(d) => (
            <>
              <div className="p-3">
                <LineChart series={retSeries} format={fracSigned} fitKey={range} timeVisible={false} className={CHART} />
              </div>
              <SymbolTable
                head={['Total', 'Mean daily', 'Obs.']}
                rows={order.map((s) => ({
                  key: s,
                  cells: [
                    <ChangeText value={d.summary[s]?.total_return} kind="frac" />,
                    <ChangeText value={d.summary[s]?.mean_return} kind="frac" />,
                    fmtInt(d.summary[s]?.observations),
                  ],
                }))}
              />
            </>
          )}
        </QueryCard>

        <QueryCard
          title="Rolling volatility · annualized"
          query={vol}
          skeletonClass={CHART}
          actions={<WindowField label="Window" value={volWindow} min={5} max={120} onChange={setVolWindow} />}
        >
          {(d) => (
            <>
              <div className="p-3">
                <LineChart series={volSeries} format={fracPlain} fitKey={range} timeVisible={false} className={CHART} />
              </div>
              <SymbolTable
                head={[`Latest (${d.window}D)`, 'Full period']}
                rows={order.map((s) => ({ key: s, cells: [fmtFrac(d.latest[s], { signed: false }), fmtFrac(d.full_period[s], { signed: false })] }))}
              />
            </>
          )}
        </QueryCard>

        <QueryCard title="Maximum drawdown" query={dd} skeletonClass={CHART}>
          {(d) => (
            <>
              <div className="p-3">
                <LineChart series={ddSeries} format={fracSigned} fitKey={range} timeVisible={false} className={CHART} />
              </div>
              <SymbolTable
                head={['Max DD', 'Peak', 'Trough', 'Recovered']}
                rows={order.map((s) => {
                  const m = d.max_drawdown[s];
                  return {
                    key: s,
                    cells: [
                      <ChangeText value={m?.value} kind="frac" />,
                      fmtDate(m?.peak_at),
                      fmtDate(m?.trough_at),
                      m ? (m.recovered_at ? fmtDate(m.recovered_at) : <span className="text-warning">not yet</span>) : '—',
                    ],
                  };
                })}
              />
            </>
          )}
        </QueryCard>

        <QueryCard title={`Correlation · ${method} returns`} query={corr} skeletonClass="h-72">
          {(d) => <CorrelationMatrix data={d} />}
        </QueryCard>

        <QueryCard
          title="Moving averages"
          query={ma}
          skeletonClass={CHART}
          actions={
            <div className="flex flex-wrap items-center gap-2">
              {focusPicker}
              <WindowField label="SMA" value={sma} min={2} max={200} onChange={setSma} />
              <WindowField label="EMA" value={ema} min={2} max={200} onChange={setEma} />
            </div>
          }
        >
          {() => (
            <div className="p-3">
              <LineChart series={maSeries} format={level} fitKey={`${range}${focus}`} timeVisible={false} className={CHART} />
            </div>
          )}
        </QueryCard>

        <QueryCard
          title="Volume trends"
          query={volume}
          skeletonClass={CHART}
          actions={
            <div className="flex flex-wrap items-center gap-2">
              {focusPicker}
              <WindowField label="Avg window" value={volumeWindow} min={2} max={120} onChange={setVolumeWindow} />
            </div>
          }
        >
          {() => (
            <div className="p-3">
              <LineChart series={volumeSeries} format={compact} fitKey={`${range}${focus}`} timeVisible={false} className={CHART} />
            </div>
          )}
        </QueryCard>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Methods</CardTitle>
        </CardHeader>
        <CardContent>
          <dl className="grid gap-x-6 gap-y-2 text-[11px] md:grid-cols-2">
            {[
              ['Simple return', 'R(t) = (P(t) − P(t−1)) / P(t−1)'],
              ['Log return', 'r(t) = ln(P(t) / P(t−1))'],
              ['SMA(n)', 'arithmetic mean of the last n closes'],
              ['EMA(n)', 'EMA(t) = α·P(t) + (1 − α)·EMA(t−1), α = 2 / (n + 1)'],
              ['Rolling volatility', 'sample std of returns over the window × √365 (the simulated venue trades 24/7)'],
              ['Correlation', 'Pearson on pairwise-aligned returns; n/a below 10 observations'],
              ['Max drawdown', 'min(P / cummax(P) − 1)'],
              ['Normalized performance', '100 × P(t) / P(first)'],
            ].map(([k, v]) => (
              <div key={k} className="flex flex-col gap-0.5 sm:flex-row sm:gap-3">
                <dt className="w-40 shrink-0 text-muted-foreground">{k}</dt>
                <dd>
                  <code className="rounded bg-panel px-1 py-px font-mono text-[11px]">{v}</code>
                </dd>
              </div>
            ))}
          </dl>
        </CardContent>
      </Card>
    </>
  );
}
