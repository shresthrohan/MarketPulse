import { ArrowRight, ChevronDown, ChevronRight } from 'lucide-react';
import { Fragment, useMemo, useState } from 'react';
import { Link } from 'react-router-dom';
import { cssVar } from '@/charts/colors';
import { LineChart, type SeriesSpec } from '@/charts/LineChart';
import { QueryRows, ShortId } from '@/components/DataTable';
import { PageHeader } from '@/components/PageHeader';
import { RangeTabs } from '@/components/RangeTabs';
import { StatCard } from '@/components/StatCard';
import { StatusBadge, StatusDot, statusVariant } from '@/components/StatusBadge';
import { ErrorState, QueryCard } from '@/components/States';
import { Card, CardHeader, CardTitle } from '@/components/ui/card';
import { Skeleton } from '@/components/ui/skeleton';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { Hint } from '@/components/ui/tooltip';
import { usePipelineEvents, usePipelineMetrics, usePipelineOverview, usePipelineServices, usePipelineThroughput } from '@/hooks/queries';
import { useTheme } from '@/hooks/useTheme';
import type { Point, Service, ServiceKey } from '@/types/api';
import { cn } from '@/utils/cn';
import { fmtAgo, fmtBytes, fmtCompact, fmtDateTime, fmtFrac, fmtInt, fmtMs, fmtNum, fmtPct, fmtTime } from '@/utils/format';

/** Diagram layout only (which box goes where); names and statuses come from /api/pipeline/services. */
const STAGES: ServiceKey[][] = [['ingestion'], ['kafka'], ['processor'], ['postgres', 'redis'], ['api'], ['websocket']];
const FALLBACK_NAME: Record<ServiceKey, string> = {
  ingestion: 'Data ingestion',
  kafka: 'Kafka broker',
  processor: 'Stream processor',
  postgres: 'PostgreSQL',
  redis: 'Redis',
  api: 'FastAPI',
  websocket: 'WebSocket service',
};

const MINUTES = [15, 60, 360, 1440] as const;
type Minutes = (typeof MINUTES)[number];
const BUCKET_S: Record<Minutes, number> = { 15: 15, 60: 60, 360: 300, 1440: 900 };
const minutesLabel = (m: Minutes) => (m < 60 ? `${m}m` : `${m / 60}h`);

const rate = (v: number) => `${fmtNum(v, 1)}/s`;
const ms = (v: number) => fmtMs(v);
const ratio = (v: number) => fmtFrac(v, { signed: false });
const count = (v: number) => fmtInt(v);

/** Service metrics are free-form: format by value shape (bytes / timestamps / big counts). */
function fmtMetric(name: string, val: number | string | boolean | null): string {
  if (val === null) return '—';
  if (typeof val === 'boolean') return val ? 'yes' : 'no';
  if (typeof val === 'string') return /^\d{4}-\d{2}-\d{2}T/.test(val) ? fmtTime(val) : val;
  if (name.endsWith('_bytes')) return fmtBytes(val);
  if (Math.abs(val) >= 100_000) return fmtCompact(val);
  return fmtNum(val, Number.isInteger(val) ? 0 : 2);
}

const borderByStatus = { positive: 'border-positive/40', warning: 'border-warning/50', negative: 'border-negative/50' } as const;

function ServiceNode({ k, service }: { k: ServiceKey; service: Service | undefined }) {
  const v = statusVariant(service?.status);
  const metrics = Object.entries(service?.metrics ?? {}).slice(0, 4);
  return (
    <div className={cn('min-w-0 rounded-md border bg-panel/60 p-2.5', borderByStatus[v as keyof typeof borderByStatus])}>
      <div className="flex items-center justify-between gap-2">
        <span className="truncate text-xs font-semibold">{service?.name ?? FALLBACK_NAME[k]}</span>
        <StatusDot status={service?.status} />
      </div>
      <div className="mt-1 flex items-center justify-between gap-2 text-[10px] text-muted-foreground">
        <StatusBadge status={service?.status ?? null} label={service?.status ?? 'not reported'} />
        {service?.latency_ms !== null && service?.latency_ms !== undefined && <span>{fmtMs(service.latency_ms)}</span>}
      </div>
      {service?.detail && (
        <Hint content={service.detail}>
          <p className="mt-1.5 line-clamp-2 text-[11px] text-muted-foreground">{service.detail}</p>
        </Hint>
      )}
      {metrics.length > 0 && (
        <dl className="mt-1.5 grid gap-0.5 border-t pt-1.5 text-[11px]">
          {metrics.map(([name, val]) => (
            <div key={name} className="flex justify-between gap-2">
              <dt className="min-w-0 truncate text-muted-foreground">{name.replace(/_/g, ' ')}</dt>
              <dd className="max-w-[55%] truncate text-right">
                <Hint content={typeof val === 'string' ? val : undefined}>{fmtMetric(name, val)}</Hint>
              </dd>
            </div>
          ))}
        </dl>
      )}
      {service?.last_seen && <p className="mt-1 text-[10px] text-muted-foreground">last seen {fmtAgo(service.last_seen)}</p>}
    </div>
  );
}

function PipelineDiagram() {
  const q = usePipelineServices();
  return (
    <Card>
      <CardHeader>
        <CardTitle>Pipeline topology</CardTitle>
        {q.data && <span className="text-[10px] text-muted-foreground">checked {fmtTime(q.data.checked_at)} UTC</span>}
      </CardHeader>
      {q.isPending ? (
        <Skeleton className="m-3 h-40" />
      ) : q.isError && !q.data ? (
        <ErrorState error={q.error} onRetry={() => void q.refetch()} />
      ) : (
        <div className="flex flex-col items-stretch gap-1.5 p-3 xl:flex-row xl:items-center">
          {STAGES.map((stage, i) => (
            <Fragment key={stage.join('+')}>
              {i > 0 && (
                <div className="flex shrink-0 justify-center text-muted-foreground" aria-hidden>
                  <ChevronDown className="size-4 xl:hidden" />
                  <ChevronRight className="hidden size-4 xl:block" />
                </div>
              )}
              <div className="grid min-w-0 flex-1 gap-1.5">
                {stage.map((k) => (
                  <ServiceNode key={k} k={k} service={q.data.items.find((s) => s.key === k)} />
                ))}
              </div>
            </Fragment>
          ))}
        </div>
      )}
    </Card>
  );
}

function OverviewTiles() {
  const q = usePipelineOverview();
  if (q.isError && !q.data)
    return (
      <Card>
        <ErrorState error={q.error} onRetry={() => void q.refetch()} className="py-4" />
      </Card>
    );
  const p = q.data;
  const loading = q.isPending;
  const window = p ? `last ${p.window_hours}h` : undefined;
  return (
    <div className="grid grid-cols-2 gap-3 md:grid-cols-4 2xl:grid-cols-8">
      <StatCard label="Events received" loading={loading} value={fmtCompact(p?.events_received)} sub={window} />
      <StatCard label="Events processed" loading={loading} value={fmtCompact(p?.events_processed)} sub={p && `${fmtInt(p.events_retried)} retried`} />
      <StatCard
        label="Events failed"
        loading={loading}
        value={fmtInt(p?.events_failed)}
        tone={p && p.events_failed > 0 ? 'negative' : undefined}
        sub={p && `${fmtInt(p.events_duplicate)} duplicates`}
      />
      <StatCard label="Events / sec" loading={loading} value={fmtNum(p?.events_per_second, 1)} hint="Processed events over the last 60 s" sub="last 60s" />
      <StatCard label="Processing latency" loading={loading} value={fmtMs(p?.avg_latency_ms)} hint="Average, last 5 minutes" sub={p && `max ${fmtMs(p.max_latency_ms)}`} />
      <StatCard
        label="Consumer lag"
        loading={loading}
        value={p?.consumer_lag === null ? 'n/a' : fmtInt(p?.consumer_lag)}
        tone={p?.consumer_lag === null ? 'warning' : undefined}
        hint="Live from Kafka: committed offset vs. high watermark"
        sub={p?.consumer_lag === null ? 'Kafka unreachable' : 'messages'}
      />
      <StatCard label="DB write latency" loading={loading} value={fmtMs(p?.db_write_ms)} hint="Average, last 5 minutes" sub="per batch" />
      <StatCard
        label="Last successful event"
        loading={loading}
        value={fmtTime(p?.last_successful_event_at)}
        sub={p?.last_successful_event_at ? fmtAgo(p.last_successful_event_at) : 'none yet'}
      />
    </div>
  );
}

function DataQuality() {
  const q = usePipelineOverview();
  return (
    <QueryCard title="Data quality" query={q} skeletonClass="h-48">
      {(p) => {
        const dq = p.data_quality;
        const maxErr = Math.max(1, ...p.error_breakdown.map((e) => e.count));
        const parts = [
          { label: 'Valid', value: dq.valid, cls: 'bg-positive' },
          { label: 'Invalid', value: dq.invalid, cls: 'bg-negative' },
          { label: 'Duplicate', value: dq.duplicate, cls: 'bg-warning' },
        ];
        return (
          <div className="grid gap-4 p-3 md:grid-cols-2">
            <div>
              <div className="flex items-baseline justify-between">
                <span className="text-2xl font-semibold">{fmtPct(dq.valid_pct, { signed: false })}</span>
                <span className="text-[11px] text-muted-foreground">valid of {fmtInt(dq.total)} total</span>
              </div>
              <div className="mt-2 flex h-2 overflow-hidden rounded-full bg-panel" role="img" aria-label="Valid / invalid / duplicate split">
                {parts.map((x) => (
                  <div key={x.label} className={x.cls} style={{ width: `${dq.total ? (x.value / dq.total) * 100 : 0}%` }} />
                ))}
              </div>
              <dl className="mt-3 grid grid-cols-2 gap-x-4 gap-y-1 text-[11px] sm:grid-cols-4">
                {[...parts, { label: 'Rejected', value: dq.rejected, cls: 'bg-muted-foreground' }].map((x) => (
                  <div key={x.label}>
                    <dt className="flex items-center gap-1.5 text-muted-foreground">
                      <span className={cn('size-2 rounded-sm', x.cls)} /> {x.label}
                    </dt>
                    <dd className="font-medium">{fmtInt(x.value)}</dd>
                  </div>
                ))}
              </dl>
            </div>
            <div>
              <div className="mb-1.5 text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">Error breakdown</div>
              {p.error_breakdown.length === 0 ? (
                <p className="text-[11px] text-muted-foreground">No validation errors in the window.</p>
              ) : (
                <ul className="grid gap-1.5">
                  {p.error_breakdown.map((e) => (
                    <li key={e.error_type} className="grid grid-cols-[minmax(0,9rem)_1fr_3rem] items-center gap-2 text-[11px]">
                      <span className="truncate text-muted-foreground">{e.error_type}</span>
                      <span className="h-1.5 rounded-full bg-panel">
                        <span className="block h-full rounded-full bg-negative/80" style={{ width: `${(e.count / maxErr) * 100}%` }} />
                      </span>
                      <span className="text-right">{fmtInt(e.count)}</span>
                    </li>
                  ))}
                </ul>
              )}
            </div>
          </div>
        );
      }}
    </QueryCard>
  );
}

const spec = (id: string, label: string, color: string, data: Point[], kind: SeriesSpec['kind'] = 'line'): SeriesSpec => ({ id, label, color, data, kind });

function MetricCharts({ minutes }: { minutes: Minutes }) {
  const theme = useTheme();
  const throughput = usePipelineThroughput(minutes, BUCKET_S[minutes]);
  const metrics = usePipelineMetrics(minutes, BUCKET_S[minutes]);
  const c = useMemo(
    // theme: re-read the tokens when it flips
    () => ({ pos: cssVar('--positive'), neg: cssVar('--negative'), blue: cssVar('--secondary'), warn: cssVar('--warning') }),
    [theme],
  );
  const tp = useMemo(
    () =>
      throughput.data
        ? [
            spec('received', 'Received', c.blue, throughput.data.received),
            spec('processed', 'Processed', c.pos, throughput.data.processed),
            spec('failed', 'Failed', c.neg, throughput.data.failed),
          ]
        : [],
    [throughput.data, c],
  );
  const s = metrics.data?.series;
  const latency = useMemo(() => (s ? [spec('lat', 'Avg processing', c.blue, s.avg_latency_ms, 'area'), spec('db', 'DB write', c.warn, s.db_write_ms)] : []), [s, c]);
  const errors = useMemo(() => (s ? [spec('err', 'Error rate', c.neg, s.error_rate, 'area')] : []), [s, c]);
  const lag = useMemo(() => (s ? [spec('lag', 'Consumer lag', c.warn, s.consumer_lag, 'area')] : []), [s, c]);
  const h = 'h-[220px]';
  const bucket = (b: number | undefined) => (b ? <span className="text-[10px] text-muted-foreground">{b}s buckets</span> : null);

  return (
    <div className="grid gap-3 lg:grid-cols-2">
      <QueryCard title="Event throughput · events/s" query={throughput} skeletonClass={h} actions={bucket(throughput.data?.bucket_s)}>
        {() => <LineChart series={tp} format={rate} fitKey={minutes} className={cn(h, 'p-3')} />}
      </QueryCard>
      <QueryCard title="Processing latency" query={metrics} skeletonClass={h} actions={bucket(metrics.data?.bucket_s)}>
        {() => <LineChart series={latency} format={ms} fitKey={minutes} className={cn(h, 'p-3')} />}
      </QueryCard>
      <QueryCard title="Error rate" query={metrics} skeletonClass={h}>
        {() => <LineChart series={errors} format={ratio} fitKey={minutes} className={cn(h, 'p-3')} />}
      </QueryCard>
      <QueryCard title="Consumer lag · messages" query={metrics} skeletonClass={h}>
        {() => <LineChart series={lag} format={count} fitKey={minutes} className={cn(h, 'p-3')} />}
      </QueryCard>
    </div>
  );
}

function RecentPipelineEvents() {
  const q = usePipelineEvents({ view: 'all', page_size: 15 });
  const cols = 7;
  return (
    <Card>
      <CardHeader>
        <CardTitle>Recent pipeline events</CardTitle>
        <Link to="/logs" className="inline-flex items-center gap-1 text-[11px] text-secondary hover:underline">
          Event logs <ArrowRight className="size-3" />
        </Link>
      </CardHeader>
      <Table>
        <TableHeader>
          <TableRow className="hover:bg-transparent">
            <TableHead>Time</TableHead>
            <TableHead>Event</TableHead>
            <TableHead>Symbol</TableHead>
            <TableHead>Type</TableHead>
            <TableHead>Severity</TableHead>
            <TableHead>Status</TableHead>
            <TableHead>Details</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          <QueryRows query={q} cols={cols} rows={6} isEmpty={(d) => !d.items.length} empty="No pipeline events yet">
            {(d) =>
              d.items.map((e, i) => (
                <TableRow key={`${e.event_id ?? 'none'}-${e.timestamp}-${i}`}>
                  <TableCell className="text-muted-foreground">{fmtDateTime(e.timestamp)}</TableCell>
                  <TableCell>
                    <ShortId id={e.event_id} />
                  </TableCell>
                  <TableCell className="font-semibold">{e.symbol ?? '—'}</TableCell>
                  <TableCell className="font-mono text-[11px]">{e.event_type}</TableCell>
                  <TableCell>
                    <StatusBadge status={e.severity} />
                  </TableCell>
                  <TableCell>
                    <StatusBadge status={e.processing_status} />
                  </TableCell>
                  <TableCell className="max-w-72 truncate text-muted-foreground">
                    <Hint content={e.error_details}>{e.error_details ?? '—'}</Hint>
                  </TableCell>
                </TableRow>
              ))
            }
          </QueryRows>
        </TableBody>
      </Table>
    </Card>
  );
}

export function PipelinePage() {
  const [minutes, setMinutes] = useState<Minutes>(60);
  return (
    <>
      <PageHeader
        title="Pipeline monitor"
        description="Live component status, processor metrics and data quality. Refreshes every 5 s."
        actions={<RangeTabs label="Metrics window" value={minutes} options={MINUTES} onChange={setMinutes} format={minutesLabel} />}
      />
      <PipelineDiagram />
      <OverviewTiles />
      <DataQuality />
      <MetricCharts minutes={minutes} />
      <RecentPipelineEvents />
    </>
  );
}
