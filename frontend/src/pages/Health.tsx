import type { UseQueryResult } from '@tanstack/react-query';
import { Cpu, Database, HardDrive, MonitorSmartphone, Network, Radio, Server, Workflow, Zap, type LucideIcon } from 'lucide-react';
import type { ReactNode } from 'react';
import { PageHeader } from '@/components/PageHeader';
import { StatCard } from '@/components/StatCard';
import { StatusBadge } from '@/components/StatusBadge';
import { ErrorState, QueryState } from '@/components/States';
import { Card, CardHeader, CardTitle } from '@/components/ui/card';
import { Skeleton } from '@/components/ui/skeleton';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { Hint } from '@/components/ui/tooltip';
import { useHealth, useHealthDatabase, useHealthKafka, useHealthRedis, useSystemHealth } from '@/hooks/queries';
import { useLastMessageAt, useLiveQuotes, useMessageCount, useWsStatus } from '@/hooks/useLive';
import { wsStatusText } from '@/services/ws';
import type { ComponentHealth, SystemHealth } from '@/types/api';
import { cn } from '@/utils/cn';
import { fmtAgo, fmtBytes, fmtDateTime, fmtDuration, fmtInt, fmtMs, fmtNum, fmtPct, fmtTime } from '@/utils/format';

type Fact = [label: string, value: ReactNode];

function HealthCard({
  title,
  icon: Icon,
  query,
  pick,
}: {
  title: string;
  icon: LucideIcon;
  query: UseQueryResult<unknown>;
  pick: () => { c: ComponentHealth | undefined; facts?: Fact[] };
}) {
  const { c, facts = [] } = query.data ? pick() : { c: undefined };
  return (
    <Card className="flex flex-col">
      <CardHeader>
        <CardTitle className="flex items-center gap-1.5">
          <Icon className="size-3.5" aria-hidden /> {title}
        </CardTitle>
        {query.isPending ? <Skeleton className="h-4 w-12" /> : <StatusBadge status={c?.status ?? (query.isError ? 'down' : null)} label={c?.status ?? (query.isError ? 'unreachable' : 'unknown')} />}
      </CardHeader>
      {query.isPending ? (
        <Skeleton className="m-3 h-16" />
      ) : query.isError && !query.data ? (
        <ErrorState error={query.error} onRetry={() => void query.refetch()} className="py-4" />
      ) : (
        <dl className="grid grid-cols-1 gap-1 p-3 text-[11px]">
          <div className="flex justify-between gap-3">
            <dt className="text-muted-foreground">Check latency</dt>
            <dd>{fmtMs(c?.latency_ms)}</dd>
          </div>
          {facts.map(([k, v]) => (
            <div key={k} className="flex justify-between gap-3">
              <dt className="text-muted-foreground">{k}</dt>
              <dd className="min-w-0 truncate text-right">{v}</dd>
            </div>
          ))}
          {c?.detail && (
            <Hint content={c.detail}>
              <p className="mt-1 line-clamp-2 text-muted-foreground">{c.detail}</p>
            </Hint>
          )}
        </dl>
      )}
    </Card>
  );
}

function Meter({ label, pct, detail }: { label: string; pct: number | null | undefined; detail?: ReactNode }) {
  const v = Math.max(0, Math.min(100, pct ?? 0));
  const tone = v >= 90 ? 'bg-negative' : v >= 70 ? 'bg-warning' : 'bg-primary';
  return (
    <div>
      <div className="flex justify-between text-[11px]">
        <span className="text-muted-foreground">{label}</span>
        <span className="font-medium">{fmtPct(pct, { signed: false, digits: 1 })}</span>
      </div>
      <div className="mt-1 h-1.5 overflow-hidden rounded-full bg-panel" role="meter" aria-valuenow={v} aria-valuemin={0} aria-valuemax={100} aria-label={label}>
        <div className={cn('h-full rounded-full transition-[width]', tone)} style={{ width: `${v}%` }} />
      </div>
      {detail && <div className="mt-0.5 text-[10px] text-muted-foreground">{detail}</div>}
    </div>
  );
}

function BrowserSocketCard() {
  const status = useWsStatus();
  const last = useLastMessageAt();
  const messages = useMessageCount();
  const symbols = Object.keys(useLiveQuotes()).length;
  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-1.5">
          <MonitorSmartphone className="size-3.5" aria-hidden /> This browser · WebSocket
        </CardTitle>
        <StatusBadge status={status.state} />
      </CardHeader>
      <dl className="grid grid-cols-1 gap-1 p-3 text-[11px]">
        {(
          [
            ['Connection', wsStatusText(status)],
            ['Messages received', fmtInt(messages)],
            ['Symbols with live quotes', fmtInt(symbols)],
            ['Last message', last ? `${fmtTime(last)} UTC (${fmtAgo(last)})` : 'none yet'],
          ] as Fact[]
        ).map(([k, v]) => (
          <div key={k} className="flex justify-between gap-3">
            <dt className="shrink-0 text-muted-foreground">{k}</dt>
            <dd className="min-w-0 text-right">{v}</dd>
          </div>
        ))}
      </dl>
    </Card>
  );
}

function SystemSection({ s }: { s: SystemHealth }) {
  const { process: proc, host, api } = s;
  return (
    <>
      <div className="grid grid-cols-2 gap-3 lg:grid-cols-5">
        <StatCard label="API p50" value={fmtMs(api.p50_ms)} sub={`${fmtInt(api.requests)} requests sampled`} />
        <StatCard label="API p95" value={fmtMs(api.p95_ms)} tone={api.p95_ms !== null && api.p95_ms > 500 ? 'warning' : undefined} />
        <StatCard label="API p99" value={fmtMs(api.p99_ms)} tone={api.p99_ms !== null && api.p99_ms > 1000 ? 'warning' : undefined} />
        <StatCard label="Backend uptime" value={fmtDuration(s.uptime_s)} sub={`since ${fmtDateTime(s.started_at)}`} />
        <StatCard
          label="WS server clients"
          value={fmtInt(s.websocket.connections)}
          sub={`${fmtInt(s.websocket.messages_sent)} sent · ${fmtInt(s.websocket.dropped_clients)} dropped`}
          tone={s.websocket.listener_connected ? undefined : 'negative'}
          hint={s.websocket.listener_connected ? 'Redis listener connected' : 'Redis listener disconnected: no quotes are fanned out'}
        />
      </div>

      <div className="grid gap-3 lg:grid-cols-3">
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-1.5">
              <Cpu className="size-3.5" aria-hidden /> CPU · memory
            </CardTitle>
            <span className="text-[10px] text-muted-foreground">backend container view</span>
          </CardHeader>
          <div className="grid gap-3 p-3">
            <Meter label="Process CPU" pct={proc.cpu_percent} detail={`${fmtInt(proc.threads)} threads · RSS ${fmtBytes(proc.rss_bytes)}`} />
            <Meter label="Host CPU" pct={host.cpu_percent} detail={`${fmtInt(host.cpu_count)} cores${host.load_avg ? ` · load ${host.load_avg.map((l) => fmtNum(l, 2)).join(' / ')}` : ''}`} />
            <Meter label="Host memory" pct={host.memory_percent} detail={`${fmtBytes(host.memory_used_bytes)} of ${fmtBytes(host.memory_total_bytes)}`} />
          </div>
        </Card>

        <Card className="lg:col-span-2">
          <CardHeader>
            <CardTitle>API response times by route</CardTitle>
            <span className="text-[10px] text-muted-foreground">last 2,000 requests</span>
          </CardHeader>
          {api.routes.length === 0 ? (
            <p className="p-3 text-[11px] text-muted-foreground">No requests recorded yet.</p>
          ) : (
            <div className="max-h-72 overflow-y-auto">
              <Table>
                <TableHeader>
                  <TableRow className="hover:bg-transparent">
                    <TableHead>Route</TableHead>
                    <TableHead className="text-right">Count</TableHead>
                    <TableHead className="text-right">p50</TableHead>
                    <TableHead className="text-right">p95</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {[...api.routes]
                    .sort((a, b) => b.count - a.count)
                    .map((r) => (
                      <TableRow key={r.route}>
                        <TableCell className="font-mono text-[11px]">{r.route}</TableCell>
                        <TableCell className="text-right">{fmtInt(r.count)}</TableCell>
                        <TableCell className="text-right">{fmtMs(r.p50_ms)}</TableCell>
                        <TableCell className="text-right">{fmtMs(r.p95_ms)}</TableCell>
                      </TableRow>
                    ))}
                </TableBody>
              </Table>
            </div>
          )}
        </Card>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Service heartbeats · uptime</CardTitle>
        </CardHeader>
        {s.services.length === 0 ? (
          <p className="p-3 text-[11px] text-muted-foreground">No service heartbeats in Redis.</p>
        ) : (
          <Table>
            <TableHeader>
              <TableRow className="hover:bg-transparent">
                <TableHead>Service</TableHead>
                <TableHead>Host</TableHead>
                <TableHead>Status</TableHead>
                <TableHead className="text-right">CPU</TableHead>
                <TableHead className="text-right">RSS</TableHead>
                <TableHead className="text-right">Uptime</TableHead>
                <TableHead>Heartbeat</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {s.services.map((svc) => (
                <TableRow key={`${svc.key}-${svc.host ?? ''}`}>
                  <TableCell className="font-semibold">{svc.key}</TableCell>
                  <TableCell className="font-mono text-[11px] text-muted-foreground">{svc.host ?? '—'}</TableCell>
                  <TableCell>
                    <StatusBadge status={svc.status} />
                  </TableCell>
                  <TableCell className="text-right">{fmtPct(svc.cpu_percent, { signed: false, digits: 1 })}</TableCell>
                  <TableCell className="text-right">{fmtBytes(svc.rss_bytes)}</TableCell>
                  <TableCell className="text-right">{fmtDuration(svc.uptime_s)}</TableCell>
                  <TableCell className="text-muted-foreground">
                    <Hint content={fmtDateTime(svc.heartbeat_at)}>{fmtAgo(svc.heartbeat_at)}</Hint>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        )}
      </Card>
    </>
  );
}

export function HealthPage() {
  const health = useHealth();
  const db = useHealthDatabase();
  const redis = useHealthRedis();
  const kafka = useHealthKafka();
  const system = useSystemHealth();
  const comp = health.data?.components;

  return (
    <>
      <PageHeader title="System health" description="Availability checks run by the backend on each request. Refreshes every 5 s." />
      <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        <HealthCard
          title="Backend API"
          icon={Server}
          query={health}
          pick={() => ({
            c: { status: health.data!.status, latency_ms: null, detail: '' },
            facts: [
              ['Version', health.data!.version],
              ['Uptime', fmtDuration(health.data!.uptime_s)],
            ],
          })}
        />
        <HealthCard
          title="PostgreSQL"
          icon={Database}
          query={db}
          pick={() => ({
            c: db.data,
            facts: db.data?.pool ? [['Pool (size / out / overflow)', `${db.data.pool.size} / ${db.data.pool.checked_out} / ${db.data.pool.overflow}`]] : [],
          })}
        />
        <HealthCard
          title="Redis"
          icon={HardDrive}
          query={redis}
          pick={() => ({
            c: redis.data,
            facts: [
              ['Used memory', fmtBytes(redis.data?.used_memory_bytes)],
              ['Clients', fmtInt(redis.data?.connected_clients)],
            ],
          })}
        />
        <HealthCard
          title="Kafka"
          icon={Network}
          query={kafka}
          pick={() => {
            const k = kafka.data!;
            const brokers = Array.isArray(k.brokers) ? k.brokers.length : k.brokers;
            return {
              c: k,
              facts: [
                ['Brokers', fmtInt(brokers)],
                ['Topics', Object.entries(k.topics ?? {}).map(([t, p]) => `${t} (${p})`).join(', ') || '—'],
                ['Missing topics', k.missing_topics?.length ? <span className="text-negative">{k.missing_topics.join(', ')}</span> : 'none'],
                ['Consumer lag', k.consumer_lag === null ? 'n/a' : fmtInt(k.consumer_lag)],
              ],
            };
          }}
        />
        <HealthCard title="Data ingestion" icon={Zap} query={health} pick={() => ({ c: comp?.ingestion })} />
        <HealthCard title="Stream processor" icon={Workflow} query={health} pick={() => ({ c: comp?.processor })} />
        <HealthCard
          title="WebSocket service"
          icon={Radio}
          query={health}
          pick={() => ({
            c: comp?.websocket,
            facts: system.data
              ? [
                  ['Connections', fmtInt(system.data.websocket.connections)],
                  ['Redis listener', system.data.websocket.listener_connected ? 'connected' : <span className="text-negative">disconnected</span>],
                ]
              : [],
          })}
        />
        <BrowserSocketCard />
      </div>
      <QueryState query={system} skeleton={<Skeleton className="h-48" />}>
        {(s) => <SystemSection s={s} />}
      </QueryState>
    </>
  );
}
