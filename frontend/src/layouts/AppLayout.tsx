import * as DialogPrimitive from '@radix-ui/react-dialog';
import {
  CandlestickChart,
  Database,
  HeartPulse,
  LayoutDashboard,
  Menu,
  Moon,
  ScrollText,
  Sigma,
  Star,
  Sun,
  Table2,
  Workflow,
  type LucideIcon,
} from 'lucide-react';
import { useState } from 'react';
import { NavLink, Outlet, useLocation, useNavigate } from 'react-router-dom';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Skeleton } from '@/components/ui/skeleton';
import { Hint, Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip';
import { InstrumentSearch } from '@/components/InstrumentSearch';
import { StatusBadge, StatusDot } from '@/components/StatusBadge';
import { useMarketOverview, usePipelineServices } from '@/hooks/queries';
import { useLastMessageAt, useWsStatus } from '@/hooks/useLive';
import { setTheme, useTheme } from '@/hooks/useTheme';
import { cn } from '@/utils/cn';
import { fmtTime } from '@/utils/format';
import { wsStatusText } from '@/services/ws';

const NAV: { to: string; label: string; icon: LucideIcon; end?: boolean }[] = [
  { to: '/', label: 'Overview', icon: LayoutDashboard, end: true },
  { to: '/markets', label: 'Markets', icon: Table2 },
  { to: '/instruments', label: 'Instrument', icon: CandlestickChart },
  { to: '/analytics', label: 'Analytics', icon: Sigma },
  { to: '/watchlists', label: 'Watchlists', icon: Star },
  { to: '/explorer', label: 'Data Explorer', icon: Database },
  { to: '/pipeline', label: 'Pipeline Monitor', icon: Workflow },
  { to: '/health', label: 'System Health', icon: HeartPulse },
  { to: '/logs', label: 'Event Logs', icon: ScrollText },
];

function Nav({ rail, onNavigate }: { rail?: boolean; onNavigate?: () => void }) {
  return (
    <nav aria-label="Main" className="flex flex-col gap-0.5 p-2">
      {NAV.map(({ to, label, icon: Icon, end }) => {
        const link = (
          <NavLink
            key={to}
            to={to}
            end={end}
            onClick={onNavigate}
            className={({ isActive }) =>
              cn(
                'relative flex h-8 items-center gap-2.5 rounded-md px-2.5 text-xs font-medium transition-colors',
                isActive ? 'bg-panel text-foreground' : 'text-muted-foreground hover:bg-panel/60 hover:text-foreground',
                rail && 'justify-center px-0 lg:justify-start lg:px-2.5',
              )
            }
          >
            {({ isActive }) => (
              <>
                {isActive && <span className="absolute inset-y-1.5 left-0 w-0.5 rounded-full bg-primary" aria-hidden />}
                <Icon className={cn('size-4 shrink-0', isActive && 'text-primary')} aria-hidden />
                <span className={cn(rail && 'sr-only lg:not-sr-only')}>{label}</span>
              </>
            )}
          </NavLink>
        );
        return rail ? (
          <Tooltip key={to}>
            {/* wrapper: Slot can't merge NavLink's function className */}
            <TooltipTrigger asChild>
              <span className="block">{link}</span>
            </TooltipTrigger>
            <TooltipContent side="right" className="lg:hidden">
              {label}
            </TooltipContent>
          </Tooltip>
        ) : (
          link
        );
      })}
    </nav>
  );
}

function Logo({ name }: { name?: boolean }) {
  return (
    <NavLink to="/" className="flex shrink-0 items-center gap-2" aria-label="MarketPulse home">
      <svg viewBox="0 0 32 32" className="size-6" aria-hidden>
        <rect width="32" height="32" rx="7" className="fill-panel" />
        <path d="M5 18h5l3-8 5 14 3-6h6" fill="none" stroke="var(--primary)" strokeWidth="2.6" strokeLinecap="round" strokeLinejoin="round" />
      </svg>
      <span className={cn('text-sm font-semibold tracking-tight', !name && 'hidden sm:inline')}>
        Market<span className="text-primary">Pulse</span>
      </span>
    </NavLink>
  );
}

/** STREAMING only when the ingestion service itself reports "up". */
function MarketStatus() {
  const services = usePipelineServices();
  if (services.isPending) return <Skeleton className="h-5 w-20" />;
  // A failed refresh keeps the previous data in the query cache; never present that stale status as live.
  const offline = services.isError;
  const ing = offline ? undefined : services.data?.items.find((s) => s.key === 'ingestion');
  const status = offline ? 'down' : (ing?.status ?? 'down');
  const label = offline ? 'Offline' : status === 'up' ? 'Streaming' : status === 'degraded' ? 'Degraded' : 'Halted';
  const why = services.isError
    ? `Pipeline status unavailable: ${services.error.message}`
    : ing
      ? `${ing.name}: ${ing.detail}`
      : 'Ingestion service not reported by the backend';
  return (
    <Hint content={why}>
      <StatusBadge status={status} label={label} />
    </Hint>
  );
}

const MODE_VARIANT = { simulated: 'warning', live: 'positive', delayed: 'info' } as const;

function DataSource() {
  const overview = useMarketOverview();
  const lastMsg = useLastMessageAt();
  if (overview.isPending) return <Skeleton className="h-5 w-24" />;
  const modes = overview.data?.data_modes ?? [];
  const sources = overview.data?.sources ?? [];
  const last = lastMsg ?? overview.data?.last_update ?? null;
  return (
    <>
      {overview.isError && !overview.data ? (
        <Hint content={overview.error.message}>
          <Badge variant="outline">Mode unknown</Badge>
        </Hint>
      ) : modes.length === 0 ? (
        <Badge variant="outline">No data</Badge>
      ) : (
        modes.map((m) => (
          <Hint key={m} content={`Data mode reported by the API: ${m}`}>
            <Badge variant={MODE_VARIANT[m as keyof typeof MODE_VARIANT] ?? 'outline'}>{m}</Badge>
          </Hint>
        ))
      )}
      {sources.length > 0 && (
        <span className="hidden text-[11px] text-muted-foreground md:inline">
          SRC <span className="text-foreground">{sources.join(', ')}</span>
        </span>
      )}
      <span className="hidden text-[11px] text-muted-foreground xl:inline">
        Updated <span className="text-foreground">{fmtTime(last)}</span> UTC
      </span>
    </>
  );
}

function WsIndicator() {
  const status = useWsStatus();
  return (
    <Hint content={wsStatusText(status)}>
      <span className="inline-flex items-center gap-1.5 text-[11px] text-muted-foreground">
        <StatusDot status={status.state} />
        <span className="hidden lg:inline">WS</span>
      </span>
    </Hint>
  );
}

function ThemeToggle() {
  const theme = useTheme();
  const next = theme === 'dark' ? 'light' : 'dark';
  return (
    <Hint content={`Switch to ${next} theme`} asChild>
      <Button variant="ghost" size="icon-sm" onClick={() => setTheme(next)} aria-label={`Switch to ${next} theme`}>
        {theme === 'dark' ? <Sun /> : <Moon />}
      </Button>
    </Hint>
  );
}

export function AppLayout() {
  const [drawer, setDrawer] = useState(false);
  const navigate = useNavigate();
  const { pathname } = useLocation();

  return (
    <div className="flex h-dvh flex-col overflow-hidden">
      <header className="flex shrink-0 flex-wrap items-center gap-x-3 gap-y-2 border-b bg-sidebar px-3 py-2 md:h-12 md:flex-nowrap md:py-0">
        <Button variant="ghost" size="icon-sm" className="md:hidden" onClick={() => setDrawer(true)} aria-label="Open navigation">
          <Menu />
        </Button>
        <Logo />
        <InstrumentSearch
          className="order-last w-full md:order-none md:ml-4 md:w-72 lg:w-80"
          onSelect={(s) => navigate(`/instruments/${encodeURIComponent(s)}`)}
        />
        <div className="ml-auto flex items-center gap-2.5">
          <MarketStatus />
          <DataSource />
          <WsIndicator />
          <ThemeToggle />
        </div>
      </header>

      <div className="flex min-h-0 flex-1">
        <aside className="hidden w-14 shrink-0 flex-col overflow-y-auto border-r bg-sidebar md:flex lg:w-52">
          <Nav rail />
        </aside>
        <main className="relative min-w-0 flex-1 overflow-y-auto overflow-x-hidden">
          <div key={pathname} className="mx-auto max-w-[1680px] animate-fade-in space-y-3 p-3 md:p-4">
            <Outlet />
          </div>
        </main>
      </div>

      <DialogPrimitive.Root open={drawer} onOpenChange={setDrawer}>
        <DialogPrimitive.Portal>
          <DialogPrimitive.Overlay className="fixed inset-0 z-50 animate-fade-in bg-black/60 md:hidden" />
          <DialogPrimitive.Content className="fixed inset-y-0 left-0 z-50 w-64 animate-slide-in border-r bg-sidebar shadow-2xl md:hidden">
            <DialogPrimitive.Title className="flex h-12 items-center border-b px-3">
              <Logo name />
            </DialogPrimitive.Title>
            <DialogPrimitive.Description className="sr-only">Site navigation</DialogPrimitive.Description>
            <Nav onNavigate={() => setDrawer(false)} />
          </DialogPrimitive.Content>
        </DialogPrimitive.Portal>
      </DialogPrimitive.Root>
    </div>
  );
}
