import { BrowserRouter, Route, Routes } from 'react-router-dom';
import { LiveProvider } from '@/components/LiveProvider';
import { EmptyState } from '@/components/States';
import { Toaster } from '@/components/ui/sonner';
import { TooltipProvider } from '@/components/ui/tooltip';
import { Button } from '@/components/ui/button';
import { Link } from 'react-router-dom';
import { AppLayout } from '@/layouts/AppLayout';
import { AnalyticsPage } from '@/pages/Analytics';
import { ExplorerPage } from '@/pages/Explorer';
import { HealthPage } from '@/pages/Health';
import { InstrumentIndex, InstrumentPage } from '@/pages/Instrument';
import { LogsPage } from '@/pages/Logs';
import { MarketsPage } from '@/pages/Markets';
import { OverviewPage } from '@/pages/Overview';
import { PipelinePage } from '@/pages/Pipeline';
import { WatchlistsPage } from '@/pages/Watchlists';

export function App() {
  return (
    <TooltipProvider delayDuration={250}>
      <LiveProvider>
        <BrowserRouter future={{ v7_startTransition: true, v7_relativeSplatPath: true }}>
          <Routes>
            <Route element={<AppLayout />}>
              <Route index element={<OverviewPage />} />
              <Route path="markets" element={<MarketsPage />} />
              <Route path="instruments" element={<InstrumentIndex />} />
              <Route path="instruments/:symbol" element={<InstrumentPage />} />
              <Route path="analytics" element={<AnalyticsPage />} />
              <Route path="watchlists" element={<WatchlistsPage />} />
              <Route path="explorer" element={<ExplorerPage />} />
              <Route path="pipeline" element={<PipelinePage />} />
              <Route path="health" element={<HealthPage />} />
              <Route path="logs" element={<LogsPage />} />
              <Route
                path="*"
                element={
                  <EmptyState
                    title="Page not found"
                    description="That route does not exist."
                    action={
                      <Button asChild size="sm" variant="secondary">
                        <Link to="/">Back to overview</Link>
                      </Button>
                    }
                  />
                }
              />
            </Route>
          </Routes>
        </BrowserRouter>
        <Toaster />
      </LiveProvider>
    </TooltipProvider>
  );
}
