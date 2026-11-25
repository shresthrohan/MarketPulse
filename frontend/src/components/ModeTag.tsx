import { Badge } from '@/components/ui/badge';
import { Hint } from '@/components/ui/tooltip';
import { useMarketOverview } from '@/hooks/queries';
import type { InstrumentRow } from '@/types/api';

const LABEL: Record<string, string> = { simulated: 'SIM', live: 'LIVE', delayed: 'DELAYED' };
const VARIANT = { simulated: 'warning', live: 'positive', delayed: 'info' } as const;

/** Data source of one instrument (the instrument's recorded mode, else its latest quote's). */
export function rowMode(row: Pick<InstrumentRow, 'data_mode' | 'quote'>): string | null {
  return row.data_mode ?? row.quote?.data_mode ?? null;
}

/**
 * Per-row data-source tag. Shown only when the terminal holds more than one kind of data
 * (e.g. simulated equities next to live Binance crypto); with a single source the header badge
 * already says it, and tagging every row would be noise.
 */
export function ModeTag({ mode, always = false }: { mode: string | null; always?: boolean }) {
  const overview = useMarketOverview();
  const mixed = (overview.data?.data_modes.length ?? 0) > 1;
  if (!mode || (!mixed && !always)) return null;
  const text = mode === 'simulated' ? 'Simulated data, not market prices' : `Real market data (${mode})`;
  return (
    <Hint content={text}>
      <Badge variant={VARIANT[mode as keyof typeof VARIANT] ?? 'outline'} className="ml-1.5 align-middle">
        {LABEL[mode] ?? mode}
      </Badge>
    </Hint>
  );
}
