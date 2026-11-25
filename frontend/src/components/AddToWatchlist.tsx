import { Check, Loader2, Plus, Star } from 'lucide-react';
import { useState, type FormEvent } from 'react';
import { Link } from 'react-router-dom';
import { Button } from '@/components/ui/button';
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle, DialogTrigger } from '@/components/ui/dialog';
import { Input } from '@/components/ui/input';
import { Skeleton } from '@/components/ui/skeleton';
import { Hint } from '@/components/ui/tooltip';
import { ErrorState } from '@/components/States';
import { useWatchlistMutation, useWatchlists } from '@/hooks/queries';
import { api } from '@/services/api';

export function AddToWatchlistButton({ symbol, label }: { symbol: string; label?: boolean }) {
  const [open, setOpen] = useState(false);
  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <Hint content={`Add ${symbol} to a watchlist`} asChild>
        <DialogTrigger asChild>
          <Button
            variant={label ? 'secondary' : 'ghost'}
            size={label ? 'sm' : 'icon-sm'}
            aria-label={`Add ${symbol} to watchlist`}
            onClick={(e) => e.stopPropagation()}
          >
            <Star />
            {label && 'Watch'}
          </Button>
        </DialogTrigger>
      </Hint>
      {/* stopPropagation: React events bubble out of portals into the clickable table row */}
      <DialogContent onClick={(e) => e.stopPropagation()}>
        <DialogHeader>
          <DialogTitle>Add {symbol} to watchlist</DialogTitle>
          <DialogDescription>Pick a watchlist, or create one with {symbol} in it.</DialogDescription>
        </DialogHeader>
        {open && <WatchlistPicker symbol={symbol} />}
      </DialogContent>
    </Dialog>
  );
}

function WatchlistPicker({ symbol }: { symbol: string }) {
  const lists = useWatchlists();
  const [name, setName] = useState('');
  const add = useWatchlistMutation(
    ({ id }: { id: number; name: string }) => api.addToWatchlist(id, symbol),
    (v) => `Added ${symbol} to ${v.name}`,
  );
  const create = useWatchlistMutation(
    async (n: string) => api.addToWatchlist((await api.createWatchlist(n)).id, symbol),
    (n) => `Created ${n} with ${symbol}`,
  );
  const submit = (e: FormEvent) => {
    e.preventDefault();
    if (name.trim()) create.mutate(name.trim(), { onSuccess: () => setName('') });
  };

  return (
    <div className="grid gap-3">
      {lists.isPending ? (
        <Skeleton className="h-20" />
      ) : lists.isError ? (
        <ErrorState error={lists.error} onRetry={() => void lists.refetch()} className="py-4" />
      ) : lists.data.items.length === 0 ? (
        <p className="rounded-md border border-dashed px-3 py-4 text-center text-[11px] text-muted-foreground">
          No watchlists yet. Create one below or on the{' '}
          <Link to="/watchlists" className="text-secondary hover:underline">
            Watchlists
          </Link>{' '}
          page.
        </p>
      ) : (
        <ul className="divide-y rounded-md border">
          {lists.data.items.map((w) => {
            const has = w.instruments.some((i) => i.symbol === symbol);
            const busy = add.isPending && add.variables?.id === w.id;
            return (
              <li key={w.id} className="flex items-center justify-between gap-2 px-3 py-1.5">
                <span className="min-w-0 truncate">
                  {w.name} <span className="text-muted-foreground">· {w.instruments.length}</span>
                </span>
                {has ? (
                  <span className="inline-flex items-center gap-1 text-[11px] text-positive">
                    <Check className="size-3.5" /> In list
                  </span>
                ) : (
                  <Button size="sm" variant="secondary" disabled={busy} onClick={() => add.mutate({ id: w.id, name: w.name })}>
                    {busy ? <Loader2 className="animate-spin" /> : <Plus />} Add
                  </Button>
                )}
              </li>
            );
          })}
        </ul>
      )}
      <form onSubmit={submit} className="flex gap-2">
        <Input value={name} onChange={(e) => setName(e.target.value)} placeholder="New watchlist name" maxLength={64} aria-label="New watchlist name" />
        <Button type="submit" disabled={!name.trim() || create.isPending}>
          {create.isPending ? <Loader2 className="animate-spin" /> : <Plus />} Create
        </Button>
      </form>
    </div>
  );
}
