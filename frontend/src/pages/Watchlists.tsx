import { Loader2, Pencil, Plus, Star, Trash2, X } from 'lucide-react';
import { useState, type FormEvent } from 'react';
import { Link } from 'react-router-dom';
import { ModeTag, rowMode } from '@/components/ModeTag';
import { ChangeText, FlashPrice } from '@/components/ChangeText';
import { InstrumentSearch } from '@/components/InstrumentSearch';
import { PageHeader } from '@/components/PageHeader';
import { EmptyState, ErrorState } from '@/components/States';
import { Button } from '@/components/ui/button';
import { Card, CardHeader, CardTitle } from '@/components/ui/card';
import { Dialog, DialogClose, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog';
import { Input } from '@/components/ui/input';
import { Skeleton } from '@/components/ui/skeleton';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from '@/components/ui/table';
import { Hint } from '@/components/ui/tooltip';
import { useWatchlistMutation, useWatchlists } from '@/hooks/queries';
import { useQuote } from '@/hooks/useLive';
import { api } from '@/services/api';
import type { InstrumentRow, Watchlist } from '@/types/api';
import { cn } from '@/utils/cn';
import { fmtDate, fmtDateTime, fmtVolume } from '@/utils/format';

export function WatchlistsPage() {
  const lists = useWatchlists();
  const [selectedId, setSelectedId] = useState<number | null>(null);
  const [name, setName] = useState('');
  const create = useWatchlistMutation(api.createWatchlist, (n) => `Created watchlist ${n}`);

  const items = lists.data?.items ?? [];
  const selected = items.find((w) => w.id === selectedId) ?? items[0];

  const submit = (e: FormEvent) => {
    e.preventDefault();
    const n = name.trim();
    if (n)
      create.mutate(n, {
        onSuccess: (w) => {
          setName('');
          setSelectedId(w.id);
        },
      });
  };

  return (
    <>
      <PageHeader title="Watchlists" description="Stored server-side. Prices overlay live from the WebSocket." />
      <div className="grid gap-3 lg:grid-cols-[260px_minmax(0,1fr)]">
        <Card className="self-start">
          <CardHeader>
            <CardTitle>Lists</CardTitle>
          </CardHeader>
          <form onSubmit={submit} className="flex gap-2 border-b p-3">
            <Input value={name} onChange={(e) => setName(e.target.value)} placeholder="New watchlist" maxLength={64} aria-label="New watchlist name" />
            <Button type="submit" size="icon" disabled={!name.trim() || create.isPending} aria-label="Create watchlist">
              {create.isPending ? <Loader2 className="animate-spin" /> : <Plus />}
            </Button>
          </form>
          {lists.isPending ? (
            <div className="space-y-2 p-3">
              <Skeleton className="h-7" />
              <Skeleton className="h-7" />
            </div>
          ) : lists.isError && !lists.data ? (
            <ErrorState error={lists.error} onRetry={() => void lists.refetch()} />
          ) : !items.length ? (
            <EmptyState title="No watchlists" description="Create one above." />
          ) : (
            <ul className="p-1.5">
              {items.map((w) => (
                <li key={w.id}>
                  <button
                    type="button"
                    onClick={() => setSelectedId(w.id)}
                    aria-current={selected?.id === w.id}
                    className={cn(
                      'flex w-full cursor-pointer items-center gap-2 rounded-md px-2.5 py-1.5 text-left transition-colors',
                      selected?.id === w.id ? 'bg-panel text-foreground' : 'text-muted-foreground hover:bg-panel/60 hover:text-foreground',
                    )}
                  >
                    <Star className={cn('size-3.5 shrink-0', selected?.id === w.id && 'text-primary')} />
                    <span className="min-w-0 flex-1 truncate">{w.name}</span>
                    <span className="text-[10px]">{w.instruments.length}</span>
                  </button>
                </li>
              ))}
            </ul>
          )}
        </Card>

        {selected ? (
          <WatchlistDetail key={selected.id} list={selected} />
        ) : (
          !lists.isPending &&
          !lists.isError && (
            <Card>
              <EmptyState title="Select or create a watchlist" />
            </Card>
          )
        )}
      </div>
    </>
  );
}

function WatchlistDetail({ list }: { list: Watchlist }) {
  const [renaming, setRenaming] = useState(false);
  const [deleting, setDeleting] = useState(false);
  const add = useWatchlistMutation((symbol: string) => api.addToWatchlist(list.id, symbol), (s) => `Added ${s} to ${list.name}`);
  const symbols = list.instruments.map((i) => i.symbol);

  return (
    <Card>
      <CardHeader>
        <div className="min-w-0">
          <h2 className="truncate text-sm font-semibold">{list.name}</h2>
          <p className="text-[11px] text-muted-foreground">
            {list.instruments.length} instruments · created {fmtDate(list.created_at)}
          </p>
        </div>
        <div className="flex items-center gap-1.5">
          <Button size="sm" variant="secondary" onClick={() => setRenaming(true)}>
            <Pencil /> Rename
          </Button>
          <Button size="sm" variant="ghost" className="text-negative hover:text-negative" onClick={() => setDeleting(true)}>
            <Trash2 /> Delete
          </Button>
        </div>
      </CardHeader>
      <div className="flex flex-wrap items-center gap-2 border-b px-3 py-2">
        <InstrumentSearch className="w-full sm:w-80" placeholder="Add instrument…" exclude={symbols} onSelect={(s) => add.mutate(s)} />
        {add.isPending && <Loader2 className="size-3.5 animate-spin text-muted-foreground" aria-label="Adding" />}
      </div>
      {list.instruments.length === 0 ? (
        <EmptyState title="This watchlist is empty" description="Search above to add instruments." />
      ) : (
        <Table>
          <TableHeader>
            <TableRow className="hover:bg-transparent">
              <TableHead>Symbol</TableHead>
              <TableHead>Company</TableHead>
              <TableHead className="text-right">Last</TableHead>
              <TableHead className="text-right">Chg</TableHead>
              <TableHead className="text-right">% Chg</TableHead>
              <TableHead className="text-right">Volume</TableHead>
              <TableHead>Updated</TableHead>
              <TableHead>
                <span className="sr-only">Remove</span>
              </TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {list.instruments.map((r) => (
              <WatchRow key={r.id} row={r} list={list} />
            ))}
          </TableBody>
        </Table>
      )}
      <RenameDialog list={list} open={renaming} onOpenChange={setRenaming} />
      <DeleteDialog list={list} open={deleting} onOpenChange={setDeleting} />
    </Card>
  );
}

function WatchRow({ row, list }: { row: InstrumentRow; list: Watchlist }) {
  const { quote, live } = useQuote(row);
  const remove = useWatchlistMutation(() => api.removeFromWatchlist(list.id, row.symbol), () => `Removed ${row.symbol} from ${list.name}`);
  return (
    <TableRow>
      <TableCell className="font-semibold">
        <Link to={`/instruments/${row.symbol}`} className="hover:text-secondary">
          {row.symbol}
        </Link>
        <ModeTag mode={rowMode(row)} />
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
      <TableCell className="text-muted-foreground">{quote ? fmtDateTime(quote.processed_at) : 'no quote yet'}</TableCell>
      <TableCell className="w-10 px-1 text-right">
        <Hint content={`Remove ${row.symbol}`} asChild>
          <Button variant="ghost" size="icon-sm" disabled={remove.isPending} onClick={() => remove.mutate(undefined)} aria-label={`Remove ${row.symbol}`}>
            {remove.isPending ? <Loader2 className="animate-spin" /> : <X />}
          </Button>
        </Hint>
      </TableCell>
    </TableRow>
  );
}

type DialogProps = { list: Watchlist; open: boolean; onOpenChange: (open: boolean) => void };

function RenameDialog({ list, open, onOpenChange }: DialogProps) {
  const [name, setName] = useState(list.name);
  const rename = useWatchlistMutation((n: string) => api.renameWatchlist(list.id, n), (n) => `Renamed to ${n}`);
  const submit = (e: FormEvent) => {
    e.preventDefault();
    const n = name.trim();
    if (n && n !== list.name) rename.mutate(n, { onSuccess: () => onOpenChange(false) });
  };
  return (
    <Dialog
      open={open}
      onOpenChange={(o) => {
        if (o) setName(list.name);
        onOpenChange(o);
      }}
    >
      <DialogContent>
        <form onSubmit={submit} className="grid gap-3">
          <DialogHeader>
            <DialogTitle>Rename watchlist</DialogTitle>
            <DialogDescription>1–64 characters; names must be unique.</DialogDescription>
          </DialogHeader>
          <Input value={name} onChange={(e) => setName(e.target.value)} maxLength={64} autoFocus aria-label="Watchlist name" />
          <DialogFooter>
            <DialogClose asChild>
              <Button type="button" variant="outline">
                Cancel
              </Button>
            </DialogClose>
            <Button type="submit" disabled={!name.trim() || name.trim() === list.name || rename.isPending}>
              {rename.isPending && <Loader2 className="animate-spin" />} Save
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}

function DeleteDialog({ list, open, onOpenChange }: DialogProps) {
  const del = useWatchlistMutation(() => api.deleteWatchlist(list.id), () => `Deleted ${list.name}`);
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Delete “{list.name}”?</DialogTitle>
          <DialogDescription>
            This removes the watchlist and its {list.instruments.length} instrument link{list.instruments.length === 1 ? '' : 's'}. Instruments themselves are unaffected. This cannot be undone.
          </DialogDescription>
        </DialogHeader>
        <DialogFooter>
          <DialogClose asChild>
            <Button variant="outline">Cancel</Button>
          </DialogClose>
          <Button variant="destructive" disabled={del.isPending} onClick={() => del.mutate(undefined, { onSuccess: () => onOpenChange(false) })}>
            {del.isPending && <Loader2 className="animate-spin" />} Delete
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
