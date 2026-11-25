import { Loader2, Search } from 'lucide-react';
import { useId, useState, type KeyboardEvent } from 'react';
import { Input } from '@/components/ui/input';
import { useDebounced } from '@/hooks/useDebounced';
import { useInstrumentSearch } from '@/hooks/queries';
import { cn } from '@/utils/cn';

/** Type-ahead over /api/instruments?search=. Enter picks the highlighted match, or the typed symbol if none loaded yet. */
export function InstrumentSearch({
  onSelect,
  placeholder = 'Search symbol or company…',
  className,
  exclude = [],
}: {
  onSelect: (symbol: string) => void;
  placeholder?: string;
  className?: string;
  exclude?: string[];
}) {
  const [term, setTerm] = useState('');
  const [open, setOpen] = useState(false);
  const [active, setActive] = useState(0);
  const q = useDebounced(term.trim(), 200);
  const search = useInstrumentSearch(q);
  const items = (search.data?.items ?? []).filter((r) => !exclude.includes(r.symbol));
  const listId = useId();
  const show = open && q.length > 0;

  const choose = (symbol: string) => {
    onSelect(symbol.toUpperCase());
    setTerm('');
    setOpen(false);
  };

  const onKeyDown = (e: KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
      e.preventDefault();
      setOpen(true);
      if (items.length) setActive((a) => (a + (e.key === 'ArrowDown' ? 1 : items.length - 1)) % items.length);
    } else if (e.key === 'Enter') {
      e.preventDefault();
      const pick = items[active]?.symbol ?? term.trim();
      if (pick) choose(pick);
    } else if (e.key === 'Escape') {
      setOpen(false);
    }
  };

  return (
    <div className={cn('relative', className)}>
      <Search className="pointer-events-none absolute left-2.5 top-1/2 size-3.5 -translate-y-1/2 text-muted-foreground" aria-hidden />
      <Input
        role="combobox"
        aria-expanded={show}
        aria-controls={listId}
        aria-autocomplete="list"
        aria-label="Search instruments"
        value={term}
        placeholder={placeholder}
        className="pl-8"
        onChange={(e) => {
          setTerm(e.target.value);
          setActive(0);
          setOpen(true);
        }}
        onFocus={() => setOpen(true)}
        onBlur={() => setOpen(false)}
        onKeyDown={onKeyDown}
      />
      {show && (
        <ul
          id={listId}
          role="listbox"
          className="absolute left-0 right-0 top-full z-40 mt-1 max-h-72 overflow-y-auto rounded-md border bg-card py-1 shadow-xl shadow-black/30"
        >
          {search.isFetching && !items.length ? (
            <li className="flex items-center gap-2 px-3 py-2 text-muted-foreground">
              <Loader2 className="size-3.5 animate-spin" /> Searching…
            </li>
          ) : search.isError ? (
            <li className="px-3 py-2 text-negative">{search.error.message}</li>
          ) : !items.length ? (
            <li className="px-3 py-2 text-muted-foreground">No instruments match “{q}”</li>
          ) : (
            items.map((r, i) => (
              <li
                key={r.id}
                role="option"
                aria-selected={i === active}
                // mousedown (not click) so the input's blur doesn't close the list first
                onMouseDown={(e) => {
                  e.preventDefault();
                  choose(r.symbol);
                }}
                onMouseEnter={() => setActive(i)}
                className={cn('flex cursor-pointer items-center gap-2 px-3 py-1.5', i === active && 'bg-panel')}
              >
                <span className="w-14 shrink-0 font-semibold">{r.symbol}</span>
                <span className="truncate text-muted-foreground">{r.company_name}</span>
                <span className="ml-auto shrink-0 text-[10px] uppercase text-muted-foreground">{r.exchange}</span>
              </li>
            ))
          )}
        </ul>
      )}
    </div>
  );
}
