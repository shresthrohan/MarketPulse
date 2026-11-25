import type { HTMLAttributes, TdHTMLAttributes, ThHTMLAttributes } from 'react';
import { cn } from '@/utils/cn';

/** Scrolls horizontally inside its card so the page itself never does. */
export const Table = ({ className, ...p }: HTMLAttributes<HTMLTableElement>) => (
  <div className="relative w-full overflow-x-auto">
    <table className={cn('w-full border-collapse text-xs', className)} {...p} />
  </div>
);

export const TableHeader = (p: HTMLAttributes<HTMLTableSectionElement>) => <thead {...p} />;
export const TableBody = (p: HTMLAttributes<HTMLTableSectionElement>) => <tbody {...p} />;

export const TableRow = ({ className, ...p }: HTMLAttributes<HTMLTableRowElement>) => (
  <tr className={cn('border-b transition-colors last:border-0 hover:bg-panel/60', className)} {...p} />
);

export const TableHead = ({ className, ...p }: ThHTMLAttributes<HTMLTableCellElement>) => (
  <th
    className={cn(
      'h-8 whitespace-nowrap border-b bg-panel/50 px-3 text-left align-middle text-[10px] font-semibold uppercase tracking-wider text-muted-foreground',
      className,
    )}
    {...p}
  />
);

export const TableCell = ({ className, ...p }: TdHTMLAttributes<HTMLTableCellElement>) => (
  <td className={cn('h-9 whitespace-nowrap px-3 align-middle', className)} {...p} />
);
