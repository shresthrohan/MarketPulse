import type { HTMLAttributes } from 'react';
import { cn } from '@/utils/cn';

export const Card = ({ className, ...p }: HTMLAttributes<HTMLDivElement>) => (
  <div className={cn('min-w-0 rounded-lg border bg-card', className)} {...p} />
);

export const CardHeader = ({ className, ...p }: HTMLAttributes<HTMLDivElement>) => (
  <div className={cn('flex min-h-10 flex-wrap items-center justify-between gap-2 border-b px-3 py-1.5', className)} {...p} />
);

export const CardTitle = ({ className, ...p }: HTMLAttributes<HTMLHeadingElement>) => (
  <h2 className={cn('text-[11px] font-semibold uppercase tracking-[0.08em] text-muted-foreground', className)} {...p} />
);

export const CardContent = ({ className, ...p }: HTMLAttributes<HTMLDivElement>) => <div className={cn('p-3', className)} {...p} />;
