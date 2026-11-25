import * as TabsPrimitive from '@radix-ui/react-tabs';
import type { ComponentPropsWithoutRef } from 'react';
import { cn } from '@/utils/cn';

export const Tabs = TabsPrimitive.Root;

export const TabsList = ({ className, ...p }: ComponentPropsWithoutRef<typeof TabsPrimitive.List>) => (
  <TabsPrimitive.List className={cn('inline-flex h-7 max-w-full items-center gap-0.5 overflow-x-auto rounded-md border bg-panel p-0.5', className)} {...p} />
);

export const TabsTrigger = ({ className, ...p }: ComponentPropsWithoutRef<typeof TabsPrimitive.Trigger>) => (
  <TabsPrimitive.Trigger
    className={cn(
      'inline-flex h-full cursor-pointer items-center justify-center whitespace-nowrap rounded px-2 text-[11px] font-medium text-muted-foreground transition-colors hover:text-foreground disabled:opacity-50 data-[state=active]:bg-card data-[state=active]:text-foreground data-[state=active]:shadow-sm',
      className,
    )}
    {...p}
  />
);
