import type { HTMLAttributes } from 'react';
import { cn } from '@/utils/cn';

export const Skeleton = ({ className, ...p }: HTMLAttributes<HTMLDivElement>) => (
  <div className={cn('animate-pulse rounded bg-panel', className)} {...p} />
);
