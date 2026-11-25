import { cva, type VariantProps } from 'class-variance-authority';
import type { HTMLAttributes } from 'react';
import { cn } from '@/utils/cn';

const badgeVariants = cva(
  'inline-flex items-center gap-1 whitespace-nowrap rounded border px-1.5 py-px text-[10px] font-semibold uppercase tracking-wider',
  {
    variants: {
      variant: {
        default: 'border-transparent bg-panel text-muted-foreground',
        outline: 'text-muted-foreground',
        positive: 'border-positive/30 bg-positive/10 text-positive',
        negative: 'border-negative/30 bg-negative/10 text-negative',
        warning: 'border-warning/30 bg-warning/10 text-warning',
        info: 'border-secondary/30 bg-secondary/10 text-secondary',
      },
    },
    defaultVariants: { variant: 'default' },
  },
);

export type BadgeVariant = NonNullable<VariantProps<typeof badgeVariants>['variant']>;

export const Badge = ({ className, variant, ...p }: HTMLAttributes<HTMLSpanElement> & VariantProps<typeof badgeVariants>) => (
  <span className={cn(badgeVariants({ variant }), className)} {...p} />
);
