import { Badge, type BadgeVariant } from '@/components/ui/badge';
import { cn } from '@/utils/cn';

/** Maps any backend status word to a tone. Unknown words render neutral, never "healthy". */
export function statusVariant(status: string | null | undefined): BadgeVariant {
  switch (status) {
    case 'up':
    case 'ok':
    case 'valid':
    case 'info':
    case 'open':
      return 'positive';
    case 'degraded':
    case 'retried':
    case 'warning':
    case 'reconnecting':
    case 'connecting':
      return 'warning';
    case 'down':
    case 'error':
    case 'rejected':
    case 'failed':
    case 'closed':
      return 'negative';
    default:
      return 'default';
  }
}

const dotClass: Record<BadgeVariant, string> = {
  positive: 'bg-positive',
  warning: 'bg-warning',
  negative: 'bg-negative',
  info: 'bg-secondary',
  default: 'bg-muted-foreground',
  outline: 'bg-muted-foreground',
};

export function StatusDot({ status, className }: { status: string | null | undefined; className?: string }) {
  const v = statusVariant(status);
  return (
    <span className={cn('relative inline-flex size-2 shrink-0', className)} aria-hidden>
      {v === 'positive' && <span className={cn('absolute inset-0 animate-ping rounded-full opacity-50', dotClass[v])} />}
      <span className={cn('relative inline-flex size-2 rounded-full', dotClass[v])} />
    </span>
  );
}

export function StatusBadge({ status, label, className }: { status: string | null | undefined; label?: string; className?: string }) {
  return (
    <Badge variant={statusVariant(status)} className={className}>
      <StatusDot status={status} className="size-1.5 [&>span]:size-1.5" />
      {label ?? status ?? 'unknown'}
    </Badge>
  );
}
