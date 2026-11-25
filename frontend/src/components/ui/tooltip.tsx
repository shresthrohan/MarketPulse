import * as TooltipPrimitive from '@radix-ui/react-tooltip';
import type { ComponentPropsWithoutRef, ReactNode } from 'react';
import { cn } from '@/utils/cn';

export const TooltipProvider = TooltipPrimitive.Provider;
export const Tooltip = TooltipPrimitive.Root;
export const TooltipTrigger = TooltipPrimitive.Trigger;

export const TooltipContent = ({ className, sideOffset = 4, ...p }: ComponentPropsWithoutRef<typeof TooltipPrimitive.Content>) => (
  <TooltipPrimitive.Portal>
    <TooltipPrimitive.Content
      sideOffset={sideOffset}
      className={cn(
        'z-50 max-w-sm animate-fade-in break-words rounded-md border bg-card px-2 py-1 text-[11px] text-foreground shadow-lg shadow-black/30',
        className,
      )}
      {...p}
    />
  </TooltipPrimitive.Portal>
);

/** Tooltip on any element; non-interactive children get a focusable wrapper so keyboard users can reach it. */
export function Hint({ content, children, asChild }: { content: ReactNode; children: ReactNode; asChild?: boolean }) {
  if (content === null || content === undefined || content === '') return <>{children}</>;
  return (
    <Tooltip>
      <TooltipTrigger asChild>{asChild ? children : <span tabIndex={0}>{children}</span>}</TooltipTrigger>
      <TooltipContent>{content}</TooltipContent>
    </Tooltip>
  );
}
