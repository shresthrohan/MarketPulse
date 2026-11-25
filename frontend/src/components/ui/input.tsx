import { forwardRef, type InputHTMLAttributes, type SelectHTMLAttributes } from 'react';
import { cn } from '@/utils/cn';

const field =
  'h-8 rounded-md border bg-panel px-2.5 text-xs text-foreground transition-colors placeholder:text-muted-foreground focus-visible:border-secondary focus-visible:outline-none disabled:opacity-50';

export const Input = forwardRef<HTMLInputElement, InputHTMLAttributes<HTMLInputElement>>(({ className, ...p }, ref) => (
  <input ref={ref} className={cn(field, 'w-full min-w-0', className)} {...p} />
));
Input.displayName = 'Input';

/** shadcn "native-select": the platform <select>, styled to match. */
export const NativeSelect = forwardRef<HTMLSelectElement, SelectHTMLAttributes<HTMLSelectElement>>(({ className, ...p }, ref) => (
  <select ref={ref} className={cn(field, 'cursor-pointer pr-6', className)} {...p} />
));
NativeSelect.displayName = 'NativeSelect';
