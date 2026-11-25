import { Tabs, TabsList, TabsTrigger } from '@/components/ui/tabs';

/** Segmented control for ranges / views. */
export function RangeTabs<T extends string | number>({
  value,
  options,
  onChange,
  label,
  format = String,
}: {
  value: T;
  options: readonly T[];
  onChange: (v: T) => void;
  label: string;
  format?: (v: T) => string;
}) {
  return (
    <Tabs value={String(value)} onValueChange={(v) => onChange(options.find((o) => String(o) === v) ?? value)}>
      <TabsList aria-label={label}>
        {options.map((o) => (
          <TabsTrigger key={String(o)} value={String(o)}>
            {format(o)}
          </TabsTrigger>
        ))}
      </TabsList>
    </Tabs>
  );
}
