import { Toaster as Sonner } from 'sonner';
import { useTheme } from '@/hooks/useTheme';

export function Toaster() {
  const theme = useTheme();
  return <Sonner theme={theme} position="bottom-right" richColors closeButton toastOptions={{ className: 'font-sans text-xs' }} />;
}
