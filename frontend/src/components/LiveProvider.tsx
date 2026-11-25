import { useEffect, type ReactNode } from 'react';
import { toast } from 'sonner';
import { liveStore } from '@/services/liveStore';
import { MarketSocket, wsUrl } from '@/services/ws';

/** Owns the app's single market-data WebSocket and feeds the live store. */
export function LiveProvider({ children }: { children: ReactNode }) {
  useEffect(() => {
    const socket = new MarketSocket(wsUrl('/ws/market'), {
      onStatus: liveStore.setStatus,
      onMessage: (msg) => {
        if (msg.type === 'error') toast.error(`Market stream: ${msg.message}`);
        else liveStore.handleMessage(msg);
      },
    });
    // ponytail: one "*" subscription covers the tens-of-instruments universe; switch to ref-counted per-symbol subscriptions if it grows to thousands.
    socket.subscribe(['*']);
    socket.start();
    return () => socket.stop();
  }, []);
  return <>{children}</>;
}
