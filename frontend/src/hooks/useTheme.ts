import { useSyncExternalStore } from 'react';

export type Theme = 'dark' | 'light';

const STORAGE_KEY = 'marketpulse-theme';
const listeners = new Set<() => void>();
const root = () => document.documentElement;
const current = (): Theme => (root().classList.contains('dark') ? 'dark' : 'light');

export function setTheme(theme: Theme): void {
  root().classList.toggle('dark', theme === 'dark');
  try {
    localStorage.setItem(STORAGE_KEY, theme);
  } catch {
    // storage blocked (private mode): the choice just isn't remembered
  }
  listeners.forEach((l) => l());
}

export function useTheme(): Theme {
  return useSyncExternalStore((l) => {
    listeners.add(l);
    return () => void listeners.delete(l);
  }, current);
}
