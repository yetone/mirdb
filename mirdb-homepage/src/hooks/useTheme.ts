import type { Theme } from '../types';

export function useTheme(): { theme: Theme; toggleTheme: () => void } {
  // To be implemented by Scenario 9
  return { theme: 'light', toggleTheme: () => {} };
}
