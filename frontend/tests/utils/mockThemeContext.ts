/**
 * Mock ThemeContext for testing themes
 *
 * Owner: First Builder (Shared Resource)
 */

import { vi } from 'vitest';

type Theme = 'light' | 'dark' | 'cyberpunk' | 'synthwave' | 'retro' | 'valentine' | 'night';

interface ThemeContextType {
  theme: Theme;
  setTheme: (theme: Theme) => void;
}

export const mockLightThemeContext: ThemeContextType = {
  theme: 'light',
  setTheme: vi.fn(),
};

export const mockDarkThemeContext: ThemeContextType = {
  theme: 'dark',
  setTheme: vi.fn(),
};

export const mockCyberpunkThemeContext: ThemeContextType = {
  theme: 'cyberpunk',
  setTheme: vi.fn(),
};

export const mockSynthwaveThemeContext: ThemeContextType = {
  theme: 'synthwave',
  setTheme: vi.fn(),
};
