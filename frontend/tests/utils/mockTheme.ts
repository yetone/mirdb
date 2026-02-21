/**
 * Theme mocking utilities for tests.
 * Owner: First scenario builder
 *
 * Expected exports:
 * - mockThemeContext(theme: Theme): void
 * - createMockThemeProvider(theme?: Theme): React.FC
 * - getAvailableThemes(): Theme[]
 */

import { Theme } from '@/types';
import { vi } from 'vitest';

const availableThemes: Theme[] = ['light', 'dark', 'cyberpunk', 'synthwave', 'retro', 'valentine'];

export function getAvailableThemes(): Theme[] {
  return availableThemes;
}

export function mockThemeContext(theme: Theme = 'light') {
  return {
    theme,
    setTheme: vi.fn(),
  };
}

export function createMockThemeValue(theme: Theme = 'light') {
  return {
    theme,
    setTheme: vi.fn(),
  };
}
