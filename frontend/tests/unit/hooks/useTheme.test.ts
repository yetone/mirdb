/**
 * Unit Tests for useTheme Hook
 * Owner: Scenario 5 - Dark/Light Mode Theme Support
 *
 * Tests theme detection and system preference handling
 *
 * Requirements: REQ-5, US-4
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { renderHook, act, waitFor } from '@testing-library/react';
import { useTheme, Theme } from '../../../src/hooks/useTheme';

// Helper to create mock matchMedia
function createMockMatchMedia(prefersDark: boolean) {
  const listeners: Array<(event: MediaQueryListEvent) => void> = [];

  const mockMatchMedia = vi.fn((query: string) => ({
    matches: query === '(prefers-color-scheme: dark)' ? prefersDark : false,
    media: query,
    onchange: null,
    addListener: vi.fn((listener: (event: MediaQueryListEvent) => void) => {
      listeners.push(listener);
    }),
    removeListener: vi.fn((listener: (event: MediaQueryListEvent) => void) => {
      const index = listeners.indexOf(listener);
      if (index > -1) listeners.splice(index, 1);
    }),
    addEventListener: vi.fn((event: string, listener: (event: MediaQueryListEvent) => void) => {
      if (event === 'change') listeners.push(listener);
    }),
    removeEventListener: vi.fn((event: string, listener: (event: MediaQueryListEvent) => void) => {
      if (event === 'change') {
        const index = listeners.indexOf(listener);
        if (index > -1) listeners.splice(index, 1);
      }
    }),
    dispatchEvent: vi.fn(),
  }));

  return {
    mockMatchMedia,
    triggerChange: (newValue: boolean) => {
      listeners.forEach((listener) => {
        listener({ matches: newValue } as MediaQueryListEvent);
      });
    },
  };
}

describe('useTheme', () => {
  let originalMatchMedia: typeof window.matchMedia;

  beforeEach(() => {
    originalMatchMedia = window.matchMedia;
    document.documentElement.removeAttribute('data-theme');
  });

  afterEach(() => {
    window.matchMedia = originalMatchMedia;
    document.documentElement.removeAttribute('data-theme');
    vi.clearAllMocks();
  });

  describe('initial state', () => {
    it('detects light mode system preference', () => {
      const { mockMatchMedia } = createMockMatchMedia(false);
      window.matchMedia = mockMatchMedia;

      const { result } = renderHook(() => useTheme());

      expect(result.current.systemPreference).toBe('light');
      expect(result.current.theme).toBe('light');
    });

    it('detects dark mode system preference', () => {
      const { mockMatchMedia } = createMockMatchMedia(true);
      window.matchMedia = mockMatchMedia;

      const { result } = renderHook(() => useTheme());

      expect(result.current.systemPreference).toBe('dark');
      expect(result.current.theme).toBe('dark');
    });
  });

  describe('theme application', () => {
    it('applies light theme to document', () => {
      const { mockMatchMedia } = createMockMatchMedia(false);
      window.matchMedia = mockMatchMedia;

      renderHook(() => useTheme());

      expect(document.documentElement.getAttribute('data-theme')).toBe('light');
    });

    it('applies dark theme to document', () => {
      const { mockMatchMedia } = createMockMatchMedia(true);
      window.matchMedia = mockMatchMedia;

      renderHook(() => useTheme());

      expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
    });
  });

  describe('setTheme', () => {
    it('allows manually setting theme to dark', () => {
      const { mockMatchMedia } = createMockMatchMedia(false);
      window.matchMedia = mockMatchMedia;

      const { result } = renderHook(() => useTheme());

      act(() => {
        result.current.setTheme('dark');
      });

      expect(result.current.theme).toBe('dark');
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
    });

    it('allows manually setting theme to light', () => {
      const { mockMatchMedia } = createMockMatchMedia(true);
      window.matchMedia = mockMatchMedia;

      const { result } = renderHook(() => useTheme());

      act(() => {
        result.current.setTheme('light');
      });

      expect(result.current.theme).toBe('light');
      expect(document.documentElement.getAttribute('data-theme')).toBe('light');
    });
  });

  describe('system preference change', () => {
    it('updates theme when system preference changes from light to dark', async () => {
      const { mockMatchMedia, triggerChange } = createMockMatchMedia(false);
      window.matchMedia = mockMatchMedia;

      const { result } = renderHook(() => useTheme());

      expect(result.current.theme).toBe('light');
      expect(result.current.systemPreference).toBe('light');

      act(() => {
        triggerChange(true);
      });

      await waitFor(() => {
        expect(result.current.theme).toBe('dark');
        expect(result.current.systemPreference).toBe('dark');
        expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
      });
    });

    it('updates theme when system preference changes from dark to light', async () => {
      const { mockMatchMedia, triggerChange } = createMockMatchMedia(true);
      window.matchMedia = mockMatchMedia;

      const { result } = renderHook(() => useTheme());

      expect(result.current.theme).toBe('dark');
      expect(result.current.systemPreference).toBe('dark');

      act(() => {
        triggerChange(false);
      });

      await waitFor(() => {
        expect(result.current.theme).toBe('light');
        expect(result.current.systemPreference).toBe('light');
        expect(document.documentElement.getAttribute('data-theme')).toBe('light');
      });
    });
  });

  describe('ThemeContext provides current theme value', () => {
    it('returns consistent theme and systemPreference values', () => {
      const { mockMatchMedia } = createMockMatchMedia(true);
      window.matchMedia = mockMatchMedia;

      const { result } = renderHook(() => useTheme());

      expect(result.current.theme).toBe('dark');
      expect(result.current.systemPreference).toBe('dark');
      expect(typeof result.current.setTheme).toBe('function');
    });

    it('provides all expected return values', () => {
      const { mockMatchMedia } = createMockMatchMedia(false);
      window.matchMedia = mockMatchMedia;

      const { result } = renderHook(() => useTheme());

      expect(result.current).toHaveProperty('theme');
      expect(result.current).toHaveProperty('systemPreference');
      expect(result.current).toHaveProperty('setTheme');
    });
  });
});
