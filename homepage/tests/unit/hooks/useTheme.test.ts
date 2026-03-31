/**
 * Unit tests for useTheme hook
 * Owner: Scenario 7 - Dark Mode and Theming
 *
 * Tests:
 * - Initial theme detection (system preference)
 * - Theme toggling
 * - Theme persistence via localStorage
 * - Body class updates
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import { useTheme } from '../../../src/hooks/useTheme';

// Mock localStorage
const localStorageMock = (() => {
  let store: Record<string, string> = {};
  return {
    getItem: vi.fn((key: string) => store[key] || null),
    setItem: vi.fn((key: string, value: string) => {
      store[key] = value;
    }),
    removeItem: vi.fn((key: string) => {
      delete store[key];
    }),
    clear: vi.fn(() => {
      store = {};
    }),
  };
})();

// Mock matchMedia
const createMatchMediaMock = (matches: boolean) => {
  return vi.fn().mockImplementation((query: string) => ({
    matches: query.includes('dark') ? matches : !matches,
    media: query,
    onchange: null,
    addListener: vi.fn(),
    removeListener: vi.fn(),
    addEventListener: vi.fn(),
    removeEventListener: vi.fn(),
    dispatchEvent: vi.fn(),
  }));
};

describe('useTheme', () => {
  beforeEach(() => {
    // Reset mocks
    Object.defineProperty(window, 'localStorage', { value: localStorageMock });
    localStorageMock.clear();
    vi.clearAllMocks();

    // Remove dark class from document
    document.documentElement.classList.remove('dark');
    document.documentElement.classList.remove('theme-transition');
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe('initial theme detection', () => {
    it('returns current theme and toggle function', () => {
      window.matchMedia = createMatchMediaMock(false);

      const { result } = renderHook(() => useTheme());

      expect(result.current.theme).toBeDefined();
      expect(typeof result.current.toggleTheme).toBe('function');
      expect(typeof result.current.setTheme).toBe('function');
      expect(result.current.systemTheme).toBeDefined();
    });

    it('respects system dark mode preference', () => {
      window.matchMedia = createMatchMediaMock(true);

      const { result } = renderHook(() => useTheme());

      expect(result.current.theme).toBe('dark');
      expect(result.current.systemTheme).toBe('dark');
    });

    it('respects system light mode preference', () => {
      window.matchMedia = createMatchMediaMock(false);

      const { result } = renderHook(() => useTheme());

      expect(result.current.theme).toBe('light');
      expect(result.current.systemTheme).toBe('light');
    });

    it('uses stored theme over system preference', () => {
      window.matchMedia = createMatchMediaMock(true); // System prefers dark
      localStorageMock.setItem('mirdb-theme', 'light'); // But user chose light

      const { result } = renderHook(() => useTheme());

      expect(result.current.theme).toBe('light');
    });
  });

  describe('toggleTheme', () => {
    it('toggles from light to dark', () => {
      window.matchMedia = createMatchMediaMock(false);

      const { result } = renderHook(() => useTheme());

      expect(result.current.theme).toBe('light');

      act(() => {
        result.current.toggleTheme();
      });

      expect(result.current.theme).toBe('dark');
    });

    it('toggles from dark to light', () => {
      window.matchMedia = createMatchMediaMock(true);

      const { result } = renderHook(() => useTheme());

      expect(result.current.theme).toBe('dark');

      act(() => {
        result.current.toggleTheme();
      });

      expect(result.current.theme).toBe('light');
    });

    it('updates body class when toggling', () => {
      window.matchMedia = createMatchMediaMock(false);

      const { result } = renderHook(() => useTheme());

      act(() => {
        result.current.toggleTheme();
      });

      expect(document.documentElement.classList.contains('dark')).toBe(true);

      act(() => {
        result.current.toggleTheme();
      });

      expect(document.documentElement.classList.contains('dark')).toBe(false);
    });

    it('persists theme choice to localStorage', () => {
      window.matchMedia = createMatchMediaMock(false);

      const { result } = renderHook(() => useTheme());

      act(() => {
        result.current.toggleTheme();
      });

      expect(localStorageMock.setItem).toHaveBeenCalledWith('mirdb-theme', 'dark');
    });
  });

  describe('setTheme', () => {
    it('sets theme to dark', () => {
      window.matchMedia = createMatchMediaMock(false);

      const { result } = renderHook(() => useTheme());

      act(() => {
        result.current.setTheme('dark');
      });

      expect(result.current.theme).toBe('dark');
      expect(document.documentElement.classList.contains('dark')).toBe(true);
    });

    it('sets theme to light', () => {
      window.matchMedia = createMatchMediaMock(true);

      const { result } = renderHook(() => useTheme());

      act(() => {
        result.current.setTheme('light');
      });

      expect(result.current.theme).toBe('light');
      expect(document.documentElement.classList.contains('dark')).toBe(false);
    });

    it('persists theme choice to localStorage', () => {
      window.matchMedia = createMatchMediaMock(false);

      const { result } = renderHook(() => useTheme());

      act(() => {
        result.current.setTheme('dark');
      });

      expect(localStorageMock.setItem).toHaveBeenCalledWith('mirdb-theme', 'dark');
    });
  });

  describe('theme persistence', () => {
    it('reads theme from localStorage on mount', () => {
      window.matchMedia = createMatchMediaMock(false); // System is light
      localStorageMock.setItem('mirdb-theme', 'dark'); // But stored is dark

      const { result } = renderHook(() => useTheme());

      expect(result.current.theme).toBe('dark');
    });

    it('handles invalid localStorage value', () => {
      window.matchMedia = createMatchMediaMock(false);
      localStorageMock.setItem('mirdb-theme', 'invalid');

      const { result } = renderHook(() => useTheme());

      // Should fall back to system preference
      expect(result.current.theme).toBe('light');
    });
  });

  describe('theme transition', () => {
    it('adds transition class when toggling theme', async () => {
      vi.useFakeTimers();
      window.matchMedia = createMatchMediaMock(false);

      const { result } = renderHook(() => useTheme());

      act(() => {
        result.current.toggleTheme();
      });

      expect(document.documentElement.classList.contains('theme-transition')).toBe(true);

      // Wait for transition to complete
      act(() => {
        vi.advanceTimersByTime(300);
      });

      expect(document.documentElement.classList.contains('theme-transition')).toBe(false);

      vi.useRealTimers();
    });
  });
});
