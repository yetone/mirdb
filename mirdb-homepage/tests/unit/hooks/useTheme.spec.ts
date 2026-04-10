/**
 * Unit Tests for useTheme Hook
 * Owner: Scenario 16 - Dark/Light Mode Toggle
 */

import { renderHook, act } from '@testing-library/react';
import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';
import { useTheme } from '../../../src/hooks/useTheme';
import { THEME_STORAGE_KEY } from '../../../src/utils/constants';

describe('useTheme Hook - Scenario 16', () => {
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
  let matchMediaMock: ReturnType<typeof vi.fn>;
  let mediaQueryListeners: Array<(e: MediaQueryListEvent) => void> = [];

  beforeEach(() => {
    // Reset localStorage mock
    localStorageMock.clear();
    Object.defineProperty(window, 'localStorage', {
      value: localStorageMock,
      writable: true,
    });

    // Reset matchMedia mock
    mediaQueryListeners = [];
    matchMediaMock = vi.fn((query: string) => ({
      matches: query.includes('dark') ? false : true, // Default to light mode
      media: query,
      onchange: null,
      addListener: vi.fn(),
      removeListener: vi.fn(),
      addEventListener: vi.fn((_event: string, listener: (e: MediaQueryListEvent) => void) => {
        mediaQueryListeners.push(listener);
      }),
      removeEventListener: vi.fn((_event: string, listener: (e: MediaQueryListEvent) => void) => {
        mediaQueryListeners = mediaQueryListeners.filter((l) => l !== listener);
      }),
      dispatchEvent: vi.fn(),
    }));
    Object.defineProperty(window, 'matchMedia', {
      value: matchMediaMock,
      writable: true,
    });

    // Reset document attribute
    document.documentElement.removeAttribute('data-theme');
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  describe('TC4: Theme preference stored in browser localStorage', () => {
    it('should store theme preference in localStorage when toggled', () => {
      const { result } = renderHook(() => useTheme());

      // Toggle theme
      act(() => {
        result.current.toggleTheme();
      });

      // Verify localStorage was called with correct key
      expect(localStorageMock.setItem).toHaveBeenCalledWith(
        THEME_STORAGE_KEY,
        expect.any(String)
      );
    });

    it('should store correct theme value in localStorage', () => {
      const { result } = renderHook(() => useTheme());

      // Get initial theme
      const initialTheme = result.current.theme;

      // Toggle theme
      act(() => {
        result.current.toggleTheme();
      });

      // Expected new theme
      const expectedTheme = initialTheme === 'light' ? 'dark' : 'light';

      // Verify correct value was stored
      expect(localStorageMock.setItem).toHaveBeenCalledWith(
        THEME_STORAGE_KEY,
        expectedTheme
      );
    });

    it('should use stored theme preference from localStorage on mount', () => {
      // Pre-set localStorage
      localStorageMock.getItem.mockReturnValue('dark');

      const { result } = renderHook(() => useTheme());

      // Should use stored preference
      expect(result.current.theme).toBe('dark');
    });

    it('should persist theme using setTheme function', () => {
      const { result } = renderHook(() => useTheme());

      act(() => {
        result.current.setTheme('dark');
      });

      expect(localStorageMock.setItem).toHaveBeenCalledWith(
        THEME_STORAGE_KEY,
        'dark'
      );
      expect(result.current.theme).toBe('dark');
    });
  });

  describe('Theme initialization', () => {
    it('should default to light theme when no preference is stored and system prefers light', () => {
      // Ensure no stored preference
      localStorageMock.getItem.mockReturnValue(null);

      // Mock system preference to light
      matchMediaMock.mockReturnValue({
        matches: false, // false means light mode (prefers-color-scheme: dark = false)
        media: '(prefers-color-scheme: dark)',
        addEventListener: vi.fn(),
        removeEventListener: vi.fn(),
      });

      const { result } = renderHook(() => useTheme());

      expect(result.current.theme).toBe('light');
    });

    it('should default to dark theme when no preference is stored and system prefers dark', () => {
      // Ensure no stored preference
      localStorageMock.getItem.mockReturnValue(null);

      // Mock system preference to dark
      matchMediaMock.mockReturnValue({
        matches: true, // true means dark mode (prefers-color-scheme: dark = true)
        media: '(prefers-color-scheme: dark)',
        addEventListener: vi.fn(),
        removeEventListener: vi.fn(),
      });

      const { result } = renderHook(() => useTheme());

      expect(result.current.theme).toBe('dark');
    });
  });

  describe('Theme toggling', () => {
    it('should toggle from light to dark', () => {
      localStorageMock.getItem.mockReturnValue('light');

      const { result } = renderHook(() => useTheme());

      expect(result.current.theme).toBe('light');

      act(() => {
        result.current.toggleTheme();
      });

      expect(result.current.theme).toBe('dark');
    });

    it('should toggle from dark to light', () => {
      localStorageMock.getItem.mockReturnValue('dark');

      const { result } = renderHook(() => useTheme());

      expect(result.current.theme).toBe('dark');

      act(() => {
        result.current.toggleTheme();
      });

      expect(result.current.theme).toBe('light');
    });
  });

  describe('Document attribute application', () => {
    it('should apply data-theme attribute to document', () => {
      localStorageMock.getItem.mockReturnValue('light');

      renderHook(() => useTheme());

      expect(document.documentElement.getAttribute('data-theme')).toBe('light');
    });

    it('should update data-theme attribute when theme changes', () => {
      localStorageMock.getItem.mockReturnValue('light');

      const { result } = renderHook(() => useTheme());

      expect(document.documentElement.getAttribute('data-theme')).toBe('light');

      act(() => {
        result.current.toggleTheme();
      });

      expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
    });
  });

  describe('setTheme function', () => {
    it('should allow setting theme directly to dark', () => {
      localStorageMock.getItem.mockReturnValue('light');

      const { result } = renderHook(() => useTheme());

      act(() => {
        result.current.setTheme('dark');
      });

      expect(result.current.theme).toBe('dark');
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
    });

    it('should allow setting theme directly to light', () => {
      localStorageMock.getItem.mockReturnValue('dark');

      const { result } = renderHook(() => useTheme());

      act(() => {
        result.current.setTheme('light');
      });

      expect(result.current.theme).toBe('light');
      expect(document.documentElement.getAttribute('data-theme')).toBe('light');
    });
  });

  describe('Return value structure', () => {
    it('should return theme, toggleTheme, and setTheme', () => {
      const { result } = renderHook(() => useTheme());

      expect(result.current).toHaveProperty('theme');
      expect(result.current).toHaveProperty('toggleTheme');
      expect(result.current).toHaveProperty('setTheme');
      expect(typeof result.current.theme).toBe('string');
      expect(typeof result.current.toggleTheme).toBe('function');
      expect(typeof result.current.setTheme).toBe('function');
    });
  });
});
