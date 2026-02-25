import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import { useTheme } from '../../../src/hooks/useTheme';

describe('useTheme Hook', () => {
  const originalMatchMedia = window.matchMedia;
  const originalLocalStorage = window.localStorage;

  let mockLocalStorage: { [key: string]: string };

  beforeEach(() => {
    // Reset localStorage mock
    mockLocalStorage = {};

    Object.defineProperty(window, 'localStorage', {
      value: {
        getItem: vi.fn((key: string) => mockLocalStorage[key] || null),
        setItem: vi.fn((key: string, value: string) => {
          mockLocalStorage[key] = value;
        }),
        removeItem: vi.fn((key: string) => {
          delete mockLocalStorage[key];
        }),
        clear: vi.fn(() => {
          mockLocalStorage = {};
        }),
      },
      writable: true,
      configurable: true,
    });

    // Default to light system preference
    Object.defineProperty(window, 'matchMedia', {
      value: vi.fn().mockImplementation((query: string) => ({
        matches: false,
        media: query,
        onchange: null,
        addListener: vi.fn(),
        removeListener: vi.fn(),
        addEventListener: vi.fn(),
        removeEventListener: vi.fn(),
        dispatchEvent: vi.fn(),
      })),
      writable: true,
      configurable: true,
    });

    // Clear any existing data-theme attribute
    document.documentElement.removeAttribute('data-theme');
  });

  afterEach(() => {
    vi.clearAllMocks();
    document.documentElement.removeAttribute('data-theme');
  });

  // Test Case 4: Call useTheme hook - Returns current theme and toggleTheme function
  describe('Test Case 4: Hook returns current theme and toggleTheme function', () => {
    it('returns theme, toggleTheme, and setTheme', () => {
      const { result } = renderHook(() => useTheme());

      expect(result.current).toHaveProperty('theme');
      expect(result.current).toHaveProperty('toggleTheme');
      expect(result.current).toHaveProperty('setTheme');
      expect(typeof result.current.theme).toBe('string');
      expect(typeof result.current.toggleTheme).toBe('function');
      expect(typeof result.current.setTheme).toBe('function');
    });

    it('theme is either light or dark', () => {
      const { result } = renderHook(() => useTheme());

      expect(['light', 'dark']).toContain(result.current.theme);
    });
  });

  // Test Case 5: Call toggleTheme when current theme is light - Theme changes to dark and localStorage is updated
  describe('Test Case 5: Toggle from light to dark', () => {
    it('changes theme from light to dark when toggled', () => {
      const { result } = renderHook(() => useTheme());

      // Ensure we start with light theme
      if (result.current.theme !== 'light') {
        act(() => {
          result.current.setTheme('light');
        });
      }

      expect(result.current.theme).toBe('light');

      act(() => {
        result.current.toggleTheme();
      });

      expect(result.current.theme).toBe('dark');
    });

    it('updates localStorage when toggling from light to dark', () => {
      const { result } = renderHook(() => useTheme());

      // Ensure we start with light theme
      act(() => {
        result.current.setTheme('light');
      });

      act(() => {
        result.current.toggleTheme();
      });

      expect(localStorage.setItem).toHaveBeenCalledWith('mirdb-theme-preference', 'dark');
    });
  });

  // Test Case 6: Call toggleTheme when current theme is dark - Theme changes to light and localStorage is updated
  describe('Test Case 6: Toggle from dark to light', () => {
    it('changes theme from dark to light when toggled', () => {
      const { result } = renderHook(() => useTheme());

      // Set to dark theme first
      act(() => {
        result.current.setTheme('dark');
      });

      expect(result.current.theme).toBe('dark');

      act(() => {
        result.current.toggleTheme();
      });

      expect(result.current.theme).toBe('light');
    });

    it('updates localStorage when toggling from dark to light', () => {
      const { result } = renderHook(() => useTheme());

      // Set to dark theme first
      act(() => {
        result.current.setTheme('dark');
      });

      // Clear mock to check next call
      vi.mocked(localStorage.setItem).mockClear();

      act(() => {
        result.current.toggleTheme();
      });

      expect(localStorage.setItem).toHaveBeenCalledWith('mirdb-theme-preference', 'light');
    });
  });

  // Test Case 7: Initialize useTheme with existing localStorage value - Hook returns stored theme preference
  describe('Test Case 7: Initialize with existing localStorage value', () => {
    it('returns dark theme when localStorage has dark preference', () => {
      mockLocalStorage['mirdb-theme-preference'] = 'dark';

      const { result } = renderHook(() => useTheme());

      expect(result.current.theme).toBe('dark');
    });

    it('returns light theme when localStorage has light preference', () => {
      mockLocalStorage['mirdb-theme-preference'] = 'light';

      const { result } = renderHook(() => useTheme());

      expect(result.current.theme).toBe('light');
    });

    it('ignores invalid localStorage values', () => {
      mockLocalStorage['mirdb-theme-preference'] = 'invalid';

      const { result } = renderHook(() => useTheme());

      // Should fall back to system preference (light in our mock)
      expect(['light', 'dark']).toContain(result.current.theme);
    });
  });

  // Test Case 8: Initialize useTheme with system preference dark and no localStorage - Hook returns dark theme
  describe('Test Case 8: Initialize with system preference dark', () => {
    it('returns dark theme when system prefers dark and no localStorage value', () => {
      // Mock system preference to dark
      Object.defineProperty(window, 'matchMedia', {
        value: vi.fn().mockImplementation((query: string) => ({
          matches: query === '(prefers-color-scheme: dark)',
          media: query,
          onchange: null,
          addListener: vi.fn(),
          removeListener: vi.fn(),
          addEventListener: vi.fn(),
          removeEventListener: vi.fn(),
          dispatchEvent: vi.fn(),
        })),
        writable: true,
        configurable: true,
      });

      const { result } = renderHook(() => useTheme());

      expect(result.current.theme).toBe('dark');
    });

    it('returns light theme when system prefers light and no localStorage value', () => {
      // matchMedia already mocked to return false for dark scheme
      const { result } = renderHook(() => useTheme());

      expect(result.current.theme).toBe('light');
    });
  });

  // Additional tests for setTheme functionality
  describe('setTheme function', () => {
    it('sets theme to dark using setTheme', () => {
      const { result } = renderHook(() => useTheme());

      act(() => {
        result.current.setTheme('dark');
      });

      expect(result.current.theme).toBe('dark');
      expect(localStorage.setItem).toHaveBeenCalledWith('mirdb-theme-preference', 'dark');
    });

    it('sets theme to light using setTheme', () => {
      const { result } = renderHook(() => useTheme());

      act(() => {
        result.current.setTheme('light');
      });

      expect(result.current.theme).toBe('light');
      expect(localStorage.setItem).toHaveBeenCalledWith('mirdb-theme-preference', 'light');
    });
  });

  // Test data-theme attribute on document
  describe('data-theme attribute', () => {
    it('applies data-theme attribute to document element', () => {
      const { result } = renderHook(() => useTheme());

      // Initial theme should be applied
      expect(document.documentElement.getAttribute('data-theme')).toBe(result.current.theme);
    });

    it('updates data-theme attribute when theme changes', () => {
      const { result } = renderHook(() => useTheme());

      act(() => {
        result.current.setTheme('dark');
      });

      expect(document.documentElement.getAttribute('data-theme')).toBe('dark');

      act(() => {
        result.current.setTheme('light');
      });

      expect(document.documentElement.getAttribute('data-theme')).toBe('light');
    });
  });
});
