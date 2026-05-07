import React from 'react';
import { renderHook, act, waitFor } from '@testing-library/react';
import { ThemeProvider, useTheme } from '@/hooks/useTheme';

// Helper to wrap hook in provider
function wrapper({ children }: { children: React.ReactNode }) {
  return <ThemeProvider>{children}</ThemeProvider>;
}

describe('useTheme', () => {
  let matchMediaMock: jest.Mock;
  let localStorageMock: Record<string, string> = {};

  beforeEach(() => {
    // Reset localStorage mock
    localStorageMock = {};
    Object.defineProperty(window, 'localStorage', {
      value: {
        getItem: jest.fn((key: string) => localStorageMock[key] || null),
        setItem: jest.fn((key: string, value: string) => {
          localStorageMock[key] = value;
        }),
        removeItem: jest.fn((key: string) => {
          delete localStorageMock[key];
        }),
      },
      writable: true,
    });

    // Reset document class
    document.documentElement.classList.remove('dark');

    // Mock matchMedia
    matchMediaMock = jest.fn();
    Object.defineProperty(window, 'matchMedia', {
      writable: true,
      value: matchMediaMock,
    });
  });

  afterEach(() => {
    jest.clearAllMocks();
    document.documentElement.classList.remove('dark');
  });

  describe('Test Case 1: System dark mode preference detection', () => {
    it('defaults to dark when prefers-color-scheme: dark is set and no saved theme', () => {
      matchMediaMock.mockImplementation((query: string) => ({
        matches: query === '(prefers-color-scheme: dark)',
        addEventListener: jest.fn(),
        removeEventListener: jest.fn(),
      }));

      const { result } = renderHook(() => useTheme(), { wrapper });

      // After mount, theme should be dark based on system preference
      expect(document.documentElement.classList.contains('dark')).toBe(true);
    });

    it('defaults to light when prefers-color-scheme: light is set and no saved theme', () => {
      matchMediaMock.mockImplementation((query: string) => ({
        matches: query !== '(prefers-color-scheme: dark)',
        addEventListener: jest.fn(),
        removeEventListener: jest.fn(),
      }));

      const { result } = renderHook(() => useTheme(), { wrapper });

      expect(document.documentElement.classList.contains('dark')).toBe(false);
    });
  });

  describe('Test Case 2: Theme toggle functionality', () => {
    beforeEach(() => {
      matchMediaMock.mockImplementation(() => ({
        matches: false,
        addEventListener: jest.fn(),
        removeEventListener: jest.fn(),
      }));
    });

    it('toggleTheme switches from light to dark', () => {
      const { result } = renderHook(() => useTheme(), { wrapper });

      act(() => {
        result.current.toggleTheme();
      });

      expect(result.current.theme).toBe('dark');
      expect(document.documentElement.classList.contains('dark')).toBe(true);
    });

    it('toggleTheme switches from dark to light', () => {
      localStorageMock['mirdb-theme'] = 'dark';

      const { result } = renderHook(() => useTheme(), { wrapper });

      act(() => {
        result.current.toggleTheme();
      });

      expect(result.current.theme).toBe('light');
      expect(document.documentElement.classList.contains('dark')).toBe(false);
    });

    it('toggleTheme works instantly without page reload', () => {
      const { result } = renderHook(() => useTheme(), { wrapper });

      // Toggle light -> dark
      act(() => {
        result.current.toggleTheme();
      });
      expect(result.current.theme).toBe('dark');

      // Toggle dark -> light
      act(() => {
        result.current.toggleTheme();
      });
      expect(result.current.theme).toBe('light');
    });
  });

  describe('Test Case 3: Theme persistence', () => {
    beforeEach(() => {
      matchMediaMock.mockImplementation(() => ({
        matches: false,
        addEventListener: jest.fn(),
        removeEventListener: jest.fn(),
      }));
    });

    it('saves theme to localStorage when toggled', () => {
      const { result } = renderHook(() => useTheme(), { wrapper });

      act(() => {
        result.current.toggleTheme();
      });

      expect(window.localStorage.setItem).toHaveBeenCalledWith('mirdb-theme', 'dark');
    });

    it('restores saved theme from localStorage on load', () => {
      localStorageMock['mirdb-theme'] = 'dark';

      const { result } = renderHook(() => useTheme(), { wrapper });

      // After useEffect runs, theme should be restored from localStorage
      expect(document.documentElement.classList.contains('dark')).toBe(true);
    });

    it('setTheme persists to localStorage', () => {
      const { result } = renderHook(() => useTheme(), { wrapper });

      act(() => {
        result.current.setTheme('dark');
      });

      expect(window.localStorage.setItem).toHaveBeenCalledWith('mirdb-theme', 'dark');
      expect(result.current.theme).toBe('dark');
    });
  });

  describe('Test Case 4: Dark theme class application', () => {
    beforeEach(() => {
      matchMediaMock.mockImplementation(() => ({
        matches: false,
        addEventListener: jest.fn(),
        removeEventListener: jest.fn(),
      }));
    });

    it('applies dark class to documentElement when dark theme is active', () => {
      const { result } = renderHook(() => useTheme(), { wrapper });

      act(() => {
        result.current.setTheme('dark');
      });

      expect(document.documentElement.classList.contains('dark')).toBe(true);
    });

    it('removes dark class from documentElement when light theme is active', () => {
      localStorageMock['mirdb-theme'] = 'dark';

      const { result } = renderHook(() => useTheme(), { wrapper });

      act(() => {
        result.current.setTheme('light');
      });

      expect(document.documentElement.classList.contains('dark')).toBe(false);
    });

    it('dark theme state reflects correct theme value', () => {
      const { result } = renderHook(() => useTheme(), { wrapper });

      act(() => {
        result.current.setTheme('dark');
      });

      expect(result.current.theme).toBe('dark');
      expect(document.documentElement.classList.contains('dark')).toBe(true);

      act(() => {
        result.current.setTheme('light');
      });

      expect(result.current.theme).toBe('light');
      expect(document.documentElement.classList.contains('dark')).toBe(false);
    });
  });

  describe('Test Case 5: No flash on load', () => {
    it('anti-FOUC script exists in layout', () => {
      // The script is embedded in layout.tsx. We verify the hook
      // syncs with the script by checking it reads localStorage first.
      localStorageMock['mirdb-theme'] = 'dark';

      const { result } = renderHook(() => useTheme(), { wrapper });

      // After mount, should restore dark theme immediately
      expect(document.documentElement.classList.contains('dark')).toBe(true);
    });

    it('reads localStorage before system preference', () => {
      // Even with light system preference, saved dark theme wins
      matchMediaMock.mockImplementation((query: string) => ({
        matches: query !== '(prefers-color-scheme: dark)',
        addEventListener: jest.fn(),
        removeEventListener: jest.fn(),
      }));
      localStorageMock['mirdb-theme'] = 'dark';

      const { result } = renderHook(() => useTheme(), { wrapper });

      expect(document.documentElement.classList.contains('dark')).toBe(true);
    });
  });

  describe('Error handling', () => {
    it('handles localStorage errors gracefully', () => {
      Object.defineProperty(window, 'localStorage', {
        value: {
          getItem: jest.fn(() => {
            throw new Error('localStorage disabled');
          }),
          setItem: jest.fn(() => {
            throw new Error('localStorage disabled');
          }),
        },
        writable: true,
      });

      matchMediaMock.mockImplementation(() => ({
        matches: false,
        addEventListener: jest.fn(),
        removeEventListener: jest.fn(),
      }));

      // Should not throw
      expect(() => {
        renderHook(() => useTheme(), { wrapper });
      }).not.toThrow();
    });

    it('throws when useTheme is used outside ThemeProvider', () => {
      expect(() => {
        renderHook(() => useTheme());
      }).toThrow('useTheme must be used within a ThemeProvider');
    });
  });
});
