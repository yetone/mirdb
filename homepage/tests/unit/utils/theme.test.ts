/**
 * Unit tests for theme utility.
 * Owner: Scenario 9 - Dark Mode Support
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { getTheme, setTheme, toggleTheme, initTheme } from '../../../src/utils/theme';

describe('theme utility', () => {
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
  const matchMediaMock = vi.fn((query: string) => ({
    matches: false,
    media: query,
    onchange: null,
    addListener: vi.fn(),
    removeListener: vi.fn(),
    addEventListener: vi.fn(),
    removeEventListener: vi.fn(),
    dispatchEvent: vi.fn(),
  }));

  beforeEach(() => {
    // Set up mocks
    Object.defineProperty(window, 'localStorage', {
      value: localStorageMock,
      writable: true,
    });
    Object.defineProperty(window, 'matchMedia', {
      value: matchMediaMock,
      writable: true,
    });

    // Clear localStorage before each test
    localStorageMock.clear();
    vi.clearAllMocks();
  });

  afterEach(() => {
    // Clean up
    document.documentElement.removeAttribute('data-theme');
  });

  describe('getTheme', () => {
    it('returns system preference when no stored preference exists', () => {
      // Test case 1: Call getTheme() with no stored preference
      // Expected: Returns system preference or default 'light'
      localStorageMock.getItem.mockReturnValue(null);
      matchMediaMock.mockReturnValue({
        matches: false, // prefers-color-scheme: dark is false, so light
        media: '(prefers-color-scheme: dark)',
        onchange: null,
        addListener: vi.fn(),
        removeListener: vi.fn(),
        addEventListener: vi.fn(),
        removeEventListener: vi.fn(),
        dispatchEvent: vi.fn(),
      });

      const theme = getTheme();
      expect(theme).toBe('light');
    });

    it('returns dark when system prefers dark and no stored preference', () => {
      localStorageMock.getItem.mockReturnValue(null);
      matchMediaMock.mockReturnValue({
        matches: true, // prefers-color-scheme: dark
        media: '(prefers-color-scheme: dark)',
        onchange: null,
        addListener: vi.fn(),
        removeListener: vi.fn(),
        addEventListener: vi.fn(),
        removeEventListener: vi.fn(),
        dispatchEvent: vi.fn(),
      });

      const theme = getTheme();
      expect(theme).toBe('dark');
    });

    it('returns stored preference when available', () => {
      localStorageMock.getItem.mockReturnValue('dark');

      const theme = getTheme();
      expect(theme).toBe('dark');
    });

    it('ignores invalid stored values', () => {
      localStorageMock.getItem.mockReturnValue('invalid');
      matchMediaMock.mockReturnValue({
        matches: false,
        media: '(prefers-color-scheme: dark)',
        onchange: null,
        addListener: vi.fn(),
        removeListener: vi.fn(),
        addEventListener: vi.fn(),
        removeEventListener: vi.fn(),
        dispatchEvent: vi.fn(),
      });

      const theme = getTheme();
      expect(theme).toBe('light');
    });
  });

  describe('setTheme', () => {
    it('sets theme to dark and saves to localStorage', () => {
      // Test case 2: Call setTheme('dark')
      // Expected: Theme is set to dark and saved to localStorage
      setTheme('dark');

      expect(localStorageMock.setItem).toHaveBeenCalledWith('mirdb-theme', 'dark');
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
    });

    it('sets theme to light and saves to localStorage', () => {
      setTheme('light');

      expect(localStorageMock.setItem).toHaveBeenCalledWith('mirdb-theme', 'light');
      expect(document.documentElement.getAttribute('data-theme')).toBe('light');
    });
  });

  describe('toggleTheme', () => {
    it('toggles from light to dark', () => {
      // Test case 3: Call toggleTheme() when theme is 'light'
      // Expected: Theme changes to 'dark'
      localStorageMock.getItem.mockReturnValue('light');
      matchMediaMock.mockReturnValue({
        matches: false,
        media: '(prefers-color-scheme: dark)',
        onchange: null,
        addListener: vi.fn(),
        removeListener: vi.fn(),
        addEventListener: vi.fn(),
        removeEventListener: vi.fn(),
        dispatchEvent: vi.fn(),
      });

      toggleTheme();

      expect(localStorageMock.setItem).toHaveBeenCalledWith('mirdb-theme', 'dark');
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
    });

    it('toggles from dark to light', () => {
      localStorageMock.getItem.mockReturnValue('dark');

      toggleTheme();

      expect(localStorageMock.setItem).toHaveBeenCalledWith('mirdb-theme', 'light');
      expect(document.documentElement.getAttribute('data-theme')).toBe('light');
    });
  });

  describe('initTheme', () => {
    it('applies stored theme on initialization', () => {
      localStorageMock.getItem.mockReturnValue('dark');

      initTheme();

      expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
    });

    it('applies system preference when no stored theme', () => {
      localStorageMock.getItem.mockReturnValue(null);
      matchMediaMock.mockReturnValue({
        matches: true, // prefers dark
        media: '(prefers-color-scheme: dark)',
        onchange: null,
        addListener: vi.fn(),
        removeListener: vi.fn(),
        addEventListener: vi.fn(),
        removeEventListener: vi.fn(),
        dispatchEvent: vi.fn(),
      });

      initTheme();

      expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
    });

    it('sets up listener for system preference changes', () => {
      const addEventListener = vi.fn();
      matchMediaMock.mockReturnValue({
        matches: false,
        media: '(prefers-color-scheme: dark)',
        onchange: null,
        addListener: vi.fn(),
        removeListener: vi.fn(),
        addEventListener,
        removeEventListener: vi.fn(),
        dispatchEvent: vi.fn(),
      });
      localStorageMock.getItem.mockReturnValue(null);

      initTheme();

      expect(addEventListener).toHaveBeenCalledWith('change', expect.any(Function));
    });
  });
});
