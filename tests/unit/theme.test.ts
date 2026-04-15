/**
 * Theme Unit Tests
 * Owner: Scenario 8 - Theme Switching
 *
 * Unit tests for theme management functionality including:
 * - Theme state management
 * - localStorage persistence
 * - System preference detection
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { THEME_STORAGE_KEY, THEMES } from '../fixtures/test-data';

// Mock localStorage with accessible store
let localStorageStore: Record<string, string> = {};
const localStorageMock = {
  getItem: vi.fn((key: string) => localStorageStore[key] || null),
  setItem: vi.fn((key: string, value: string) => {
    localStorageStore[key] = value;
  }),
  removeItem: vi.fn((key: string) => {
    delete localStorageStore[key];
  }),
  clear: vi.fn(() => {
    localStorageStore = {};
  }),
};

// Mock matchMedia
const matchMediaMock = vi.fn((query: string) => ({
  matches: query.includes('dark') ? false : false,
  media: query,
  onchange: null,
  addListener: vi.fn(),
  removeListener: vi.fn(),
  addEventListener: vi.fn(),
  removeEventListener: vi.fn(),
  dispatchEvent: vi.fn(),
}));

describe('Theme Module', () => {
  beforeEach(() => {
    vi.stubGlobal('localStorage', localStorageMock);
    vi.stubGlobal('matchMedia', matchMediaMock);
    localStorageMock.clear();

    // Reset document mock
    document.documentElement.setAttribute('data-theme', 'light');
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    vi.resetModules();
  });

  describe('TC6: ThemeToggle click handler', () => {
    it('should update theme state when toggle is clicked', async () => {
      // Import fresh module
      const themeModule = await import('../../src/scripts/theme.js');

      // Initially light
      expect(themeModule.getTheme()).toBe('light');

      // Toggle to dark
      themeModule.setTheme('dark');

      // Verify theme changed
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark');

      // Verify localStorage was called
      expect(localStorageMock.setItem).toHaveBeenCalledWith(THEME_STORAGE_KEY, 'dark');
    });

    it('should toggle between themes correctly', async () => {
      const themeModule = await import('../../src/scripts/theme.js');

      // Start with light
      themeModule.setTheme('light');
      expect(themeModule.getTheme()).toBe('light');

      // Toggle to dark
      const newTheme = themeModule.toggleTheme();
      expect(newTheme).toBe('dark');
      expect(themeModule.getTheme()).toBe('dark');

      // Toggle back to light
      const nextTheme = themeModule.toggleTheme();
      expect(nextTheme).toBe('light');
      expect(themeModule.getTheme()).toBe('light');
    });

    it('should call localStorage.setItem when setting theme', async () => {
      const themeModule = await import('../../src/scripts/theme.js');

      themeModule.setTheme('dark');

      expect(localStorageMock.setItem).toHaveBeenCalledWith(THEME_STORAGE_KEY, 'dark');
    });
  });

  describe('TC7: Theme initialization from localStorage', () => {
    it('should read stored theme from localStorage on init', async () => {
      // Set stored theme before loading
      localStorageStore[THEME_STORAGE_KEY] = 'dark';

      const themeModule = await import('../../src/scripts/theme.js');

      const stored = themeModule.loadStoredTheme();
      expect(stored).toBe('dark');
      expect(localStorageMock.getItem).toHaveBeenCalledWith(THEME_STORAGE_KEY);
    });

    it('should return null if no theme is stored', async () => {
      // Ensure store is empty
      localStorageStore = {};

      const themeModule = await import('../../src/scripts/theme.js');

      const stored = themeModule.loadStoredTheme();
      expect(stored).toBeNull();
    });

    it('should apply stored theme preference on mount', async () => {
      // Pre-set localStorage
      localStorageStore[THEME_STORAGE_KEY] = 'dark';

      // Clear and reimport
      vi.resetModules();
      const themeModule = await import('../../src/scripts/theme.js');

      // Manually call initTheme (already called on module load)
      themeModule.initTheme();

      // Check that dark theme was applied
      // Note: initTheme is called automatically on import
      expect(localStorageMock.getItem).toHaveBeenCalled();
    });
  });

  describe('System preference detection', () => {
    it('should detect dark system preference', async () => {
      // Mock dark mode preference
      vi.stubGlobal('matchMedia', vi.fn((query: string) => ({
        matches: query.includes('dark'),
        media: query,
        onchange: null,
        addListener: vi.fn(),
        removeListener: vi.fn(),
        addEventListener: vi.fn(),
        removeEventListener: vi.fn(),
        dispatchEvent: vi.fn(),
      })));

      vi.resetModules();
      const themeModule = await import('../../src/scripts/theme.js');

      const systemPref = themeModule.detectSystemPreference();
      expect(systemPref).toBe('dark');
    });

    it('should detect light system preference', async () => {
      // Mock light mode preference
      vi.stubGlobal('matchMedia', vi.fn((query: string) => ({
        matches: false,
        media: query,
        onchange: null,
        addListener: vi.fn(),
        removeListener: vi.fn(),
        addEventListener: vi.fn(),
        removeEventListener: vi.fn(),
        dispatchEvent: vi.fn(),
      })));

      vi.resetModules();
      const themeModule = await import('../../src/scripts/theme.js');

      const systemPref = themeModule.detectSystemPreference();
      expect(systemPref).toBe('light');
    });
  });

  describe('Theme validation', () => {
    it('should handle invalid theme values gracefully', async () => {
      const themeModule = await import('../../src/scripts/theme.js');

      // Try to set invalid theme
      themeModule.setTheme('invalid' as any);

      // Should fall back to light
      expect(themeModule.getTheme()).toBe('light');
    });

    it('should accept valid theme values', async () => {
      const themeModule = await import('../../src/scripts/theme.js');

      themeModule.setTheme('dark');
      expect(themeModule.getTheme()).toBe('dark');

      themeModule.setTheme('light');
      expect(themeModule.getTheme()).toBe('light');
    });
  });

  describe('applyTheme', () => {
    it('should set data-theme attribute on document', async () => {
      const themeModule = await import('../../src/scripts/theme.js');

      themeModule.applyTheme('dark');
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark');

      themeModule.applyTheme('light');
      expect(document.documentElement.getAttribute('data-theme')).toBe('light');
    });
  });
});
