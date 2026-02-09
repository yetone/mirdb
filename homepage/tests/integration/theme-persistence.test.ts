/**
 * Integration tests for theme persistence.
 * Owner: Scenario 9 - Dark Mode Support
 *
 * Tests the full theme persistence flow including:
 * - localStorage persistence
 * - Page reload simulation
 * - Theme state consistency
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { getTheme, setTheme, initTheme, toggleTheme } from '../../src/utils/theme';
import { renderThemeToggle } from '../../src/components/ThemeToggle';

describe('theme persistence integration', () => {
  // Simulated localStorage store
  let store: Record<string, string> = {};

  // Mock localStorage with persistent store
  const localStorageMock = {
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
    // Reset store
    store = {};

    // Set up mocks
    Object.defineProperty(window, 'localStorage', {
      value: localStorageMock,
      writable: true,
    });
    Object.defineProperty(window, 'matchMedia', {
      value: matchMediaMock,
      writable: true,
    });

    vi.clearAllMocks();
    document.documentElement.removeAttribute('data-theme');

    // Clean up style tags
    const existingStyle = document.getElementById('theme-toggle-styles');
    if (existingStyle) {
      existingStyle.remove();
    }
  });

  afterEach(() => {
    document.documentElement.removeAttribute('data-theme');
  });

  describe('theme state persistence', () => {
    it('persists dark theme preference across simulated page reload', () => {
      // Test case 6: Set theme to dark, refresh page
      // Expected: Page loads with dark theme (preference persisted)

      // Step 1: Set theme to dark
      setTheme('dark');
      expect(store['mirdb-theme']).toBe('dark');
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark');

      // Step 2: Simulate page reload by clearing DOM state
      document.documentElement.removeAttribute('data-theme');

      // Step 3: Re-initialize (simulating page load)
      initTheme();

      // Step 4: Verify theme is restored from localStorage
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
    });

    it('persists light theme preference across simulated page reload', () => {
      // Set theme to light
      setTheme('light');
      expect(store['mirdb-theme']).toBe('light');

      // Simulate page reload
      document.documentElement.removeAttribute('data-theme');
      initTheme();

      expect(document.documentElement.getAttribute('data-theme')).toBe('light');
    });

    it('maintains theme consistency through multiple toggles', () => {
      // Start with light
      initTheme();
      expect(getTheme()).toBe('light');

      // Toggle to dark
      toggleTheme();
      expect(store['mirdb-theme']).toBe('dark');
      expect(getTheme()).toBe('dark');

      // Toggle back to light
      toggleTheme();
      expect(store['mirdb-theme']).toBe('light');
      expect(getTheme()).toBe('light');

      // Simulate page reload
      document.documentElement.removeAttribute('data-theme');
      initTheme();

      // Should still be light
      expect(document.documentElement.getAttribute('data-theme')).toBe('light');
    });
  });

  describe('ThemeToggle component with persistence', () => {
    it('renders with correct initial state from localStorage', () => {
      // Pre-set dark theme in storage
      store['mirdb-theme'] = 'dark';

      const container = renderThemeToggle();
      const button = container.querySelector('button');

      // Should show sun icon (indicating we can switch to light)
      expect(button?.innerHTML).toContain('theme-icon--sun');
      expect(button?.getAttribute('data-theme-state')).toBe('dark');
    });

    it('persists theme change made via toggle button', () => {
      // Start with light theme
      store['mirdb-theme'] = 'light';

      const container = renderThemeToggle();
      const button = container.querySelector('button');

      // Click to toggle to dark
      button?.click();

      // Verify persistence
      expect(store['mirdb-theme']).toBe('dark');

      // Simulate page reload and create new component
      document.documentElement.removeAttribute('data-theme');
      const existingStyle = document.getElementById('theme-toggle-styles');
      if (existingStyle) {
        existingStyle.remove();
      }

      const newContainer = renderThemeToggle();
      const newButton = newContainer.querySelector('button');

      // Should initialize with dark theme
      expect(newButton?.getAttribute('data-theme-state')).toBe('dark');
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
    });
  });

  describe('full user flow', () => {
    it('simulates complete user session with theme changes', () => {
      // User arrives with no preference (system is light)
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

      // Render toggle
      const container = renderThemeToggle();
      const button = container.querySelector('button');

      // Initial state should be light
      expect(document.documentElement.getAttribute('data-theme')).toBe('light');
      expect(button?.getAttribute('data-theme-state')).toBe('light');

      // User clicks to switch to dark mode
      button?.click();

      // Theme should be dark now
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
      expect(button?.getAttribute('data-theme-state')).toBe('dark');
      expect(store['mirdb-theme']).toBe('dark');

      // User reloads page
      document.documentElement.removeAttribute('data-theme');

      // Initialize theme (simulating page load)
      initTheme();

      // Theme should persist
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
    });
  });
});
