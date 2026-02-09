/**
 * Unit tests for ThemeToggle component.
 * Owner: Scenario 9 - Dark Mode Support
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { renderThemeToggle } from '../../../src/components/ThemeToggle';

describe('ThemeToggle component', () => {
  // Store for localStorage mock
  let store: Record<string, string>;

  // Mock matchMedia
  const createMatchMediaMock = (prefersDark: boolean) => vi.fn((query: string) => ({
    matches: query === '(prefers-color-scheme: dark)' ? prefersDark : false,
    media: query,
    onchange: null,
    addListener: vi.fn(),
    removeListener: vi.fn(),
    addEventListener: vi.fn(),
    removeEventListener: vi.fn(),
    dispatchEvent: vi.fn(),
  }));

  beforeEach(() => {
    // Reset store for each test
    store = {};

    // Create localStorage mock that uses the store
    const localStorageMock = {
      getItem: vi.fn((key: string) => store[key] ?? null),
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

    // Set up mocks
    Object.defineProperty(window, 'localStorage', {
      value: localStorageMock,
      writable: true,
      configurable: true,
    });

    Object.defineProperty(window, 'matchMedia', {
      value: createMatchMediaMock(false), // default to light mode
      writable: true,
      configurable: true,
    });

    vi.clearAllMocks();
    document.documentElement.removeAttribute('data-theme');

    // Remove any existing style tags
    const existingStyle = document.getElementById('theme-toggle-styles');
    if (existingStyle) {
      existingStyle.remove();
    }
  });

  afterEach(() => {
    // Clean up
    document.documentElement.removeAttribute('data-theme');
  });

  describe('render', () => {
    it('renders toggle button with correct structure', () => {
      // Test case 4: Render theme toggle component
      // Expected: Toggle button displays sun or moon icon based on current theme
      const container = renderThemeToggle();

      expect(container.classList.contains('theme-toggle-container')).toBe(true);

      const button = container.querySelector('button');
      expect(button).not.toBeNull();
      expect(button?.classList.contains('theme-toggle')).toBe(true);
      expect(button?.type).toBe('button');
    });

    it('displays moon icon when in light mode', () => {
      store['mirdb-theme'] = 'light';

      const container = renderThemeToggle();
      const button = container.querySelector('button');

      // In light mode, we show moon icon (to switch to dark)
      expect(button?.innerHTML).toContain('theme-icon--moon');
      expect(button?.getAttribute('aria-label')).toBe('Switch to dark theme');
    });

    it('displays sun icon when in dark mode', () => {
      store['mirdb-theme'] = 'dark';

      const container = renderThemeToggle();
      const button = container.querySelector('button');

      // In dark mode, we show sun icon (to switch to light)
      expect(button?.innerHTML).toContain('theme-icon--sun');
      expect(button?.getAttribute('aria-label')).toBe('Switch to light theme');
    });

    it('has proper accessibility attributes', () => {
      store['mirdb-theme'] = 'light';

      const container = renderThemeToggle();
      const button = container.querySelector('button');

      expect(button?.getAttribute('type')).toBe('button');
      expect(button?.getAttribute('aria-label')).toBeTruthy();
      expect(button?.getAttribute('aria-pressed')).toBe('false');
    });

    it('has data-theme-state attribute', () => {
      store['mirdb-theme'] = 'dark';

      const container = renderThemeToggle();
      const button = container.querySelector('button');

      expect(button?.getAttribute('data-theme-state')).toBe('dark');
    });
  });

  describe('interaction', () => {
    it('toggles theme on click', () => {
      store['mirdb-theme'] = 'light';

      const container = renderThemeToggle();
      const button = container.querySelector('button');

      // Initial state check
      expect(button?.getAttribute('data-theme-state')).toBe('light');

      // Click to toggle
      button?.click();

      // Should now be dark
      expect(store['mirdb-theme']).toBe('dark');
      expect(button?.innerHTML).toContain('theme-icon--sun');
      expect(button?.getAttribute('aria-label')).toBe('Switch to light theme');
      expect(button?.getAttribute('data-theme-state')).toBe('dark');
    });

    it('updates button appearance after toggle', () => {
      store['mirdb-theme'] = 'light';

      const container = renderThemeToggle();
      const button = container.querySelector('button');

      // Initial state - light mode
      expect(button?.getAttribute('data-theme-state')).toBe('light');
      expect(button?.innerHTML).toContain('theme-icon--moon');

      // Click to toggle to dark
      button?.click();
      expect(button?.getAttribute('data-theme-state')).toBe('dark');
      expect(button?.innerHTML).toContain('theme-icon--sun');

      // Click to toggle back to light
      button?.click();
      expect(button?.getAttribute('data-theme-state')).toBe('light');
      expect(button?.innerHTML).toContain('theme-icon--moon');
    });
  });

  describe('styles', () => {
    it('injects styles once', () => {
      store['mirdb-theme'] = 'light';

      // Render twice
      renderThemeToggle();
      renderThemeToggle();

      // Should only have one style tag
      const styleTags = document.querySelectorAll('#theme-toggle-styles');
      expect(styleTags.length).toBe(1);
    });

    it('includes necessary CSS rules', () => {
      store['mirdb-theme'] = 'light';

      renderThemeToggle();

      const styleTag = document.getElementById('theme-toggle-styles');
      expect(styleTag?.textContent).toContain('.theme-toggle');
      expect(styleTag?.textContent).toContain('.theme-icon');
    });
  });
});
