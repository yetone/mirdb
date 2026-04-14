/**
 * Theme Utility Unit Tests
 * Owner: Scenario 13 - Dark Mode Support
 *
 * Unit tests for:
 * - getPreferredColorScheme function
 * - applyTheme function
 * - watchColorSchemeChange function
 * - initTheme function
 * - isDarkMode function
 * - toggleTheme function
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

// Store original matchMedia
const originalMatchMedia = window.matchMedia;

// Mock matchMedia factory
function createMockMatchMedia(matches: boolean) {
  return vi.fn().mockImplementation((query: string) => ({
    matches,
    media: query,
    onchange: null,
    addListener: vi.fn(),
    removeListener: vi.fn(),
    addEventListener: vi.fn(),
    removeEventListener: vi.fn(),
    dispatchEvent: vi.fn(),
  }));
}

describe('Theme Utility', () => {
  beforeEach(() => {
    // Reset DOM
    document.documentElement.removeAttribute('data-theme');
    // Reset modules to get fresh imports
    vi.resetModules();
  });

  afterEach(() => {
    // Restore original matchMedia
    window.matchMedia = originalMatchMedia;
    vi.restoreAllMocks();
  });

  describe('getPreferredColorScheme', () => {
    it('should return "dark" when system prefers dark mode', async () => {
      // Mock dark mode preference
      window.matchMedia = createMockMatchMedia(true);

      const { getPreferredColorScheme } = await import('../../src/js/utils/theme.js');
      const result = getPreferredColorScheme();

      expect(result).toBe('dark');
    });

    it('should return "light" when system prefers light mode', async () => {
      // Mock light mode preference
      window.matchMedia = createMockMatchMedia(false);

      const { getPreferredColorScheme } = await import('../../src/js/utils/theme.js');
      const result = getPreferredColorScheme();

      expect(result).toBe('light');
    });

    it('should return "light" as default when matchMedia is not supported', async () => {
      // Remove matchMedia
      (window as any).matchMedia = undefined;

      const { getPreferredColorScheme } = await import('../../src/js/utils/theme.js');
      const result = getPreferredColorScheme();

      expect(result).toBe('light');
    });
  });

  describe('applyTheme', () => {
    it('should set data-theme attribute to "dark"', async () => {
      window.matchMedia = createMockMatchMedia(false);

      const { applyTheme } = await import('../../src/js/utils/theme.js');
      applyTheme('dark');

      expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
    });

    it('should set data-theme attribute to "light"', async () => {
      window.matchMedia = createMockMatchMedia(true);

      const { applyTheme } = await import('../../src/js/utils/theme.js');
      applyTheme('light');

      expect(document.documentElement.getAttribute('data-theme')).toBe('light');
    });

    it('should update meta theme-color when present', async () => {
      // Add meta theme-color element
      const meta = document.createElement('meta');
      meta.name = 'theme-color';
      meta.content = '#ffffff';
      document.head.appendChild(meta);

      window.matchMedia = createMockMatchMedia(false);

      const { applyTheme } = await import('../../src/js/utils/theme.js');
      applyTheme('dark');

      expect(meta.getAttribute('content')).toBe('#0d1117');

      // Cleanup
      document.head.removeChild(meta);
    });
  });

  describe('watchColorSchemeChange', () => {
    it('should call callback when color scheme changes to dark', async () => {
      const callback = vi.fn();
      let changeHandler: ((event: MediaQueryListEvent) => void) | null = null;

      // Mock matchMedia with addEventListener
      window.matchMedia = vi.fn().mockImplementation((query: string) => ({
        matches: false,
        media: query,
        onchange: null,
        addListener: vi.fn(),
        removeListener: vi.fn(),
        addEventListener: vi.fn((event: string, handler: (e: MediaQueryListEvent) => void) => {
          changeHandler = handler;
        }),
        removeEventListener: vi.fn(),
        dispatchEvent: vi.fn(),
      }));

      const { watchColorSchemeChange } = await import('../../src/js/utils/theme.js');
      watchColorSchemeChange(callback);

      // Simulate color scheme change
      if (changeHandler) {
        changeHandler({ matches: true } as MediaQueryListEvent);
      }

      expect(callback).toHaveBeenCalledWith('dark');
    });

    it('should call callback when color scheme changes to light', async () => {
      const callback = vi.fn();
      let changeHandler: ((event: MediaQueryListEvent) => void) | null = null;

      window.matchMedia = vi.fn().mockImplementation((query: string) => ({
        matches: true,
        media: query,
        onchange: null,
        addListener: vi.fn(),
        removeListener: vi.fn(),
        addEventListener: vi.fn((event: string, handler: (e: MediaQueryListEvent) => void) => {
          changeHandler = handler;
        }),
        removeEventListener: vi.fn(),
        dispatchEvent: vi.fn(),
      }));

      const { watchColorSchemeChange } = await import('../../src/js/utils/theme.js');
      watchColorSchemeChange(callback);

      // Simulate color scheme change
      if (changeHandler) {
        changeHandler({ matches: false } as MediaQueryListEvent);
      }

      expect(callback).toHaveBeenCalledWith('light');
    });

    it('should return a cleanup function', async () => {
      window.matchMedia = createMockMatchMedia(false);

      const { watchColorSchemeChange } = await import('../../src/js/utils/theme.js');
      const cleanup = watchColorSchemeChange(vi.fn());

      expect(typeof cleanup).toBe('function');
      expect(() => cleanup()).not.toThrow();
    });

    it('should return no-op cleanup when matchMedia not supported', async () => {
      (window as any).matchMedia = undefined;

      const { watchColorSchemeChange } = await import('../../src/js/utils/theme.js');
      const cleanup = watchColorSchemeChange(vi.fn());

      expect(typeof cleanup).toBe('function');
      expect(() => cleanup()).not.toThrow();
    });
  });

  describe('initTheme', () => {
    it('should apply the preferred theme on initialization', async () => {
      // Mock dark mode preference
      window.matchMedia = createMockMatchMedia(true);

      const { initTheme } = await import('../../src/js/utils/theme.js');
      initTheme();

      expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
    });

    it('should apply light theme when system prefers light', async () => {
      // Mock light mode preference
      window.matchMedia = createMockMatchMedia(false);

      const { initTheme } = await import('../../src/js/utils/theme.js');
      initTheme();

      expect(document.documentElement.getAttribute('data-theme')).toBe('light');
    });

    it('should set up color scheme change watching', async () => {
      const addEventListenerMock = vi.fn();

      window.matchMedia = vi.fn().mockImplementation((query: string) => ({
        matches: false,
        media: query,
        onchange: null,
        addListener: vi.fn(),
        removeListener: vi.fn(),
        addEventListener: addEventListenerMock,
        removeEventListener: vi.fn(),
        dispatchEvent: vi.fn(),
      }));

      const { initTheme } = await import('../../src/js/utils/theme.js');
      initTheme();

      expect(addEventListenerMock).toHaveBeenCalledWith('change', expect.any(Function));
    });
  });

  describe('isDarkMode', () => {
    it('should return true when data-theme is "dark"', async () => {
      window.matchMedia = createMockMatchMedia(false);
      document.documentElement.setAttribute('data-theme', 'dark');

      const { isDarkMode } = await import('../../src/js/utils/theme.js');
      expect(isDarkMode()).toBe(true);
    });

    it('should return false when data-theme is "light"', async () => {
      window.matchMedia = createMockMatchMedia(true);
      document.documentElement.setAttribute('data-theme', 'light');

      const { isDarkMode } = await import('../../src/js/utils/theme.js');
      expect(isDarkMode()).toBe(false);
    });

    it('should fall back to system preference when no data-theme', async () => {
      // Mock dark mode preference
      window.matchMedia = createMockMatchMedia(true);

      const { isDarkMode } = await import('../../src/js/utils/theme.js');
      expect(isDarkMode()).toBe(true);
    });
  });

  describe('toggleTheme', () => {
    it('should toggle from dark to light', async () => {
      window.matchMedia = createMockMatchMedia(true);
      document.documentElement.setAttribute('data-theme', 'dark');

      const { toggleTheme } = await import('../../src/js/utils/theme.js');
      const result = toggleTheme();

      expect(result).toBe('light');
      expect(document.documentElement.getAttribute('data-theme')).toBe('light');
    });

    it('should toggle from light to dark', async () => {
      window.matchMedia = createMockMatchMedia(false);
      document.documentElement.setAttribute('data-theme', 'light');

      const { toggleTheme } = await import('../../src/js/utils/theme.js');
      const result = toggleTheme();

      expect(result).toBe('dark');
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
    });
  });
});

describe('Theme Integration', () => {
  beforeEach(() => {
    document.documentElement.removeAttribute('data-theme');
    vi.resetModules();
  });

  afterEach(() => {
    window.matchMedia = originalMatchMedia;
    vi.restoreAllMocks();
  });

  it('should correctly detect and apply dark mode end-to-end', async () => {
    // Set up dark mode preference
    window.matchMedia = createMockMatchMedia(true);

    const { initTheme, isDarkMode, getPreferredColorScheme } = await import('../../src/js/utils/theme.js');

    // Initialize
    initTheme();

    // Verify all functions agree on dark mode
    expect(getPreferredColorScheme()).toBe('dark');
    expect(isDarkMode()).toBe(true);
    expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
  });

  it('should correctly detect and apply light mode end-to-end', async () => {
    // Set up light mode preference
    window.matchMedia = createMockMatchMedia(false);

    const { initTheme, isDarkMode, getPreferredColorScheme } = await import('../../src/js/utils/theme.js');

    // Initialize
    initTheme();

    // Verify all functions agree on light mode
    expect(getPreferredColorScheme()).toBe('light');
    expect(isDarkMode()).toBe(false);
    expect(document.documentElement.getAttribute('data-theme')).toBe('light');
  });
});
