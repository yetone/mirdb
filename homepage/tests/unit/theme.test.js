/**
 * Theme Detection Unit Tests
 * Owner: Scenario 10 - Dark Mode Theme Support
 *
 * Unit tests for the theme.js module functions.
 * Tests CSS custom properties detection and theme change callbacks.
 */

import { jest } from '@jest/globals';

// Mock matchMedia before importing the module
const mockMatchMedia = jest.fn();

beforeAll(() => {
  Object.defineProperty(window, 'matchMedia', {
    writable: true,
    value: mockMatchMedia
  });
});

beforeEach(() => {
  jest.resetModules();
  mockMatchMedia.mockReset();
});

describe('Theme Detection Module', () => {

  describe('getSystemTheme()', () => {
    test('returns "dark" when system prefers dark mode', async () => {
      mockMatchMedia.mockImplementation((query) => ({
        matches: query === '(prefers-color-scheme: dark)',
        media: query,
        addEventListener: jest.fn(),
        removeEventListener: jest.fn()
      }));

      const { getSystemTheme } = await import('../../js/utils/theme.js');
      expect(getSystemTheme()).toBe('dark');
    });

    test('returns "light" when system prefers light mode', async () => {
      mockMatchMedia.mockImplementation((query) => ({
        matches: false,
        media: query,
        addEventListener: jest.fn(),
        removeEventListener: jest.fn()
      }));

      const { getSystemTheme } = await import('../../js/utils/theme.js');
      expect(getSystemTheme()).toBe('light');
    });

    test('returns "light" as default when matchMedia is not available', async () => {
      // Mock matchMedia to return undefined
      mockMatchMedia.mockImplementation(() => undefined);

      // Clear module cache and reimport
      jest.resetModules();

      // Redefine matchMedia to return undefined
      Object.defineProperty(window, 'matchMedia', {
        writable: true,
        value: undefined
      });

      const { getSystemTheme } = await import('../../js/utils/theme.js');

      expect(getSystemTheme()).toBe('light');

      // Restore matchMedia for other tests
      Object.defineProperty(window, 'matchMedia', {
        writable: true,
        value: mockMatchMedia
      });
    });
  });

  describe('prefersDarkMode()', () => {
    test('returns true when system prefers dark mode', async () => {
      mockMatchMedia.mockImplementation((query) => ({
        matches: query === '(prefers-color-scheme: dark)',
        media: query,
        addEventListener: jest.fn(),
        removeEventListener: jest.fn()
      }));

      const { prefersDarkMode } = await import('../../js/utils/theme.js');
      expect(prefersDarkMode()).toBe(true);
    });

    test('returns false when system prefers light mode', async () => {
      mockMatchMedia.mockImplementation((query) => ({
        matches: false,
        media: query,
        addEventListener: jest.fn(),
        removeEventListener: jest.fn()
      }));

      const { prefersDarkMode } = await import('../../js/utils/theme.js');
      expect(prefersDarkMode()).toBe(false);
    });
  });

  describe('onThemeChange()', () => {
    test('registers callback for theme changes', async () => {
      const addEventListenerMock = jest.fn();
      const removeEventListenerMock = jest.fn();

      mockMatchMedia.mockImplementation((query) => ({
        matches: false,
        media: query,
        addEventListener: addEventListenerMock,
        removeEventListener: removeEventListenerMock
      }));

      const { onThemeChange } = await import('../../js/utils/theme.js');
      const callback = jest.fn();

      const cleanup = onThemeChange(callback);

      expect(addEventListenerMock).toHaveBeenCalledWith('change', expect.any(Function));
      expect(typeof cleanup).toBe('function');
    });

    test('cleanup function removes event listener', async () => {
      const addEventListenerMock = jest.fn();
      const removeEventListenerMock = jest.fn();

      mockMatchMedia.mockImplementation((query) => ({
        matches: false,
        media: query,
        addEventListener: addEventListenerMock,
        removeEventListener: removeEventListenerMock
      }));

      const { onThemeChange } = await import('../../js/utils/theme.js');
      const callback = jest.fn();

      const cleanup = onThemeChange(callback);
      cleanup();

      expect(removeEventListenerMock).toHaveBeenCalledWith('change', expect.any(Function));
    });

    test('callback is invoked with correct theme on change', async () => {
      let changeHandler;
      const addEventListenerMock = jest.fn((event, handler) => {
        changeHandler = handler;
      });

      mockMatchMedia.mockImplementation((query) => ({
        matches: false,
        media: query,
        addEventListener: addEventListenerMock,
        removeEventListener: jest.fn()
      }));

      const { onThemeChange } = await import('../../js/utils/theme.js');
      const callback = jest.fn();

      onThemeChange(callback);

      // Simulate theme change to dark
      changeHandler({ matches: true });
      expect(callback).toHaveBeenCalledWith('dark');

      // Simulate theme change to light
      changeHandler({ matches: false });
      expect(callback).toHaveBeenCalledWith('light');
    });

    test('falls back to addListener for older browsers', async () => {
      const addListenerMock = jest.fn();
      const removeListenerMock = jest.fn();

      mockMatchMedia.mockImplementation((query) => ({
        matches: false,
        media: query,
        // No addEventListener - simulate older browser
        addListener: addListenerMock,
        removeListener: removeListenerMock
      }));

      const { onThemeChange } = await import('../../js/utils/theme.js');
      const callback = jest.fn();

      const cleanup = onThemeChange(callback);

      expect(addListenerMock).toHaveBeenCalled();

      cleanup();
      expect(removeListenerMock).toHaveBeenCalled();
    });
  });

  describe('initTheme()', () => {
    test('returns current theme and cleanup function', async () => {
      mockMatchMedia.mockImplementation((query) => ({
        matches: query === '(prefers-color-scheme: dark)',
        media: query,
        addEventListener: jest.fn(),
        removeEventListener: jest.fn()
      }));

      const { initTheme } = await import('../../js/utils/theme.js');
      const result = initTheme();

      expect(result).toHaveProperty('theme', 'dark');
      expect(result).toHaveProperty('cleanup');
      expect(typeof result.cleanup).toBe('function');
    });

    test('sets data-theme attribute on document root', async () => {
      mockMatchMedia.mockImplementation((query) => ({
        matches: query === '(prefers-color-scheme: dark)',
        media: query,
        addEventListener: jest.fn(),
        removeEventListener: jest.fn()
      }));

      const { initTheme } = await import('../../js/utils/theme.js');
      initTheme();

      expect(document.documentElement.dataset.theme).toBe('dark');
    });

    test('updates data-theme attribute on theme change', async () => {
      let changeHandler;
      mockMatchMedia.mockImplementation((query) => ({
        matches: false,
        media: query,
        addEventListener: jest.fn((event, handler) => {
          changeHandler = handler;
        }),
        removeEventListener: jest.fn()
      }));

      const { initTheme } = await import('../../js/utils/theme.js');
      initTheme();

      // Initially light
      expect(document.documentElement.dataset.theme).toBe('light');

      // Simulate change to dark
      changeHandler({ matches: true });
      expect(document.documentElement.dataset.theme).toBe('dark');

      // Simulate change back to light
      changeHandler({ matches: false });
      expect(document.documentElement.dataset.theme).toBe('light');
    });
  });

  describe('Test Case 5: CSS custom properties for theming', () => {
    test('theme uses CSS variables that respond to color-scheme media query', async () => {
      // This test validates that CSS custom properties are used
      // The actual CSS variables are tested in E2E tests
      // Here we verify the JS module exposes the right interface

      // Reset modules to ensure fresh import
      jest.resetModules();

      // Reset the mock and restore matchMedia
      mockMatchMedia.mockReset();
      mockMatchMedia.mockImplementation((query) => ({
        matches: query === '(prefers-color-scheme: dark)',
        media: query,
        addEventListener: jest.fn(),
        removeEventListener: jest.fn()
      }));

      Object.defineProperty(window, 'matchMedia', {
        writable: true,
        value: mockMatchMedia
      });

      const themeModule = await import('../../js/utils/theme.js');

      // Verify module exports the expected functions
      expect(typeof themeModule.getSystemTheme).toBe('function');
      expect(typeof themeModule.prefersDarkMode).toBe('function');
      expect(typeof themeModule.onThemeChange).toBe('function');
      expect(typeof themeModule.initTheme).toBe('function');

      // Call a function to ensure matchMedia is used
      const theme = themeModule.getSystemTheme();
      expect(theme).toBe('dark');

      // Verify the module uses prefers-color-scheme media query
      expect(mockMatchMedia).toHaveBeenCalledWith('(prefers-color-scheme: dark)');
    });
  });

});
