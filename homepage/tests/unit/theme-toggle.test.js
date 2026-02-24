/**
 * Theme Toggle Unit Tests
 * Owner: Scenario 9 - Dark Mode Theme Toggle
 *
 * Test cases:
 * - initTheme reads from localStorage
 * - toggleTheme switches between light and dark
 * - Theme preference is saved to localStorage
 * - System preference is respected when no localStorage value
 * - Theme toggle button renders with correct icon
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { mockLocalStorage } from '../setup/test-utils.js';
import {
  initTheme,
  toggleTheme,
  getTheme,
  initThemeToggle,
  STORAGE_KEY,
  DARK_THEME,
  LIGHT_THEME,
  getStoredTheme,
  storeTheme,
  applyTheme,
} from '../../assets/js/theme-toggle.js';

describe('Theme Toggle Module', () => {
  let mockStorage;
  let originalLocalStorage;
  let originalMatchMedia;

  beforeEach(() => {
    // Set up mock localStorage
    mockStorage = mockLocalStorage();
    originalLocalStorage = global.localStorage;
    Object.defineProperty(global, 'localStorage', {
      value: mockStorage,
      writable: true,
    });

    // Set up mock matchMedia
    originalMatchMedia = window.matchMedia;
    window.matchMedia = vi.fn().mockImplementation((query) => ({
      matches: false, // default to light mode
      media: query,
      onchange: null,
      addListener: vi.fn(),
      removeListener: vi.fn(),
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
      dispatchEvent: vi.fn(),
    }));

    // Reset document root
    document.documentElement.removeAttribute('data-theme');
  });

  afterEach(() => {
    // Restore original localStorage
    Object.defineProperty(global, 'localStorage', {
      value: originalLocalStorage,
      writable: true,
    });

    // Restore original matchMedia
    window.matchMedia = originalMatchMedia;

    // Clean up any added elements
    document.body.innerHTML = '';
    document.documentElement.removeAttribute('data-theme');
  });

  describe('Constants', () => {
    it('should export correct storage key', () => {
      expect(STORAGE_KEY).toBe('theme');
    });

    it('should export correct theme values', () => {
      expect(DARK_THEME).toBe('dark');
      expect(LIGHT_THEME).toBe('light');
    });
  });

  describe('getStoredTheme', () => {
    it('should return null when no theme is stored', () => {
      const result = getStoredTheme();
      expect(result).toBeNull();
    });

    it('should return stored dark theme', () => {
      mockStorage.setItem(STORAGE_KEY, DARK_THEME);
      const result = getStoredTheme();
      expect(result).toBe(DARK_THEME);
    });

    it('should return stored light theme', () => {
      mockStorage.setItem(STORAGE_KEY, LIGHT_THEME);
      const result = getStoredTheme();
      expect(result).toBe(LIGHT_THEME);
    });
  });

  describe('storeTheme', () => {
    it('should store theme in localStorage', () => {
      storeTheme(DARK_THEME);
      expect(mockStorage.getItem(STORAGE_KEY)).toBe(DARK_THEME);
    });

    it('should overwrite existing theme', () => {
      storeTheme(DARK_THEME);
      storeTheme(LIGHT_THEME);
      expect(mockStorage.getItem(STORAGE_KEY)).toBe(LIGHT_THEME);
    });
  });

  describe('applyTheme', () => {
    it('should set data-theme attribute to dark', () => {
      applyTheme(DARK_THEME);
      expect(document.documentElement.getAttribute('data-theme')).toBe(DARK_THEME);
    });

    it('should set data-theme attribute to light', () => {
      applyTheme(LIGHT_THEME);
      expect(document.documentElement.getAttribute('data-theme')).toBe(LIGHT_THEME);
    });
  });

  describe('getTheme', () => {
    it('should return light when no theme is set', () => {
      const result = getTheme();
      expect(result).toBe(LIGHT_THEME);
    });

    it('should return dark when dark theme is applied', () => {
      document.documentElement.setAttribute('data-theme', DARK_THEME);
      const result = getTheme();
      expect(result).toBe(DARK_THEME);
    });

    it('should return light when light theme is applied', () => {
      document.documentElement.setAttribute('data-theme', LIGHT_THEME);
      const result = getTheme();
      expect(result).toBe(LIGHT_THEME);
    });
  });

  describe('initTheme', () => {
    it('should apply stored dark theme from localStorage', () => {
      mockStorage.setItem(STORAGE_KEY, DARK_THEME);
      const result = initTheme();

      expect(result).toBe(DARK_THEME);
      expect(document.documentElement.getAttribute('data-theme')).toBe(DARK_THEME);
    });

    it('should apply stored light theme from localStorage', () => {
      mockStorage.setItem(STORAGE_KEY, LIGHT_THEME);
      const result = initTheme();

      expect(result).toBe(LIGHT_THEME);
      expect(document.documentElement.getAttribute('data-theme')).toBe(LIGHT_THEME);
    });

    it('should use system preference when no stored theme', () => {
      // System prefers dark
      window.matchMedia = vi.fn().mockImplementation((query) => ({
        matches: query === '(prefers-color-scheme: dark)',
        media: query,
        addEventListener: vi.fn(),
        removeEventListener: vi.fn(),
      }));

      const result = initTheme();

      expect(result).toBe(DARK_THEME);
      expect(document.documentElement.getAttribute('data-theme')).toBe(DARK_THEME);
    });

    it('should default to light when system prefers light', () => {
      window.matchMedia = vi.fn().mockImplementation((query) => ({
        matches: false, // prefers light
        media: query,
        addEventListener: vi.fn(),
        removeEventListener: vi.fn(),
      }));

      const result = initTheme();

      expect(result).toBe(LIGHT_THEME);
      expect(document.documentElement.getAttribute('data-theme')).toBe(LIGHT_THEME);
    });

    it('should store the initial theme preference', () => {
      window.matchMedia = vi.fn().mockImplementation((query) => ({
        matches: query === '(prefers-color-scheme: dark)',
        media: query,
        addEventListener: vi.fn(),
        removeEventListener: vi.fn(),
      }));

      initTheme();

      expect(mockStorage.getItem(STORAGE_KEY)).toBe(DARK_THEME);
    });
  });

  describe('toggleTheme', () => {
    it('should toggle from light to dark', () => {
      document.documentElement.setAttribute('data-theme', LIGHT_THEME);

      const result = toggleTheme();

      expect(result).toBe(DARK_THEME);
      expect(document.documentElement.getAttribute('data-theme')).toBe(DARK_THEME);
      expect(mockStorage.getItem(STORAGE_KEY)).toBe(DARK_THEME);
    });

    it('should toggle from dark to light', () => {
      document.documentElement.setAttribute('data-theme', DARK_THEME);

      const result = toggleTheme();

      expect(result).toBe(LIGHT_THEME);
      expect(document.documentElement.getAttribute('data-theme')).toBe(LIGHT_THEME);
      expect(mockStorage.getItem(STORAGE_KEY)).toBe(LIGHT_THEME);
    });

    it('should persist theme after toggle', () => {
      document.documentElement.setAttribute('data-theme', LIGHT_THEME);

      toggleTheme(); // light -> dark
      expect(mockStorage.getItem(STORAGE_KEY)).toBe(DARK_THEME);

      toggleTheme(); // dark -> light
      expect(mockStorage.getItem(STORAGE_KEY)).toBe(LIGHT_THEME);
    });
  });

  describe('initThemeToggle', () => {
    it('should set up click handlers for theme toggle buttons', () => {
      // Create theme toggle button
      document.body.innerHTML = `
        <button class="theme-toggle" aria-label="Toggle dark mode">
          <svg class="icon-sun"></svg>
          <svg class="icon-moon"></svg>
        </button>
      `;

      // Initialize the theme toggle
      initThemeToggle();

      const button = document.querySelector('.theme-toggle');
      expect(document.documentElement.getAttribute('data-theme')).toBeTruthy();

      // Click should toggle theme
      const initialTheme = getTheme();
      button.click();

      const newTheme = getTheme();
      expect(newTheme).not.toBe(initialTheme);
    });

    it('should update aria-label after toggle', () => {
      document.body.innerHTML = `
        <button class="theme-toggle" aria-label="Toggle dark mode">
          <svg class="icon-sun"></svg>
          <svg class="icon-moon"></svg>
        </button>
      `;

      initThemeToggle();

      // Set to light mode first
      document.documentElement.setAttribute('data-theme', LIGHT_THEME);

      const button = document.querySelector('.theme-toggle');
      button.click();

      // After toggling to dark, aria-label should mention switching to light
      expect(button.getAttribute('aria-label')).toBe('Switch to light mode');
    });

    it('should handle multiple toggle buttons', () => {
      document.body.innerHTML = `
        <button class="theme-toggle" id="btn1" aria-label="Toggle dark mode"></button>
        <button class="theme-toggle" id="btn2" aria-label="Toggle dark mode"></button>
      `;

      initThemeToggle();

      document.documentElement.setAttribute('data-theme', LIGHT_THEME);

      const btn1 = document.querySelector('#btn1');
      const btn2 = document.querySelector('#btn2');

      // Click first button
      btn1.click();
      expect(getTheme()).toBe(DARK_THEME);

      // Click second button
      btn2.click();
      expect(getTheme()).toBe(LIGHT_THEME);
    });
  });

  describe('Theme Persistence', () => {
    it('should remember dark theme after page reload simulation', () => {
      // Set dark theme
      mockStorage.setItem(STORAGE_KEY, DARK_THEME);

      // Simulate page reload by calling initTheme
      const theme = initTheme();

      expect(theme).toBe(DARK_THEME);
      expect(document.documentElement.getAttribute('data-theme')).toBe(DARK_THEME);
    });

    it('should remember light theme after page reload simulation', () => {
      // Set light theme
      mockStorage.setItem(STORAGE_KEY, LIGHT_THEME);

      // Simulate page reload by calling initTheme
      const theme = initTheme();

      expect(theme).toBe(LIGHT_THEME);
      expect(document.documentElement.getAttribute('data-theme')).toBe(LIGHT_THEME);
    });
  });
});
