/**
 * Dark Mode Integration Tests
 * Owner: Scenario 14 - Dark Mode Toggle
 *
 * Tests:
 * - Toggle switches theme
 * - System preference detection
 * - Preference persistence
 * - Dark mode contrast maintained
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { readFileSync } from 'fs';
import { resolve } from 'path';

describe('Dark Mode Toggle', () => {
  let mockMatchMedia;

  beforeEach(() => {
    // Clear document state
    document.documentElement.removeAttribute('data-theme');

    // Clear localStorage
    localStorage.clear();

    // Reset matchMedia mock
    mockMatchMedia = vi.fn().mockImplementation((query) => ({
      matches: false,
      media: query,
      onchange: null,
      addListener: vi.fn(),
      removeListener: vi.fn(),
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
      dispatchEvent: vi.fn(),
    }));
    window.matchMedia = mockMatchMedia;

    // Clear module cache to get fresh imports
    vi.resetModules();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe('Test Case 1: Dark mode toggle presence', () => {
    it('should have a dark mode toggle button in the navigation', () => {
      // Load the HTML
      const htmlPath = resolve(process.cwd(), 'src/index.html');
      const html = readFileSync(htmlPath, 'utf-8');
      document.body.innerHTML = html;

      const toggle = document.querySelector('[data-testid="theme-toggle"]');
      expect(toggle).not.toBeNull();
      expect(toggle.tagName.toLowerCase()).toBe('button');
    });

    it('should have proper accessibility attributes', () => {
      const htmlPath = resolve(process.cwd(), 'src/index.html');
      const html = readFileSync(htmlPath, 'utf-8');
      document.body.innerHTML = html;

      const toggle = document.querySelector('[data-testid="theme-toggle"]');
      expect(toggle).not.toBeNull();
      expect(toggle.getAttribute('aria-label')).toBeTruthy();
      expect(toggle.getAttribute('aria-pressed')).toBeTruthy();
    });

    it('should have sun and moon icons', () => {
      const htmlPath = resolve(process.cwd(), 'src/index.html');
      const html = readFileSync(htmlPath, 'utf-8');
      document.body.innerHTML = html;

      const toggle = document.querySelector('[data-testid="theme-toggle"]');
      const sunIcon = toggle?.querySelector('.theme-toggle__icon--sun');
      const moonIcon = toggle?.querySelector('.theme-toggle__icon--moon');

      expect(sunIcon).not.toBeNull();
      expect(moonIcon).not.toBeNull();
    });
  });

  describe('Test Case 2: Toggle theme switching', () => {
    it('should switch to dark mode when toggle is clicked', async () => {
      // Import theme functions fresh
      const { initTheme, toggleTheme } = await import('../../../src/scripts/theme.js');

      // Initialize theme
      initTheme();

      // Verify starts in light mode
      expect(document.documentElement.getAttribute('data-theme')).toBeNull();

      // Toggle to dark mode
      toggleTheme();

      // Verify dark mode is applied
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
    });

    it('should switch back to light mode when toggled again', async () => {
      const { initTheme, toggleTheme } = await import('../../../src/scripts/theme.js');
      initTheme();

      // Toggle to dark
      toggleTheme();
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark');

      // Toggle back to light
      toggleTheme();
      expect(document.documentElement.getAttribute('data-theme')).toBeNull();
    });
  });

  describe('Test Case 3: Dark mode color contrast', () => {
    it('should define dark mode color variables with proper contrast', () => {
      // Read the CSS variables file
      const cssPath = resolve(process.cwd(), 'src/styles/variables.css');
      const css = readFileSync(cssPath, 'utf-8');

      // Check dark theme section exists
      expect(css).toContain('[data-theme="dark"]');

      // Check dark background colors are defined
      expect(css).toContain('--color-background: #0f172a');
      expect(css).toContain('--color-text: #f1f5f9');

      // Verify contrast ratio calculation
      // Dark background (#0f172a) with light text (#f1f5f9)
      const darkBg = '#0f172a';
      const lightText = '#f1f5f9';

      // Basic luminance calculation to verify high contrast
      const bgLuminance = getRelativeLuminance(darkBg);
      const textLuminance = getRelativeLuminance(lightText);
      const contrastRatio = (Math.max(bgLuminance, textLuminance) + 0.05) /
                            (Math.min(bgLuminance, textLuminance) + 0.05);

      // WCAG AA requires 4.5:1 for normal text
      expect(contrastRatio).toBeGreaterThan(4.5);
    });
  });

  describe('Test Case 4: System preference detection', () => {
    it('should apply dark theme when system prefers dark and no stored preference', async () => {
      // Mock system preference to dark
      window.matchMedia = vi.fn().mockImplementation((query) => ({
        matches: query === '(prefers-color-scheme: dark)',
        media: query,
        onchange: null,
        addListener: vi.fn(),
        removeListener: vi.fn(),
        addEventListener: vi.fn(),
        removeEventListener: vi.fn(),
        dispatchEvent: vi.fn(),
      }));

      // Clear any stored preference
      localStorage.clear();

      // Reset modules before import to get fresh module
      vi.resetModules();

      const { getSystemPreference } = await import('../../../src/scripts/theme.js');

      // Check system preference is detected
      expect(getSystemPreference()).toBe('dark');
    });

    it('should apply light theme when system prefers light and no stored preference', async () => {
      // Mock system preference to light
      window.matchMedia = vi.fn().mockImplementation((query) => ({
        matches: false,
        media: query,
        onchange: null,
        addListener: vi.fn(),
        removeListener: vi.fn(),
        addEventListener: vi.fn(),
        removeEventListener: vi.fn(),
        dispatchEvent: vi.fn(),
      }));

      localStorage.clear();
      vi.resetModules();

      const { getSystemPreference } = await import('../../../src/scripts/theme.js');

      expect(getSystemPreference()).toBe('light');
    });

    it('should prioritize stored preference over system preference', async () => {
      // Mock system preference to dark
      window.matchMedia = vi.fn().mockImplementation((query) => ({
        matches: query === '(prefers-color-scheme: dark)',
        media: query,
        onchange: null,
        addListener: vi.fn(),
        removeListener: vi.fn(),
        addEventListener: vi.fn(),
        removeEventListener: vi.fn(),
        dispatchEvent: vi.fn(),
      }));

      // Store light preference
      localStorage.setItem('mirdb-theme', 'light');

      vi.resetModules();
      const { getCurrentTheme } = await import('../../../src/scripts/theme.js');

      // Should return stored preference, not system preference
      expect(getCurrentTheme()).toBe('light');
    });
  });

  describe('Test Case 5: Preference persistence', () => {
    it('should save theme preference to localStorage', async () => {
      vi.resetModules();
      const { saveThemePreference } = await import('../../../src/scripts/theme.js');

      // Save dark preference
      saveThemePreference('dark');
      expect(localStorage.getItem('mirdb-theme')).toBe('dark');

      // Save light preference
      saveThemePreference('light');
      expect(localStorage.getItem('mirdb-theme')).toBe('light');
    });

    it('should restore theme preference on page load', async () => {
      // Pre-set dark theme in storage
      localStorage.setItem('mirdb-theme', 'dark');

      vi.resetModules();
      const { getCurrentTheme } = await import('../../../src/scripts/theme.js');

      // getCurrentTheme should return stored preference
      expect(getCurrentTheme()).toBe('dark');
    });

    it('should clear theme preference when clearThemePreference is called', async () => {
      vi.resetModules();
      const { saveThemePreference, clearThemePreference } = await import('../../../src/scripts/theme.js');

      // Set and verify preference exists
      saveThemePreference('dark');
      expect(localStorage.getItem('mirdb-theme')).toBe('dark');

      // Clear preference
      clearThemePreference();
      expect(localStorage.getItem('mirdb-theme')).toBeNull();
    });
  });
});

/**
 * Calculate relative luminance of a hex color
 * @param {string} hex - Hex color code
 * @returns {number} Relative luminance value
 */
function getRelativeLuminance(hex) {
  const rgb = hexToRgb(hex);
  const [r, g, b] = [rgb.r, rgb.g, rgb.b].map(c => {
    const sRGB = c / 255;
    return sRGB <= 0.03928
      ? sRGB / 12.92
      : Math.pow((sRGB + 0.055) / 1.055, 2.4);
  });
  return 0.2126 * r + 0.7152 * g + 0.0722 * b;
}

/**
 * Convert hex color to RGB
 * @param {string} hex - Hex color code
 * @returns {object} RGB values
 */
function hexToRgb(hex) {
  const result = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex);
  return result ? {
    r: parseInt(result[1], 16),
    g: parseInt(result[2], 16),
    b: parseInt(result[3], 16)
  } : null;
}
