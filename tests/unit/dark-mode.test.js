/**
 * Unit tests for dark mode module.
 * Owner: Scenario 10 - Dark Mode Theme
 *
 * Tests:
 * - Theme detection from system preferences
 * - localStorage persistence
 * - Toggle behavior
 * - CSS class application
 */

import { describe, test, expect, beforeEach, afterEach, vi } from 'vitest';

// We need to load the module fresh for each test
function loadModule() {
  // Clear module cache
  vi.resetModules();
  // Reset the DOM
  document.documentElement.removeAttribute('data-theme');
  return import('../../js/dark-mode.js');
}

describe('getPreferredTheme', () => {
  let originalMatchMedia;

  beforeEach(() => {
    originalMatchMedia = window.matchMedia;
  });

  afterEach(() => {
    window.matchMedia = originalMatchMedia;
  });

  test('returns "dark" when prefers-color-scheme is dark', async () => {
    window.matchMedia = vi.fn().mockImplementation((query) => ({
      matches: query === '(prefers-color-scheme: dark)',
      media: query,
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
      dispatchEvent: vi.fn(),
    }));

    const { getPreferredTheme } = await loadModule();
    expect(getPreferredTheme()).toBe('dark');
  });

  test('returns "light" when prefers-color-scheme is light', async () => {
    window.matchMedia = vi.fn().mockImplementation((query) => ({
      matches: query !== '(prefers-color-scheme: dark)',
      media: query,
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
      dispatchEvent: vi.fn(),
    }));

    const { getPreferredTheme } = await loadModule();
    expect(getPreferredTheme()).toBe('light');
  });

  test('returns "light" when matchMedia is not available', async () => {
    window.matchMedia = undefined;

    const { getPreferredTheme } = await loadModule();
    expect(getPreferredTheme()).toBe('light');
  });
});

describe('setTheme', () => {
  beforeEach(() => {
    document.documentElement.removeAttribute('data-theme');
    localStorage.clear();
  });

  test('sets data-theme attribute to "dark" on html element', async () => {
    const { setTheme } = await loadModule();
    setTheme('dark');
    expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
  });

  test('sets data-theme attribute to "light" on html element', async () => {
    const { setTheme } = await loadModule();
    setTheme('light');
    expect(document.documentElement.getAttribute('data-theme')).toBe('light');
  });

  test('persists theme to localStorage', async () => {
    const { setTheme } = await loadModule();
    setTheme('dark');
    expect(localStorage.getItem('mirdb-theme')).toBe('dark');
  });

  test('persists light theme to localStorage', async () => {
    const { setTheme } = await loadModule();
    setTheme('light');
    expect(localStorage.getItem('mirdb-theme')).toBe('light');
  });
});

describe('toggleTheme', () => {
  beforeEach(() => {
    document.documentElement.removeAttribute('data-theme');
    localStorage.clear();
  });

  test('toggles from light to dark', async () => {
    const { setTheme, toggleTheme } = await loadModule();
    setTheme('light');
    toggleTheme();
    expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
  });

  test('toggles from dark to light', async () => {
    const { setTheme, toggleTheme } = await loadModule();
    setTheme('dark');
    toggleTheme();
    expect(document.documentElement.getAttribute('data-theme')).toBe('light');
  });

  test('toggles from no theme to dark', async () => {
    const { toggleTheme } = await loadModule();
    // Simulate dark system preference
    window.matchMedia = vi.fn().mockImplementation((query) => ({
      matches: query === '(prefers-color-scheme: dark)',
      media: query,
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
      dispatchEvent: vi.fn(),
    }));
    toggleTheme();
    expect(document.documentElement.getAttribute('data-theme')).toBe('light');
  });
});

describe('initTheme', () => {
  beforeEach(() => {
    document.documentElement.removeAttribute('data-theme');
    localStorage.clear();
  });

  test('uses stored theme from localStorage when available', async () => {
    localStorage.setItem('mirdb-theme', 'dark');
    const { initTheme } = await loadModule();
    initTheme();
    expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
  });

  test('falls back to system preference when no localStorage value', async () => {
    window.matchMedia = vi.fn().mockImplementation((query) => ({
      matches: query === '(prefers-color-scheme: dark)',
      media: query,
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
      dispatchEvent: vi.fn(),
    }));

    const { initTheme } = await loadModule();
    initTheme();
    expect(document.documentElement.getAttribute('data-theme')).toBe('dark');
  });

  test('defaults to light when no preference exists', async () => {
    window.matchMedia = undefined;
    const { initTheme } = await loadModule();
    initTheme();
    expect(document.documentElement.getAttribute('data-theme')).toBe('light');
  });
});

describe('dark mode CSS variables', () => {
  test('dark-mode.css file contains [data-theme="dark"] overrides', () => {
    // We verify the CSS file has the expected selectors by reading it
    const fs = require('fs');
    const css = fs.readFileSync('./css/dark-mode.css', 'utf-8');

    expect(css).toContain('[data-theme="dark"]');
    expect(css).toContain('--color-background');
    expect(css).toContain('--color-text');
    expect(css).toContain('--color-primary');
    expect(css).toContain('--color-accent');
  });

  test('dark-mode.css sets dark background color (#0f172a or similar)', () => {
    const fs = require('fs');
    const css = fs.readFileSync('./css/dark-mode.css', 'utf-8');

    expect(css).toContain('#0f172a');
  });

  test('dark-mode.css sets light text color (#e2e8f0 or similar)', () => {
    const fs = require('fs');
    const css = fs.readFileSync('./css/dark-mode.css', 'utf-8');

    expect(css).toContain('#e2e8f0');
  });

  test('dark-mode.css contains code block dark background override', () => {
    const fs = require('fs');
    const css = fs.readFileSync('./css/dark-mode.css', 'utf-8');

    expect(css).toContain('[data-theme="dark"] .code-block');
    expect(css).toContain('#1e1e1e');
  });
});
