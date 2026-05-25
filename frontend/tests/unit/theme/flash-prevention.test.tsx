/**
 * Tests for flash-of-light-theme prevention.
 * Covers REQ-12 (no flash of light theme on reload).
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

describe('Flash Prevention', () => {
  beforeEach(() => {
    document.documentElement.classList.remove('dark');
    window.localStorage.clear();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('simulates flash prevention script with dark theme in localStorage', () => {
    window.localStorage.setItem('theme', 'dark');
    const script = `
      (function () {
        var theme = localStorage.getItem('theme');
        if (theme === 'dark' || (!theme && window.matchMedia('(prefers-color-scheme: dark)').matches)) {
          document.documentElement.classList.add('dark');
        }
      })();
    `;
    expect(document.documentElement.classList.contains('dark')).toBe(false);
    new Function(script)();
    expect(document.documentElement.classList.contains('dark')).toBe(true);
  });

  it('simulates flash prevention script with light theme in localStorage', () => {
    window.localStorage.setItem('theme', 'light');
    const script = `
      (function () {
        var theme = localStorage.getItem('theme');
        if (theme === 'dark' || (!theme && window.matchMedia('(prefers-color-scheme: dark)').matches)) {
          document.documentElement.classList.add('dark');
        }
      })();
    `;
    new Function(script)();
    expect(document.documentElement.classList.contains('dark')).toBe(false);
  });

  it('simulates flash prevention script with no localStorage and no system dark', () => {
    vi.stubGlobal(
      'matchMedia',
      vi.fn(() => ({
        matches: false,
        media: '',
        onchange: null,
        addListener: vi.fn(),
        removeListener: vi.fn(),
        addEventListener: vi.fn(),
        removeEventListener: vi.fn(),
        dispatchEvent: vi.fn(),
      }))
    );
    const script = `
      (function () {
        var theme = localStorage.getItem('theme');
        if (theme === 'dark' || (!theme && window.matchMedia('(prefers-color-scheme: dark)').matches)) {
          document.documentElement.classList.add('dark');
        }
      })();
    `;
    new Function(script)();
    expect(document.documentElement.classList.contains('dark')).toBe(false);
  });

  it('simulates flash prevention script with no localStorage and system dark preference', () => {
    vi.stubGlobal(
      'matchMedia',
      vi.fn(() => ({
        matches: true,
        media: '',
        onchange: null,
        addListener: vi.fn(),
        removeListener: vi.fn(),
        addEventListener: vi.fn(),
        removeEventListener: vi.fn(),
        dispatchEvent: vi.fn(),
      }))
    );
    const script = `
      (function () {
        var theme = localStorage.getItem('theme');
        if (theme === 'dark' || (!theme && window.matchMedia('(prefers-color-scheme: dark)').matches)) {
          document.documentElement.classList.add('dark');
        }
      })();
    `;
    new Function(script)();
    expect(document.documentElement.classList.contains('dark')).toBe(true);
  });
});
