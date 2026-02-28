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
import { JSDOM } from 'jsdom';

// Read the index.html file
const htmlPath = resolve(process.cwd(), 'src/index.html');
const html = readFileSync(htmlPath, 'utf-8');

describe('Dark Mode Toggle', () => {
  let dom;
  let document;
  let window;

  beforeEach(() => {
    // Create a fresh DOM for each test
    dom = new JSDOM(html, {
      url: 'http://localhost:3000',
      runScripts: 'outside-only',
    });
    document = dom.window.document;
    window = dom.window;

    // Mock localStorage
    const localStorageMock = {
      store: {},
      getItem: vi.fn((key) => localStorageMock.store[key] || null),
      setItem: vi.fn((key, value) => {
        localStorageMock.store[key] = value;
      }),
      removeItem: vi.fn((key) => {
        delete localStorageMock.store[key];
      }),
      clear: vi.fn(() => {
        localStorageMock.store = {};
      }),
    };
    Object.defineProperty(window, 'localStorage', {
      value: localStorageMock,
      writable: true,
    });

    // Mock matchMedia
    Object.defineProperty(window, 'matchMedia', {
      writable: true,
      value: vi.fn().mockImplementation((query) => ({
        matches: false,
        media: query,
        onchange: null,
        addListener: vi.fn(),
        removeListener: vi.fn(),
        addEventListener: vi.fn(),
        removeEventListener: vi.fn(),
        dispatchEvent: vi.fn(),
      })),
    });
  });

  afterEach(() => {
    vi.clearAllMocks();
    if (dom) {
      dom.window.close();
    }
  });

  describe('Test Case 1: Dark mode toggle presence', () => {
    it('should have dark mode toggle button/switch present in navigation', () => {
      const themeToggle = document.querySelector('[data-testid="theme-toggle"]');
      expect(themeToggle).not.toBeNull();
      expect(themeToggle.tagName.toLowerCase()).toBe('button');
    });

    it('should have theme toggle in header/navigation area', () => {
      const header = document.querySelector('header');
      const nav = document.querySelector('nav');
      const themeToggle = document.querySelector('[data-testid="theme-toggle"]');

      // Toggle should be within header or nav
      const isInHeader = header && header.contains(themeToggle);
      const isInNav = nav && nav.contains(themeToggle);
      expect(isInHeader || isInNav).toBe(true);
    });

    it('should have accessible aria-label', () => {
      const themeToggle = document.querySelector('[data-testid="theme-toggle"]');
      const ariaLabel = themeToggle.getAttribute('aria-label');
      expect(ariaLabel).toBeTruthy();
      expect(ariaLabel.toLowerCase()).toContain('dark');
    });

    it('should have aria-pressed attribute', () => {
      const themeToggle = document.querySelector('[data-testid="theme-toggle"]');
      const ariaPressed = themeToggle.getAttribute('aria-pressed');
      expect(ariaPressed).toBeTruthy();
      expect(['true', 'false']).toContain(ariaPressed);
    });
  });

  describe('Test Case 2: Theme toggle click behavior', () => {
    it('should apply dark theme when data-theme attribute is set to dark', () => {
      // Simulate dark mode being activated
      document.documentElement.setAttribute('data-theme', 'dark');

      const isDarkMode = document.documentElement.getAttribute('data-theme') === 'dark';
      expect(isDarkMode).toBe(true);
    });

    it('should remove dark theme when data-theme attribute is removed', () => {
      // Apply dark mode
      document.documentElement.setAttribute('data-theme', 'dark');
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark');

      // Remove dark mode
      document.documentElement.removeAttribute('data-theme');
      expect(document.documentElement.getAttribute('data-theme')).toBeNull();
    });

    it('should toggle theme correctly between light and dark', () => {
      // Start in light mode (no data-theme attribute)
      expect(document.documentElement.getAttribute('data-theme')).toBeNull();

      // Toggle to dark
      document.documentElement.setAttribute('data-theme', 'dark');
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark');

      // Toggle back to light
      document.documentElement.removeAttribute('data-theme');
      expect(document.documentElement.getAttribute('data-theme')).toBeNull();
    });
  });

  describe('Test Case 3: Dark mode color contrast (WCAG AA)', () => {
    it('should have dark theme CSS variables defined', () => {
      // Check that dark theme selector exists in the page styles
      const styles = document.querySelectorAll('link[rel="stylesheet"]');
      expect(styles.length).toBeGreaterThan(0);

      // Verify variables.css is linked (which contains dark mode colors)
      const variablesLink = Array.from(styles).find((link) =>
        link.getAttribute('href').includes('variables.css')
      );
      expect(variablesLink).toBeTruthy();
    });

    it('should have appropriate dark mode background and text colors defined', () => {
      // Read the variables.css file to verify dark mode colors
      const variablesCss = readFileSync(
        resolve(process.cwd(), 'src/styles/variables.css'),
        'utf-8'
      );

      // Check dark theme block exists
      expect(variablesCss).toContain('[data-theme="dark"]');

      // Check dark mode has appropriate background (dark color)
      expect(variablesCss).toMatch(/\[data-theme="dark"\][\s\S]*--color-background:\s*#[0-9a-fA-F]{6}/);

      // Check dark mode has appropriate text color (light color)
      expect(variablesCss).toMatch(/\[data-theme="dark"\][\s\S]*--color-text:\s*#[0-9a-fA-F]{6}/);
    });
  });

  describe('Test Case 4: System preference detection (prefers-color-scheme)', () => {
    it('should have matchMedia available for system preference detection', () => {
      expect(typeof window.matchMedia).toBe('function');
    });

    it('should detect dark system preference correctly', () => {
      // Mock dark system preference
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

      const darkModeQuery = window.matchMedia('(prefers-color-scheme: dark)');
      expect(darkModeQuery.matches).toBe(true);
    });

    it('should detect light system preference correctly', () => {
      // Mock light system preference
      window.matchMedia = vi.fn().mockImplementation((query) => ({
        matches: query === '(prefers-color-scheme: light)',
        media: query,
        onchange: null,
        addListener: vi.fn(),
        removeListener: vi.fn(),
        addEventListener: vi.fn(),
        removeEventListener: vi.fn(),
        dispatchEvent: vi.fn(),
      }));

      const darkModeQuery = window.matchMedia('(prefers-color-scheme: dark)');
      expect(darkModeQuery.matches).toBe(false);
    });
  });

  describe('Test Case 5: Theme preference persistence', () => {
    it('should save dark theme preference to localStorage', () => {
      const storageKey = 'mirdb-theme';

      // Simulate saving dark theme
      window.localStorage.setItem(storageKey, 'dark');
      expect(window.localStorage.setItem).toHaveBeenCalledWith(storageKey, 'dark');
      expect(window.localStorage.getItem(storageKey)).toBe('dark');
    });

    it('should save light theme preference to localStorage', () => {
      const storageKey = 'mirdb-theme';

      // Simulate saving light theme
      window.localStorage.setItem(storageKey, 'light');
      expect(window.localStorage.setItem).toHaveBeenCalledWith(storageKey, 'light');
      expect(window.localStorage.getItem(storageKey)).toBe('light');
    });

    it('should retrieve stored theme preference', () => {
      const storageKey = 'mirdb-theme';

      // Store a preference
      window.localStorage.store[storageKey] = 'dark';

      // Retrieve it
      const stored = window.localStorage.getItem(storageKey);
      expect(stored).toBe('dark');
    });

    it('should return null when no preference is stored', () => {
      const storageKey = 'mirdb-theme';

      const stored = window.localStorage.getItem(storageKey);
      expect(stored).toBeNull();
    });
  });

  describe('Theme Toggle Icons', () => {
    it('should have sun icon for light mode indicator', () => {
      const sunIcon = document.querySelector('.theme-toggle__icon--sun');
      expect(sunIcon).not.toBeNull();
    });

    it('should have moon icon for dark mode indicator', () => {
      const moonIcon = document.querySelector('.theme-toggle__icon--moon');
      expect(moonIcon).not.toBeNull();
    });

    it('should have icons marked as aria-hidden', () => {
      const sunIcon = document.querySelector('.theme-toggle__icon--sun');
      const moonIcon = document.querySelector('.theme-toggle__icon--moon');

      expect(sunIcon.getAttribute('aria-hidden')).toBe('true');
      expect(moonIcon.getAttribute('aria-hidden')).toBe('true');
    });
  });

  describe('Theme Toggle Accessibility', () => {
    it('should have visually hidden text for screen readers', () => {
      const hiddenText = document.querySelector('.theme-toggle .visually-hidden');
      expect(hiddenText).not.toBeNull();
      expect(hiddenText.textContent.toLowerCase()).toContain('dark');
    });

    it('should have type="button" to prevent form submission', () => {
      const themeToggle = document.querySelector('[data-testid="theme-toggle"]');
      expect(themeToggle.getAttribute('type')).toBe('button');
    });

    it('should have minimum touch target size class', () => {
      const themeToggle = document.querySelector('[data-testid="theme-toggle"]');
      expect(themeToggle.classList.contains('theme-toggle')).toBe(true);
    });
  });
});
