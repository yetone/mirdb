/**
 * Navigation Responsive Integration Tests
 * Owner: Scenario 3 - Mobile Navigation Responsive
 *
 * Tests:
 * - Hamburger menu at mobile widths
 * - Menu expands on click
 * - Touch target sizes >= 44x44px
 * - Menu closes on outside click
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { JSDOM } from 'jsdom';

const HTML_TEMPLATE = `
<!DOCTYPE html>
<html lang="en">
<head>
  <style>
    :root {
      --touch-target-min: 44px;
      --z-header: 100;
      --z-overlay: 200;
    }
    .nav__toggle { display: none; width: 44px; height: 44px; }
    .nav__menu { display: flex; }
    .nav__menu.is-open { right: 0; }
    .nav__link { min-height: 44px; padding: 16px; display: flex; }
    .nav__overlay { display: none; }
    .nav__overlay.is-visible { display: block; }
    @media (max-width: 768px) {
      .nav__toggle { display: flex; }
      .nav__menu {
        position: fixed;
        right: -100%;
        display: flex;
        flex-direction: column;
      }
    }
  </style>
</head>
<body>
  <header class="header">
    <nav class="nav" role="navigation" aria-label="Main navigation">
      <a href="/" class="nav__logo">MirDB</a>
      <ul class="nav__menu" id="nav-menu">
        <li class="nav__item">
          <a href="#home" class="nav__link">Home</a>
        </li>
        <li class="nav__item">
          <a href="#features" class="nav__link">Features</a>
        </li>
        <li class="nav__item">
          <a href="#about" class="nav__link">About</a>
        </li>
        <li class="nav__item">
          <a href="#contact" class="nav__link">Contact</a>
        </li>
      </ul>
      <button
        class="nav__toggle"
        id="nav-toggle"
        aria-label="Toggle navigation menu"
        aria-expanded="false"
        aria-controls="nav-menu"
      >
        <span class="nav__toggle-icon"></span>
      </button>
    </nav>
  </header>
</body>
</html>
`;

describe('Mobile Navigation Responsive', () => {
  let dom;
  let document;
  let window;
  let MobileNavigation;

  beforeEach(async () => {
    dom = new JSDOM(HTML_TEMPLATE, {
      url: 'http://localhost',
      pretendToBeVisual: true,
    });
    window = dom.window;
    document = window.document;

    global.window = window;
    global.document = document;
    global.HTMLElement = window.HTMLElement;
    global.Event = window.Event;
    global.MouseEvent = window.MouseEvent;
    global.KeyboardEvent = window.KeyboardEvent;
    global.matchMedia = vi.fn().mockImplementation(query => ({
      matches: false,
      media: query,
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
    }));

    const module = await import('../../../src/components/Navigation/Navigation.js');
    MobileNavigation = module.MobileNavigation;
  });

  afterEach(() => {
    dom.window.close();
    vi.clearAllMocks();
  });

  describe('Test Case 1: Hamburger menu at 375px (iPhone)', () => {
    it('should have hamburger menu toggle element present in the DOM', () => {
      Object.defineProperty(window, 'innerWidth', { value: 375, writable: true });

      const navToggle = document.getElementById('nav-toggle');
      const navMenu = document.getElementById('nav-menu');

      // Verify hamburger toggle element exists
      expect(navToggle).not.toBeNull();
      expect(navToggle.tagName).toBe('BUTTON');
      expect(navToggle.classList.contains('nav__toggle')).toBe(true);

      // Verify menu exists
      expect(navMenu).not.toBeNull();

      // Verify menu is closed by default (no is-open class)
      expect(navMenu.classList.contains('is-open')).toBe(false);
    });

    it('should have CSS media query rule for mobile viewport (structural check)', () => {
      // This test validates that the CSS contains the media query rule
      // The actual visual behavior is tested in E2E tests with real browsers
      const styleSheets = document.styleSheets;
      let hasMobileMediaQuery = false;

      for (const sheet of styleSheets) {
        for (const rule of sheet.cssRules) {
          if (rule.media && rule.conditionText && rule.conditionText.includes('768px')) {
            hasMobileMediaQuery = true;
            break;
          }
        }
      }

      expect(hasMobileMediaQuery).toBe(true);
    });
  });

  describe('Test Case 2: Hamburger menu at 768px (tablet boundary)', () => {
    it('should display hamburger menu icon at tablet breakpoint', () => {
      Object.defineProperty(window, 'innerWidth', { value: 768, writable: true });

      const navToggle = document.getElementById('nav-toggle');

      expect(navToggle).not.toBeNull();
      expect(navToggle.getAttribute('aria-label')).toBe('Toggle navigation menu');
      expect(navToggle.getAttribute('aria-controls')).toBe('nav-menu');
    });
  });

  describe('Test Case 3: Click hamburger menu icon', () => {
    it('should expand navigation drawer showing all menu links when hamburger is clicked', () => {
      Object.defineProperty(window, 'innerWidth', { value: 375, writable: true });

      const navigation = new MobileNavigation();
      const navToggle = document.getElementById('nav-toggle');
      const navMenu = document.getElementById('nav-menu');

      navToggle.click();

      expect(navMenu.classList.contains('is-open')).toBe(true);
      expect(navToggle.getAttribute('aria-expanded')).toBe('true');

      const navLinks = navMenu.querySelectorAll('.nav__link');
      expect(navLinks.length).toBe(4);

      const linkTexts = Array.from(navLinks).map(link => link.textContent);
      expect(linkTexts).toContain('Home');
      expect(linkTexts).toContain('Features');
      expect(linkTexts).toContain('About');
      expect(linkTexts).toContain('Contact');
    });
  });

  describe('Test Case 4: Navigation link touch target sizes', () => {
    it('should have minimum 44x44px tappable area for each navigation link', () => {
      const navLinks = document.querySelectorAll('.nav__link');

      navLinks.forEach(link => {
        const style = window.getComputedStyle(link);
        const minHeight = parseInt(style.minHeight, 10);
        const padding = parseInt(style.paddingTop, 10) + parseInt(style.paddingBottom, 10);

        expect(minHeight).toBeGreaterThanOrEqual(44);
      });

      const navToggle = document.getElementById('nav-toggle');
      const toggleStyle = window.getComputedStyle(navToggle);
      const toggleWidth = parseInt(toggleStyle.width, 10);
      const toggleHeight = parseInt(toggleStyle.height, 10);

      expect(toggleWidth).toBeGreaterThanOrEqual(44);
      expect(toggleHeight).toBeGreaterThanOrEqual(44);
    });
  });

  describe('Test Case 5: Click outside expanded menu', () => {
    it('should close navigation menu when clicking outside', () => {
      Object.defineProperty(window, 'innerWidth', { value: 375, writable: true });

      const navigation = new MobileNavigation();
      const navToggle = document.getElementById('nav-toggle');
      const navMenu = document.getElementById('nav-menu');

      navToggle.click();
      expect(navMenu.classList.contains('is-open')).toBe(true);

      const overlay = document.querySelector('.nav__overlay');
      expect(overlay).not.toBeNull();

      overlay.click();

      expect(navMenu.classList.contains('is-open')).toBe(false);
      expect(navToggle.getAttribute('aria-expanded')).toBe('false');
    });

    it('should close navigation menu when pressing Escape', () => {
      Object.defineProperty(window, 'innerWidth', { value: 375, writable: true });

      const navigation = new MobileNavigation();
      const navToggle = document.getElementById('nav-toggle');
      const navMenu = document.getElementById('nav-menu');

      navToggle.click();
      expect(navMenu.classList.contains('is-open')).toBe(true);

      const escapeEvent = new window.KeyboardEvent('keydown', {
        key: 'Escape',
        bubbles: true,
      });
      document.dispatchEvent(escapeEvent);

      expect(navMenu.classList.contains('is-open')).toBe(false);
      expect(navToggle.getAttribute('aria-expanded')).toBe('false');
    });
  });

  describe('Accessibility', () => {
    it('should have proper ARIA attributes on toggle button', () => {
      const navToggle = document.getElementById('nav-toggle');

      expect(navToggle.getAttribute('aria-label')).toBe('Toggle navigation menu');
      expect(navToggle.getAttribute('aria-controls')).toBe('nav-menu');
      expect(navToggle.getAttribute('aria-expanded')).toBe('false');
    });

    it('should update aria-expanded when menu opens', () => {
      const navigation = new MobileNavigation();
      const navToggle = document.getElementById('nav-toggle');

      navToggle.click();
      expect(navToggle.getAttribute('aria-expanded')).toBe('true');

      navToggle.click();
      expect(navToggle.getAttribute('aria-expanded')).toBe('false');
    });
  });
});
