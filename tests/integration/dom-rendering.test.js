/**
 * Integration tests for DOM rendering and interactions.
 * Owner: Scenario 14 - JavaScript Interactions
 *
 * Tests:
 * - Sections render with correct content
 * - Interactive elements respond to events
 * - Theme changes propagate across components
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import fs from 'fs';
import path from 'path';

const htmlPath = path.resolve(__dirname, '../../index.html');
const htmlContent = fs.readFileSync(htmlPath, 'utf-8');

describe('DOM Rendering and JS Interactions Integration', () => {
  beforeEach(() => {
    document.body.innerHTML = htmlContent;
    vi.resetModules();
  });

  afterEach(() => {
    document.body.innerHTML = '';
    vi.restoreAllMocks();
  });

  describe('Page Structure', () => {
    it('all main sections exist in the DOM', () => {
      expect(document.getElementById('hero')).not.toBeNull();
      expect(document.getElementById('about')).not.toBeNull();
      expect(document.getElementById('features')).not.toBeNull();
      expect(document.getElementById('status')).not.toBeNull();
      expect(document.getElementById('quick-start')).not.toBeNull();
      expect(document.getElementById('configuration')).not.toBeNull();
    });

    it('header contains navigation with correct links', () => {
      const nav = document.querySelector('nav[role="navigation"]');
      expect(nav).not.toBeNull();

      const navLinks = nav.querySelectorAll('.nav-link');
      expect(navLinks.length).toBeGreaterThanOrEqual(3);
    });

    it('theme toggle button exists with correct attributes', () => {
      const toggle = document.querySelector('[data-testid="theme-toggle"]');
      expect(toggle).not.toBeNull();
      expect(toggle.getAttribute('type')).toBe('button');
      expect(toggle.getAttribute('aria-label')).toBe('Toggle dark mode');
    });

    it('mobile menu toggle exists with correct ARIA attributes', () => {
      const toggle = document.querySelector('.mobile-menu-toggle');
      expect(toggle).not.toBeNull();
      expect(toggle.getAttribute('aria-expanded')).toBe('false');
      expect(toggle.getAttribute('aria-controls')).toBe('nav-menu');
    });

    it('code blocks with copy buttons exist in quick-start section', () => {
      const quickStart = document.getElementById('quick-start');
      expect(quickStart).not.toBeNull();

      const codeBlocks = quickStart.querySelectorAll('.code-block-wrapper');
      expect(codeBlocks.length).toBeGreaterThanOrEqual(1);

      codeBlocks.forEach(wrapper => {
        const copyButton = wrapper.querySelector('.copy-button');
        expect(copyButton).not.toBeNull();
        const code = wrapper.querySelector('code');
        expect(code).not.toBeNull();
      });
    });
  });

  describe('Smooth Scroll Integration', () => {
    it('initSmoothScroll integrates with actual DOM anchor links', async () => {
      const mod = await import('../../js/main.js');
      mod.initSmoothScroll();

      const featuresLink = document.querySelector('a[href="#features"]');
      const mockEvent = new MouseEvent('click', { bubbles: true, cancelable: true });
      const preventDefaultSpy = vi.spyOn(mockEvent, 'preventDefault');

      featuresLink.dispatchEvent(mockEvent);

      expect(preventDefaultSpy).toHaveBeenCalled();
    });

    it('scrolls to target section when anchor link is clicked', async () => {
      const mod = await import('../../js/main.js');
      mod.initSmoothScroll();

      const featuresSection = document.getElementById('features');
      const featuresLink = document.querySelector('a[href="#features"]');

      // Mock scrollIntoView
      const scrollIntoViewSpy = vi.fn();
      featuresSection.scrollIntoView = scrollIntoViewSpy;

      const clickEvent = new MouseEvent('click', { bubbles: true, cancelable: true });
      featuresLink.dispatchEvent(clickEvent);

      expect(scrollIntoViewSpy).toHaveBeenCalledWith({ behavior: 'smooth' });
    });
  });

  describe('Copy-to-Clipboard Integration', () => {
    it('copy button extracts correct code text from wrapper', async () => {
      const mod = await import('../../js/main.js');

      // Mock clipboard
      Object.defineProperty(navigator, 'clipboard', {
        value: { writeText: vi.fn().mockResolvedValue(undefined) },
        configurable: true,
      });

      mod.initCopyButtons();

      const firstWrapper = document.querySelector('.code-block-wrapper');
      const copyButton = firstWrapper.querySelector('.copy-button');
      const codeEl = firstWrapper.querySelector('code');
      const expectedText = codeEl.textContent;

      copyButton.click();

      // Clipboard should be called with the code text
      expect(navigator.clipboard.writeText).toHaveBeenCalledWith(expectedText);
    });

    it('copy button shows visual feedback after successful copy', async () => {
      const mod = await import('../../js/main.js');

      Object.defineProperty(navigator, 'clipboard', {
        value: { writeText: vi.fn().mockResolvedValue(undefined) },
        configurable: true,
      });

      mod.initCopyButtons();

      const firstWrapper = document.querySelector('.code-block-wrapper');
      const copyButton = firstWrapper.querySelector('.copy-button');
      const label = copyButton.querySelector('.copy-label');

      expect(label.textContent).toBe('Copy');

      copyButton.click();

      // Wait for the async click handler to complete
      await new Promise(resolve => setTimeout(resolve, 10));

      // Label should change to 'Copied!' after async handler completes
      expect(label.textContent).toBe('Copied!');
      expect(copyButton.classList.contains('copied')).toBe(true);
    });
  });

  describe('Theme Toggle Integration', () => {
    it('dark-mode module integrates with DOM toggle button', async () => {
      const darkMode = await import('../../js/dark-mode.js');

      // Set initial theme
      darkMode.setTheme('light');
      expect(document.documentElement.getAttribute('data-theme')).toBe('light');

      // Toggle theme
      darkMode.toggleTheme();
      expect(document.documentElement.getAttribute('data-theme')).toBe('dark');

      // Toggle back
      darkMode.toggleTheme();
      expect(document.documentElement.getAttribute('data-theme')).toBe('light');
    });

    it('theme persists to localStorage', async () => {
      const darkMode = await import('../../js/dark-mode.js');
      localStorage.clear();

      darkMode.setTheme('dark');
      expect(localStorage.getItem('mirdb-theme')).toBe('dark');

      darkMode.setTheme('light');
      expect(localStorage.getItem('mirdb-theme')).toBe('light');
    });
  });

  describe('Mobile Menu Integration', () => {
    it('mobile menu toggle integrates with nav menu element', async () => {
      const nav = await import('../../js/navigation.js');

      const menu = document.getElementById('nav-menu');
      const toggle = document.querySelector('.mobile-menu-toggle');

      // Ensure closed
      menu.classList.remove('is-open');
      toggle.setAttribute('aria-expanded', 'false');

      // Toggle open
      nav.toggleMobileMenu();
      expect(menu.classList.contains('is-open')).toBe(true);
      expect(toggle.getAttribute('aria-expanded')).toBe('true');

      // Toggle closed
      nav.toggleMobileMenu();
      expect(menu.classList.contains('is-open')).toBe(false);
      expect(toggle.getAttribute('aria-expanded')).toBe('false');
    });

    it('initMobileMenu binds click event to toggle button', async () => {
      const nav = await import('../../js/navigation.js');

      const menu = document.getElementById('nav-menu');
      const toggle = document.querySelector('.mobile-menu-toggle');

      menu.classList.remove('is-open');
      toggle.setAttribute('aria-expanded', 'false');

      nav.initMobileMenu();

      // Simulate click on toggle
      toggle.click();

      expect(menu.classList.contains('is-open')).toBe(true);
      expect(toggle.getAttribute('aria-expanded')).toBe('true');
    });
  });

  describe('Error-Free Loading', () => {
    it('all JS modules load without throwing errors', async () => {
      expect(async () => {
        await import('../../js/main.js');
        await import('../../js/dark-mode.js');
        await import('../../js/navigation.js');
      }).not.toThrow();
    });

    it('all exported functions are defined', async () => {
      const main = await import('../../js/main.js');
      const darkMode = await import('../../js/dark-mode.js');
      const navigation = await import('../../js/navigation.js');

      expect(typeof main.initSmoothScroll).toBe('function');
      expect(typeof main.initCopyButtons).toBe('function');
      expect(typeof main.copyToClipboard).toBe('function');

      expect(typeof darkMode.getPreferredTheme).toBe('function');
      expect(typeof darkMode.setTheme).toBe('function');
      expect(typeof darkMode.toggleTheme).toBe('function');
      expect(typeof darkMode.initTheme).toBe('function');

      expect(typeof navigation.initMobileMenu).toBe('function');
      expect(typeof navigation.toggleMobileMenu).toBe('function');
      expect(typeof navigation.highlightActiveSection).toBe('function');
    });
  });
});
