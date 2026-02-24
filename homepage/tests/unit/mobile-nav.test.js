/**
 * Mobile Navigation Unit Tests
 * Owner: Scenario 2 - Navigation Header
 *
 * Test cases:
 * - Hamburger menu opens nav on click
 * - Nav closes when link is clicked
 * - Nav closes on escape key
 * - aria-expanded attribute updates correctly
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { JSDOM } from 'jsdom';

// Mobile nav module functions will be imported after DOM setup
let mobileNavModule;

/**
 * Create a minimal header HTML structure for testing
 */
function createHeaderHTML() {
  return `
    <!DOCTYPE html>
    <html lang="en">
    <head>
      <meta charset="UTF-8">
      <title>Test</title>
    </head>
    <body>
      <header class="header">
        <nav class="container flex-between" aria-label="Main navigation">
          <a href="/" class="logo">MirDB</a>

          <div class="nav-links" id="nav-links">
            <a href="#quickstart">Quick Start</a>
            <a href="https://github.com/example/mirdb" id="github-link">GitHub</a>
          </div>

          <button type="button" class="hamburger-btn" id="hamburger-btn"
                  aria-label="Open navigation menu" aria-expanded="false" aria-controls="mobile-nav">
            <span class="hamburger-line"></span>
            <span class="hamburger-line"></span>
            <span class="hamburger-line"></span>
          </button>
        </nav>

        <div class="mobile-nav" id="mobile-nav" aria-hidden="true">
          <nav class="mobile-nav-content" aria-label="Mobile navigation">
            <a href="#quickstart" class="mobile-nav-link">Quick Start</a>
            <a href="#features" class="mobile-nav-link">Features</a>
            <a href="https://github.com/example/mirdb" class="mobile-nav-link" id="mobile-github-link">GitHub</a>
          </nav>
        </div>
      </header>
    </body>
    </html>
  `;
}

describe('Mobile Navigation', () => {
  let dom;
  let document;
  let window;
  let hamburgerBtn;
  let mobileNav;

  beforeEach(async () => {
    // Create fresh DOM for each test
    dom = new JSDOM(createHeaderHTML(), {
      url: 'http://localhost',
      runScripts: 'dangerously',
    });

    window = dom.window;
    document = window.document;

    // Set up global references for the module
    global.window = window;
    global.document = document;
    global.MouseEvent = window.MouseEvent;
    global.KeyboardEvent = window.KeyboardEvent;

    // Import module fresh for each test
    vi.resetModules();
    mobileNavModule = await import('../../assets/js/mobile-nav.js');

    // Reset module state
    mobileNavModule.resetMobileNav();

    hamburgerBtn = document.getElementById('hamburger-btn');
    mobileNav = document.getElementById('mobile-nav');
  });

  afterEach(() => {
    // Clean up global references
    delete global.window;
    delete global.document;
    delete global.MouseEvent;
    delete global.KeyboardEvent;
    vi.resetModules();
  });

  describe('initMobileNav', () => {
    it('should initialize without errors', () => {
      expect(() => mobileNavModule.initMobileNav()).not.toThrow();
    });

    it('should handle missing elements gracefully', () => {
      // Remove elements
      hamburgerBtn.remove();
      mobileNav.remove();

      // Should not throw
      expect(() => mobileNavModule.initMobileNav()).not.toThrow();
    });
  });

  describe('openMobileNav', () => {
    beforeEach(() => {
      mobileNavModule.initMobileNav();
    });

    it('should set aria-expanded to true on hamburger button', () => {
      mobileNavModule.openMobileNav();
      expect(hamburgerBtn.getAttribute('aria-expanded')).toBe('true');
    });

    it('should set aria-hidden to false on mobile nav', () => {
      mobileNavModule.openMobileNav();
      expect(mobileNav.getAttribute('aria-hidden')).toBe('false');
    });

    it('should add is-open class to mobile nav', () => {
      mobileNavModule.openMobileNav();
      expect(mobileNav.classList.contains('is-open')).toBe(true);
    });

    it('should add is-active class to hamburger button', () => {
      mobileNavModule.openMobileNav();
      expect(hamburgerBtn.classList.contains('is-active')).toBe(true);
    });

    it('should update aria-label to Close navigation menu', () => {
      mobileNavModule.openMobileNav();
      expect(hamburgerBtn.getAttribute('aria-label')).toBe('Close navigation menu');
    });

    it('should set isOpen to true', () => {
      mobileNavModule.openMobileNav();
      expect(mobileNavModule.isOpen()).toBe(true);
    });
  });

  describe('closeMobileNav', () => {
    beforeEach(() => {
      mobileNavModule.initMobileNav();
      mobileNavModule.openMobileNav();
    });

    it('should set aria-expanded to false on hamburger button', () => {
      mobileNavModule.closeMobileNav();
      expect(hamburgerBtn.getAttribute('aria-expanded')).toBe('false');
    });

    it('should set aria-hidden to true on mobile nav', () => {
      mobileNavModule.closeMobileNav();
      expect(mobileNav.getAttribute('aria-hidden')).toBe('true');
    });

    it('should remove is-open class from mobile nav', () => {
      mobileNavModule.closeMobileNav();
      expect(mobileNav.classList.contains('is-open')).toBe(false);
    });

    it('should remove is-active class from hamburger button', () => {
      mobileNavModule.closeMobileNav();
      expect(hamburgerBtn.classList.contains('is-active')).toBe(false);
    });

    it('should update aria-label to Open navigation menu', () => {
      mobileNavModule.closeMobileNav();
      expect(hamburgerBtn.getAttribute('aria-label')).toBe('Open navigation menu');
    });

    it('should set isOpen to false', () => {
      mobileNavModule.closeMobileNav();
      expect(mobileNavModule.isOpen()).toBe(false);
    });
  });

  describe('toggleMobileNav', () => {
    beforeEach(() => {
      mobileNavModule.initMobileNav();
    });

    it('should open nav when closed', () => {
      expect(mobileNavModule.isOpen()).toBe(false);
      mobileNavModule.toggleMobileNav();
      expect(mobileNavModule.isOpen()).toBe(true);
    });

    it('should close nav when open', () => {
      mobileNavModule.openMobileNav();
      expect(mobileNavModule.isOpen()).toBe(true);
      mobileNavModule.toggleMobileNav();
      expect(mobileNavModule.isOpen()).toBe(false);
    });
  });

  describe('hamburger button click', () => {
    beforeEach(() => {
      mobileNavModule.initMobileNav();
    });

    it('should toggle nav on hamburger button click', () => {
      expect(mobileNavModule.isOpen()).toBe(false);

      // Click to open
      hamburgerBtn.click();
      expect(mobileNavModule.isOpen()).toBe(true);
      expect(hamburgerBtn.getAttribute('aria-expanded')).toBe('true');

      // Click to close
      hamburgerBtn.click();
      expect(mobileNavModule.isOpen()).toBe(false);
      expect(hamburgerBtn.getAttribute('aria-expanded')).toBe('false');
    });
  });

  describe('Escape key handling', () => {
    beforeEach(() => {
      mobileNavModule.initMobileNav();
    });

    it('should close nav on Escape key press when open', () => {
      mobileNavModule.openMobileNav();
      expect(mobileNavModule.isOpen()).toBe(true);

      const escapeEvent = new window.KeyboardEvent('keydown', {
        key: 'Escape',
        bubbles: true,
      });
      document.dispatchEvent(escapeEvent);

      expect(mobileNavModule.isOpen()).toBe(false);
    });

    it('should not affect nav on Escape key press when closed', () => {
      expect(mobileNavModule.isOpen()).toBe(false);

      const escapeEvent = new window.KeyboardEvent('keydown', {
        key: 'Escape',
        bubbles: true,
      });
      document.dispatchEvent(escapeEvent);

      expect(mobileNavModule.isOpen()).toBe(false);
    });
  });

  describe('navigation link clicks', () => {
    beforeEach(() => {
      mobileNavModule.initMobileNav();
      mobileNavModule.openMobileNav();
    });

    it('should close nav when anchor link is clicked', () => {
      const anchorLink = mobileNav.querySelector('a[href="#quickstart"]');
      expect(mobileNavModule.isOpen()).toBe(true);

      anchorLink.click();

      expect(mobileNavModule.isOpen()).toBe(false);
    });

    it('should not close nav when external link is clicked', () => {
      const externalLink = document.getElementById('mobile-github-link');
      expect(mobileNavModule.isOpen()).toBe(true);

      // External links don't have # prefix, so nav should stay open
      externalLink.click();

      // The nav stays open for external links
      expect(mobileNavModule.isOpen()).toBe(true);
    });
  });

  describe('isOpen', () => {
    beforeEach(() => {
      mobileNavModule.initMobileNav();
    });

    it('should return false initially', () => {
      expect(mobileNavModule.isOpen()).toBe(false);
    });

    it('should return true when nav is opened', () => {
      mobileNavModule.openMobileNav();
      expect(mobileNavModule.isOpen()).toBe(true);
    });

    it('should return false after nav is closed', () => {
      mobileNavModule.openMobileNav();
      mobileNavModule.closeMobileNav();
      expect(mobileNavModule.isOpen()).toBe(false);
    });
  });
});
