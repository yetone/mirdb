/**
 * Navigation tests for MirDB homepage.
 * Owner: Scenario 4 - Navigation System
 *
 * Test framework: Vitest + jsdom
 *
 * Test coverage:
 * - Navigation bar contains required nav links (Features, Documentation, GitHub, Community)
 * - Sticky header CSS position is defined
 * - Internal links use smooth scroll behavior
 * - External links (GitHub) open in new tab with rel="noopener noreferrer"
 * - Hamburger menu button hidden on desktop, visible on mobile
 * - Mobile menu toggles on hamburger click
 * - Mobile menu closes on Escape key
 * - Mobile menu closes on link selection
 * - ARIA attributes for menu state
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import { JSDOM } from 'jsdom';
import fs from 'node:fs';
import path from 'node:path';

const htmlPath = path.resolve(__dirname, '../index.html');
const cssPath = path.resolve(__dirname, '../css/navigation.css');
const jsPath = path.resolve(__dirname, '../js/navigation.js');

function loadDOM() {
  const html = fs.readFileSync(htmlPath, 'utf-8');
  const dom = new JSDOM(html, {
    url: 'http://localhost:8080',
    runScripts: 'dangerously',
    resources: 'usable',
    beforeParse(window) {
      // jsdom does not implement scrollIntoView
      if (!window.HTMLElement.prototype.scrollIntoView) {
        window.HTMLElement.prototype.scrollIntoView = vi.fn();
      }
    },
  });
  return dom;
}

describe('Navigation System', () => {
  let dom;
  let document;
  let window;

  describe('Test Case 1: Navigation bar structure and required links', () => {
    beforeEach(() => {
      dom = loadDOM();
      document = dom.window.document;
      window = dom.window;
    });

    it('should have a nav element with aria-label "Main navigation"', () => {
      const nav = document.querySelector('nav');
      expect(nav).not.toBeNull();
      expect(nav.getAttribute('aria-label')).toBe('Main navigation');
    });

    it('should contain a "Features" link that points to #features', () => {
      const featuresLink = Array.from(document.querySelectorAll('.nav-link')).find(
        (l) => l.textContent.trim() === 'Features'
      );
      expect(featuresLink).not.toBeNull();
      expect(featuresLink.getAttribute('href')).toBe('#features');
    });

    it('should contain a "Documentation" link that points to #quick-start', () => {
      const docLink = Array.from(document.querySelectorAll('.nav-link')).find(
        (l) => l.textContent.trim() === 'Documentation'
      );
      expect(docLink).not.toBeNull();
      expect(docLink.getAttribute('href')).toBe('#quick-start');
    });

    it('should contain a "GitHub" link that points to the MirDB GitHub repository', () => {
      const ghLink = Array.from(document.querySelectorAll('.nav-link')).find(
        (l) => l.textContent.trim() === 'GitHub'
      );
      expect(ghLink).not.toBeNull();
      expect(ghLink.getAttribute('href')).toMatch(/github\.com\/.*mirdb/i);
    });

    it('should contain a "Community" link that points to #resources', () => {
      const commLink = Array.from(document.querySelectorAll('.nav-link')).find(
        (l) => l.textContent.trim() === 'Community'
      );
      expect(commLink).not.toBeNull();
      expect(commLink.getAttribute('href')).toBe('#resources');
    });

    it('should have nav links in a ul list', () => {
      const navList = document.querySelector('nav .nav-links');
      expect(navList).not.toBeNull();
      expect(navList.tagName).toBe('UL');
    });
  });

  describe('Test Case 2: Sticky header behavior', () => {
    it('should have header with position: sticky in CSS', () => {
      expect(fs.existsSync(cssPath)).toBe(true);
      const css = fs.readFileSync(cssPath, 'utf-8');
      expect(css).toMatch(/header\s*\{[^}]*position\s*:\s*sticky/);
    });

    it('should have header with top: 0', () => {
      const css = fs.readFileSync(cssPath, 'utf-8');
      expect(css).toMatch(/header\s*\{[^}]*top\s*:\s*0/);
    });

    it('should have a z-index on header to stay above content', () => {
      const css = fs.readFileSync(cssPath, 'utf-8');
      expect(css).toMatch(/header\s*\{[^}]*z-index\s*:/);
    });

    it('should define a scrolled state for header with box-shadow', () => {
      const css = fs.readFileSync(cssPath, 'utf-8');
      expect(css).toMatch(/header\.scrolled\s*\{/);
    });

    beforeEach(() => {
      dom = loadDOM();
      document = dom.window.document;
      window = dom.window;
    });

    it('should have a header element present', () => {
      const header = document.querySelector('header');
      expect(header).not.toBeNull();
    });

    it('should have a logo visible in the header', () => {
      const logo = document.querySelector('header img');
      expect(logo).not.toBeNull();
    });
  });

  describe('Test Case 3: Smooth scroll for internal links', () => {
    beforeEach(() => {
      dom = loadDOM();
      document = dom.window.document;
      window = dom.window;
    });

    it('should have scroll-behavior: smooth on html element', () => {
      const baseCss = fs.readFileSync(
        path.resolve(__dirname, '../css/base.css'),
        'utf-8'
      );
      expect(baseCss).toMatch(/scroll-behavior\s*:\s*smooth/);
    });

    it('should have a Features section target for internal link', () => {
      const features = document.getElementById('features');
      expect(features).not.toBeNull();
    });

    it('should have internal links that use href starting with #', () => {
      const internalLinks = Array.from(
        document.querySelectorAll('.nav-link[href^="#"]')
      );
      expect(internalLinks.length).toBeGreaterThanOrEqual(2);
    });
  });

  describe('Test Case 4: GitHub nav link opens in new tab', () => {
    beforeEach(() => {
      dom = loadDOM();
      document = dom.window.document;
    });

    it('should have GitHub link with target="_blank"', () => {
      const ghLink = Array.from(document.querySelectorAll('.nav-link')).find(
        (l) => l.textContent.trim() === 'GitHub'
      );
      expect(ghLink).not.toBeNull();
      expect(ghLink.getAttribute('target')).toBe('_blank');
    });

    it('should have GitHub link with rel="noopener noreferrer"', () => {
      const ghLink = Array.from(document.querySelectorAll('.nav-link')).find(
        (l) => l.textContent.trim() === 'GitHub'
      );
      const rel = ghLink.getAttribute('rel');
      expect(rel).toMatch(/noopener/);
      expect(rel).toMatch(/noreferrer/);
    });

    it('should have no in-page anchor link for GitHub', () => {
      const ghLink = Array.from(document.querySelectorAll('.nav-link')).find(
        (l) => l.textContent.trim() === 'GitHub'
      );
      expect(ghLink.getAttribute('href')).not.toMatch(/^#/);
    });
  });

  describe('Test Case 5: Documentation link navigates to Quick Start', () => {
    beforeEach(() => {
      dom = loadDOM();
      document = dom.window.document;
    });

    it('should have Documentation link pointing to #quick-start', () => {
      const docLink = Array.from(document.querySelectorAll('.nav-link')).find(
        (l) => l.textContent.trim() === 'Documentation'
      );
      expect(docLink).not.toBeNull();
      expect(docLink.getAttribute('href')).toBe('#quick-start');
    });

    it('should have a quick-start section on the page', () => {
      const quickStart = document.getElementById('quick-start');
      expect(quickStart).not.toBeNull();
    });
  });

  describe('Test Case 6: Mobile hamburger menu visibility', () => {
    beforeEach(() => {
      dom = loadDOM();
      document = dom.window.document;
    });

    it('should have a hamburger button element', () => {
      const hamburger = document.querySelector('.hamburger');
      expect(hamburger).not.toBeNull();
    });

    it('should have hamburger button with aria-label "Toggle navigation menu"', () => {
      const hamburger = document.querySelector('.hamburger');
      expect(hamburger).not.toBeNull();
      expect(hamburger.getAttribute('aria-label')).toMatch(/Toggle navigation/i);
    });

    it('should have hamburger with aria-expanded="false" by default', () => {
      const hamburger = document.querySelector('.hamburger');
      expect(hamburger.getAttribute('aria-expanded')).toBe('false');
    });

    it('should hide hamburger on desktop in CSS (display: none by default)', () => {
      expect(fs.existsSync(cssPath)).toBe(true);
      const css = fs.readFileSync(cssPath, 'utf-8');
      // Hamburger should have display: none at its base (shown only on mobile via media query)
      expect(css).toMatch(/\.hamburger\s*\{[^}]*display\s*:\s*none/);
    });

    it('should show hamburger on mobile viewport via media query', () => {
      const css = fs.readFileSync(cssPath, 'utf-8');
      // Within a max-width media query, hamburger should become flex
      expect(css).toMatch(/display\s*:\s*flex/);
    });

    it('should have hamburger with 3 child span elements (hamburger lines)', () => {
      const hamburger = document.querySelector('.hamburger');
      const lines = hamburger.querySelectorAll('.hamburger-line');
      expect(lines.length).toBe(3);
    });

    it('should have a mobile-menu element', () => {
      const mobileMenu = document.querySelector('.mobile-menu');
      expect(mobileMenu).not.toBeNull();
    });

    it('should have nav links inside mobile-menu', () => {
      const mobileLinks = document.querySelectorAll('.mobile-menu .nav-link');
      expect(mobileLinks.length).toBeGreaterThanOrEqual(4);
    });
  });

  describe('Test Case 7: Mobile hamburger menu opens', () => {
    function setupWithJS() {
      const html = fs.readFileSync(htmlPath, 'utf-8');
      const dom = new JSDOM(html, {
        url: 'http://localhost:8080',
        runScripts: 'dangerously',
        resources: 'usable',
        beforeParse(window) {
          if (!window.HTMLElement.prototype.scrollIntoView) {
            window.HTMLElement.prototype.scrollIntoView = vi.fn();
          }
        },
      });
      // Execute the navigation.js script in the jsdom context
      const navJS = fs.readFileSync(jsPath, 'utf-8');
      const scriptEl = dom.window.document.createElement('script');
      scriptEl.textContent = navJS;
      dom.window.document.body.appendChild(scriptEl);
      return dom;
    }

    it('should set aria-expanded to "true" when hamburger is clicked', () => {
      const dom = setupWithJS();
      const hamburger = dom.window.document.querySelector('.hamburger');
      expect(hamburger.getAttribute('aria-expanded')).toBe('false');
      hamburger.click();
      expect(hamburger.getAttribute('aria-expanded')).toBe('true');
    });

    it('should add "open" class to mobile-menu when hamburger is clicked', () => {
      const dom = setupWithJS();
      const hamburger = dom.window.document.querySelector('.hamburger');
      const mobileMenu = dom.window.document.querySelector('.mobile-menu');
      expect(mobileMenu.classList.contains('open')).toBe(false);
      hamburger.click();
      expect(mobileMenu.classList.contains('open')).toBe(true);
    });

    it('should have mobile menu transition defined with ~300ms', () => {
      const css = fs.readFileSync(cssPath, 'utf-8');
      expect(css).toMatch(/transition\s*:\s*transform\s+0\.3s/);
    });

    it('should have mobile menu links visible when open', () => {
      const dom = setupWithJS();
      const hamburger = dom.window.document.querySelector('.hamburger');
      hamburger.click();
      const mobileLinks = dom.window.document.querySelectorAll(
        '.mobile-menu .nav-link'
      );
      expect(mobileLinks.length).toBeGreaterThanOrEqual(4);
    });
  });

  describe('Test Case 8: Selecting a menu item closes menu', () => {
    function setupWithJS() {
      const html = fs.readFileSync(htmlPath, 'utf-8');
      const dom = new JSDOM(html, {
        url: 'http://localhost:8080',
        runScripts: 'dangerously',
        resources: 'usable',
        beforeParse(window) {
          if (!window.HTMLElement.prototype.scrollIntoView) {
            window.HTMLElement.prototype.scrollIntoView = vi.fn();
          }
        },
      });
      const navJS = fs.readFileSync(jsPath, 'utf-8');
      const scriptEl = dom.window.document.createElement('script');
      scriptEl.textContent = navJS;
      dom.window.document.body.appendChild(scriptEl);
      return dom;
    }

    it('should close mobile menu when a link is clicked', () => {
      const dom = setupWithJS();
      const hamburger = dom.window.document.querySelector('.hamburger');
      const mobileMenu = dom.window.document.querySelector('.mobile-menu');

      // Open the menu first
      hamburger.click();
      expect(mobileMenu.classList.contains('open')).toBe(true);

      // Click a link in the mobile menu
      const mobileLink = mobileMenu.querySelector('.nav-link');
      mobileLink.click();

      expect(mobileMenu.classList.contains('open')).toBe(false);
    });

    it('should set aria-expanded to "false" after link click', () => {
      const dom = setupWithJS();
      const hamburger = dom.window.document.querySelector('.hamburger');
      const mobileMenu = dom.window.document.querySelector('.mobile-menu');

      hamburger.click();
      expect(hamburger.getAttribute('aria-expanded')).toBe('true');

      const mobileLink = mobileMenu.querySelector('.nav-link');
      mobileLink.click();

      expect(hamburger.getAttribute('aria-expanded')).toBe('false');
    });

    it('should call scrollIntoView when internal anchor link is clicked', () => {
      const dom = setupWithJS();
      const featuresLink = Array.from(
        dom.window.document.querySelectorAll('.nav-link')
      ).find((l) => l.textContent.trim() === 'Features');
      const featuresEl = dom.window.document.getElementById('features');

      // Spy on scrollIntoView
      const scrollSpy = vi.fn();
      featuresEl.scrollIntoView = scrollSpy;

      featuresLink.click();

      expect(scrollSpy).toHaveBeenCalledWith({ behavior: 'smooth' });
    });
  });

  describe('Test Case 9: Escape key closes mobile menu', () => {
    function setupWithJS() {
      const html = fs.readFileSync(htmlPath, 'utf-8');
      const dom = new JSDOM(html, {
        url: 'http://localhost:8080',
        runScripts: 'dangerously',
        resources: 'usable',
        beforeParse(window) {
          if (!window.HTMLElement.prototype.scrollIntoView) {
            window.HTMLElement.prototype.scrollIntoView = vi.fn();
          }
        },
      });
      const navJS = fs.readFileSync(jsPath, 'utf-8');
      const scriptEl = dom.window.document.createElement('script');
      scriptEl.textContent = navJS;
      dom.window.document.body.appendChild(scriptEl);
      return dom;
    }

    it('should close mobile menu on Escape key press', () => {
      const dom = setupWithJS();
      const hamburger = dom.window.document.querySelector('.hamburger');
      const mobileMenu = dom.window.document.querySelector('.mobile-menu');

      // Open the mobile menu
      hamburger.click();
      expect(mobileMenu.classList.contains('open')).toBe(true);

      // Press Escape
      const event = new dom.window.KeyboardEvent('keydown', {
        key: 'Escape',
        code: 'Escape',
        keyCode: 27,
        bubbles: true,
      });
      dom.window.document.dispatchEvent(event);

      expect(mobileMenu.classList.contains('open')).toBe(false);
    });

    it('should set aria-expanded to "false" after Escape key', () => {
      const dom = setupWithJS();
      const hamburger = dom.window.document.querySelector('.hamburger');
      const mobileMenu = dom.window.document.querySelector('.mobile-menu');

      hamburger.click();
      expect(hamburger.getAttribute('aria-expanded')).toBe('true');

      const event = new dom.window.KeyboardEvent('keydown', {
        key: 'Escape',
        code: 'Escape',
        keyCode: 27,
        bubbles: true,
      });
      dom.window.document.dispatchEvent(event);

      expect(hamburger.getAttribute('aria-expanded')).toBe('false');
    });

    it('should not close menu on non-Escape key press', () => {
      const dom = setupWithJS();
      const hamburger = dom.window.document.querySelector('.hamburger');
      const mobileMenu = dom.window.document.querySelector('.mobile-menu');

      hamburger.click();
      expect(mobileMenu.classList.contains('open')).toBe(true);

      const event = new dom.window.KeyboardEvent('keydown', {
        key: 'Enter',
        code: 'Enter',
        keyCode: 13,
        bubbles: true,
      });
      dom.window.document.dispatchEvent(event);

      // Should still be open (only Escape closes)
      expect(mobileMenu.classList.contains('open')).toBe(true);
    });
  });

  describe('Additional: Accessibility ARIA attributes', () => {
    beforeEach(() => {
      dom = loadDOM();
      document = dom.window.document;
    });

    it('should have hamburger button as a button element', () => {
      const hamburger = document.querySelector('.hamburger');
      expect(hamburger.tagName).toBe('BUTTON');
    });

    it('should have hamburger lines as spans with class hamburger-line', () => {
      const hamburger = document.querySelector('.hamburger');
      const lines = hamburger.querySelectorAll('.hamburger-line');
      expect(lines.length).toBe(3);
      lines.forEach((line) => {
        expect(line.tagName).toBe('SPAN');
      });
    });

    it('should have accessible name on hamburger via aria-label', () => {
      const hamburger = document.querySelector('.hamburger');
      const label = hamburger.getAttribute('aria-label');
      expect(label).toBeTruthy();
    });
  });
});
