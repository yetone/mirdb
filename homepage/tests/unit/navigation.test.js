/**
 * Scenario 13 - Navigation & Smooth Scroll Unit Tests
 * Owner: Scenario 13
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { JSDOM } from 'jsdom';
import { initNavigation } from '../../js/navigation.js';

function createDom(html) {
  const dom = new JSDOM(html, {
    url: 'http://localhost:3000',
    runScripts: 'dangerously',
  });
  global.document = dom.window.document;
  global.window = dom.window;

  // JSDOM does not implement scrollIntoView; stub it for spying
  if (!dom.window.Element.prototype.scrollIntoView) {
    dom.window.Element.prototype.scrollIntoView = function scrollIntoView() {};
  }

  return dom;
}

function resetGlobals() {
  delete global.document;
  delete global.window;
}

const TEST_HTML = `
<!DOCTYPE html>
<html>
<body>
  <nav id="navbar" class="navbar">
    <div class="nav-container">
      <a href="#" class="nav-logo">MirDB</a>
      <button id="hamburger" class="hamburger" aria-controls="nav-list" aria-expanded="false" aria-label="Toggle navigation menu">
        <span class="hamburger-line"></span>
        <span class="hamburger-line"></span>
        <span class="hamburger-line"></span>
      </button>
      <ul id="nav-list" class="nav-list">
        <li><a href="#features">Features</a></li>
        <li><a href="#install">Install</a></li>
        <li><a href="#usage">Usage</a></li>
        <li><a href="#architecture">Architecture</a></li>
        <li><a href="#resources">Resources</a></li>
      </ul>
      <button id="theme-toggle" class="theme-toggle" aria-label="Toggle dark mode">
        <span>☀</span>
      </button>
    </div>
  </nav>
  <section id="features">Features</section>
  <section id="install">Install</section>
  <section id="usage">Usage</section>
  <section id="architecture">Architecture</section>
  <section id="resources">Resources</section>
</body>
</html>
`;

describe('navigation.js', () => {
  let dom;

  beforeEach(() => {
    dom = createDom(TEST_HTML);
  });

  afterEach(() => {
    resetGlobals();
    vi.restoreAllMocks();
  });

  // Test Case 1 (integration-style check): nav contains at least 5 anchor links
  describe('nav structure', () => {
    it('has at least 5 anchor links with href starting with #', () => {
      initNavigation();
      const anchors = document.querySelectorAll('nav a[href^="#"]');
      expect(anchors.length).toBeGreaterThanOrEqual(5);
    });

    it('contains anchor links to all required sections', () => {
      initNavigation();
      const requiredHrefs = ['#features', '#install', '#usage', '#architecture', '#resources'];
      requiredHrefs.forEach((href) => {
        const anchor = document.querySelector(`nav a[href="${href}"]`);
        expect(anchor).not.toBeNull();
      });
    });
  });

  // Test Case 2: smooth scroll on anchor click
  describe('smooth scroll', () => {
    it('calls scrollIntoView with behavior smooth when clicking a nav anchor', () => {
      const scrollIntoViewSpy = vi.spyOn(
        dom.window.Element.prototype,
        'scrollIntoView'
      );

      initNavigation();

      const installLink = document.querySelector('nav a[href="#install"]');
      installLink.click();

      expect(scrollIntoViewSpy).toHaveBeenCalledTimes(1);
      expect(scrollIntoViewSpy).toHaveBeenCalledWith({ behavior: 'smooth' });
    });

    it('does not intercept clicks on the logo link (href="#")', () => {
      const scrollIntoViewSpy = vi.spyOn(
        dom.window.Element.prototype,
        'scrollIntoView'
      );

      initNavigation();

      const logoLink = document.querySelector('nav a[href="#"]');
      logoLink.click();

      expect(scrollIntoViewSpy).not.toHaveBeenCalled();
    });
  });

  // Test Case 3: scrolled class on scroll
  describe('sticky nav scroll behavior', () => {
    it('adds "scrolled" class when window.scrollY > 32', () => {
      const navbar = document.querySelector('#navbar');
      initNavigation();

      // Simulate scroll past threshold
      dom.window.scrollY = 64;
      dom.window.dispatchEvent(new dom.window.Event('scroll'));

      expect(navbar.classList.contains('scrolled')).toBe(true);
    });

    it('removes "scrolled" class when window.scrollY <= 32', () => {
      const navbar = document.querySelector('#navbar');
      initNavigation();

      // First scroll past threshold
      dom.window.scrollY = 64;
      dom.window.dispatchEvent(new dom.window.Event('scroll'));
      expect(navbar.classList.contains('scrolled')).toBe(true);

      // Then scroll back up
      dom.window.scrollY = 10;
      dom.window.dispatchEvent(new dom.window.Event('scroll'));
      expect(navbar.classList.contains('scrolled')).toBe(false);
    });
  });

  // Test Case 4: hamburger toggle on
  describe('hamburger menu toggle', () => {
    it('sets aria-expanded to true and adds open class on first click', () => {
      initNavigation();

      const hamburger = document.querySelector('#hamburger');
      const navList = document.querySelector('#nav-list');

      expect(hamburger.getAttribute('aria-expanded')).toBe('false');
      expect(navList.classList.contains('open')).toBe(false);

      hamburger.click();

      expect(hamburger.getAttribute('aria-expanded')).toBe('true');
      expect(navList.classList.contains('open')).toBe(true);
    });
  });

  // Test Case 5: hamburger toggle off
  describe('hamburger menu toggle off', () => {
    it('toggles aria-expanded between true and false on repeated clicks', () => {
      initNavigation();

      const hamburger = document.querySelector('#hamburger');

      // First click: open
      hamburger.click();
      expect(hamburger.getAttribute('aria-expanded')).toBe('true');

      // Second click: close
      hamburger.click();
      expect(hamburger.getAttribute('aria-expanded')).toBe('false');
    });

    it('toggles open class on the nav list on repeated clicks', () => {
      initNavigation();

      const hamburger = document.querySelector('#hamburger');
      const navList = document.querySelector('#nav-list');

      hamburger.click();
      expect(navList.classList.contains('open')).toBe(true);

      hamburger.click();
      expect(navList.classList.contains('open')).toBe(false);
    });
  });

  // Test Case 6: Escape key closes hamburger menu
  describe('Escape key behavior', () => {
    it('closes the menu and sets aria-expanded to false when Escape is pressed', () => {
      initNavigation();

      const hamburger = document.querySelector('#hamburger');
      const navList = document.querySelector('#nav-list');

      // Open the menu first
      hamburger.click();
      expect(hamburger.getAttribute('aria-expanded')).toBe('true');
      expect(navList.classList.contains('open')).toBe(true);

      // Press Escape
      const escapeEvent = new dom.window.KeyboardEvent('keydown', {
        key: 'Escape',
        bubbles: true,
      });
      document.dispatchEvent(escapeEvent);

      expect(hamburger.getAttribute('aria-expanded')).toBe('false');
      expect(navList.classList.contains('open')).toBe(false);
    });

    it('does not modify state when a non-Escape key is pressed', () => {
      initNavigation();

      const hamburger = document.querySelector('#hamburger');
      const navList = document.querySelector('#nav-list');

      // Open the menu
      hamburger.click();
      expect(hamburger.getAttribute('aria-expanded')).toBe('true');

      // Press a different key
      const otherEvent = new dom.window.KeyboardEvent('keydown', {
        key: 'Enter',
        bubbles: true,
      });
      document.dispatchEvent(otherEvent);

      expect(hamburger.getAttribute('aria-expanded')).toBe('true');
      expect(navList.classList.contains('open')).toBe(true);
    });
  });
});
