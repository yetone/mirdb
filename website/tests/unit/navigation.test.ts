/**
 * Navigation Unit Tests
 * Owner: Scenario 7 - Navigation and Smooth Scrolling
 *
 * Unit tests for:
 * - initNavigation function
 * - smoothScrollTo function
 * - toggleMobileMenu function
 * - Mobile menu open/close functions
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

// Mock DOM setup for navigation tests
function createMockDOM() {
  document.body.innerHTML = `
    <header class="header" id="site-header">
      <div class="container header-container">
        <a href="#" class="header-logo" aria-label="MirDB Home">
          <span class="logo-text">MirDB</span>
        </a>
        <nav class="nav" aria-label="Main navigation">
          <ul class="nav-list">
            <li><a href="#features" class="nav-link">Features</a></li>
            <li><a href="#quick-start" class="nav-link">Quick Start</a></li>
            <li><a href="#architecture" class="nav-link">Architecture</a></li>
            <li><a href="#protocol" class="nav-link">Protocol</a></li>
            <li><a href="#status" class="nav-link">Status</a></li>
          </ul>
        </nav>
        <button class="mobile-menu-toggle" type="button" aria-label="Toggle navigation menu" aria-expanded="false" aria-controls="mobile-nav">
          <span class="hamburger-line"></span>
          <span class="hamburger-line"></span>
          <span class="hamburger-line"></span>
        </button>
      </div>
    </header>
    <main>
      <section id="hero">Hero Section</section>
      <section id="features">Features Section</section>
      <section id="quick-start">Quick Start Section</section>
      <section id="architecture">Architecture Section</section>
      <section id="protocol">Protocol Section</section>
      <section id="status">Status Section</section>
    </main>
  `;
}

describe('Navigation Component', () => {
  beforeEach(() => {
    createMockDOM();
    // Mock scrollIntoView
    Element.prototype.scrollIntoView = vi.fn();
    // Mock requestAnimationFrame
    vi.spyOn(window, 'requestAnimationFrame').mockImplementation((cb) => {
      cb(0);
      return 0;
    });
  });

  afterEach(() => {
    vi.restoreAllMocks();
    document.body.innerHTML = '';
  });

  describe('smoothScrollTo', () => {
    it('should call scrollIntoView on target element', async () => {
      const { smoothScrollTo } = await import('../../src/js/components/navigation.js');

      smoothScrollTo('features');

      const featuresSection = document.getElementById('features');
      expect(featuresSection?.scrollIntoView).toHaveBeenCalledWith({
        behavior: 'smooth',
        block: 'start'
      });
    });

    it('should not throw error if target does not exist', async () => {
      const { smoothScrollTo } = await import('../../src/js/components/navigation.js');

      expect(() => smoothScrollTo('nonexistent')).not.toThrow();
    });

    it('should close mobile menu after scrolling', async () => {
      const { smoothScrollTo, toggleMobileMenu } = await import('../../src/js/components/navigation.js');

      // Open mobile menu first
      toggleMobileMenu();

      const nav = document.querySelector('.nav');
      expect(nav?.classList.contains('is-open')).toBe(true);

      // Scroll to section
      smoothScrollTo('features');

      // Menu should be closed
      expect(nav?.classList.contains('is-open')).toBe(false);
    });
  });

  describe('toggleMobileMenu', () => {
    it('should open mobile menu when closed', async () => {
      const { toggleMobileMenu } = await import('../../src/js/components/navigation.js');

      const nav = document.querySelector('.nav');
      const toggle = document.querySelector('.mobile-menu-toggle');

      // Initially closed
      expect(nav?.classList.contains('is-open')).toBe(false);
      expect(toggle?.getAttribute('aria-expanded')).toBe('false');

      // Toggle open
      toggleMobileMenu();

      expect(nav?.classList.contains('is-open')).toBe(true);
      expect(toggle?.getAttribute('aria-expanded')).toBe('true');
    });

    it('should close mobile menu when open', async () => {
      const { toggleMobileMenu } = await import('../../src/js/components/navigation.js');

      // Open menu first
      toggleMobileMenu();

      const nav = document.querySelector('.nav');
      const toggle = document.querySelector('.mobile-menu-toggle');

      expect(nav?.classList.contains('is-open')).toBe(true);

      // Toggle closed
      toggleMobileMenu();

      expect(nav?.classList.contains('is-open')).toBe(false);
      expect(toggle?.getAttribute('aria-expanded')).toBe('false');
    });
  });

  describe('openMobileMenu', () => {
    it('should add is-open class and set aria-expanded to true', async () => {
      // Import directly - need to access internal function
      const module = await import('../../src/js/components/navigation.js');
      const { openMobileMenu } = module;

      openMobileMenu();

      const nav = document.querySelector('.nav');
      const toggle = document.querySelector('.mobile-menu-toggle');

      expect(nav?.classList.contains('is-open')).toBe(true);
      expect(toggle?.getAttribute('aria-expanded')).toBe('true');
    });
  });

  describe('closeMobileMenu', () => {
    it('should remove is-open class and set aria-expanded to false', async () => {
      const module = await import('../../src/js/components/navigation.js');
      const { openMobileMenu, closeMobileMenu } = module;

      // Open first
      openMobileMenu();

      const nav = document.querySelector('.nav');
      const toggle = document.querySelector('.mobile-menu-toggle');

      expect(nav?.classList.contains('is-open')).toBe(true);

      // Close
      closeMobileMenu();

      expect(nav?.classList.contains('is-open')).toBe(false);
      expect(toggle?.getAttribute('aria-expanded')).toBe('false');
    });
  });

  describe('initNavigation', () => {
    it('should not throw errors during initialization', async () => {
      const { initNavigation } = await import('../../src/js/components/navigation.js');

      expect(() => initNavigation()).not.toThrow();
    });

    it('should set up click handler on mobile menu toggle', async () => {
      const { initNavigation } = await import('../../src/js/components/navigation.js');

      initNavigation();

      const toggle = document.querySelector('.mobile-menu-toggle') as HTMLButtonElement;
      const nav = document.querySelector('.nav');

      // Click toggle
      toggle.click();

      expect(nav?.classList.contains('is-open')).toBe(true);
    });

    it('should set up click handlers on nav links', async () => {
      const { initNavigation } = await import('../../src/js/components/navigation.js');

      initNavigation();

      const featuresLink = document.querySelector('.nav-link[href="#features"]') as HTMLAnchorElement;
      const featuresSection = document.getElementById('features');

      // Click nav link
      featuresLink.click();

      expect(featuresSection?.scrollIntoView).toHaveBeenCalled();
    });
  });
});

describe('Header Scroll Effects', () => {
  let originalScrollY: number;

  beforeEach(() => {
    createMockDOM();
    originalScrollY = window.scrollY;
    vi.spyOn(window, 'requestAnimationFrame').mockImplementation((cb) => {
      cb(0);
      return 0;
    });
  });

  afterEach(() => {
    vi.restoreAllMocks();
    vi.resetModules();
    document.body.innerHTML = '';
    Object.defineProperty(window, 'scrollY', { value: originalScrollY, writable: true, configurable: true });
  });

  it('should add scrolled class when scrollY > 10', async () => {
    const { initNavigation } = await import('../../src/js/components/navigation.js');

    initNavigation();

    const header = document.querySelector('.header');

    // Simulate scroll
    Object.defineProperty(window, 'scrollY', { value: 100, writable: true, configurable: true });
    window.dispatchEvent(new Event('scroll'));

    expect(header?.classList.contains('scrolled')).toBe(true);
  });

  it('should remove scrolled class when scrollY <= 10', async () => {
    // Reset modules to get fresh event listeners
    vi.resetModules();
    const { initNavigation } = await import('../../src/js/components/navigation.js');

    initNavigation();

    const header = document.querySelector('.header');

    // First scroll down - add scrolled class directly for setup
    header?.classList.add('scrolled');
    expect(header?.classList.contains('scrolled')).toBe(true);

    // Then scroll to top - use fresh scroll event
    Object.defineProperty(window, 'scrollY', { value: 0, writable: true, configurable: true });
    window.dispatchEvent(new Event('scroll'));

    expect(header?.classList.contains('scrolled')).toBe(false);
  });
});

describe('Navigation Link Validation', () => {
  beforeEach(() => {
    createMockDOM();
  });

  afterEach(() => {
    document.body.innerHTML = '';
  });

  it('should have 5 navigation links', () => {
    const navLinks = document.querySelectorAll('.nav-link');
    expect(navLinks.length).toBe(5);
  });

  it('should have correct href attributes for all nav links', () => {
    const expectedHrefs = ['#features', '#quick-start', '#architecture', '#protocol', '#status'];
    const navLinks = document.querySelectorAll('.nav-link');

    navLinks.forEach((link, index) => {
      expect(link.getAttribute('href')).toBe(expectedHrefs[index]);
    });
  });

  it('should have correct text content for all nav links', () => {
    const expectedTexts = ['Features', 'Quick Start', 'Architecture', 'Protocol', 'Status'];
    const navLinks = document.querySelectorAll('.nav-link');

    navLinks.forEach((link, index) => {
      expect(link.textContent).toBe(expectedTexts[index]);
    });
  });

  it('should have corresponding sections for all nav links', () => {
    const navLinks = document.querySelectorAll('.nav-link');

    navLinks.forEach((link) => {
      const href = link.getAttribute('href');
      if (href) {
        const sectionId = href.substring(1);
        const section = document.getElementById(sectionId);
        expect(section).not.toBeNull();
      }
    });
  });
});
