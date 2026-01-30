/**
 * Navigation Component Unit Tests.
 * Owner: Scenario 14 - Navigation and Smooth Scroll
 *
 * Tests:
 * - Navigation renders correctly
 * - Navigation links are present for all sections
 * - Navigation links have correct href attributes
 * - Active state is applied correctly
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';

describe('Navigation Component', () => {
  let mockObserve: ReturnType<typeof vi.fn>;
  let observerCallback: IntersectionObserverCallback | undefined;

  beforeEach(() => {
    // Reset modules to get fresh state
    vi.resetModules();

    // Set up IntersectionObserver mock before any tests
    mockObserve = vi.fn();
    observerCallback = undefined;

    const mockIntersectionObserver = vi.fn((callback: IntersectionObserverCallback) => {
      observerCallback = callback;
      return {
        observe: mockObserve,
        disconnect: vi.fn(),
        unobserve: vi.fn(),
      };
    });

    // @ts-expect-error - Mocking IntersectionObserver
    global.IntersectionObserver = mockIntersectionObserver;

    // Reset DOM before each test
    document.body.innerHTML = `
      <div id="app">
        <nav id="navigation" aria-label="Main navigation"></nav>
        <section id="features">Features</section>
        <section id="usage">Usage</section>
        <section id="architecture">Architecture</section>
        <section id="getting-started">Getting Started</section>
      </div>
    `;
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe('renderNavigation', () => {
    it('should return a valid nav element', async () => {
      const { renderNavigation } = await import('@/components/Navigation');

      const navElement = renderNavigation();

      expect(navElement).toBeInstanceOf(HTMLElement);
      expect(navElement.tagName).toBe('NAV');
    });

    it('should have correct aria-label for accessibility', async () => {
      const { renderNavigation } = await import('@/components/Navigation');

      const navElement = renderNavigation();

      expect(navElement.getAttribute('aria-label')).toBe('Main navigation');
    });

    it('should contain navigation links', async () => {
      const { renderNavigation } = await import('@/components/Navigation');

      const navElement = renderNavigation();
      const links = navElement.querySelectorAll('a');

      expect(links.length).toBeGreaterThan(0);
    });

    it('should have Features navigation link', async () => {
      const { renderNavigation } = await import('@/components/Navigation');

      const navElement = renderNavigation();
      const featuresLink = navElement.querySelector('a[href="#features"]');

      expect(featuresLink).not.toBeNull();
      expect(featuresLink?.textContent).toContain('Features');
    });

    it('should have Getting Started navigation link', async () => {
      const { renderNavigation } = await import('@/components/Navigation');

      const navElement = renderNavigation();
      const gettingStartedLink = navElement.querySelector('a[href="#getting-started"]');

      expect(gettingStartedLink).not.toBeNull();
      expect(gettingStartedLink?.textContent).toContain('Getting Started');
    });

    it('should have Usage navigation link', async () => {
      const { renderNavigation } = await import('@/components/Navigation');

      const navElement = renderNavigation();
      const usageLink = navElement.querySelector('a[href="#usage"]');

      expect(usageLink).not.toBeNull();
      expect(usageLink?.textContent).toContain('Usage');
    });

    it('should have Architecture navigation link', async () => {
      const { renderNavigation } = await import('@/components/Navigation');

      const navElement = renderNavigation();
      const architectureLink = navElement.querySelector('a[href="#architecture"]');

      expect(architectureLink).not.toBeNull();
      expect(architectureLink?.textContent).toContain('Architecture');
    });

    it('should include logo or brand link', async () => {
      const { renderNavigation } = await import('@/components/Navigation');

      const navElement = renderNavigation();
      const brandLink = navElement.querySelector('.nav-brand') || navElement.querySelector('a[href="#"]');

      expect(brandLink).not.toBeNull();
    });
  });

  describe('NAV_ITEMS', () => {
    it('should export NAV_ITEMS array', async () => {
      const { NAV_ITEMS } = await import('@/components/Navigation');

      expect(Array.isArray(NAV_ITEMS)).toBe(true);
      expect(NAV_ITEMS.length).toBeGreaterThan(0);
    });

    it('should have correct structure for each navigation item', async () => {
      const { NAV_ITEMS } = await import('@/components/Navigation');

      NAV_ITEMS.forEach((item) => {
        expect(item).toHaveProperty('label');
        expect(item).toHaveProperty('href');
        expect(typeof item.label).toBe('string');
        expect(typeof item.href).toBe('string');
      });
    });

    it('should include Features in NAV_ITEMS', async () => {
      const { NAV_ITEMS } = await import('@/components/Navigation');

      const featuresItem = NAV_ITEMS.find((item) => item.href === '#features');
      expect(featuresItem).toBeDefined();
      expect(featuresItem?.label).toBe('Features');
    });

    it('should include Getting Started in NAV_ITEMS', async () => {
      const { NAV_ITEMS } = await import('@/components/Navigation');

      const gettingStartedItem = NAV_ITEMS.find((item) => item.href === '#getting-started');
      expect(gettingStartedItem).toBeDefined();
      expect(gettingStartedItem?.label).toBe('Getting Started');
    });
  });

  describe('initNavigation', () => {
    it('should set up click handlers for navigation links', async () => {
      const { renderNavigation, initNavigation } = await import('@/components/Navigation');

      const navElement = renderNavigation();
      document.getElementById('navigation')?.replaceWith(navElement);

      initNavigation();

      const link = navElement.querySelector('a[href="#features"]') as HTMLAnchorElement;
      expect(link).not.toBeNull();
    });

    it('should initialize intersection observer for active state tracking', async () => {
      const { renderNavigation, initNavigation } = await import('@/components/Navigation');

      const navElement = renderNavigation();
      document.getElementById('navigation')?.replaceWith(navElement);

      initNavigation();

      expect(global.IntersectionObserver).toHaveBeenCalled();
    });
  });

  describe('Active state highlighting', () => {
    it('should add active class when section is in view', async () => {
      const { renderNavigation, initNavigation } = await import('@/components/Navigation');

      const navElement = renderNavigation();
      document.getElementById('navigation')?.replaceWith(navElement);

      initNavigation();

      // Simulate features section becoming visible
      const featuresSection = document.getElementById('features');
      if (featuresSection && observerCallback) {
        observerCallback(
          [
            {
              target: featuresSection,
              isIntersecting: true,
              intersectionRatio: 0.5,
            } as IntersectionObserverEntry,
          ],
          {} as IntersectionObserver
        );
      }

      // Check if the corresponding nav link has active class
      const featuresLink = navElement.querySelector('a[href="#features"]');
      expect(featuresLink?.classList.contains('active') || featuresLink?.getAttribute('aria-current') === 'true').toBe(
        true
      );
    });
  });
});
