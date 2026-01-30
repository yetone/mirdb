/**
 * Navigation Integration Tests.
 * Owner: Scenario 14 - Navigation and Smooth Scroll
 *
 * Tests:
 * - Navigation links scroll to correct sections
 * - URL anchor navigation works correctly
 * - Active state is updated on scroll
 * - Get Started CTA scrolls to getting-started section
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';

describe('Navigation Integration', () => {
  let mockObserve: ReturnType<typeof vi.fn>;
  let observerCallback: IntersectionObserverCallback | undefined;

  beforeEach(() => {
    // Reset modules first
    vi.resetModules();

    // Set up IntersectionObserver mock
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

    // Clear location hash to prevent auto-scroll on module load
    Object.defineProperty(window, 'location', {
      writable: true,
      value: { hash: '', href: 'http://localhost/', ...window.location },
    });

    // Reset DOM with full page structure
    document.documentElement.innerHTML = `
      <html>
        <head></head>
        <body>
          <div id="app">
            <nav id="navigation" aria-label="Main navigation"></nav>
            <header id="hero">
              <h1>MirDB</h1>
              <a href="#getting-started" class="cta-primary">Get Started</a>
            </header>
            <section id="features" style="height: 500px; margin-top: 100px;">
              <h2>Features</h2>
            </section>
            <section id="usage" style="height: 500px;">
              <h2>Usage Examples</h2>
            </section>
            <section id="architecture" style="height: 500px;">
              <h2>Architecture</h2>
            </section>
            <section id="getting-started" style="height: 500px;">
              <h2>Getting Started</h2>
            </section>
          </div>
        </body>
      </html>
    `;
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe('Navigation rendering and integration with page', () => {
    it('should render navigation and integrate with smooth scroll', async () => {
      const { renderNavigation, initNavigation } = await import('@/components/Navigation');
      const { initSmoothScroll } = await import('@/utils/smooth-scroll');

      const navElement = renderNavigation();
      document.getElementById('navigation')?.replaceWith(navElement);

      initSmoothScroll();
      initNavigation();

      // Verify navigation is in the document
      const nav = document.querySelector('nav[aria-label="Main navigation"]');
      expect(nav).not.toBeNull();

      // Verify smooth scroll is enabled
      expect(document.documentElement.style.scrollBehavior).toBe('smooth');
    });

    it('should have all expected navigation links', async () => {
      const { renderNavigation, initNavigation } = await import('@/components/Navigation');

      const navElement = renderNavigation();
      document.getElementById('navigation')?.replaceWith(navElement);

      initNavigation();

      const expectedLinks = ['#features', '#usage', '#architecture', '#getting-started'];
      expectedLinks.forEach((href) => {
        const link = document.querySelector(`nav a[href="${href}"]`);
        expect(link).not.toBeNull();
      });
    });
  });

  describe('Click navigation behavior', () => {
    it('should scroll to features section when Features link is clicked', async () => {
      const { renderNavigation, initNavigation } = await import('@/components/Navigation');
      const { initSmoothScroll } = await import('@/utils/smooth-scroll');

      // Mock scrollIntoView on features section BEFORE any module initialization
      const featuresSection = document.getElementById('features');
      const mockScrollIntoView = vi.fn();
      if (featuresSection) {
        featuresSection.scrollIntoView = mockScrollIntoView;
      }

      const navElement = renderNavigation();
      document.getElementById('navigation')?.replaceWith(navElement);

      initSmoothScroll();
      initNavigation();

      const featuresLink = navElement.querySelector('a[href="#features"]') as HTMLAnchorElement;
      const clickEvent = new MouseEvent('click', {
        bubbles: true,
        cancelable: true,
      });

      featuresLink.dispatchEvent(clickEvent);

      expect(mockScrollIntoView).toHaveBeenCalledWith({
        behavior: 'smooth',
        block: 'start',
      });
    });

    it('should scroll to getting-started section when Get Started CTA is clicked', async () => {
      const { initSmoothScroll } = await import('@/utils/smooth-scroll');

      // Mock scrollIntoView BEFORE initializing
      const gettingStartedSection = document.getElementById('getting-started');
      const mockScrollIntoView = vi.fn();
      if (gettingStartedSection) {
        gettingStartedSection.scrollIntoView = mockScrollIntoView;
      }

      initSmoothScroll();

      const ctaButton = document.querySelector('a.cta-primary[href="#getting-started"]') as HTMLAnchorElement;
      const clickEvent = new MouseEvent('click', {
        bubbles: true,
        cancelable: true,
      });

      ctaButton.dispatchEvent(clickEvent);

      expect(mockScrollIntoView).toHaveBeenCalledWith({
        behavior: 'smooth',
        block: 'start',
      });
    });
  });

  describe('Active state management', () => {
    it('should have only one active link at a time', async () => {
      const { renderNavigation, initNavigation, setActiveNavItem } = await import('@/components/Navigation');

      const navElement = renderNavigation();
      document.getElementById('navigation')?.replaceWith(navElement);

      initNavigation();

      // Set features as active
      setActiveNavItem('features');

      const activeLinks = navElement.querySelectorAll('a.active, a[aria-current="true"]');
      expect(activeLinks.length).toBe(1);

      // Change active to getting-started
      setActiveNavItem('getting-started');

      const newActiveLinks = navElement.querySelectorAll('a.active, a[aria-current="true"]');
      expect(newActiveLinks.length).toBe(1);
      expect(newActiveLinks[0].getAttribute('href')).toBe('#getting-started');
    });

    it('should update active state based on visible section', async () => {
      const { renderNavigation, initNavigation } = await import('@/components/Navigation');

      const navElement = renderNavigation();
      document.getElementById('navigation')?.replaceWith(navElement);

      initNavigation();

      // Simulate architecture section becoming visible via the observer callback
      const architectureSection = document.getElementById('architecture');
      if (architectureSection && observerCallback) {
        observerCallback(
          [
            {
              target: architectureSection,
              isIntersecting: true,
              intersectionRatio: 0.5,
            } as IntersectionObserverEntry,
          ],
          {} as IntersectionObserver
        );
      }

      const architectureLink = navElement.querySelector('a[href="#architecture"]');
      expect(
        architectureLink?.classList.contains('active') || architectureLink?.getAttribute('aria-current') === 'true'
      ).toBe(true);
    });
  });

  describe('Accessibility', () => {
    it('should have proper aria-label on navigation', async () => {
      const { renderNavigation } = await import('@/components/Navigation');

      const navElement = renderNavigation();

      expect(navElement.getAttribute('aria-label')).toBe('Main navigation');
    });

    it('should use aria-current for active navigation item', async () => {
      const { renderNavigation, initNavigation, setActiveNavItem } = await import('@/components/Navigation');

      const navElement = renderNavigation();
      document.getElementById('navigation')?.replaceWith(navElement);

      initNavigation();
      setActiveNavItem('features');

      const featuresLink = navElement.querySelector('a[href="#features"]');
      expect(featuresLink?.getAttribute('aria-current')).toBe('true');
    });
  });

  describe('URL anchor handling', () => {
    it('should handle page load with hash in URL', async () => {
      const { handleHashNavigation } = await import('@/utils/smooth-scroll');

      const featuresSection = document.getElementById('features');
      const mockScrollIntoView = vi.fn();
      if (featuresSection) {
        featuresSection.scrollIntoView = mockScrollIntoView;
      }

      // Simulate hash in URL
      handleHashNavigation('#features');

      expect(mockScrollIntoView).toHaveBeenCalledWith({
        behavior: 'smooth',
        block: 'start',
      });
    });
  });
});
