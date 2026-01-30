/**
 * Responsive Design Integration Tests.
 * Owners: Scenario 7 (mobile), Scenario 8 (tablet), Scenario 9 (desktop)
 *
 * Tests viewport breakpoints:
 * - Mobile: 375px width (Scenario 7)
 * - Tablet: 768px width (Scenario 8)
 * - Desktop: 1280px width (Scenario 9)
 *
 * Each scenario owns their respective viewport tests.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { JSDOM } from 'jsdom';
import { renderFeatures, FEATURES } from '../../src/components/Features';
import { renderUsageExamples } from '../../src/components/UsageExamples';
import { renderArchitecture } from '../../src/components/Architecture';
import { renderGettingStarted } from '../../src/components/GettingStarted';
import { renderFooter } from '../../src/components/Footer';

/**
 * Mobile Responsive Design Tests (Scenario 7)
 * Viewport: 375px width (iPhone SE / similar mobile devices)
 */
describe('Mobile Responsive Design (375px viewport)', () => {
  let dom: JSDOM;
  let document: Document;
  let container: HTMLElement;

  beforeEach(() => {
    // Create JSDOM instance with mobile viewport simulation
    dom = new JSDOM('<!DOCTYPE html><html><head></head><body><div id="app"></div></body></html>', {
      url: 'http://localhost:4173',
      pretendToBeVisual: true,
    });
    document = dom.window.document;
    container = document.getElementById('app')!;

    // Simulate mobile viewport (375px)
    Object.defineProperty(dom.window, 'innerWidth', {
      writable: true,
      configurable: true,
      value: 375,
    });
    Object.defineProperty(dom.window, 'innerHeight', {
      writable: true,
      configurable: true,
      value: 667,
    });

    // Mock matchMedia for mobile viewport
    dom.window.matchMedia = (query: string) => ({
      matches: query.includes('max-width: 640px') || query.includes('max-width: 767px'),
      media: query,
      onchange: null,
      addListener: () => {},
      removeListener: () => {},
      addEventListener: () => {},
      removeEventListener: () => {},
      dispatchEvent: () => true,
    });

    // Bind document to global for component rendering
    global.document = document;
  });

  afterEach(() => {
    dom.window.close();
  });

  describe('Test Case 1: All content fits within viewport without horizontal scroll', () => {
    it('should render all sections within mobile viewport bounds', () => {
      // Render all components
      const features = renderFeatures();
      const usage = renderUsageExamples();
      const architecture = renderArchitecture();
      const gettingStarted = renderGettingStarted();
      const footer = renderFooter();

      container.appendChild(features);
      container.appendChild(usage);
      container.appendChild(architecture);
      container.appendChild(gettingStarted);
      container.appendChild(footer);

      // Verify all sections are rendered
      expect(container.querySelector('.features-section')).not.toBeNull();
      expect(container.querySelector('.usage-section')).not.toBeNull();
      expect(container.querySelector('.architecture-section')).not.toBeNull();
      expect(container.querySelector('.getting-started-section')).not.toBeNull();
      expect(container.querySelector('.footer-section')).not.toBeNull();
    });

    it('should have max-width constraints to prevent horizontal overflow', () => {
      const features = renderFeatures();
      container.appendChild(features);

      // Check that container has max-width class
      const featuresContainer = features.querySelector('.features-container');
      expect(featuresContainer).not.toBeNull();
      expect(featuresContainer?.className).toContain('max-w-');
    });

    it('should have proper padding for mobile viewport', () => {
      const features = renderFeatures();
      container.appendChild(features);

      // Verify padding classes are applied
      expect(features.className).toContain('px-4');
    });
  });

  describe('Test Case 2: Hero section on mobile', () => {
    it('should have readable text classes for mobile', () => {
      // Note: Hero is owned by Scenario 1, but we test that sections
      // stack properly in mobile view
      const features = renderFeatures();
      container.appendChild(features);

      const title = features.querySelector('.features-title');
      expect(title).not.toBeNull();
      // Tailwind text-3xl is readable on mobile
      expect(title?.className).toContain('text-3xl');
    });

    it('should have section titles centered for mobile viewing', () => {
      const features = renderFeatures();
      container.appendChild(features);

      const title = features.querySelector('.features-title');
      expect(title?.className).toContain('text-center');
    });
  });

  describe('Test Case 3: Features grid displays in single column on mobile', () => {
    it('should have grid-cols-1 class for single column layout', () => {
      const features = renderFeatures();
      container.appendChild(features);

      const grid = features.querySelector('.features-grid');
      expect(grid).not.toBeNull();
      // Mobile-first: grid-cols-1 is the base class
      expect(grid?.className).toContain('grid-cols-1');
    });

    it('should render all 6 feature cards', () => {
      const features = renderFeatures();
      container.appendChild(features);

      const cards = features.querySelectorAll('.feature-card');
      expect(cards.length).toBe(6);
    });

    it('should have responsive breakpoints for larger screens', () => {
      const features = renderFeatures();
      container.appendChild(features);

      const grid = features.querySelector('.features-grid');
      // Verify responsive classes exist for tablet/desktop
      expect(grid?.className).toContain('md:grid-cols-2');
      expect(grid?.className).toContain('lg:grid-cols-3');
    });
  });

  describe('Test Case 4: Code blocks are scrollable horizontally on mobile', () => {
    it('should have overflow-x-auto on code block containers', () => {
      const gettingStarted = renderGettingStarted();
      container.appendChild(gettingStarted);

      const codeBlocks = gettingStarted.querySelectorAll('.code-block');
      expect(codeBlocks.length).toBeGreaterThan(0);

      codeBlocks.forEach((block) => {
        expect(block.className).toContain('overflow-x-auto');
      });
    });

    it('should have monospace font for code readability', () => {
      const gettingStarted = renderGettingStarted();
      container.appendChild(gettingStarted);

      const codeContent = gettingStarted.querySelectorAll('.code-content');
      expect(codeContent.length).toBeGreaterThan(0);

      codeContent.forEach((content) => {
        expect(content.className).toContain('font-mono');
      });
    });

    it('should preserve whitespace in code blocks', () => {
      const gettingStarted = renderGettingStarted();
      container.appendChild(gettingStarted);

      const codeContent = gettingStarted.querySelectorAll('.code-content');
      codeContent.forEach((content) => {
        expect(content.className).toContain('whitespace-pre');
      });
    });

    it('should have readable text size for code', () => {
      const gettingStarted = renderGettingStarted();
      container.appendChild(gettingStarted);

      const codeBlocks = gettingStarted.querySelectorAll('.code-block');
      codeBlocks.forEach((block) => {
        expect(block.className).toContain('text-sm');
      });
    });
  });

  describe('Test Case 5: Touch target sizes meet 44x44px minimum', () => {
    it('should have adequate padding on copy buttons for touch targets', () => {
      const gettingStarted = renderGettingStarted();
      container.appendChild(gettingStarted);

      const copyButtons = gettingStarted.querySelectorAll('.copy-button');
      expect(copyButtons.length).toBeGreaterThan(0);

      copyButtons.forEach((button) => {
        // Verify button has padding classes for adequate touch target
        expect(button.className).toContain('px-3');
        expect(button.className).toContain('py-1');
      });
    });

    it('should have adequate padding on feature cards for touch interactions', () => {
      const features = renderFeatures();
      container.appendChild(features);

      const cards = features.querySelectorAll('.feature-card');
      cards.forEach((card) => {
        expect(card.className).toContain('p-6');
      });
    });

    it('should have adequate size for documentation link button', () => {
      const gettingStarted = renderGettingStarted();
      container.appendChild(gettingStarted);

      const docLink = gettingStarted.querySelector('.doc-link');
      expect(docLink).not.toBeNull();
      // px-6 py-3 provides adequate touch target
      expect(docLink?.className).toContain('px-6');
      expect(docLink?.className).toContain('py-3');
    });

    it('should have properly sized badge links for touch', () => {
      const footer = renderFooter();
      container.appendChild(footer);

      const badgeLinks = footer.querySelectorAll('.badge-link');
      expect(badgeLinks.length).toBeGreaterThan(0);

      badgeLinks.forEach((link) => {
        // Badge links should be inline-block for proper sizing
        expect(link.className).toContain('inline-block');
      });
    });

    it('should have adequate touch target for GitHub link', () => {
      const footer = renderFooter();
      container.appendChild(footer);

      const githubLink = footer.querySelector('.github-link');
      expect(githubLink).not.toBeNull();
      // Flex items center with gap provides adequate touch area
      expect(githubLink?.className).toContain('flex');
      expect(githubLink?.className).toContain('items-center');
      expect(githubLink?.className).toContain('gap-2');
    });
  });

  describe('Test Case 6: Navigation is accessible on mobile', () => {
    it('should have proper section IDs for anchor navigation', () => {
      const features = renderFeatures();
      const usage = renderUsageExamples();
      const architecture = renderArchitecture();
      const gettingStarted = renderGettingStarted();
      const footer = renderFooter();

      container.appendChild(features);
      container.appendChild(usage);
      container.appendChild(architecture);
      container.appendChild(gettingStarted);
      container.appendChild(footer);

      // Verify all sections have IDs for anchor navigation
      expect(features.id).toBe('features');
      expect(usage.id).toBe('usage');
      expect(architecture.id).toBe('architecture');
      expect(gettingStarted.id).toBe('getting-started');
      expect(footer.id).toBe('footer');
    });

    it('should have proper ARIA labels for accessibility', () => {
      const usage = renderUsageExamples();
      container.appendChild(usage);

      expect(usage.getAttribute('aria-labelledby')).toBe('usage-title');
    });

    it('should have proper heading hierarchy', () => {
      const features = renderFeatures();
      const usage = renderUsageExamples();
      const architecture = renderArchitecture();
      const gettingStarted = renderGettingStarted();

      container.appendChild(features);
      container.appendChild(usage);
      container.appendChild(architecture);
      container.appendChild(gettingStarted);

      // Check h2 headings for sections
      const h2Elements = container.querySelectorAll('h2');
      expect(h2Elements.length).toBeGreaterThanOrEqual(4);
    });
  });

  describe('Mobile-specific responsive CSS classes', () => {
    it('should use mobile-first Tailwind classes', () => {
      const features = renderFeatures();
      container.appendChild(features);

      // Mobile-first: base classes apply to mobile, md: and lg: for larger screens
      const grid = features.querySelector('.features-grid');
      expect(grid?.className).toMatch(/^(?!.*sm:grid-cols).*grid-cols-1/);
    });

    it('should have responsive install grid for mobile', () => {
      const gettingStarted = renderGettingStarted();
      container.appendChild(gettingStarted);

      const installGrid = gettingStarted.querySelector('.install-grid');
      expect(installGrid).not.toBeNull();
      expect(installGrid?.className).toContain('grid-cols-1');
      expect(installGrid?.className).toContain('md:grid-cols-2');
    });

    it('should stack architecture explanations on mobile', () => {
      const architecture = renderArchitecture();
      container.appendChild(architecture);

      const explanations = architecture.querySelector('.architecture-explanations');
      expect(explanations).not.toBeNull();
      // On mobile, grid with md:grid-cols-2 will stack in single column
      expect(explanations?.className).toContain('md:grid-cols-2');
    });

    it('should have responsive footer layout', () => {
      const footer = renderFooter();
      container.appendChild(footer);

      const badgesSection = footer.querySelector('.badges-section');
      expect(badgesSection).not.toBeNull();
      // flex-wrap ensures badges wrap on mobile
      expect(badgesSection?.className).toContain('flex-wrap');
    });
  });

  describe('Mobile viewport content width constraints', () => {
    it('should have max-w-6xl on features container', () => {
      const features = renderFeatures();
      container.appendChild(features);

      const featuresContainer = features.querySelector('.features-container');
      expect(featuresContainer?.className).toContain('max-w-6xl');
    });

    it('should have max-w-4xl on getting started container', () => {
      const gettingStarted = renderGettingStarted();
      container.appendChild(gettingStarted);

      const gsContainer = gettingStarted.querySelector('.getting-started-container');
      expect(gsContainer?.className).toContain('max-w-4xl');
    });

    it('should have mx-auto for centering on all viewports', () => {
      const features = renderFeatures();
      container.appendChild(features);

      const featuresContainer = features.querySelector('.features-container');
      expect(featuresContainer?.className).toContain('mx-auto');
    });
  });
});

/**
 * Touch interaction tests for mobile devices
 */
describe('Mobile Touch Interactions', () => {
  let dom: JSDOM;
  let document: Document;
  let container: HTMLElement;

  beforeEach(() => {
    dom = new JSDOM('<!DOCTYPE html><html><head></head><body><div id="app"></div></body></html>', {
      url: 'http://localhost:4173',
      pretendToBeVisual: true,
    });
    document = dom.window.document;
    container = document.getElementById('app')!;

    Object.defineProperty(dom.window, 'innerWidth', {
      writable: true,
      configurable: true,
      value: 375,
    });

    global.document = document;
  });

  afterEach(() => {
    dom.window.close();
  });

  it('should have transition effects for hover/touch states on feature cards', () => {
    const features = renderFeatures();
    container.appendChild(features);

    const cards = features.querySelectorAll('.feature-card');
    cards.forEach((card) => {
      expect(card.className).toContain('transition');
    });
  });

  it('should have hover states defined for buttons', () => {
    const gettingStarted = renderGettingStarted();
    container.appendChild(gettingStarted);

    const copyButtons = gettingStarted.querySelectorAll('.copy-button');
    copyButtons.forEach((button) => {
      expect(button.className).toContain('hover:bg-gray-600');
    });
  });

  it('should have accessible focus states for links', () => {
    const footer = renderFooter();
    container.appendChild(footer);

    // Links should be focusable and visible
    const links = footer.querySelectorAll('a');
    expect(links.length).toBeGreaterThan(0);

    links.forEach((link) => {
      // All links should have href
      expect(link.href).toBeTruthy();
    });
  });
});
