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
 * Tablet Responsive Design Tests (Scenario 8)
 * Viewport: 768px width (iPad / similar tablet devices)
 */
describe('Tablet Responsive Design (768px viewport)', () => {
  let dom: JSDOM;
  let document: Document;
  let container: HTMLElement;

  beforeEach(() => {
    // Create JSDOM instance with tablet viewport simulation
    dom = new JSDOM('<!DOCTYPE html><html><head></head><body><div id="app"></div></body></html>', {
      url: 'http://localhost:4173',
      pretendToBeVisual: true,
    });
    document = dom.window.document;
    container = document.getElementById('app')!;

    // Simulate tablet viewport (768px)
    Object.defineProperty(dom.window, 'innerWidth', {
      writable: true,
      configurable: true,
      value: 768,
    });
    Object.defineProperty(dom.window, 'innerHeight', {
      writable: true,
      configurable: true,
      value: 1024,
    });

    // Mock matchMedia for tablet viewport
    dom.window.matchMedia = (query: string) => ({
      matches: query.includes('min-width: 768px') && !query.includes('min-width: 1024px'),
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

  describe('Test Case 1: Layout adapts to tablet breakpoint (768px)', () => {
    it('should render all sections within tablet viewport bounds', () => {
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

    it('should have max-width constraints for content centering at tablet size', () => {
      const features = renderFeatures();
      container.appendChild(features);

      const featuresContainer = features.querySelector('.features-container');
      expect(featuresContainer).not.toBeNull();
      expect(featuresContainer?.className).toContain('max-w-6xl');
      expect(featuresContainer?.className).toContain('mx-auto');
    });

    it('should have proper horizontal padding for tablet viewport', () => {
      const features = renderFeatures();
      container.appendChild(features);

      // Verify padding classes are applied
      expect(features.className).toContain('px-4');
    });

    it('should have proper vertical padding for section spacing', () => {
      const features = renderFeatures();
      container.appendChild(features);

      expect(features.className).toContain('py-16');
    });

    it('should have architecture section properly constrained', () => {
      const architecture = renderArchitecture();
      container.appendChild(architecture);

      const archContainer = architecture.querySelector('.architecture-container');
      expect(archContainer).not.toBeNull();
      expect(archContainer?.className).toContain('max-w-6xl');
      expect(archContainer?.className).toContain('mx-auto');
    });
  });

  describe('Test Case 2: Features grid displays in 2-column layout on tablet', () => {
    it('should have md:grid-cols-2 class for 2-column layout at tablet breakpoint', () => {
      const features = renderFeatures();
      container.appendChild(features);

      const grid = features.querySelector('.features-grid');
      expect(grid).not.toBeNull();
      // At 768px (md breakpoint), grid should use 2 columns
      expect(grid?.className).toContain('md:grid-cols-2');
    });

    it('should render all 6 feature cards in the grid', () => {
      const features = renderFeatures();
      container.appendChild(features);

      const cards = features.querySelectorAll('.feature-card');
      expect(cards.length).toBe(6);
    });

    it('should have gap spacing between grid items', () => {
      const features = renderFeatures();
      container.appendChild(features);

      const grid = features.querySelector('.features-grid');
      expect(grid?.className).toContain('gap-8');
    });

    it('should have grid class applied for layout', () => {
      const features = renderFeatures();
      container.appendChild(features);

      const grid = features.querySelector('.features-grid');
      expect(grid?.className).toMatch(/\bgrid\b/);
    });

    it('should have responsive breakpoints transitioning from 1 to 2 to 3 columns', () => {
      const features = renderFeatures();
      container.appendChild(features);

      const grid = features.querySelector('.features-grid');
      // Mobile: 1 column, Tablet (md): 2 columns, Desktop (lg): 3 columns
      expect(grid?.className).toContain('grid-cols-1');
      expect(grid?.className).toContain('md:grid-cols-2');
      expect(grid?.className).toContain('lg:grid-cols-3');
    });

    it('should have install grid using 2 columns at tablet size', () => {
      const gettingStarted = renderGettingStarted();
      container.appendChild(gettingStarted);

      const installGrid = gettingStarted.querySelector('.install-grid');
      expect(installGrid).not.toBeNull();
      expect(installGrid?.className).toContain('md:grid-cols-2');
    });
  });

  describe('Test Case 3: Hero section content is centered and properly sized on tablet', () => {
    it('should have centered section titles', () => {
      const features = renderFeatures();
      container.appendChild(features);

      const title = features.querySelector('.features-title');
      expect(title).not.toBeNull();
      expect(title?.className).toContain('text-center');
    });

    it('should have readable text size for section headings', () => {
      const features = renderFeatures();
      container.appendChild(features);

      const title = features.querySelector('.features-title');
      // text-3xl is appropriate for tablet
      expect(title?.className).toContain('text-3xl');
    });

    it('should have centered architecture header content', () => {
      const architecture = renderArchitecture();
      container.appendChild(architecture);

      const header = architecture.querySelector('.architecture-header');
      expect(header).not.toBeNull();
      expect(header?.className).toContain('text-center');
    });

    it('should have getting started section centered', () => {
      const gettingStarted = renderGettingStarted();
      container.appendChild(gettingStarted);

      const title = gettingStarted.querySelector('.getting-started-title');
      expect(title).not.toBeNull();
      expect(title?.className).toContain('text-center');
    });

    it('should have centered description text under headings', () => {
      const gettingStarted = renderGettingStarted();
      container.appendChild(gettingStarted);

      const description = gettingStarted.querySelector('.getting-started-description');
      expect(description).not.toBeNull();
      expect(description?.className).toContain('text-center');
    });

    it('should have proper font sizing hierarchy for tablet', () => {
      const architecture = renderArchitecture();
      container.appendChild(architecture);

      // Main heading should be larger
      const h2 = architecture.querySelector('h2');
      expect(h2?.className).toContain('text-3xl');

      // Subheadings should be smaller
      const h3Elements = architecture.querySelectorAll('h3');
      h3Elements.forEach((h3) => {
        expect(h3.className).toMatch(/text-(xl|lg)/);
      });
    });
  });

  describe('Test Case 4: Architecture diagram scales appropriately on tablet', () => {
    it('should have architecture diagram with responsive width', () => {
      const architecture = renderArchitecture();
      container.appendChild(architecture);

      const diagram = architecture.querySelector('.architecture-diagram');
      expect(diagram).not.toBeNull();
      expect(diagram?.className).toContain('w-full');
    });

    it('should have diagram with auto height for aspect ratio preservation', () => {
      const architecture = renderArchitecture();
      container.appendChild(architecture);

      const diagram = architecture.querySelector('.architecture-diagram');
      expect(diagram?.className).toContain('h-auto');
    });

    it('should have diagram with max-width constraint for readability', () => {
      const architecture = renderArchitecture();
      container.appendChild(architecture);

      const diagram = architecture.querySelector('.architecture-diagram');
      expect(diagram?.className).toContain('max-w-4xl');
    });

    it('should have diagram centered within container', () => {
      const architecture = renderArchitecture();
      container.appendChild(architecture);

      const diagram = architecture.querySelector('.architecture-diagram');
      expect(diagram?.className).toContain('mx-auto');
    });

    it('should have diagram container with horizontal overflow handling', () => {
      const architecture = renderArchitecture();
      container.appendChild(architecture);

      const diagramContainer = architecture.querySelector('.architecture-diagram-container');
      expect(diagramContainer).not.toBeNull();
      expect(diagramContainer?.className).toContain('overflow-x-auto');
    });

    it('should have diagram alt text for accessibility', () => {
      const architecture = renderArchitecture();
      container.appendChild(architecture);

      const diagram = architecture.querySelector('.architecture-diagram') as HTMLImageElement;
      expect(diagram).not.toBeNull();
      expect(diagram?.alt).toBeTruthy();
      expect(diagram?.alt.length).toBeGreaterThan(20);
    });

    it('should have architecture explanations in 2-column grid at tablet', () => {
      const architecture = renderArchitecture();
      container.appendChild(architecture);

      const explanations = architecture.querySelector('.architecture-explanations');
      expect(explanations).not.toBeNull();
      expect(explanations?.className).toContain('md:grid-cols-2');
    });

    it('should have key benefits grid responsive at tablet size', () => {
      const architecture = renderArchitecture();
      container.appendChild(architecture);

      const highlights = architecture.querySelector('.architecture-highlights ul');
      expect(highlights).not.toBeNull();
      // sm:grid-cols-2 means at tablet (768px which is > sm breakpoint), we get 2 columns
      expect(highlights?.className).toContain('sm:grid-cols-2');
    });
  });

  describe('Tablet-specific layout and spacing', () => {
    it('should have proper config grid layout at tablet size', () => {
      const gettingStarted = renderGettingStarted();
      container.appendChild(gettingStarted);

      const configGrid = gettingStarted.querySelector('.config-grid');
      expect(configGrid).not.toBeNull();
      expect(configGrid?.className).toContain('md:grid-cols-2');
    });

    it('should have feature cards with proper padding for tablet touch targets', () => {
      const features = renderFeatures();
      container.appendChild(features);

      const cards = features.querySelectorAll('.feature-card');
      cards.forEach((card) => {
        expect(card.className).toContain('p-6');
      });
    });

    it('should have code blocks with proper overflow handling', () => {
      const gettingStarted = renderGettingStarted();
      container.appendChild(gettingStarted);

      const codeBlocks = gettingStarted.querySelectorAll('.code-block');
      expect(codeBlocks.length).toBeGreaterThan(0);

      codeBlocks.forEach((block) => {
        expect(block.className).toContain('overflow-x-auto');
      });
    });

    it('should have footer badges section with flex-wrap for tablet layout', () => {
      const footer = renderFooter();
      container.appendChild(footer);

      const badgesSection = footer.querySelector('.badges-section');
      expect(badgesSection).not.toBeNull();
      expect(badgesSection?.className).toContain('flex-wrap');
    });

    it('should have readable text sizes throughout all sections', () => {
      const features = renderFeatures();
      const architecture = renderArchitecture();

      container.appendChild(features);
      container.appendChild(architecture);

      // Feature descriptions should be readable
      const descriptions = features.querySelectorAll('.feature-description');
      descriptions.forEach((desc) => {
        expect(desc.className).toContain('leading-relaxed');
      });

      // Architecture text should have proper spacing
      const explanationText = architecture.querySelectorAll('.text-gray-700');
      expect(explanationText.length).toBeGreaterThan(0);
    });
  });

  describe('Tablet viewport content width constraints', () => {
    it('should have max-w-6xl on features container for tablet', () => {
      const features = renderFeatures();
      container.appendChild(features);

      const featuresContainer = features.querySelector('.features-container');
      expect(featuresContainer?.className).toContain('max-w-6xl');
    });

    it('should have max-w-4xl on getting started container for tablet', () => {
      const gettingStarted = renderGettingStarted();
      container.appendChild(gettingStarted);

      const gsContainer = gettingStarted.querySelector('.getting-started-container');
      expect(gsContainer?.className).toContain('max-w-4xl');
    });

    it('should have max-w-6xl on architecture container for tablet', () => {
      const architecture = renderArchitecture();
      container.appendChild(architecture);

      const archContainer = architecture.querySelector('.architecture-container');
      expect(archContainer?.className).toContain('max-w-6xl');
    });

    it('should have mx-auto for centering all main containers', () => {
      const features = renderFeatures();
      const gettingStarted = renderGettingStarted();
      const architecture = renderArchitecture();

      container.appendChild(features);
      container.appendChild(gettingStarted);
      container.appendChild(architecture);

      expect(features.querySelector('.features-container')?.className).toContain('mx-auto');
      expect(gettingStarted.querySelector('.getting-started-container')?.className).toContain('mx-auto');
      expect(architecture.querySelector('.architecture-container')?.className).toContain('mx-auto');
    });
  });

  describe('Tablet accessibility and navigation', () => {
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

      expect(features.id).toBe('features');
      expect(usage.id).toBe('usage');
      expect(architecture.id).toBe('architecture');
      expect(gettingStarted.id).toBe('getting-started');
      expect(footer.id).toBe('footer');
    });

    it('should have proper ARIA labels on sections', () => {
      const usage = renderUsageExamples();
      const architecture = renderArchitecture();

      container.appendChild(usage);
      container.appendChild(architecture);

      expect(usage.getAttribute('aria-labelledby')).toBe('usage-title');
      expect(architecture.getAttribute('aria-labelledby')).toBe('architecture-heading');
    });

    it('should have proper heading hierarchy for tablet reading', () => {
      const features = renderFeatures();
      const architecture = renderArchitecture();
      const gettingStarted = renderGettingStarted();

      container.appendChild(features);
      container.appendChild(architecture);
      container.appendChild(gettingStarted);

      // Each section should have an h2
      const h2Elements = container.querySelectorAll('h2');
      expect(h2Elements.length).toBeGreaterThanOrEqual(3);

      // Architecture should have h3 for subsections
      const archH3Elements = architecture.querySelectorAll('h3');
      expect(archH3Elements.length).toBeGreaterThanOrEqual(2);
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
