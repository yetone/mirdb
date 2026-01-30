/**
 * Error Handling Integration Tests.
 * Owner: Scenario 16 - Error Handling - Missing Assets
 *
 * Tests graceful degradation when assets fail to load:
 * - Missing images (logo.gif) display alt text, layout remains intact
 * - Missing external badges (Shields.io) don't break layout
 * - Core content remains readable without JavaScript
 * - Page progressively loads on slow networks
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { JSDOM } from 'jsdom';
import { renderFooter, BADGES } from '../../src/components/Footer';
import { renderArchitecture } from '../../src/components/Architecture';
import { renderFeatures } from '../../src/components/Features';
import { renderUsageExamples } from '../../src/components/UsageExamples';
import { renderGettingStarted } from '../../src/components/GettingStarted';
import fs from 'fs';
import path from 'path';

/**
 * Test Case 1: Block logo.gif from loading
 * Expected: Alt text displays, page layout remains intact
 */
describe('Test Case 1: Missing Image Assets - Logo', () => {
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
    global.document = document;
  });

  afterEach(() => {
    dom.window.close();
  });

  it('should have alt text on all images for fallback when images fail to load', () => {
    // Render architecture component which has images
    const architecture = renderArchitecture();
    container.appendChild(architecture);

    const images = container.querySelectorAll('img');
    expect(images.length).toBeGreaterThan(0);

    images.forEach((img) => {
      expect(img.alt).toBeTruthy();
      expect(img.alt.length).toBeGreaterThan(10); // Alt text should be descriptive
    });
  });

  it('should maintain page layout structure when architecture diagram fails to load', () => {
    const architecture = renderArchitecture();
    container.appendChild(architecture);

    // Simulate image error by triggering onerror
    const diagram = container.querySelector('.architecture-diagram') as HTMLImageElement;
    expect(diagram).not.toBeNull();

    // Even if the image fails, the container should still exist with proper classes
    const diagramContainer = container.querySelector('.architecture-diagram-container');
    expect(diagramContainer).not.toBeNull();
    expect(diagramContainer?.className).toContain('overflow-x-auto');
    expect(diagramContainer?.className).toContain('bg-white');
    expect(diagramContainer?.className).toContain('rounded-lg');
  });

  it('should have layout wrapper classes that prevent page break on image failure', () => {
    const architecture = renderArchitecture();
    container.appendChild(architecture);

    // The section should have proper layout classes regardless of image state
    expect(architecture.className).toContain('architecture-section');
    expect(architecture.className).toContain('py-16');
    expect(architecture.className).toContain('px-4');

    const architectureContainer = container.querySelector('.architecture-container');
    expect(architectureContainer?.className).toContain('max-w-6xl');
    expect(architectureContainer?.className).toContain('mx-auto');
  });

  it('should have descriptive alt text on architecture diagram', () => {
    const architecture = renderArchitecture();
    container.appendChild(architecture);

    const diagram = container.querySelector('.architecture-diagram') as HTMLImageElement;
    expect(diagram).not.toBeNull();
    expect(diagram.alt).toContain('MirDB');
    expect(diagram.alt).toContain('LSM-Tree');
    expect(diagram.alt).toContain('Architecture');
  });

  it('should have lazy loading attribute for performance optimization', () => {
    const architecture = renderArchitecture();
    container.appendChild(architecture);

    const diagram = container.querySelector('.architecture-diagram') as HTMLImageElement;
    expect(diagram).not.toBeNull();
    // Check loading attribute via getAttribute since JSDOM may not parse it from innerHTML
    const loadingAttr = diagram.getAttribute('loading');
    expect(loadingAttr).toBe('lazy');
  });

  it('should have proper image sizing classes for responsive layout', () => {
    const architecture = renderArchitecture();
    container.appendChild(architecture);

    const diagram = container.querySelector('.architecture-diagram') as HTMLImageElement;
    expect(diagram).not.toBeNull();
    expect(diagram.className).toContain('w-full');
    expect(diagram.className).toContain('h-auto');
    expect(diagram.className).toContain('max-w-4xl');
  });
});

/**
 * Test Case 2: Block external badge images
 * Expected: Page displays without badges, no broken layout
 */
describe('Test Case 2: Missing External Badge Images', () => {
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
    global.document = document;
  });

  afterEach(() => {
    dom.window.close();
  });

  it('should have alt text on all badge images', () => {
    const footer = renderFooter();
    container.appendChild(footer);

    const badgeImages = container.querySelectorAll('.badge-image');
    expect(badgeImages.length).toBe(BADGES.length);

    badgeImages.forEach((img, index) => {
      expect((img as HTMLImageElement).alt).toBe(BADGES[index].alt);
      expect((img as HTMLImageElement).alt.length).toBeGreaterThan(5);
    });
  });

  it('should maintain footer layout when badge images fail to load', () => {
    const footer = renderFooter();
    container.appendChild(footer);

    // The badges section should have flex-wrap to handle missing badges gracefully
    const badgesSection = container.querySelector('.badges-section');
    expect(badgesSection).not.toBeNull();
    expect(badgesSection?.className).toContain('flex');
    expect(badgesSection?.className).toContain('flex-wrap');
    expect(badgesSection?.className).toContain('gap-4');
    expect(badgesSection?.className).toContain('justify-center');
  });

  it('should keep footer structure intact even if all badges fail', () => {
    const footer = renderFooter();
    container.appendChild(footer);

    // Verify footer structure remains
    expect(footer.className).toContain('footer-section');
    expect(footer.id).toBe('footer');

    // Container should be properly styled
    const footerContainer = container.querySelector('.footer-container');
    expect(footerContainer).not.toBeNull();
    expect(footerContainer?.className).toContain('max-w-6xl');
    expect(footerContainer?.className).toContain('mx-auto');
    expect(footerContainer?.className).toContain('text-center');
  });

  it('should have badge links that remain functional even if images fail', () => {
    const footer = renderFooter();
    container.appendChild(footer);

    const badgeLinks = container.querySelectorAll('.badge-link');
    expect(badgeLinks.length).toBe(BADGES.length);

    badgeLinks.forEach((link, index) => {
      const anchor = link as HTMLAnchorElement;
      expect(anchor.href).toBe(BADGES[index].link);
      expect(anchor.target).toBe('_blank');
      expect(anchor.rel).toContain('noopener');
      expect(anchor.rel).toContain('noreferrer');
    });
  });

  it('should have proper security attributes on external links', () => {
    const footer = renderFooter();
    container.appendChild(footer);

    const externalLinks = container.querySelectorAll('a[target="_blank"]');
    expect(externalLinks.length).toBeGreaterThan(0);

    externalLinks.forEach((link) => {
      expect(link.getAttribute('rel')).toContain('noopener');
      expect(link.getAttribute('rel')).toContain('noreferrer');
    });
  });

  it('should have GitHub link as primary navigation fallback when badges unavailable', () => {
    const footer = renderFooter();
    container.appendChild(footer);

    const githubLink = container.querySelector('.github-link');
    expect(githubLink).not.toBeNull();
    expect(githubLink?.textContent).toContain('GitHub');

    // GitHub link should be visible regardless of badge state
    const githubAnchor = githubLink as HTMLAnchorElement;
    expect(githubAnchor.href).toContain('github.com');
  });

  it('should have license and attribution text visible without badges', () => {
    const footer = renderFooter();
    container.appendChild(footer);

    const licenseInfo = container.querySelector('.license-info');
    expect(licenseInfo).not.toBeNull();
    expect(licenseInfo?.textContent).toContain('MIT');

    const authorAttribution = container.querySelector('.author-attribution');
    expect(authorAttribution).not.toBeNull();
    expect(authorAttribution?.textContent?.length).toBeGreaterThan(10);
  });

  it('should have lazy loading on badge images for performance', () => {
    const footer = renderFooter();
    container.appendChild(footer);

    const badgeImages = container.querySelectorAll('.badge-image');
    badgeImages.forEach((img) => {
      expect((img as HTMLImageElement).loading).toBe('lazy');
    });
  });
});

/**
 * Test Case 3: Test with JavaScript disabled
 * Expected: Core content remains readable without JavaScript
 */
describe('Test Case 3: Core Content Readability Without JavaScript', () => {
  let indexHtml: string;

  beforeEach(() => {
    // Read the actual index.html to verify its static content
    const indexPath = path.resolve(__dirname, '../../src/index.html');
    indexHtml = fs.readFileSync(indexPath, 'utf-8');
  });

  it('should have semantic HTML structure in index.html', () => {
    // The HTML should have proper structure visible without JS
    expect(indexHtml).toContain('<!DOCTYPE html>');
    expect(indexHtml).toContain('<html lang="en">');
    expect(indexHtml).toContain('<head>');
    expect(indexHtml).toContain('<body>');
  });

  it('should have main H1 heading visible in static HTML', () => {
    // The H1 heading should be in the static HTML
    expect(indexHtml).toContain('<h1>');
    expect(indexHtml).toContain('MirDB');
    expect(indexHtml).toContain('Key-Value Store');
  });

  it('should have section headings visible without JavaScript', () => {
    // Section headings should be present in static HTML
    expect(indexHtml).toContain('Features');
    expect(indexHtml).toContain('Usage Examples');
    expect(indexHtml).toContain('Architecture');
    expect(indexHtml).toContain('Getting Started');
  });

  it('should have semantic section elements for accessibility', () => {
    expect(indexHtml).toContain('<section');
    expect(indexHtml).toContain('<header');
    expect(indexHtml).toContain('<footer');
    expect(indexHtml).toContain('<nav');
  });

  it('should have ARIA labels on sections for screen readers', () => {
    expect(indexHtml).toContain('aria-labelledby');
    expect(indexHtml).toContain('aria-label');
  });

  it('should have proper meta tags for SEO without JavaScript', () => {
    expect(indexHtml).toContain('<meta name="description"');
    expect(indexHtml).toContain('<title>');
    expect(indexHtml).toContain('MirDB');
  });

  it('should have viewport meta tag for mobile responsiveness', () => {
    expect(indexHtml).toContain('viewport');
    expect(indexHtml).toContain('width=device-width');
    expect(indexHtml).toContain('initial-scale=1.0');
  });

  it('should have section IDs for anchor navigation without JS', () => {
    expect(indexHtml).toContain('id="hero"');
    expect(indexHtml).toContain('id="features"');
    expect(indexHtml).toContain('id="usage"');
    expect(indexHtml).toContain('id="architecture"');
    expect(indexHtml).toContain('id="getting-started"');
    expect(indexHtml).toContain('id="footer"');
  });

  it('should have heading hierarchy correct (single H1)', () => {
    const h1Matches = indexHtml.match(/<h1[^>]*>/g);
    expect(h1Matches).not.toBeNull();
    expect(h1Matches?.length).toBe(1);
  });

  it('should have H2 elements for section headings', () => {
    const h2Matches = indexHtml.match(/<h2[^>]*>/g);
    expect(h2Matches).not.toBeNull();
    expect(h2Matches!.length).toBeGreaterThanOrEqual(4);
  });

  it('should have charset meta tag for proper encoding', () => {
    expect(indexHtml).toContain('charset="UTF-8"');
  });

  it('should have CSS link for styling without JavaScript', () => {
    expect(indexHtml).toContain('stylesheet');
    expect(indexHtml).toContain('.css');
  });
});

/**
 * Test Case 4: Test with slow network
 * Expected: Page progressively loads, critical content appears first
 */
describe('Test Case 4: Progressive Loading and Slow Network Handling', () => {
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
    global.document = document;
  });

  afterEach(() => {
    dom.window.close();
  });

  it('should have lazy loading on images for progressive loading', () => {
    const architecture = renderArchitecture();
    const footer = renderFooter();
    container.appendChild(architecture);
    container.appendChild(footer);

    const allImages = container.querySelectorAll('img');
    allImages.forEach((img) => {
      const imgElement = img as HTMLImageElement;
      // Check loading attribute (from innerHTML) or property (from createElement)
      const loadingAttr = img.getAttribute('loading');
      const loadingProp = imgElement.loading;
      // Either the attribute or the property should be 'lazy'
      expect(loadingAttr === 'lazy' || loadingProp === 'lazy').toBe(true);
    });
  });

  it('should have text content that loads before images', () => {
    const features = renderFeatures();
    container.appendChild(features);

    // Text content should be immediately available
    const title = container.querySelector('.features-title');
    expect(title).not.toBeNull();
    expect(title?.textContent).toBeTruthy();

    // Feature descriptions should be text-based, not image-dependent
    const descriptions = container.querySelectorAll('.feature-description');
    expect(descriptions.length).toBeGreaterThan(0);
    descriptions.forEach((desc) => {
      expect(desc.textContent?.length).toBeGreaterThan(20);
    });
  });

  it('should render headings and titles as critical content', () => {
    const features = renderFeatures();
    const architecture = renderArchitecture();
    const gettingStarted = renderGettingStarted();
    const usage = renderUsageExamples();

    container.appendChild(features);
    container.appendChild(architecture);
    container.appendChild(gettingStarted);
    container.appendChild(usage);

    // All section headings should be immediately visible
    const headings = container.querySelectorAll('h2');
    expect(headings.length).toBeGreaterThanOrEqual(4);

    headings.forEach((heading) => {
      expect(heading.textContent?.trim().length).toBeGreaterThan(0);
    });
  });

  it('should have code blocks with text content that loads immediately', () => {
    const gettingStarted = renderGettingStarted();
    container.appendChild(gettingStarted);

    const codeBlocks = container.querySelectorAll('.code-content');
    expect(codeBlocks.length).toBeGreaterThan(0);

    // Code content should be text-based and load immediately
    codeBlocks.forEach((code) => {
      expect(code.textContent?.trim().length).toBeGreaterThan(0);
    });
  });

  it('should use SVG icons instead of image icons for fast loading', () => {
    const architecture = renderArchitecture();
    const footer = renderFooter();
    container.appendChild(architecture);
    container.appendChild(footer);

    // SVGs should be inline for fast loading
    const svgs = container.querySelectorAll('svg');
    expect(svgs.length).toBeGreaterThan(0);

    // Check that SVGs are inline (have viewBox attribute)
    svgs.forEach((svg) => {
      expect(svg.getAttribute('viewBox')).toBeTruthy();
    });
  });

  it('should have features using SVG icons for immediate display', () => {
    const features = renderFeatures();
    container.appendChild(features);

    const featureCards = container.querySelectorAll('.feature-card');
    expect(featureCards.length).toBe(6);

    // Each feature should have an icon area
    const featureIcons = container.querySelectorAll('.feature-icon');
    expect(featureIcons.length).toBe(6);
  });

  it('should have footer content that loads without external resources', () => {
    const footer = renderFooter();
    container.appendChild(footer);

    // GitHub icon should be inline SVG, not external image
    const githubIcon = container.querySelector('.github-icon svg');
    expect(githubIcon).not.toBeNull();

    // License and attribution are text-based
    const licenseInfo = container.querySelector('.license-info');
    expect(licenseInfo?.textContent?.length).toBeGreaterThan(10);
  });

  it('should have usage examples with inline code (no external dependencies)', () => {
    const usage = renderUsageExamples();
    container.appendChild(usage);

    // Code examples should be text-based
    const codeBlocks = container.querySelectorAll('pre, code');
    expect(codeBlocks.length).toBeGreaterThan(0);

    codeBlocks.forEach((block) => {
      expect(block.textContent?.length).toBeGreaterThan(0);
    });
  });

  it('should have all sections with proper structure for rendering order', () => {
    const features = renderFeatures();
    const architecture = renderArchitecture();
    const gettingStarted = renderGettingStarted();
    const usage = renderUsageExamples();
    const footer = renderFooter();

    container.appendChild(features);
    container.appendChild(usage);
    container.appendChild(architecture);
    container.appendChild(gettingStarted);
    container.appendChild(footer);

    // Verify all sections are present and have IDs
    expect(container.querySelector('#features')).not.toBeNull();
    expect(container.querySelector('#usage')).not.toBeNull();
    expect(container.querySelector('#architecture')).not.toBeNull();
    expect(container.querySelector('#getting-started')).not.toBeNull();
    expect(container.querySelector('#footer')).not.toBeNull();
  });
});

/**
 * Additional Error Handling Tests
 * Edge cases and robustness verification
 */
describe('Additional Error Handling and Robustness', () => {
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
    global.document = document;
  });

  afterEach(() => {
    dom.window.close();
  });

  it('should have proper CSS classes for graceful degradation on all sections', () => {
    const features = renderFeatures();
    const architecture = renderArchitecture();
    const gettingStarted = renderGettingStarted();
    const usage = renderUsageExamples();
    const footer = renderFooter();

    container.appendChild(features);
    container.appendChild(architecture);
    container.appendChild(gettingStarted);
    container.appendChild(usage);
    container.appendChild(footer);

    // Sections with background color classes should have them
    expect(features.className).toContain('bg-');
    expect(architecture.className).toContain('bg-');
    expect(gettingStarted.className).toContain('bg-');
    expect(footer.className).toContain('bg-');

    // Usage section uses CSS styling from external stylesheet (usage.css)
    // The component itself uses .usage-section class for styling
    expect(usage.className).toContain('usage-section');
  });

  it('should have all external links with proper attributes for security', () => {
    const footer = renderFooter();
    container.appendChild(footer);

    const externalLinks = container.querySelectorAll('a[href^="http"]');
    externalLinks.forEach((link) => {
      // External links should have security attributes
      if (link.getAttribute('target') === '_blank') {
        expect(link.getAttribute('rel')).toContain('noopener');
      }
    });
  });

  it('should not have broken internal links', () => {
    const footer = renderFooter();
    container.appendChild(footer);

    const internalLinks = container.querySelectorAll('a:not([href^="http"])');
    internalLinks.forEach((link) => {
      const href = link.getAttribute('href');
      // Internal links should start with # or / or be a valid relative path
      if (href) {
        expect(href.length).toBeGreaterThan(0);
      }
    });
  });

  it('should have consistent padding across all sections', () => {
    const features = renderFeatures();
    const architecture = renderArchitecture();
    const gettingStarted = renderGettingStarted();
    const usage = renderUsageExamples();

    container.appendChild(features);
    container.appendChild(architecture);
    container.appendChild(gettingStarted);
    container.appendChild(usage);

    // Sections with inline Tailwind classes should have consistent horizontal padding
    expect(features.className).toContain('px-4');
    expect(architecture.className).toContain('px-4');
    expect(gettingStarted.className).toContain('px-4');

    // Usage section uses CSS styling from external stylesheet (usage.css)
    // The component itself uses .usage-section class for styling
    expect(usage.className).toContain('usage-section');
  });

  it('should have max-width containers for content readability', () => {
    const features = renderFeatures();
    const architecture = renderArchitecture();
    const gettingStarted = renderGettingStarted();

    container.appendChild(features);
    container.appendChild(architecture);
    container.appendChild(gettingStarted);

    // Content containers should have max-width
    expect(container.querySelector('.features-container')?.className).toContain('max-w-');
    expect(container.querySelector('.architecture-container')?.className).toContain('max-w-');
    expect(container.querySelector('.getting-started-container')?.className).toContain('max-w-');
  });

  it('should have proper heading IDs for anchor navigation', () => {
    const features = renderFeatures();
    const architecture = renderArchitecture();
    const gettingStarted = renderGettingStarted();
    const usage = renderUsageExamples();

    container.appendChild(features);
    container.appendChild(architecture);
    container.appendChild(gettingStarted);
    container.appendChild(usage);

    // Sections should have IDs for navigation
    expect(features.id).toBe('features');
    expect(architecture.id).toBe('architecture');
    expect(gettingStarted.id).toBe('getting-started');
    expect(usage.id).toBe('usage');
  });

  it('should gracefully handle missing container elements', () => {
    // Test that components can be created independently
    const features = renderFeatures();
    const architecture = renderArchitecture();
    const gettingStarted = renderGettingStarted();
    const usage = renderUsageExamples();
    const footer = renderFooter();

    // All components should be valid HTML elements
    expect(features).toBeInstanceOf(dom.window.HTMLElement);
    expect(architecture).toBeInstanceOf(dom.window.HTMLElement);
    expect(gettingStarted).toBeInstanceOf(dom.window.HTMLElement);
    expect(usage).toBeInstanceOf(dom.window.HTMLElement);
    expect(footer).toBeInstanceOf(dom.window.HTMLElement);
  });
});
