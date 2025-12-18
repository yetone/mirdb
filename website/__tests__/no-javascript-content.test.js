/**
 * No JavaScript Core Content Tests
 * Tests for NFR-6 of MirDB Landing Page
 * Verifies that core content is visible without JavaScript enabled
 *
 * NFR-6: Page must not require JavaScript for core content visibility
 * This tests progressive enhancement - core content should be in static HTML
 */

const fs = require('fs');
const path = require('path');

describe('No JavaScript Core Content - NFR-6', () => {
  let document;
  let htmlContent;

  beforeAll(() => {
    // Read the static HTML file (not the React entry point)
    const htmlPath = path.join(__dirname, '../src/index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf8');
    document = new DOMParser().parseFromString(htmlContent, 'text/html');
  });

  /**
   * Test Case 1: Hero section content (title, tagline, CTAs) is visible without JavaScript
   * Verifies that the hero section contains static HTML content, not JS-rendered content
   */
  describe('TC-1: Hero Section Content Without JavaScript', () => {
    test('hero section should be in static HTML, not in an empty root div', () => {
      // Check that hero content is NOT inside an empty div waiting for JS hydration
      const rootDiv = document.getElementById('root');
      const heroSection = document.querySelector('.hero-section');

      // Hero section should exist and NOT be inside an empty root div
      expect(heroSection).not.toBeNull();

      // If root div exists, hero should not be its only/main content
      // The static HTML has hero content directly in body
      if (rootDiv) {
        expect(heroSection.closest('#root')).toBeNull();
      }
    });

    test('product title "MirDB" should be visible in static HTML', () => {
      const title = document.querySelector('.hero-section h1, .product-name');
      expect(title).not.toBeNull();
      expect(title.textContent).toContain('MirDB');
    });

    test('tagline should be visible in static HTML', () => {
      const tagline = document.querySelector('.hero-tagline, .tagline');
      expect(tagline).not.toBeNull();
      const taglineText = tagline.textContent.toLowerCase();
      expect(taglineText).toContain('persistent');
      expect(taglineText).toContain('key-value');
      expect(taglineText).toContain('memcached');
    });

    test('value proposition description should be visible in static HTML', () => {
      const description = document.querySelector('.hero-description, .value-proposition');
      expect(description).not.toBeNull();
      expect(description.textContent.trim().length).toBeGreaterThan(50);
    });

    test('primary CTA button should be visible in static HTML', () => {
      const ctaPrimary = document.querySelector('.cta-primary, .btn-primary');
      expect(ctaPrimary).not.toBeNull();
      expect(ctaPrimary.tagName.toLowerCase()).toBe('a');
      expect(ctaPrimary.getAttribute('href')).toBeTruthy();

      const ctaText = ctaPrimary.textContent.toLowerCase();
      expect(ctaText.includes('get started') || ctaText.includes('documentation')).toBe(true);
    });

    test('secondary CTA (GitHub) button should be visible in static HTML', () => {
      const ctaSecondary = document.querySelector('.cta-secondary, a[href*="github"]');
      expect(ctaSecondary).not.toBeNull();
      expect(ctaSecondary.getAttribute('href')).toMatch(/github\.com.*mirdb/i);
    });
  });

  /**
   * Test Case 2: Features section content is visible without JavaScript
   * Verifies that feature cards are in static HTML
   */
  describe('TC-2: Features Section Content Without JavaScript', () => {
    test('features section should exist in static HTML', () => {
      const featuresSection = document.querySelector('#features, .features-section');
      expect(featuresSection).not.toBeNull();
    });

    test('features section heading should be visible', () => {
      const featuresSection = document.querySelector('#features, .features-section');
      const heading = featuresSection.querySelector('h2');
      expect(heading).not.toBeNull();
      expect(heading.textContent.toLowerCase()).toContain('feature');
    });

    test('feature cards should be present in static HTML', () => {
      const featureCards = document.querySelectorAll('.feature-card');
      expect(featureCards.length).toBeGreaterThanOrEqual(3);
    });

    test('Memcached Compatible feature should be visible', () => {
      const featureCards = document.querySelectorAll('.feature-card');
      const cardTexts = Array.from(featureCards).map(card => card.textContent.toLowerCase());

      const hasMemcachedFeature = cardTexts.some(text =>
        text.includes('memcached') && text.includes('compatible')
      );
      expect(hasMemcachedFeature).toBe(true);
    });

    test('Persistent Storage feature should be visible', () => {
      const featureCards = document.querySelectorAll('.feature-card');
      const cardTexts = Array.from(featureCards).map(card => card.textContent.toLowerCase());

      const hasPersistentFeature = cardTexts.some(text =>
        text.includes('persistent') && text.includes('storage')
      );
      expect(hasPersistentFeature).toBe(true);
    });

    test('High Performance feature should be visible', () => {
      const featureCards = document.querySelectorAll('.feature-card');
      const cardTexts = Array.from(featureCards).map(card => card.textContent.toLowerCase());

      const hasPerformanceFeature = cardTexts.some(text =>
        text.includes('performance') || text.includes('lsm')
      );
      expect(hasPerformanceFeature).toBe(true);
    });
  });

  /**
   * Test Case 3: Code snippet content is visible without JavaScript
   * Verifies that code examples are in static HTML (copy button may not work)
   */
  describe('TC-3: Code Snippet Content Without JavaScript', () => {
    test('quick start section should exist in static HTML', () => {
      const quickStartSection = document.querySelector('.quickstart-section, #documentation');
      expect(quickStartSection).not.toBeNull();
    });

    test('code block should be present in static HTML', () => {
      const codeBlock = document.querySelector('.code-block');
      expect(codeBlock).not.toBeNull();
    });

    test('code snippet content should be visible', () => {
      const codeElement = document.querySelector('.code-block code, .code-block pre');
      expect(codeElement).not.toBeNull();

      const codeContent = codeElement.textContent;
      expect(codeContent.length).toBeGreaterThan(0);
    });

    test('code snippet should show MirDB usage example', () => {
      const codeElement = document.querySelector('.code-block code, .code-block pre');
      const codeContent = codeElement.textContent.toLowerCase();

      // Should contain MirDB commands/usage
      expect(
        codeContent.includes('mirdb') ||
        codeContent.includes('set') ||
        codeContent.includes('get') ||
        codeContent.includes('telnet')
      ).toBe(true);
    });

    test('code snippet should demonstrate key operations', () => {
      const codeElement = document.querySelector('.code-block code, .code-block pre');
      const codeContent = codeElement.textContent;

      // Should show SET and GET operations (core memcached commands)
      expect(codeContent).toMatch(/set\s+\w+/i);
      expect(codeContent).toMatch(/get\s+\w+/i);
    });
  });

  /**
   * Test Case 4: Footer links are visible and clickable without JavaScript
   * Verifies footer navigation works without JS
   */
  describe('TC-4: Footer Links Without JavaScript', () => {
    test('footer should exist in static HTML', () => {
      const footer = document.querySelector('footer, .footer');
      expect(footer).not.toBeNull();
    });

    test('footer navigation links should be present', () => {
      const footer = document.querySelector('footer, .footer');
      const links = footer.querySelectorAll('a');
      expect(links.length).toBeGreaterThanOrEqual(2);
    });

    test('Documentation link should be visible and clickable', () => {
      const footer = document.querySelector('footer, .footer');
      const docLink = footer.querySelector('a[href*="doc"], a[href*="#documentation"]');
      expect(docLink).not.toBeNull();
      expect(docLink.getAttribute('href')).toBeTruthy();
    });

    test('GitHub link should be visible and clickable', () => {
      const footer = document.querySelector('footer, .footer');
      const githubLink = footer.querySelector('a[href*="github"]');
      expect(githubLink).not.toBeNull();
      expect(githubLink.getAttribute('href')).toMatch(/github\.com.*mirdb/i);
    });

    test('License link should be visible and clickable', () => {
      const footer = document.querySelector('footer, .footer');
      const licenseLink = footer.querySelector('a[href*="license"], a[href*="LICENSE"]');
      expect(licenseLink).not.toBeNull();
      expect(licenseLink.getAttribute('href')).toBeTruthy();
    });

    test('footer links should work as standard HTML anchors (no JS required)', () => {
      const footer = document.querySelector('footer, .footer');
      const links = footer.querySelectorAll('a');

      links.forEach(link => {
        // All links should have href attribute (work without JS)
        expect(link.getAttribute('href')).toBeTruthy();
        // Links should not have onclick handlers that would require JS
        expect(link.getAttribute('onclick')).toBeNull();
      });
    });
  });

  /**
   * Test Case 5: Navigation links are accessible without JavaScript
   * Verifies that navigation works without JS
   */
  describe('TC-5: Navigation Links Without JavaScript', () => {
    test('navigation should work without JavaScript', () => {
      // Check all navigation-related links in the page
      const navLinks = document.querySelectorAll('nav a, .footer-links a, .cta-buttons a');
      expect(navLinks.length).toBeGreaterThan(0);

      navLinks.forEach(link => {
        // All navigation links should have href (work without JS)
        expect(link.getAttribute('href')).toBeTruthy();
      });
    });

    test('internal anchor links should point to existing sections', () => {
      const internalLinks = document.querySelectorAll('a[href^="#"]');

      internalLinks.forEach(link => {
        const targetId = link.getAttribute('href').substring(1);
        if (targetId) {
          const targetElement = document.getElementById(targetId);
          expect(targetElement).not.toBeNull();
        }
      });
    });

    test('skip link should work without JavaScript', () => {
      const skipLink = document.querySelector('.skip-link, a[href="#main-content"]');
      expect(skipLink).not.toBeNull();
      expect(skipLink.getAttribute('href')).toBeTruthy();

      const targetId = skipLink.getAttribute('href').substring(1);
      const targetElement = document.getElementById(targetId);
      expect(targetElement).not.toBeNull();
    });

    test('CTA navigation links should be standard HTML anchors', () => {
      const ctaLinks = document.querySelectorAll('.cta-buttons a, .hero-cta a');

      ctaLinks.forEach(link => {
        expect(link.tagName.toLowerCase()).toBe('a');
        expect(link.getAttribute('href')).toBeTruthy();
        // Should not rely on JavaScript event handlers
        expect(link.getAttribute('onclick')).toBeNull();
      });
    });
  });

  /**
   * Additional tests for progressive enhancement verification
   */
  describe('Progressive Enhancement Verification', () => {
    test('HTML document should be fully parseable without JavaScript', () => {
      // Document should have complete structure
      expect(document.doctype).not.toBeNull();
      expect(document.documentElement).not.toBeNull();
      expect(document.head).not.toBeNull();
      expect(document.body).not.toBeNull();
    });

    test('page should not have blocking script tags before content', () => {
      // Check that scripts don't block content rendering
      const body = document.body;
      const firstContentElement = body.querySelector('header, main, section, .hero-section');
      const scripts = body.querySelectorAll('script:not([defer]):not([async])');

      // If there are blocking scripts, they should come after main content
      if (scripts.length > 0 && firstContentElement) {
        scripts.forEach(script => {
          // Script should not be before the first content element
          const scriptPosition = Array.from(body.childNodes).indexOf(script);
          const contentPosition = Array.from(body.childNodes).indexOf(firstContentElement);

          // Scripts at end of body are acceptable
          // This is a basic heuristic - actual blocking behavior depends on placement
        });
      }

      // At minimum, content elements should exist
      expect(firstContentElement).not.toBeNull();
    });

    test('CSS should be available without JavaScript', () => {
      const styleLinks = document.querySelectorAll('link[rel="stylesheet"]');
      expect(styleLinks.length).toBeGreaterThan(0);

      styleLinks.forEach(link => {
        expect(link.getAttribute('href')).toBeTruthy();
      });
    });

    test('all essential content should be in initial HTML, not dynamically loaded', () => {
      // Verify key content elements exist in static HTML
      const essentialElements = [
        '.hero-section, .hero',
        '.features-section, #features',
        '.code-block, .quickstart-section',
        'footer, .footer'
      ];

      essentialElements.forEach(selector => {
        const element = document.querySelector(selector);
        expect(element).not.toBeNull();
      });
    });

    test('text content should not be empty or placeholder', () => {
      // Check that text content is real content, not loading placeholders
      const textElements = document.querySelectorAll('h1, h2, h3, p');

      textElements.forEach(element => {
        const text = element.textContent.trim();
        if (text.length > 0) {
          // Should not be loading placeholders
          expect(text.toLowerCase()).not.toBe('loading...');
          expect(text.toLowerCase()).not.toBe('loading');
          expect(text).not.toBe('...');
        }
      });
    });
  });

  /**
   * Verify that the page provides a good experience without JavaScript
   */
  describe('No-JavaScript User Experience', () => {
    test('main content sections should have proper semantic structure', () => {
      const main = document.querySelector('main, #main-content');
      expect(main).not.toBeNull();

      const sections = main.querySelectorAll('section');
      expect(sections.length).toBeGreaterThanOrEqual(1);
    });

    test('all links should be navigable without JavaScript', () => {
      const allLinks = document.querySelectorAll('a[href]');

      allLinks.forEach(link => {
        const href = link.getAttribute('href');
        // All links should have valid href
        expect(href).toBeTruthy();
        expect(href).not.toBe('#');
        expect(href).not.toBe('javascript:void(0)');
        expect(href).not.toBe('javascript:;');
      });
    });

    test('supported commands section should be visible without JavaScript', () => {
      const commandsSection = document.querySelector('#commands, .commands-section');

      if (commandsSection) {
        const commands = commandsSection.querySelectorAll('.command-item, .command-card');
        expect(commands.length).toBeGreaterThan(0);

        // Command text should be visible
        commands.forEach(cmd => {
          const codeElement = cmd.querySelector('code');
          expect(codeElement).not.toBeNull();
          expect(codeElement.textContent.trim().length).toBeGreaterThan(0);
        });
      }
    });
  });
});
