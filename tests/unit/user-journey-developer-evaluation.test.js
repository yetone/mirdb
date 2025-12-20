/**
 * Unit Tests for Developer Evaluation User Journey
 * Scenario: User Journey - Developer Evaluation Flow
 * Validates the complete user journey from landing to accessing documentation,
 * simulating Developer Dan's evaluation workflow.
 */

const fs = require('fs');
const path = require('path');
const { JSDOM } = require('jsdom');

describe('User Journey - Developer Evaluation Flow', () => {
  let document;
  let dom;

  beforeAll(() => {
    const htmlPath = path.resolve(__dirname, '../../index.html');
    const html = fs.readFileSync(htmlPath, 'utf-8');
    dom = new JSDOM(html, { url: 'http://localhost' });
    document = dom.window.document;
  });

  /**
   * Test Case 1: Complete Developer Dan user journey from landing to docs click
   * Expected: User can complete entire journey without confusion or dead ends
   */
  describe('Test Case 1: Complete User Journey', () => {
    test('should have page title containing MirDB', () => {
      const title = document.querySelector('title');
      expect(title.textContent).toContain('MirDB');
    });

    test('should display hero section with product name', () => {
      const hero = document.querySelector('.hero');
      expect(hero).not.toBeNull();

      const h1 = document.querySelector('h1');
      expect(h1).not.toBeNull();
      expect(h1.textContent).toBe('MirDB');
    });

    test('should display tagline with key value proposition', () => {
      const tagline = document.querySelector('.hero-tagline');
      expect(tagline).not.toBeNull();
      expect(tagline.textContent.toLowerCase()).toContain('persistent');
      expect(tagline.textContent.toLowerCase()).toContain('memcached');
    });

    test('should display features section for evaluation', () => {
      const featuresSection = document.getElementById('features');
      expect(featuresSection).not.toBeNull();

      const featureCards = featuresSection.querySelectorAll('.feature-card');
      expect(featureCards.length).toBeGreaterThanOrEqual(5);
    });

    test('should display quick-start section for getting started', () => {
      const quickStart = document.getElementById('quick-start');
      expect(quickStart).not.toBeNull();

      // Verify code examples exist
      const codeBlocks = quickStart.querySelectorAll('.code-block, pre code');
      expect(codeBlocks.length).toBeGreaterThan(0);
    });

    test('should have CTA buttons for documentation access', () => {
      const primaryCTA = document.querySelector('.btn-primary');
      expect(primaryCTA).not.toBeNull();
      expect(primaryCTA.textContent.toLowerCase()).toMatch(/get started|start|begin/);

      const githubCTA = document.querySelector('.btn-secondary');
      expect(githubCTA).not.toBeNull();
      expect(githubCTA.getAttribute('href')).toContain('github.com');
    });

    test('should have no broken internal navigation links', () => {
      const internalLinks = document.querySelectorAll('a[href^="#"]');

      internalLinks.forEach(link => {
        const href = link.getAttribute('href');
        if (href === '#') return; // Skip empty hash links

        const targetId = href.replace('#', '');
        const target = document.getElementById(targetId);
        expect(target).not.toBeNull();
      });
    });

    test('should have all major sections present in DOM', () => {
      // Hero section
      const hero = document.querySelector('.hero');
      expect(hero).not.toBeNull();

      // Features section
      const features = document.getElementById('features');
      expect(features).not.toBeNull();

      // Quick Start section
      const quickStart = document.getElementById('quick-start');
      expect(quickStart).not.toBeNull();

      // Footer
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();
    });
  });

  /**
   * Test Case 2: Measure time to understand value proposition
   * Expected: Value proposition is clear within 3 seconds of page load
   */
  describe('Test Case 2: Value Proposition Clarity', () => {
    test('should have all hero content elements present for immediate display', () => {
      const h1 = document.querySelector('h1');
      expect(h1).not.toBeNull();

      const tagline = document.querySelector('.hero-tagline');
      expect(tagline).not.toBeNull();

      const description = document.querySelector('.hero-description');
      expect(description).not.toBeNull();

      const ctaButtons = document.querySelector('.hero-cta');
      expect(ctaButtons).not.toBeNull();
    });

    test('should clearly communicate persistence advantage in hero', () => {
      const heroContent = document.querySelector('.hero-content') || document.querySelector('.hero');
      const text = heroContent.textContent.toLowerCase();

      expect(text).toContain('persistent');
    });

    test('should mention memcached compatibility in hero', () => {
      const heroContent = document.querySelector('.hero-content') || document.querySelector('.hero');
      const text = heroContent.textContent.toLowerCase();

      expect(text).toContain('memcached');
    });

    test('should have concise tagline (< 100 characters)', () => {
      const tagline = document.querySelector('.hero-tagline');
      expect(tagline.textContent.length).toBeLessThan(100);
      expect(tagline.textContent.length).toBeGreaterThan(20);
    });

    test('should have detailed description explaining value', () => {
      const description = document.querySelector('.hero-description');
      expect(description.textContent.length).toBeGreaterThan(50);
    });

    test('should display key differentiators', () => {
      const differentiators = document.querySelector('.hero-differentiators');
      expect(differentiators).not.toBeNull();

      const text = differentiators.textContent.toLowerCase();
      expect(text).toContain('persistent');
      expect(text).toContain('rust');
    });
  });

  /**
   * Test Case 3: Verify logical content flow from hero to footer
   * Expected: Content sections flow logically: Hero -> Features -> Quick Start -> Docs CTA
   */
  describe('Test Case 3: Logical Content Flow', () => {
    test('should have sections in correct DOM order', () => {
      const main = document.querySelector('main');
      const sections = main.querySelectorAll('section');

      // Convert to array and get section identifiers
      const sectionIds = Array.from(sections).map(s => s.id || s.className.split(' ')[0]);

      // Check hero is first
      expect(sectionIds[0]).toMatch(/hero/);

      // Check features comes before quick-start
      const featuresIndex = sectionIds.findIndex(id => id.includes('features'));
      const quickStartIndex = sectionIds.findIndex(id => id.includes('quick-start'));

      expect(featuresIndex).toBeLessThan(quickStartIndex);
    });

    test('should have navigation matching content sections', () => {
      const nav = document.querySelector('nav');
      if (nav) {
        const navLinks = nav.querySelectorAll('a[href^="#"]');

        navLinks.forEach(link => {
          const href = link.getAttribute('href');
          if (href && href !== '#') {
            const targetId = href.replace('#', '');
            const target = document.getElementById(targetId);
            expect(target).not.toBeNull();
          }
        });
      }
    });

    test('should have footer as last element', () => {
      const body = document.body;
      const lastElement = body.lastElementChild;
      expect(lastElement.tagName.toLowerCase()).toBe('footer');
    });

    test('should have proper heading hierarchy', () => {
      const h1 = document.querySelectorAll('h1');
      const h2 = document.querySelectorAll('h2');

      // Should have exactly one h1
      expect(h1.length).toBe(1);

      // Should have multiple h2s for sections
      expect(h2.length).toBeGreaterThanOrEqual(2);
    });
  });

  /**
   * Test Case 4: Check for clear next-step CTAs throughout page
   * Expected: Each section has clear call-to-action or navigation hint
   */
  describe('Test Case 4: Clear CTAs Throughout Page', () => {
    test('should have prominent CTAs in hero section', () => {
      const heroCTA = document.querySelector('.hero-cta');
      expect(heroCTA).not.toBeNull();

      const buttons = heroCTA.querySelectorAll('.btn');
      expect(buttons.length).toBeGreaterThanOrEqual(2);
    });

    test('should have primary CTA with clear action text', () => {
      const primaryCTA = document.querySelector('.btn-primary');
      expect(primaryCTA).not.toBeNull();

      const text = primaryCTA.textContent.toLowerCase();
      expect(text).toMatch(/get started|start|try|begin/);
    });

    test('should have secondary CTA linking to GitHub', () => {
      const secondaryCTA = document.querySelector('.btn-secondary');
      expect(secondaryCTA).not.toBeNull();

      const href = secondaryCTA.getAttribute('href');
      expect(href).toContain('github.com');
    });

    test('should have documentation links in footer', () => {
      const footer = document.querySelector('footer');
      const docsLink = footer.querySelector('a[href*="docs"], a[href*="Documentation"], a[href*="README"]');
      expect(docsLink).not.toBeNull();
    });

    test('should have external links with proper attributes', () => {
      const externalLinks = document.querySelectorAll('a[href^="http"]');

      externalLinks.forEach(link => {
        const target = link.getAttribute('target');
        const rel = link.getAttribute('rel');

        expect(target).toBe('_blank');
        expect(rel).toContain('noopener');
      });
    });

    test('should have section IDs for anchor navigation', () => {
      const expectedSections = ['features', 'quick-start', 'configuration'];

      expectedSections.forEach(sectionId => {
        const section = document.getElementById(sectionId);
        expect(section).not.toBeNull();
      });
    });
  });

  /**
   * Additional tests for complete user journey experience
   */
  describe('User Journey Experience Quality', () => {
    test('should have proper HTML structure with semantic elements', () => {
      const header = document.querySelector('header');
      const main = document.querySelector('main');
      const footer = document.querySelector('footer');

      expect(header).not.toBeNull();
      expect(main).not.toBeNull();
      expect(footer).not.toBeNull();
    });

    test('should have accessible navigation', () => {
      const nav = document.querySelector('nav');
      expect(nav).not.toBeNull();

      const ariaLabel = nav.getAttribute('aria-label');
      expect(ariaLabel).not.toBeNull();
    });

    test('should have proper meta description for SEO', () => {
      const metaDesc = document.querySelector('meta[name="description"]');
      expect(metaDesc).not.toBeNull();

      const content = metaDesc.getAttribute('content');
      expect(content).toContain('MirDB');
      expect(content.length).toBeGreaterThan(50);
    });

    test('should have viewport meta tag for responsiveness', () => {
      const viewport = document.querySelector('meta[name="viewport"]');
      expect(viewport).not.toBeNull();

      const content = viewport.getAttribute('content');
      expect(content).toContain('width=device-width');
    });

    test('should display all 6 key features', () => {
      const featureCards = document.querySelectorAll('.feature-card');
      expect(featureCards.length).toBeGreaterThanOrEqual(6);

      // Check feature titles
      const expectedFeatures = [
        'memcached',
        'persistent',
        'lsm-tree',
        'async',
        'compaction',
        'write-ahead'
      ];

      const featureTitles = Array.from(document.querySelectorAll('.feature-title'))
        .map(el => el.textContent.toLowerCase());

      expectedFeatures.forEach(feature => {
        const found = featureTitles.some(title => title.includes(feature));
        expect(found).toBe(true);
      });
    });

    test('should have installation commands in quick start', () => {
      const quickStart = document.getElementById('quick-start');
      const text = quickStart.textContent.toLowerCase();

      expect(text).toContain('cargo');
      expect(text).toContain('git clone');
    });

    test('should have memcached command examples in quick start', () => {
      const quickStart = document.getElementById('quick-start');
      const text = quickStart.textContent.toLowerCase();

      expect(text).toContain('set');
      expect(text).toContain('get');
    });
  });
});
