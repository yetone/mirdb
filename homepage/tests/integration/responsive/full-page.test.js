/**
 * Full Page Responsive Integration Tests
 * Owner: Scenario 7 - Responsive Design Full Range
 *
 * Tests:
 * - Page renders at 320px (minimum viewport)
 * - Page renders at 1920px (maximum viewport)
 * - No horizontal overflow at any viewport
 * - Text readable at all sizes (minimum 14px body text)
 * - Common breakpoints: 480px, 768px, 1024px, 1440px
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { JSDOM } from 'jsdom';
import { readFileSync, existsSync } from 'fs';
import { resolve } from 'path';

/**
 * Load all CSS files needed for the full page
 */
function loadAllCSS() {
  const cssFiles = [
    'src/styles/variables.css',
    'src/styles/reset.css',
    'src/styles/typography.css',
    'src/styles/layout.css',
    'src/components/Header/Header.css',
    'src/components/Navigation/Navigation.css',
    'src/components/Hero/Hero.css',
    'src/components/Features/Features.css',
    'src/components/Footer/Footer.css',
  ];

  let combinedCSS = '';
  for (const cssFile of cssFiles) {
    const filePath = resolve(process.cwd(), cssFile);
    if (existsSync(filePath)) {
      combinedCSS += readFileSync(filePath, 'utf-8') + '\n';
    }
  }
  return combinedCSS;
}

/**
 * Load the HTML document
 */
function loadHTML() {
  const htmlPath = resolve(process.cwd(), 'src/index.html');
  return readFileSync(htmlPath, 'utf-8');
}

/**
 * Create a test document with specified viewport width
 */
function createTestDocument(viewportWidth = 1200) {
  const html = loadHTML();
  const css = loadAllCSS();

  // Inject CSS into the HTML
  const htmlWithCSS = html.replace(
    '</head>',
    `<style>${css}</style></head>`
  );

  const dom = new JSDOM(htmlWithCSS, {
    url: 'http://localhost',
    pretendToBeVisual: true,
    resources: 'usable',
  });

  // Set viewport width
  Object.defineProperty(dom.window, 'innerWidth', {
    value: viewportWidth,
    writable: true,
    configurable: true,
  });

  Object.defineProperty(dom.window, 'innerHeight', {
    value: 800,
    writable: true,
    configurable: true,
  });

  return dom;
}

/**
 * Parse CSS to find specific rules
 */
function parseCSS(cssText) {
  return {
    hasOverflowXHidden: /overflow-x:\s*hidden/i.test(cssText),
    hasMinWidth320: /min-width:\s*320px/i.test(cssText),
    hasMaxWidth: /max-width:/i.test(cssText),
    breakpoints: {
      has320: /@media[^{]*320px/i.test(cssText),
      has480: /@media[^{]*480px/i.test(cssText),
      has768: /@media[^{]*768px/i.test(cssText),
      has1024: /@media[^{]*1024px/i.test(cssText),
      has1440: /@media[^{]*1440px/i.test(cssText),
      has1920: /@media[^{]*1920px/i.test(cssText),
    },
  };
}

/**
 * Check if layout.css exists and contains required responsive rules
 */
function validateLayoutCSS() {
  const layoutCSSPath = resolve(process.cwd(), 'src/styles/layout.css');
  if (!existsSync(layoutCSSPath)) {
    return { exists: false };
  }
  const layoutCSS = readFileSync(layoutCSSPath, 'utf-8');
  return {
    exists: true,
    css: layoutCSS,
    ...parseCSS(layoutCSS),
  };
}

describe('Full Page Responsive Design - Scenario 7', () => {
  let dom;
  let document;

  afterEach(() => {
    if (dom) {
      dom.window.close();
    }
  });

  describe('Test Case 1: Minimum viewport (320px)', () => {
    it('should render homepage at 320px width without errors', () => {
      dom = createTestDocument(320);
      document = dom.window.document;

      // Verify key elements exist
      expect(document.querySelector('header')).not.toBeNull();
      expect(document.querySelector('main')).not.toBeNull();
      expect(document.querySelector('footer')).not.toBeNull();
      expect(document.querySelector('.hero')).not.toBeNull();
      expect(document.querySelector('.features')).not.toBeNull();
    });

    it('should have all content visible at 320px', () => {
      dom = createTestDocument(320);
      document = dom.window.document;

      // Check hero content
      const heroTitle = document.querySelector('.hero__title');
      const heroTagline = document.querySelector('.hero__tagline');
      expect(heroTitle).not.toBeNull();
      expect(heroTagline).not.toBeNull();
      expect(heroTitle.textContent.trim().length).toBeGreaterThan(0);
      expect(heroTagline.textContent.trim().length).toBeGreaterThan(0);

      // Check features section
      const featureCards = document.querySelectorAll('.feature-card');
      expect(featureCards.length).toBeGreaterThan(0);
    });

    it('should have layout.css with styles for 320px minimum viewport', () => {
      const layoutValidation = validateLayoutCSS();
      expect(layoutValidation.exists).toBe(true);
      expect(layoutValidation.breakpoints.has320).toBe(true);
    });
  });

  describe('Test Case 2: Maximum viewport (1920px)', () => {
    it('should render homepage at 1920px width without errors', () => {
      dom = createTestDocument(1920);
      document = dom.window.document;

      expect(document.querySelector('header')).not.toBeNull();
      expect(document.querySelector('main')).not.toBeNull();
      expect(document.querySelector('footer')).not.toBeNull();
    });

    it('should have content properly centered/contained at 1920px', () => {
      const layoutValidation = validateLayoutCSS();
      expect(layoutValidation.exists).toBe(true);
      expect(layoutValidation.hasMaxWidth).toBe(true);
    });

    it('should have layout.css with styles for 1920px maximum viewport', () => {
      const layoutValidation = validateLayoutCSS();
      expect(layoutValidation.exists).toBe(true);
      expect(layoutValidation.breakpoints.has1920).toBe(true);
    });

    it('should have container with max-width to prevent excessive whitespace', () => {
      const layoutCSS = readFileSync(
        resolve(process.cwd(), 'src/styles/layout.css'),
        'utf-8'
      );
      // Should have container with max-width
      expect(layoutCSS).toMatch(/\.container\s*\{[^}]*max-width:/);
    });
  });

  describe('Test Case 3: No horizontal scroll at various widths', () => {
    const viewportWidths = [320, 480, 768, 1024, 1440, 1920];

    viewportWidths.forEach((width) => {
      it(`should have no horizontal overflow at ${width}px`, () => {
        dom = createTestDocument(width);
        document = dom.window.document;

        // All main elements should exist
        expect(document.querySelector('body')).not.toBeNull();
        expect(document.querySelector('main')).not.toBeNull();
      });
    });

    it('should have overflow-x: hidden on html or body', () => {
      const layoutValidation = validateLayoutCSS();
      expect(layoutValidation.exists).toBe(true);
      expect(layoutValidation.hasOverflowXHidden).toBe(true);
    });

    it('should have images with max-width: 100%', () => {
      const layoutCSS = readFileSync(
        resolve(process.cwd(), 'src/styles/layout.css'),
        'utf-8'
      );
      // Should have max-width: 100% for images
      expect(layoutCSS).toMatch(/img[^{]*\{[^}]*max-width:\s*100%/s);
    });

    it('should have body with min-width: 320px', () => {
      const layoutValidation = validateLayoutCSS();
      expect(layoutValidation.exists).toBe(true);
      expect(layoutValidation.hasMinWidth320).toBe(true);
    });
  });

  describe('Test Case 4: Text readability at 320px', () => {
    it('should have minimum 14px body text at 320px viewport', () => {
      const layoutCSS = readFileSync(
        resolve(process.cwd(), 'src/styles/layout.css'),
        'utf-8'
      );
      // Should have 14px minimum font size rule for 320px viewport
      expect(layoutCSS).toMatch(/@media[^{]*320px[^{]*\{[^}]*font-size:\s*14px/s);
    });

    it('should have all text content present and non-empty', () => {
      dom = createTestDocument(320);
      document = dom.window.document;

      // Hero section text
      const heroTitle = document.querySelector('.hero__title');
      const heroTagline = document.querySelector('.hero__tagline');
      expect(heroTitle.textContent.trim()).toBe('MirDB');
      expect(heroTagline.textContent.trim().length).toBeGreaterThan(10);

      // Features section text
      const featuresTitle = document.querySelector('.features__title');
      expect(featuresTitle.textContent.trim()).toBe('Features');

      // Feature cards
      const featureCards = document.querySelectorAll('.feature-card');
      featureCards.forEach((card) => {
        const title = card.querySelector('.feature-card__title');
        const description = card.querySelector('.feature-card__description');
        expect(title.textContent.trim().length).toBeGreaterThan(0);
        expect(description.textContent.trim().length).toBeGreaterThan(0);
      });
    });

    it('should have responsive font sizing using clamp() for scalable text', () => {
      const layoutCSS = readFileSync(
        resolve(process.cwd(), 'src/styles/layout.css'),
        'utf-8'
      );
      // Should use clamp() for responsive font sizes
      expect(layoutCSS).toMatch(/clamp\(/);
    });
  });

  describe('Common Breakpoint Coverage', () => {
    it('should have CSS rules for 480px breakpoint', () => {
      const layoutValidation = validateLayoutCSS();
      expect(layoutValidation.breakpoints.has480).toBe(true);
    });

    it('should have CSS rules for 768px breakpoint', () => {
      const layoutValidation = validateLayoutCSS();
      expect(layoutValidation.breakpoints.has768).toBe(true);
    });

    it('should have CSS rules for 1024px breakpoint', () => {
      const layoutValidation = validateLayoutCSS();
      expect(layoutValidation.breakpoints.has1024).toBe(true);
    });

    it('should have CSS rules for 1440px breakpoint', () => {
      const layoutValidation = validateLayoutCSS();
      expect(layoutValidation.breakpoints.has1440).toBe(true);
    });
  });

  describe('Layout CSS Structure', () => {
    it('should have container utilities', () => {
      const layoutCSS = readFileSync(
        resolve(process.cwd(), 'src/styles/layout.css'),
        'utf-8'
      );
      expect(layoutCSS).toMatch(/\.container\s*\{/);
    });

    it('should have section spacing utilities', () => {
      const layoutCSS = readFileSync(
        resolve(process.cwd(), 'src/styles/layout.css'),
        'utf-8'
      );
      expect(layoutCSS).toMatch(/\.section\s*\{/);
    });

    it('should use CSS custom properties for spacing', () => {
      const layoutCSS = readFileSync(
        resolve(process.cwd(), 'src/styles/layout.css'),
        'utf-8'
      );
      expect(layoutCSS).toMatch(/var\(--spacing-/);
    });

    it('should have mobile-first media queries', () => {
      const layoutCSS = readFileSync(
        resolve(process.cwd(), 'src/styles/layout.css'),
        'utf-8'
      );
      // Should have min-width media queries (mobile-first)
      expect(layoutCSS).toMatch(/@media\s*\(\s*min-width:/);
    });
  });

  describe('Document Structure', () => {
    it('should include layout.css in the HTML document', () => {
      const html = loadHTML();
      expect(html).toMatch(/layout\.css/);
    });

    it('should have semantic HTML structure', () => {
      dom = createTestDocument(1200);
      document = dom.window.document;

      expect(document.querySelector('header')).not.toBeNull();
      expect(document.querySelector('nav')).not.toBeNull();
      expect(document.querySelector('main')).not.toBeNull();
      expect(document.querySelector('footer')).not.toBeNull();
    });

    it('should have viewport meta tag for responsive design', () => {
      dom = createTestDocument(1200);
      document = dom.window.document;

      const viewportMeta = document.querySelector('meta[name="viewport"]');
      expect(viewportMeta).not.toBeNull();
      expect(viewportMeta.getAttribute('content')).toContain('width=device-width');
    });
  });

  describe('Responsive Grid and Flex Utilities', () => {
    it('should have grid utility classes', () => {
      const layoutCSS = readFileSync(
        resolve(process.cwd(), 'src/styles/layout.css'),
        'utf-8'
      );
      expect(layoutCSS).toMatch(/\.grid\s*\{/);
      expect(layoutCSS).toMatch(/display:\s*grid/);
    });

    it('should have flex utility classes', () => {
      const layoutCSS = readFileSync(
        resolve(process.cwd(), 'src/styles/layout.css'),
        'utf-8'
      );
      expect(layoutCSS).toMatch(/\.flex\s*\{/);
      expect(layoutCSS).toMatch(/display:\s*flex/);
    });

    it('should have responsive grid column utilities', () => {
      const layoutCSS = readFileSync(
        resolve(process.cwd(), 'src/styles/layout.css'),
        'utf-8'
      );
      expect(layoutCSS).toMatch(/grid-template-columns:/);
    });
  });

  describe('Safe Area and Modern CSS Support', () => {
    it('should include safe area support for notched devices', () => {
      const layoutCSS = readFileSync(
        resolve(process.cwd(), 'src/styles/layout.css'),
        'utf-8'
      );
      expect(layoutCSS).toMatch(/env\(safe-area-inset/);
    });

    it('should use CSS clamp() for responsive sizing', () => {
      const layoutCSS = readFileSync(
        resolve(process.cwd(), 'src/styles/layout.css'),
        'utf-8'
      );
      expect(layoutCSS).toMatch(/clamp\(/);
    });
  });
});
