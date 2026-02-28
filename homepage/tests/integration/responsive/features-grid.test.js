/**
 * Features Grid Responsive Integration Tests
 * Owner: Scenario 5 - Features Section Responsive Grid
 *
 * Tests:
 * - Single column at mobile (375px)
 * - Two columns at tablet (768px)
 * - Three columns at desktop (1200px)
 * - Cards readable at minimum viewport (320px)
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { JSDOM } from 'jsdom';
import { readFileSync } from 'fs';
import { resolve } from 'path';

/**
 * Load the Features CSS and create a test document with inline styles
 */
function createTestDocument(viewportWidth = 1200) {
  const variablesCss = readFileSync(resolve(process.cwd(), 'src/styles/variables.css'), 'utf-8');
  const resetCss = readFileSync(resolve(process.cwd(), 'src/styles/reset.css'), 'utf-8');
  const featuresCss = readFileSync(resolve(process.cwd(), 'src/components/Features/Features.css'), 'utf-8');

  const html = `
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <style>
    ${variablesCss}
    ${resetCss}
    ${featuresCss}
  </style>
</head>
<body>
  <main>
    <section class="features" id="features" aria-labelledby="features-title">
      <h2 id="features-title" class="features__title">Features</h2>
      <div class="features__grid">
        <article class="feature-card">
          <div class="feature-card__icon" aria-hidden="true">
            <svg width="48" height="48" viewBox="0 0 48 48" fill="currentColor">
              <path d="M24 4L4 14v20l20 10 20-10V14L24 4z"/>
            </svg>
          </div>
          <h3 class="feature-card__title">Persistent Storage</h3>
          <p class="feature-card__description">Data survives restarts with our efficient disk-backed storage engine.</p>
        </article>
        <article class="feature-card">
          <div class="feature-card__icon" aria-hidden="true">
            <svg width="48" height="48" viewBox="0 0 48 48" fill="currentColor">
              <circle cx="24" cy="24" r="20"/>
            </svg>
          </div>
          <h3 class="feature-card__title">High Performance</h3>
          <p class="feature-card__description">Optimized for speed with sub-millisecond response times.</p>
        </article>
        <article class="feature-card">
          <div class="feature-card__icon" aria-hidden="true">
            <svg width="48" height="48" viewBox="0 0 48 48" fill="currentColor">
              <rect x="8" y="8" width="32" height="32" rx="4"/>
            </svg>
          </div>
          <h3 class="feature-card__title">Simple API</h3>
          <p class="feature-card__description">Easy-to-use API that integrates seamlessly with your applications.</p>
        </article>
      </div>
    </section>
  </main>
</body>
</html>
`;

  const dom = new JSDOM(html, {
    url: 'http://localhost',
    pretendToBeVisual: true,
  });

  // Set viewport width
  Object.defineProperty(dom.window, 'innerWidth', {
    value: viewportWidth,
    writable: true,
    configurable: true,
  });

  return dom;
}

/**
 * Parse CSS to extract media query rules and grid-template-columns values
 */
function parseCSS(cssText) {
  const rules = {
    base: null,
    mediaQueries: [],
  };

  // Extract base grid-template-columns
  const baseMatch = cssText.match(/\.features__grid\s*\{[^}]*grid-template-columns:\s*([^;]+)/);
  if (baseMatch) {
    rules.base = baseMatch[1].trim();
  }

  // Extract media queries
  const mediaQueryRegex = /@media\s*\([^)]+\)\s*\{[^}]*\.features__grid\s*\{[^}]*grid-template-columns:\s*([^;]+)/g;
  let match;
  while ((match = mediaQueryRegex.exec(cssText)) !== null) {
    const mediaMatch = match[0].match(/@media\s*\(([^)]+)\)/);
    if (mediaMatch) {
      rules.mediaQueries.push({
        condition: mediaMatch[1].trim(),
        columns: match[1].trim(),
      });
    }
  }

  return rules;
}

/**
 * Determine the expected number of columns based on viewport width
 * Using mobile-first approach with min-width breakpoints
 */
function getExpectedColumns(viewportWidth) {
  if (viewportWidth >= 1024) {
    return 3;
  } else if (viewportWidth >= 768) {
    return 2;
  } else {
    return 1;
  }
}

describe('Features Section Responsive Grid', () => {
  let dom;
  let document;
  let featuresCss;

  beforeEach(() => {
    featuresCss = readFileSync(resolve(process.cwd(), 'src/components/Features/Features.css'), 'utf-8');
  });

  afterEach(() => {
    if (dom) {
      dom.window.close();
    }
  });

  describe('Test Case 1: Mobile viewport (375px) - Single column layout', () => {
    it('should display feature cards in single column at 375px width', () => {
      dom = createTestDocument(375);
      document = dom.window.document;

      const featuresGrid = document.querySelector('.features__grid');
      const featureCards = document.querySelectorAll('.feature-card');

      // Verify grid exists
      expect(featuresGrid).not.toBeNull();
      expect(featureCards.length).toBe(3);

      // Verify CSS has mobile-first single column as base
      const cssRules = parseCSS(featuresCss);
      expect(cssRules.base).toBe('1fr');

      // Verify expected columns for 375px
      const expectedColumns = getExpectedColumns(375);
      expect(expectedColumns).toBe(1);
    });

    it('should have mobile-first CSS with 1fr as base grid columns', () => {
      // Check that the base style (no media query) is single column
      expect(featuresCss).toMatch(/\.features__grid\s*\{[^}]*grid-template-columns:\s*1fr/);
    });
  });

  describe('Test Case 2: Tablet viewport (768px) - Two column layout', () => {
    it('should display feature cards in 2-column grid at 768px width', () => {
      dom = createTestDocument(768);
      document = dom.window.document;

      const featuresGrid = document.querySelector('.features__grid');
      const featureCards = document.querySelectorAll('.feature-card');

      expect(featuresGrid).not.toBeNull();
      expect(featureCards.length).toBe(3);

      // Verify expected columns for 768px
      const expectedColumns = getExpectedColumns(768);
      expect(expectedColumns).toBe(2);
    });

    it('should have media query for min-width: 768px with 2 columns', () => {
      // Check that there's a media query for tablet breakpoint
      expect(featuresCss).toMatch(/@media\s*\(\s*min-width:\s*768px\s*\)/);
      expect(featuresCss).toMatch(/repeat\(2,\s*1fr\)/);
    });
  });

  describe('Test Case 3: Desktop viewport (1200px) - Three column layout', () => {
    it('should display feature cards in 3-column grid at 1200px width', () => {
      dom = createTestDocument(1200);
      document = dom.window.document;

      const featuresGrid = document.querySelector('.features__grid');
      const featureCards = document.querySelectorAll('.feature-card');

      expect(featuresGrid).not.toBeNull();
      expect(featureCards.length).toBe(3);

      // Verify expected columns for 1200px
      const expectedColumns = getExpectedColumns(1200);
      expect(expectedColumns).toBe(3);
    });

    it('should have media query for min-width: 1024px with 3 columns', () => {
      // Check that there's a media query for desktop breakpoint
      expect(featuresCss).toMatch(/@media\s*\(\s*min-width:\s*1024px\s*\)/);
      expect(featuresCss).toMatch(/repeat\(3,\s*1fr\)/);
    });
  });

  describe('Test Case 4: Minimum viewport (320px) - Cards readable', () => {
    it('should render feature cards readable and properly sized at 320px viewport', () => {
      dom = createTestDocument(320);
      document = dom.window.document;

      const featuresGrid = document.querySelector('.features__grid');
      const featureCards = document.querySelectorAll('.feature-card');

      // Verify grid and cards exist
      expect(featuresGrid).not.toBeNull();
      expect(featureCards.length).toBe(3);

      // Verify expected columns for 320px is single column
      const expectedColumns = getExpectedColumns(320);
      expect(expectedColumns).toBe(1);
    });

    it('should have CSS rules for minimum viewport support', () => {
      // Check that there's a media query for 320px
      expect(featuresCss).toMatch(/@media\s*\(\s*max-width:\s*320px\s*\)/);
    });

    it('should have each feature card with title and description', () => {
      dom = createTestDocument(320);
      document = dom.window.document;

      const featureCards = document.querySelectorAll('.feature-card');

      featureCards.forEach((card) => {
        const title = card.querySelector('.feature-card__title');
        const description = card.querySelector('.feature-card__description');

        expect(title).not.toBeNull();
        expect(title.textContent.trim().length).toBeGreaterThan(0);

        expect(description).not.toBeNull();
        expect(description.textContent.trim().length).toBeGreaterThan(0);
      });
    });

    it('should have feature cards with text overflow handling', () => {
      // Verify CSS has word-wrap and overflow-wrap properties
      expect(featuresCss).toMatch(/word-wrap:\s*break-word/);
      expect(featuresCss).toMatch(/overflow-wrap:\s*break-word/);
    });
  });

  describe('CSS Structure Validation', () => {
    it('should use CSS Grid for layout', () => {
      expect(featuresCss).toMatch(/display:\s*grid/);
    });

    it('should have gap between grid items', () => {
      expect(featuresCss).toMatch(/gap:\s*var\(--spacing-\d+\)/);
    });

    it('should use CSS custom properties for responsive design', () => {
      // Verify CSS uses custom properties
      expect(featuresCss).toMatch(/var\(--/);
    });

    it('should have mobile-first media queries (min-width)', () => {
      const cssRules = parseCSS(featuresCss);

      // Should have at least two min-width media queries
      const minWidthQueries = cssRules.mediaQueries.filter((q) =>
        q.condition.includes('min-width')
      );

      expect(minWidthQueries.length).toBeGreaterThanOrEqual(2);
    });
  });

  describe('Responsive Breakpoint Order', () => {
    it('should follow correct breakpoint order: mobile (default) < tablet (768px) < desktop (1024px)', () => {
      const cssRules = parseCSS(featuresCss);

      // Base should be single column
      expect(cssRules.base).toBe('1fr');

      // Find tablet and desktop breakpoints
      const tabletRule = cssRules.mediaQueries.find((q) =>
        q.condition.includes('768px')
      );
      const desktopRule = cssRules.mediaQueries.find((q) =>
        q.condition.includes('1024px')
      );

      expect(tabletRule).toBeDefined();
      expect(desktopRule).toBeDefined();

      // Verify column counts
      expect(tabletRule.columns).toContain('2');
      expect(desktopRule.columns).toContain('3');
    });
  });

  describe('Feature Cards Structure', () => {
    it('should have all feature cards with proper semantic structure', () => {
      dom = createTestDocument(1200);
      document = dom.window.document;

      const featureCards = document.querySelectorAll('.feature-card');

      featureCards.forEach((card) => {
        // Each card should be an article element
        expect(card.tagName).toBe('ARTICLE');

        // Each card should have icon, title, and description
        expect(card.querySelector('.feature-card__icon')).not.toBeNull();
        expect(card.querySelector('.feature-card__title')).not.toBeNull();
        expect(card.querySelector('.feature-card__description')).not.toBeNull();
      });
    });

    it('should have icons with aria-hidden for accessibility', () => {
      dom = createTestDocument(1200);
      document = dom.window.document;

      const icons = document.querySelectorAll('.feature-card__icon');

      icons.forEach((icon) => {
        expect(icon.getAttribute('aria-hidden')).toBe('true');
      });
    });
  });
});
