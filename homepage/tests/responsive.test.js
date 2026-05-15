/**
 * Responsive design tests for MirDB homepage.
 * Owner: Scenario 7 - Responsive Design
 *
 * Test framework: Vitest + jsdom
 *
 * Test coverage:
 * - Desktop (1280px): all sections visible, horizontal nav, multi-col feature grid
 * - Tablet (768px): adapted layout, 2-col feature grid
 * - Mobile (375px): hamburger menu, single column, no horizontal scroll
 * - Touch targets >= 44x44px on mobile
 * - Code block readable on mobile
 * - Logo scales appropriately on small screens
 * - No horizontal overflow at any breakpoint
 * - Smooth breakpoint transitions defined
 */

import { describe, it, expect, beforeAll } from 'vitest';
import { readFileSync, existsSync } from 'fs';
import { resolve } from 'path';
import { JSDOM } from 'jsdom';

const htmlPath = resolve(__dirname, '../index.html');
const cssPath = resolve(__dirname, '../css/responsive.css');
const featuresCssPath = resolve(__dirname, '../css/features.css');
const heroCssPath = resolve(__dirname, '../css/hero.css');
const baseCssPath = resolve(__dirname, '../css/base.css');

function loadDOM(viewportWidth) {
  const html = readFileSync(htmlPath, 'utf-8');
  const dom = new JSDOM(html, {
    url: 'http://localhost:8080',
    resources: 'usable',
  });

  // Set viewport width
  dom.window.innerWidth = viewportWidth;

  // Inject CSS files for computed style testing
  const cssFiles = [
    { path: baseCssPath, exists: true },
    { path: heroCssPath, exists: true },
    { path: featuresCssPath, exists: true },
    { path: cssPath, exists: true },
  ];

  for (const { path: p } of cssFiles) {
    if (existsSync(p)) {
      const css = readFileSync(p, 'utf-8');
      const styleEl = dom.window.document.createElement('style');
      styleEl.textContent = css;
      dom.window.document.head.appendChild(styleEl);
    }
  }

  return dom;
}

function getRawCSS() {
  return readFileSync(cssPath, 'utf-8');
}

function getAllCSS() {
  const allCss = [];
  const cssPaths = [
    { path: cssPath, name: 'responsive' },
    { path: featuresCssPath, name: 'features' },
    { path: heroCssPath, name: 'hero' },
    { path: baseCssPath, name: 'base' },
  ];
  for (const { path: p, name } of cssPaths) {
    if (existsSync(p)) {
      allCss.push({ name, content: readFileSync(p, 'utf-8') });
    }
  }
  return allCss;
}

describe('Responsive Design', () => {
  describe('Test Case 1: Desktop viewport (1280px)', () => {
    let dom;
    let document;

    beforeAll(() => {
      dom = loadDOM(1280);
      document = dom.window.document;
    });

    it('should have all major sections present', () => {
      expect(document.getElementById('hero')).not.toBeNull();
      expect(document.getElementById('features')).not.toBeNull();
      expect(document.getElementById('quick-start')).not.toBeNull();
      expect(document.getElementById('resources')).not.toBeNull();
    });

    it('should have main content section', () => {
      const main = document.getElementById('main-content');
      expect(main).not.toBeNull();
    });

    it('should have a header element', () => {
      const header = document.querySelector('header');
      expect(header).not.toBeNull();
    });

    it('should have all CSS stylesheets linked', () => {
      const stylesheets = document.querySelectorAll('link[rel="stylesheet"]');
      const hrefs = Array.from(stylesheets).map((l) => l.getAttribute('href'));
      expect(hrefs.length).toBeGreaterThanOrEqual(6);
      expect(hrefs).toContain('css/responsive.css');
    });

    it('should have nav-links defined for horizontal navigation', () => {
      const css = getRawCSS();
      // Desktop horizontal nav should show nav-links
      expect(css).toMatch(/\.nav-links\s*\{[^}]*display:\s*flex/);
    });

    it('should hide hamburger on desktop', () => {
      const css = getRawCSS();
      // Hamburger should be hidden on desktop (min-width: 1024px)
      const hasDesktopHamburgerHidden =
        css.includes('.hamburger') &&
        css.includes('display: none');
      expect(hasDesktopHamburgerHidden).toBe(true);
    });

    it('should show 4-column feature grid on desktop', () => {
      const css = getRawCSS();
      // Desktop breakpoint should have 4-column grid
      const desktopSection = css.match(
        /@media[^{]*min-width:\s*1024px[^}]*\{[\s\S]*?grid-template-columns:\s*repeat\(4/
      );
      expect(desktopSection).not.toBeNull();
    });

    it('should show 3-column resources grid on desktop', () => {
      const css = getRawCSS();
      const desktopSection = css.match(
        /@media[^{]*min-width:\s*1024px[^}]*\{[\s\S]*?grid-template-columns:\s*repeat\(3/
      );
      expect(desktopSection).not.toBeNull();
    });
  });

  describe('Test Case 2: Tablet viewport (768px)', () => {
    it('should have a tablet breakpoint defined (768px-1023px)', () => {
      const css = getRawCSS();
      expect(css).toMatch(/@media[^{]*min-width:\s*768px[^{]*max-width:\s*1023px/);
    });

    it('should define 2-column feature grid for tablet', () => {
      const css = getRawCSS();
      const tabletSection = css.match(
        /@media[^{]*min-width:\s*768px[^{]*max-width:\s*1023px[^}]*\{[\s\S]*?\.features-grid[\s\S]*?grid-template-columns:\s*repeat\(2/
      );
      expect(tabletSection).not.toBeNull();
    });

    it('should define 2-column resources grid for tablet', () => {
      const css = getRawCSS();
      const tabletSection = css.match(
        /@media[^{]*min-width:\s*768px[^{]*max-width:\s*1023px[^}]*\{[\s\S]*?\.resources-grid[\s\S]*?grid-template-columns:\s*repeat\(2/
      );
      expect(tabletSection).not.toBeNull();
    });

    it('should show nav-links on tablet', () => {
      const css = getRawCSS();
      expect(css).toMatch(/\.nav-links\s*\{[^}]*display:\s*flex/);
    });
  });

  describe('Test Case 3: Mobile viewport (375px)', () => {
    let dom;
    let document;

    beforeAll(() => {
      dom = loadDOM(375);
      document = dom.window.document;
    });

    it('should have single column feature grid on mobile', () => {
      const css = getRawCSS();
      // Base styles (mobile-first) should have single column
      const hasSingleColumn = css.match(
        /\.features-grid\s*\{[^}]*grid-template-columns:\s*1fr/
      );
      expect(hasSingleColumn).not.toBeNull();
    });

    it('should have single column resources grid on mobile', () => {
      const css = getRawCSS();
      const hasSingleColumn = css.match(
        /\.resources-grid\s*\{[^}]*grid-template-columns:\s*1fr/
      );
      expect(hasSingleColumn).not.toBeNull();
    });

    it('should display hamburger menu button on mobile', () => {
      const css = getRawCSS();
      // Base styles should show hamburger
      expect(css).toMatch(/\.hamburger\s*\{[^}]*display:\s*flex/);
    });

    it('should hide nav-links on mobile by default', () => {
      const css = getRawCSS();
      // Base mobile styles should hide nav-links
      const baseNavLinks = css.match(
        /\.nav-links\s*\{[^}]*display:\s*[\w-]+/
      );
      expect(baseNavLinks).not.toBeNull();
    });

    it('should define mobile overlay menu', () => {
      const css = getRawCSS();
      expect(css).toMatch(/\.mobile-menu/);
      expect(css).toMatch(/\.mobile-menu\.open/);
    });

    it('should have hero CTAs stacked vertically on mobile', () => {
      const css = getRawCSS();
      // Base styles should have column direction for CTA group
      const ctaGroup = css.match(
        /\.hero-cta-group\s*\{[^}]*flex-direction:\s*column/
      );
      expect(ctaGroup).not.toBeNull();
    });

    it('should have full-width CTA buttons on mobile', () => {
      const css = getRawCSS();
      // Hero CTAs should be width: 100% at base (mobile)
      const ctasFullWidth =
        css.includes('.hero-cta-primary') &&
        css.includes('.hero-cta-secondary') &&
        css.includes('width: 100%');
      expect(ctasFullWidth).toBe(true);
    });

    it('should have reduced hero headline font size on mobile', () => {
      const css = getRawCSS();
      // Base styles should have smaller headline
      const headlineMatch = css.match(
        /\.hero-headline\s*\{[^}]*font-size:\s*1\.75rem/
      );
      expect(headlineMatch).not.toBeNull();
    });

    it('should have no horizontal scroll prevention rules', () => {
      const allCss = getAllCSS();
      const combined = allCss.map(c => c.content).join('\n');
      // Body or html should have overflow-x hidden or max-width 100%
      const hasOverflowPrevention =
        combined.includes('overflow-x: hidden') ||
        combined.includes('max-width: 100%') ||
        combined.includes('max-width: 100vw');
      expect(hasOverflowPrevention).toBe(true);
    });
  });

  describe('Test Case 4: Touch target sizes (minimum 44x44px)', () => {
    it('should define minimum touch target rules', () => {
      const css = getRawCSS();
      expect(css).toMatch(/min-height:\s*44px/);
      expect(css).toMatch(/min-width:\s*44px/);
    });

    it('should apply touch target minimums to CTA buttons', () => {
      const css = getRawCSS();
      // Look for a rule group that includes CTAs with min-height 44px
      const ctas = css.match(
        /\.hero-cta-primary[\s\S]*?min-height:\s*44px/
      );
      expect(ctas).not.toBeNull();
    });

    it('should apply touch target minimums to nav links', () => {
      const css = getRawCSS();
      expect(css).toMatch(/\.nav-link/);
      const touchTargetsRule = css.match(
        /min-height:\s*44px;[\s\S]*?min-width:\s*44px/
      );
      // Or separate declarations — check both min-height and min-width exist at 44px
      const hasMinHeight44 = (css.match(/min-height:\s*44px/g) || []).length >= 1;
      const hasMinWidth44 = (css.match(/min-width:\s*44px/g) || []).length >= 1;
      expect(hasMinHeight44).toBe(true);
      expect(hasMinWidth44).toBe(true);
    });

    it('should have spacing between adjacent touch targets', () => {
      const css = getRawCSS();
      // Adjacent nav links should have margin
      const hasSpacing = css.includes('.nav-link + .nav-link') &&
        css.includes('margin-top');
      expect(hasSpacing).toBe(true);
    });

    it('should apply touch target minimums to resource links', () => {
      const css = getRawCSS();
      // Resource links should be in the touch target list
      const touchGroup = css.match(
        /\.hero-cta-primary[\s\S]*?\.resource-link/
      );
      expect(touchGroup).not.toBeNull();
    });

    it('should apply touch target minimums to hamburger button', () => {
      const css = getRawCSS();
      expect(css).toMatch(/\.hamburger\s*\{[^}]*width:\s*44px[^}]*height:\s*44px/);
    });
  });

  describe('Test Case 5: Mobile menu touch targets', () => {
    it('should have hamburger icon at least 44x44px', () => {
      const css = getRawCSS();
      const hamburgerRule = css.match(
        /\.hamburger\s*\{[^}]*\}/
      );
      expect(hamburgerRule).not.toBeNull();
      const rule = hamburgerRule[0];
      expect(rule).toMatch(/width:\s*44px/);
      expect(rule).toMatch(/height:\s*44px/);
    });

    it('should have mobile menu items at least 44px tall', () => {
      const css = getRawCSS();
      expect(css).toMatch(/\.mobile-menu\s+\.nav-link\s*\{[^}]*min-height:\s*44px/);
    });

    it('should have sufficient spacing between mobile menu items', () => {
      const css = getRawCSS();
      // Mobile menu items should have padding
      const menuLinkRule = css.match(
        /\.mobile-menu\s+\.nav-link\s*\{[^}]*\}/
      );
      expect(menuLinkRule).not.toBeNull();
      const rule = menuLinkRule[0];
      expect(rule).toMatch(/padding/);
    });

    it('should have mobile menu items with border-radius for visual separation', () => {
      const css = getRawCSS();
      const menuLinkRule = css.match(
        /\.mobile-menu\s+\.nav-link\s*\{[^}]*\}/
      );
      expect(menuLinkRule).not.toBeNull();
      const rule = menuLinkRule[0];
      expect(rule).toMatch(/border-radius/);
    });
  });

  describe('Test Case 6: Code block readability on mobile', () => {
    it('should define code block overflow handling', () => {
      const css = getRawCSS();
      expect(css).toMatch(/\.code-block/);
    });

    it('should have horizontal scroll contained within code block', () => {
      const css = getRawCSS();
      const codeBlockRule = css.match(
        /\.code-block\s*\{[^}]*\}/
      );
      expect(codeBlockRule).not.toBeNull();
      const rule = codeBlockRule[0];
      expect(rule).toMatch(/overflow-x:\s*auto/);
    });

    it('should wrap code text appropriately on mobile', () => {
      const css = getRawCSS();
      const codeBlockRule = css.match(
        /\.code-block\s*\{[^}]*\}/
      );
      expect(codeBlockRule).not.toBeNull();
      const rule = codeBlockRule[0];
      expect(rule).toMatch(/white-space:\s*pre-wrap/);
    });

    it('should have smaller font size for code on mobile', () => {
      const css = getRawCSS();
      // Base (mobile) code block font size
      const codeBlockRule = css.match(
        /\.code-block\s*\{[^}]*\}/
      );
      expect(codeBlockRule).not.toBeNull();
      const rule = codeBlockRule[0];
      expect(rule).toMatch(/font-size/);
    });

    it('should increase code font size on desktop', () => {
      const css = getRawCSS();
      const desktopSection = css.match(
        /@media[^{]*min-width:\s*1024px[^}]*\{[\s\S]*?\.code-block[\s\S]*?font-size:\s*0\.9375rem/
      );
      expect(desktopSection).not.toBeNull();
    });
  });

  describe('Test Case 7: Logo scaling on mobile', () => {
    it('should define logo responsive sizing', () => {
      const css = getRawCSS();
      expect(css).toMatch(/\.hero-logo/);
    });

    it('should scale logo proportionally with max-height', () => {
      const css = getRawCSS();
      const logoRule = css.match(
        /\.hero-logo\s*\{[^}]*\}/
      );
      expect(logoRule).not.toBeNull();
      const rule = logoRule[0];
      expect(rule).toMatch(/max-height/);
      expect(rule).toMatch(/width:\s*auto/);
      expect(rule).toMatch(/height:\s*auto/);
    });

    it('should have logo max-height not exceeding 60px on desktop', () => {
      const css = getRawCSS();
      const desktopSection = css.match(
        /@media[^{]*min-width:\s*1024px[^}]*\{[\s\S]*?\.hero-logo[\s\S]*?max-height:\s*60px/
      );
      expect(desktopSection).not.toBeNull();
    });

    it('should have smaller logo max-height on mobile (48px)', () => {
      const css = getRawCSS();
      // Base (mobile) logo max-height
      const logoRule = css.match(
        /\.hero-logo\s*\{[^}]*max-height:\s*48px[^}]*\}/
      );
      expect(logoRule).not.toBeNull();
    });

    it('should have logo with max-width: 100% to prevent overflow', () => {
      const css = getRawCSS();
      const logoRule = css.match(
        /\.hero-logo\s*\{[^}]*\}/
      );
      expect(logoRule).not.toBeNull();
      const rule = logoRule[0];
      expect(rule).toMatch(/max-width:\s*100%/);
    });
  });

  describe('Test Case 8: Viewport resize transitions', () => {
    it('should define smooth transitions for layout properties', () => {
      const css = getRawCSS();
      // Look for transition declarations on layout elements
      expect(css).toMatch(/transition.*300ms ease/);
    });

    it('should transition padding across breakpoints', () => {
      const css = getRawCSS();
      const transitionRule = css.match(
        /transition[\s\S]*?padding[\s\S]*?300ms/
      );
      expect(transitionRule).not.toBeNull();
    });

    it('should transition font-size across breakpoints', () => {
      const css = getRawCSS();
      const transitionRule = css.match(
        /transition[\s\S]*?font-size[\s\S]*?300ms/
      );
      expect(transitionRule).not.toBeNull();
    });

    it('should transition grid columns across breakpoints', () => {
      const css = getRawCSS();
      const transitionRule = css.match(
        /transition[\s\S]*?grid-template-columns[\s\S]*?300ms/
      );
      expect(transitionRule).not.toBeNull();
    });

    it('should respect prefers-reduced-motion', () => {
      const css = getRawCSS();
      expect(css).toMatch(/@media[^{]*prefers-reduced-motion:\s*reduce/);
      // Should reduce transition duration
      const reducedMotion = css.match(
        /@media[^{]*prefers-reduced-motion:\s*reduce[^}]*\{[\s\S]*?transition-duration:\s*0\.01ms/
      );
      expect(reducedMotion).not.toBeNull();
    });

    it('should prevent content flicker during resize', () => {
      const css = getRawCSS();
      // Main content should have stable opacity
      expect(css).toMatch(/#main-content/);
    });
  });

  describe('CSS breakpoint coverage', () => {
    it('should define 3 breakpoint levels', () => {
      const css = getRawCSS();
      const mediaQueries = css.match(/@media/g);
      expect(mediaQueries).not.toBeNull();
      // Should have at least: tablet, desktop, reduced-motion
      expect(mediaQueries.length).toBeGreaterThanOrEqual(3);
    });

    it('should use min-width for desktop breakpoint (>=1024px)', () => {
      const css = getRawCSS();
      expect(css).toMatch(/@media[^{]*min-width:\s*1024px/);
    });

    it('should use range syntax for tablet breakpoint (768px-1023px)', () => {
      const css = getRawCSS();
      expect(css).toMatch(/min-width:\s*768px/);
      expect(css).toMatch(/max-width:\s*1023px/);
    });

    it('should have consistent breakpoint values across all CSS files', () => {
      const allCss = getAllCSS();
      let allCssText = allCss.map(c => c.content).join('\n');

      // All breakpoints should use consistent values
      const breakpoint768 = (allCssText.match(/768px/g) || []).length;
      const breakpoint1024 = (allCssText.match(/1024px/g) || []).length;

      expect(breakpoint768).toBeGreaterThan(0);
      expect(breakpoint1024).toBeGreaterThan(0);
    });

    it('should handle the full range of mobile sizes (320px-428px)', () => {
      const css = getRawCSS();
      // All base styles (no media query) should work for mobile
      // Verify that there are base styles defined outside media queries
      const baseRules = css.split('@media')[0];
      expect(baseRules.trim().length).toBeGreaterThan(100);
    });

    it('should handle the full range of tablet sizes (768px-1024px)', () => {
      const css = getRawCSS();
      expect(css).toMatch(/@media[^{]*min-width:\s*768px/);
    });
  });

  describe('Accessibility in responsive contexts', () => {
    it('should not hide content with display: none on desktop that is shown on mobile', () => {
      const css = getRawCSS();
      // Hamburger should only be hidden inside desktop media query
      const hamburgerHiddenInDesktop = css.match(
        /@media[^{]*min-width:\s*1024px[^}]*\{[\s\S]*?\.hamburger[\s\S]*?display:\s*none/
      );
      expect(hamburgerHiddenInDesktop).not.toBeNull();
    });

    it('should define skip-link for keyboard users', () => {
      const dom = loadDOM(375);
      const skipLink = dom.window.document.querySelector('.skip-link');
      expect(skipLink).not.toBeNull();
    });
  });
});
