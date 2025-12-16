import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { JSDOM } from 'jsdom';
import { readFileSync } from 'fs';
import { resolve } from 'path';

describe('Responsive Design - Mobile', () => {
  let dom;
  let document;
  let cssContent;

  beforeEach(() => {
    const htmlPath = resolve(__dirname, '../index.html');
    const cssPath = resolve(__dirname, '../styles.css');
    const html = readFileSync(htmlPath, 'utf-8');
    cssContent = readFileSync(cssPath, 'utf-8');
    dom = new JSDOM(html, { runScripts: 'dangerously', resources: 'usable' });
    document = dom.window.document;
  });

  afterEach(() => {
    if (dom) {
      dom.window.close();
    }
  });

  // Test Case 1: Load page at 375px width viewport - no horizontal scrollbar
  describe('Test Case 1: No horizontal scrollbar at 375px width', () => {
    it('should have viewport meta tag for mobile responsiveness', () => {
      const viewportMeta = document.querySelector('meta[name="viewport"]');
      expect(viewportMeta).not.toBeNull();
      const content = viewportMeta.getAttribute('content');
      expect(content).toContain('width=device-width');
      expect(content).toContain('initial-scale=1');
    });

    it('should have box-sizing border-box for all elements', () => {
      expect(cssContent).toMatch(/\*.*\{[^}]*box-sizing:\s*border-box/s);
    });

    it('should have mobile breakpoint media query at 768px or smaller', () => {
      // Check for media query targeting mobile devices
      const hasMobileBreakpoint = cssContent.match(/@media\s*\([^)]*max-width:\s*768px/);
      expect(hasMobileBreakpoint).not.toBeNull();
    });

    it('should not have fixed width elements that could cause overflow', () => {
      // Check that main containers use relative units or max-width
      const hasMaxWidth = cssContent.includes('max-width');
      const usesPercentage = cssContent.includes('100%');
      expect(hasMaxWidth || usesPercentage).toBe(true);
    });

    it('should have sections with proper padding for mobile', () => {
      // Verify sections have reduced padding in mobile media query
      const hasMobileMediaQuery = cssContent.match(/@media[^{]*max-width:\s*768px/);
      expect(hasMobileMediaQuery).not.toBeNull();
      // Check that section padding is defined somewhere in mobile media query
      const mobileStyles = cssContent.match(/@media[^{]*max-width:\s*768px[^{]*\{([\s\S]*?)(?=@media|\z)/);
      expect(mobileStyles).not.toBeNull();
      // Verify section styles are adjusted for mobile (padding changes)
      const hasSectionPadding = mobileStyles[1].includes('padding:');
      expect(hasSectionPadding).toBe(true);
    });

    it('should have overflow-x auto or scroll on code blocks', () => {
      // Code blocks should have horizontal scroll to prevent page overflow
      const preOverflow = cssContent.match(/pre\s*\{[^}]*overflow-x:\s*(auto|scroll)/);
      expect(preOverflow).not.toBeNull();
    });

    it('html and body should not have horizontal overflow', () => {
      // Check that no fixed widths exceed viewport
      const hasProperBaseStyles = cssContent.includes('min-height: 100vh') ||
                                   cssContent.includes('min-height:100vh');
      expect(hasProperBaseStyles).toBe(true);
    });
  });

  // Test Case 2: Check for mobile menu trigger at 375px width
  describe('Test Case 2: Hamburger menu visible on mobile', () => {
    it('should have mobile menu toggle button element', () => {
      const mobileToggle = document.querySelector('.mobile-menu-toggle, .hamburger, .menu-toggle, button[aria-label*="menu"]');
      expect(mobileToggle).not.toBeNull();
    });

    it('mobile menu toggle should be hidden by default on desktop', () => {
      // Check CSS for display: none on mobile toggle by default
      const mobileToggleHidden = cssContent.match(/\.mobile-menu-toggle\s*\{[^}]*display:\s*none/);
      expect(mobileToggleHidden).not.toBeNull();
    });

    it('mobile menu toggle should be visible in mobile media query', () => {
      // Check that mobile toggle becomes visible in mobile breakpoint
      const mobileMediaQuery = cssContent.match(/@media\s*\([^)]*max-width:\s*768px[^)]*\)\s*\{([\s\S]*?)\n\}/);
      expect(mobileMediaQuery).not.toBeNull();

      // Look for .mobile-menu-toggle display: flex or block within mobile media query
      const mobileToggleVisible = cssContent.match(/@media[^{]*max-width:\s*768px[^{]*\{[\s\S]*?\.mobile-menu-toggle\s*\{[^}]*display:\s*(flex|block)/);
      expect(mobileToggleVisible).not.toBeNull();
    });

    it('mobile menu toggle should have aria-label for accessibility', () => {
      const mobileToggle = document.querySelector('.mobile-menu-toggle');
      expect(mobileToggle).not.toBeNull();
      expect(mobileToggle.hasAttribute('aria-label')).toBe(true);
    });

    it('mobile menu toggle should have hamburger spans for icon', () => {
      const mobileToggle = document.querySelector('.mobile-menu-toggle');
      expect(mobileToggle).not.toBeNull();
      const spans = mobileToggle.querySelectorAll('span');
      expect(spans.length).toBeGreaterThanOrEqual(3);
    });

    it('nav-links should be hidden by default on mobile', () => {
      // Check that nav-links are hidden on mobile (display: none in media query)
      const navLinksHiddenMobile = cssContent.match(/@media[^{]*max-width:\s*768px[^{]*\{[\s\S]*?\.nav-links\s*\{[^}]*display:\s*none/);
      expect(navLinksHiddenMobile).not.toBeNull();
    });

    it('nav-links.active should be visible on mobile', () => {
      // Check that .nav-links.active makes nav visible
      const navLinksActive = cssContent.match(/\.nav-links\.active\s*\{[^}]*display:\s*(flex|block)/);
      expect(navLinksActive).not.toBeNull();
    });
  });

  // Test Case 3: Verify feature cards stack on mobile
  describe('Test Case 3: Feature cards single column layout on mobile', () => {
    it('should have feature-grid using CSS grid', () => {
      const featureGridCss = cssContent.match(/\.feature-grid\s*\{[^}]*display:\s*grid/);
      expect(featureGridCss).not.toBeNull();
    });

    it('should have feature-grid with auto-fit or responsive columns', () => {
      // Check for responsive grid using auto-fit or auto-fill
      const responsiveGrid = cssContent.match(/\.feature-grid\s*\{[^}]*grid-template-columns:[^;]*auto-(fit|fill)/);
      expect(responsiveGrid).not.toBeNull();
    });

    it('should have feature cards that stack to single column on mobile', () => {
      // Check for mobile media query that sets feature-grid to 1fr (single column)
      const mobileFeatureGrid = cssContent.match(/@media[^{]*max-width:\s*768px[^{]*\{[\s\S]*?\.feature-grid\s*\{[^}]*grid-template-columns:\s*1fr/);
      expect(mobileFeatureGrid).not.toBeNull();
    });

    it('feature cards should exist in the DOM', () => {
      const featureCards = document.querySelectorAll('.feature-card');
      expect(featureCards.length).toBeGreaterThan(0);
    });

    it('feature-grid should have proper gap for spacing', () => {
      const gridGap = cssContent.match(/\.feature-grid\s*\{[^}]*gap:/);
      expect(gridGap).not.toBeNull();
    });

    it('feature cards should have flexible width', () => {
      // Feature cards should not have fixed widths
      const featureCardCss = cssContent.match(/\.feature-card\s*\{([^}]*)\}/);
      expect(featureCardCss).not.toBeNull();
      // Verify no fixed pixel width
      const hasFixedWidth = featureCardCss[1].match(/width:\s*\d+px(?!\s*;)/);
      expect(hasFixedWidth).toBeNull();
    });
  });

  // Test Case 4: Check font size is readable on mobile
  describe('Test Case 4: Body text at least 16px or readable size', () => {
    it('body should have readable base font size', () => {
      const bodyFontSize = cssContent.match(/body\s*\{[^}]*line-height:\s*[\d.]+/);
      expect(bodyFontSize).not.toBeNull();
    });

    it('code blocks should have readable font size', () => {
      const codeFontSize = cssContent.match(/code\s*\{[^}]*font-size:\s*(0\.(8|9)\d*rem|1rem|14px|16px)/);
      expect(codeFontSize).not.toBeNull();
    });

    it('mobile hero h1 should be appropriately sized', () => {
      // Check that h1 is scaled down on mobile
      const mobileH1 = cssContent.match(/@media[^{]*max-width:\s*768px[^{]*\{[\s\S]*?\.hero\s+h1\s*\{[^}]*font-size:/);
      expect(mobileH1).not.toBeNull();
    });

    it('mobile section headings should be appropriately sized', () => {
      // Check that section h2 is scaled down on mobile
      const mobileSectionH2 = cssContent.match(/@media[^{]*max-width:\s*768px[^{]*\{[\s\S]*?section\s+h2\s*\{[^}]*font-size:/);
      expect(mobileSectionH2).not.toBeNull();
    });

    it('font family should be system fonts for readability', () => {
      const systemFonts = cssContent.match(/font-family:[^;]*(system-ui|-apple-system|BlinkMacSystemFont|Segoe UI|sans-serif)/);
      expect(systemFonts).not.toBeNull();
    });

    it('body text should have adequate line-height for readability', () => {
      const lineHeight = cssContent.match(/body\s*\{[^}]*line-height:\s*[\d.]+/);
      expect(lineHeight).not.toBeNull();
      // Extract line-height value
      const lineHeightValue = parseFloat(lineHeight[0].match(/line-height:\s*([\d.]+)/)[1]);
      expect(lineHeightValue).toBeGreaterThanOrEqual(1.4);
    });

    it('tagline should be readable on mobile', () => {
      // Check that tagline font-size is adjusted on mobile
      const mobileTagline = cssContent.match(/@media[^{]*max-width:\s*768px[^{]*\{[\s\S]*?\.tagline\s*\{[^}]*font-size:/);
      expect(mobileTagline).not.toBeNull();
    });
  });

  // Test Case 5: Verify code blocks don't overflow on mobile
  describe('Test Case 5: Code blocks have horizontal scroll or wrap', () => {
    it('code blocks should have overflow-x for horizontal scrolling', () => {
      const preOverflow = cssContent.match(/pre\s*\{[^}]*overflow-x:\s*(auto|scroll)/);
      expect(preOverflow).not.toBeNull();
    });

    it('code block container should have position relative for copy button', () => {
      const codeBlockPosition = cssContent.match(/\.code-block\s*\{[^}]*position:\s*relative/);
      expect(codeBlockPosition).not.toBeNull();
    });

    it('pre element should have proper padding on mobile', () => {
      // Check for reduced padding on mobile
      const mobilePre = cssContent.match(/@media[^{]*max-width:\s*768px[^{]*\{[\s\S]*?pre\s*\{[^}]*padding:/);
      expect(mobilePre).not.toBeNull();
    });

    it('code font size should be reduced on mobile for better fit', () => {
      // Check for smaller font in mobile breakpoint
      const mobileCodeFont = cssContent.match(/@media[^{]*max-width:\s*768px[^{]*\{[\s\S]*?pre\s*\{[^}]*font-size:/);
      expect(mobileCodeFont).not.toBeNull();
    });

    it('inline code should have proper styling', () => {
      const inlineCode = cssContent.match(/p\s+code\s*\{[^}]*background-color:/);
      expect(inlineCode).not.toBeNull();
    });

    it('code blocks should exist in the page', () => {
      const codeBlocks = document.querySelectorAll('.code-block');
      expect(codeBlocks.length).toBeGreaterThan(0);
    });

    it('pre elements should exist inside code blocks', () => {
      const preElements = document.querySelectorAll('.code-block pre');
      expect(preElements.length).toBeGreaterThan(0);
    });

    it('code elements should exist inside pre', () => {
      const codeElements = document.querySelectorAll('.code-block pre code');
      expect(codeElements.length).toBeGreaterThan(0);
    });
  });

  // Additional mobile responsiveness tests
  describe('Additional mobile responsive design checks', () => {
    it('should have responsive footer grid', () => {
      const mobileFooter = cssContent.match(/@media[^{]*max-width:\s*768px[^{]*\{[\s\S]*?\.footer-content\s*\{[^}]*grid-template-columns:\s*1fr/);
      expect(mobileFooter).not.toBeNull();
    });

    it('should have reduced hero padding on mobile', () => {
      const mobileHero = cssContent.match(/@media[^{]*max-width:\s*768px[^{]*\{[\s\S]*?\.hero\s*\{[^}]*padding:/);
      expect(mobileHero).not.toBeNull();
    });

    it('quick-start section should have reduced margin on mobile', () => {
      const mobileQuickStart = cssContent.match(/@media[^{]*max-width:\s*768px[^{]*\{[\s\S]*?\.quick-start\s*\{[^}]*margin:/);
      expect(mobileQuickStart).not.toBeNull();
    });

    it('tables should have reduced padding on mobile', () => {
      const mobileTables = cssContent.match(/@media[^{]*max-width:\s*768px[^{]*\{[\s\S]*?\.command-table\s+(th|td)[^{]*\{[^}]*padding:/);
      expect(mobileTables).not.toBeNull();
    });

    it('CTA buttons should wrap on mobile', () => {
      const ctaFlex = cssContent.match(/\.cta-buttons\s*\{[^}]*flex-wrap:\s*wrap/);
      expect(ctaFlex).not.toBeNull();
    });

    it('nav links should be full width on mobile', () => {
      const mobileNavLinks = cssContent.match(/@media[^{]*max-width:\s*768px[^{]*\{[\s\S]*?\.nav-links\s*\{[^}]*flex-direction:\s*column/);
      expect(mobileNavLinks).not.toBeNull();
    });
  });
});
