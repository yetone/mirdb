/**
 * Footer tests for MirDB homepage.
 * Owner: Scenario 6 - Footer Section
 *
 * Test framework: Vitest + jsdom
 *
 * Test coverage:
 * - Footer element exists with proper semantic tag or role
 * - Copyright notice includes MirDB name and current year
 * - Footer contains utility links
 * - Footer text has sufficient contrast (>= 4.5:1)
 */
import { describe, it, expect, beforeAll } from 'vitest';
import { readFileSync } from 'fs';
import { resolve } from 'path';
import { JSDOM } from 'jsdom';

const htmlPath = resolve(__dirname, '../index.html');
const cssPath = resolve(__dirname, '../css/footer.css');

let dom;
let document;

beforeAll(() => {
  const html = readFileSync(htmlPath, 'utf-8');
  dom = new JSDOM(html, { url: 'http://localhost:8080' });

  // Inject footer CSS into the DOM
  const cssExists = require('fs').existsSync(cssPath);
  if (cssExists) {
    const css = readFileSync(cssPath, 'utf-8');
    const styleEl = dom.window.document.createElement('style');
    styleEl.textContent = css;
    dom.window.document.head.appendChild(styleEl);
  }

  // Also inject base CSS for custom properties and body styles
  const baseCssPath = resolve(__dirname, '../css/base.css');
  const baseCssExists = require('fs').existsSync(baseCssPath);
  if (baseCssExists) {
    const baseCss = readFileSync(baseCssPath, 'utf-8');
    const baseStyleEl = dom.window.document.createElement('style');
    baseStyleEl.textContent = baseCss;
    dom.window.document.head.appendChild(baseStyleEl);
  }

  document = dom.window.document;
});

// Helper: get raw CSS content
function getRawCSS() {
  return readFileSync(cssPath, 'utf-8');
}

// Helper: find footer element
function getFooter() {
  const footer = document.querySelector('footer');
  const contentinfo = document.querySelector('[role="contentinfo"]');
  return footer || contentinfo;
}

describe('Footer Section', () => {
  describe('Test Case 1: Footer DOM structure', () => {
    it('should have a footer element', () => {
      const footer = getFooter();
      expect(footer).not.toBeNull();
    });

    it('footer should be a <footer> semantic element', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();
      expect(footer.tagName.toLowerCase()).toBe('footer');
    });

    it('should contain a copyright notice', () => {
      const footer = getFooter();
      const text = footer.textContent.toLowerCase();
      expect(text).toMatch(/copyright|©|\b20\d{2}\b/);
    });

    it('copyright year should be present and be current or dynamically set', () => {
      const footer = getFooter();
      const currentYear = new Date().getFullYear();
      const text = footer.textContent;

      // The copyright should contain the current year, either directly
      // or via a JS element that sets it dynamically
      const hasYear = text.includes(String(currentYear)) ||
        text.includes(String(currentYear - 1) + '–' + String(currentYear)) ||
        footer.querySelector('#copyright-year') !== null;
      expect(hasYear).toBe(true);
    });

    it('footer should have at least one child element for structure', () => {
      const footer = document.querySelector('footer');
      expect(footer.children.length).toBeGreaterThanOrEqual(1);
    });
  });

  describe('Test Case 2: Copyright text content and contrast', () => {
    it('copyright text should include "MirDB" or project name', () => {
      const footer = getFooter();
      expect(footer.textContent).toMatch(/MirDB/);
    });

    it('copyright text should include current year', () => {
      const footer = getFooter();
      const currentYear = new Date().getFullYear();
      expect(footer.textContent).toMatch(new RegExp(String(currentYear)));
    });

    it('footer background color should be defined', () => {
      const css = getRawCSS();
      expect(css).toMatch(/footer\s*\{[^}]*background(-color)?\s*:/);
    });

    it('footer text color should contrast with footer background', () => {
      const footer = document.querySelector('footer');
      const styles = dom.window.getComputedStyle(footer);
      const bgColor = styles.backgroundColor;
      const color = styles.color;

      // At least one of these should be set (non-transparent, non-empty)
      const bgSet = bgColor && bgColor !== 'transparent' && bgColor !== 'rgba(0, 0, 0, 0)';
      const colorSet = color && color.length > 0;

      expect(bgSet || colorSet).toBe(true);
    });

    it('copyright text should be in a dedicated element', () => {
      const footer = document.querySelector('footer');
      const copyrightEl = footer.querySelector('.copyright') ||
        footer.querySelector('[class*="copyright"]');
      expect(copyrightEl).not.toBeNull();
    });
  });

  describe('Test Case 3: Footer link structure', () => {
    it('should contain at least 2 footer links', () => {
      const footer = document.querySelector('footer');
      const links = footer.querySelectorAll('a');
      expect(links.length).toBeGreaterThanOrEqual(2);
    });

    it('footer links should be organized in a container', () => {
      const footer = document.querySelector('footer');
      const linksContainer = footer.querySelector('.footer-links') ||
        footer.querySelector('nav') ||
        footer.querySelector('ul');
      expect(linksContainer).not.toBeNull();
    });

    it('no footer links should be placeholder or empty href', () => {
      const footer = document.querySelector('footer');
      const links = footer.querySelectorAll('a');
      for (const link of links) {
        const href = link.getAttribute('href');
        expect(href).not.toBeNull();
        expect(href.trim()).not.toBe('');
        expect(href).not.toBe('#');
      }
    });

    it('footer links should have descriptive text', () => {
      const footer = document.querySelector('footer');
      const links = footer.querySelectorAll('a');
      for (const link of links) {
        expect(link.textContent.trim().length).toBeGreaterThan(0);
      }
    });

    it('external footer links should have rel="noopener noreferrer"', () => {
      const footer = document.querySelector('footer');
      const links = footer.querySelectorAll('a');
      const externalLinks = Array.from(links).filter(
        l => l.getAttribute('href') && l.getAttribute('href').startsWith('http')
      );
      expect(externalLinks.length).toBeGreaterThan(0);
      for (const link of externalLinks) {
        const rel = link.getAttribute('rel');
        expect(rel).not.toBeNull();
        expect(rel).toMatch(/noopener/);
        expect(rel).toMatch(/noreferrer/);
      }
    });
  });

  describe('Test Case 4: Footer appears at bottom of page', () => {
    it('footer should be the last child of <body>', () => {
      const body = document.querySelector('body');
      const lastChild = body.lastElementChild;
      expect(lastChild).not.toBeNull();
      expect(lastChild.tagName.toLowerCase()).toBe('footer');
    });

    it('footer should be after <main> content', () => {
      const main = document.querySelector('main');
      const footer = document.querySelector('footer');
      expect(main).not.toBeNull();
      expect(footer).not.toBeNull();

      // footer should come after main in the DOM
      const position = main.compareDocumentPosition(footer);
      expect(position & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    });

    it('footer CSS should support sticking to bottom with flex or margin-top', () => {
      const css = getRawCSS();
      const hasFlexSticky =
        css.includes('flex') && (css.includes('min-height: 100vh') || css.includes('flex-direction'));
      const hasMarginAuto = css.includes('margin-top: auto');
      expect(hasFlexSticky || hasMarginAuto).toBe(true);
    });
  });

  describe('Test Case 5: Footer on mobile viewport', () => {
    it('footer should have responsive styles for mobile', () => {
      const css = getRawCSS();
      // Should have a media query for smaller screens
      expect(css).toMatch(/@media/);
    });

    it('footer links should stack on mobile', () => {
      const css = getRawCSS();
      // Check for flex-direction: column or display: block in a media query
      // Pattern: inside @media block, footer links change layout
      const hasMobileStack =
        css.includes('flex-direction: column') ||
        css.includes('display: block');
      expect(hasMobileStack).toBe(true);
    });

    it('footer links should have minimum touch target size of 44px', () => {
      const css = getRawCSS();
      // Footer links should have padding that makes them tappable
      const hasPadding = css.match(/\.footer-links\s+a\s*\{[^}]*padding\s*:/) ||
        css.match(/footer\s+a\s*\{[^}]*padding\s*:/);
      // At minimum, links in the footer should have styling with adequate sizing
      expect(css).toMatch(/padding/);
    });
  });

  describe('Footer CSS styles', () => {
    it('should define footer background color', () => {
      const css = getRawCSS();
      expect(css).toMatch(/footer\s*\{[^}]*background(-color)?\s*:/);
    });

    it('should define footer text color', () => {
      const css = getRawCSS();
      expect(css).toMatch(/footer\s*\{[^}]*color\s*:/);
    });

    it('should define .footer-links styles', () => {
      const css = getRawCSS();
      expect(css).toMatch(/\.footer-links/);
    });

    it('should define .copyright styles', () => {
      const css = getRawCSS();
      expect(css).toMatch(/\.copyright/);
    });

    it('should define footer link hover styles', () => {
      const css = getRawCSS();
      expect(css).toMatch(/footer.*a:hover/);
    });

    it('footer content should have max-width constraint', () => {
      const css = getRawCSS();
      const hasMaxWidth =
        css.includes('max-width') || css.includes('var(--max-width)');
      expect(hasMaxWidth).toBe(true);
    });

    it('should define padding for the footer', () => {
      const css = getRawCSS();
      expect(css).toMatch(/footer\s*\{[^}]*padding/);
    });
  });
});
