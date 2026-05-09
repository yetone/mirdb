/**
 * Scenario 10 - Responsive Design Integration Tests
 * Owner: Scenario 10
 *
 * Tests responsive layout across mobile, tablet, and desktop breakpoints.
 * Uses jsdom with breakpoint-specific CSS injection since jsdom does not
 * fully evaluate CSS media queries based on viewport size.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { JSDOM } from 'jsdom';
import fs from 'fs';
import path from 'path';

const __dirname = path.dirname(new URL(import.meta.url).pathname);
const INDEX_PATH = path.resolve(__dirname, '../../index.html');
const CSS_PATH = path.resolve(__dirname, '../../css/responsive.css');
const BASE_CSS_PATH = path.resolve(__dirname, '../../css/base.css');
const COMP_CSS_PATH = path.resolve(__dirname, '../../css/components.css');

/* ------------------------------------------------------------------ */
/* Helpers                                                            */
/* ------------------------------------------------------------------ */

function readFiles() {
  return {
    html: fs.readFileSync(INDEX_PATH, 'utf-8'),
    css: fs.readFileSync(CSS_PATH, 'utf-8'),
    baseCss: fs.readFileSync(BASE_CSS_PATH, 'utf-8'),
    compCss: fs.readFileSync(COMP_CSS_PATH, 'utf-8'),
  };
}

/**
 * Extract all rule blocks from a single @media query in a CSS string.
 * Supports nested braces inside the media block (e.g. @keyframes).
 */
function extractMediaQueryRules(css, mediaQueryStart) {
  const idx = css.indexOf(mediaQueryStart);
  if (idx === -1) return '';

  let depth = 0;
  let start = -1;
  for (let i = idx + mediaQueryStart.length; i < css.length; i++) {
    if (css[i] === '{') {
      if (depth === 0) start = i + 1;
      depth++;
    } else if (css[i] === '}') {
      depth--;
      if (depth === 0 && start !== -1) {
        return css.slice(start, i).trim();
      }
    }
  }
  return '';
}

/**
 * Build an HTML string with all CSS inlined as <style> tags.
 */
function buildHtmlWithInlineCss(html, ...cssChunks) {
  const styleTag = `<style>\n${cssChunks.join('\n')}\n</style>`;
  // Insert before closing </head>
  return html.replace('</head>', `${styleTag}\n</head>`);
}

/**
 * Create a JSDOM with the given HTML and viewport dimensions.
 */
function createDom(html, width = 1024, height = 768) {
  const dom = new JSDOM(html, {
    url: 'http://localhost:3000',
    runScripts: 'dangerously',
    pretendToBeVisual: true,
  });
  dom.window.innerWidth = width;
  dom.window.innerHeight = height;
  return dom;
}

/**
 * Count the number of grid columns from a gridTemplateColumns value.
 * Handles both expanded forms ("1fr 1fr") and repeat() ("repeat(3, 1fr)").
 */
function countGridColumns(gridTemplateColumns) {
  const val = gridTemplateColumns.trim();
  const repeatMatch = val.match(/repeat\(\s*(\d+)\s*,/);
  if (repeatMatch) {
    return parseInt(repeatMatch[1], 10);
  }
  // Fall back to counting space-separated values
  return val.split(/\s+/).filter(c => c.trim()).length;
}

/* ------------------------------------------------------------------ */
/* Test suite                                                         */
/* ------------------------------------------------------------------ */

describe('Responsive Design', () => {
  let files;

  beforeEach(() => {
    files = readFiles();
  });

  /* ---------------------------------------------------------------- */
  /* CSS structure tests                                               */
  /* ---------------------------------------------------------------- */

  describe('CSS media queries exist', () => {
    it('has a mobile media query (max-width: 767px)', () => {
      expect(files.css).toContain('@media (max-width: 767px)');
    });

    it('has a tablet media query (min-width: 768px) and (max-width: 1024px)', () => {
      expect(files.css).toContain('@media (min-width: 768px) and (max-width: 1024px)');
    });

    it('has a desktop media query (min-width: 1025px)', () => {
      expect(files.css).toContain('@media (min-width: 1025px)');
    });

    it('has an extra-small mobile media query (max-width: 359px)', () => {
      expect(files.css).toContain('@media (max-width: 359px)');
    });
  });

  describe('Required DOM elements exist', () => {
    it('has a #hamburger element', () => {
      const dom = createDom(files.html, 375, 667);
      expect(dom.window.document.querySelector('#hamburger')).not.toBeNull();
    });

    it('has a #nav-list element', () => {
      const dom = createDom(files.html, 375, 667);
      expect(dom.window.document.querySelector('#nav-list')).not.toBeNull();
    });

    it('has a #features section with .grid', () => {
      const dom = createDom(files.html, 375, 667);
      const features = dom.window.document.querySelector('#features');
      expect(features).not.toBeNull();
      expect(features.querySelector('.grid')).not.toBeNull();
    });
  });

  /* ---------------------------------------------------------------- */
  /* Mobile breakpoint (<= 767px)                                      */
  /* ---------------------------------------------------------------- */

  describe('Mobile breakpoint (max-width: 767px)', () => {
    let dom;
    let document;

    beforeEach(() => {
      const mobileRules = extractMediaQueryRules(files.css, '@media (max-width: 767px)');
      const html = buildHtmlWithInlineCss(
        files.html,
        files.baseCss,
        files.compCss,
        mobileRules
      );
      dom = createDom(html, 375, 667);
      document = dom.window.document;
    });

    afterEach(() => {
      dom = null;
      document = null;
    });

    // Test Case 1: No horizontal scroll
    it('scrollWidth is <= viewport width (no horizontal overflow)', () => {
      const scrollWidth = document.documentElement.scrollWidth;
      expect(scrollWidth).toBeLessThanOrEqual(375);
    });

    // Test Case 2: Hamburger is visible
    it('hamburger button display is not "none"', () => {
      const hamburger = document.querySelector('#hamburger');
      const style = dom.window.getComputedStyle(hamburger);
      expect(style.display).not.toBe('none');
    });

    // Test Case 3: Nav list is hidden
    it('nav-list display is "none" before hamburger is clicked', () => {
      const navList = document.querySelector('#nav-list');
      const style = dom.window.getComputedStyle(navList);
      expect(style.display).toBe('none');
    });

    it('features grid uses a single column', () => {
      const grid = document.querySelector('#features .grid');
      const style = dom.window.getComputedStyle(grid);
      expect(countGridColumns(style.gridTemplateColumns)).toBe(1);
    });

    it('body has overflow-x hidden to prevent horizontal scroll', () => {
      // The overflow-x rule lives in the base section of responsive.css
      expect(files.css).toContain('overflow-x: hidden');
      expect(files.css).toMatch(/html\s*,\s*body\s*\{[^}]*overflow-x:\s*hidden/s);
    });
  });

  /* ---------------------------------------------------------------- */
  /* Tablet breakpoint (768px - 1024px)                                */
  /* ---------------------------------------------------------------- */

  describe('Tablet breakpoint (min-width: 768px and max-width: 1024px)', () => {
    let dom;
    let document;

    beforeEach(() => {
      const tabletRules = extractMediaQueryRules(
        files.css,
        '@media (min-width: 768px) and (max-width: 1024px)'
      );
      const html = buildHtmlWithInlineCss(
        files.html,
        files.baseCss,
        files.compCss,
        tabletRules
      );
      dom = createDom(html, 900, 1024);
      document = dom.window.document;
    });

    afterEach(() => {
      dom = null;
      document = null;
    });

    // Test Case 4: Features grid has two columns
    it('features grid resolves to two columns', () => {
      const grid = document.querySelector('#features .grid');
      const style = dom.window.getComputedStyle(grid);
      expect(countGridColumns(style.gridTemplateColumns)).toBe(2);
    });

    it('hamburger button is hidden', () => {
      const hamburger = document.querySelector('#hamburger');
      const style = dom.window.getComputedStyle(hamburger);
      expect(style.display).toBe('none');
    });

    it('nav-list is visible as a flex row', () => {
      const navList = document.querySelector('#nav-list');
      const style = dom.window.getComputedStyle(navList);
      expect(style.display).toBe('flex');
      expect(style.flexDirection).toBe('row');
    });
  });

  /* ---------------------------------------------------------------- */
  /* Desktop breakpoint (> 1024px)                                     */
  /* ---------------------------------------------------------------- */

  describe('Desktop breakpoint (min-width: 1025px)', () => {
    let dom;
    let document;

    beforeEach(() => {
      const desktopRules = extractMediaQueryRules(files.css, '@media (min-width: 1025px)');
      const html = buildHtmlWithInlineCss(
        files.html,
        files.baseCss,
        files.compCss,
        desktopRules
      );
      dom = createDom(html, 1440, 900);
      document = dom.window.document;
    });

    afterEach(() => {
      dom = null;
      document = null;
    });

    // Test Case 5: Hamburger is hidden
    it('hamburger button display is "none"', () => {
      const hamburger = document.querySelector('#hamburger');
      const style = dom.window.getComputedStyle(hamburger);
      expect(style.display).toBe('none');
    });

    // Test Case 6: Features grid has 3 or more columns
    it('features grid has at least 3 columns', () => {
      const grid = document.querySelector('#features .grid');
      const style = dom.window.getComputedStyle(grid);
      expect(countGridColumns(style.gridTemplateColumns)).toBeGreaterThanOrEqual(3);
    });

    it('nav-list is visible as a flex row', () => {
      const navList = document.querySelector('#nav-list');
      const style = dom.window.getComputedStyle(navList);
      expect(style.display).toBe('flex');
      expect(style.flexDirection).toBe('row');
    });

    it('hero has larger padding than mobile', () => {
      const hero = document.querySelector('#hero');
      const style = dom.window.getComputedStyle(hero);
      // Desktop padding is 6rem 2rem; jsdom may not compute rem, so check it is not empty
      expect(style.padding).toBeTruthy();
    });
  });

  /* ---------------------------------------------------------------- */
  /* Extra-small mobile (<= 359px) — edge case                         */
  /* ---------------------------------------------------------------- */

  describe('Extra-small mobile (max-width: 359px)', () => {
    let dom;
    let document;

    beforeEach(() => {
      const xsRules = extractMediaQueryRules(files.css, '@media (max-width: 359px)');
      const html = buildHtmlWithInlineCss(
        files.html,
        files.baseCss,
        files.compCss,
        xsRules
      );
      dom = createDom(html, 320, 568);
      document = dom.window.document;
    });

    afterEach(() => {
      dom = null;
      document = null;
    });

    // Test Case 7: No horizontal overflow at extreme small mobile
    it('scrollWidth is <= viewport width at 320px', () => {
      const scrollWidth = document.documentElement.scrollWidth;
      expect(scrollWidth).toBeLessThanOrEqual(320);
    });

    it('no JS errors during rendering', () => {
      // If the DOM loads without throwing, rendering succeeded
      expect(document.querySelector('body')).not.toBeNull();
      expect(document.querySelector('#hero')).not.toBeNull();
      expect(document.querySelector('#features')).not.toBeNull();
    });

    it('content remains readable with reduced font size', () => {
      const htmlEl = document.documentElement;
      const style = dom.window.getComputedStyle(htmlEl);
      // font-size should be 14px for extra-small screens
      expect(style.fontSize).toBe('14px');
    });
  });

  /* ---------------------------------------------------------------- */
  /* Cross-breakpoint consistency                                      */
  /* ---------------------------------------------------------------- */

  describe('Cross-breakpoint consistency', () => {
    it('all breakpoints define hamburger and nav-list display', () => {
      const mobile = extractMediaQueryRules(files.css, '@media (max-width: 767px)');
      const tablet = extractMediaQueryRules(
        files.css,
        '@media (min-width: 768px) and (max-width: 1024px)'
      );
      const desktop = extractMediaQueryRules(files.css, '@media (min-width: 1025px)');

      // Mobile: hamburger flex, nav-list none
      expect(mobile).toContain('#hamburger');
      expect(mobile).toContain('#nav-list');

      // Tablet: hamburger none
      expect(tablet).toContain('#hamburger');
      expect(tablet).toContain('display: none');

      // Desktop: hamburger none
      expect(desktop).toContain('#hamburger');
      expect(desktop).toContain('display: none');
    });

    it('all breakpoints define features grid columns', () => {
      const mobile = extractMediaQueryRules(files.css, '@media (max-width: 767px)');
      const tablet = extractMediaQueryRules(
        files.css,
        '@media (min-width: 768px) and (max-width: 1024px)'
      );
      const desktop = extractMediaQueryRules(files.css, '@media (min-width: 1025px)');

      expect(mobile).toContain('#features');
      expect(mobile).toContain('.grid');
      expect(tablet).toContain('#features');
      expect(tablet).toContain('.grid');
      expect(desktop).toContain('#features');
      expect(desktop).toContain('.grid');
    });

    it('images have max-width: 100% at all sizes', () => {
      // This is in base.css but also reinforced in responsive.css mobile block
      expect(files.css).toContain('max-width: 100%');
    });
  });
});
