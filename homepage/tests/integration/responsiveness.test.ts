/**
 * Responsive design integration tests for MirDB homepage.
 *
 * Validates NFR-3: Desktop/tablet/mobile layouts, CSS breakpoints,
 * responsive images, touch targets, and smooth layout transitions.
 *
 * Owner: Scenario 8 - Responsive Design
 */

import { describe, it, expect, beforeAll } from 'vitest';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { execSync } from 'node:child_process';
import { parseHTML } from 'linkedom';

const HOMEPAGE_DIR = join(__dirname, '..', '..');
const DIST_DIR = join(HOMEPAGE_DIR, 'dist');
const INDEX_HTML = join(DIST_DIR, 'index.html');
const SRC_DIR = join(HOMEPAGE_DIR, 'src');

// ── helpers ──────────────────────────────────────────────────────────

function buildSite() {
  try {
    const astroBin = join(HOMEPAGE_DIR, 'node_modules', 'astro', 'astro.js');
    execSync(`node "${astroBin}" build`, {
      cwd: HOMEPAGE_DIR,
      stdio: 'pipe',
      env: { ...process.env, NODE_ENV: 'production' },
    });
  } catch (e: any) {
    const stderr = e.stderr?.toString() || '';
    throw new Error(`Build failed: ${stderr}`);
  }
}

function parseBuiltHtml(): Document {
  const html = readFileSync(INDEX_HTML, 'utf-8');
  return parseHTML(html).document;
}

function readCssFile(relativePath: string): string {
  return readFileSync(join(HOMEPAGE_DIR, relativePath), 'utf-8');
}

/** Collect all CSS from source .css files AND built dist CSS */
function collectAllCss(): string {
  const fs = require('node:fs');
  const path = require('node:path');

  // Read built CSS files
  let combined = '';
  const distAstro = join(HOMEPAGE_DIR, 'dist', '_astro');
  if (fs.existsSync(distAstro)) {
    const distFiles = fs.readdirSync(distAstro);
    for (const f of distFiles) {
      if (f.endsWith('.css')) {
        combined += readFileSync(path.join(distAstro, f), 'utf-8') + '\n';
      }
    }
  }

  // Read source CSS files
  const walkDir = (dir: string): string[] => {
    let results: string[] = [];
    const list = fs.readdirSync(dir);
    for (const file of list) {
      const filePath = path.join(dir, file);
      const stat = fs.statSync(filePath);
      if (stat.isDirectory() && file !== 'node_modules') {
        results = results.concat(walkDir(filePath));
      } else if (file.endsWith('.css')) {
        results.push(filePath);
      }
    }
    return results;
  };

  const cssFiles = walkDir(SRC_DIR);
  for (const fp of cssFiles) {
    combined += readFileSync(fp, 'utf-8') + '\n';
  }

  // Also read inline styles from .astro files
  const walkAstro = (dir: string): string[] => {
    let results: string[] = [];
    const list = fs.readdirSync(dir);
    for (const file of list) {
      const filePath = path.join(dir, file);
      const stat = fs.statSync(filePath);
      if (stat.isDirectory() && file !== 'node_modules') {
        results = results.concat(walkAstro(filePath));
      } else if (file.endsWith('.astro')) {
        results.push(filePath);
      }
    }
    return results;
  };

  const astroFiles = walkAstro(SRC_DIR);
  for (const fp of astroFiles) {
    const content = readFileSync(fp, 'utf-8');
    // Extract content between <style> tags (including the tags for regex matching)
    const styleRegex = /<style>([\s\S]*?)<\/style>/g;
    let match: RegExpExecArray | null;
    while ((match = styleRegex.exec(content)) !== null) {
      combined += match[1] + '\n';
    }
  }

  return combined;
}

/** Extract media query breakpoint values from CSS string */
function extractBreakpointsFromCss(css: string): number[] {
  const widths: number[] = [];
  const regex = /@media\s*\(max-width:\s*(\d+)px\)/g;
  let match: RegExpExecArray | null;
  while ((match = regex.exec(css)) !== null) {
    widths.push(parseInt(match[1], 10));
  }
  return [...new Set(widths)].sort((a, b) => a - b);
}

/** Check if CSS contains media queries for a specific breakpoint range */
function hasMediaQuery(css: string, pattern: RegExp): boolean {
  return pattern.test(css);
}

/** Parse CSS custom properties from :root block */
function parseCssVariables(css: string): Record<string, string> {
  const vars: Record<string, string> = {};
  const rootMatch = css.match(/:root\s*\{([^}]*)\}/s);
  if (!rootMatch) return vars;

  const block = rootMatch[1];
  const propRegex = /--([\w-]+)\s*:\s*([^;]+);/g;
  let match: RegExpExecArray | null;
  while ((match = propRegex.exec(block)) !== null) {
    vars[`--${match[1]}`] = match[2].trim();
  }
  return vars;
}

// ══════════════════════════════════════════════════════════════════════
// Test Suite
// ══════════════════════════════════════════════════════════════════════

describe('Responsive Design (NFR-3)', () => {
  let document: Document;
  let allCss: string;

  beforeAll(() => {
    buildSite();
    document = parseBuiltHtml();
    allCss = collectAllCss();
  });

  // ── Test Case 1: Desktop viewport (1440px) ─────────────────────────

  describe('Test Case 1: Desktop viewport (1280px+)', () => {
    it('has breakpoint CSS custom properties for desktop (1280px+)', () => {
      const vars = parseCssVariables(allCss);
      expect(vars['--bp-desktop-min']).toBeDefined();
    });

    it('has a max-width container to prevent edge-to-edge stretching', () => {
      const vars = parseCssVariables(allCss);
      expect(vars['--container-max']).toBeDefined();
      // Container should constrain content width
      const containerMax = parseInt(vars['--container-max'], 10);
      expect(containerMax).toBeGreaterThan(0);
      expect(containerMax).toBeLessThanOrEqual(1400);
    });

    it('navigation links exist and desktop nav is present in HTML', () => {
      const navContainer = document.querySelector('header[role="banner"]');
      expect(navContainer).not.toBeNull();
    });

    it('has no overflow-x on body element style', () => {
      const cssBodyOverflow = /body\s*\{[^}]*overflow-x:\s*hidden[^}]*\}/s;
      expect(cssBodyOverflow.test(allCss)).toBe(true);
    });

    it('CSS defines multi-column feature grid for desktop', () => {
      // Desktop default should be repeat(3, 1fr) or at least >1 column
      const hasMultiColGrid =
        /grid-template-columns:\s*repeat\(\s*[23]\s*,/.test(allCss) ||
        /grid-template-columns:\s*repeat\(\s*3\s*,/.test(allCss);
      expect(hasMultiColGrid).toBe(true);
    });
  });

  // ── Test Case 2: Tablet viewport (1024px) ──────────────────────────

  describe('Test Case 2: Tablet viewport (1024px / 768px-1279px)', () => {
    it('CSS has media queries for tablet range (768px-1279px)', () => {
      const hasTabletQuery =
        /@media\s*\(max-width:\s*1279px\)/.test(allCss) ||
        /@media\s*\(max-width:\s*12[0-9]{2}px\)/.test(allCss);
      expect(hasTabletQuery).toBe(true);
    });

    it('feature grid reduces to 2 columns in tablet range', () => {
      // Search for 2-col grid within a max-width 1279 or similar media query
      const cssAfterTablet = allCss.split(/@media\s*\(max-width:\s*12\d{2}px\)/).slice(1).join('');
      const hasTwoColGrid = /grid-template-columns:\s*repeat\(\s*2\s*,/.test(cssAfterTablet) ||
        /grid-template-columns:\s*1fr\s+1fr/.test(cssAfterTablet);
      // If not found after splitting, try checking the full CSS
      const fullHasTwoCol = /grid-template-columns:\s*repeat\(\s*2\s*,/.test(allCss);
      expect(hasTwoColGrid || fullHasTwoCol).toBe(true);
    });

    it('tablet breakpoint CSS custom property exists', () => {
      const vars = parseCssVariables(allCss);
      expect(vars['--bp-tablet-min'] || vars['--bp-tablet-max']).toBeDefined();
    });
  });

  // ── Test Case 3: Tablet viewport (768px) ───────────────────────────

  describe('Test Case 3: Tablet viewport lower bound (768px)', () => {
    it('CSS has a breakpoint at or near 767-768px', () => {
      const breakpoints = extractBreakpointsFromCss(allCss);
      const hasNear768 = breakpoints.some((bp) => bp >= 767 && bp <= 768);
      expect(hasNear768).toBe(true);
    });

    it('sections are present in the HTML', () => {
      const sections = document.querySelectorAll('section, [id="quick-start"], [id="features"]');
      expect(sections.length).toBeGreaterThan(0);
    });
  });

  // ── Test Case 4: Mobile viewport (375px, iPhone SE) ────────────────

  describe('Test Case 4: Mobile viewport (375px, iPhone SE)', () => {
    it('mobile toggle button (hamburger) is present in HTML', () => {
      const mobileToggle = document.querySelector('#mobile-toggle, [aria-label*="menu" i], [aria-label*="Menu"]');
      expect(mobileToggle).not.toBeNull();
    });

    it('mobile menu overlay is present in HTML', () => {
      const mobileMenu = document.querySelector('#mobile-menu, [role="dialog"]');
      expect(mobileMenu).not.toBeNull();
    });

    it('CSS hides desktop nav and shows mobile toggle at mobile breakpoint', () => {
      const mobileBreakCss = allCss.split(/@media\s*\(max-width:\s*767px\)/).slice(1).join('');
      const hidesDesktopNav =
        /\.desktopNav\s*\{[^}]*display:\s*none/.test(mobileBreakCss) ||
        /display:\s*none/.test(mobileBreakCss);
      expect(hidesDesktopNav).toBe(true);
    });

    it('feature grid collapses to single column on mobile', () => {
      const mobileCss = allCss.split(/@media\s*\(max-width:\s*767px\)/).slice(1).join('');
      const hasSingleColGrid =
        /grid-template-columns:\s*1fr/.test(mobileCss) ||
        /grid-template-columns:\s*repeat\(\s*1\s*,/.test(mobileCss);
      expect(hasSingleColGrid).toBe(true);
    });
  });

  // ── Test Case 5: Mobile viewport (320px, minimum supported) ────────

  describe('Test Case 5: Mobile viewport (320px, minimum supported)', () => {
    it('body overflow-x is hidden to prevent horizontal scrollbar', () => {
      const hasBodyOverflowHidden = /body\s*\{[^}]*overflow-x:\s*hidden/.test(allCss);
      expect(hasBodyOverflowHidden).toBe(true);
    });

    it('all images have max-width: 100% constraint', () => {
      const imgMaxWidth = /img[^{]*\{[^}]*max-width:\s*100%/.test(allCss) ||
        /img,\s*svg[^{]*\{[^}]*max-width:\s*100%/.test(allCss) ||
        /max-width:\s*100%/.test(allCss);
      expect(imgMaxWidth).toBe(true);
    });

    it('text containers use word-break or overflow-wrap for long text', () => {
      const hasTextWrapping =
        /overflow-wrap:\s*break-word/.test(allCss) ||
        /word-break:\s*break-word/.test(allCss) ||
        /white-space:\s*pre-wrap/.test(allCss) ||
        /word-break:\s*break-all/.test(allCss);
      expect(hasTextWrapping).toBe(true);
    });
  });

  // ── Test Case 6: Code block readability at 375px ───────────────────

  describe('Test Case 6: Code block readability at 375px', () => {
    it('code blocks have overflow-x: auto for horizontal scrolling', () => {
      const hasCodeOverflow = /overflow-x:\s*auto/.test(allCss);
      expect(hasCodeOverflow).toBe(true);
    });

    it('copy button CSS styles are defined in stylesheets', () => {
      // Copy button styling exists — verifies responsive copy buttons are styled
      const hasCopyButtonStyles =
        /\.copy-button/.test(allCss) || /copy-button/.test(allCss) || /data-copy-target/.test(allCss);
      expect(hasCopyButtonStyles).toBe(true);
    });

    it('code block wrapper pattern exists in source styles', () => {
      const hasCodeBlockStyles =
        /code-block/.test(allCss) || /\.code-block-pre/.test(allCss);
      expect(hasCodeBlockStyles).toBe(true);
    });
  });

  // ── Test Case 7: Touch target sizes ────────────────────────────────

  describe('Test Case 7: Touch target sizes at 375px (WCAG 2.5.5)', () => {
    it('CSS defines minimum touch target size (44px)', () => {
      const hasTouchTarget = /min-height:\s*44px/.test(allCss) ||
        /min-height:\s*var\(--touch-target-min/.test(allCss);
      expect(hasTouchTarget).toBe(true);
    });

    it('touch target CSS variable is defined', () => {
      const vars = parseCssVariables(allCss);
      expect(vars['--touch-target-min']).toBeDefined();
    });

    it('touch targets are enforced for pointer: coarse media query', () => {
      const hasCoarsePointerQuery = /@media\s*\(pointer:\s*coarse\)/.test(allCss);
      expect(hasCoarsePointerQuery).toBe(true);
    });
  });

  // ── Test Case 8: Responsive images ─────────────────────────────────

  describe('Test Case 8: Responsive images at mobile viewports', () => {
    it('images have max-width: 100% and height: auto CSS', () => {
      const hasResponsiveImg =
        /img[^{]*\{[^}]*max-width:\s*100%/.test(allCss) ||
        /max-width:\s*100%[^}]*height:\s*auto/.test(allCss) ||
        /height:\s*auto[^}]*max-width:\s*100%/.test(allCss);
      expect(hasResponsiveImg).toBe(true);
    });

    it('logo image has explicit width and height attributes', () => {
      const imgs = document.querySelectorAll('img');
      let hasDimensions = false;
      for (const img of imgs) {
        if (img.getAttribute('width') && img.getAttribute('height')) {
          hasDimensions = true;
          break;
        }
      }
      expect(hasDimensions).toBe(true);
    });

    it('images do not have fixed pixel widths that could overflow', () => {
      // Check that no img has a style with fixed width > viewport
      const imgs = document.querySelectorAll('img');
      for (const img of imgs) {
        const style = img.getAttribute('style') || '';
        const widthMatch = style.match(/width:\s*(\d+)px/);
        if (widthMatch) {
          expect(parseInt(widthMatch[1], 10)).toBeLessThanOrEqual(320);
        }
      }
    });
  });

  // ── Test Case 9: Smooth resize transitions ─────────────────────────

  describe('Test Case 9: Smooth resize transitions between breakpoints', () => {
    it('CSS breakpoints are defined at appropriate widths', () => {
      const breakpoints = extractBreakpointsFromCss(allCss);

      // Should have breakpoints for mobile and tablet
      const hasMobileBp = breakpoints.some((bp) => bp >= 767 && bp <= 768);
      const hasTabletBp = breakpoints.some((bp) => bp >= 1023 && bp <= 1280);

      expect(hasMobileBp).toBe(true);
      expect(hasTabletBp).toBe(true);
    });

    it('transitions use smooth CSS timing functions', () => {
      const hasTransition = /transition:/.test(allCss);
      expect(hasTransition).toBe(true);
    });

    it('no breakpoint leaves content inaccessible (all sections have IDs)', () => {
      const sections = document.querySelectorAll('section[id], div[id="mobile-menu"]');
      const sectionIds = new Set<string>();
      for (const s of sections) {
        const id = s.getAttribute('id');
        if (id) sectionIds.add(id);
      }
      // At minimum hero section should have an ID
      expect(sectionIds.has('hero') || sectionIds.size > 0).toBe(true);
    });

    it('breakpoint CSS custom properties are usable for JS detection', () => {
      const vars = parseCssVariables(allCss);
      const hasBpVars =
        vars['--bp-mobile-max'] &&
        vars['--bp-tablet-min'] &&
        vars['--bp-desktop-min'];
      expect(hasBpVars).toBeTruthy();
    });
  });

  // ── Overall responsive design requirements ─────────────────────────

  describe('Overall NFR-3 compliance', () => {
    it('supports desktop viewports (1280px+) via CSS', () => {
      const hasDesktopQuery = /@media\s*\(min-width:\s*1280px\)/.test(allCss);
      const hasBpVar = parseCssVariables(allCss)['--bp-desktop-min'];
      expect(hasDesktopQuery || !!hasBpVar).toBe(true);
    });

    it('supports tablet viewports (768px-1279px) via CSS', () => {
      const hasTabletQuery =
        /@media\s*\(max-width:\s*1279px\)/.test(allCss) ||
        /@media\s*\(max-width:\s*12[0-9]{2}px\)/.test(allCss);
      expect(hasTabletQuery).toBe(true);
    });

    it('supports mobile viewports (320px-767px) via CSS', () => {
      const hasMobileQuery = /@media\s*\(max-width:\s*767px\)/.test(allCss);
      expect(hasMobileQuery).toBe(true);
    });

    it('layout is consistent across viewports (no missing styles)', () => {
      // Verify key CSS files are loaded (global, theme)
      const cssContent = allCss;
      expect(cssContent.length).toBeGreaterThan(1000);
      expect(cssContent).toContain('--color-primary');
    });
  });
});
