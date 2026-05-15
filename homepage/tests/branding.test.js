/**
 * Branding and visual consistency tests for MirDB homepage.
 * Owner: Scenario 12 - Branding and Visual Consistency
 *
 * Test framework: Vitest + jsdom
 *
 * Covers test cases 1-7 from scenario 12:
 *   1. Logo usage and placement
 *   2. Color palette consistency via CSS custom properties
 *   3. Typography consistency (heading sizes, font families)
 *   4. Section vertical spacing on a defined scale
 *   5. Button styles consistency (primary/secondary, hover/focus/active)
 *   6. Card styles consistency
 *   7. Visual artifacts / transition timing
 */

import { describe, it, expect, beforeAll } from 'vitest';
import { JSDOM } from 'jsdom';
import fs from 'node:fs';
import path from 'node:path';

const HOMEPAGE_ROOT = path.resolve(__dirname, '..');
const CSS_DIR = path.join(HOMEPAGE_ROOT, 'css');
const ASSETS_DIR = path.join(HOMEPAGE_ROOT, 'assets');
const INDEX_HTML = path.join(HOMEPAGE_ROOT, 'index.html');

/** Load the homepage as a JSDOM document. */
function loadDOM() {
  const html = fs.readFileSync(INDEX_HTML, 'utf-8');
  return new JSDOM(html, { url: 'http://localhost:8080' });
}

/** Read every CSS file in the css/ directory, concatenated. */
function readAllCss() {
  const files = fs.readdirSync(CSS_DIR).filter((f) => f.endsWith('.css'));
  return files
    .map((f) => ({
      name: f,
      content: fs.readFileSync(path.join(CSS_DIR, f), 'utf-8'),
    }));
}

/** Concatenate every CSS file together plus the inline critical CSS from index.html. */
function readAllStyles() {
  const cssFiles = readAllCss();
  let combined = cssFiles.map((f) => f.content).join('\n');
  const html = fs.readFileSync(INDEX_HTML, 'utf-8');
  const inlineMatch = html.match(/<style[^>]*>([\s\S]*?)<\/style>/g);
  if (inlineMatch) {
    for (const tag of inlineMatch) {
      const inner = tag.replace(/<style[^>]*>/, '').replace(/<\/style>/, '');
      combined += '\n' + inner;
    }
  }
  return combined;
}

/** Strip CSS comments so they don't pollute pattern matches. */
function stripComments(css) {
  return css.replace(/\/\*[\s\S]*?\*\//g, '');
}

/** Extract every hex literal that is not part of a CSS custom property declaration. */
function extractHexColors(css) {
  const cleaned = stripComments(css);
  const matches = cleaned.match(/#[0-9a-fA-F]{3,8}\b/g) || [];
  return matches.map((m) => m.toLowerCase());
}

/** Extract every CSS rgb/rgba literal. */
function extractRgbColors(css) {
  const cleaned = stripComments(css);
  return cleaned.match(/rgba?\([^)]+\)/g) || [];
}

describe('Branding and Visual Consistency', () => {
  let dom;
  let document;
  let combinedCss;

  beforeAll(() => {
    dom = loadDOM();
    document = dom.window.document;
    combinedCss = readAllStyles();
  });

  describe('Test Case 1: Logo usage and placement', () => {
    it('preserves the canonical MirDB logo asset (assets/logo.gif)', () => {
      const gifPath = path.join(ASSETS_DIR, 'logo.gif');
      expect(fs.existsSync(gifPath)).toBe(true);
    });

    it('displays a logo image in the header', () => {
      const logo = document.querySelector('header img');
      expect(logo).not.toBeNull();
    });

    it('references the existing MirDB logo (logo.gif or its derived logo.webp)', () => {
      const logo = document.querySelector('header img');
      expect(logo).not.toBeNull();
      const src = logo.getAttribute('src') || '';
      expect(src).toMatch(/assets\/logo\.(gif|webp)$/i);
      const resolved = path.resolve(HOMEPAGE_ROOT, src);
      expect(fs.existsSync(resolved)).toBe(true);
    });

    it('has descriptive alt text on the logo image', () => {
      const logo = document.querySelector('header img');
      const alt = logo.getAttribute('alt') || '';
      expect(alt.trim().length).toBeGreaterThan(0);
      expect(alt.toLowerCase()).toMatch(/mirdb|logo/);
    });

    it('declares explicit width and height to avoid distortion / layout shift', () => {
      const logo = document.querySelector('header img');
      const width = parseInt(logo.getAttribute('width') || '0', 10);
      const height = parseInt(logo.getAttribute('height') || '0', 10);
      expect(width).toBeGreaterThan(0);
      expect(height).toBeGreaterThan(0);
      // Approximate aspect ratio sanity check — guards against distortion.
      const ratio = width / height;
      expect(ratio).toBeGreaterThan(0.25);
      expect(ratio).toBeLessThan(4);
    });

    it('keeps logo dimensions visually appropriate (32-200px tall)', () => {
      const logo = document.querySelector('header img');
      const height = parseInt(logo.getAttribute('height') || '0', 10);
      expect(height).toBeGreaterThanOrEqual(32);
      expect(height).toBeLessThanOrEqual(200);
    });
  });

  describe('Test Case 2: Color palette consistency', () => {
    it('defines a color palette via CSS custom properties in :root', () => {
      const baseCss = fs.readFileSync(path.join(CSS_DIR, 'base.css'), 'utf-8');
      const rootMatch = baseCss.match(/:root\s*\{([\s\S]*?)\}/);
      expect(rootMatch).not.toBeNull();
      const rootBlock = rootMatch[1];
      // Required palette tokens
      const required = [
        '--color-primary',
        '--color-primary-contrast',
        '--color-text',
        '--color-text-muted',
        '--color-background',
        '--color-surface',
        '--color-border',
      ];
      for (const token of required) {
        expect(rootBlock).toContain(token);
      }
    });

    it('uses a limited palette of unique hex colors (<=12 plus neutrals)', () => {
      const hexes = extractHexColors(combinedCss);
      const unique = Array.from(new Set(hexes));
      // Limited palette: a small number of brand + semantic + neutral colors.
      expect(unique.length).toBeLessThanOrEqual(12);
    });

    it('keeps stray non-tokenized hex colors low in component CSS files', () => {
      const cssFiles = readAllCss().filter((f) => f.name !== 'base.css');
      let strayCount = 0;
      for (const file of cssFiles) {
        const hexes = extractHexColors(file.content);
        // Each component file may have at most a couple of incidental hex literals
        // (e.g. rgba shadows are extracted separately).
        if (hexes.length > 2) {
          strayCount += hexes.length;
        }
      }
      expect(strayCount).toBeLessThanOrEqual(4);
    });

    it('references colors through CSS custom properties (var(--color-*))', () => {
      const cssFiles = readAllCss().filter((f) => f.name !== 'base.css');
      let filesUsingTokens = 0;
      for (const file of cssFiles) {
        if (/var\(--color-[a-z0-9-]+\)/i.test(file.content)) {
          filesUsingTokens += 1;
        }
      }
      // The majority of component stylesheets must consume color tokens.
      expect(filesUsingTokens).toBeGreaterThanOrEqual(4);
    });
  });

  describe('Test Case 3: Typography consistency', () => {
    it('defines a sans and mono font stack token in base.css', () => {
      const baseCss = fs.readFileSync(path.join(CSS_DIR, 'base.css'), 'utf-8');
      expect(baseCss).toMatch(/--font-sans\s*:/);
      expect(baseCss).toMatch(/--font-mono\s*:/);
    });

    it('does not override font-family with raw stacks in component CSS', () => {
      const cssFiles = readAllCss().filter((f) => f.name !== 'base.css');
      for (const file of cssFiles) {
        const cleaned = stripComments(file.content);
        // font-family declarations are allowed only when they reference a CSS variable.
        const fontFamilyDecls = cleaned.match(/font-family\s*:\s*[^;]+;/g) || [];
        for (const decl of fontFamilyDecls) {
          expect(decl).toMatch(/var\(--font-(sans|mono)/i);
        }
      }
    });

    it('renders exactly one h1 element on the page', () => {
      const h1s = document.querySelectorAll('h1');
      expect(h1s.length).toBe(1);
    });

    it('uses one consistent class (.section-title) for all h2 section titles', () => {
      const h2s = Array.from(document.querySelectorAll('main h2'));
      expect(h2s.length).toBeGreaterThanOrEqual(2);
      for (const h2 of h2s) {
        expect(h2.classList.contains('section-title')).toBe(true);
      }
    });

    it('defines exactly one .section-title font-size rule in component CSS', () => {
      const cssFiles = readAllCss();
      // Match only the base .section-title rule (no descendant/child combinators)
      // outside of @media queries — those handle responsive variants.
      let baseFontSizeDecls = 0;
      for (const file of cssFiles) {
        const cleaned = stripComments(file.content);
        // Remove all @media blocks to isolate base rules only.
        const baseScope = cleaned.replace(/@media[^{]*\{(?:[^{}]*\{[^}]*\}[^{}]*)*\}/g, '');
        // Look for a base ".section-title { ... font-size: ...; ... }" rule.
        const rulePattern = /\.section-title\s*\{[^}]*font-size\s*:[^;}]+;[^}]*\}/g;
        const matches = baseScope.match(rulePattern) || [];
        baseFontSizeDecls += matches.length;
      }
      // Exactly one base declaration ensures all section titles share size.
      expect(baseFontSizeDecls).toBe(1);
    });

    it('uses the same heading-3 class across feature cards and resource groups', () => {
      // Each H3 should belong to a known class so it inherits a uniform style.
      const knownClasses = new Set([
        'feature-title',
        'resource-group-title',
      ]);
      const h3s = Array.from(document.querySelectorAll('main h3'));
      expect(h3s.length).toBeGreaterThan(0);
      for (const h3 of h3s) {
        const classes = Array.from(h3.classList);
        const overlap = classes.filter((c) => knownClasses.has(c));
        expect(overlap.length).toBeGreaterThan(0);
      }
    });

    it('uses consistent line-height base on body (set in base.css)', () => {
      const baseCss = fs.readFileSync(path.join(CSS_DIR, 'base.css'), 'utf-8');
      expect(baseCss).toMatch(/body\s*\{[^}]*line-height\s*:/);
    });
  });

  describe('Test Case 4: Section vertical spacing on a defined scale', () => {
    it('defines a spacing scale via CSS custom properties (xs/sm/md/lg/xl)', () => {
      const baseCss = fs.readFileSync(path.join(CSS_DIR, 'base.css'), 'utf-8');
      const required = [
        '--spacing-xs',
        '--spacing-sm',
        '--spacing-md',
        '--spacing-lg',
        '--spacing-xl',
      ];
      for (const token of required) {
        expect(baseCss).toContain(token);
      }
    });

    it('spaces scale values use rem units (consistent rhythm)', () => {
      const baseCss = fs.readFileSync(path.join(CSS_DIR, 'base.css'), 'utf-8');
      const rootMatch = baseCss.match(/:root\s*\{([\s\S]*?)\}/);
      const rootBlock = rootMatch[1];
      const spacingDecls = rootBlock.match(/--spacing-[a-z0-9]+\s*:[^;]+;/g) || [];
      expect(spacingDecls.length).toBeGreaterThanOrEqual(5);
      for (const decl of spacingDecls) {
        expect(decl).toMatch(/\b\d+(\.\d+)?\s*rem\b/);
      }
    });

    it('section padding declarations consume the spacing scale', () => {
      // Each main section should have a padding rule that uses var(--spacing-*).
      // Sections may appear in grouped selectors (e.g. "#features, #quick-start").
      const sectionIds = ['#hero', '#features', '#quick-start', '#resources'];
      for (const id of sectionIds) {
        const safeId = id.replace('#', '');
        let foundTokenizedPadding = false;
        for (const file of readAllCss()) {
          const cleaned = stripComments(file.content);
          // Match the section ID anywhere in a selector list, then look for
          // a padding declaration that references a spacing token.
          const pattern = new RegExp(
            `(?:^|[,\\s])#${safeId}(?:[,\\s]|\\s*\\{)`,
            'im'
          );
          if (!pattern.test(cleaned)) continue;
          // Now search within the file for a padding: ... var(--spacing-*) ...
          const padPattern = /padding\s*:\s*[^;]*var\(--spacing-[^)]+\)[^;]*;/gi;
          if (padPattern.test(cleaned)) {
            foundTokenizedPadding = true;
            break;
          }
        }
        expect(foundTokenizedPadding).toBe(true);
      }
    });

    it('does not use abrupt fixed pixel section paddings (>= 30px) outside the scale', () => {
      // Section padding values should reference the spacing scale; bare pixel padding
      // values larger than the scale's smallest step would indicate inconsistency.
      const cssFiles = readAllCss();
      const sectionIdRe = /#(hero|features|quick-start|resources)\s*\{[^}]*\}/g;
      let abruptCount = 0;
      for (const file of cssFiles) {
        const cleaned = stripComments(file.content);
        const matches = cleaned.match(sectionIdRe) || [];
        for (const block of matches) {
          // padding declarations using bare px > 30px count as abrupt.
          const padDecl = block.match(/padding\s*:\s*([^;]+);/);
          if (!padDecl) continue;
          const value = padDecl[1];
          const pxValues = value.match(/(\d+)px/g) || [];
          for (const pv of pxValues) {
            const num = parseInt(pv.replace('px', ''), 10);
            if (num >= 30) abruptCount += 1;
          }
        }
      }
      expect(abruptCount).toBe(0);
    });
  });

  describe('Test Case 5: Button style consistency', () => {
    let heroCss;

    beforeAll(() => {
      heroCss = fs.readFileSync(path.join(CSS_DIR, 'hero.css'), 'utf-8');
    });

    it('declares background-color, padding, border-radius, font-size on .hero-cta-primary', () => {
      const cleaned = stripComments(heroCss);
      const match = cleaned.match(/\.hero-cta-primary\s*\{([^}]+)\}/);
      expect(match).not.toBeNull();
      const block = match[1];
      expect(block).toMatch(/background-color\s*:/);
      expect(block).toMatch(/padding\s*:/);
      expect(block).toMatch(/border-radius\s*:/);
      expect(block).toMatch(/font-size\s*:/);
      expect(block).toMatch(/font-weight\s*:/);
    });

    it('declares matching padding/border-radius/font-size on .hero-cta-secondary', () => {
      const cleaned = stripComments(heroCss);
      const primary = cleaned.match(/\.hero-cta-primary\s*\{([^}]+)\}/)[1];
      const secondary = cleaned.match(/\.hero-cta-secondary\s*\{([^}]+)\}/)[1];

      function extract(prop, block) {
        const m = block.match(new RegExp(`${prop}\\s*:\\s*([^;]+);`));
        return m ? m[1].trim() : null;
      }

      expect(extract('padding', secondary)).toBe(extract('padding', primary));
      expect(extract('border-radius', secondary)).toBe(extract('border-radius', primary));
      expect(extract('font-size', secondary)).toBe(extract('font-size', primary));
      expect(extract('font-weight', secondary)).toBe(extract('font-weight', primary));
    });

    it('defines a hover state for both primary and secondary CTAs', () => {
      expect(heroCss).toMatch(/\.hero-cta-primary:hover\s*\{/);
      expect(heroCss).toMatch(/\.hero-cta-secondary:hover\s*\{/);
    });

    it('defines a focus state for the primary CTA (keyboard a11y)', () => {
      expect(heroCss).toMatch(/\.hero-cta-primary:focus(-visible)?\s*\{/);
    });

    it('uses the same transition timing across CTA buttons (200ms)', () => {
      const cleaned = stripComments(heroCss);
      const primary = cleaned.match(/\.hero-cta-primary\s*\{([^}]+)\}/)[1];
      const secondary = cleaned.match(/\.hero-cta-secondary\s*\{([^}]+)\}/)[1];
      expect(primary).toMatch(/transition[^;]*200ms/);
      expect(secondary).toMatch(/transition[^;]*200ms/);
    });

    it('uses the primary brand color token on the primary CTA', () => {
      const cleaned = stripComments(heroCss);
      const block = cleaned.match(/\.hero-cta-primary\s*\{([^}]+)\}/)[1];
      expect(block).toMatch(/var\(--color-primary\)/);
    });
  });

  describe('Test Case 6: Card style consistency', () => {
    let featuresCss;

    beforeAll(() => {
      featuresCss = fs.readFileSync(path.join(CSS_DIR, 'features.css'), 'utf-8');
    });

    it('defines feature-card with padding, border-radius, background', () => {
      const cleaned = stripComments(featuresCss);
      const block = cleaned.match(/\.feature-card\s*\{([^}]+)\}/);
      expect(block).not.toBeNull();
      const body = block[1];
      expect(body).toMatch(/padding\s*:/);
      expect(body).toMatch(/border-radius\s*:/);
      expect(body).toMatch(/background\s*:/);
    });

    it('defines a hover effect for feature cards with a transition', () => {
      expect(featuresCss).toMatch(/\.feature-card:hover\s*\{[^}]*transform\s*:/);
      const cleaned = stripComments(featuresCss);
      const base = cleaned.match(/\.feature-card\s*\{([^}]+)\}/)[1];
      expect(base).toMatch(/transition[^;]*200ms/);
    });

    it('renders the same number of feature-card elements as the markup defines', () => {
      const cards = document.querySelectorAll('#features .feature-card');
      expect(cards.length).toBeGreaterThanOrEqual(4);
      // Each card has the same child structure: icon, h3, p.
      for (const card of cards) {
        expect(card.querySelector('.feature-icon')).not.toBeNull();
        expect(card.querySelector('h3.feature-title')).not.toBeNull();
        expect(card.querySelector('p.feature-description')).not.toBeNull();
      }
    });

    it('uses the same border-radius token across card-like containers', () => {
      // Both feature-card and resource-group should consume the same radius token.
      const featuresCard = stripComments(featuresCss).match(/\.feature-card\s*\{([^}]+)\}/)[1];
      const resourcesCss = fs.readFileSync(path.join(CSS_DIR, 'resources.css'), 'utf-8');
      const resourceGroup = stripComments(resourcesCss).match(/\.resource-group\s*\{([^}]+)\}/)[1];
      // Both should reference var(--border-radius).
      expect(featuresCard).toMatch(/border-radius\s*:[^;]*var\(--border-radius/);
      expect(resourceGroup).toMatch(/border-radius\s*:[^;]*var\(--border-radius/);
    });
  });

  describe('Test Case 7: Hover/focus/active state and transition consistency', () => {
    it('defines reasonable transition durations across all CSS (100-500ms)', () => {
      const cssFiles = readAllCss();
      for (const file of cssFiles) {
        let cleaned = stripComments(file.content);
        // Exclude reduced-motion accessibility overrides (0.01ms is intentional).
        cleaned = cleaned.replace(
          /@media\s*\(\s*prefers-reduced-motion[^{]*\{[\s\S]*?\}\s*\}/gi,
          ''
        );
        // Also strip any other prefers-reduced-motion blocks.
        const durations = cleaned.match(/(?:transition[^;]*?)(\d+)\s*ms/g) || [];
        for (const d of durations) {
          const num = parseInt(d.match(/(\d+)\s*ms/)[1], 10);
          expect(num).toBeGreaterThanOrEqual(100);
          expect(num).toBeLessThanOrEqual(500);
        }
      }
    });

    it('defines :focus or :focus-visible states for primary interactive elements', () => {
      const cssFiles = readAllCss();
      const required = [
        '.hero-cta-primary',
        '.nav-link',
        '.resource-link',
      ];
      for (const selector of required) {
        const escaped = selector.replace('.', '\\.');
        const pattern = new RegExp(`${escaped}:focus(-visible)?\\s*\\{`);
        let found = false;
        for (const file of cssFiles) {
          if (pattern.test(file.content)) {
            found = true;
            break;
          }
        }
        expect(found).toBe(true);
      }
    });

    it('defines :hover state for primary interactive elements', () => {
      const cssFiles = readAllCss();
      const required = [
        '.hero-cta-primary',
        '.hero-cta-secondary',
        '.feature-card',
        '.nav-link',
        '.resource-link',
      ];
      for (const selector of required) {
        const escaped = selector.replace('.', '\\.');
        const pattern = new RegExp(`${escaped}:hover\\s*\\{`);
        let found = false;
        for (const file of cssFiles) {
          if (pattern.test(file.content)) {
            found = true;
            break;
          }
        }
        expect(found).toBe(true);
      }
    });

    it('respects users who prefer reduced motion', () => {
      const responsiveCss = fs.readFileSync(path.join(CSS_DIR, 'responsive.css'), 'utf-8');
      expect(responsiveCss).toMatch(/@media\s*\(\s*prefers-reduced-motion/);
    });

    it('uses consistent border styles (no mixed border-style declarations)', () => {
      const cssFiles = readAllCss();
      const borderStyles = new Set();
      for (const file of cssFiles) {
        const cleaned = stripComments(file.content);
        const styles = cleaned.match(/border(?:-(?:top|right|bottom|left))?\s*:\s*[^;]+;/g) || [];
        for (const decl of styles) {
          // Match the style keyword (solid/dashed/dotted/...).
          const styleMatch = decl.match(/\b(solid|dashed|dotted|double|groove|ridge|inset|outset|none)\b/);
          if (styleMatch) borderStyles.add(styleMatch[1]);
        }
      }
      // Limit to a small, deliberate set of border styles.
      // Acceptable: 'solid' for actual borders and possibly 'none' as resets.
      const allowed = new Set(['solid', 'none']);
      for (const style of borderStyles) {
        expect(allowed.has(style)).toBe(true);
      }
    });

    it('uses no overflowing or NaN z-index values', () => {
      const cssFiles = readAllCss();
      for (const file of cssFiles) {
        const cleaned = stripComments(file.content);
        const zIndexes = cleaned.match(/z-index\s*:\s*(-?\d+)/g) || [];
        for (const z of zIndexes) {
          const num = parseInt(z.match(/(-?\d+)/)[1], 10);
          expect(num).toBeGreaterThanOrEqual(-10);
          expect(num).toBeLessThanOrEqual(1000);
        }
      }
    });
  });
});
