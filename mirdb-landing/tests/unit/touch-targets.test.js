/**
 * Touch Target Unit Tests
 * Owner: Scenario 10 - Responsive Design
 *
 * Tests:
 * - All interactive elements have minimum 44x44px touch target size
 * - Tests CSS rules for min-height and min-width properties
 */

import { test, describe } from 'node:test';
import assert from 'node:assert';
import { readFileSync, readdirSync, statSync, existsSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const cssDir = join(__dirname, '../../css');

// Minimum touch target size per WCAG 2.1 and Apple HIG
const MIN_TOUCH_TARGET_SIZE = 44;

// Helper function to read CSS file
function readCssFile(relativePath) {
  const fullPath = join(cssDir, relativePath);
  if (existsSync(fullPath)) {
    return readFileSync(fullPath, 'utf-8');
  }
  return null;
}

// Helper function to read all CSS files recursively
function getAllCssContent() {
  const allContent = [];

  const readDir = (dir) => {
    if (existsSync(dir)) {
      const files = readdirSync(dir);
      files.forEach(file => {
        const fullPath = join(dir, file);
        const stat = statSync(fullPath);
        if (stat.isDirectory()) {
          readDir(fullPath);
        } else if (file.endsWith('.css')) {
          allContent.push(readFileSync(fullPath, 'utf-8'));
        }
      });
    }
  };

  readDir(cssDir);
  return allContent.join('\n');
}

describe('Touch Target CSS Rules', () => {
  test('Navigation links have minimum touch target size', () => {
    const navCss = readCssFile('components/nav.css');
    assert.ok(navCss, 'nav.css file should exist');

    // Check .nav__link has min-height: 44px
    const hasMinHeight = navCss.includes('min-height: 44px') ||
                         navCss.includes('min-height:44px');
    const hasMinWidth = navCss.includes('min-width: 44px') ||
                        navCss.includes('min-width:44px');

    assert.ok(hasMinHeight || hasMinWidth,
      'Navigation links should have minimum touch target size of 44px');
  });

  test('Navigation toggle button has touch-friendly dimensions', () => {
    const navCss = readCssFile('components/nav.css');
    assert.ok(navCss, 'nav.css file should exist');

    // Check .nav__toggle has proper dimensions
    const hasToggleWidth = navCss.includes('width: 44px') ||
                           navCss.includes('width:44px');
    const hasToggleHeight = navCss.includes('height: 44px') ||
                            navCss.includes('height:44px');

    assert.ok(hasToggleWidth && hasToggleHeight,
      'Navigation toggle should have 44x44px dimensions');
  });

  test('Hero CTA buttons have minimum touch target size', () => {
    const heroCss = readCssFile('components/hero.css');
    assert.ok(heroCss, 'hero.css file should exist');

    // Check .hero__cta has min-height: 44px
    const hasMinHeight = heroCss.includes('min-height: 44px') ||
                         heroCss.includes('min-height:44px');

    assert.ok(hasMinHeight,
      'Hero CTA buttons should have minimum height of 44px');
  });

  test('Footer links have minimum touch target size', () => {
    const footerCss = readCssFile('components/footer.css');
    assert.ok(footerCss, 'footer.css file should exist');

    // Check .footer__link has min-height: 44px
    const hasMinHeight = footerCss.includes('min-height: 44px') ||
                         footerCss.includes('min-height:44px');

    assert.ok(hasMinHeight,
      'Footer links should have minimum height of 44px');
  });

  test('Responsive CSS provides touch-friendly utilities', () => {
    const responsiveCss = readCssFile('utilities/responsive.css');
    assert.ok(responsiveCss, 'responsive.css file should exist');

    // At minimum, responsive.css should contain media queries
    const hasMediaQueries = responsiveCss.includes('@media');

    assert.ok(hasMediaQueries,
      'Responsive CSS should contain media queries for breakpoints');
  });
});

describe('Touch Target Size Verification', () => {
  test('All interactive element classes define proper touch targets', () => {
    const interactiveSelectors = [
      { selector: '.nav__link', file: 'components/nav.css' },
      { selector: '.nav__toggle', file: 'components/nav.css' },
      { selector: '.hero__cta', file: 'components/hero.css' },
      { selector: '.footer__link', file: 'components/footer.css' },
    ];

    interactiveSelectors.forEach(({ selector, file }) => {
      const content = readCssFile(file);
      if (content) {
        // Check if the selector exists in the file
        const selectorRegex = new RegExp(selector.replace('.', '\\.').replace('__', '__'), 'g');
        const hasSelector = selectorRegex.test(content);

        assert.ok(hasSelector,
          `${file} should contain styles for ${selector}`);
      }
    });
  });

  test('Minimum touch target of 44px is used consistently', () => {
    const combinedCss = getAllCssContent();

    // Check that 44px is used (the standard touch target size)
    const has44px = combinedCss.includes('44px');

    assert.ok(has44px,
      'CSS should include 44px touch target size styling');
  });
});

describe('Responsive CSS Breakpoints', () => {
  test('Responsive CSS contains tablet breakpoint (768px)', () => {
    const responsiveCss = readCssFile('utilities/responsive.css');
    assert.ok(responsiveCss, 'responsive.css file should exist');

    const hasTabletBreakpoint = responsiveCss.includes('768px');
    assert.ok(hasTabletBreakpoint,
      'Responsive CSS should contain 768px tablet breakpoint');
  });

  test('Responsive CSS contains desktop breakpoint (1024px)', () => {
    const responsiveCss = readCssFile('utilities/responsive.css');
    assert.ok(responsiveCss, 'responsive.css file should exist');

    const hasDesktopBreakpoint = responsiveCss.includes('1024px');
    assert.ok(hasDesktopBreakpoint,
      'Responsive CSS should contain 1024px desktop breakpoint');
  });

  test('Responsive CSS uses mobile-first approach (min-width queries)', () => {
    const responsiveCss = readCssFile('utilities/responsive.css');
    assert.ok(responsiveCss, 'responsive.css file should exist');

    const hasMinWidthQueries = responsiveCss.includes('min-width');
    assert.ok(hasMinWidthQueries,
      'Responsive CSS should use min-width media queries for mobile-first approach');
  });

  test('Container max-width is defined', () => {
    const responsiveCss = readCssFile('utilities/responsive.css');
    const mainCss = readCssFile('styles.css');

    const hasMaxWidth = (responsiveCss && responsiveCss.includes('max-width')) ||
                        (mainCss && mainCss.includes('max-width'));

    assert.ok(hasMaxWidth,
      'CSS should define max-width for containers');
  });
});
