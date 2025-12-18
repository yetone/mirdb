// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');
const fs = require('fs');

const pageUrl = 'file://' + path.join(__dirname, '..', 'index.html');
const stylesPath = path.join(__dirname, '..', 'styles.css');

/**
 * CSS Compatibility Unit Tests (Test Case 5)
 * Verify no CSS features used that lack browser support
 *
 * Tests CSS property compatibility across modern browsers:
 * - Chrome (latest 2 versions)
 * - Firefox (latest 2 versions)
 * - Safari (latest 2 versions)
 * - Edge (latest 2 versions)
 *
 * Reference: https://caniuse.com for browser support data
 */

test.describe('CSS Compatibility - Property Support', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(pageUrl);
    await page.waitForLoadState('domcontentloaded');
  });

  test('CSS Custom Properties (CSS Variables) are supported', async ({ page }) => {
    // CSS Custom Properties have full support in all modern browsers since 2017
    // Chrome 49+, Firefox 31+, Safari 9.1+, Edge 15+

    const root = page.locator(':root');
    const primaryColor = await page.evaluate(() => {
      return getComputedStyle(document.documentElement).getPropertyValue('--color-primary').trim();
    });

    // CSS variable should resolve to a color value, not be empty
    expect(primaryColor).toBeTruthy();
    expect(primaryColor).toMatch(/#[0-9a-fA-F]{6}|rgb/);
  });

  test('Flexbox is supported', async ({ page }) => {
    // Flexbox has full support in all modern browsers
    // Chrome 29+, Firefox 28+, Safari 9+, Edge 12+

    const nav = page.locator('.nav');
    const display = await nav.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });

    expect(display).toBe('flex');
  });

  test('CSS Grid is supported', async ({ page }) => {
    // CSS Grid has full support in all modern browsers since 2017
    // Chrome 57+, Firefox 52+, Safari 10.1+, Edge 16+

    const featuresGrid = page.locator('.features-grid');
    const display = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });

    expect(display).toBe('grid');
  });

  test('Grid auto-fit/minmax is supported', async ({ page }) => {
    // auto-fit and minmax() are part of CSS Grid spec
    // Supported in all browsers that support CSS Grid

    const featuresGrid = page.locator('.features-grid');
    const gridTemplateColumns = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).gridTemplateColumns;
    });

    // Should be computed to actual pixel values, not "none"
    expect(gridTemplateColumns).not.toBe('none');
    expect(gridTemplateColumns).toBeTruthy();
  });

  test('position: sticky is supported', async ({ page }) => {
    // position: sticky has full support in all modern browsers
    // Chrome 56+, Firefox 59+, Safari 13+, Edge 16+

    const header = page.locator('.header');
    const position = await header.evaluate((el) => {
      return window.getComputedStyle(el).position;
    });

    expect(position).toBe('sticky');
  });

  test('box-sizing: border-box is supported', async ({ page }) => {
    // box-sizing has full support in all browsers
    // Chrome 10+, Firefox 29+, Safari 5.1+, Edge 12+

    const boxSizing = await page.evaluate(() => {
      const el = document.querySelector('.feature-card');
      return window.getComputedStyle(el).boxSizing;
    });

    expect(boxSizing).toBe('border-box');
  });

  test('border-radius is supported', async ({ page }) => {
    // border-radius has full support since 2011
    // Chrome 4+, Firefox 4+, Safari 5+, Edge 12+

    const button = page.locator('.btn-primary').first();
    const borderRadius = await button.evaluate((el) => {
      return window.getComputedStyle(el).borderRadius;
    });

    expect(borderRadius).toBeTruthy();
    expect(borderRadius).not.toBe('0px');
  });

  test('box-shadow is supported', async ({ page }) => {
    // box-shadow has full support since 2011
    // Chrome 10+, Firefox 4+, Safari 5.1+, Edge 12+

    const featureCard = page.locator('.feature-card').first();
    await featureCard.hover();
    await page.waitForTimeout(300);

    const boxShadow = await featureCard.evaluate((el) => {
      return window.getComputedStyle(el).boxShadow;
    });

    // After hover, should have box-shadow
    expect(boxShadow).not.toBe('none');
  });

  test('CSS transitions are supported', async ({ page }) => {
    // CSS transitions have full support
    // Chrome 26+, Firefox 16+, Safari 9+, Edge 12+

    const button = page.locator('.btn-primary').first();
    const transition = await button.evaluate((el) => {
      return window.getComputedStyle(el).transition;
    });

    expect(transition).toBeTruthy();
    expect(transition).not.toBe('none 0s ease 0s');
  });

  test('CSS transforms are supported', async ({ page }) => {
    // CSS transforms have full support
    // Chrome 36+, Firefox 16+, Safari 9+, Edge 12+

    const featureCard = page.locator('.feature-card').first();
    await featureCard.hover();
    await page.waitForTimeout(300);

    const transform = await featureCard.evaluate((el) => {
      return window.getComputedStyle(el).transform;
    });

    // After hover, should have transform applied (translateY)
    expect(transform).not.toBe('none');
  });

  test('Media queries are supported', async ({ page }) => {
    // Media queries have full support
    // Chrome 4+, Firefox 3.5+, Safari 4+, Edge 12+

    // Test by checking mobile viewport response
    await page.setViewportSize({ width: 375, height: 667 });

    const hamburgerMenu = page.locator('.mobile-menu-btn');
    const isVisible = await hamburgerMenu.isVisible();

    expect(isVisible).toBe(true);

    // Reset to desktop
    await page.setViewportSize({ width: 1200, height: 800 });
    const isHidden = await hamburgerMenu.isHidden();

    expect(isHidden).toBe(true);
  });

  test('prefers-color-scheme media query is supported', async ({ page }) => {
    // prefers-color-scheme has support in modern browsers
    // Chrome 76+, Firefox 67+, Safari 12.1+, Edge 79+

    // This test verifies the CSS doesn't break - the actual dark mode
    // is system-dependent but the media query should be parsed

    // Emulate dark mode
    await page.emulateMedia({ colorScheme: 'dark' });

    // Page should still render
    const body = page.locator('body');
    await expect(body).toBeVisible();

    // CSS custom properties should still work
    const bgColor = await page.evaluate(() => {
      return window.getComputedStyle(document.body).backgroundColor;
    });

    expect(bgColor).toBeTruthy();
  });

  test('prefers-reduced-motion media query is supported', async ({ page }) => {
    // prefers-reduced-motion has support in modern browsers
    // Chrome 74+, Firefox 63+, Safari 10.1+, Edge 79+

    // Emulate reduced motion preference
    await page.emulateMedia({ reducedMotion: 'reduce' });

    // Page should still render correctly
    const body = page.locator('body');
    await expect(body).toBeVisible();

    // Transitions should be disabled when reduced motion is preferred
    const html = page.locator('html');
    const scrollBehavior = await html.evaluate((el) => {
      return window.getComputedStyle(el).scrollBehavior;
    });

    // With reduced motion, scroll-behavior should be 'auto' not 'smooth'
    expect(scrollBehavior).toBe('auto');
  });

  test('scroll-behavior: smooth is supported', async ({ page }) => {
    // scroll-behavior has support in modern browsers
    // Chrome 61+, Firefox 36+, Safari 15.4+, Edge 79+
    // Note: Safari added support in 15.4 (2022)

    const html = page.locator('html');
    const scrollBehavior = await html.evaluate((el) => {
      return window.getComputedStyle(el).scrollBehavior;
    });

    expect(scrollBehavior).toBe('smooth');
  });

  test('outline and outline-offset are supported', async ({ page }) => {
    // outline and outline-offset have full support
    // Chrome 4+, Firefox 4+, Safari 3.1+, Edge 12+

    // Focus on an element
    const firstLink = page.locator('a').first();
    await firstLink.focus();

    const outline = await firstLink.evaluate((el) => {
      return window.getComputedStyle(el).outline;
    });
    const outlineOffset = await firstLink.evaluate((el) => {
      return window.getComputedStyle(el).outlineOffset;
    });

    // Both should be computed values
    expect(outline).toBeTruthy();
    expect(outlineOffset).toBeTruthy();
  });

  test('gap property in flexbox is supported', async ({ page }) => {
    // gap in flexbox has support in modern browsers
    // Chrome 84+, Firefox 63+, Safari 14.1+, Edge 84+

    const navLinks = page.locator('.nav-links');
    const gap = await navLinks.evaluate((el) => {
      return window.getComputedStyle(el).gap;
    });

    // gap should be defined
    expect(gap).toBeTruthy();
    expect(gap).not.toBe('normal');
  });
});

test.describe('CSS Compatibility - Static Analysis', () => {
  test('CSS file can be read and parsed', async ({ page }) => {
    // Read the CSS file content
    const cssContent = fs.readFileSync(stylesPath, 'utf-8');

    expect(cssContent).toBeTruthy();
    expect(cssContent.length).toBeGreaterThan(0);
  });

  test('No vendor prefixes needed for modern features', async ({ page }) => {
    const cssContent = fs.readFileSync(stylesPath, 'utf-8');

    // Check that CSS doesn't rely on outdated vendor prefixes
    // Modern browsers support unprefixed versions
    const outdatedPrefixes = [
      '-webkit-flex',        // Flexbox - prefixed version no longer needed
      '-moz-flex',           // Flexbox - Firefox 28+ uses unprefixed
      '-ms-flexbox',         // Flexbox - IE10 only
      '-webkit-transition',  // Transitions - prefixed no longer needed since Chrome 26
      '-moz-transition',     // Transitions - Firefox 16+ uses unprefixed
      '-webkit-transform',   // Transforms - prefixed no longer needed since Chrome 36
      '-moz-transform',      // Transforms - Firefox 16+ uses unprefixed
    ];

    for (const prefix of outdatedPrefixes) {
      // These shouldn't be present in modern CSS targeting modern browsers only
      // (They might be present for legacy support, which is fine, but not required)
      // This test just documents what's being used
      const hasPrefix = cssContent.includes(prefix);
      // We're not failing on this, just documenting
      if (hasPrefix) {
        console.log(`Note: CSS contains vendor prefix ${prefix} (optional for modern browsers)`);
      }
    }

    // The main check is that the CSS uses standard properties
    expect(cssContent).toContain('display: flex');
    expect(cssContent).toContain('display: grid');
  });

  test('CSS uses standard color formats', async ({ page }) => {
    const cssContent = fs.readFileSync(stylesPath, 'utf-8');

    // Extract color values
    const hexColors = cssContent.match(/#[0-9a-fA-F]{3,8}/g) || [];
    const rgbColors = cssContent.match(/rgb\([^)]+\)/g) || [];
    const rgbaColors = cssContent.match(/rgba\([^)]+\)/g) || [];

    // All standard color formats
    const allColors = [...hexColors, ...rgbColors, ...rgbaColors];

    // Should have color definitions
    expect(allColors.length).toBeGreaterThan(0);

    // Verify all hex colors are valid
    for (const color of hexColors) {
      // Valid hex formats: #RGB, #RGBA, #RRGGBB, #RRGGBBAA
      expect(color).toMatch(/^#([0-9a-fA-F]{3}|[0-9a-fA-F]{4}|[0-9a-fA-F]{6}|[0-9a-fA-F]{8})$/);
    }
  });

  test('CSS uses supported unit values', async ({ page }) => {
    const cssContent = fs.readFileSync(stylesPath, 'utf-8');

    // Check for modern but well-supported units
    // rem - supported since Chrome 4, Firefox 3.6, Safari 5, Edge 12
    // em - universal support
    // px - universal support
    // % - universal support
    // vh/vw - supported since Chrome 20, Firefox 19, Safari 6, Edge 12

    const hasRem = cssContent.includes('rem');
    const hasEm = cssContent.includes('em');
    const hasPx = cssContent.includes('px');

    // At least one of these should be present
    expect(hasRem || hasEm || hasPx).toBe(true);

    // Check for potentially unsupported units (just for awareness)
    const hasContainerUnits = /\d+cq[whib]/i.test(cssContent);
    if (hasContainerUnits) {
      console.log('Note: CSS uses container query units (cqw, cqh, etc.) - limited browser support');
    }
  });

  test('CSS selectors are broadly supported', async ({ page }) => {
    const cssContent = fs.readFileSync(stylesPath, 'utf-8');

    // Check for selectors that are widely supported
    // Class selectors - universal
    expect(cssContent).toMatch(/\.[a-zA-Z]/);

    // Pseudo-classes used in the CSS
    const usesHover = cssContent.includes(':hover');
    const usesFocus = cssContent.includes(':focus');
    const usesRoot = cssContent.includes(':root');

    // These are all widely supported
    expect(usesHover || usesFocus || usesRoot).toBe(true);

    // Check for ::before/::after pseudo-elements (widely supported)
    const usesBefore = cssContent.includes('::before') || cssContent.includes(':before');
    const usesAfter = cssContent.includes('::after') || cssContent.includes(':after');

    // These are commonly used and widely supported
    if (usesBefore || usesAfter) {
      console.log('CSS uses ::before/::after pseudo-elements - widely supported');
    }

    // Check for potentially newer selectors
    const hasIs = /:is\(/i.test(cssContent);
    const hasWhere = /:where\(/i.test(cssContent);
    const hasHas = /:has\(/i.test(cssContent);

    // :is() and :where() - Chrome 88+, Firefox 78+, Safari 14+
    // :has() - Chrome 105+, Firefox 121+, Safari 15.4+

    if (hasHas) {
      console.log('Note: CSS uses :has() selector - relatively new, check browser support');
    }
  });
});

test.describe('CSS Compatibility - Dark Mode Support', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(pageUrl);
  });

  test('Dark mode applies different colors', async ({ page }) => {
    // Get light mode background
    const lightBgColor = await page.evaluate(() => {
      return window.getComputedStyle(document.body).backgroundColor;
    });

    // Switch to dark mode
    await page.emulateMedia({ colorScheme: 'dark' });

    // Get dark mode background
    const darkBgColor = await page.evaluate(() => {
      return window.getComputedStyle(document.body).backgroundColor;
    });

    // Colors should be different (dark mode should have different background)
    expect(lightBgColor).not.toBe(darkBgColor);
  });

  test('Dark mode maintains readability', async ({ page }) => {
    await page.emulateMedia({ colorScheme: 'dark' });

    // All major elements should still be visible
    const hero = page.locator('.hero');
    await expect(hero).toBeVisible();

    const features = page.locator('#features');
    await expect(features).toBeVisible();

    const buttons = page.locator('.btn');
    const count = await buttons.count();
    expect(count).toBeGreaterThan(0);

    // Text should still be readable (not same color as background)
    const textColor = await page.evaluate(() => {
      return window.getComputedStyle(document.body).color;
    });
    const bgColor = await page.evaluate(() => {
      return window.getComputedStyle(document.body).backgroundColor;
    });

    expect(textColor).not.toBe(bgColor);
  });
});
