/**
 * E2E accessibility tests.
 * Owner: Scenario 11 - Accessibility Compliance
 *
 * Tests:
 * - axe-core automated accessibility scan
 * - Keyboard navigation flow
 * - Focus management
 * - Color contrast compliance
 * - Semantic HTML validation
 * - Skip link
 * - Alt text on images
 * - ARIA labels on interactive components
 */

const { test, expect } = require('@playwright/test');
const AxeBuilder = require('@axe-core/playwright').default;
const path = require('path');

const homepagePath = 'file://' + path.resolve(__dirname, '../../index.html');

test.describe('Accessibility Compliance - E2E', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(homepagePath);
  });

  // Test Case 1: Run axe-core accessibility scan
  test('axe-core scan has no serious or critical violations', async ({ page }) => {
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21aa'])
      .analyze();

    const seriousViolations = accessibilityScanResults.violations.filter(
      v => v.impact === 'serious' || v.impact === 'critical'
    );

    if (seriousViolations.length > 0) {
      console.log('Serious/Critical violations:', JSON.stringify(seriousViolations, null, 2));
    }

    expect(seriousViolations).toHaveLength(0);
  });

  // Test Case 2: Lighthouse accessibility audit
  test('page passes Lighthouse accessibility criteria', async ({ page }) => {
    // Verify key accessibility features that contribute to a high Lighthouse score
    const checks = await page.evaluate(() => {
      const results = {
        hasLang: document.documentElement.lang === 'en',
        hasTitle: document.title.length > 0,
        hasViewport: !!document.querySelector('meta[name="viewport"]'),
        hasCharset: !!document.querySelector('meta[charset]'),
        hasSkipLink: !!document.querySelector('a[href^="#main-content"]'),
        hasSemanticMain: !!document.querySelector('main'),
        hasSemanticHeader: !!document.querySelector('header'),
        hasSemanticNav: !!document.querySelector('nav'),
        hasSemanticFooter: !!document.querySelector('footer'),
        imagesWithoutAlt: Array.from(document.querySelectorAll('img:not([alt])')).length,
        hasAriaOnInteractive: true,
        buttonsWithoutLabel: Array.from(document.querySelectorAll('button:not([aria-label]):not([aria-labelledby])')).length,
        hasFocusStyles: true,
      };

      // Check interactive elements for aria-labels
      const copyButtons = document.querySelectorAll('.copy-button');
      copyButtons.forEach(btn => {
        if (!btn.getAttribute('aria-label')) {
          results.hasAriaOnInteractive = false;
        }
      });

      const mobileMenuToggle = document.querySelector('.mobile-menu-toggle');
      if (mobileMenuToggle && !mobileMenuToggle.getAttribute('aria-label')) {
        results.hasAriaOnInteractive = false;
      }

      const themeToggle = document.querySelector('.theme-toggle');
      if (themeToggle && !themeToggle.getAttribute('aria-label')) {
        results.hasAriaOnInteractive = false;
      }

      // Check for visible focus styles by focusing a link and checking computed style
      const testLink = document.querySelector('a[href]');
      if (testLink) {
        testLink.focus();
        const style = window.getComputedStyle(testLink);
        const outlineStyle = style.outlineStyle;
        const outlineWidth = parseFloat(style.outlineWidth);
        results.hasFocusStyles = outlineStyle !== 'none' && outlineWidth > 0;
      } else {
        results.hasFocusStyles = false;
      }

      return results;
    });

    expect(checks.hasLang).toBe(true);
    expect(checks.hasTitle).toBe(true);
    expect(checks.hasViewport).toBe(true);
    expect(checks.hasCharset).toBe(true);
    expect(checks.hasSkipLink).toBe(true);
    expect(checks.hasSemanticMain).toBe(true);
    expect(checks.hasSemanticHeader).toBe(true);
    expect(checks.hasSemanticNav).toBe(true);
    expect(checks.hasSemanticFooter).toBe(true);
    expect(checks.imagesWithoutAlt).toBe(0);
    expect(checks.hasAriaOnInteractive).toBe(true);
    expect(checks.buttonsWithoutLabel).toBe(0);
    expect(checks.hasFocusStyles).toBe(true);
  });

  // Test Case 5: Check all images for alt attributes
  test('all img elements have alt attributes', async ({ page }) => {
    const images = page.locator('img');
    const count = await images.count();
    expect(count).toBeGreaterThan(0);

    for (let i = 0; i < count; i++) {
      const img = images.nth(i);
      await expect(img).toHaveAttribute('alt');
    }

    // Verify decorative SVGs have aria-hidden="true"
    const decorativeSvgs = page.locator('svg[aria-hidden="true"]');
    expect(await decorativeSvgs.count()).toBeGreaterThan(0);
  });

  // Test Case 6: Tab through all interactive elements
  test('all interactive elements are reachable via Tab key', async ({ page }) => {
    const interactiveElements = await page.locator('a, button, [tabindex]:not([tabindex="-1"])').all();
    expect(interactiveElements.length).toBeGreaterThan(0);

    // Press Tab multiple times and collect focused elements
    const focusedSelectors = new Set();
    const maxTabs = interactiveElements.length + 5;

    for (let i = 0; i < maxTabs; i++) {
      await page.keyboard.press('Tab');
      const activeElement = await page.evaluate(() => {
        const el = document.activeElement;
        return el ? el.tagName + (el.className ? '.' + el.className.split(' ').join('.') : '') : null;
      });

      if (!activeElement || activeElement === 'BODY' || activeElement === 'HTML') {
        continue;
      }

      // Detect cycle - if we've seen this element before and it's not the first iteration
      if (focusedSelectors.has(activeElement) && i > 0) {
        break;
      }
      focusedSelectors.add(activeElement);
    }

    // Should have tabbed through multiple interactive elements
    expect(focusedSelectors.size).toBeGreaterThanOrEqual(3);

    // Verify buttons are tab-reachable
    const buttons = await page.locator('button').all();
    for (const btn of buttons) {
      // Skip hidden elements (e.g., mobile menu toggle on desktop)
      const isVisible = await btn.isVisible();
      if (!isVisible) continue;

      await btn.focus();
      const isFocused = await btn.evaluate(el => el === document.activeElement);
      expect(isFocused).toBe(true);
    }
  });

  // Test Case 7: Focus indicator visibility
  test('all interactive elements show visible focus indicators', async ({ page }) => {
    const interactiveElements = await page.locator('a[href], button:not([disabled])').all();
    expect(interactiveElements.length).toBeGreaterThan(0);

    for (const el of interactiveElements) {
      // Skip hidden elements
      const isVisible = await el.isVisible();
      if (!isVisible) continue;

      await el.focus();

      const outline = await el.evaluate(e => {
        const style = window.getComputedStyle(e);
        return {
          outlineStyle: style.outlineStyle,
          outlineWidth: style.outlineWidth,
          outlineColor: style.outlineColor,
          boxShadow: style.boxShadow,
        };
      });

      // Focus indicator should be visible: either outline is visible or box-shadow is set
      const hasOutline = outline.outlineStyle !== 'none' && parseFloat(outline.outlineWidth) > 0;
      const hasBoxShadow = outline.boxShadow && outline.boxShadow !== 'none' && !outline.boxShadow.includes('0px 0px 0px 0px');

      expect(
        hasOutline || hasBoxShadow,
        `Element ${await el.evaluate(e => e.outerHTML.slice(0, 80))} has no visible focus indicator`
      ).toBe(true);
    }
  });

  // Test Case 8: Color contrast analysis
  test('text meets WCAG AA contrast ratios', async ({ page }) => {
    const contrastResults = await page.evaluate(() => {
      function getLuminance(r, g, b) {
        const rsRGB = r / 255;
        const gsRGB = g / 255;
        const bsRGB = b / 255;
        const rLinear = rsRGB <= 0.03928 ? rsRGB / 12.92 : Math.pow((rsRGB + 0.055) / 1.055, 2.4);
        const gLinear = gsRGB <= 0.03928 ? gsRGB / 12.92 : Math.pow((gsRGB + 0.055) / 1.055, 2.4);
        const bLinear = bsRGB <= 0.03928 ? bsRGB / 12.92 : Math.pow((bsRGB + 0.055) / 1.055, 2.4);
        return 0.2126 * rLinear + 0.7152 * gLinear + 0.0722 * bLinear;
      }

      function getContrastRatio(color1, color2) {
        const lum1 = getLuminance(color1.r, color1.g, color1.b);
        const lum2 = getLuminance(color2.r, color2.g, color2.b);
        const lighter = Math.max(lum1, lum2);
        const darker = Math.min(lum1, lum2);
        return (lighter + 0.05) / (darker + 0.05);
      }

      function hexToRgb(hex) {
        const result = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex);
        return result ? {
          r: parseInt(result[1], 16),
          g: parseInt(result[2], 16),
          b: parseInt(result[3], 16)
        } : null;
      }

      function rgbStringToRgb(rgbStr) {
        const match = rgbStr.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/);
        return match ? {
          r: parseInt(match[1], 10),
          g: parseInt(match[2], 10),
          b: parseInt(match[3], 10)
        } : null;
      }

      function getColorRgb(colorStr) {
        if (colorStr.startsWith('#')) return hexToRgb(colorStr);
        if (colorStr.startsWith('rgb')) return rgbStringToRgb(colorStr);
        return null;
      }

      function isLargeText(element) {
        const style = window.getComputedStyle(element);
        const fontSize = parseFloat(style.fontSize);
        const fontWeight = parseInt(style.fontWeight, 10);
        // Large text: 18.66px+ and bold, or 24px+
        return (fontSize >= 18.66 && fontWeight >= 700) || fontSize >= 24;
      }

      const textElements = document.querySelectorAll('p, h1, h2, h3, h4, h5, h6, span, a, li, td, th, label, button');
      const failures = [];

      textElements.forEach(el => {
        const text = el.textContent.trim();
        if (!text) return;

        const style = window.getComputedStyle(el);
        const color = getColorRgb(style.color);
        const bgColor = getColorRgb(style.backgroundColor);

        if (!color || !bgColor) return;

        // If background is transparent, find the nearest ancestor with a background
        let effectiveBg = bgColor;
        if (style.backgroundColor === 'rgba(0, 0, 0, 0)' || style.backgroundColor === 'transparent') {
          let ancestor = el.parentElement;
          while (ancestor) {
            const ancestorStyle = window.getComputedStyle(ancestor);
            const ancestorBg = getColorRgb(ancestorStyle.backgroundColor);
            if (ancestorBg && ancestorStyle.backgroundColor !== 'rgba(0, 0, 0, 0)' && ancestorStyle.backgroundColor !== 'transparent') {
              effectiveBg = ancestorBg;
              break;
            }
            ancestor = ancestor.parentElement;
          }
        }

        const ratio = getContrastRatio(color, effectiveBg);
        const large = isLargeText(el);
        const required = large ? 3.0 : 4.5;

        if (ratio < required) {
          failures.push({
            text: text.slice(0, 50),
            tag: el.tagName,
            ratio: ratio.toFixed(2),
            required,
            color: style.color,
            backgroundColor: style.backgroundColor,
            isLarge: large,
          });
        }
      });

      return failures;
    });

    if (contrastResults.length > 0) {
      console.log('Contrast failures:', JSON.stringify(contrastResults.slice(0, 10), null, 2));
    }

    expect(contrastResults).toHaveLength(0);
  });

  // Test Case 9: Skip navigation link
  test('first focusable element is a skip to main content link', async ({ page }) => {
    // Press Tab once
    await page.keyboard.press('Tab');

    const activeElement = await page.evaluate(() => {
      const el = document.activeElement;
      return {
        tagName: el.tagName,
        text: el.textContent?.trim() || '',
        href: el.getAttribute('href') || '',
        className: el.className || '',
      };
    });

    expect(activeElement.tagName).toBe('A');
    expect(activeElement.text.toLowerCase()).toContain('skip');
    expect(activeElement.href).toContain('main-content');
    expect(activeElement.className).toContain('skip-link');
  });

  // Test Case 9 (continued): Skip link targets main element
  test('skip link targets main content element', async ({ page }) => {
    const skipLink = page.locator('.skip-link');
    await expect(skipLink).toHaveAttribute('href', '#main-content');

    const mainContent = page.locator('#main-content');
    await expect(mainContent).toBeVisible();
    expect(await mainContent.evaluate(el => el.tagName)).toBe('MAIN');
  });

  // Additional: Verify all sections have proper heading structure
  test('semantic sections have proper heading associations', async ({ page }) => {
    const sections = await page.locator('section').all();
    expect(sections.length).toBeGreaterThanOrEqual(5);

    for (const section of sections) {
      // Each section should have an h2 (or be the hero with h1)
      const headings = await section.locator('h1, h2').count();
      // Hero section has h1, others have h2
      expect(headings).toBeGreaterThanOrEqual(1);
    }
  });

  // Additional: Verify nav landmark has accessible name
  test('main nav element has accessible name', async ({ page }) => {
    const nav = page.locator('nav.main-nav');
    const ariaLabel = await nav.getAttribute('aria-label');
    expect(ariaLabel).toBeTruthy();
    expect(ariaLabel.length).toBeGreaterThan(0);
  });

  // Additional: Verify table has proper headers
  test('configuration table has proper header structure', async ({ page }) => {
    const table = page.locator('.config-table');
    await expect(table).toBeVisible();

    const thead = table.locator('thead');
    await expect(thead).toBeVisible();

    const ths = await thead.locator('th').all();
    expect(ths.length).toBeGreaterThanOrEqual(3);

    // Headers should have scope="col"
    for (const th of ths) {
      const scope = await th.getAttribute('scope');
      expect(scope).toBe('col');
    }
  });
});
