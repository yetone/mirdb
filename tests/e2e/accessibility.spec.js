/**
 * E2E Tests: Accessibility Compliance
 * Owner: Scenario 10 - Accessibility Compliance
 *
 * Test coverage:
 * - axe-core scan returns zero WCAG 2.1 AA violations
 * - All interactive elements are keyboard accessible
 * - Focus indicators are visible
 * - Images have alt text
 * - Heading hierarchy is correct
 * - Color contrast meets AA standards
 * - ARIA landmarks are present
 */

const { test, expect } = require('@playwright/test');
const { AxeBuilder } = require('@axe-core/playwright');

test.describe('Accessibility Compliance', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');
  });

  test('axe-core scan returns zero WCAG 2.1 AA violations', async ({ page }) => {
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21aa'])
      .analyze();

    expect(accessibilityScanResults.violations).toHaveLength(0);
  });

  test('all interactive elements are reachable via keyboard', async ({ page }) => {
    // Get all interactive elements
    const interactiveElements = await page.locator('a, button, [tabindex]:not([tabindex="-1"])').all();
    expect(interactiveElements.length).toBeGreaterThan(0);

    // Focus the first element and then tab through all
    await page.keyboard.press('Tab');

    const focusedTagNames = new Set();
    const maxTabs = interactiveElements.length + 5;

    for (let i = 0; i < maxTabs; i++) {
      const activeElement = await page.evaluate(() => document.activeElement?.tagName.toLowerCase());
      const activeElementRole = await page.evaluate(() => {
        const el = document.activeElement;
        return el ? el.getAttribute('role') || el.tagName.toLowerCase() : null;
      });

      if (activeElement && activeElement !== 'body') {
        focusedTagNames.add(activeElementRole);
      }

      // Check if we've cycled back to the beginning
      if (i > 0 && activeElement === 'body') {
        break;
      }

      await page.keyboard.press('Tab');
    }

    // Verify we can reach links and buttons via keyboard
    const focusableTypes = Array.from(focusedTagNames);
    expect(focusableTypes.some(t => t === 'a' || t === 'link' || t === 'tab')).toBe(true);
    expect(focusableTypes.some(t => t === 'button')).toBe(true);
  });

  test('focus indicators are visible on all focusable elements', async ({ page }) => {
    const focusableSelectors = [
      'a',
      'button',
      '[tabindex]:not([tabindex="-1"])',
    ];

    for (const selector of focusableSelectors) {
      const elements = await page.locator(selector).all();

      for (const element of elements) {
        // Skip elements that are not visible
        const isVisible = await element.isVisible().catch(() => false);
        if (!isVisible) continue;

        // Focus the element
        await element.focus();

        // Check that focus indicator is visible
        const hasVisibleFocus = await element.evaluate((el) => {
          const styles = window.getComputedStyle(el);
          const outlineWidth = parseFloat(styles.outlineWidth);
          const outlineStyle = styles.outlineStyle;
          const outlineColor = styles.outlineColor;
          const boxShadow = styles.boxShadow;
          const borderWidth = parseFloat(styles.borderWidth);

          // Check for outline
          const hasOutline = outlineWidth > 0 && outlineStyle !== 'none' && outlineColor !== 'transparent';

          // Check for visible box-shadow (focus ring)
          const hasBoxShadow = boxShadow && boxShadow !== 'none';

          // Check for border change
          const hasBorder = borderWidth > 0;

          // Also check :focus-visible pseudo-class styles if applicable
          const isKeyboardFocused = el.matches(':focus-visible');

          return hasOutline || hasBoxShadow || isKeyboardFocused;
        });

        expect(hasVisibleFocus).toBe(true);
      }
    }
  });

  test('all img elements have non-empty alt text', async ({ page }) => {
    const images = await page.locator('img').all();

    for (const img of images) {
      const alt = await img.getAttribute('alt');
      expect(alt).not.toBeNull();
      expect(alt?.trim()).not.toBe('');
    }
  });

  test('decorative images and icons are hidden from assistive technology', async ({ page }) => {
    const svgs = await page.locator('svg').all();

    for (const svg of svgs) {
      // Check if SVG itself is hidden
      const ariaHidden = await svg.getAttribute('aria-hidden');
      const hasTitle = await svg.locator('title').count() > 0;
      const role = await svg.getAttribute('role');

      // Check if any ancestor has aria-hidden="true"
      const ancestorHidden = await svg.evaluate((el) => {
        let current = el.parentElement;
        while (current) {
          if (current.getAttribute('aria-hidden') === 'true') return true;
          current = current.parentElement;
        }
        return false;
      });

      // SVG is accessible if it or an ancestor is hidden, or it has a title/role
      const isAccessible = ariaHidden === 'true' || ancestorHidden || hasTitle || role === 'img';
      expect(isAccessible).toBe(true);
    }
  });

  test('heading hierarchy follows proper semantic order', async ({ page }) => {
    const headings = await page.locator('h1, h2, h3, h4, h5, h6').all();
    expect(headings.length).toBeGreaterThan(0);

    const headingLevels = [];
    for (const heading of headings) {
      const tagName = await heading.evaluate(el => el.tagName.toLowerCase());
      const level = parseInt(tagName.replace('h', ''), 10);
      headingLevels.push(level);
    }

    // Check for single h1
    const h1Count = headingLevels.filter(l => l === 1).length;
    expect(h1Count).toBe(1);

    // Check that headings don't skip levels
    for (let i = 1; i < headingLevels.length; i++) {
      const prevLevel = headingLevels[i - 1];
      const currLevel = headingLevels[i];
      // Current level should not be more than 1 level deeper than previous
      // (h2 -> h4 is a skip, but h3 -> h2 is OK for subsections ending)
      if (currLevel > prevLevel) {
        expect(currLevel - prevLevel).toBeLessThanOrEqual(1);
      }
    }
  });

  test('page has proper ARIA landmarks and semantic HTML5 elements', async ({ page }) => {
    // Check for main landmark
    const main = await page.locator('main').count();
    expect(main).toBeGreaterThanOrEqual(1);

    // Check for navigation landmark
    const nav = await page.locator('nav').count();
    expect(nav).toBeGreaterThanOrEqual(1);

    // Check for footer
    const footer = await page.locator('footer').count();
    expect(footer).toBeGreaterThanOrEqual(1);

    // Check for header
    const header = await page.locator('header').count();
    expect(header).toBeGreaterThanOrEqual(1);

    // Nav should have aria-label
    const navElement = page.locator('nav').first();
    const navAriaLabel = await navElement.getAttribute('aria-label');
    expect(navAriaLabel).toBeTruthy();
  });

  test('skip link is present for bypassing navigation', async ({ page }) => {
    const skipLink = page.locator('a[href="#main-content"], .skip-link, [class*="skip"]');
    await expect(skipLink).toBeVisible();

    const href = await skipLink.getAttribute('href');
    expect(href).toBe('#main-content');
  });

  test('color contrast meets WCAG AA for body text programmatically', async ({ page }) => {
    // Sample common text elements and verify contrast
    const textSelectors = ['body', 'p', 'a', 'h1', 'h2', 'h3', 'button'];

    for (const selector of textSelectors) {
      const elements = await page.locator(selector).all();
      for (const element of elements) {
        const isVisible = await element.isVisible().catch(() => false);
        if (!isVisible) continue;

        const contrast = await element.evaluate((el) => {
          const styles = window.getComputedStyle(el);
          const color = styles.color;
          const bgColor = styles.backgroundColor;

          // Parse RGB values
          const parseColor = (colorStr) => {
            const match = colorStr.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)(?:,\s*([\d.]+))?\)/);
            if (!match) return null;
            return {
              r: parseInt(match[1], 10),
              g: parseInt(match[2], 10),
              b: parseInt(match[3], 10),
              a: match[4] ? parseFloat(match[4]) : 1,
            };
          };

          const textRgb = parseColor(color);
          const bgRgb = parseColor(bgColor);

          if (!textRgb || !bgRgb) return null;

          // Calculate relative luminance
          const luminance = (rgb) => {
            const rsRGB = rgb.r / 255;
            const gsRGB = rgb.g / 255;
            const bsRGB = rgb.b / 255;

            const r = rsRGB <= 0.03928 ? rsRGB / 12.92 : Math.pow((rsRGB + 0.055) / 1.055, 2.4);
            const g = gsRGB <= 0.03928 ? gsRGB / 12.92 : Math.pow((gsRGB + 0.055) / 1.055, 2.4);
            const b = bsRGB <= 0.03928 ? bsRGB / 12.92 : Math.pow((bsRGB + 0.055) / 1.055, 2.4);

            return 0.2126 * r + 0.7152 * g + 0.0722 * b;
          };

          const textLum = luminance(textRgb);
          const bgLum = luminance(bgRgb);

          const lighter = Math.max(textLum, bgLum);
          const darker = Math.min(textLum, bgLum);

          return (lighter + 0.05) / (darker + 0.05);
        });

        if (contrast !== null) {
          expect(contrast).toBeGreaterThanOrEqual(4.5);
        }
      }
    }
  });

  test('buttons have accessible names', async ({ page }) => {
    const buttons = await page.locator('button').all();
    expect(buttons.length).toBeGreaterThan(0);

    for (const button of buttons) {
      const isVisible = await button.isVisible().catch(() => false);
      if (!isVisible) continue;

      // Check for aria-label, aria-labelledby, or text content
      const ariaLabel = await button.getAttribute('aria-label');
      const ariaLabelledBy = await button.getAttribute('aria-labelledby');
      const textContent = await button.textContent();
      const title = await button.getAttribute('title');

      const hasAccessibleName = !!(ariaLabel || ariaLabelledBy || (textContent && textContent.trim().length > 0) || title);
      expect(hasAccessibleName).toBe(true);
    }
  });

  test('links have discernible text', async ({ page }) => {
    const links = await page.locator('a').all();

    for (const link of links) {
      const isVisible = await link.isVisible().catch(() => false);
      if (!isVisible) continue;

      const textContent = await link.textContent();
      const ariaLabel = await link.getAttribute('aria-label');
      const ariaLabelledBy = await link.getAttribute('aria-labelledby');
      const title = await link.getAttribute('title');

      const hasAccessibleText = (textContent && textContent.trim().length > 0) || ariaLabel || ariaLabelledBy || title;
      expect(hasAccessibleText).toBe(true);
    }
  });

  test('tables have proper header associations', async ({ page }) => {
    const tables = await page.locator('table').all();

    for (const table of tables) {
      const headers = await table.locator('th').all();
      expect(headers.length).toBeGreaterThan(0);

      for (const header of headers) {
        const scope = await header.getAttribute('scope');
        // Headers should have scope attribute for accessibility
        expect(scope).toMatch(/^(col|row)$/);
      }
    }
  });

  test('form controls and interactive elements have associated labels', async ({ page }) => {
    // Check tablist has aria-label
    const tablists = await page.locator('[role="tablist"]').all();
    for (const tablist of tablists) {
      const ariaLabel = await tablist.getAttribute('aria-label');
      expect(ariaLabel).toBeTruthy();
    }

    // Check tabs have proper ARIA attributes
    const tabs = await page.locator('[role="tab"]').all();
    for (const tab of tabs) {
      const ariaSelected = await tab.getAttribute('aria-selected');
      const ariaControls = await tab.getAttribute('aria-controls');
      expect(ariaSelected).toMatch(/^(true|false)$/);
      expect(ariaControls).toBeTruthy();
    }
  });

  test('page language is declared', async ({ page }) => {
    const htmlLang = await page.locator('html').getAttribute('lang');
    expect(htmlLang).toBe('en');
  });
});
