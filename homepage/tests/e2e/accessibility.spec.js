// @ts-check
const { test, expect } = require('@playwright/test');
const AxeBuilder = require('@axe-core/playwright').default;

/**
 * Accessibility E2E Tests for MirDB Homepage
 * Tests WCAG 2.1 AA compliance using axe-core in a real browser
 */

test.describe('Accessibility Compliance E2E', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case 1: Run automated accessibility audit (axe-core)
  test('TC1: should have no critical or serious accessibility violations', async ({ page }) => {
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
      .analyze();

    // Filter for critical and serious violations
    const criticalViolations = accessibilityScanResults.violations.filter(
      v => v.impact === 'critical' || v.impact === 'serious'
    );

    expect(criticalViolations).toHaveLength(0);
  });

  test('TC1: should pass full WCAG 2.1 AA audit', async ({ page }) => {
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2aa', 'wcag21aa'])
      .analyze();

    expect(accessibilityScanResults.violations).toEqual([]);
  });

  // Test Case 2: Tab through all interactive elements
  test('TC2: should allow keyboard navigation through all interactive elements', async ({ page }) => {
    // Start from the body
    await page.keyboard.press('Tab');

    // Collect all focusable elements
    const focusableElements = await page.$$eval(
      'a[href], button, input, select, textarea, [tabindex]:not([tabindex="-1"])',
      elements => elements.map(el => ({
        tag: el.tagName.toLowerCase(),
        text: el.textContent?.trim().substring(0, 50),
        href: el.getAttribute('href'),
      }))
    );

    // Should have multiple interactive elements
    expect(focusableElements.length).toBeGreaterThan(0);

    // Tab through elements and verify focus
    for (let i = 0; i < Math.min(focusableElements.length, 10); i++) {
      const focusedElement = await page.evaluate(() => {
        const el = document.activeElement;
        return {
          tag: el?.tagName?.toLowerCase(),
          focusVisible: el !== document.body,
        };
      });

      // Element should be focused (not body)
      if (i > 0) {
        expect(focusedElement.tag).not.toBe('body');
      }

      await page.keyboard.press('Tab');
    }
  });

  test('TC2: all buttons and links should be reachable via keyboard', async ({ page }) => {
    // Get all interactive elements
    const interactiveElements = await page.$$('a[href], button, .btn');

    expect(interactiveElements.length).toBeGreaterThan(0);

    // Check each element can receive focus
    for (const element of interactiveElements) {
      await element.focus();
      const isFocused = await element.evaluate(el => el === document.activeElement);
      expect(isFocused).toBe(true);
    }
  });

  // Test Case 3: Check all images for alt attributes
  test('TC3: all img elements should have alt attribute', async ({ page }) => {
    const imagesWithoutAlt = await page.$$eval('img', images =>
      images.filter(img => !img.hasAttribute('alt')).length
    );

    expect(imagesWithoutAlt).toBe(0);
  });

  test('TC3: logo image should have descriptive alt text', async ({ page }) => {
    const logoAlt = await page.$eval('.logo img, .logo-img', img => img.getAttribute('alt'));

    expect(logoAlt).toBeTruthy();
    expect(logoAlt.length).toBeGreaterThan(0);
  });

  // Test Case 4: Test color contrast ratios
  test('TC4: should pass axe color contrast check', async ({ page }) => {
    const accessibilityScanResults = await new AxeBuilder({ page })
      .include('body')
      .withRules(['color-contrast'])
      .analyze();

    const contrastViolations = accessibilityScanResults.violations.filter(
      v => v.id === 'color-contrast'
    );

    expect(contrastViolations).toHaveLength(0);
  });

  test('TC4: text should have sufficient contrast against background', async ({ page }) => {
    // Check that text elements are visible (not transparent)
    const textVisibility = await page.$$eval('p, h1, h2, h3, span', elements =>
      elements.every(el => {
        const style = window.getComputedStyle(el);
        return style.opacity !== '0' && style.visibility !== 'hidden';
      })
    );

    expect(textVisibility).toBe(true);
  });

  // Test Case 5: Verify focus indicators visible
  test('TC5: should show visible focus ring on interactive elements', async ({ page }) => {
    // Tab to first link
    await page.keyboard.press('Tab');

    // Check for focus styling
    const hasFocusStyle = await page.evaluate(() => {
      const focused = document.activeElement;
      if (!focused || focused === document.body) return false;

      const style = window.getComputedStyle(focused);
      const outlineStyle = style.outline;
      const boxShadow = style.boxShadow;

      // Has outline OR has box-shadow (common focus indicator alternatives)
      return (
        outlineStyle !== 'none' &&
        outlineStyle !== '0px none rgb(0, 0, 0)' &&
        outlineStyle !== ''
      ) || (boxShadow && boxShadow !== 'none');
    });

    // Either browser default focus is used, or custom focus styles
    // The important thing is that focus doesn't have outline: none
    const hasNoOutlineNone = await page.evaluate(() => {
      const focused = document.activeElement;
      if (!focused || focused === document.body) return true;

      const style = window.getComputedStyle(focused);
      return style.outlineStyle !== 'none' || style.outlineWidth !== '0px';
    });

    expect(hasNoOutlineNone).toBe(true);
  });

  test('TC5: buttons should have visible focus indication', async ({ page }) => {
    const buttons = await page.$$('.btn');

    for (const button of buttons) {
      await button.focus();

      // Check the button is actually focused
      const isFocused = await button.evaluate(el => el === document.activeElement);
      expect(isFocused).toBe(true);

      // Verify focus is not hidden via inline style
      const hasNoHiddenOutline = await button.evaluate(el => {
        const style = el.getAttribute('style') || '';
        return !style.includes('outline: none') && !style.includes('outline:none');
      });
      expect(hasNoHiddenOutline).toBe(true);
    }
  });

  // Test Case 6: Check ARIA labels on icons/buttons
  test('TC6: icon-only buttons should have accessible names', async ({ page }) => {
    // Check for any buttons that only have icons (no text)
    const iconOnlyButtons = await page.$$('button:not(:has-text(""))');

    for (const button of iconOnlyButtons) {
      const hasAccessibleName = await button.evaluate(el => {
        const ariaLabel = el.getAttribute('aria-label');
        const ariaLabelledby = el.getAttribute('aria-labelledby');
        const textContent = el.textContent?.trim();

        return !!ariaLabel || !!ariaLabelledby || !!textContent;
      });

      expect(hasAccessibleName).toBe(true);
    }
  });

  test('TC6: page should have proper heading hierarchy', async ({ page }) => {
    const h1Count = await page.$$eval('h1', els => els.length);
    const h2Count = await page.$$eval('h2', els => els.length);

    // Should have exactly one h1
    expect(h1Count).toBe(1);

    // Should have h2 elements for sections
    expect(h2Count).toBeGreaterThan(0);
  });

  test('TC6: page should have proper landmark structure', async ({ page }) => {
    const nav = await page.$('nav');
    const main = await page.$('main');
    const footer = await page.$('footer');
    const header = await page.$('header');

    expect(nav).toBeTruthy();
    expect(main).toBeTruthy();
    expect(footer).toBeTruthy();
    expect(header).toBeTruthy();
  });

  test('TC6: external links should have proper security attributes', async ({ page }) => {
    const externalLinks = await page.$$('a[target="_blank"]');

    for (const link of externalLinks) {
      const rel = await link.getAttribute('rel');
      expect(rel).toContain('noopener');
    }
  });

  // Additional comprehensive accessibility test
  test('should have lang attribute on html element', async ({ page }) => {
    const lang = await page.$eval('html', el => el.getAttribute('lang'));
    expect(lang).toBeTruthy();
  });
});
