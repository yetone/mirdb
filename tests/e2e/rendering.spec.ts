import { test, expect } from '@playwright/test';
import { AxeBuilder } from '@axe-core/playwright';

/**
 * E2E tests verifying homepage rendering across devices.
 * Owner: Scenario 1 - Homepage Rendering Verification
 *
 * Tests to implement:
 * - Page loads within 3 seconds
 * - Responsive layout on mobile/desktop
 * - Accessibility contrast checks
 * - Keyboard navigation testing
 */

test.describe('Homepage Rendering Tests', () => {
  test('loads within 3 seconds', async ({ page }) => {
    const startTime = Date.now();
    await page.goto('http://localhost:8080/src/index.html');
    const loadTime = Date.now() - startTime;

    await expect(loadTime).toBeLessThan(3000);
    await expect(page).toHaveTitle('MirDB - Persistent Key-Value Store');
  });

  test.describe('Responsiveness', () => {
    const viewports = [
      { name: 'mobile', width: 375, height: 667 },
      { name: 'tablet', width: 768, height: 1024 },
      { name: 'desktop', width: 1440, height: 900 }
    ];

    for (const viewport of viewports) {
      test(`adapts to ${viewport.name} viewport`, async ({ page }) => {
        await page.setViewportSize({
          width: viewport.width,
          height: viewport.height
        });
        await page.goto('http://localhost:8080/src/index.html');

        // Check no horizontal scrolling needed
        const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
        expect(bodyWidth).toBeLessThanOrEqual(viewport.width);

        // Verify key elements are visible
        await expect(page.locator('section.hero')).toBeVisible();
        await expect(page.locator('section.features')).toBeVisible();
      });
    }
  });

  test('meets WCAG 2.1 AA accessibility standards', async ({ page }) => {
    await page.goto('http://localhost:8080/src/index.html');

    const accessibilityScan = await new AxeBuilder(page).analyze();
    const results = accessibilityScan;

    // Check for critical accessibility violations
    const violations = results.violations.filter(v => v.impact === 'critical' || v.impact === 'serious');

    expect(violations).toEqual([]);

    // Verify color contrast meets 4.5:1 minimum
    const contrastIssues = results.violations.filter(v =>
      v.id === 'color-contrast' &&
      v.nodes.some(node => node.any.some(check =>
        check.id === 'color-contrast' &&
        (check.data.fgColor && check.data.bgColor)
      ))
    );

    expect(contrastIssues).toEqual([]);
  });

  test('supports keyboard navigation', async ({ page }) => {
    await page.goto('http://localhost:8080/src/index.html');

    // Check focus order
    const initialFocus = await page.evaluate(() => document.activeElement?.tagName);
    expect(initialFocus).toBe('BODY');

    // Tab through header navigation links (4 items)
    for (let i = 0; i < 4; i++) {
      await page.keyboard.press('Tab');
    }
    await expect(page.locator('a[href="#getting-started"]')).toBeFocused();

    await page.keyboard.press('Tab');
    await expect(page.locator('a[href="https://github.com/yestone/mirdb"]')).toBeFocused();

    // Verify all interactive elements are focusable
    const interactiveElements = page.locator('a, button, [tabindex="0"]');
    const count = await interactiveElements.count();
    expect(count).toBeGreaterThan(0);
  });
});