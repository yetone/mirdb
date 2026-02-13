// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Features Section Integration Tests - Scenario 2
 *
 * Test Case 5: Check feature grid on desktop viewport (1024px+)
 * Validates that features display in 3-4 column grid layout on desktop.
 */

test.describe('Features Section Grid Layout', () => {
  // Test Case 5: Check feature grid on desktop viewport (1024px+)
  test('should display features in 3-4 column grid layout on desktop viewport (1024px+)', async ({ page }) => {
    // Set desktop viewport
    await page.setViewportSize({ width: 1200, height: 800 });
    await page.goto('/index.html');

    const featuresGrid = page.locator('#features .features__grid');
    await expect(featuresGrid).toBeVisible();

    // Get the computed grid-template-columns
    const gridStyle = await featuresGrid.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        display: styles.display,
        gridTemplateColumns: styles.gridTemplateColumns,
      };
    });

    // Should use CSS Grid
    expect(gridStyle.display).toBe('grid');

    // grid-template-columns should have 3 or 4 columns
    // The value will be something like "384px 384px 384px" for 3 columns
    const columnValues = gridStyle.gridTemplateColumns.split(' ').filter(v => v.trim() !== '');
    const columnCount = columnValues.length;

    expect(columnCount).toBeGreaterThanOrEqual(3);
    expect(columnCount).toBeLessThanOrEqual(4);
  });

  test('should display features in 2 columns on tablet viewport (640px-1023px)', async ({ page }) => {
    // Set tablet viewport
    await page.setViewportSize({ width: 800, height: 600 });
    await page.goto('/index.html');

    const featuresGrid = page.locator('#features .features__grid');
    await expect(featuresGrid).toBeVisible();

    const gridStyle = await featuresGrid.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        display: styles.display,
        gridTemplateColumns: styles.gridTemplateColumns,
      };
    });

    expect(gridStyle.display).toBe('grid');

    const columnValues = gridStyle.gridTemplateColumns.split(' ').filter(v => v.trim() !== '');
    const columnCount = columnValues.length;

    expect(columnCount).toBe(2);
  });

  test('should display features in 1 column on mobile viewport (<640px)', async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/index.html');

    const featuresGrid = page.locator('#features .features__grid');
    await expect(featuresGrid).toBeVisible();

    const gridStyle = await featuresGrid.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        display: styles.display,
        gridTemplateColumns: styles.gridTemplateColumns,
      };
    });

    expect(gridStyle.display).toBe('grid');

    const columnValues = gridStyle.gridTemplateColumns.split(' ').filter(v => v.trim() !== '');
    const columnCount = columnValues.length;

    expect(columnCount).toBe(1);
  });
});
