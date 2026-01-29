/**
 * E2E tests for Architecture section responsive behavior.
 * Owner: Scenario 4 - Architecture Visualization
 *
 * Test Case 4: View architecture section on mobile viewport
 */

import { test, expect } from '@playwright/test';

test.describe('Architecture Section Responsive Layout', () => {
  test('Test Case 4: Diagram scales appropriately on mobile viewport (375px)', async ({ page }) => {
    // Set viewport to 375px width (iPhone SE / small mobile)
    await page.setViewportSize({ width: 375, height: 667 });

    // Navigate to the homepage
    await page.goto('/');

    // Wait for page to load and scroll to architecture section
    await page.waitForLoadState('networkidle');

    // Navigate to architecture section
    const architectureSection = page.locator('#architecture');
    await architectureSection.scrollIntoViewIfNeeded();

    // Wait for the architecture section to be visible
    await expect(architectureSection).toBeVisible();

    // Check section heading is visible
    const heading = page.locator('#architecture h2');
    await expect(heading).toBeVisible();
    await expect(heading).toContainText('Architecture');

    // Check that the SVG diagram container is visible
    const diagramContainer = page.locator('.architecture-diagram-container');
    await expect(diagramContainer).toBeVisible();

    // Check the SVG is rendered (lazy loading should have triggered by scrolling)
    const svg = page.locator('.lsm-tree-svg svg');
    await expect(svg).toBeVisible();

    // Get bounding box of the SVG
    const svgBox = await svg.boundingBox();
    expect(svgBox).not.toBeNull();

    if (svgBox) {
      // SVG should fit within viewport width (with some padding)
      expect(svgBox.width).toBeLessThanOrEqual(375);

      // SVG should have reasonable height
      expect(svgBox.height).toBeGreaterThan(100);
    }

    // Check that component descriptions are visible and stacked vertically
    const descriptionCards = page.locator('[data-description]');
    const cardCount = await descriptionCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(4); // At least WAL, Memtable, SSTable, Compaction

    // Verify cards are stacked vertically on mobile
    const firstCard = descriptionCards.first();
    const secondCard = descriptionCards.nth(1);

    const firstBox = await firstCard.boundingBox();
    const secondBox = await secondCard.boundingBox();

    if (firstBox && secondBox) {
      // Cards should be stacked vertically (second below first)
      expect(secondBox.y).toBeGreaterThan(firstBox.y + firstBox.height - 20);

      // Cards should take full width (minus container padding)
      expect(firstBox.width).toBeGreaterThan(300);
    }
  });

  test('Architecture diagram container allows horizontal scroll on very small viewports', async ({ page }) => {
    // Set very narrow viewport
    await page.setViewportSize({ width: 320, height: 568 });

    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Scroll to architecture section
    const architectureSection = page.locator('#architecture');
    await architectureSection.scrollIntoViewIfNeeded();

    // Check the diagram container has overflow handling
    const diagramContainer = page.locator('.architecture-diagram-container');
    await expect(diagramContainer).toBeVisible();

    // The container should be scrollable or the SVG should scale
    const containerStyle = await diagramContainer.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        overflowX: style.overflowX,
        overflowY: style.overflowY,
      };
    });

    // Container should allow horizontal overflow on very small screens
    expect(['auto', 'scroll', 'visible']).toContain(containerStyle.overflowX);
  });

  test('Architecture section displays properly on tablet viewport', async ({ page }) => {
    // Set tablet viewport
    await page.setViewportSize({ width: 768, height: 1024 });

    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Scroll to architecture section
    const architectureSection = page.locator('#architecture');
    await architectureSection.scrollIntoViewIfNeeded();

    await expect(architectureSection).toBeVisible();

    // Check component description grid layout (should show 2 columns on tablet)
    const descriptionCards = page.locator('[data-description]');
    const firstCard = descriptionCards.first();
    const secondCard = descriptionCards.nth(1);

    const firstBox = await firstCard.boundingBox();
    const secondBox = await secondCard.boundingBox();

    if (firstBox && secondBox) {
      // On tablet (md breakpoint), cards might be side by side
      // The Y difference should be small (same row) or large (stacked)
      const yDiff = Math.abs(firstBox.y - secondBox.y);

      // Either side by side (yDiff < 20) or stacked (second below first)
      const isSideBySide = yDiff < 20;
      const isStacked = secondBox.y > firstBox.y + firstBox.height - 20;

      expect(isSideBySide || isStacked).toBe(true);
    }
  });

  test('Architecture section displays full diagram on desktop viewport', async ({ page }) => {
    // Set desktop viewport
    await page.setViewportSize({ width: 1280, height: 800 });

    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Scroll to architecture section
    const architectureSection = page.locator('#architecture');
    await architectureSection.scrollIntoViewIfNeeded();

    await expect(architectureSection).toBeVisible();

    // SVG should be fully visible without scrolling
    const svg = page.locator('.lsm-tree-svg svg');
    await expect(svg).toBeVisible();

    const svgBox = await svg.boundingBox();
    expect(svgBox).not.toBeNull();

    if (svgBox) {
      // SVG should have comfortable width on desktop
      expect(svgBox.width).toBeGreaterThan(500);
    }

    // Component descriptions should be in 3-column grid on desktop
    const descriptionCards = page.locator('[data-description]');
    const firstCard = descriptionCards.first();
    const secondCard = descriptionCards.nth(1);
    const thirdCard = descriptionCards.nth(2);

    const firstBox = await firstCard.boundingBox();
    const secondBox = await secondCard.boundingBox();
    const thirdBox = await thirdCard.boundingBox();

    if (firstBox && secondBox && thirdBox) {
      // On desktop (lg breakpoint), first 3 cards should be side by side
      const yDiff12 = Math.abs(firstBox.y - secondBox.y);
      const yDiff23 = Math.abs(secondBox.y - thirdBox.y);

      expect(yDiff12).toBeLessThan(20); // Same row
      expect(yDiff23).toBeLessThan(20); // Same row
    }
  });

  test('Test Case 5: Diagram is lazy loaded when scrolled into view', async ({ page }) => {
    await page.setViewportSize({ width: 1280, height: 800 });

    await page.goto('/');

    // Initially, the architecture section is below the fold
    // Check that the data-loaded attribute is set after scrolling

    // Scroll to architecture section
    const architectureSection = page.locator('#architecture');
    await architectureSection.scrollIntoViewIfNeeded();

    // Wait for lazy loading to complete
    await page.waitForTimeout(500);

    // Check that the diagram container is marked as loaded
    const diagramContainer = page.locator('[data-lazy-diagram]');

    // The data-loaded attribute should be set to "true" after scrolling
    await expect(diagramContainer).toHaveAttribute('data-loaded', 'true');

    // SVG should now be visible
    const svg = page.locator('.lsm-tree-svg svg');
    await expect(svg).toBeVisible();
  });
});
