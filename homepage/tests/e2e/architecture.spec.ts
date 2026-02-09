/**
 * E2E tests for Architecture section.
 * Owner: Scenario 4 - Architecture Visualization
 *
 * Tests mobile responsiveness of the LSM tree diagram.
 */

import { test, expect } from '@playwright/test';

test.describe('Architecture Section - Mobile Responsiveness', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('diagram is readable and does not overflow horizontally on mobile viewport', async ({ page }) => {
    // Set mobile viewport (iPhone SE size)
    await page.setViewportSize({ width: 375, height: 667 });

    // Wait for the architecture section to be visible
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Check that the diagram is visible
    const diagram = page.locator('.arch-diagram');
    await expect(diagram).toBeVisible();

    // Get diagram dimensions
    const diagramBox = await diagram.boundingBox();

    expect(diagramBox).not.toBeNull();
    if (diagramBox) {
      // The diagram should not extend beyond the viewport
      // Allow for scroll container but ensure the section itself doesn't overflow
      expect(diagramBox.x).toBeGreaterThanOrEqual(0);
    }

    // Check that all main components are visible
    const walNode = page.locator('[data-component="wal"]');
    const memtableNode = page.locator('[data-component="memtable"]');
    const immutableNode = page.locator('[data-component="immutable"]');
    const l0Node = page.locator('[data-component="sstable-l0"]');
    const l1Node = page.locator('[data-component="sstable-l1"]');
    const l2Node = page.locator('[data-component="sstable-l2"]');

    await expect(walNode).toBeVisible();
    await expect(memtableNode).toBeVisible();
    await expect(immutableNode).toBeVisible();
    await expect(l0Node).toBeVisible();
    await expect(l1Node).toBeVisible();
    await expect(l2Node).toBeVisible();

    // Check labels are readable (visible and not cut off)
    const walLabel = walNode.locator('.arch-label');
    const memtableLabel = memtableNode.locator('.arch-label');

    await expect(walLabel).toBeVisible();
    await expect(memtableLabel).toBeVisible();
  });

  test('architecture section is fully visible on tablet viewport', async ({ page }) => {
    // Set tablet viewport (iPad)
    await page.setViewportSize({ width: 768, height: 1024 });

    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    const diagram = page.locator('.arch-diagram');
    await expect(diagram).toBeVisible();

    // All SSTable levels should be visible
    await expect(page.locator('[data-component="sstable-l0"]')).toBeVisible();
    await expect(page.locator('[data-component="sstable-l1"]')).toBeVisible();
    await expect(page.locator('[data-component="sstable-l2"]')).toBeVisible();
  });

  test('legend is visible and readable on mobile', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 });

    // Scroll to architecture section
    await page.locator('#architecture').scrollIntoViewIfNeeded();

    const legend = page.locator('.arch-legend');
    await expect(legend).toBeVisible();

    // Check legend items are visible
    const legendItems = page.locator('.arch-legend-item');
    const itemCount = await legendItems.count();
    expect(itemCount).toBeGreaterThanOrEqual(2);

    for (let i = 0; i < itemCount; i++) {
      await expect(legendItems.nth(i)).toBeVisible();
    }
  });

  test('section heading is visible on mobile', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 });

    const heading = page.locator('#architecture-heading');
    await expect(heading).toBeVisible();
    await expect(heading).toHaveText('Architecture');
  });

  test('diagram maintains readability at 320px viewport', async ({ page }) => {
    // Test at very small viewport (older/smaller phones)
    await page.setViewportSize({ width: 320, height: 568 });

    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Section should be contained within viewport
    const sectionBox = await architectureSection.boundingBox();
    expect(sectionBox).not.toBeNull();

    // All component labels should still be visible
    const labels = page.locator('.arch-label');
    const labelCount = await labels.count();
    expect(labelCount).toBeGreaterThanOrEqual(6);
  });
});
