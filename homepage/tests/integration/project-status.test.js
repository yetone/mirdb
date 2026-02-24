/**
 * Project Status Section Integration Tests
 * Owner: Scenario 7 - Project Status Section
 *
 * Test cases:
 * - Implemented features section rendered
 * - Roadmap section rendered
 * - Visual distinction between implemented and planned
 * - Raft consensus shown as pending
 */

import { test, expect } from '@playwright/test';

test.describe('Project Status Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Implemented features section is rendered', async ({ page }) => {
    // Check status section exists
    const statusSection = page.locator('#status');
    await expect(statusSection).toBeVisible();

    // Check implemented features column exists
    const implementedColumn = page.locator('.status-column[data-status-type="implemented"]');
    await expect(implementedColumn).toBeVisible();

    // Check implemented title
    const implementedTitle = implementedColumn.locator('.status-column-title');
    await expect(implementedTitle).toContainText('Implemented');

    // Check that there are implemented items
    const implementedItems = implementedColumn.locator('.status-item-implemented');
    const itemCount = await implementedItems.count();
    expect(itemCount).toBeGreaterThan(0);
  });

  test('TC2: Roadmap/planned features section is rendered', async ({ page }) => {
    // Check roadmap column exists
    const roadmapColumn = page.locator('.status-column[data-status-type="roadmap"]');
    await expect(roadmapColumn).toBeVisible();

    // Check roadmap title
    const roadmapTitle = roadmapColumn.locator('.status-column-title');
    await expect(roadmapTitle).toContainText('Roadmap');

    // Check that there are pending items
    const pendingItems = roadmapColumn.locator('.status-item-pending');
    const itemCount = await pendingItems.count();
    expect(itemCount).toBeGreaterThan(0);
  });

  test('TC3: Visual distinction between implemented and planned features', async ({ page }) => {
    // Get implemented indicator
    const implementedIndicator = page.locator('.status-indicator-implemented').first();
    await expect(implementedIndicator).toBeVisible();

    // Get pending indicator
    const pendingIndicator = page.locator('.status-indicator-pending').first();
    await expect(pendingIndicator).toBeVisible();

    // Check that implemented indicator contains checkmark SVG
    const implementedSvg = implementedIndicator.locator('svg polyline');
    await expect(implementedSvg).toBeVisible();

    // Check that pending indicator contains circle SVG
    const pendingSvg = pendingIndicator.locator('svg circle');
    await expect(pendingSvg).toBeVisible();

    // Verify visual distinction via CSS classes
    const implementedColumn = page.locator('.status-column[data-status-type="implemented"]');
    const roadmapColumn = page.locator('.status-column[data-status-type="roadmap"]');

    // Check that columns have different data-status-type attributes
    await expect(implementedColumn).toHaveAttribute('data-status-type', 'implemented');
    await expect(roadmapColumn).toHaveAttribute('data-status-type', 'roadmap');

    // Check border-top colors are different (visual distinction)
    const implementedBorderColor = await implementedColumn.evaluate((el) => {
      return window.getComputedStyle(el).borderTopColor;
    });
    const roadmapBorderColor = await roadmapColumn.evaluate((el) => {
      return window.getComputedStyle(el).borderTopColor;
    });

    expect(implementedBorderColor).not.toBe(roadmapBorderColor);
  });

  test('TC4: Raft consensus is shown as a pending/roadmap item', async ({ page }) => {
    // Find the Raft consensus item
    const raftItem = page.locator('.status-item[data-feature="raft-consensus"]');
    await expect(raftItem).toBeVisible();

    // Check it's in the roadmap column
    const roadmapColumn = page.locator('.status-column[data-status-type="roadmap"]');
    const raftInRoadmap = roadmapColumn.locator('.status-item[data-feature="raft-consensus"]');
    await expect(raftInRoadmap).toBeVisible();

    // Check it has the pending class
    await expect(raftItem).toHaveClass(/status-item-pending/);

    // Check the text contains "Raft Consensus"
    const raftText = raftItem.locator('.status-item-text');
    await expect(raftText).toContainText('Raft Consensus');

    // Check it has a pending badge
    const pendingBadge = raftItem.locator('.status-badge-pending');
    await expect(pendingBadge).toBeVisible();
    await expect(pendingBadge).toContainText('Planned');
  });

  test('Status section has proper heading hierarchy', async ({ page }) => {
    // Check h2 title exists
    const sectionTitle = page.locator('#status-title');
    await expect(sectionTitle).toBeVisible();
    await expect(sectionTitle).toHaveText('Project Status');

    // Check column titles are h3
    const columnH3s = page.locator('.status-column h3.status-column-title');
    await expect(columnH3s).toHaveCount(2);
  });

  test('Status section is accessible with ARIA attributes', async ({ page }) => {
    // Check section has aria-labelledby
    const statusSection = page.locator('#status');
    await expect(statusSection).toHaveAttribute('aria-labelledby', 'status-title');

    // Check status lists have aria-labelledby
    const implementedList = page.locator('.status-column[data-status-type="implemented"] .status-list');
    await expect(implementedList).toHaveAttribute('aria-labelledby', 'implemented-title');

    const roadmapList = page.locator('.status-column[data-status-type="roadmap"] .status-list');
    await expect(roadmapList).toHaveAttribute('aria-labelledby', 'roadmap-title');

    // Check icons are decorative (aria-hidden)
    const headerIcons = page.locator('.status-header-icon');
    const iconCount = await headerIcons.count();
    for (let i = 0; i < iconCount; i++) {
      await expect(headerIcons.nth(i)).toHaveAttribute('aria-hidden', 'true');
    }
  });

  test('Implemented features include core MirDB capabilities', async ({ page }) => {
    const implementedColumn = page.locator('.status-column[data-status-type="implemented"]');

    // Check Memcached Protocol is implemented
    const memcachedItem = implementedColumn.locator('.status-item[data-feature="memcached-protocol"]');
    await expect(memcachedItem).toBeVisible();

    // Check SSTable Persistence is implemented
    const sstableItem = implementedColumn.locator('.status-item[data-feature="sstable-persistence"]');
    await expect(sstableItem).toBeVisible();

    // Check LSM Tree is implemented
    const lsmItem = implementedColumn.locator('.status-item[data-feature="lsm-tree"]');
    await expect(lsmItem).toBeVisible();

    // Check WAL is implemented
    const walItem = implementedColumn.locator('.status-item[data-feature="wal"]');
    await expect(walItem).toBeVisible();
  });

  test('Status items have visual indicators', async ({ page }) => {
    // Check all implemented items have checkmark indicators
    const implementedIndicators = page.locator('.status-indicator-implemented');
    const implementedCount = await implementedIndicators.count();
    expect(implementedCount).toBeGreaterThan(0);

    // Check all pending items have circle indicators
    const pendingIndicators = page.locator('.status-indicator-pending');
    const pendingCount = await pendingIndicators.count();
    expect(pendingCount).toBeGreaterThan(0);

    // Verify each indicator has an SVG inside
    for (let i = 0; i < implementedCount; i++) {
      const svg = implementedIndicators.nth(i).locator('svg');
      await expect(svg).toBeVisible();
    }
  });

  test('Status column hover effect applies', async ({ page }) => {
    // Get a status column
    const statusColumn = page.locator('.status-column').first();
    await expect(statusColumn).toBeVisible();

    // Get initial transform
    const initialTransform = await statusColumn.evaluate((el) => {
      return window.getComputedStyle(el).transform;
    });

    // Hover over the column
    await statusColumn.hover();

    // Wait for transition
    await page.waitForTimeout(300);

    // Get hover transform
    const hoverTransform = await statusColumn.evaluate((el) => {
      return window.getComputedStyle(el).transform;
    });

    // Check that transform changed (translateY effect)
    const transformChanged = initialTransform !== hoverTransform;
    expect(transformChanged).toBe(true);
  });
});
