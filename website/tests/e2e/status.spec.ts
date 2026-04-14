/**
 * Status Section E2E Tests
 * Owner: Scenario 6 - Project Status Section
 *
 * Tests for:
 * - Implemented features with checkmarks
 * - Planned features with "coming soon" indicators
 * - Visual distinction between implemented and planned
 */
import { test, expect } from '@playwright/test';
import { SELECTORS } from '../fixtures/test-data';

test.describe('Status Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Navigate to the status section
    await page.locator(SELECTORS.status).scrollIntoViewIfNeeded();
  });

  test('displays status section with title', async ({ page }) => {
    const statusSection = page.locator(SELECTORS.status);
    await expect(statusSection).toBeVisible();

    const title = statusSection.locator('.status-title');
    await expect(title).toHaveText('Project Status');
  });

  test('TC1: shows implemented features with checkmarks', async ({ page }) => {
    // Test Case 1: Check implemented features list
    // Expected: Implemented features shown with checkmarks: memcached protocol, memtable, minor compaction, major compaction

    const implementedColumn = page.locator('[data-status-type="implemented"]');
    await expect(implementedColumn).toBeVisible();

    // Verify the implemented features title
    const implementedTitle = implementedColumn.locator('.status-column-title');
    await expect(implementedTitle).toContainText('Implemented Features');

    // Verify required implemented features are present using data-feature attributes
    const implementedFeatures = [
      { slug: 'memcached-protocol', name: 'Memcached protocol' },
      { slug: 'memtable', name: 'Memtable' },
      { slug: 'minor-compaction', name: 'Minor compaction' },
      { slug: 'major-compaction', name: 'Major compaction' }
    ];

    for (const feature of implementedFeatures) {
      const featureItem = implementedColumn.locator(`[data-feature="${feature.slug}"]`);
      await expect(featureItem).toBeVisible();
      await expect(featureItem.locator('.status-name')).toContainText(feature.name);

      // Verify checkmark icon exists for each implemented feature
      const checkIcon = featureItem.locator('.status-icon.implemented svg polyline');
      await expect(checkIcon).toBeVisible();
    }
  });

  test('TC2: shows planned feature Raft consensus as coming soon', async ({ page }) => {
    // Test Case 2: Check planned features list
    // Expected: Planned feature 'Raft consensus' is listed without checkmark or marked as 'coming soon'

    const plannedColumn = page.locator('[data-status-type="planned"]');
    await expect(plannedColumn).toBeVisible();

    // Verify the planned features title
    const plannedTitle = plannedColumn.locator('.status-column-title');
    await expect(plannedTitle).toContainText('Planned Features');

    // Verify Raft consensus is listed
    const raftFeature = plannedColumn.locator('[data-feature="raft-consensus"]');
    await expect(raftFeature).toBeVisible();
    await expect(raftFeature.locator('.status-name')).toContainText('Raft consensus');

    // Verify "Coming Soon" badge is present
    const comingSoonBadge = raftFeature.locator('.coming-soon-badge');
    await expect(comingSoonBadge).toBeVisible();
    await expect(comingSoonBadge).toContainText('Coming Soon');

    // Verify it does NOT have a checkmark (has circle icon instead)
    const checkIcon = raftFeature.locator('.status-icon.implemented');
    await expect(checkIcon).not.toBeVisible();

    const plannedIcon = raftFeature.locator('.status-icon.planned');
    await expect(plannedIcon).toBeVisible();
  });

  test('TC3: has visual distinction between implemented and planned features', async ({ page }) => {
    // Test Case 3: Verify visual distinction
    // Expected: Clear visual difference between implemented (checked) and planned (unchecked) features

    const implementedColumn = page.locator('[data-status-type="implemented"]');
    const plannedColumn = page.locator('[data-status-type="planned"]');

    // Both columns should be visible
    await expect(implementedColumn).toBeVisible();
    await expect(plannedColumn).toBeVisible();

    // Check that implemented items have checkmark icons with 'implemented' class
    const implementedIcons = implementedColumn.locator('.status-icon.implemented');
    const implementedCount = await implementedIcons.count();
    expect(implementedCount).toBeGreaterThan(0);

    // Check that planned items have circle icons with 'planned' class
    const plannedIcons = plannedColumn.locator('.status-icon.planned');
    const plannedCount = await plannedIcons.count();
    expect(plannedCount).toBeGreaterThan(0);

    // Verify visual styles are different
    // Get the first implemented icon's background color
    const firstImplementedIcon = implementedIcons.first();
    const implementedBgColor = await firstImplementedIcon.evaluate(
      (el) => getComputedStyle(el).backgroundColor
    );

    // Get the first planned icon's background color
    const firstPlannedIcon = plannedIcons.first();
    const plannedBgColor = await firstPlannedIcon.evaluate(
      (el) => getComputedStyle(el).backgroundColor
    );

    // Verify the colors are different (visual distinction)
    expect(implementedBgColor).not.toBe(plannedBgColor);

    // Verify column titles have different colors
    const implementedTitle = implementedColumn.locator('.status-column-title.implemented');
    const plannedTitle = plannedColumn.locator('.status-column-title.planned');

    const implementedTitleColor = await implementedTitle.evaluate(
      (el) => getComputedStyle(el).color
    );
    const plannedTitleColor = await plannedTitle.evaluate(
      (el) => getComputedStyle(el).color
    );

    expect(implementedTitleColor).not.toBe(plannedTitleColor);
  });

  test('displays all implemented features with descriptions', async ({ page }) => {
    const implementedColumn = page.locator('[data-status-type="implemented"]');

    // All expected implemented features using data-feature attributes
    const implementedFeatures = [
      { slug: 'memcached-protocol', name: 'Memcached protocol', description: 'Full compatibility with standard memcached clients' },
      { slug: 'memtable', name: 'Memtable', description: 'In-memory write buffer with skip list data structure' },
      { slug: 'minor-compaction', name: 'Minor compaction', description: 'Automatic flush of memtable to SSTable on disk' },
      { slug: 'major-compaction', name: 'Major compaction', description: 'Merge and compact multiple SSTables to reclaim space' }
    ];

    for (const feature of implementedFeatures) {
      const featureItem = implementedColumn.locator(`[data-feature="${feature.slug}"]`);
      await expect(featureItem).toBeVisible();
      await expect(featureItem.locator('.status-name')).toContainText(feature.name);

      const description = featureItem.locator('.status-description');
      await expect(description).toContainText(feature.description);
    }
  });

  test('all planned features have coming soon badges', async ({ page }) => {
    const plannedColumn = page.locator('[data-status-type="planned"]');
    const plannedItems = plannedColumn.locator('.status-item');
    const count = await plannedItems.count();

    expect(count).toBeGreaterThan(0);

    for (let i = 0; i < count; i++) {
      const item = plannedItems.nth(i);
      const badge = item.locator('.coming-soon-badge');
      await expect(badge).toBeVisible();
      await expect(badge).toContainText('Coming Soon');
    }
  });

  test('status section has proper accessibility structure', async ({ page }) => {
    const statusSection = page.locator(SELECTORS.status);

    // Section should have proper heading hierarchy
    const mainTitle = statusSection.locator('h2.status-title');
    await expect(mainTitle).toBeVisible();

    const columnTitles = statusSection.locator('h3.status-column-title');
    const columnCount = await columnTitles.count();
    expect(columnCount).toBe(2);

    // Feature lists should be proper lists
    const lists = statusSection.locator('ul.status-list');
    const listCount = await lists.count();
    expect(listCount).toBe(2);

    // List items should have proper structure
    const items = statusSection.locator('li.status-item');
    const itemCount = await items.count();
    expect(itemCount).toBeGreaterThan(0);
  });
});
