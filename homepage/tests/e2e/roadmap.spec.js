/**
 * Roadmap Section E2E Tests
 * Owner: Scenario 10 - Roadmap and TODO Section
 *
 * Expected tests:
 * - Roadmap section exists with proper heading
 * - Completed features are listed and marked as done
 * - Planned features are listed
 * - Visual distinction between completed and planned items
 */

const { test, expect } = require('@playwright/test');

test.describe('Roadmap and TODO Section (Scenario 10)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case 1: Roadmap section exists with heading
  test('TC1: Roadmap section exists with heading "Roadmap"', async ({ page }) => {
    // Verify roadmap section exists
    const roadmapSection = page.locator('#roadmap');
    await expect(roadmapSection).toBeVisible();

    // Verify heading text
    const heading = page.locator('#roadmap-title');
    await expect(heading).toBeVisible();
    await expect(heading).toContainText('Roadmap');
  });

  // Test Case 2: Tokio with memcached protocol is marked as completed
  test('TC2: Tokio with memcached protocol is marked as completed', async ({ page }) => {
    // Find the completed features column
    const completedColumn = page.locator('.roadmap__column').filter({
      has: page.locator('.roadmap__column-title--completed')
    });
    await expect(completedColumn).toBeVisible();

    // Find the Tokio item within completed features
    const tokioItem = completedColumn.locator('.roadmap__item--completed').filter({
      hasText: /tokio/i
    });
    await expect(tokioItem).toBeVisible();

    // Verify it contains memcached protocol text
    const tokioText = await tokioItem.locator('.roadmap__item-text').textContent();
    expect(tokioText.toLowerCase()).toContain('memcached');

    // Verify it has the completed checkmark icon
    const checkIcon = tokioItem.locator('.roadmap__item-icon');
    await expect(checkIcon).toBeVisible();
  });

  // Test Case 3: Memtable with skiplist is marked as completed
  test('TC3: Memtable with skiplist is marked as completed', async ({ page }) => {
    // Find the completed features column
    const completedColumn = page.locator('.roadmap__column').filter({
      has: page.locator('.roadmap__column-title--completed')
    });
    await expect(completedColumn).toBeVisible();

    // Find the memtable item within completed features
    const memtableItem = completedColumn.locator('.roadmap__item--completed').filter({
      hasText: /memtable/i
    });
    await expect(memtableItem).toBeVisible();

    // Verify it mentions skip-list
    const memtableText = await memtableItem.locator('.roadmap__item-text').textContent();
    expect(memtableText.toLowerCase()).toMatch(/skip[-\s]?list/i);
  });

  // Test Case 4: Minor compaction is marked as completed
  test('TC4: Minor compaction is marked as completed', async ({ page }) => {
    // Find the completed features column
    const completedColumn = page.locator('.roadmap__column').filter({
      has: page.locator('.roadmap__column-title--completed')
    });

    // Find the minor compaction item
    const minorCompactionItem = completedColumn.locator('.roadmap__item--completed').filter({
      hasText: /minor compaction/i
    });
    await expect(minorCompactionItem).toBeVisible();

    // Verify it has the completed class
    await expect(minorCompactionItem).toHaveClass(/roadmap__item--completed/);
  });

  // Test Case 5: Major compaction is marked as completed
  test('TC5: Major compaction is marked as completed', async ({ page }) => {
    // Find the completed features column
    const completedColumn = page.locator('.roadmap__column').filter({
      has: page.locator('.roadmap__column-title--completed')
    });

    // Find the major compaction item
    const majorCompactionItem = completedColumn.locator('.roadmap__item--completed').filter({
      hasText: /major compaction/i
    });
    await expect(majorCompactionItem).toBeVisible();

    // Verify it has the completed class
    await expect(majorCompactionItem).toHaveClass(/roadmap__item--completed/);
  });

  // Test Case 6: Raft consensus is listed as planned/upcoming
  test('TC6: Raft consensus is listed as planned/upcoming', async ({ page }) => {
    // Find the planned features column
    const plannedColumn = page.locator('.roadmap__column').filter({
      has: page.locator('.roadmap__column-title--planned')
    });
    await expect(plannedColumn).toBeVisible();

    // Find the Raft item within planned features
    const raftItem = plannedColumn.locator('.roadmap__item--planned').filter({
      hasText: /raft/i
    });
    await expect(raftItem).toBeVisible();

    // Verify it has the planned class
    await expect(raftItem).toHaveClass(/roadmap__item--planned/);
  });

  // Test Case 7: Visual distinction between completed and planned items
  test('TC7: Completed items have checkmark; planned items are visually different', async ({ page }) => {
    // Get completed items
    const completedItems = page.locator('.roadmap__item--completed');
    const completedCount = await completedItems.count();
    expect(completedCount).toBeGreaterThan(0);

    // Verify completed items have checkmark icon (SVG with polyline checkmark path)
    const firstCompletedItem = completedItems.first();
    const completedIcon = firstCompletedItem.locator('svg.roadmap__item-icon');
    await expect(completedIcon).toBeVisible();

    // Check that the completed icon contains a checkmark (polyline element)
    const checkmarkPath = completedIcon.locator('polyline');
    await expect(checkmarkPath).toBeVisible();

    // Get planned items
    const plannedItems = page.locator('.roadmap__item--planned');
    const plannedCount = await plannedItems.count();
    expect(plannedCount).toBeGreaterThan(0);

    // Verify planned items have different icon (circle, not checkmark)
    const firstPlannedItem = plannedItems.first();
    const plannedIcon = firstPlannedItem.locator('svg.roadmap__item-icon');
    await expect(plannedIcon).toBeVisible();

    // Check that planned icon contains a circle (different from checkmark)
    const circlePath = plannedIcon.locator('circle');
    await expect(circlePath).toBeVisible();

    // Verify visual distinction via CSS classes
    const completedItemClass = await firstCompletedItem.getAttribute('class');
    const plannedItemClass = await firstPlannedItem.getAttribute('class');
    expect(completedItemClass).toContain('roadmap__item--completed');
    expect(plannedItemClass).toContain('roadmap__item--planned');
    expect(completedItemClass).not.toContain('roadmap__item--planned');
    expect(plannedItemClass).not.toContain('roadmap__item--completed');
  });

  // Additional test: Navigation to roadmap section
  test('Navigation link scrolls to roadmap section', async ({ page }) => {
    // Find the roadmap navigation link
    const navLink = page.locator('.header__nav-link[href="#roadmap"]');
    await expect(navLink).toBeVisible();

    // Click the nav link
    await navLink.click();

    // Wait for smooth scroll
    await page.waitForTimeout(500);

    // Verify roadmap section is in viewport
    const roadmapSection = page.locator('#roadmap');
    const isInViewport = await roadmapSection.evaluate((el) => {
      const rect = el.getBoundingClientRect();
      return rect.top >= 0 && rect.top < window.innerHeight;
    });
    expect(isInViewport).toBe(true);
  });

  // Additional test: Column headers are properly styled
  test('Completed and Planned column headers are visible', async ({ page }) => {
    // Verify completed column title
    const completedTitle = page.locator('.roadmap__column-title--completed');
    await expect(completedTitle).toBeVisible();
    await expect(completedTitle).toContainText('Completed');

    // Verify planned column title
    const plannedTitle = page.locator('.roadmap__column-title--planned');
    await expect(plannedTitle).toBeVisible();
    await expect(plannedTitle).toContainText('Planned');
  });
});
