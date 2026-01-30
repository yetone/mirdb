/**
 * Project Status and Roadmap Tests
 * Owner: Scenario 6 - Project Status and Roadmap
 *
 * Tests:
 * - Status section presence
 * - Implemented features (tokio, memtable, compaction)
 * - Planned features (raft)
 */

const { test, expect } = require('@playwright/test');

const BASE_URL = process.env.BASE_URL || 'http://localhost:8080';

test.describe('Project Status and Roadmap Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(BASE_URL);
  });

  test('status section exists with id="status"', async ({ page }) => {
    // Test case 1: Check status section element
    const statusSection = page.locator('#status');
    await expect(statusSection).toBeVisible();
    await expect(statusSection).toHaveAttribute('id', 'status');
  });

  test('checklist or list element exists showing feature status', async ({ page }) => {
    // Test case 2: Check for checklist/list element
    const statusSection = page.locator('#status');
    await expect(statusSection).toBeVisible();

    // Check for a list element (ul/ol) or a checklist container
    const checklist = statusSection.locator('.status__checklist, .status__list, ul, ol');
    await expect(checklist.first()).toBeVisible();

    // Verify there are list items
    const listItems = statusSection.locator('.status__item, li');
    const count = await listItems.count();
    expect(count).toBeGreaterThanOrEqual(4); // At least tokio, memtable, minor compaction, major compaction
  });

  test('tokio with memcached protocol is listed as completed', async ({ page }) => {
    // Test case 3: Search for 'tokio' feature
    const statusSection = page.locator('#status');
    await expect(statusSection).toBeVisible();

    // Find the tokio feature item
    const tokioItem = statusSection.locator('.status__item--completed, .status__item.completed, li').filter({
      hasText: /tokio/i
    });
    await expect(tokioItem.first()).toBeVisible();

    // Verify it's marked as completed (has a checkmark or completed class)
    const tokioText = await statusSection.textContent();
    expect(tokioText.toLowerCase()).toContain('tokio');

    // Check for visual completion indicator (checkmark, ✓, ✔, or completed styling)
    const completedIndicator = statusSection.locator('[data-completed="true"], .status__item--completed, .status__check--completed, [aria-checked="true"]').filter({
      hasText: /tokio/i
    });
    const hasCompletedStyling = await completedIndicator.count() > 0;

    // Alternative: check for checkmark characters or SVG icon in tokio item
    const checkmarkInTokio = tokioItem.locator('svg, .status__check, .checkmark');
    const hasCheckmark = await checkmarkInTokio.count() > 0 || tokioText.includes('✓') || tokioText.includes('✔') || tokioText.includes('[x]');

    expect(hasCompletedStyling || hasCheckmark).toBe(true);
  });

  test('memtable with skiplist is listed as completed', async ({ page }) => {
    // Test case 4: Search for 'memtable' feature
    const statusSection = page.locator('#status');
    await expect(statusSection).toBeVisible();

    // Find the memtable feature item
    const memtableItem = statusSection.locator('.status__item, li').filter({
      hasText: /memtable/i
    });
    await expect(memtableItem.first()).toBeVisible();

    // Verify memtable is shown as completed
    const sectionText = await statusSection.textContent();
    expect(sectionText.toLowerCase()).toContain('memtable');

    // Check for skiplist mention (either "skiplist" or "skip list")
    const hasSkiplist = sectionText.toLowerCase().includes('skiplist') || sectionText.toLowerCase().includes('skip list');
    expect(hasSkiplist).toBe(true);

    // Check for visual completion indicator
    const completedMemtable = statusSection.locator('[data-completed="true"], .status__item--completed').filter({
      hasText: /memtable/i
    });
    const hasCompletedStyling = await completedMemtable.count() > 0;

    const checkmarkInMemtable = memtableItem.locator('svg, .status__check');
    const hasCheckmark = await checkmarkInMemtable.count() > 0 || sectionText.includes('✓') || sectionText.includes('✔');

    expect(hasCompletedStyling || hasCheckmark).toBe(true);
  });

  test('minor and major compaction are listed as completed', async ({ page }) => {
    // Test case 5: Search for 'compaction' features
    const statusSection = page.locator('#status');
    await expect(statusSection).toBeVisible();

    const sectionText = await statusSection.textContent();
    const lowerText = sectionText.toLowerCase();

    // Verify both minor and major compaction are mentioned
    expect(lowerText).toContain('minor');
    expect(lowerText).toContain('major');
    expect(lowerText).toContain('compaction');

    // Check that compaction items are completed
    const compactionItems = statusSection.locator('.status__item, li').filter({
      hasText: /compaction/i
    });
    const count = await compactionItems.count();
    expect(count).toBeGreaterThanOrEqual(1); // At least one compaction item (could be combined or separate)

    // Verify completion status for compaction items
    const completedCompaction = statusSection.locator('[data-completed="true"], .status__item--completed').filter({
      hasText: /compaction/i
    });
    const hasCompletedStyling = await completedCompaction.count() > 0;

    const checkmarkInCompaction = compactionItems.locator('svg, .status__check');
    const hasCheckmark = await checkmarkInCompaction.count() > 0 || sectionText.includes('✓') || sectionText.includes('✔');

    expect(hasCompletedStyling || hasCheckmark).toBe(true);
  });

  test('raft is listed as planned/not completed', async ({ page }) => {
    // Test case 6: Search for 'raft' feature
    const statusSection = page.locator('#status');
    await expect(statusSection).toBeVisible();

    const sectionText = await statusSection.textContent();
    const lowerText = sectionText.toLowerCase();

    // Verify raft is mentioned
    expect(lowerText).toContain('raft');

    // Find the raft feature item
    const raftItem = statusSection.locator('.status__item, li').filter({
      hasText: /raft/i
    });
    await expect(raftItem.first()).toBeVisible();

    // Verify raft is NOT completed (is planned)
    const completedRaft = statusSection.locator('[data-completed="true"], .status__item--completed').filter({
      hasText: /raft/i
    });
    const raftIsCompleted = await completedRaft.count() > 0;

    // Raft should be pending/planned, not completed
    const pendingRaft = statusSection.locator('[data-completed="false"], .status__item--pending, .status__item--planned').filter({
      hasText: /raft/i
    });
    const raftIsPending = await pendingRaft.count() > 0;

    // Either raft has pending styling OR it doesn't have completed styling
    expect(raftIsPending || !raftIsCompleted).toBe(true);
  });

  test('status section has a title', async ({ page }) => {
    // Additional test: Verify the section has a title
    const title = page.locator('#status .status__title, #status h2');
    await expect(title.first()).toBeVisible();

    const titleText = await title.first().textContent();
    expect(titleText.length).toBeGreaterThan(0);
  });

  test('status section has proper structure', async ({ page }) => {
    // Additional test: Verify proper structure
    const statusSection = page.locator('#status');
    await expect(statusSection).toBeVisible();

    // Should have a container
    const container = statusSection.locator('.status__container, .container');
    await expect(container.first()).toBeVisible();

    // Should have items with check indicators
    const items = statusSection.locator('.status__item, li');
    const count = await items.count();
    expect(count).toBeGreaterThanOrEqual(5); // At least 5 features (4 completed + 1 planned)
  });
});
