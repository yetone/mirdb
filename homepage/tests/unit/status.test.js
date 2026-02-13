/**
 * Unit Tests for Status Section
 * Owner: Scenario 5 - Status Section Implementation
 *
 * Tests HTML structure, checklist items, and visual indicators
 */

const { test, expect } = require('@playwright/test');

test.describe('Status Section Unit Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 1: Status section with progress checklist is present', async ({ page }) => {
    // Check status section exists
    const statusSection = page.locator('#status');
    await expect(statusSection).toBeVisible();

    // Verify it has the status class
    await expect(statusSection).toHaveClass(/status/);

    // Verify it has a title
    const title = statusSection.locator('.status__title');
    await expect(title).toBeVisible();
    await expect(title).toContainText('Project Status');

    // Verify checklist is present
    const checklist = statusSection.locator('.status__list');
    await expect(checklist).toBeVisible();

    // Verify aria-labelledby for accessibility
    await expect(statusSection).toHaveAttribute('aria-labelledby', 'status-title');
  });

  test('Test Case 2: At least 5 checklist items exist including all required features', async ({ page }) => {
    const statusSection = page.locator('#status');
    const statusItems = statusSection.locator('.status__item');

    // Verify at least 5 items
    const count = await statusItems.count();
    expect(count).toBeGreaterThanOrEqual(5);

    // Get all item texts
    const itemTexts = await statusItems.allTextContents();
    const combinedText = itemTexts.join(' ').toLowerCase();

    // Verify required items are present
    expect(combinedText).toContain('tokio');
    expect(combinedText).toContain('memcached');
    expect(combinedText).toContain('memtable');
    expect(combinedText).toContain('skiplist');
    expect(combinedText).toContain('minor compaction');
    expect(combinedText).toContain('major compaction');
    expect(combinedText).toContain('raft');
  });

  test('Test Case 3: Tokio with Memcached protocol is marked as completed', async ({ page }) => {
    const statusSection = page.locator('#status');

    // Find the item containing "tokio" and "memcached"
    const tokioItem = statusSection.locator('.status__item', {
      hasText: /tokio.*memcached/i
    });
    await expect(tokioItem).toBeVisible();

    // Verify it has the complete modifier class
    await expect(tokioItem).toHaveClass(/status__item--complete/);

    // Verify it has data-status="complete"
    await expect(tokioItem).toHaveAttribute('data-status', 'complete');

    // Verify checkmark icon is present
    const checkIcon = tokioItem.locator('.status__icon-check');
    await expect(checkIcon).toBeVisible();
  });

  test('Test Case 4: Memtable with skiplist is marked as completed', async ({ page }) => {
    const statusSection = page.locator('#status');

    // Find the item containing "memtable" and "skiplist"
    const memtableItem = statusSection.locator('.status__item', {
      hasText: /memtable.*skiplist/i
    });
    await expect(memtableItem).toBeVisible();

    // Verify it has the complete modifier class
    await expect(memtableItem).toHaveClass(/status__item--complete/);

    // Verify it has data-status="complete"
    await expect(memtableItem).toHaveAttribute('data-status', 'complete');

    // Verify checkmark icon is present
    const checkIcon = memtableItem.locator('.status__icon-check');
    await expect(checkIcon).toBeVisible();
  });

  test('Test Case 5: Minor compaction is marked as completed', async ({ page }) => {
    const statusSection = page.locator('#status');

    // Find the item containing "minor compaction"
    const minorCompactionItem = statusSection.locator('.status__item', {
      hasText: /minor compaction/i
    });
    await expect(minorCompactionItem).toBeVisible();

    // Verify it has the complete modifier class
    await expect(minorCompactionItem).toHaveClass(/status__item--complete/);

    // Verify it has data-status="complete"
    await expect(minorCompactionItem).toHaveAttribute('data-status', 'complete');

    // Verify checkmark icon is present
    const checkIcon = minorCompactionItem.locator('.status__icon-check');
    await expect(checkIcon).toBeVisible();
  });

  test('Test Case 6: Major compaction is marked as completed', async ({ page }) => {
    const statusSection = page.locator('#status');

    // Find the item containing "major compaction"
    const majorCompactionItem = statusSection.locator('.status__item', {
      hasText: /major compaction/i
    });
    await expect(majorCompactionItem).toBeVisible();

    // Verify it has the complete modifier class
    await expect(majorCompactionItem).toHaveClass(/status__item--complete/);

    // Verify it has data-status="complete"
    await expect(majorCompactionItem).toHaveAttribute('data-status', 'complete');

    // Verify checkmark icon is present
    const checkIcon = majorCompactionItem.locator('.status__icon-check');
    await expect(checkIcon).toBeVisible();
  });

  test('Test Case 7: Raft is marked as pending/incomplete', async ({ page }) => {
    const statusSection = page.locator('#status');

    // Find the item containing "raft"
    const raftItem = statusSection.locator('.status__item', {
      hasText: /raft/i
    });
    await expect(raftItem).toBeVisible();

    // Verify it has the pending modifier class
    await expect(raftItem).toHaveClass(/status__item--pending/);

    // Verify it has data-status="pending"
    await expect(raftItem).toHaveAttribute('data-status', 'pending');

    // Verify pending icon is present (circle, not checkmark)
    const pendingIcon = raftItem.locator('.status__icon-pending');
    await expect(pendingIcon).toBeVisible();

    // Verify it does NOT have checkmark icon
    const checkIcon = raftItem.locator('.status__icon-check');
    await expect(checkIcon).toHaveCount(0);
  });

  test('Status section has proper semantic structure', async ({ page }) => {
    // Verify status is a section element
    const statusSection = page.locator('section#status');
    await expect(statusSection).toBeVisible();

    // Verify it has an h2 title
    const title = statusSection.locator('h2#status-title');
    await expect(title).toBeVisible();

    // Verify list has proper role
    const list = statusSection.locator('.status__list');
    await expect(list).toHaveAttribute('role', 'list');

    // Verify list has aria-label
    const ariaLabel = await list.getAttribute('aria-label');
    expect(ariaLabel).toBeTruthy();
    expect(ariaLabel.length).toBeGreaterThan(0);
  });

  test('Progress bar is visible and shows correct completion', async ({ page }) => {
    const statusSection = page.locator('#status');

    // Verify progress bar exists
    const progressBar = statusSection.locator('.status__progress-bar');
    await expect(progressBar).toBeVisible();

    // Verify progress fill exists
    const progressFill = statusSection.locator('.status__progress-fill');
    await expect(progressFill).toBeVisible();

    // Verify progress has proper ARIA attributes
    await expect(progressFill).toHaveAttribute('role', 'progressbar');
    await expect(progressFill).toHaveAttribute('aria-valuemin', '0');
    await expect(progressFill).toHaveAttribute('aria-valuemax', '100');

    // Verify progress text
    const progressText = statusSection.locator('.status__progress-text');
    await expect(progressText).toBeVisible();
    await expect(progressText).toContainText(/\d+.*of.*\d+/); // "X of Y" pattern
  });
});
