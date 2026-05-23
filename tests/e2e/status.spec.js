/**
 * E2E tests for Project Status Section.
 * Owner: Scenario 4 - Project Status Section
 *
 * Tests:
 * - Status section renders with correct heading
 * - Completed features are present and marked as completed
 * - Planned features are present and marked as planned
 * - Visual indicators distinguish completed vs planned items
 */

const { test, expect } = require('@playwright/test');

const filePath = 'file://' + process.cwd() + '/index.html';

test.describe('Project Status Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(filePath);
  });

  test('Status section exists with id="status" and h2 heading containing "Status"', async ({ page }) => {
    const statusSection = page.locator('#status');
    await expect(statusSection).toBeVisible();

    const heading = statusSection.locator('h2');
    await expect(heading).toBeVisible();
    const headingText = await heading.textContent();
    expect(headingText.toLowerCase()).toMatch(/status|project status/);
  });

  test('Completed feature "tokio with memcached protocol" is present and marked completed', async ({ page }) => {
    const item = page.locator('[data-testid="status-tokio"]');
    await expect(item).toBeVisible();

    const text = item.locator('.status-text');
    await expect(text).toContainText('Tokio');
    await expect(text).toContainText('memcached protocol');

    const checkmark = item.locator('.status-checkmark');
    await expect(checkmark).toBeVisible();
  });

  test('Completed feature "memtable with skiplist" is present and marked completed', async ({ page }) => {
    const item = page.locator('[data-testid="status-memtable"]');
    await expect(item).toBeVisible();

    const text = item.locator('.status-text');
    await expect(text).toContainText('Memtable');
    await expect(text).toContainText('skiplist');

    const checkmark = item.locator('.status-checkmark');
    await expect(checkmark).toBeVisible();
  });

  test('Completed feature "minor compaction" is present and marked completed', async ({ page }) => {
    const item = page.locator('[data-testid="status-minor-compaction"]');
    await expect(item).toBeVisible();

    const text = item.locator('.status-text');
    await expect(text).toContainText('Minor compaction');

    const checkmark = item.locator('.status-checkmark');
    await expect(checkmark).toBeVisible();
  });

  test('Completed feature "major compaction" is present and marked completed', async ({ page }) => {
    const item = page.locator('[data-testid="status-major-compaction"]');
    await expect(item).toBeVisible();

    const text = item.locator('.status-text');
    await expect(text).toContainText('Major compaction');

    const checkmark = item.locator('.status-checkmark');
    await expect(checkmark).toBeVisible();
  });

  test('Planned feature "Raft" is present and marked as planned', async ({ page }) => {
    const item = page.locator('[data-testid="status-raft"]');
    await expect(item).toBeVisible();

    const text = item.locator('.status-text');
    await expect(text).toContainText('Raft');

    // Should have empty checkbox indicator, not checkmark
    const emptyIndicator = item.locator('.status-empty');
    await expect(emptyIndicator).toBeVisible();

    const checkmark = item.locator('.status-checkmark');
    await expect(checkmark).toHaveCount(0);
  });

  test('Completed items have green checkmark visual indicators', async ({ page }) => {
    const completedItems = page.locator('#status .status-completed');
    const count = await completedItems.count();
    expect(count).toBeGreaterThanOrEqual(4);

    for (let i = 0; i < count; i++) {
      const checkmark = completedItems.nth(i).locator('.status-checkmark');
      await expect(checkmark).toBeVisible();
    }
  });

  test('Planned items have empty checkbox visual indicators', async ({ page }) => {
    const plannedItems = page.locator('#status .status-planned');
    const count = await plannedItems.count();
    expect(count).toBeGreaterThanOrEqual(1);

    for (let i = 0; i < count; i++) {
      const emptyIndicator = plannedItems.nth(i).locator('.status-empty');
      await expect(emptyIndicator).toBeVisible();
    }
  });
});
