/**
 * Project Status E2E Tests
 * Owner: Scenario 14 - Project Status Display
 *
 * Tests for:
 * - Project status section display
 * - Status indicators (completed/planned) for each feature
 * - Tokio/Proto, Skip List, Compaction features marked as completed
 * - Raft feature marked as planned
 */

import { test, expect } from '@playwright/test';

test.describe('Project Status Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('project status section is visible on the page', async ({ page }) => {
    const statusSection = page.locator('#project-status');
    await expect(statusSection).toBeVisible();
    await expect(statusSection.locator('.project-status__title')).toHaveText('Project Status');
  });

  test('displays 5 status items', async ({ page }) => {
    const statusItems = page.locator('.project-status__item');
    await expect(statusItems).toHaveCount(5);
  });

  // Test Case 1: Check Tokio/Proto status indicator
  test('Tokio/Proto shows completed/checkmark status', async ({ page }) => {
    const tokioItem = page.locator('.project-status__item[data-feature="tokio-proto"]');
    await expect(tokioItem).toBeVisible();

    // Check the item has completed status
    await expect(tokioItem).toHaveAttribute('data-status', 'completed');

    // Check name is displayed
    const name = tokioItem.locator('.project-status__name');
    await expect(name).toHaveText('Tokio/Proto');

    // Check for completed indicator with checkmark
    const indicator = tokioItem.locator('.project-status__indicator--completed');
    await expect(indicator).toBeVisible();

    // Verify checkmark SVG is present
    const checkIcon = indicator.locator('svg');
    await expect(checkIcon).toBeVisible();

    // Check label shows "Completed"
    const label = tokioItem.locator('.project-status__label--completed');
    await expect(label).toBeVisible();
    await expect(label).toContainText('Completed');
  });

  // Test Case 2: Check Skip List status indicator
  test('Skip List shows completed/checkmark status', async ({ page }) => {
    const skipListItem = page.locator('.project-status__item[data-feature="skip-list"]');
    await expect(skipListItem).toBeVisible();

    // Check the item has completed status
    await expect(skipListItem).toHaveAttribute('data-status', 'completed');

    // Check name is displayed
    const name = skipListItem.locator('.project-status__name');
    await expect(name).toHaveText('Skip List');

    // Check for completed indicator with checkmark
    const indicator = skipListItem.locator('.project-status__indicator--completed');
    await expect(indicator).toBeVisible();

    // Verify checkmark SVG is present
    const checkIcon = indicator.locator('svg');
    await expect(checkIcon).toBeVisible();

    // Check label shows "Completed"
    const label = skipListItem.locator('.project-status__label--completed');
    await expect(label).toBeVisible();
    await expect(label).toContainText('Completed');
  });

  // Test Case 3: Check Minor Compaction status indicator
  test('Minor Compaction shows completed/checkmark status', async ({ page }) => {
    const minorCompactionItem = page.locator('.project-status__item[data-feature="minor-compaction"]');
    await expect(minorCompactionItem).toBeVisible();

    // Check the item has completed status
    await expect(minorCompactionItem).toHaveAttribute('data-status', 'completed');

    // Check name is displayed
    const name = minorCompactionItem.locator('.project-status__name');
    await expect(name).toHaveText('Minor Compaction');

    // Check for completed indicator with checkmark
    const indicator = minorCompactionItem.locator('.project-status__indicator--completed');
    await expect(indicator).toBeVisible();

    // Verify checkmark SVG is present
    const checkIcon = indicator.locator('svg');
    await expect(checkIcon).toBeVisible();

    // Check label shows "Completed"
    const label = minorCompactionItem.locator('.project-status__label--completed');
    await expect(label).toBeVisible();
    await expect(label).toContainText('Completed');
  });

  // Test Case 4: Check Major Compaction status indicator
  test('Major Compaction shows completed/checkmark status', async ({ page }) => {
    const majorCompactionItem = page.locator('.project-status__item[data-feature="major-compaction"]');
    await expect(majorCompactionItem).toBeVisible();

    // Check the item has completed status
    await expect(majorCompactionItem).toHaveAttribute('data-status', 'completed');

    // Check name is displayed
    const name = majorCompactionItem.locator('.project-status__name');
    await expect(name).toHaveText('Major Compaction');

    // Check for completed indicator with checkmark
    const indicator = majorCompactionItem.locator('.project-status__indicator--completed');
    await expect(indicator).toBeVisible();

    // Verify checkmark SVG is present
    const checkIcon = indicator.locator('svg');
    await expect(checkIcon).toBeVisible();

    // Check label shows "Completed"
    const label = majorCompactionItem.locator('.project-status__label--completed');
    await expect(label).toBeVisible();
    await expect(label).toContainText('Completed');
  });

  // Test Case 5: Check Raft status indicator
  test('Raft shows planned/pending/future status', async ({ page }) => {
    const raftItem = page.locator('.project-status__item[data-feature="raft"]');
    await expect(raftItem).toBeVisible();

    // Check the item has planned status
    await expect(raftItem).toHaveAttribute('data-status', 'planned');

    // Check name is displayed
    const name = raftItem.locator('.project-status__name');
    await expect(name).toHaveText('Raft');

    // Check for planned indicator (not completed)
    const indicator = raftItem.locator('.project-status__indicator--planned');
    await expect(indicator).toBeVisible();

    // Verify clock icon is present for planned items
    const clockIcon = indicator.locator('svg');
    await expect(clockIcon).toBeVisible();

    // Check label shows "Planned"
    const label = raftItem.locator('.project-status__label--planned');
    await expect(label).toBeVisible();
    await expect(label).toContainText('Planned');
  });

  test('all completed features have data-status="completed"', async ({ page }) => {
    const completedItems = page.locator('.project-status__item[data-status="completed"]');
    await expect(completedItems).toHaveCount(4);

    // Verify each has the completed indicator
    for (let i = 0; i < 4; i++) {
      const item = completedItems.nth(i);
      const indicator = item.locator('.project-status__indicator--completed');
      await expect(indicator).toBeVisible();
    }
  });

  test('all planned features have data-status="planned"', async ({ page }) => {
    const plannedItems = page.locator('.project-status__item[data-status="planned"]');
    await expect(plannedItems).toHaveCount(1);

    const indicator = plannedItems.locator('.project-status__indicator--planned');
    await expect(indicator).toBeVisible();
  });

  test('project status section has proper accessibility attributes', async ({ page }) => {
    const statusSection = page.locator('#project-status');
    await expect(statusSection).toHaveAttribute('aria-labelledby', 'project-status-heading');

    const title = page.locator('#project-status-heading');
    await expect(title).toBeVisible();

    // Check list has aria-label
    const list = page.locator('.project-status__list');
    await expect(list).toHaveAttribute('aria-label', 'Feature implementation status');
  });

  test('status labels have aria-label for accessibility', async ({ page }) => {
    const completedLabel = page.locator('.project-status__label--completed').first();
    await expect(completedLabel).toHaveAttribute('aria-label', 'Status: Completed');

    const plannedLabel = page.locator('.project-status__label--planned').first();
    await expect(plannedLabel).toHaveAttribute('aria-label', 'Status: Planned');
  });
});
