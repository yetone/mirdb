/**
 * E2E tests for Project Status section.
 * Owner: Scenario 12 - Project Status and Roadmap
 *
 * Tests:
 * - Project status section loads and displays
 * - Completed features checklist with checkmarks
 * - Roadmap items are displayed (including Raft consensus)
 */

import { test, expect } from '@playwright/test';

test.describe('Project Status Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('project status section is visible on page', async ({ page }) => {
    const projectStatus = page.locator('#project-status');
    await projectStatus.scrollIntoViewIfNeeded();
    await expect(projectStatus).toBeVisible();
  });

  test('section displays Project Status heading', async ({ page }) => {
    const projectStatus = page.locator('#project-status');
    await projectStatus.scrollIntoViewIfNeeded();

    const heading = projectStatus.locator('.section-title');
    await expect(heading).toBeVisible();
    await expect(heading).toHaveText('Project Status');
  });

  test('displays completed features checklist', async ({ page }) => {
    const projectStatus = page.locator('#project-status');
    await projectStatus.scrollIntoViewIfNeeded();

    // Check for completed features list
    const completedList = projectStatus.locator('.completed-list');
    await expect(completedList).toBeVisible();

    // Check for completed feature items
    const completedItems = projectStatus.locator('[data-testid="completed-feature"]');
    const count = await completedItems.count();
    expect(count).toBeGreaterThan(0);
  });

  test('completed features have checkmark indicators', async ({ page }) => {
    const projectStatus = page.locator('#project-status');
    await projectStatus.scrollIntoViewIfNeeded();

    // Check that completed items have checkmarks
    const completedItems = projectStatus.locator('[data-testid="completed-feature"]');
    const firstItem = completedItems.first();
    await expect(firstItem).toBeVisible();

    // Each completed item should have a checkmark span
    const checkmark = firstItem.locator('.checkmark');
    await expect(checkmark).toBeVisible();
  });

  test('displays Completed Features heading with icon', async ({ page }) => {
    const projectStatus = page.locator('#project-status');
    await projectStatus.scrollIntoViewIfNeeded();

    // Find the completed features column heading
    const completedHeading = projectStatus.locator('.status-heading').filter({ hasText: 'Completed Features' });
    await expect(completedHeading).toBeVisible();

    // Should have a completed icon
    const completedIcon = completedHeading.locator('.completed-icon');
    await expect(completedIcon).toBeVisible();
  });

  test('displays roadmap items', async ({ page }) => {
    const projectStatus = page.locator('#project-status');
    await projectStatus.scrollIntoViewIfNeeded();

    // Check for roadmap list
    const roadmapList = projectStatus.locator('.roadmap-list');
    await expect(roadmapList).toBeVisible();

    // Check for roadmap items
    const roadmapItems = projectStatus.locator('[data-testid="roadmap-item"]');
    const count = await roadmapItems.count();
    expect(count).toBeGreaterThan(0);
  });

  test('displays Roadmap heading with icon', async ({ page }) => {
    const projectStatus = page.locator('#project-status');
    await projectStatus.scrollIntoViewIfNeeded();

    // Find the roadmap column heading
    const roadmapHeading = projectStatus.locator('.status-heading').filter({ hasText: 'Roadmap' });
    await expect(roadmapHeading).toBeVisible();

    // Should have a roadmap icon
    const roadmapIcon = roadmapHeading.locator('.roadmap-icon');
    await expect(roadmapIcon).toBeVisible();
  });

  test('roadmap includes Raft consensus as planned feature', async ({ page }) => {
    const projectStatus = page.locator('#project-status');
    await projectStatus.scrollIntoViewIfNeeded();

    // Find Raft consensus in roadmap items
    const raftItem = projectStatus.locator('[data-testid="roadmap-item"]').filter({ hasText: 'Raft' });
    await expect(raftItem).toBeVisible();
  });

  test('completed features include Memcached Protocol Support', async ({ page }) => {
    const projectStatus = page.locator('#project-status');
    await projectStatus.scrollIntoViewIfNeeded();

    // Find Memcached Protocol in completed items
    const memcachedItem = projectStatus.locator('[data-testid="completed-feature"]').filter({ hasText: 'Memcached' });
    await expect(memcachedItem).toBeVisible();
  });

  test('completed features include Persistent Storage', async ({ page }) => {
    const projectStatus = page.locator('#project-status');
    await projectStatus.scrollIntoViewIfNeeded();

    // Find Persistent Storage in completed items
    const persistentItem = projectStatus.locator('[data-testid="completed-feature"]').filter({ hasText: 'Persistent' });
    await expect(persistentItem).toBeVisible();
  });

  test('feature items display name and description', async ({ page }) => {
    const projectStatus = page.locator('#project-status');
    await projectStatus.scrollIntoViewIfNeeded();

    // Get first completed feature
    const firstCompleted = projectStatus.locator('[data-testid="completed-feature"]').first();
    await expect(firstCompleted).toBeVisible();

    // Should have feature name
    const featureName = firstCompleted.locator('.feature-name');
    await expect(featureName).toBeVisible();

    // Should have feature description
    const featureDescription = firstCompleted.locator('.feature-description');
    await expect(featureDescription).toBeVisible();
  });

  test('section has accessible structure with proper roles', async ({ page }) => {
    const projectStatus = page.locator('#project-status');
    await projectStatus.scrollIntoViewIfNeeded();

    // Check completed list has role="list"
    const completedList = projectStatus.locator('.completed-list');
    await expect(completedList).toHaveAttribute('role', 'list');

    // Check roadmap list has role="list"
    const roadmapList = projectStatus.locator('.roadmap-list');
    await expect(roadmapList).toHaveAttribute('role', 'list');

    // Check for aria-label on lists
    await expect(completedList).toHaveAttribute('aria-label', 'Completed features');
    await expect(roadmapList).toHaveAttribute('aria-label', 'Planned features');
  });

  test('section displays in two-column layout on desktop', async ({ page }) => {
    const projectStatus = page.locator('#project-status');
    await projectStatus.scrollIntoViewIfNeeded();

    const statusGrid = projectStatus.locator('.status-grid');
    await expect(statusGrid).toBeVisible();

    // Check that grid has two columns
    const gridStyles = await statusGrid.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        display: styles.display,
        gridTemplateColumns: styles.gridTemplateColumns
      };
    });

    expect(gridStyles.display).toBe('grid');
    const columnCount = gridStyles.gridTemplateColumns.split(' ').filter(col => col.trim() !== '').length;
    expect(columnCount).toBe(2);
  });
});
