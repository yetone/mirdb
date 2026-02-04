/**
 * Roadmap Section E2E Tests
 * Owner: Scenario 5 - Roadmap Section
 *
 * Test coverage:
 * - Section displays with heading and checklist visible
 * - 4 completed items displayed correctly
 * - 1 planned item (Raft consensus) displayed as unchecked
 * - Visual distinction between completed and planned items
 */

const { test, expect } = require('@playwright/test');

test.describe('Roadmap Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Section displays with heading and checklist visible', async ({ page }) => {
    // Verify roadmap section exists
    const roadmapSection = page.locator('#roadmap');
    await expect(roadmapSection).toBeVisible();

    // Verify heading is present
    const heading = page.locator('#roadmap-heading');
    await expect(heading).toBeVisible();
    await expect(heading).toHaveText('Roadmap');

    // Verify checklist is visible
    const checklist = page.locator('.roadmap-checklist');
    await expect(checklist).toBeVisible();

    // Verify checklist has items
    const items = page.locator('.roadmap-item');
    await expect(items).toHaveCount(5);
  });

  test('TC2: Four items shown as completed', async ({ page }) => {
    // Get all completed items
    const completedItems = page.locator('.roadmap-item--completed');
    await expect(completedItems).toHaveCount(4);

    // Verify specific completed features
    const expectedCompleted = [
      'Tokio with memcached protocol',
      'Memtable with skiplist',
      'Minor compaction',
      'Major compaction'
    ];

    for (let i = 0; i < expectedCompleted.length; i++) {
      const item = completedItems.nth(i);
      await expect(item).toContainText(expectedCompleted[i]);
      await expect(item).toHaveAttribute('data-status', 'completed');
    }

    // Verify completed items have green checkmark indicators
    const completedIndicators = page.locator('.roadmap-item--completed .roadmap-indicator');
    for (let i = 0; i < 4; i++) {
      const indicator = completedIndicators.nth(i);
      await expect(indicator).toHaveClass(/bg-green-500/);
    }

    // Verify completed badges are present
    const completedBadges = page.locator('.roadmap-item--completed .roadmap-badge');
    for (let i = 0; i < 4; i++) {
      await expect(completedBadges.nth(i)).toContainText('Completed');
    }
  });

  test('TC3: Raft consensus shown as unchecked/planned item', async ({ page }) => {
    // Get planned items
    const plannedItems = page.locator('.roadmap-item--planned');
    await expect(plannedItems).toHaveCount(1);

    // Verify Raft consensus is the planned item
    const raftItem = plannedItems.first();
    await expect(raftItem).toContainText('Raft consensus');
    await expect(raftItem).toHaveAttribute('data-status', 'planned');

    // Verify planned item has "Planned" badge
    const plannedBadge = raftItem.locator('.roadmap-badge');
    await expect(plannedBadge).toContainText('Planned');
  });

  test('TC4: Visual distinction between completed and planned items', async ({ page }) => {
    // Completed items should have green filled indicator
    const completedIndicator = page.locator('.roadmap-item--completed .roadmap-indicator').first();
    await expect(completedIndicator).toHaveClass(/bg-green-500/);

    // Planned items should have empty/border-only indicator
    const plannedIndicator = page.locator('.roadmap-item--planned .roadmap-indicator').first();
    await expect(plannedIndicator).toHaveClass(/border-2/);
    await expect(plannedIndicator).toHaveClass(/border-gray-500/);

    // Completed badge should have green styling
    const completedBadge = page.locator('.roadmap-item--completed .roadmap-badge').first();
    await expect(completedBadge).toHaveClass(/bg-green-900/);
    await expect(completedBadge).toHaveClass(/text-green-300/);

    // Planned badge should have gray styling
    const plannedBadge = page.locator('.roadmap-item--planned .roadmap-badge').first();
    await expect(plannedBadge).toHaveClass(/bg-gray-700/);
    await expect(plannedBadge).toHaveClass(/text-gray-400/);

    // Completed items text should be white, planned items text should be gray
    const completedText = page.locator('.roadmap-item--completed .roadmap-text .font-semibold').first();
    await expect(completedText).toHaveClass(/text-white/);

    const plannedText = page.locator('.roadmap-item--planned .roadmap-text .font-semibold').first();
    await expect(plannedText).toHaveClass(/text-gray-300/);
  });

  test('Progress bar shows correct percentage', async ({ page }) => {
    // Verify progress bar exists and shows 80% (4/5 completed)
    const progressBar = page.locator('[role="progressbar"]');
    await expect(progressBar).toBeVisible();
    await expect(progressBar).toHaveAttribute('aria-valuenow', '80');

    // Verify progress text
    await expect(progressBar).toContainText('4 of 5 features completed');

    // Verify progress bar fill width (Zola's minify_html removes space in style)
    const progressFill = progressBar.locator('.bg-green-500');
    await expect(progressFill).toHaveAttribute('style', /width:?\s*80%/);
  });

  test('Accessibility: Section has proper ARIA labels', async ({ page }) => {
    // Section should have aria-labelledby
    const section = page.locator('#roadmap');
    await expect(section).toHaveAttribute('aria-labelledby', 'roadmap-heading');

    // Checklist should have role="list" and aria-label
    const checklist = page.locator('.roadmap-checklist');
    await expect(checklist).toHaveAttribute('role', 'list');
    await expect(checklist).toHaveAttribute('aria-label', 'Feature checklist');

    // Progress bar should have proper ARIA attributes
    const progressBar = page.locator('[role="progressbar"]');
    await expect(progressBar).toHaveAttribute('aria-valuemin', '0');
    await expect(progressBar).toHaveAttribute('aria-valuemax', '100');
  });
});
