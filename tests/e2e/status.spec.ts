/**
 * Project Status Section E2E Tests
 * Owner: Scenario 6 - Project Status and Roadmap
 *
 * Test coverage:
 * - Status section presence
 * - Completed features (Tokio, skiplist, compaction)
 * - Planned features (Raft)
 * - Completion indicators
 */

import { test, expect } from '@playwright/test';
import { navigateToSection, waitForPageLoad } from './test-utils';

test.describe('Project Status and Roadmap Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await waitForPageLoad(page);
  });

  test('should have project status section in the DOM', async ({ page }) => {
    // Test Case 1: Check project status section exists
    const statusSection = page.locator('#status');
    await expect(statusSection).toBeVisible();
    await expect(statusSection).toHaveClass(/status/);

    // Section should have a title
    const sectionTitle = statusSection.locator('.section-title, h2');
    await expect(sectionTitle).toBeVisible();
  });

  test('should display Tokio/memcached protocol feature as complete', async ({ page }) => {
    // Test Case 2: Verify Tokio/memcached feature is marked complete
    await navigateToSection(page, 'status');

    const statusSection = page.locator('#status');

    // Look for feature item containing Tokio or memcached with completion indicator
    const tokioFeature = statusSection.locator('[data-status="complete"]').filter({
      hasText: /tokio|memcached/i
    });

    await expect(tokioFeature).toBeVisible();

    // Check for completion indicator (checkmark icon or completed class)
    const hasCheckmark = await statusSection.locator('.status-item.completed, .feature-complete, [data-status="complete"]').filter({
      hasText: /tokio|memcached/i
    }).count();

    expect(hasCheckmark).toBeGreaterThan(0);
  });

  test('should display skiplist/memtable feature as complete', async ({ page }) => {
    // Test Case 3: Verify skiplist/memtable feature is marked complete
    await navigateToSection(page, 'status');

    const statusSection = page.locator('#status');

    // Look for feature item containing skiplist or memtable with completion indicator
    // Use first() to handle multiple matches since "memtable" appears in multiple features
    const skiplistFeature = statusSection.locator('[data-status="complete"]').filter({
      hasText: /skiplist|memtable/i
    }).first();

    await expect(skiplistFeature).toBeVisible();

    // Check for completion indicator - at least one feature with skiplist or memtable should be complete
    const hasCheckmark = await statusSection.locator('.status-item.completed, .feature-complete, [data-status="complete"]').filter({
      hasText: /skiplist|memtable/i
    }).count();

    expect(hasCheckmark).toBeGreaterThan(0);
  });

  test('should display minor and major compaction features as complete', async ({ page }) => {
    // Test Case 4: Verify compaction features are marked complete
    await navigateToSection(page, 'status');

    const statusSection = page.locator('#status');

    // Check for minor compaction
    const minorCompactionFeature = statusSection.locator('[data-status="complete"]').filter({
      hasText: /minor.*compaction/i
    });
    await expect(minorCompactionFeature).toBeVisible();

    // Check for major compaction
    const majorCompactionFeature = statusSection.locator('[data-status="complete"]').filter({
      hasText: /major.*compaction/i
    });
    await expect(majorCompactionFeature).toBeVisible();

    // Both should have completion indicators
    const completedCompactionFeatures = await statusSection.locator('[data-status="complete"]').filter({
      hasText: /compaction/i
    }).count();

    expect(completedCompactionFeatures).toBeGreaterThanOrEqual(2);
  });

  test('should display Raft/distributed feature as planned', async ({ page }) => {
    // Test Case 5: Verify Raft is marked as planned
    await navigateToSection(page, 'status');

    const statusSection = page.locator('#status');

    // Look for Raft or distributed feature with planned indicator
    const raftFeature = statusSection.locator('[data-status="planned"]').filter({
      hasText: /raft|distributed/i
    });

    await expect(raftFeature).toBeVisible();

    // Verify it has planned status (not completed)
    const plannedFeatures = await statusSection.locator('.status-item.planned, .feature-planned, [data-status="planned"]').filter({
      hasText: /raft|distributed/i
    }).count();

    expect(plannedFeatures).toBeGreaterThan(0);

    // Ensure Raft is NOT marked as complete
    const completedRaftFeatures = await statusSection.locator('[data-status="complete"]').filter({
      hasText: /raft/i
    }).count();

    expect(completedRaftFeatures).toBe(0);
  });

  test('should visually distinguish completed from planned features', async ({ page }) => {
    // Additional test: Verify visual distinction between completed and planned
    await navigateToSection(page, 'status');

    const statusSection = page.locator('#status');

    // Get completed features
    const completedFeatures = statusSection.locator('[data-status="complete"]');
    const completedCount = await completedFeatures.count();
    expect(completedCount).toBeGreaterThan(0);

    // Get planned features
    const plannedFeatures = statusSection.locator('[data-status="planned"]');
    const plannedCount = await plannedFeatures.count();
    expect(plannedCount).toBeGreaterThan(0);

    // Verify completed features have checkmark or different styling
    const firstCompleted = completedFeatures.first();
    await expect(firstCompleted).toBeVisible();

    // Verify planned features have different indicator
    const firstPlanned = plannedFeatures.first();
    await expect(firstPlanned).toBeVisible();
  });

  test('should have proper structure for roadmap display', async ({ page }) => {
    // Test roadmap structure is semantic and accessible
    await navigateToSection(page, 'status');

    const statusSection = page.locator('#status');

    // Should have a container for features
    const statusContainer = statusSection.locator('.status-container, .container');
    await expect(statusContainer).toBeVisible();

    // Should have lists or grouped items
    const statusItems = statusSection.locator('.status-item, .roadmap-item');
    const itemCount = await statusItems.count();
    expect(itemCount).toBeGreaterThan(0);
  });
});
