/**
 * Status Section E2E Tests
 * Owner: Scenario 5 - Project Status and Roadmap
 *
 * Tests:
 * - Status section visibility
 * - Implemented features display with checkmarks
 * - Roadmap items display (including Raft consensus)
 * - CircleCI badge integration
 */

const { test, expect } = require('@playwright/test');

test.describe('Status Section - Project Status and Roadmap', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Status section exists showing project maturity indicators', async ({ page }) => {
    // Test Case 1: Query page for status or roadmap section
    // Expected: Status section exists showing project maturity indicators

    // Check that the status section exists
    const statusSection = page.locator('#status');
    await expect(statusSection).toBeVisible();

    // Check that it has the proper class
    await expect(statusSection).toHaveClass(/status-section/);

    // Check for the heading
    const statusHeading = page.locator('#status-heading');
    await expect(statusHeading).toBeVisible();
    await expect(statusHeading).toHaveText('Project Status');

    // Check for subtitle showing maturity context
    const statusSubtitle = page.locator('.status-subtitle');
    await expect(statusSubtitle).toBeVisible();
    await expect(statusSubtitle).toContainText('progress');

    // Check that status grid exists with cards
    const statusGrid = page.locator('.status-grid');
    await expect(statusGrid).toBeVisible();

    // Verify both implemented and roadmap cards exist
    const implementedCard = page.locator('.status-card--implemented');
    const roadmapCard = page.locator('.status-card--roadmap');
    await expect(implementedCard).toBeVisible();
    await expect(roadmapCard).toBeVisible();
  });

  test('TC2: Implemented features are visually distinguished', async ({ page }) => {
    // Test Case 2: Check for implemented features markers
    // Expected: Implemented features are visually distinguished (checkmarks, badges, etc.)

    const implementedCard = page.locator('.status-card--implemented');
    await expect(implementedCard).toBeVisible();

    // Check for implemented features list
    const implementedItems = implementedCard.locator('.status-list__item--complete');
    const itemCount = await implementedItems.count();
    expect(itemCount).toBeGreaterThanOrEqual(4);

    // Check that each implemented item has a checkmark
    const checkmarks = implementedCard.locator('.status-check');
    const checkmarkCount = await checkmarks.count();
    expect(checkmarkCount).toBeGreaterThanOrEqual(4);

    // Check for "Complete" badges
    const completeBadges = implementedCard.locator('.status-badge-inline--complete');
    const badgeCount = await completeBadges.count();
    expect(badgeCount).toBeGreaterThanOrEqual(4);

    // Verify specific implemented features are present
    const asyncNetworking = implementedCard.locator('text=Async');
    await expect(asyncNetworking).toBeVisible();

    const memtable = implementedCard.locator('text=Memtable');
    await expect(memtable).toBeVisible();

    const minorCompaction = implementedCard.locator('text=Minor Compaction');
    await expect(minorCompaction).toBeVisible();

    const majorCompaction = implementedCard.locator('text=Major Compaction');
    await expect(majorCompaction).toBeVisible();

    // Check the card title icon for implemented features
    const implementedIcon = implementedCard.locator('.status-icon--complete');
    await expect(implementedIcon).toBeVisible();
    await expect(implementedIcon).toContainText('✓');
  });

  test('TC3: Raft consensus is listed as a planned/roadmap item', async ({ page }) => {
    // Test Case 3: Check for 'Raft' or 'consensus' in roadmap
    // Expected: Raft consensus is listed as a planned/roadmap item

    const roadmapCard = page.locator('.status-card--roadmap');
    await expect(roadmapCard).toBeVisible();

    // Check for Raft consensus in roadmap
    const raftItem = roadmapCard.locator('[data-feature="raft-consensus"]');
    await expect(raftItem).toBeVisible();

    // Verify the text contains "Raft" or "Consensus"
    const raftText = roadmapCard.locator('text=Raft Consensus');
    await expect(raftText).toBeVisible();

    // Check that roadmap items have planned markers (not checkmarks)
    const plannedMarkers = roadmapCard.locator('.status-planned');
    const plannedCount = await plannedMarkers.count();
    expect(plannedCount).toBeGreaterThanOrEqual(1);

    // Check for "Planned" badges
    const plannedBadges = roadmapCard.locator('.status-badge-inline--planned');
    const badgeCount = await plannedBadges.count();
    expect(badgeCount).toBeGreaterThanOrEqual(1);

    // Verify roadmap card has appropriate title icon
    const roadmapIcon = roadmapCard.locator('.status-icon--planned');
    await expect(roadmapIcon).toBeVisible();

    // Ensure Raft is NOT marked as complete
    const raftCompleteCheck = roadmapCard.locator('[data-feature="raft-consensus"] .status-check');
    await expect(raftCompleteCheck).toHaveCount(0);
  });

  test('TC4: CircleCI build status badge is displayed', async ({ page }) => {
    // Test Case 4: Verify CircleCI badge integration
    // Expected: Build status badge from CircleCI is displayed

    // Check for the badge container
    const badgeContainer = page.locator('.status-badge-container');
    await expect(badgeContainer).toBeVisible();

    // Check for the CircleCI badge image
    const circleciBadge = page.locator('.status-badge');
    await expect(circleciBadge).toBeVisible();

    // Verify the badge has the correct src attribute containing CircleCI
    await expect(circleciBadge).toHaveAttribute('src', /circleci/i);

    // Verify the badge has alt text
    await expect(circleciBadge).toHaveAttribute('alt', /CircleCI|Build Status/i);

    // Check that the badge is wrapped in a link to CircleCI
    const badgeLink = page.locator('.status-badge-container a');
    await expect(badgeLink).toHaveAttribute('href', /circleci.*mirdb/i);

    // Verify security attributes on external link
    await expect(badgeLink).toHaveAttribute('target', '_blank');
    await expect(badgeLink).toHaveAttribute('rel', /noopener/);
  });

  test('Status section has accessible heading hierarchy', async ({ page }) => {
    // Accessibility test: verify proper heading structure
    const statusSection = page.locator('#status');

    // Check h2 heading
    const h2 = statusSection.locator('h2');
    await expect(h2).toBeVisible();

    // Check h3 headings for cards
    const h3Headings = statusSection.locator('h3');
    const h3Count = await h3Headings.count();
    expect(h3Count).toBe(2); // One for implemented, one for roadmap

    // Check that lists have proper role
    const lists = statusSection.locator('[role="list"]');
    const listCount = await lists.count();
    expect(listCount).toBe(2);
  });

  test('Status section is responsive', async ({ page }) => {
    // Test mobile responsiveness
    await page.setViewportSize({ width: 375, height: 667 });

    const statusSection = page.locator('#status');
    await expect(statusSection).toBeVisible();

    // Cards should stack on mobile
    const statusGrid = page.locator('.status-grid');
    await expect(statusGrid).toBeVisible();

    // Both cards should still be visible
    const implementedCard = page.locator('.status-card--implemented');
    const roadmapCard = page.locator('.status-card--roadmap');
    await expect(implementedCard).toBeVisible();
    await expect(roadmapCard).toBeVisible();
  });
});
