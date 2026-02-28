/**
 * Status Section E2E Tests
 * Owner: Scenario 5 - Project Status Section
 *
 * Test cases:
 * - Feature checklist presence
 * - Completed features marked with checkmarks
 * - Raft marked as "Coming Soon"
 * - CircleCI badge loads and links correctly
 */

const { test, expect } = require('@playwright/test');
const { waitForPageLoad } = require('./test-utils');

test.describe('Project Status Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await waitForPageLoad(page);
  });

  test('status section displays feature checklist', async ({ page }) => {
    // Navigate to status section
    const statusSection = page.locator('#status');
    await expect(statusSection).toBeVisible();

    // Check that feature checklist exists
    const checklist = statusSection.locator('.feature-checklist');
    await expect(checklist).toBeVisible();

    // Verify checklist items exist
    const checklistItems = statusSection.locator('.feature-item');
    const count = await checklistItems.count();
    expect(count).toBeGreaterThanOrEqual(4);
  });

  test('tokio with memcached protocol is marked complete', async ({ page }) => {
    const statusSection = page.locator('#status');

    // Find the feature item containing "tokio" (case-insensitive)
    const tokioFeature = statusSection.locator('.feature-item', {
      hasText: /tokio.*memcached|memcached.*protocol/i
    });
    await expect(tokioFeature).toBeVisible();

    // Verify it has the completed class
    await expect(tokioFeature).toHaveClass(/completed/);

    // Verify checkmark is visible
    const checkmark = tokioFeature.locator('.checkmark');
    await expect(checkmark).toBeVisible();
    await expect(checkmark).toHaveText('✓');
  });

  test('raft is marked as planned/coming soon', async ({ page }) => {
    const statusSection = page.locator('#status');

    // Find the raft feature item
    const raftFeature = statusSection.locator('.feature-item', {
      hasText: /raft/i
    });
    await expect(raftFeature).toBeVisible();

    // Verify it has the coming-soon class
    await expect(raftFeature).toHaveClass(/coming-soon/);

    // Verify "Coming Soon" badge is visible
    const badge = raftFeature.locator('.badge');
    await expect(badge).toBeVisible();
    await expect(badge).toHaveText('Coming Soon');
  });

  test('CircleCI badge image loads', async ({ page }) => {
    const statusSection = page.locator('#status');

    // Find the CI badge container
    const badgeContainer = statusSection.locator('.status-badge');
    await expect(badgeContainer).toBeVisible();

    // Find the badge image
    const badgeImg = badgeContainer.locator('img');
    await expect(badgeImg).toBeVisible();

    // Verify the image source is from CircleCI
    const src = await badgeImg.getAttribute('src');
    expect(src).toContain('circleci.com');

    // Verify the image has proper alt text
    const alt = await badgeImg.getAttribute('alt');
    expect(alt).toBeTruthy();
    expect(alt.toLowerCase()).toContain('circleci');
  });

  test('CircleCI badge links to CircleCI project page in new tab', async ({ page }) => {
    const statusSection = page.locator('#status');

    // Find the badge link
    const badgeLink = statusSection.locator('.status-badge a');
    await expect(badgeLink).toBeVisible();

    // Verify link href points to CircleCI
    const href = await badgeLink.getAttribute('href');
    expect(href).toContain('circleci.com');
    expect(href).toContain('mirdb');

    // Verify link opens in new tab
    const target = await badgeLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify rel attribute for security
    const rel = await badgeLink.getAttribute('rel');
    expect(rel).toContain('noopener');
  });

  test('all completed features have checkmarks', async ({ page }) => {
    const statusSection = page.locator('#status');

    // Find all completed feature items
    const completedItems = statusSection.locator('.feature-item.completed');
    const count = await completedItems.count();

    // Should have at least 3 completed features (tokio, memtable, compaction)
    expect(count).toBeGreaterThanOrEqual(3);

    // Verify each completed item has a green checkmark
    for (let i = 0; i < count; i++) {
      const item = completedItems.nth(i);
      const checkmark = item.locator('.checkmark');
      await expect(checkmark).toHaveText('✓');
    }
  });

  test('status section has proper heading', async ({ page }) => {
    const statusSection = page.locator('#status');

    // Verify the section has a proper heading
    const heading = statusSection.locator('h2');
    await expect(heading).toBeVisible();

    // The heading should contain "Status"
    const headingText = await heading.textContent();
    expect(headingText.toLowerCase()).toContain('status');
  });
});
