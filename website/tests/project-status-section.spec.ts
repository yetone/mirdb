import { test, expect } from '@playwright/test';

test.describe('Project Status Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Project Status or Roadmap section exists on the page', async ({ page }) => {
    // Look for a section that contains project status or roadmap information
    const statusSection = page.locator('#project-status, .project-status-section');
    await expect(statusSection).toBeVisible();

    // Verify the section has a title indicating project status or roadmap
    const sectionTitle = statusSection.locator('.section-title');
    await expect(sectionTitle).toBeVisible();
    const titleText = await sectionTitle.textContent();
    expect(titleText?.toLowerCase()).toMatch(/project status|roadmap|development status/i);
  });

  test('TC2: Completed features are displayed and clearly marked as complete', async ({ page }) => {
    const statusSection = page.locator('#project-status, .project-status-section');
    await expect(statusSection).toBeVisible();

    // Look for a completed features section
    const completedSection = statusSection.locator('.completed-features, [data-status="completed"]').first();
    await expect(completedSection).toBeVisible();

    // Check that there's a header/label indicating these are completed
    const completedHeader = statusSection.locator('h3, h4, .status-category-title').filter({ hasText: /completed|done|production|ready/i }).first();
    await expect(completedHeader).toBeVisible();

    // Verify there are completed feature items listed
    const completedItems = statusSection.locator('.status-item[data-status="completed"], .completed-features .status-item');
    const itemCount = await completedItems.count();
    expect(itemCount).toBeGreaterThan(0);
  });

  test('TC3: Planned features are displayed, including Raft consensus', async ({ page }) => {
    const statusSection = page.locator('#project-status, .project-status-section');
    await expect(statusSection).toBeVisible();

    // Look for a planned features section
    const plannedSection = statusSection.locator('.planned-features, [data-status="planned"]').first();
    await expect(plannedSection).toBeVisible();

    // Check that there's a header/label indicating these are planned
    const plannedHeader = statusSection.locator('h3, h4, .status-category-title').filter({ hasText: /planned|roadmap|upcoming|future/i }).first();
    await expect(plannedHeader).toBeVisible();

    // Verify Raft consensus is mentioned as a planned feature
    const raftFeature = statusSection.locator('.status-item').filter({ hasText: /raft/i });
    await expect(raftFeature).toBeVisible();

    // Verify it's in the planned section
    const raftText = await raftFeature.textContent();
    expect(raftText?.toLowerCase()).toContain('raft');
  });

  test('TC4: Completed and planned features are visually distinguishable', async ({ page }) => {
    const statusSection = page.locator('#project-status, .project-status-section');
    await expect(statusSection).toBeVisible();

    // Get completed and planned sections
    const completedSection = statusSection.locator('.completed-features, .status-category').filter({ hasText: /completed|done|production/i }).first();
    const plannedSection = statusSection.locator('.planned-features, .status-category').filter({ hasText: /planned|roadmap|upcoming/i }).first();

    await expect(completedSection).toBeVisible();
    await expect(plannedSection).toBeVisible();

    // Get a completed item and a planned item to compare styling
    const completedItem = statusSection.locator('.status-item[data-status="completed"]').first();
    const plannedItem = statusSection.locator('.status-item[data-status="planned"]').first();

    // Check that completed items have a visual indicator (checkmark or specific icon/styling)
    const completedIcon = completedItem.locator('.status-icon, .feature-check');
    await expect(completedIcon).toBeVisible();
    const completedIconText = await completedIcon.textContent();

    // Check that planned items have a different visual indicator
    const plannedIcon = plannedItem.locator('.status-icon, .feature-check');
    await expect(plannedIcon).toBeVisible();
    const plannedIconText = await plannedIcon.textContent();

    // The icons/indicators should be different
    expect(completedIconText).not.toBe(plannedIconText);
  });
});
