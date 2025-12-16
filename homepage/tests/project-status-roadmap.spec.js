// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Project Status and Roadmap (REQ-9)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Search for project status section
   * Expected: Page indicates completed features (server, memtable, compaction)
   */
  test('should display project status section with completed features', async ({ page }) => {
    // Navigate to project status section
    const statusSection = page.locator('[data-testid="project-status-section"]');
    await expect(statusSection).toBeVisible();

    // Scroll to status section to ensure it's in view
    await statusSection.scrollIntoViewIfNeeded();

    // Verify the section has a heading
    const heading = statusSection.locator('h2');
    await expect(heading).toBeVisible();

    // Find the completed features list
    const completedFeatures = statusSection.locator('[data-testid="completed-features"]');
    await expect(completedFeatures).toBeVisible();

    // Verify completed features include server, memtable, and compaction
    const completedText = await completedFeatures.textContent();
    expect(completedText?.toLowerCase()).toContain('server');
    expect(completedText?.toLowerCase()).toContain('memtable');
    expect(completedText?.toLowerCase()).toContain('compaction');
  });

  /**
   * Test Case 2: Search for roadmap information
   * Expected: Page mentions planned features (Raft consensus for distributed deployment)
   */
  test('should display roadmap with planned features', async ({ page }) => {
    // Navigate to project status section
    const statusSection = page.locator('[data-testid="project-status-section"]');
    await expect(statusSection).toBeVisible();

    // Scroll to status section to ensure it's in view
    await statusSection.scrollIntoViewIfNeeded();

    // Find the planned features / roadmap list
    const plannedFeatures = statusSection.locator('[data-testid="planned-features"]');
    await expect(plannedFeatures).toBeVisible();

    // Verify planned features mention Raft consensus for distributed deployment
    const plannedText = await plannedFeatures.textContent();
    expect(plannedText?.toLowerCase()).toContain('raft');

    // Also check for distributed deployment context
    const hasDistributed = plannedText?.toLowerCase().includes('distributed');
    const hasConsensus = plannedText?.toLowerCase().includes('consensus');
    expect(hasDistributed || hasConsensus).toBeTruthy();
  });

  /**
   * Test Case 3: Verify clear distinction between completed and planned
   * Expected: Completed and planned features are clearly differentiated
   */
  test('should clearly differentiate between completed and planned features', async ({ page }) => {
    // Navigate to project status section
    const statusSection = page.locator('[data-testid="project-status-section"]');
    await expect(statusSection).toBeVisible();

    // Scroll to status section to ensure it's in view
    await statusSection.scrollIntoViewIfNeeded();

    // Verify both completed and planned sections exist and are separate
    const completedFeatures = statusSection.locator('[data-testid="completed-features"]');
    const plannedFeatures = statusSection.locator('[data-testid="planned-features"]');

    await expect(completedFeatures).toBeVisible();
    await expect(plannedFeatures).toBeVisible();

    // Verify they have distinct headings
    const completedHeading = completedFeatures.locator('h3');
    const plannedHeading = plannedFeatures.locator('h3');

    await expect(completedHeading).toBeVisible();
    await expect(plannedHeading).toBeVisible();

    // Verify the headings contain appropriate text to differentiate them
    const completedHeadingText = await completedHeading.textContent();
    const plannedHeadingText = await plannedHeading.textContent();

    // Completed section should indicate "completed", "done", "implemented", etc.
    const hasCompletedIndicator =
      completedHeadingText?.toLowerCase().includes('completed') ||
      completedHeadingText?.toLowerCase().includes('done') ||
      completedHeadingText?.toLowerCase().includes('implemented') ||
      completedHeadingText?.toLowerCase().includes('current');
    expect(hasCompletedIndicator).toBeTruthy();

    // Planned section should indicate "planned", "roadmap", "upcoming", etc.
    const hasPlannedIndicator =
      plannedHeadingText?.toLowerCase().includes('planned') ||
      plannedHeadingText?.toLowerCase().includes('roadmap') ||
      plannedHeadingText?.toLowerCase().includes('upcoming') ||
      plannedHeadingText?.toLowerCase().includes('future');
    expect(hasPlannedIndicator).toBeTruthy();

    // Verify they are visually distinct (different styling or visual indicators)
    // Check that completed items have checkmarks or similar visual indicators
    const completedItems = completedFeatures.locator('li');
    const completedItemsCount = await completedItems.count();
    expect(completedItemsCount).toBeGreaterThan(0);

    // Check that planned items exist
    const plannedItems = plannedFeatures.locator('li');
    const plannedItemsCount = await plannedItems.count();
    expect(plannedItemsCount).toBeGreaterThan(0);
  });

  /**
   * Additional test: Navigation to project status section
   */
  test('should have navigation link to project status section', async ({ page }) => {
    // Check for navigation link to project status section
    const navLink = page.locator('nav a[href="#project-status"]');
    await expect(navLink).toBeVisible();

    // Click the navigation link
    await navLink.click();

    // Verify the section is visible
    const statusSection = page.locator('[data-testid="project-status-section"]');
    await expect(statusSection).toBeVisible();
  });

  /**
   * Additional test: Project status section has proper heading
   */
  test('should display Project Status heading', async ({ page }) => {
    const statusSection = page.locator('[data-testid="project-status-section"]');
    await expect(statusSection).toBeVisible();

    const heading = statusSection.locator('h2');
    await expect(heading).toHaveText(/Project Status|Roadmap|Status & Roadmap/i);
  });
});
