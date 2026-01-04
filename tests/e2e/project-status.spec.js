// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Project Status Display (REQ-9)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('should display list of currently implemented features', async ({ page }) => {
    // Navigate to project status section
    const statusSection = page.locator('#project-status, [data-testid="project-status"]');
    await statusSection.scrollIntoViewIfNeeded();
    await expect(statusSection).toBeVisible();

    // Check for implemented features heading
    const implementedHeading = statusSection.locator('h3:has-text("Implemented"), h4:has-text("Implemented")');
    await expect(implementedHeading).toBeVisible();

    // Verify implemented features list is displayed
    const implementedList = statusSection.locator('[data-testid="implemented-features"], .implemented-features');
    await expect(implementedList).toBeVisible();

    // Verify the list has multiple items
    const listItems = implementedList.locator('li');
    const itemCount = await listItems.count();
    expect(itemCount).toBeGreaterThanOrEqual(4);

    // Verify at least some key implemented features are present
    const listText = await implementedList.textContent();
    expect(listText).toMatch(/async|tokio|network/i);
    expect(listText).toMatch(/memtable|skip.*list/i);
    expect(listText).toMatch(/compaction/i);
  });

  test('should display planned features including Raft consensus', async ({ page }) => {
    // Navigate to project status section
    const statusSection = page.locator('#project-status, [data-testid="project-status"]');
    await statusSection.scrollIntoViewIfNeeded();
    await expect(statusSection).toBeVisible();

    // Check for planned/upcoming features heading
    const plannedHeading = statusSection.locator('h3:has-text("Planned"), h3:has-text("Upcoming"), h4:has-text("Planned"), h4:has-text("Upcoming"), h3:has-text("Coming Soon")');
    await expect(plannedHeading).toBeVisible();

    // Verify planned features list is displayed
    const plannedList = statusSection.locator('[data-testid="planned-features"], .planned-features');
    await expect(plannedList).toBeVisible();

    // Check specifically for Raft consensus as mentioned in PRD
    const plannedText = await plannedList.textContent();
    expect(plannedText).toMatch(/raft.*consensus|raft|consensus|distributed/i);
  });
});
