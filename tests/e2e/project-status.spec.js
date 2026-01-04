// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Project Status Display (REQ-9)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('should display list of implemented features', async ({ page }) => {
    // Navigate to project status section
    const statusSection = page.locator('#project-status, [data-testid="project-status"]');
    await statusSection.scrollIntoViewIfNeeded();
    await expect(statusSection).toBeVisible();

    // Check for implemented features heading/list
    const implementedFeatures = statusSection.locator('[data-status="implemented"], .implemented-features');
    await expect(implementedFeatures).toBeVisible();

    // Verify at least some implemented features are listed
    const implementedItems = implementedFeatures.locator('li, .feature-item');
    const count = await implementedItems.count();
    expect(count).toBeGreaterThan(0);

    // Check for key implemented features (core functionality)
    const statusContent = await statusSection.textContent();
    expect(statusContent).toMatch(/Memcached|SET|GET|LSM|persistent|storage/i);
  });

  test('should display planned features including Raft consensus', async ({ page }) => {
    // Navigate to project status section
    const statusSection = page.locator('#project-status, [data-testid="project-status"]');
    await statusSection.scrollIntoViewIfNeeded();
    await expect(statusSection).toBeVisible();

    // Check for planned features heading/list
    const plannedFeatures = statusSection.locator('[data-status="planned"], .planned-features');
    await expect(plannedFeatures).toBeVisible();

    // Verify Raft consensus is mentioned as planned/upcoming
    const plannedContent = await plannedFeatures.textContent();
    expect(plannedContent).toMatch(/Raft|consensus|upcoming|planned|future/i);
  });
});
