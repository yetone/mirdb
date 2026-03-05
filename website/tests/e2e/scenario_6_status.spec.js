/**
 * E2E Tests for Scenario 6: Project Status Section
 *
 * Tests verify that the project status section displays completed and planned
 * features with appropriate visual differentiation.
 */
const { test, expect } = require('@playwright/test');

test.describe('Project Status Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 1: Status section is present with appropriate heading', async ({ page }) => {
    // Locate the status section
    const statusSection = page.locator('#status');
    await expect(statusSection).toBeVisible();

    // Check for the "Project Status" heading
    const heading = statusSection.locator('h2');
    await expect(heading).toBeVisible();
    await expect(heading).toContainText('Project Status');
  });

  test('Test Case 2: Completed features are listed with checkmark indicators', async ({ page }) => {
    // Locate the completed features container
    const completedFeatures = page.locator('[data-testid="completed-features"]');
    await expect(completedFeatures).toBeVisible();

    // Check for the "Completed" heading
    const heading = completedFeatures.locator('h3');
    await expect(heading).toBeVisible();
    await expect(heading).toContainText('Completed');

    // Check for completed features list
    const completedList = page.locator('[data-testid="completed-features-list"]');
    await expect(completedList).toBeVisible();

    // Verify there are completed items
    const completedItems = page.locator('[data-testid="completed-item"]');
    const count = await completedItems.count();
    expect(count).toBeGreaterThanOrEqual(4);

    // Verify each completed item has a checkmark icon
    for (let i = 0; i < count; i++) {
      const item = completedItems.nth(i);
      const checkmark = item.locator('.checkmark-icon');
      await expect(checkmark).toBeVisible();
    }

    // Verify specific completed features are present
    await expect(completedFeatures).toContainText('Memcached protocol support');
    await expect(completedFeatures).toContainText('SSTables persistence');
    await expect(completedFeatures).toContainText('LSM Tree storage');
    await expect(completedFeatures).toContainText('Write-ahead logging');
  });

  test('Test Case 3: Planned features are listed, including Raft consensus', async ({ page }) => {
    // Locate the planned features container
    const plannedFeatures = page.locator('[data-testid="planned-features"]');
    await expect(plannedFeatures).toBeVisible();

    // Check for the "Planned" heading
    const heading = plannedFeatures.locator('h3');
    await expect(heading).toBeVisible();
    await expect(heading).toContainText('Planned');

    // Check for planned features list
    const plannedList = page.locator('[data-testid="planned-features-list"]');
    await expect(plannedList).toBeVisible();

    // Verify there are planned items
    const plannedItems = page.locator('[data-testid="planned-item"]');
    const count = await plannedItems.count();
    expect(count).toBeGreaterThanOrEqual(1);

    // Verify Raft consensus is present
    await expect(plannedFeatures).toContainText('Raft consensus');

    // Verify Raft consensus has a highlight/badge
    const raftHighlight = page.locator('[data-testid="raft-highlight"]');
    await expect(raftHighlight).toBeVisible();
    await expect(raftHighlight).toContainText('Coming Soon');
  });

  test('Test Case 4: Completed and planned features are visually differentiated', async ({ page }) => {
    // Check that completed and planned sections have different backgrounds
    const completedFeatures = page.locator('[data-testid="completed-features"]');
    const plannedFeatures = page.locator('[data-testid="planned-features"]');

    // Both sections should be visible
    await expect(completedFeatures).toBeVisible();
    await expect(plannedFeatures).toBeVisible();

    // Check that completed section has green styling
    await expect(completedFeatures).toHaveClass(/bg-green/);
    await expect(completedFeatures).toHaveClass(/border-green/);

    // Check that planned section has blue styling
    await expect(plannedFeatures).toHaveClass(/bg-blue/);
    await expect(plannedFeatures).toHaveClass(/border-blue/);

    // Verify completed items use checkmark icons (green)
    const completedItems = page.locator('[data-testid="completed-item"]');
    const completedCount = await completedItems.count();
    for (let i = 0; i < completedCount; i++) {
      const icon = completedItems.nth(i).locator('.checkmark-icon');
      await expect(icon).toBeVisible();
      await expect(icon).toHaveClass(/text-green/);
    }

    // Verify planned items use different icons (blue)
    const plannedItems = page.locator('[data-testid="planned-item"]');
    const plannedCount = await plannedItems.count();
    for (let i = 0; i < plannedCount; i++) {
      const icon = plannedItems.nth(i).locator('.planned-icon');
      await expect(icon).toBeVisible();
      await expect(icon).toHaveClass(/text-blue/);
    }
  });

  test('Status lists are displayed in a grid layout', async ({ page }) => {
    const container = page.locator('[data-testid="status-lists-container"]');
    await expect(container).toBeVisible();
    await expect(container).toHaveClass(/grid/);
  });
});
