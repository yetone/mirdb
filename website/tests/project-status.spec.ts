import { test, expect } from '@playwright/test';

/**
 * E2E Tests for Project Status Display
 * Scenario: Verify that the project status section displays implemented and planned features
 * REQ-9: Display project status including implemented and planned features
 */

test.describe('Project Status Display', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to homepage before each test
    await page.goto('/');
  });

  /**
   * Test Case 1: Check for project status section
   * Input: Check for project status section
   * Expected: Project status or roadmap section is visible
   */
  test('should display project status section with heading', async ({ page }) => {
    // Verify the project status section exists and is visible
    const statusSection = page.locator('[data-testid="project-status-section"]');
    await expect(statusSection).toBeVisible();

    // Verify the section heading exists
    const heading = page.locator('[data-testid="project-status-heading"]');
    await expect(heading).toBeVisible();

    // Verify the heading text contains "Project Status"
    const headingText = await heading.textContent();
    expect(headingText).toBeTruthy();
    expect(headingText?.toLowerCase()).toContain('project status');
  });

  /**
   * Test Case 2: Verify implemented features displayed
   * Input: Verify implemented features displayed
   * Expected: Shows implemented features: async networking, memtable, compaction
   */
  test('should display implemented features including async networking, memtable, and compaction', async ({ page }) => {
    // Verify the implemented features section exists
    const implementedSection = page.locator('[data-testid="implemented-features"]');
    await expect(implementedSection).toBeVisible();

    // Verify the implemented features heading
    const implementedHeading = page.locator('[data-testid="implemented-features-heading"]');
    await expect(implementedHeading).toBeVisible();
    const headingText = await implementedHeading.textContent();
    expect(headingText?.toLowerCase()).toContain('implemented');

    // Verify the implemented features list exists
    const featuresList = page.locator('[data-testid="implemented-features-list"]');
    await expect(featuresList).toBeVisible();

    // Verify async networking feature is displayed
    const asyncNetworking = page.locator('[data-testid="status-async-networking"]');
    await expect(asyncNetworking).toBeVisible();
    const asyncNetworkingText = await asyncNetworking.textContent();
    expect(asyncNetworkingText?.toLowerCase()).toMatch(/(async|networking|tokio)/);

    // Verify memtable feature is displayed
    const memtable = page.locator('[data-testid="status-memtable"]');
    await expect(memtable).toBeVisible();
    const memtableText = await memtable.textContent();
    expect(memtableText?.toLowerCase()).toContain('memtable');

    // Verify minor compaction feature is displayed
    const minorCompaction = page.locator('[data-testid="status-minor-compaction"]');
    await expect(minorCompaction).toBeVisible();
    const minorCompactionText = await minorCompaction.textContent();
    expect(minorCompactionText?.toLowerCase()).toMatch(/(minor|compaction)/);

    // Verify major compaction feature is displayed
    const majorCompaction = page.locator('[data-testid="status-major-compaction"]');
    await expect(majorCompaction).toBeVisible();
    const majorCompactionText = await majorCompaction.textContent();
    expect(majorCompactionText?.toLowerCase()).toMatch(/(major|compaction)/);
  });

  /**
   * Test Case 3: Verify planned features displayed
   * Input: Verify planned features displayed
   * Expected: Shows planned feature: Raft consensus for distributed operation
   */
  test('should display planned features including Raft consensus for distributed operation', async ({ page }) => {
    // Verify the planned features section exists
    const plannedSection = page.locator('[data-testid="planned-features"]');
    await expect(plannedSection).toBeVisible();

    // Verify the planned features heading
    const plannedHeading = page.locator('[data-testid="planned-features-heading"]');
    await expect(plannedHeading).toBeVisible();
    const headingText = await plannedHeading.textContent();
    expect(headingText?.toLowerCase()).toContain('planned');

    // Verify the planned features list exists
    const featuresList = page.locator('[data-testid="planned-features-list"]');
    await expect(featuresList).toBeVisible();

    // Verify Raft consensus feature is displayed
    const raftConsensus = page.locator('[data-testid="status-raft-consensus"]');
    await expect(raftConsensus).toBeVisible();

    // Verify the feature mentions Raft consensus and distributed operation
    const raftText = await raftConsensus.textContent();
    expect(raftText).toBeTruthy();
    expect(raftText?.toLowerCase()).toContain('raft');
    expect(raftText?.toLowerCase()).toMatch(/(consensus|distributed)/);
  });

  /**
   * Additional test: Verify section navigation
   */
  test('should have proper section structure with implemented and planned columns', async ({ page }) => {
    // Verify the project status section exists
    const statusSection = page.locator('[data-testid="project-status-section"]');
    await expect(statusSection).toBeVisible();

    // Verify both implemented and planned sections are visible
    const implementedSection = page.locator('[data-testid="implemented-features"]');
    const plannedSection = page.locator('[data-testid="planned-features"]');

    await expect(implementedSection).toBeVisible();
    await expect(plannedSection).toBeVisible();

    // Verify implemented features list has items
    const implementedItems = page.locator('[data-testid="implemented-features-list"] > li');
    const implementedCount = await implementedItems.count();
    expect(implementedCount).toBeGreaterThanOrEqual(3); // At least async networking, memtable, compaction

    // Verify planned features list has items
    const plannedItems = page.locator('[data-testid="planned-features-list"] > li');
    const plannedCount = await plannedItems.count();
    expect(plannedCount).toBeGreaterThanOrEqual(1); // At least Raft consensus
  });

  /**
   * Additional test: Verify feature cards have titles and descriptions
   */
  test('should display feature titles and descriptions for all status items', async ({ page }) => {
    // Test implemented features have titles and descriptions
    const featureIds = ['async-networking', 'memtable', 'minor-compaction', 'major-compaction', 'raft-consensus'];

    for (const featureId of featureIds) {
      const featureItem = page.locator(`[data-testid="status-${featureId}"]`);
      await expect(featureItem).toBeVisible();

      // Verify title exists
      const title = page.locator(`[data-testid="status-${featureId}-title"]`);
      await expect(title).toBeVisible();
      const titleText = await title.textContent();
      expect(titleText).toBeTruthy();
      expect(titleText!.length).toBeGreaterThan(0);

      // Verify description exists
      const description = page.locator(`[data-testid="status-${featureId}-description"]`);
      await expect(description).toBeVisible();
      const descText = await description.textContent();
      expect(descText).toBeTruthy();
      expect(descText!.length).toBeGreaterThan(10); // Should have meaningful description
    }
  });
});
