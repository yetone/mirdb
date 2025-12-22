// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Project Status Display Tests
 *
 * Scenario: Verify project status section shows implemented and planned features
 *
 * This test suite validates that the project status section correctly displays:
 * - Implemented features (async networking, memtable, compaction)
 * - Planned features (Raft consensus)
 * - Link to GitHub for more details
 */

test.describe('Project Status Display', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to the homepage before each test
    await page.goto('/');
  });

  /**
   * Step 1: Locate project status section
   * Verify the project status section exists and is visible on the homepage
   */
  test('should have a visible project status section', async ({ page }) => {
    const statusSection = page.locator('#status, [data-testid="status-section"], .status-section');
    await expect(statusSection).toBeVisible();

    // Verify it has a title/heading
    const sectionTitle = statusSection.locator('h2, .section-title');
    await expect(sectionTitle).toBeVisible();
    const titleText = await sectionTitle.textContent();
    expect(titleText.toLowerCase()).toMatch(/status|roadmap|features/i);
  });

  /**
   * Test Case 1: Check for async networking feature status
   * Expected: Tokio-based async networking is listed as implemented
   */
  test('should list Tokio-based async networking as implemented', async ({ page }) => {
    const statusSection = page.locator('#status, [data-testid="status-section"], .status-section');
    await expect(statusSection).toBeVisible();

    // Look for implemented features container
    const implementedSection = statusSection.locator('[data-testid="implemented-features"], .implemented-features, .status-implemented');
    await expect(implementedSection).toBeVisible();

    // Check for async networking / Tokio feature
    const asyncNetworkingFeature = statusSection.locator('text=/tokio|async.*network/i');
    await expect(asyncNetworkingFeature.first()).toBeVisible();

    // Verify it's in the implemented section (has checkmark or implemented indicator)
    const sectionText = await implementedSection.textContent();
    expect(sectionText.toLowerCase()).toMatch(/tokio|async.*network/i);
  });

  /**
   * Test Case 2: Check for memtable feature status
   * Expected: Memtable with skip list is listed as implemented
   */
  test('should list Memtable with skip list as implemented', async ({ page }) => {
    const statusSection = page.locator('#status, [data-testid="status-section"], .status-section');
    await expect(statusSection).toBeVisible();

    // Look for implemented features container
    const implementedSection = statusSection.locator('[data-testid="implemented-features"], .implemented-features, .status-implemented');
    await expect(implementedSection).toBeVisible();

    // Check for memtable / skip list feature
    const memtableFeature = statusSection.locator('text=/memtable|skip.*list/i');
    await expect(memtableFeature.first()).toBeVisible();

    // Verify it's in the implemented section
    const sectionText = await implementedSection.textContent();
    expect(sectionText.toLowerCase()).toMatch(/memtable|skip.*list/i);
  });

  /**
   * Test Case 3: Check for compaction feature status
   * Expected: Minor and major compaction are listed as implemented
   */
  test('should list minor and major compaction as implemented', async ({ page }) => {
    const statusSection = page.locator('#status, [data-testid="status-section"], .status-section');
    await expect(statusSection).toBeVisible();

    // Look for implemented features container
    const implementedSection = statusSection.locator('[data-testid="implemented-features"], .implemented-features, .status-implemented');
    await expect(implementedSection).toBeVisible();

    // Check for compaction features
    const compactionFeature = statusSection.locator('text=/compaction/i');
    await expect(compactionFeature.first()).toBeVisible();

    // Verify both minor and major compaction are mentioned
    const sectionText = await implementedSection.textContent();
    expect(sectionText.toLowerCase()).toMatch(/minor.*compaction|compaction.*minor/i);
    expect(sectionText.toLowerCase()).toMatch(/major.*compaction|compaction.*major/i);
  });

  /**
   * Test Case 4: Check for Raft consensus status
   * Expected: Raft consensus is listed as planned/upcoming
   */
  test('should list Raft consensus as planned/upcoming', async ({ page }) => {
    const statusSection = page.locator('#status, [data-testid="status-section"], .status-section');
    await expect(statusSection).toBeVisible();

    // Look for planned features container
    const plannedSection = statusSection.locator('[data-testid="planned-features"], .planned-features, .status-planned');
    await expect(plannedSection).toBeVisible();

    // Check for Raft consensus feature
    const raftFeature = statusSection.locator('text=/raft.*consensus|consensus.*raft/i');
    await expect(raftFeature.first()).toBeVisible();

    // Verify it's in the planned section
    const sectionText = await plannedSection.textContent();
    expect(sectionText.toLowerCase()).toMatch(/raft/i);
  });

  /**
   * Test Case 5: Verify link to GitHub for more details
   * Expected: Status section includes link to GitHub for detailed roadmap
   */
  test('should include link to GitHub for detailed roadmap', async ({ page }) => {
    const statusSection = page.locator('#status, [data-testid="status-section"], .status-section');
    await expect(statusSection).toBeVisible();

    // Look for GitHub link within the status section
    const githubLink = statusSection.locator('a[href*="github.com"]');
    await expect(githubLink.first()).toBeVisible();

    // Verify the link points to the GitHub repository
    const href = await githubLink.first().getAttribute('href');
    expect(href).toMatch(/github\.com/i);

    // Verify link opens in new tab for external link
    const target = await githubLink.first().getAttribute('target');
    expect(target).toBe('_blank');

    // Verify security attributes
    const rel = await githubLink.first().getAttribute('rel');
    expect(rel).toContain('noopener');
  });

  /**
   * Verify implemented features have visual indicators (checkmarks or similar)
   */
  test('should display visual indicators for implemented features', async ({ page }) => {
    const statusSection = page.locator('#status, [data-testid="status-section"], .status-section');
    await expect(statusSection).toBeVisible();

    // Look for implemented features with indicators
    const implementedSection = statusSection.locator('[data-testid="implemented-features"], .implemented-features, .status-implemented');
    await expect(implementedSection).toBeVisible();

    // Check that there are status indicators (checkmarks, icons, or visual markers)
    const statusIndicators = implementedSection.locator('.status-indicator, .check-icon, svg, [data-testid="status-indicator"]');
    const indicatorCount = await statusIndicators.count();

    // Should have at least one indicator
    expect(indicatorCount).toBeGreaterThanOrEqual(1);
  });

  /**
   * Verify planned features have visual indicators (upcoming/planned markers)
   */
  test('should display visual indicators for planned features', async ({ page }) => {
    const statusSection = page.locator('#status, [data-testid="status-section"], .status-section');
    await expect(statusSection).toBeVisible();

    // Look for planned features with indicators
    const plannedSection = statusSection.locator('[data-testid="planned-features"], .planned-features, .status-planned');
    await expect(plannedSection).toBeVisible();

    // Check that there are status indicators
    const statusIndicators = plannedSection.locator('.status-indicator, .planned-icon, svg, [data-testid="status-indicator"]');
    const indicatorCount = await statusIndicators.count();

    // Should have at least one indicator
    expect(indicatorCount).toBeGreaterThanOrEqual(1);
  });
});
