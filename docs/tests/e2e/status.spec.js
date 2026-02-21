// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E tests for Project Status Section
 * Owner: Scenario 6
 *
 * Test cases:
 * - Status section heading
 * - Implemented features list
 * - Planned features list
 * - Visual differentiation between implemented/planned
 */

test.describe('Status Section', () => {
  test.beforeEach(async ({ page, baseURL }) => {
    await page.goto(baseURL);
    await page.waitForLoadState('domcontentloaded');
  });

  test('status section exists with heading', async ({ page }) => {
    // Test case 1: Check Status section exists with h2 'Status' or 'Roadmap' heading
    const statusSection = page.locator('#status');
    await expect(statusSection).toBeVisible();

    const heading = statusSection.locator('h2');
    await expect(heading).toBeVisible();
    await expect(heading).toHaveText(/Status|Roadmap/i);
  });

  test('implemented features list shows required features', async ({ page }) => {
    // Test case 2: Verify implemented features list
    // Should show: tokio async networking, memtable with skiplist, minor compaction, major compaction
    const implementedSection = page.locator('[data-testid="implemented-features"]');
    await expect(implementedSection).toBeVisible();

    // Check for tokio async networking
    const tokioFeature = page.locator('[data-testid="feature-tokio"]');
    await expect(tokioFeature).toBeVisible();
    const tokioText = await tokioFeature.textContent();
    expect(tokioText.toLowerCase()).toMatch(/tokio|async|networking/i);

    // Check for memtable with skiplist
    const memtableFeature = page.locator('[data-testid="feature-memtable"]');
    await expect(memtableFeature).toBeVisible();
    const memtableText = await memtableFeature.textContent();
    expect(memtableText.toLowerCase()).toMatch(/memtable|skip\s*list/i);

    // Check for minor compaction
    const minorCompactionFeature = page.locator('[data-testid="feature-minor-compaction"]');
    await expect(minorCompactionFeature).toBeVisible();
    const minorText = await minorCompactionFeature.textContent();
    expect(minorText.toLowerCase()).toMatch(/minor\s*compaction/i);

    // Check for major compaction
    const majorCompactionFeature = page.locator('[data-testid="feature-major-compaction"]');
    await expect(majorCompactionFeature).toBeVisible();
    const majorText = await majorCompactionFeature.textContent();
    expect(majorText.toLowerCase()).toMatch(/major\s*compaction/i);
  });

  test('planned features list shows Raft consensus', async ({ page }) => {
    // Test case 3: Verify planned features list shows Raft consensus as planned/upcoming
    const plannedSection = page.locator('[data-testid="planned-features"]');
    await expect(plannedSection).toBeVisible();

    // Check for Raft consensus
    const raftFeature = page.locator('[data-testid="feature-raft"]');
    await expect(raftFeature).toBeVisible();
    const raftText = await raftFeature.textContent();
    expect(raftText.toLowerCase()).toMatch(/raft|consensus/i);
  });

  test('implemented features have checkmark indicators', async ({ page }) => {
    // Test case 4a: Implemented features have checkmark or completed indicator
    const implementedItems = page.locator('.status-item-implemented');
    const count = await implementedItems.count();
    expect(count).toBeGreaterThanOrEqual(4);

    // Each implemented item should have a checkmark SVG
    for (let i = 0; i < count; i++) {
      const item = implementedItems.nth(i);
      const indicator = item.locator('.status-indicator-implemented svg');
      await expect(indicator).toBeVisible();
    }
  });

  test('planned features have different visual indicator', async ({ page }) => {
    // Test case 4b: Planned features have different visual (not checkmark)
    const plannedItems = page.locator('.status-item-planned');
    const count = await plannedItems.count();
    expect(count).toBeGreaterThanOrEqual(1);

    // Each planned item should have a different indicator (clock icon)
    for (let i = 0; i < count; i++) {
      const item = plannedItems.nth(i);
      const indicator = item.locator('.status-indicator-planned svg');
      await expect(indicator).toBeVisible();
    }
  });

  test('visual differentiation between implemented and planned', async ({ page }) => {
    // Test case 4c: Verify visual differentiation via color classes
    const implementedIndicator = page.locator('.status-indicator-implemented').first();
    const plannedIndicator = page.locator('.status-indicator-planned').first();

    await expect(implementedIndicator).toBeVisible();
    await expect(plannedIndicator).toBeVisible();

    // Check that implemented features use green color (via class or computed style)
    const implementedSvg = implementedIndicator.locator('svg');
    await expect(implementedSvg).toHaveClass(/text-green-600|text-green-400/);

    // Check that planned features use amber/yellow color
    const plannedSvg = plannedIndicator.locator('svg');
    await expect(plannedSvg).toHaveClass(/text-amber-600|text-amber-400/);
  });

  test('status section has proper structure with two groups', async ({ page }) => {
    // Additional: verify section structure
    const statusSection = page.locator('#status');
    await expect(statusSection).toBeVisible();

    // Should have two status groups
    const statusGroups = statusSection.locator('.status-group');
    await expect(statusGroups).toHaveCount(2);

    // First group should be implemented features
    const implementedGroup = page.locator('[data-testid="implemented-features"]');
    await expect(implementedGroup).toBeVisible();
    const implementedHeading = implementedGroup.locator('h3');
    await expect(implementedHeading).toContainText(/Implemented/i);

    // Second group should be planned features
    const plannedGroup = page.locator('[data-testid="planned-features"]');
    await expect(plannedGroup).toBeVisible();
    const plannedHeading = plannedGroup.locator('h3');
    await expect(plannedHeading).toContainText(/Planned/i);
  });

  test('status section has correct accessibility attributes', async ({ page }) => {
    // Additional: verify accessibility
    const statusSection = page.locator('#status');
    await expect(statusSection).toHaveAttribute('aria-labelledby', 'status-heading');

    const heading = page.locator('#status-heading');
    await expect(heading).toBeVisible();

    // Lists should have proper role
    const implementedList = page.locator('[data-testid="implemented-features"] ul');
    await expect(implementedList).toHaveAttribute('role', 'list');
    await expect(implementedList).toHaveAttribute('aria-label', 'Implemented features');

    const plannedList = page.locator('[data-testid="planned-features"] ul');
    await expect(plannedList).toHaveAttribute('role', 'list');
    await expect(plannedList).toHaveAttribute('aria-label', 'Planned features');
  });
});
