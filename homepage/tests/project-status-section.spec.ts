import { test, expect } from '@playwright/test';

test.describe('Project Status Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Check for async networking feature status', async ({ page }) => {
    // Navigate to project status section
    const statusSection = page.locator('[data-testid="project-status-section"]');
    await expect(statusSection).toBeVisible();

    // Check for async networking feature in implemented features list
    const implementedList = page.locator('[data-testid="implemented-features-list"]');
    await expect(implementedList).toBeVisible();

    // Check that Tokio-based async networking is marked as implemented
    const asyncNetworkingItem = page.locator('[data-testid="feature-async-networking"]');
    await expect(asyncNetworkingItem).toBeVisible();
    await expect(asyncNetworkingItem).toContainText(/Tokio.*async.*networking/i);

    // Verify it has the implemented class (visual indicator)
    await expect(asyncNetworkingItem).toHaveClass(/implemented/);

    // Verify checkmark icon is present
    const checkIcon = asyncNetworkingItem.locator('.check-icon');
    await expect(checkIcon).toBeVisible();
  });

  test('TC2: Check for memtable feature status', async ({ page }) => {
    // Navigate to project status section
    const statusSection = page.locator('[data-testid="project-status-section"]');
    await expect(statusSection).toBeVisible();

    // Check that Memtable with skip list is marked as implemented
    const memtableItem = page.locator('[data-testid="feature-memtable"]');
    await expect(memtableItem).toBeVisible();
    await expect(memtableItem).toContainText(/Memtable.*skip.*list/i);

    // Verify it has the implemented class (visual indicator)
    await expect(memtableItem).toHaveClass(/implemented/);

    // Verify checkmark icon is present
    const checkIcon = memtableItem.locator('.check-icon');
    await expect(checkIcon).toBeVisible();
  });

  test('TC3: Check for minor compaction feature status', async ({ page }) => {
    // Navigate to project status section
    const statusSection = page.locator('[data-testid="project-status-section"]');
    await expect(statusSection).toBeVisible();

    // Check that Minor compaction is marked as implemented
    const minorCompactionItem = page.locator('[data-testid="feature-minor-compaction"]');
    await expect(minorCompactionItem).toBeVisible();
    await expect(minorCompactionItem).toContainText(/Minor.*compaction/i);

    // Verify it has the implemented class (visual indicator)
    await expect(minorCompactionItem).toHaveClass(/implemented/);

    // Verify checkmark icon is present
    const checkIcon = minorCompactionItem.locator('.check-icon');
    await expect(checkIcon).toBeVisible();
  });

  test('TC4: Check for major compaction feature status', async ({ page }) => {
    // Navigate to project status section
    const statusSection = page.locator('[data-testid="project-status-section"]');
    await expect(statusSection).toBeVisible();

    // Check that Major compaction is marked as implemented
    const majorCompactionItem = page.locator('[data-testid="feature-major-compaction"]');
    await expect(majorCompactionItem).toBeVisible();
    await expect(majorCompactionItem).toContainText(/Major.*compaction/i);

    // Verify it has the implemented class (visual indicator)
    await expect(majorCompactionItem).toHaveClass(/implemented/);

    // Verify checkmark icon is present
    const checkIcon = majorCompactionItem.locator('.check-icon');
    await expect(checkIcon).toBeVisible();
  });

  test('TC5: Check for Raft consensus roadmap item', async ({ page }) => {
    // Navigate to project status section
    const statusSection = page.locator('[data-testid="project-status-section"]');
    await expect(statusSection).toBeVisible();

    // Check for roadmap list
    const roadmapList = page.locator('[data-testid="roadmap-features-list"]');
    await expect(roadmapList).toBeVisible();

    // Check that Raft consensus is listed as planned/upcoming feature
    const raftItem = page.locator('[data-testid="feature-raft-consensus"]');
    await expect(raftItem).toBeVisible();
    await expect(raftItem).toContainText(/Raft.*consensus/i);

    // Verify it has the planned class (not implemented)
    await expect(raftItem).toHaveClass(/planned/);

    // Verify planned icon is present (circle icon, not checkmark)
    const plannedIcon = raftItem.locator('.planned-icon');
    await expect(plannedIcon).toBeVisible();
  });

  test('TC6: Verify visual distinction between implemented and planned', async ({ page }) => {
    // Navigate to project status section
    const statusSection = page.locator('[data-testid="project-status-section"]');
    await expect(statusSection).toBeVisible();

    // Check that implemented and roadmap sections have distinct headings
    const implementedHeading = page.locator('[data-testid="implemented-heading"]');
    const roadmapHeading = page.locator('[data-testid="roadmap-heading"]');
    await expect(implementedHeading).toBeVisible();
    await expect(roadmapHeading).toBeVisible();

    // Verify headings have different classes for visual distinction
    await expect(implementedHeading).toHaveClass(/implemented-heading/);
    await expect(roadmapHeading).toHaveClass(/roadmap-heading/);

    // Get computed styles to verify different colors
    const implementedColor = await implementedHeading.evaluate((el) =>
      window.getComputedStyle(el).color
    );
    const roadmapColor = await roadmapHeading.evaluate((el) =>
      window.getComputedStyle(el).color
    );

    // Verify colors are different (visual distinction)
    expect(implementedColor).not.toBe(roadmapColor);

    // Verify implemented items use checkmark icons
    const implementedItem = page.locator('[data-testid="feature-async-networking"]');
    const checkIcon = implementedItem.locator('.check-icon');
    await expect(checkIcon).toBeVisible();

    // Verify planned items use different icons (circle)
    const plannedItem = page.locator('[data-testid="feature-raft-consensus"]');
    const plannedIcon = plannedItem.locator('.planned-icon');
    await expect(plannedIcon).toBeVisible();

    // Verify they're in different sections/lists
    const implementedList = page.locator('[data-testid="implemented-features-list"]');
    const roadmapList = page.locator('[data-testid="roadmap-features-list"]');

    await expect(implementedList.locator('[data-testid="feature-async-networking"]')).toBeVisible();
    await expect(roadmapList.locator('[data-testid="feature-raft-consensus"]')).toBeVisible();
  });
});
