// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Project Status and Roadmap Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Section displays both implemented (checked) and planned (unchecked) features', async ({ page }) => {
    // Navigate to the Project Status section
    const statusSection = page.locator('[data-testid="status-section"], #status, section.status');
    await expect(statusSection).toBeVisible();

    // Verify the section has a heading
    const heading = statusSection.locator('h2');
    await expect(heading).toBeVisible();
    await expect(heading).toContainText('Project Status');

    // Check that both implemented and planned features exist
    const implementedItems = statusSection.locator('.status-item.completed, [data-status="implemented"]');
    const plannedItems = statusSection.locator('.status-item.planned, [data-status="planned"]');

    // Verify at least one implemented feature exists
    const implementedCount = await implementedItems.count();
    expect(implementedCount).toBeGreaterThan(0);

    // Verify at least one planned feature exists
    const plannedCount = await plannedItems.count();
    expect(plannedCount).toBeGreaterThan(0);

    // Verify implemented features have checkmark (✓)
    const firstImplemented = implementedItems.first();
    const implementedIcon = firstImplemented.locator('.status-icon, [data-testid="status-icon"]');
    await expect(implementedIcon).toContainText('✓');

    // Verify planned features have circle (○)
    const firstPlanned = plannedItems.first();
    const plannedIcon = firstPlanned.locator('.status-icon, [data-testid="status-icon"]');
    await expect(plannedIcon).toContainText('○');
  });

  test('TC2: Async Networking and Compaction are marked as implemented', async ({ page }) => {
    // Navigate to the Project Status section
    const statusSection = page.locator('[data-testid="status-section"], #status, section.status');
    await expect(statusSection).toBeVisible();

    // Check for Async Networking - should be implemented (completed class with checkmark)
    const asyncNetworking = statusSection.locator('[data-testid="status-item-async-networking"], .status-item:has-text("Async Networking")');
    await expect(asyncNetworking).toBeVisible();
    await expect(asyncNetworking).toHaveClass(/completed/);

    const asyncNetworkingIcon = asyncNetworking.locator('.status-icon, [data-testid="status-icon"]');
    await expect(asyncNetworkingIcon).toContainText('✓');

    // Check for Compaction features - should be implemented
    // Minor Compaction
    const minorCompaction = statusSection.locator('[data-testid="status-item-minor-compaction"], .status-item:has-text("Minor Compaction")');
    await expect(minorCompaction).toBeVisible();
    await expect(minorCompaction).toHaveClass(/completed/);

    const minorCompactionIcon = minorCompaction.locator('.status-icon, [data-testid="status-icon"]');
    await expect(minorCompactionIcon).toContainText('✓');

    // Major Compaction
    const majorCompaction = statusSection.locator('[data-testid="status-item-major-compaction"], .status-item:has-text("Major Compaction")');
    await expect(majorCompaction).toBeVisible();
    await expect(majorCompaction).toHaveClass(/completed/);

    const majorCompactionIcon = majorCompaction.locator('.status-icon, [data-testid="status-icon"]');
    await expect(majorCompactionIcon).toContainText('✓');
  });

  test('TC3: Raft Consensus is marked as planned/in-progress', async ({ page }) => {
    // Navigate to the Project Status section
    const statusSection = page.locator('[data-testid="status-section"], #status, section.status');
    await expect(statusSection).toBeVisible();

    // Check for Raft Consensus - should be planned (planned class with circle)
    const raftConsensus = statusSection.locator('[data-testid="status-item-raft-consensus"], .status-item:has-text("Raft Consensus")');
    await expect(raftConsensus).toBeVisible();
    await expect(raftConsensus).toHaveClass(/planned/);

    const raftConsensusIcon = raftConsensus.locator('.status-icon, [data-testid="status-icon"]');
    await expect(raftConsensusIcon).toContainText('○');

    // Verify it does NOT have the completed class
    await expect(raftConsensus).not.toHaveClass(/completed/);
  });

  test('TC4: Implemented and planned features have clear visual distinction (checkmarks vs circles)', async ({ page }) => {
    // Navigate to the Project Status section
    const statusSection = page.locator('[data-testid="status-section"], #status, section.status');
    await expect(statusSection).toBeVisible();

    // Get all status items
    const implementedItems = statusSection.locator('.status-item.completed, [data-status="implemented"]');
    const plannedItems = statusSection.locator('.status-item.planned, [data-status="planned"]');

    // Verify all implemented items have checkmarks
    const implementedCount = await implementedItems.count();
    for (let i = 0; i < implementedCount; i++) {
      const item = implementedItems.nth(i);
      const icon = item.locator('.status-icon, [data-testid="status-icon"]');
      await expect(icon).toContainText('✓');
    }

    // Verify all planned items have circles
    const plannedCount = await plannedItems.count();
    for (let i = 0; i < plannedCount; i++) {
      const item = plannedItems.nth(i);
      const icon = item.locator('.status-icon, [data-testid="status-icon"]');
      await expect(icon).toContainText('○');
    }

    // Verify visual distinction through CSS classes
    // Implemented items should have 'completed' class
    for (let i = 0; i < implementedCount; i++) {
      const item = implementedItems.nth(i);
      await expect(item).toHaveClass(/completed/);
    }

    // Planned items should have 'planned' class
    for (let i = 0; i < plannedCount; i++) {
      const item = plannedItems.nth(i);
      await expect(item).toHaveClass(/planned/);
    }

    // Verify different color styling for icons (visual distinction)
    // Get computed styles to ensure visual differentiation
    const implementedIconColor = await implementedItems.first().locator('.status-icon').evaluate(el => {
      return window.getComputedStyle(el).color;
    });

    const plannedIconColor = await plannedItems.first().locator('.status-icon').evaluate(el => {
      return window.getComputedStyle(el).color;
    });

    // The colors should be different (visual distinction)
    expect(implementedIconColor).not.toBe(plannedIconColor);
  });
});
