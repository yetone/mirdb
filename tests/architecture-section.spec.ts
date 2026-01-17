import { test, expect } from '@playwright/test';

/**
 * E2E Tests for Architecture Diagram Section
 *
 * This test suite verifies that the architecture section displays a visual diagram
 * showing the data flow (WAL -> Memtable -> SSTable levels) and includes
 * a brief explanation of LSM-tree design with proper accessibility attributes.
 *
 * Requirement: REQ-10 - Architecture diagram showing data flow
 */

test.describe('Architecture Diagram Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 1: Check for architecture diagram presence', async ({ page }) => {
    // Navigate to architecture section
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Verify section heading
    const sectionHeading = architectureSection.locator('h2');
    await expect(sectionHeading).toContainText('Architecture');

    // Verify architecture diagram container is present
    const architectureDiagram = architectureSection.locator('.architecture-diagram');
    await expect(architectureDiagram).toBeVisible();

    // Verify diagram flow container is present
    const archFlow = architectureDiagram.locator('.arch-flow');
    await expect(archFlow).toBeVisible();

    // Verify visual components are displayed
    const components = archFlow.locator('.arch-component');
    await expect(components).toHaveCount(3);
  });

  test('Test Case 2: Verify diagram shows data flow (WAL -> Memtable -> SSTable)', async ({ page }) => {
    // Navigate to architecture section
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    const architectureDiagram = architectureSection.locator('.architecture-diagram');
    const archFlow = architectureDiagram.locator('.arch-flow');

    // Verify WAL component
    const walComponent = archFlow.locator('[data-component="wal"]');
    await expect(walComponent).toBeVisible();
    const walBox = walComponent.locator('.component-box');
    await expect(walBox).toContainText('WAL');
    const walLabel = walComponent.locator('.component-label');
    await expect(walLabel).toContainText('Write-Ahead Log');

    // Verify Memtable component
    const memtableComponent = archFlow.locator('[data-component="memtable"]');
    await expect(memtableComponent).toBeVisible();
    const memtableBox = memtableComponent.locator('.component-box');
    await expect(memtableBox).toContainText('Memtable');
    const memtableLabel = memtableComponent.locator('.component-label');
    await expect(memtableLabel).toContainText('Skip List');

    // Verify SSTable component
    const sstableComponent = archFlow.locator('[data-component="sstable"]');
    await expect(sstableComponent).toBeVisible();
    const sstableBox = sstableComponent.locator('.component-box');
    await expect(sstableBox).toContainText('SSTables');
    const sstableLabel = sstableComponent.locator('.component-label');
    await expect(sstableLabel).toContainText('Level');

    // Verify arrows are present showing the flow direction
    const arrows = archFlow.locator('.arch-arrow');
    await expect(arrows).toHaveCount(2);
  });

  test('Test Case 3: Check for LSM-tree explanation', async ({ page }) => {
    // Navigate to architecture section
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Verify section description mentions LSM-tree
    const sectionDescription = architectureSection.locator('.section-description');
    await expect(sectionDescription).toBeVisible();
    await expect(sectionDescription).toContainText('LSM-tree');
    await expect(sectionDescription).toContainText('architecture');
    await expect(sectionDescription).toContainText('persistent storage');
  });

  test('Test Case 4: Verify diagram has alt text for accessibility', async ({ page }) => {
    // Navigate to architecture section
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Verify architecture diagram has proper ARIA attributes for accessibility
    const architectureDiagram = architectureSection.locator('.architecture-diagram');
    await expect(architectureDiagram).toBeVisible();

    // Check for role="img" attribute (semantic meaning for screen readers)
    await expect(architectureDiagram).toHaveAttribute('role', 'img');

    // Check for aria-label with meaningful description
    const ariaLabel = await architectureDiagram.getAttribute('aria-label');
    expect(ariaLabel).toBeTruthy();
    expect(ariaLabel).toContain('LSM-tree');
    expect(ariaLabel).toContain('architecture');
    expect(ariaLabel).toContain('WAL');
    expect(ariaLabel).toContain('Memtable');
    expect(ariaLabel).toContain('SSTable');

    // Verify decorative arrows have aria-hidden for screen readers
    const arrows = architectureSection.locator('.arch-arrow');
    const arrowCount = await arrows.count();
    for (let i = 0; i < arrowCount; i++) {
      const arrow = arrows.nth(i);
      await expect(arrow).toHaveAttribute('aria-hidden', 'true');
    }
  });
});
