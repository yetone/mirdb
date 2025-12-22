// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Architecture Section and LSM Tree Diagram', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('should display architecture diagram element', async ({ page }) => {
    // Navigate to architecture section
    const architectureSection = page.locator('[data-testid="architecture-section"]');
    await architectureSection.scrollIntoViewIfNeeded();

    // Verify the architecture diagram is visible
    const diagram = page.locator('[data-testid="architecture-diagram"]');
    await expect(diagram).toBeVisible();

    // Verify the diagram has role="img" for accessibility
    await expect(diagram).toHaveAttribute('role', 'img');
  });

  test('should display Memtable component in the diagram', async ({ page }) => {
    // Navigate to architecture section
    const architectureSection = page.locator('[data-testid="architecture-section"]');
    await architectureSection.scrollIntoViewIfNeeded();

    // Verify Memtable component is visible
    const memtableComponent = page.locator('[data-testid="memtable-component"]');
    await expect(memtableComponent).toBeVisible();

    // Verify the Memtable label is present
    const memtableLabel = memtableComponent.locator('.node-label');
    await expect(memtableLabel).toContainText('Memtable');
  });

  test('should display Immutable Memtable component in the diagram', async ({ page }) => {
    // Navigate to architecture section
    const architectureSection = page.locator('[data-testid="architecture-section"]');
    await architectureSection.scrollIntoViewIfNeeded();

    // Verify Immutable Memtable component is visible
    const immMemtableComponent = page.locator('[data-testid="immutable-memtable-component"]');
    await expect(immMemtableComponent).toBeVisible();

    // Verify the Immutable Memtable label is present
    const immMemtableLabel = immMemtableComponent.locator('.node-label');
    await expect(immMemtableLabel).toContainText('Immutable Memtable');
  });

  test('should display SSTables component in the diagram', async ({ page }) => {
    // Navigate to architecture section
    const architectureSection = page.locator('[data-testid="architecture-section"]');
    await architectureSection.scrollIntoViewIfNeeded();

    // Verify SSTables component is visible
    const sstablesComponent = page.locator('[data-testid="sstables-component"]');
    await expect(sstablesComponent).toBeVisible();

    // Verify the SSTables label contains "SSTables"
    const sstablesLabel = sstablesComponent.locator('.node-label');
    await expect(sstablesLabel).toContainText('SSTables');
  });

  test('should display Compaction process indicators in the diagram', async ({ page }) => {
    // Navigate to architecture section
    const architectureSection = page.locator('[data-testid="architecture-section"]');
    await architectureSection.scrollIntoViewIfNeeded();

    // Verify Minor Compaction is displayed
    const minorCompaction = page.locator('[data-testid="minor-compaction"]');
    await expect(minorCompaction).toBeVisible();
    await expect(minorCompaction).toContainText('Minor Compaction');

    // Verify Major Compaction is displayed
    const majorCompaction = page.locator('[data-testid="major-compaction"]');
    await expect(majorCompaction).toBeVisible();
    await expect(majorCompaction).toContainText('Major Compaction');
  });

  test('should display data flow explanation text', async ({ page }) => {
    // Navigate to architecture section
    const architectureSection = page.locator('[data-testid="architecture-section"]');
    await architectureSection.scrollIntoViewIfNeeded();

    // Verify data flow explanation is present
    const dataFlowExplanation = page.locator('[data-testid="data-flow-explanation"]');
    await expect(dataFlowExplanation).toBeVisible();

    // Verify the explanation mentions key concepts
    const explanationText = await dataFlowExplanation.textContent();
    expect(explanationText).toContain('LSM');
    expect(explanationText).toContain('memory');
    expect(explanationText).toContain('disk');
  });

  test('should have descriptive alt text for accessibility', async ({ page }) => {
    // Navigate to architecture section
    const architectureSection = page.locator('[data-testid="architecture-section"]');
    await architectureSection.scrollIntoViewIfNeeded();

    // Verify the architecture diagram has aria-label for screen readers
    const diagram = page.locator('[data-testid="architecture-diagram"]');
    const ariaLabel = await diagram.getAttribute('aria-label');

    // Verify aria-label is present and descriptive
    expect(ariaLabel).toBeTruthy();
    expect(ariaLabel.length).toBeGreaterThan(50); // Should be descriptive
    expect(ariaLabel).toContain('LSM Tree');
    expect(ariaLabel).toContain('Memtable');
    expect(ariaLabel).toContain('SSTable');
  });

  test('should display section title "LSM Tree Architecture"', async ({ page }) => {
    // Navigate to architecture section
    const architectureSection = page.locator('[data-testid="architecture-section"]');
    await architectureSection.scrollIntoViewIfNeeded();

    // Verify section title is present
    const sectionTitle = architectureSection.locator('.section-title');
    await expect(sectionTitle).toBeVisible();
    await expect(sectionTitle).toContainText('LSM Tree Architecture');
  });

  test('should display Memory and Disk section labels', async ({ page }) => {
    // Navigate to architecture section
    const architectureSection = page.locator('[data-testid="architecture-section"]');
    await architectureSection.scrollIntoViewIfNeeded();

    // Verify Memory section label
    const memorySection = architectureSection.locator('.memory-section .section-label');
    await expect(memorySection).toBeVisible();
    await expect(memorySection).toContainText('Memory');

    // Verify Disk section label
    const diskSection = architectureSection.locator('.disk-section .section-label');
    await expect(diskSection).toBeVisible();
    await expect(diskSection).toContainText('Disk');
  });

  test('should show Write Request entry point', async ({ page }) => {
    // Navigate to architecture section
    const architectureSection = page.locator('[data-testid="architecture-section"]');
    await architectureSection.scrollIntoViewIfNeeded();

    // Verify Write Request entry point is displayed
    const writeRequest = page.locator('[data-testid="write-request"]');
    await expect(writeRequest).toBeVisible();

    const writeRequestLabel = writeRequest.locator('.node-label');
    await expect(writeRequestLabel).toContainText('Write Request');
  });
});
