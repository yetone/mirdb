import { test, expect } from '@playwright/test';

test.describe('Architecture Section with Diagram', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Architecture section displays visual diagram of LSM Tree architecture', async ({ page }) => {
    // Navigate to architecture section
    const architectureSection = page.locator('[data-testid="architecture-section"]');
    await expect(architectureSection).toBeVisible();

    // Verify the diagram container exists
    const diagramContainer = page.locator('[data-testid="architecture-diagram-container"]');
    await expect(diagramContainer).toBeVisible();

    // Verify the LSM Tree diagram SVG is present
    const lsmTreeDiagram = page.locator('[data-testid="lsm-tree-diagram"]');
    await expect(lsmTreeDiagram).toBeVisible();

    // Verify it's an SVG element
    const tagName = await lsmTreeDiagram.evaluate((el) => el.tagName.toLowerCase());
    expect(tagName).toBe('svg');
  });

  test('TC2: Diagram shows memtable, SSTable, and compaction components', async ({ page }) => {
    const architectureSection = page.locator('[data-testid="architecture-section"]');
    await expect(architectureSection).toBeVisible();

    // Verify Memtable component in diagram
    const memtable = page.locator('[data-testid="diagram-memtable"]');
    await expect(memtable).toBeVisible();

    // Verify SSTable components at different levels
    const sstableL0 = page.locator('[data-testid="diagram-sstable-l0"]');
    await expect(sstableL0).toBeVisible();

    const sstableL1 = page.locator('[data-testid="diagram-sstable-l1"]');
    await expect(sstableL1).toBeVisible();

    const sstableL2 = page.locator('[data-testid="diagram-sstable-l2"]');
    await expect(sstableL2).toBeVisible();

    // Verify Write-Ahead Log component
    const wal = page.locator('[data-testid="diagram-wal"]');
    await expect(wal).toBeVisible();

    // Verify Write Operations component
    const writeOps = page.locator('[data-testid="diagram-write-ops"]');
    await expect(writeOps).toBeVisible();

    // Verify Read Operations component
    const readOps = page.locator('[data-testid="diagram-read-ops"]');
    await expect(readOps).toBeVisible();

    // Verify explanation contains references to compaction
    const compactionExplanation = page.locator('[data-testid="compaction-explanation"]');
    await expect(compactionExplanation).toBeVisible();
    await expect(compactionExplanation).toContainText('Compaction');
  });

  test('TC3: Diagram has descriptive alt text for accessibility', async ({ page }) => {
    const lsmTreeDiagram = page.locator('[data-testid="lsm-tree-diagram"]');
    await expect(lsmTreeDiagram).toBeVisible();

    // Check for role="img" attribute for accessibility
    await expect(lsmTreeDiagram).toHaveAttribute('role', 'img');

    // Check for aria-label with descriptive text about the architecture
    const ariaLabel = await lsmTreeDiagram.getAttribute('aria-label');
    expect(ariaLabel).toBeTruthy();
    expect(ariaLabel).toContain('LSM Tree');
    expect(ariaLabel).toContain('Memtable');
    expect(ariaLabel).toContain('SSTable');
    expect(ariaLabel).toContain('Compaction');

    // Check for <title> element within SVG (not visually visible but present in DOM)
    const svgTitle = lsmTreeDiagram.locator('title');
    await expect(svgTitle).toBeAttached();
    const titleText = await svgTitle.textContent();
    expect(titleText).toContain('LSM Tree');

    // Check for <desc> element within SVG for extended description (not visually visible but present in DOM)
    const svgDesc = lsmTreeDiagram.locator('desc');
    await expect(svgDesc).toBeAttached();
    const descText = await svgDesc.textContent();
    expect(descText).toBeTruthy();
    expect(descText!.length).toBeGreaterThan(50); // Ensure it's a meaningful description
  });

  test('TC4: Brief explanation of data flow and performance characteristics is present', async ({ page }) => {
    const architectureSection = page.locator('[data-testid="architecture-section"]');
    await expect(architectureSection).toBeVisible();

    // Verify the explanation section exists
    const explanation = page.locator('[data-testid="architecture-explanation"]');
    await expect(explanation).toBeVisible();

    // Verify architecture description text is present
    const description = page.locator('[data-testid="architecture-description"]');
    await expect(description).toBeVisible();
    await expect(description).toContainText('LSM Tree');

    // Verify data flow explanation is present
    const dataFlowExplanation = page.locator('[data-testid="data-flow-explanation"]');
    await expect(dataFlowExplanation).toBeVisible();

    // Verify memtable explanation
    const memtableExplanation = page.locator('[data-testid="memtable-explanation"]');
    await expect(memtableExplanation).toBeVisible();
    await expect(memtableExplanation).toContainText('Memtable');

    // Verify SSTable explanation
    const sstableExplanation = page.locator('[data-testid="sstable-explanation"]');
    await expect(sstableExplanation).toBeVisible();
    await expect(sstableExplanation).toContainText('SSTable');

    // Verify compaction explanation
    const compactionExplanation = page.locator('[data-testid="compaction-explanation"]');
    await expect(compactionExplanation).toBeVisible();
    await expect(compactionExplanation).toContainText('Compaction');

    // Verify performance characteristics section
    const performanceSection = page.locator('[data-testid="performance-characteristics"]');
    await expect(performanceSection).toBeVisible();

    // Verify performance text mentions key characteristics
    const performanceText = await performanceSection.textContent();
    expect(performanceText).toContain('Write');
    expect(performanceText).toContain('Read');
  });
});
