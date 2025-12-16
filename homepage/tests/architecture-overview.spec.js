// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Architecture Overview Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Check for architecture diagram element
   * Verify that architecture section contains a visual diagram (SVG, image, or rendered Mermaid)
   */
  test('should display architecture section with visual diagram', async ({ page }) => {
    // Verify architecture section exists
    const architectureSection = page.locator('[data-testid="architecture-section"]');
    await expect(architectureSection).toBeVisible();

    // Scroll to architecture section
    await architectureSection.scrollIntoViewIfNeeded();

    // Verify diagram element exists (Mermaid pre element or rendered SVG)
    const diagramContainer = page.locator('[data-testid="architecture-diagram"]');
    await expect(diagramContainer).toBeVisible();

    // Check for mermaid diagram element (either pre.mermaid or rendered svg)
    const mermaidElement = page.locator('[data-testid="mermaid-diagram"]');
    await expect(mermaidElement).toBeVisible();

    // Wait for Mermaid to render and check for SVG
    // Mermaid replaces the pre element with an SVG
    await page.waitForTimeout(1000); // Give Mermaid time to render

    // Either the pre.mermaid should exist or SVG should be rendered
    const hasMermaidPre = await mermaidElement.isVisible().catch(() => false);
    const svgInDiagram = diagramContainer.locator('svg');
    const hasSvg = await svgInDiagram.count() > 0;

    expect(hasMermaidPre || hasSvg).toBeTruthy();
  });

  /**
   * Test Case 2: Verify diagram shows write path components
   * Diagram includes WAL, Memtable, and SSTable in write flow
   */
  test('should display write path with WAL, Memtable, and SSTable', async ({ page }) => {
    const architectureSection = page.locator('[data-testid="architecture-section"]');
    await architectureSection.scrollIntoViewIfNeeded();

    // Check for write path description
    const writePathDescription = page.locator('[data-testid="write-path-description"]');
    await expect(writePathDescription).toBeVisible();

    // Get the text content
    const writePathText = await writePathDescription.textContent();

    // Verify WAL is mentioned
    expect(writePathText?.toUpperCase()).toContain('WAL');

    // Verify Memtable is mentioned
    expect(writePathText?.toLowerCase()).toContain('memtable');

    // Verify SSTable is mentioned
    expect(writePathText?.toLowerCase()).toContain('sstable');

    // Verify the diagram container itself contains the write path
    const diagramContainer = page.locator('[data-testid="architecture-diagram"]');
    await expect(diagramContainer).toBeVisible();

    const diagramText = await diagramContainer.textContent();

    // Check diagram mentions write path components
    expect(diagramText?.toUpperCase()).toContain('WAL');
    expect(diagramText?.toLowerCase()).toContain('memtable');
    expect(diagramText?.toLowerCase()).toContain('sstable');
  });

  /**
   * Test Case 3: Verify diagram shows read path components
   * Diagram includes Memtable, Immutable List, and SSTable Levels in read flow
   */
  test('should display read path with Memtable, Immutable List, and SSTable Levels', async ({ page }) => {
    const architectureSection = page.locator('[data-testid="architecture-section"]');
    await architectureSection.scrollIntoViewIfNeeded();

    // Check for read path description
    const readPathDescription = page.locator('[data-testid="read-path-description"]');
    await expect(readPathDescription).toBeVisible();

    // Get the text content
    const readPathText = await readPathDescription.textContent();

    // Verify Memtable is mentioned
    expect(readPathText?.toLowerCase()).toContain('memtable');

    // Verify Immutable List/Immutable Memtable is mentioned
    expect(readPathText?.toLowerCase()).toContain('immutable');

    // Verify SSTable Levels are mentioned
    expect(readPathText?.toLowerCase()).toContain('sstable');
    expect(readPathText?.toLowerCase()).toContain('level');

    // Verify the diagram container itself contains the read path
    const diagramContainer = page.locator('[data-testid="architecture-diagram"]');
    await expect(diagramContainer).toBeVisible();

    const diagramText = await diagramContainer.textContent();

    // Check diagram mentions read path components
    expect(diagramText?.toLowerCase()).toContain('memtable');
    expect(diagramText?.toLowerCase()).toContain('immutable');
    expect(diagramText?.toLowerCase()).toContain('level');
  });

  /**
   * Test Case 4: Check for LSM-tree explanation text
   * Brief explanation of LSM-tree benefits accompanies the diagram
   */
  test('should display LSM-tree explanation text', async ({ page }) => {
    const architectureSection = page.locator('[data-testid="architecture-section"]');
    await architectureSection.scrollIntoViewIfNeeded();

    // Check for LSM-tree explanation element
    const lsmExplanation = page.locator('[data-testid="lsm-tree-explanation"]');
    await expect(lsmExplanation).toBeVisible();

    // Get the text content
    const explanationText = await lsmExplanation.textContent();

    // Verify LSM-tree is mentioned
    expect(explanationText?.toLowerCase()).toContain('lsm');

    // Verify some benefits are mentioned (write performance, compaction, etc.)
    const hasBenefitMention =
      explanationText?.toLowerCase().includes('write') ||
      explanationText?.toLowerCase().includes('performance') ||
      explanationText?.toLowerCase().includes('efficient') ||
      explanationText?.toLowerCase().includes('compaction');

    expect(hasBenefitMention).toBeTruthy();
  });

  /**
   * Additional test: Architecture section has proper heading
   */
  test('should display Architecture Overview heading', async ({ page }) => {
    const architectureSection = page.locator('[data-testid="architecture-section"]');
    await expect(architectureSection).toBeVisible();

    const heading = architectureSection.locator('h2');
    await expect(heading).toContainText('Architecture');
  });

  /**
   * Additional test: Both write and read path descriptions are present
   */
  test('should display both write and read path descriptions', async ({ page }) => {
    const architectureSection = page.locator('[data-testid="architecture-section"]');
    await architectureSection.scrollIntoViewIfNeeded();

    const writePathDescription = page.locator('[data-testid="write-path-description"]');
    const readPathDescription = page.locator('[data-testid="read-path-description"]');

    await expect(writePathDescription).toBeVisible();
    await expect(readPathDescription).toBeVisible();

    // Verify they have distinct headings
    const writeHeading = writePathDescription.locator('h3');
    const readHeading = readPathDescription.locator('h3');

    await expect(writeHeading).toContainText('Write');
    await expect(readHeading).toContainText('Read');
  });
});
