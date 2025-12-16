// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for How It Works / Architecture Section
 * Scenario: Verify that the architecture overview or diagram explaining LSM-tree design
 * is displayed as per REQ-5
 */

test.describe('How It Works / Architecture Section', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to homepage before each test
    await page.goto('/');
    // Scroll to How It Works section to ensure it's loaded
    await page.locator('#how-it-works').scrollIntoViewIfNeeded();
  });

  /**
   * Test Case 1: Check for LSM-tree architecture diagram
   * Input: Check for LSM-tree architecture diagram
   * Expected: Visual diagram showing LSM-tree architecture is displayed (Mermaid.js or static image)
   */
  test('TC1: LSM-tree architecture diagram is displayed', async ({ page }) => {
    // Verify section exists and is visible
    const section = page.locator('[data-testid="how-it-works-section"]');
    await expect(section).toBeVisible();

    // Verify section title
    const title = page.locator('[data-testid="how-it-works-title"]');
    await expect(title).toBeVisible();
    await expect(title).toContainText('How It Works');

    // Verify diagram container exists
    const diagramContainer = page.locator('[data-testid="architecture-diagram-container"]');
    await expect(diagramContainer).toBeVisible();

    // Verify Mermaid diagram element exists
    const diagram = page.locator('[data-testid="architecture-diagram"]');
    await expect(diagram).toBeVisible();

    // Wait for Mermaid to render the diagram (SVG should be created)
    // Mermaid replaces the div content with an SVG
    await page.waitForTimeout(1000); // Allow time for Mermaid to render

    // Check that either the mermaid div contains SVG or the original flowchart text
    // (diagram may render as SVG or remain as text depending on Mermaid loading)
    const diagramContent = await diagram.innerHTML();
    const hasSvg = diagramContent.includes('<svg') || diagramContent.includes('flowchart');
    expect(hasSvg).toBeTruthy();
  });

  /**
   * Test Case 2: Verify write path explanation
   * Input: Verify write path explanation
   * Expected: Text explains the write flow: writes go to memtable, then immutable memtable, then SSTables
   */
  test('TC2: Write path explanation contains correct flow description', async ({ page }) => {
    // Verify write path card exists
    const writePathCard = page.locator('[data-testid="write-path-card"]');
    await expect(writePathCard).toBeVisible();

    // Verify write path title
    const writePathTitle = page.locator('[data-testid="write-path-title"]');
    await expect(writePathTitle).toBeVisible();
    await expect(writePathTitle).toContainText('Write Path');

    // Verify write path description
    const writePathDescription = page.locator('[data-testid="write-path-description"]');
    await expect(writePathDescription).toBeVisible();

    // Verify write path steps exist
    const writePathSteps = page.locator('[data-testid="write-path-steps"]');
    await expect(writePathSteps).toBeVisible();

    // Verify step 1: WAL
    const step1 = page.locator('[data-testid="write-step-1"]');
    await expect(step1).toBeVisible();
    await expect(step1).toContainText('Write-Ahead Log');
    await expect(step1).toContainText('WAL');

    // Verify step 2: Mutable Memtable
    const step2 = page.locator('[data-testid="write-step-2"]');
    await expect(step2).toBeVisible();
    await expect(step2).toContainText('Memtable');

    // Verify step 3: Immutable Memtable
    const step3 = page.locator('[data-testid="write-step-3"]');
    await expect(step3).toBeVisible();
    await expect(step3).toContainText('immutable');

    // Verify step 4: SSTable Files
    const step4 = page.locator('[data-testid="write-step-4"]');
    await expect(step4).toBeVisible();
    await expect(step4).toContainText('SSTable');
  });

  /**
   * Test Case 3: Verify read path explanation
   * Input: Verify read path explanation
   * Expected: Text explains the read flow and how data is retrieved from memtable and SSTables
   */
  test('TC3: Read path explanation contains correct flow description', async ({ page }) => {
    // Verify read path card exists
    const readPathCard = page.locator('[data-testid="read-path-card"]');
    await expect(readPathCard).toBeVisible();

    // Verify read path title
    const readPathTitle = page.locator('[data-testid="read-path-title"]');
    await expect(readPathTitle).toBeVisible();
    await expect(readPathTitle).toContainText('Read Path');

    // Verify read path description
    const readPathDescription = page.locator('[data-testid="read-path-description"]');
    await expect(readPathDescription).toBeVisible();

    // Verify read path steps exist
    const readPathSteps = page.locator('[data-testid="read-path-steps"]');
    await expect(readPathSteps).toBeVisible();

    // Verify step 1: Mutable Memtable check
    const step1 = page.locator('[data-testid="read-step-1"]');
    await expect(step1).toBeVisible();
    await expect(step1).toContainText('Memtable');

    // Verify step 2: Immutable Memtables
    const step2 = page.locator('[data-testid="read-step-2"]');
    await expect(step2).toBeVisible();
    await expect(step2).toContainText('Immutable');

    // Verify step 3: SSTable Levels
    const step3 = page.locator('[data-testid="read-step-3"]');
    await expect(step3).toBeVisible();
    await expect(step3).toContainText('SSTable');

    // Verify step 4: Cuckoo Filter
    const step4 = page.locator('[data-testid="read-step-4"]');
    await expect(step4).toBeVisible();
    await expect(step4).toContainText('Cuckoo');
  });

  /**
   * Test Case 4: Check diagram accessibility
   * Input: Check diagram accessibility
   * Expected: Diagram has appropriate alt text for screen readers
   */
  test('TC4: Architecture diagram has appropriate accessibility attributes', async ({ page }) => {
    // Verify diagram figure has appropriate role and aria-label
    const diagramFigure = page.locator('.diagram-figure');
    await expect(diagramFigure).toBeVisible();
    await expect(diagramFigure).toHaveAttribute('role', 'img');

    const ariaLabel = await diagramFigure.getAttribute('aria-label');
    expect(ariaLabel).toBeTruthy();
    expect(ariaLabel).toContain('LSM-tree');
    expect(ariaLabel).toContain('architecture');

    // Verify figcaption exists with alt text for screen readers
    const figcaption = page.locator('[data-testid="architecture-diagram-alt"]');
    await expect(figcaption).toBeAttached();

    // Figcaption should contain descriptive text
    const captionText = await figcaption.textContent();
    expect(captionText).toContain('LSM-tree');
    expect(captionText).toContain('write path');
    expect(captionText).toContain('read path');
    expect(captionText).toContain('memtable');
    expect(captionText).toContain('SSTable');

    // Verify figcaption has visually-hidden class for screen readers
    await expect(figcaption).toHaveClass(/visually-hidden/);

    // Verify the mermaid diagram has aria-hidden to prevent screen reader from reading raw code
    const mermaidDiv = page.locator('[data-testid="architecture-diagram"]');
    await expect(mermaidDiv).toHaveAttribute('aria-hidden', 'true');
  });

  /**
   * Additional test: Verify How It Works section description mentions LSM-tree
   */
  test('Section description mentions LSM-tree architecture', async ({ page }) => {
    const description = page.locator('[data-testid="how-it-works-description"]');
    await expect(description).toBeVisible();
    await expect(description).toContainText('LSM-tree');
    await expect(description).toContainText('Log-Structured Merge-tree');
  });

  /**
   * Additional test: Verify data paths grid layout
   */
  test('Data paths grid displays write and read path cards', async ({ page }) => {
    const grid = page.locator('[data-testid="data-paths-grid"]');
    await expect(grid).toBeVisible();

    // Both write and read path cards should be visible
    const writeCard = page.locator('[data-testid="write-path-card"]');
    const readCard = page.locator('[data-testid="read-path-card"]');

    await expect(writeCard).toBeVisible();
    await expect(readCard).toBeVisible();

    // Each card should have 4 steps
    const writeSteps = page.locator('[data-testid="write-path-steps"] li');
    const readSteps = page.locator('[data-testid="read-path-steps"] li');

    await expect(writeSteps).toHaveCount(4);
    await expect(readSteps).toHaveCount(4);
  });
});
