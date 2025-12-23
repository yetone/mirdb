// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for Architecture Diagram Display
 * Scenario: Verify architecture diagram illustrating LSM tree data flow is displayed as specified in REQ-8
 * Related Requirements: REQ-8, US-3
 */

test.describe('Architecture Diagram Display', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to the homepage before each test
    await page.goto('/');
  });

  /**
   * Test Case 1: Check for architecture section presence
   * Input: Check for architecture section presence
   * Expected: Architecture overview section is present on the page
   */
  test('TC1: Architecture overview section is present on the page', async ({ page }) => {
    // Verify the architecture section exists
    const architectureSection = page.locator('[data-testid="architecture-section"]');
    await expect(architectureSection).toBeVisible();

    // Verify the section has a heading
    const heading = architectureSection.locator('h2');
    await expect(heading).toBeVisible();
    await expect(heading).toHaveText('Architecture Overview');

    // Verify the section has a description
    const description = architectureSection.locator('.section-description');
    await expect(description).toBeVisible();
    const descText = await description.textContent();
    expect(descText.toLowerCase()).toContain('lsm');
    expect(descText.toLowerCase()).toContain('architecture');

    // Verify section is accessible via navigation
    const navLink = page.locator('nav a[href="#architecture"]');
    await expect(navLink).toBeVisible();
    await expect(navLink).toHaveText('Architecture');
  });

  /**
   * Test Case 2: Check for LSM tree diagram
   * Input: Check for LSM tree diagram
   * Expected: Visual diagram of LSM tree data flow is displayed
   */
  test('TC2: Visual diagram of LSM tree data flow is displayed', async ({ page }) => {
    // Verify the diagram container exists
    const diagramContainer = page.locator('[data-testid="lsm-tree-diagram"]');
    await expect(diagramContainer).toBeVisible();

    // Verify the SVG diagram is present
    const svgDiagram = diagramContainer.locator('svg.lsm-diagram');
    await expect(svgDiagram).toBeVisible();

    // Verify the diagram has the proper SVG role attribute for accessibility
    await expect(svgDiagram).toHaveAttribute('role', 'img');

    // Verify key LSM tree components are present in the diagram
    const diagramText = await diagramContainer.textContent();

    // Check for memtable representation
    expect(diagramText.toLowerCase()).toContain('memtable');

    // Check for SSTable/Level representation
    expect(diagramText.toLowerCase()).toContain('sstable');

    // Check for WAL representation
    expect(diagramText.toLowerCase()).toContain('wal');

    // Check for compaction representation
    expect(diagramText.toLowerCase()).toContain('compaction');

    // Verify the diagram shows multiple levels
    expect(diagramText).toContain('Level 0');
    expect(diagramText).toContain('Level 1');
  });

  /**
   * Test Case 3: Verify diagram accessibility
   * Input: Verify diagram accessibility
   * Expected: Diagram has alt text or accompanying text description for accessibility
   */
  test('TC3: Diagram has alt text or accompanying text description for accessibility', async ({ page }) => {
    const diagramContainer = page.locator('[data-testid="lsm-tree-diagram"]');
    await expect(diagramContainer).toBeVisible();

    // Verify SVG has proper accessibility attributes
    const svgDiagram = diagramContainer.locator('svg.lsm-diagram');

    // Check for aria-labelledby attribute pointing to title and description
    const ariaLabelledBy = await svgDiagram.getAttribute('aria-labelledby');
    expect(ariaLabelledBy).toBeTruthy();
    expect(ariaLabelledBy).toContain('diagram-title');
    expect(ariaLabelledBy).toContain('diagram-desc');

    // Verify the SVG title element exists and has meaningful content
    // Note: SVG title and desc elements are hidden by default, so we check for their existence
    // and content rather than visibility
    const svgTitle = svgDiagram.locator('title#diagram-title');
    await expect(svgTitle).toHaveCount(1);
    const titleText = await svgTitle.textContent();
    expect(titleText.toLowerCase()).toContain('lsm');
    expect(titleText.toLowerCase()).toContain('diagram');

    // Verify the SVG desc element exists and has meaningful content
    const svgDesc = svgDiagram.locator('desc#diagram-desc');
    await expect(svgDesc).toHaveCount(1);
    const descText = await svgDesc.textContent();
    expect(descText.toLowerCase()).toContain('data flow');
    expect(descText.toLowerCase()).toContain('write');
    expect(descText.toLowerCase()).toContain('read');

    // Verify figure has a caption for additional context (this IS visible)
    const figcaption = page.locator('[data-testid="diagram-caption"]');
    await expect(figcaption).toBeVisible();
    const captionText = await figcaption.textContent();
    expect(captionText.toLowerCase()).toContain('lsm');
    expect(captionText.length).toBeGreaterThan(50); // Meaningful description
  });

  /**
   * Test Case 4: Check for write/read path explanation
   * Input: Check for write/read path explanation
   * Expected: Brief explanation of write and read paths is provided
   */
  test('TC4: Brief explanation of write and read paths is provided', async ({ page }) => {
    // Verify the architecture explanation section exists
    const explanationSection = page.locator('[data-testid="architecture-explanation"]');
    await expect(explanationSection).toBeVisible();

    // Verify Write Path explanation exists
    const writePathExplanation = page.locator('[data-testid="write-path-explanation"]');
    await expect(writePathExplanation).toBeVisible();

    // Check for Write Path heading
    const writePathHeading = writePathExplanation.locator('h3');
    await expect(writePathHeading).toHaveText('Write Path');

    // Check for write path content
    const writePathContent = await writePathExplanation.textContent();
    expect(writePathContent.toLowerCase()).toContain('wal');
    expect(writePathContent.toLowerCase()).toContain('memtable');
    expect(writePathContent.toLowerCase()).toContain('sstable');

    // Verify Read Path explanation exists
    const readPathExplanation = page.locator('[data-testid="read-path-explanation"]');
    await expect(readPathExplanation).toBeVisible();

    // Check for Read Path heading
    const readPathHeading = readPathExplanation.locator('h3');
    await expect(readPathHeading).toHaveText('Read Path');

    // Check for read path content
    const readPathContent = await readPathExplanation.textContent();
    expect(readPathContent.toLowerCase()).toContain('memtable');
    expect(readPathContent.toLowerCase()).toContain('search');
    expect(readPathContent.toLowerCase()).toContain('level');

    // Verify both explanations contain ordered lists
    const writePathList = writePathExplanation.locator('ol');
    await expect(writePathList).toBeVisible();
    const writePathItems = writePathExplanation.locator('ol li');
    expect(await writePathItems.count()).toBeGreaterThanOrEqual(3);

    const readPathList = readPathExplanation.locator('ol');
    await expect(readPathList).toBeVisible();
    const readPathItems = readPathExplanation.locator('ol li');
    expect(await readPathItems.count()).toBeGreaterThanOrEqual(3);
  });

  /**
   * Additional Test: Verify diagram visual elements are rendered
   */
  test('TC5: Diagram visual elements (boxes, arrows) are rendered', async ({ page }) => {
    const svgDiagram = page.locator('[data-testid="lsm-tree-diagram"] svg.lsm-diagram');
    await expect(svgDiagram).toBeVisible();

    // Verify diagram boxes are present
    const diagramBoxes = svgDiagram.locator('.diagram-box');
    expect(await diagramBoxes.count()).toBeGreaterThanOrEqual(8); // Multiple component boxes

    // Verify diagram arrows are present
    const diagramArrows = svgDiagram.locator('.diagram-arrow');
    expect(await diagramArrows.count()).toBeGreaterThanOrEqual(5); // Flow arrows

    // Verify both Write Path and Read Path labels are visible
    const diagramLabels = svgDiagram.locator('.diagram-label');
    expect(await diagramLabels.count()).toBeGreaterThanOrEqual(2);

    const labelTexts = await diagramLabels.allTextContents();
    expect(labelTexts.some(t => t.includes('Write'))).toBeTruthy();
    expect(labelTexts.some(t => t.includes('Read'))).toBeTruthy();
  });

  /**
   * Additional Test: Architecture section is scrollable to via navigation
   */
  test('TC6: Architecture section is accessible via navigation link', async ({ page }) => {
    // Click the architecture navigation link
    const navLink = page.locator('nav a[href="#architecture"]');
    await expect(navLink).toBeVisible();
    await navLink.click();

    // Wait for smooth scroll to complete
    await page.waitForTimeout(500);

    // Verify the architecture section is now in view
    const architectureSection = page.locator('[data-testid="architecture-section"]');
    await expect(architectureSection).toBeInViewport();
  });
});
