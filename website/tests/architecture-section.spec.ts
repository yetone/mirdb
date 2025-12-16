import { test, expect } from '@playwright/test';

test.describe('Architecture Overview Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Architecture Overview section exists on the page', async ({ page }) => {
    // Query for architecture section
    // Expected: Architecture Overview section exists on the page
    const architectureSection = page.locator('.architecture-section');
    await expect(architectureSection).toBeVisible();

    const sectionTitle = architectureSection.locator('.section-title');
    await expect(sectionTitle).toBeVisible();
    await expect(sectionTitle).toContainText('Architecture Overview');
  });

  test('TC2: A visual representation of the LSM-tree architecture is displayed', async ({ page }) => {
    // Check for visual diagram element (image, SVG, or diagram)
    // Expected: A visual representation of the LSM-tree architecture is displayed
    const architectureSection = page.locator('.architecture-section');
    await expect(architectureSection).toBeVisible();

    // Check for the diagram container
    const diagramContainer = architectureSection.locator('.architecture-diagram');
    await expect(diagramContainer).toBeVisible();

    // Check for SVG diagram
    const svgDiagram = architectureSection.locator('svg.lsm-diagram');
    await expect(svgDiagram).toBeVisible();

    // Verify the diagram contains LSM-tree components
    const diagramTitle = svgDiagram.locator('title');
    await expect(diagramTitle).toContainText('LSM-Tree');

    // Verify key architecture components are visible in the SVG
    const memtableText = svgDiagram.locator('text:has-text("Memtable")');
    await expect(memtableText.first()).toBeVisible();

    const sstableText = svgDiagram.locator('text:has-text("SSTable")');
    await expect(sstableText.first()).toBeVisible();

    const walText = svgDiagram.locator('text:has-text("WAL")');
    await expect(walText).toBeVisible();
  });

  test('TC3: Explanation of write and read data paths is provided', async ({ page }) => {
    // Check for architecture explanation text
    // Expected: Explanation of write and read data paths is provided
    const architectureSection = page.locator('.architecture-section');
    await expect(architectureSection).toBeVisible();

    const explanationSection = architectureSection.locator('.architecture-explanation');
    await expect(explanationSection).toBeVisible();

    // Check for Write Path explanation
    const writePathCard = architectureSection.locator('.explanation-card:has(.explanation-title:has-text("Write Path"))');
    await expect(writePathCard).toBeVisible();

    const writeSteps = writePathCard.locator('.explanation-steps li');
    const writeStepCount = await writeSteps.count();
    expect(writeStepCount).toBeGreaterThanOrEqual(3);

    // Verify write path mentions key concepts
    const writePathText = await writePathCard.textContent();
    expect(writePathText).toContain('WAL');
    expect(writePathText).toContain('Memtable');

    // Check for Read Path explanation
    const readPathCard = architectureSection.locator('.explanation-card:has(.explanation-title:has-text("Read Path"))');
    await expect(readPathCard).toBeVisible();

    const readSteps = readPathCard.locator('.explanation-steps li');
    const readStepCount = await readSteps.count();
    expect(readStepCount).toBeGreaterThanOrEqual(3);

    // Verify read path mentions key concepts
    const readPathText = await readPathCard.textContent();
    expect(readPathText).toContain('Memtable');
    expect(readPathText).toContain('SSTable');
  });

  test('TC4: Image/diagram has appropriate alt text for accessibility', async ({ page }) => {
    // Verify diagram has alt text or accessibility description
    // Expected: Image/diagram has appropriate alt text for accessibility
    const architectureSection = page.locator('.architecture-section');
    await expect(architectureSection).toBeVisible();

    // Check that the diagram container has role="img" and aria-label
    const diagramContainer = architectureSection.locator('.architecture-diagram');
    await expect(diagramContainer).toHaveAttribute('role', 'img');

    const ariaLabel = await diagramContainer.getAttribute('aria-label');
    expect(ariaLabel).toBeTruthy();
    expect(ariaLabel?.toLowerCase()).toContain('lsm-tree');
    expect(ariaLabel?.toLowerCase()).toContain('architecture');

    // Check for SVG title element (for screen readers)
    // Note: SVG title elements are typically hidden visually but accessible to screen readers
    const svgDiagram = architectureSection.locator('svg.lsm-diagram');
    const svgTitle = svgDiagram.locator('title');
    await expect(svgTitle).toHaveCount(1);
    const titleText = await svgTitle.textContent();
    expect(titleText).toBeTruthy();
    expect(titleText?.toLowerCase()).toContain('lsm-tree');

    // Check for SVG desc element (detailed description)
    const svgDesc = svgDiagram.locator('desc');
    await expect(svgDesc).toHaveCount(1);
    const descText = await svgDesc.textContent();
    expect(descText).toBeTruthy();
    expect(descText?.length).toBeGreaterThan(20);

    // Verify aria-describedby is set
    const ariaDescribedBy = await svgDiagram.getAttribute('aria-describedby');
    expect(ariaDescribedBy).toBeTruthy();
  });
});
