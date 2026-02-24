/**
 * Architecture Section Integration Tests
 * Owner: Scenario 5 - Architecture Overview Section
 *
 * Test cases:
 * - LSM tree diagram is rendered
 * - WAL component is labeled
 * - Memtable component is labeled
 * - SSTable component is labeled
 * - Data flow direction is indicated
 */

import { test, expect } from '@playwright/test';

test.describe('Architecture Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('architecture section is present on the page', async ({ page }) => {
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();
    await expect(architectureSection).toHaveAttribute('aria-labelledby', 'architecture-title');
  });

  test('architecture section has correct title', async ({ page }) => {
    const title = page.locator('#architecture-title');
    await expect(title).toBeVisible();
    await expect(title).toContainText('Architecture');
  });

  test('LSM tree diagram or visualization is rendered', async ({ page }) => {
    // Test case 1: LSM tree diagram or visualization is rendered
    // The diagram can be an SVG, image, or CSS-based visualization
    const architectureSection = page.locator('#architecture');

    // Check for the LSM diagram container
    const diagramContainer = architectureSection.locator('.architecture-diagram, .lsm-diagram, [data-diagram="lsm"]');
    await expect(diagramContainer).toBeVisible();

    // Check that the diagram contains visual elements (SVG or image)
    const hasVisualElement = await architectureSection.locator('svg, img, .lsm-component').count() > 0;
    expect(hasVisualElement).toBeTruthy();
  });

  test('WAL (Write-Ahead Log) component is labeled and described', async ({ page }) => {
    // Test case 2: WAL component is labeled and described
    const architectureSection = page.locator('#architecture');

    // Check for WAL in the diagram (first occurrence)
    const walDiagramComponent = architectureSection.locator('.lsm-component[data-component="wal"]').first();
    await expect(walDiagramComponent).toBeVisible();

    // Check for WAL in the component explanations
    const walExplanation = architectureSection.locator('article.component-explanation[data-component="wal"]');
    await expect(walExplanation).toBeVisible();

    // Verify the description mentions write-ahead log
    const walText = architectureSection.locator('text=/Write-Ahead Log/i').first();
    await expect(walText).toBeVisible();
  });

  test('Memtable component is labeled and described', async ({ page }) => {
    // Test case 3: Memtable component is labeled and described
    const architectureSection = page.locator('#architecture');

    // Check for Memtable in the diagram (first occurrence in write flow)
    const memtableDiagramComponent = architectureSection.locator('.write-flow .lsm-component[data-component="memtable"]');
    await expect(memtableDiagramComponent).toBeVisible();

    // Check for Memtable in the component explanations
    const memtableExplanation = architectureSection.locator('article.component-explanation[data-component="memtable"]');
    await expect(memtableExplanation).toBeVisible();

    // Verify the Memtable text is present in explanations
    const memtableTitle = architectureSection.locator('.component-title:has-text("Memtable")');
    await expect(memtableTitle).toBeVisible();
  });

  test('SSTable component is labeled and described', async ({ page }) => {
    // Test case 4: SSTable component is labeled and described
    const architectureSection = page.locator('#architecture');

    // Check for SSTable in the diagram (first occurrence in write flow)
    const sstableDiagramComponent = architectureSection.locator('.write-flow .lsm-component[data-component="sstable"]');
    await expect(sstableDiagramComponent).toBeVisible();

    // Check for SSTable in the component explanations
    const sstableExplanation = architectureSection.locator('article.component-explanation[data-component="sstable"]');
    await expect(sstableExplanation).toBeVisible();

    // Verify the SSTable text is present (Sorted String Table)
    const sstableTitle = architectureSection.locator('.component-title:has-text("Sorted String Table")');
    await expect(sstableTitle).toBeVisible();
  });

  test('Data flow direction (write path and read path) is indicated', async ({ page }) => {
    // Test case 5: Data flow direction is indicated
    const architectureSection = page.locator('#architecture');

    // Check for write path section
    const writeFlow = architectureSection.locator('.write-flow, .architecture-flow:has-text("Write Path")');
    await expect(writeFlow.first()).toBeVisible();

    // Check for read path section
    const readFlow = architectureSection.locator('.read-flow, .architecture-flow:has-text("Read Path")');
    await expect(readFlow.first()).toBeVisible();

    // Check for flow arrows
    const flowArrows = architectureSection.locator('.flow-arrow');
    const arrowCount = await flowArrows.count();
    expect(arrowCount).toBeGreaterThan(0);

    // Check for write path title
    const writePathTitle = architectureSection.locator('.flow-title:has-text("Write Path")');
    await expect(writePathTitle).toBeVisible();

    // Check for read path title
    const readPathTitle = architectureSection.locator('.flow-title:has-text("Read Path")');
    await expect(readPathTitle).toBeVisible();
  });

  test('architecture section has subtitle explaining LSM tree', async ({ page }) => {
    const architectureSection = page.locator('#architecture');
    const subtitle = architectureSection.locator('.section-subtitle, .architecture-subtitle');
    await expect(subtitle).toBeVisible();
    await expect(subtitle).toContainText(/LSM|Log-Structured/i);
  });

  test('architecture diagram is accessible', async ({ page }) => {
    const architectureSection = page.locator('#architecture');

    // Check for proper ARIA labels on the diagram
    const diagram = architectureSection.locator('.architecture-diagram[role="img"]');
    await expect(diagram).toBeVisible();

    // Verify aria-label is present
    const ariaLabel = await diagram.getAttribute('aria-label');
    expect(ariaLabel).toBeTruthy();
    expect(ariaLabel).toContain('LSM');
  });

  test('all three LSM components are visible in the diagram', async ({ page }) => {
    const diagramSection = page.locator('#architecture .write-flow');

    // All three components should be visible in the write flow diagram
    const walComponent = diagramSection.locator('.lsm-component[data-component="wal"]');
    await expect(walComponent).toBeVisible();

    const memtableComponent = diagramSection.locator('.lsm-component[data-component="memtable"]');
    await expect(memtableComponent).toBeVisible();

    const sstableComponent = diagramSection.locator('.lsm-component[data-component="sstable"]');
    await expect(sstableComponent).toBeVisible();
  });

  test('all three component explanations are present', async ({ page }) => {
    const architectureSection = page.locator('#architecture .architecture-components');

    // All three explanations should be present
    const walExplanation = architectureSection.locator('article[data-component="wal"]');
    await expect(walExplanation).toBeVisible();

    const memtableExplanation = architectureSection.locator('article[data-component="memtable"]');
    await expect(memtableExplanation).toBeVisible();

    const sstableExplanation = architectureSection.locator('article[data-component="sstable"]');
    await expect(sstableExplanation).toBeVisible();
  });
});
