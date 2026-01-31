/**
 * Architecture Section E2E Tests
 * Owner: Scenario 3 - Architecture Overview Section
 *
 * Tests:
 * - LSM tree diagram presence
 * - Write path explanation
 * - Read path explanation
 * - Compaction description
 *
 * Requirements: REQ-3
 */

const { test, expect } = require('@playwright/test');

test.describe('Architecture Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 1: Architecture diagram is present and visible', async ({ page }) => {
    // Navigate to architecture section
    await page.click('a[href="#architecture"]');

    // Check that the architecture section exists
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Check for the architecture diagram container
    const diagramContainer = page.locator('.architecture-diagram');
    await expect(diagramContainer).toBeVisible();

    // Check for Mermaid diagram (rendered as SVG after Mermaid.js processes it)
    // The diagram should contain SVG elements after Mermaid renders
    const mermaidElement = page.locator('.architecture-diagram .mermaid');
    await expect(mermaidElement).toBeVisible();

    // Wait for Mermaid to render and check for SVG
    await page.waitForSelector('.architecture-diagram svg', { timeout: 5000 });
    const svgDiagram = page.locator('.architecture-diagram svg');
    await expect(svgDiagram).toBeVisible();

    // Verify the diagram contains LSM tree related content
    const diagramText = await page.locator('.architecture-diagram').textContent();
    expect(diagramText).toContain('Write');
    expect(diagramText).toContain('Read');
  });

  test('Test Case 2: Write path explanation is present with WAL, Memtable, Immutable Memtable, and SSTables', async ({ page }) => {
    // Navigate to architecture section
    await page.click('a[href="#architecture"]');

    // Check for write path section
    const writePathSection = page.locator('.write-path');
    await expect(writePathSection).toBeVisible();

    // Check for Write Path heading
    const writePathHeading = page.locator('.write-path h3');
    await expect(writePathHeading).toContainText('Write Path');

    // Verify WAL is mentioned
    const pageContent = await page.locator('.write-path').textContent();
    expect(pageContent).toContain('WAL');
    expect(pageContent.toLowerCase()).toContain('write-ahead log');

    // Verify Memtable is mentioned
    expect(pageContent).toContain('Memtable');

    // Verify Immutable Memtable is mentioned
    expect(pageContent.toLowerCase()).toContain('immutable');

    // Verify SSTables are mentioned
    expect(pageContent).toContain('SSTable');
  });

  test('Test Case 3: Read path explanation is present with lookup order through memtable and SSTable levels', async ({ page }) => {
    // Navigate to architecture section
    await page.click('a[href="#architecture"]');

    // Check for read path section
    const readPathSection = page.locator('.read-path');
    await expect(readPathSection).toBeVisible();

    // Check for Read Path heading
    const readPathHeading = page.locator('.read-path h3');
    await expect(readPathHeading).toContainText('Read Path');

    // Verify the lookup order is explained
    const pageContent = await page.locator('.read-path').textContent();

    // Check that Memtable is mentioned as first lookup
    expect(pageContent).toContain('Memtable');

    // Check that Immutable Memtables are mentioned
    expect(pageContent.toLowerCase()).toContain('immutable');

    // Check that SSTable levels are mentioned
    expect(pageContent).toContain('SSTable');
    expect(pageContent.toLowerCase()).toContain('level');

    // Verify the lookup order is sequential
    const memtableIndex = pageContent.indexOf('Memtable');
    const sstableIndex = pageContent.indexOf('SSTable');
    expect(memtableIndex).toBeLessThan(sstableIndex);
  });

  test('Test Case 4: Compaction explanation is present with minor and major compaction processes', async ({ page }) => {
    // Navigate to architecture section
    await page.click('a[href="#architecture"]');

    // Check for compaction section
    const compactionSection = page.locator('.compaction');
    await expect(compactionSection).toBeVisible();

    // Check for Compaction heading
    const compactionHeading = page.locator('.compaction h3');
    await expect(compactionHeading).toContainText('Compaction');

    // Check for minor compaction explanation
    const minorCompaction = page.locator('.compaction-type:has-text("Minor")');
    await expect(minorCompaction).toBeVisible();
    const minorContent = await minorCompaction.textContent();
    expect(minorContent.toLowerCase()).toContain('minor');

    // Check for major compaction explanation
    const majorCompaction = page.locator('.compaction-type:has-text("Major")');
    await expect(majorCompaction).toBeVisible();
    const majorContent = await majorCompaction.textContent();
    expect(majorContent.toLowerCase()).toContain('major');

    // Verify compaction section explains the process
    const compactionContent = await page.locator('.compaction').textContent();
    expect(compactionContent.toLowerCase()).toContain('compaction');
  });
});
