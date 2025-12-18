// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = path.resolve(__dirname, '../index.html');

/**
 * Architecture Section Tests
 * Verify the architecture section explains LSM tree and data flow
 */
test.describe('Architecture Overview Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(`file://${indexPath}`);
  });

  /**
   * Test Case 1: LSM tree explanation
   * Input: Check architecture section for LSM tree explanation
   * Expected: Section explains Log-Structured Merge-tree architecture
   */
  test('TC1: Section explains Log-Structured Merge-tree architecture', async ({ page }) => {
    // Navigate to architecture section
    const archSection = page.locator('#architecture');
    await expect(archSection).toBeVisible();

    // Verify LSM tree is explained
    const archSectionText = await archSection.textContent();
    const textLower = archSectionText.toLowerCase();

    // Check for LSM or Log-Structured Merge-tree mentions
    const hasLSM = textLower.includes('lsm') ||
                   textLower.includes('log-structured merge') ||
                   textLower.includes('log structured merge');
    expect(hasLSM).toBeTruthy();

    // Also check that it's described as a tree architecture
    const hasTree = textLower.includes('tree');
    expect(hasTree).toBeTruthy();
  });

  /**
   * Test Case 2: Memtable explanation
   * Input: Check for memtable explanation
   * Expected: Section mentions memtable as in-memory write buffer
   */
  test('TC2: Section mentions memtable as in-memory write buffer', async ({ page }) => {
    // Navigate to architecture section
    const archSection = page.locator('#architecture');
    await expect(archSection).toBeVisible();

    // Verify memtable is mentioned
    const archSectionText = await archSection.textContent();
    const textLower = archSectionText.toLowerCase();

    // Check for memtable mention
    expect(textLower).toContain('memtable');

    // Check that it's described as in-memory or memory-related
    const hasMemory = textLower.includes('memory') ||
                      textLower.includes('in-memory') ||
                      textLower.includes('ram');
    expect(hasMemory).toBeTruthy();
  });

  /**
   * Test Case 3: SSTable explanation
   * Input: Check for SSTable explanation
   * Expected: Section explains SSTables as on-disk sorted data files
   */
  test('TC3: Section explains SSTables as on-disk sorted data files', async ({ page }) => {
    // Navigate to architecture section
    const archSection = page.locator('#architecture');
    await expect(archSection).toBeVisible();

    // Verify SSTable is mentioned
    const archSectionText = await archSection.textContent();
    const textLower = archSectionText.toLowerCase();

    // Check for SSTable mention
    const hasSSTable = textLower.includes('sstable') ||
                       textLower.includes('sst') ||
                       textLower.includes('sorted string table');
    expect(hasSSTable).toBeTruthy();

    // Check that disk storage is mentioned
    const hasDisk = textLower.includes('disk') ||
                    textLower.includes('storage') ||
                    textLower.includes('file');
    expect(hasDisk).toBeTruthy();
  });

  /**
   * Test Case 4: Compaction explanation
   * Input: Check for compaction explanation
   * Expected: Section explains minor and/or major compaction process
   */
  test('TC4: Section explains minor and/or major compaction process', async ({ page }) => {
    // Navigate to architecture section
    const archSection = page.locator('#architecture');
    await expect(archSection).toBeVisible();

    // Verify compaction is explained
    const archSectionText = await archSection.textContent();
    const textLower = archSectionText.toLowerCase();

    // Check for compaction mention
    expect(textLower).toContain('compaction');

    // Check for minor and/or major compaction types
    const hasMinorOrMajor = textLower.includes('minor') ||
                            textLower.includes('major') ||
                            textLower.includes('level');
    expect(hasMinorOrMajor).toBeTruthy();
  });

  /**
   * Test Case 5: Architecture diagram
   * Input: Check for architecture diagram
   * Expected: Visual diagram or illustration of data flow present
   */
  test('TC5: Visual diagram or illustration of data flow present', async ({ page }) => {
    // Navigate to architecture section
    const archSection = page.locator('#architecture');
    await expect(archSection).toBeVisible();

    // Check for diagram element - could be SVG, img, pre (ASCII art), or specific diagram container
    const diagram = archSection.locator('[data-testid="architecture-diagram"], .architecture-diagram, svg, .data-flow-diagram, pre.diagram');
    const diagramCount = await diagram.count();

    // Alternatively check for ASCII art representation in a pre or code block
    const preBlock = archSection.locator('pre');
    const preCount = await preBlock.count();

    // Check for img element with architecture-related alt text
    const imgDiagram = archSection.locator('img');
    const imgCount = await imgDiagram.count();

    // At least one visual representation should exist
    const hasVisualDiagram = diagramCount > 0 || preCount > 0 || imgCount > 0;
    expect(hasVisualDiagram).toBeTruthy();

    // If there's a pre block, verify it contains data flow indicators
    if (preCount > 0) {
      const preText = await preBlock.first().textContent();
      const hasFlowIndicators = preText.includes('→') ||
                                preText.includes('▼') ||
                                preText.includes('│') ||
                                preText.includes('->') ||
                                preText.includes('|');
      expect(hasFlowIndicators).toBeTruthy();
    }
  });

  /**
   * Additional test: Architecture section has proper accessibility
   * Verify proper ARIA attributes and semantic structure
   */
  test('Architecture section has proper accessibility', async ({ page }) => {
    const archSection = page.locator('#architecture');
    await expect(archSection).toBeVisible();

    // Verify section has proper role
    await expect(archSection).toHaveAttribute('role', 'region');

    // Verify section has aria-labelledby pointing to heading
    await expect(archSection).toHaveAttribute('aria-labelledby', 'architecture-heading');

    // Verify heading exists
    const heading = page.locator('#architecture-heading');
    await expect(heading).toBeVisible();
  });

  /**
   * Additional test: Navigation links to architecture section
   * Verify that navigation contains link to architecture section
   */
  test('Navigation contains link to architecture section', async ({ page }) => {
    const navLinks = page.locator('.nav-links');
    await expect(navLinks).toBeVisible();

    // Check for architecture link in navigation
    const archLink = navLinks.locator('a[href="#architecture"]');
    await expect(archLink).toBeVisible();
  });

  /**
   * Additional test: Architecture section explains data flow
   * Verify data flow from writes to disk is explained
   */
  test('Architecture section explains data flow', async ({ page }) => {
    const archSection = page.locator('#architecture');
    await expect(archSection).toBeVisible();

    const archSectionText = await archSection.textContent();
    const textLower = archSectionText.toLowerCase();

    // Check for write/read path explanation
    const hasWritePath = textLower.includes('write') || textLower.includes('wal');
    const hasLevels = textLower.includes('level');

    expect(hasWritePath).toBeTruthy();
    expect(hasLevels).toBeTruthy();
  });
});
