/**
 * Architecture Section E2E Tests
 * Owner: Scenario 8 - Architecture Overview Section
 *
 * End-to-end tests for the MirDB homepage architecture overview section.
 *
 * Expected test coverage:
 * - Architecture section is present with proper heading
 * - LSM Tree approach is explained
 * - Memtables are explained
 * - SSTables are explained
 * - Compaction strategy is mentioned
 * - Optional diagram or visual representation
 *
 * Requirements traced:
 * - REQ-2: Homepage shall showcase key features and capabilities
 * - USR-7: User wants to understand MirDB's architecture and persistence model
 */

const { test, expect } = require('@playwright/test');

test.describe('Architecture Overview Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case 1: Check for architecture section presence
  test('page contains an architecture section with heading or id', async ({ page }) => {
    // Look for a section with id containing 'architecture' or a heading with 'Architecture'
    const architectureSection = page.locator('#architecture, [id*="architecture"], section:has(h2:text-matches("Architecture", "i"))');
    await expect(architectureSection.first()).toBeVisible();

    // Verify there's a heading with "Architecture" in the section
    const heading = page.locator('#architecture h2, [id*="architecture"] h2, h2:text-matches("Architecture", "i")');
    await expect(heading.first()).toBeVisible();
  });

  // Test Case 2: Search for LSM Tree explanation
  test('architecture section contains LSM Tree explanation', async ({ page }) => {
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    const sectionText = await architectureSection.textContent();
    const lowerText = sectionText.toLowerCase();

    // Check for LSM or Log-Structured Merge text
    const hasLSMExplanation =
      lowerText.includes('lsm') ||
      lowerText.includes('log-structured merge') ||
      lowerText.includes('log structured merge');

    expect(hasLSMExplanation).toBeTruthy();
  });

  // Test Case 3: Search for memtable explanation
  test('architecture section contains memtable explanation', async ({ page }) => {
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    const sectionText = await architectureSection.textContent();
    const lowerText = sectionText.toLowerCase();

    // Check for memtable or memory table text
    const hasMemtableExplanation =
      lowerText.includes('memtable') ||
      lowerText.includes('memory table') ||
      lowerText.includes('mem table');

    expect(hasMemtableExplanation).toBeTruthy();
  });

  // Test Case 4: Search for SSTable explanation
  test('architecture section contains SSTable explanation', async ({ page }) => {
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    const sectionText = await architectureSection.textContent();
    const lowerText = sectionText.toLowerCase();

    // Check for SSTable or Sorted String Table text
    const hasSSTableExplanation =
      lowerText.includes('sstable') ||
      lowerText.includes('sorted string table') ||
      lowerText.includes('ss table');

    expect(hasSSTableExplanation).toBeTruthy();
  });

  // Test Case 5: Search for compaction explanation
  test('architecture section contains compaction strategy explanation', async ({ page }) => {
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    const sectionText = await architectureSection.textContent();
    const lowerText = sectionText.toLowerCase();

    // Check for compaction or merge strategy text
    const hasCompactionExplanation =
      lowerText.includes('compaction') ||
      lowerText.includes('merge') ||
      lowerText.includes('merging');

    expect(hasCompactionExplanation).toBeTruthy();
  });

  // Test Case 6: Check for optional diagram or visual (non-blocking)
  test('architecture section may contain a diagram or visual representation', async ({ page }) => {
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Check for diagrams, images, SVGs, or visual elements
    const visualElements = architectureSection.locator('img, svg, .diagram, .flowchart, .visual, figure, .architecture-diagram');
    const count = await visualElements.count();

    // This is an optional check - just verify the section exists even without visuals
    // If visuals exist, that's a bonus
    if (count > 0) {
      await expect(visualElements.first()).toBeVisible();
    }
    // Always pass - visual is optional per test case requirements
    expect(true).toBeTruthy();
  });

  // Additional test: Verify architecture section has adequate content
  test('architecture section has substantial explanatory content', async ({ page }) => {
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    const sectionText = await architectureSection.textContent();

    // Verify the section has meaningful content (at least 100 characters)
    expect(sectionText.length).toBeGreaterThan(100);
  });
});
