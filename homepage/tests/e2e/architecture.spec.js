/**
 * Architecture Overview Section E2E Tests
 * Owner: Scenario 8 - Architecture Overview Section
 *
 * End-to-end tests for the MirDB homepage architecture overview section.
 *
 * Expected test coverage:
 * - Architecture section is present with heading
 * - LSM Tree explanation is present
 * - Memtable explanation is present
 * - SSTable explanation is present
 * - Compaction strategy is mentioned
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
  test('page contains a section with Architecture heading or id', async ({ page }) => {
    // Look for section with id="architecture" or heading containing "Architecture"
    const architectureSection = page.locator('#architecture, section:has(h2:text-is("Architecture")), section:has(h2:text("Architecture Overview"))');
    await expect(architectureSection.first()).toBeVisible();

    // Verify it has a heading
    const heading = architectureSection.first().locator('h2');
    await expect(heading).toBeVisible();
    const headingText = await heading.textContent();
    expect(headingText.toLowerCase()).toContain('architecture');
  });

  // Test Case 2: Search for LSM Tree explanation
  test('architecture section contains LSM or Log-Structured Merge text', async ({ page }) => {
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    const sectionText = await architectureSection.textContent();
    const lowerText = sectionText.toLowerCase();

    // Check for LSM-related keywords
    const hasLSM = lowerText.includes('lsm') || lowerText.includes('log-structured merge');
    expect(hasLSM).toBeTruthy();
  });

  // Test Case 3: Search for memtable explanation
  test('architecture section contains memtable or memory table text', async ({ page }) => {
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    const sectionText = await architectureSection.textContent();
    const lowerText = sectionText.toLowerCase();

    // Check for memtable-related keywords
    const hasMemtable = lowerText.includes('memtable') || lowerText.includes('memory table');
    expect(hasMemtable).toBeTruthy();
  });

  // Test Case 4: Search for SSTable explanation
  test('architecture section contains SSTable or Sorted String Table text', async ({ page }) => {
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    const sectionText = await architectureSection.textContent();
    const lowerText = sectionText.toLowerCase();

    // Check for SSTable-related keywords
    const hasSSTable = lowerText.includes('sstable') || lowerText.includes('sorted string table');
    expect(hasSSTable).toBeTruthy();
  });

  // Test Case 5: Search for compaction explanation
  test('architecture section contains compaction or merge strategy text', async ({ page }) => {
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    const sectionText = await architectureSection.textContent();
    const lowerText = sectionText.toLowerCase();

    // Check for compaction-related keywords
    const hasCompaction = lowerText.includes('compaction') || lowerText.includes('merge');
    expect(hasCompaction).toBeTruthy();
  });

  // Test Case 6: Check for optional diagram or visual (manual test, but we can check for visual elements)
  test('architecture section may contain diagram, flowchart, or visual representation', async ({ page }) => {
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Check for visual elements - this is optional per the test case
    // We check for images, SVGs, or elements with diagram/visual class names
    const visualElements = architectureSection.locator('img, svg, .diagram, .flowchart, .visual, .architecture-diagram, .architecture-visual');
    const hasVisuals = await visualElements.count() > 0;

    // This is optional per the test case, so we just log whether visuals exist
    // The test passes regardless as it's a "may contain" requirement
    if (hasVisuals) {
      const firstVisual = visualElements.first();
      await expect(firstVisual).toBeVisible();
    }
    // Test passes even without visuals as it's optional
    expect(true).toBeTruthy();
  });

  // Additional test: Architecture section is properly positioned in page flow
  test('architecture section appears after features section', async ({ page }) => {
    const featuresSection = page.locator('#features');
    const architectureSection = page.locator('#architecture');

    await expect(featuresSection).toBeVisible();
    await expect(architectureSection).toBeVisible();

    // Get bounding boxes to verify order
    const featuresBbox = await featuresSection.boundingBox();
    const architectureBbox = await architectureSection.boundingBox();

    // Architecture should be below features (higher y value)
    expect(architectureBbox.y).toBeGreaterThan(featuresBbox.y);
  });

  // Additional test: Architecture section has proper semantic structure
  test('architecture section has proper semantic HTML structure', async ({ page }) => {
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Should have a heading
    const heading = architectureSection.locator('h2');
    await expect(heading).toBeVisible();

    // Should have descriptive content (paragraphs or text content)
    const content = architectureSection.locator('p, .architecture-content, .architecture-description');
    const contentCount = await content.count();
    expect(contentCount).toBeGreaterThanOrEqual(1);
  });
});
