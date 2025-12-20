// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

test.describe('Memcached Comparison Section', () => {
  test.beforeEach(async ({ page }) => {
    const filePath = path.resolve(__dirname, '../index.html');
    await page.goto(`file://${filePath}`);
  });

  // Test Case 1: Search for comparison-related content on page
  test('TC1: Page contains content comparing MirDB to Memcached or highlighting differences', async ({ page }) => {
    // Look for a dedicated comparison section
    const comparisonSection = page.locator('[data-testid="comparison-section"]');
    await expect(comparisonSection).toBeVisible();

    // Verify the section contains comparison-related content
    const sectionContent = await comparisonSection.textContent();

    // Should mention both MirDB and Memcached in context of comparison
    expect(sectionContent.toLowerCase()).toContain('mirdb');
    expect(sectionContent.toLowerCase()).toContain('memcached');

    // Should have comparison keywords like "vs", "compared", "difference", or "unlike"
    const hasComparisonContent =
      sectionContent.toLowerCase().includes('vs') ||
      sectionContent.toLowerCase().includes('compared') ||
      sectionContent.toLowerCase().includes('difference') ||
      sectionContent.toLowerCase().includes('unlike') ||
      sectionContent.toLowerCase().includes('advantage');
    expect(hasComparisonContent).toBeTruthy();
  });

  // Test Case 2: Verify persistence advantage is mentioned
  test('TC2: Page explains that MirDB persists data while Memcached is memory-only', async ({ page }) => {
    const comparisonSection = page.locator('[data-testid="comparison-section"]');
    await expect(comparisonSection).toBeVisible();

    const sectionContent = await comparisonSection.textContent();
    const lowerContent = sectionContent.toLowerCase();

    // Should explain persistence as a key difference
    const hasPersistenceContent =
      lowerContent.includes('persist') ||
      lowerContent.includes('disk') ||
      lowerContent.includes('durable');
    expect(hasPersistenceContent).toBeTruthy();

    // Should mention Memcached's memory-only limitation
    const hasMemoryOnlyContent =
      lowerContent.includes('memory-only') ||
      lowerContent.includes('memory only') ||
      lowerContent.includes('in-memory') ||
      (lowerContent.includes('memory') && lowerContent.includes('volatile'));
    expect(hasMemoryOnlyContent).toBeTruthy();
  });

  // Test Case 3: Check for compatibility statement
  test('TC3: Page states that existing Memcached clients work with MirDB', async ({ page }) => {
    const comparisonSection = page.locator('[data-testid="comparison-section"]');
    await expect(comparisonSection).toBeVisible();

    const sectionContent = await comparisonSection.textContent();
    const lowerContent = sectionContent.toLowerCase();

    // Should mention client compatibility
    const hasClientCompatibility =
      (lowerContent.includes('client') && lowerContent.includes('work')) ||
      lowerContent.includes('existing client') ||
      lowerContent.includes('memcached client') ||
      lowerContent.includes('drop-in') ||
      lowerContent.includes('protocol compatible') ||
      lowerContent.includes('protocol compatibility');
    expect(hasClientCompatibility).toBeTruthy();
  });

  // Additional test: Comparison section has clear structure with comparison table or list
  test('TC4: Comparison section has structured comparison content', async ({ page }) => {
    const comparisonSection = page.locator('[data-testid="comparison-section"]');
    await expect(comparisonSection).toBeVisible();

    // Should have either a comparison table or comparison items/rows
    const hasComparisonTable = await comparisonSection.locator('table').count() > 0;
    const hasComparisonItems = await comparisonSection.locator('[data-testid^="comparison-item"]').count() > 0;
    const hasComparisonRows = await comparisonSection.locator('.comparison-row').count() > 0;

    // At least one of these structural elements should exist
    expect(hasComparisonTable || hasComparisonItems || hasComparisonRows).toBeTruthy();
  });

  // Additional test: Comparison section has a title/heading
  test('TC5: Comparison section has a descriptive heading', async ({ page }) => {
    const comparisonSection = page.locator('[data-testid="comparison-section"]');
    await expect(comparisonSection).toBeVisible();

    // Should have a heading (h2 or h3)
    const heading = comparisonSection.locator('h2, h3').first();
    await expect(heading).toBeVisible();

    const headingText = await heading.textContent();
    // Heading should relate to comparison or differences
    const hasRelevantHeading =
      headingText.toLowerCase().includes('compare') ||
      headingText.toLowerCase().includes('vs') ||
      headingText.toLowerCase().includes('difference') ||
      headingText.toLowerCase().includes('memcached');
    expect(hasRelevantHeading).toBeTruthy();
  });
});
