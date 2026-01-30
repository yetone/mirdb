/**
 * Architecture Section Tests
 * Owner: Scenario 4 - Architecture Section
 *
 * Tests:
 * - Architecture section presence with correct ID
 * - Architecture diagram (SVG image) presence
 * - Memtable explanation with skip list mention
 * - SSTable documentation
 * - WAL description
 * - Compaction information (minor/major)
 */

const { test, expect } = require('@playwright/test');
const { SELECTORS, waitForPageLoad } = require('./test-utils');

test.describe('Architecture Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await waitForPageLoad(page);
  });

  test('architecture section exists with correct ID', async ({ page }) => {
    // Test Case 1: Check architecture section element
    const architectureSection = page.locator(SELECTORS.architecture);
    await expect(architectureSection).toBeVisible();
    await expect(architectureSection).toHaveAttribute('id', 'architecture');
  });

  test('architecture section contains diagram or illustration', async ({ page }) => {
    // Test Case 2: Check for diagram or illustration
    const architectureSection = page.locator(SELECTORS.architecture);

    // Check for SVG image in the architecture section
    const diagramImg = architectureSection.locator('.architecture__diagram-img');
    const svgElement = architectureSection.locator('svg');
    const imgElement = architectureSection.locator('img[src*="architecture"]');

    // At least one of these should be present
    const hasDiagramImg = await diagramImg.count() > 0;
    const hasSvg = await svgElement.count() > 0;
    const hasImg = await imgElement.count() > 0;

    expect(hasDiagramImg || hasSvg || hasImg).toBeTruthy();

    // Verify the diagram image is visible if it exists
    if (hasDiagramImg) {
      await expect(diagramImg).toBeVisible();
    }
  });

  test('architecture section mentions Memtable and skip list', async ({ page }) => {
    // Test Case 3: Search for 'Memtable' text
    const architectureSection = page.locator(SELECTORS.architecture);
    const content = await architectureSection.textContent();

    // Check for memtable mention (case insensitive)
    const hasMemtable = content.toLowerCase().includes('memtable');
    expect(hasMemtable).toBeTruthy();

    // Check for skip list mention
    const hasSkipList = content.toLowerCase().includes('skip list') || content.toLowerCase().includes('skiplist');
    expect(hasSkipList).toBeTruthy();

    // Verify the memtable component card exists
    const memtableComponent = architectureSection.locator('[data-component="memtable"]');
    await expect(memtableComponent).toBeVisible();
  });

  test('architecture section mentions SSTable or sorted string tables', async ({ page }) => {
    // Test Case 4: Search for 'SSTable' text
    const architectureSection = page.locator(SELECTORS.architecture);
    const content = await architectureSection.textContent();

    // Check for SSTable mention (case insensitive)
    const hasSSTable = content.toLowerCase().includes('sstable') ||
                       content.toLowerCase().includes('sorted string table');
    expect(hasSSTable).toBeTruthy();

    // Verify the SSTable component card exists
    const sstableComponent = architectureSection.locator('[data-component="sstable"]');
    await expect(sstableComponent).toBeVisible();
  });

  test('architecture section mentions WAL or Write-Ahead Log', async ({ page }) => {
    // Test Case 5: Search for 'WAL' or 'Write-Ahead Log' text
    const architectureSection = page.locator(SELECTORS.architecture);
    const content = await architectureSection.textContent();

    // Check for WAL mention
    const hasWAL = content.toLowerCase().includes('wal') ||
                   content.toLowerCase().includes('write-ahead log') ||
                   content.toLowerCase().includes('write ahead log');
    expect(hasWAL).toBeTruthy();

    // Verify the WAL component card exists
    const walComponent = architectureSection.locator('[data-component="wal"]');
    await expect(walComponent).toBeVisible();
  });

  test('architecture section mentions Compaction (minor/major)', async ({ page }) => {
    // Test Case 6: Search for 'Compaction' text
    const architectureSection = page.locator(SELECTORS.architecture);
    const content = await architectureSection.textContent();

    // Check for compaction mention
    const hasCompaction = content.toLowerCase().includes('compaction');
    expect(hasCompaction).toBeTruthy();

    // Check for minor and major compaction mentions
    const hasMinorCompaction = content.toLowerCase().includes('minor compaction') ||
                               content.toLowerCase().includes('minor');
    const hasMajorCompaction = content.toLowerCase().includes('major compaction') ||
                               content.toLowerCase().includes('major');

    expect(hasMinorCompaction).toBeTruthy();
    expect(hasMajorCompaction).toBeTruthy();

    // Verify the compaction component card exists
    const compactionComponent = architectureSection.locator('[data-component="compaction"]');
    await expect(compactionComponent).toBeVisible();
  });

  test('architecture diagram image loads correctly', async ({ page }) => {
    // Additional test: Verify the architecture diagram image loads
    const architectureSection = page.locator(SELECTORS.architecture);
    const diagramImg = architectureSection.locator('.architecture__diagram-img');

    // Check if image exists
    const imgCount = await diagramImg.count();
    if (imgCount > 0) {
      await expect(diagramImg).toBeVisible();

      // Verify the image has loaded (naturalWidth > 0 for loaded images)
      const isLoaded = await diagramImg.evaluate((img) => {
        return img.complete && img.naturalWidth > 0;
      });
      expect(isLoaded).toBeTruthy();
    }
  });

  test('architecture section has proper heading structure', async ({ page }) => {
    // Additional test: Verify heading structure
    const architectureSection = page.locator(SELECTORS.architecture);

    // Check for main section heading (h2)
    const mainHeading = architectureSection.locator('h2.architecture__title');
    await expect(mainHeading).toBeVisible();
    await expect(mainHeading).toContainText('Architecture');

    // Check for component headings (h3)
    const componentHeadings = architectureSection.locator('.architecture__component-title');
    const headingCount = await componentHeadings.count();
    expect(headingCount).toBeGreaterThanOrEqual(4); // Memtable, SSTable, WAL, Compaction
  });

  test('architecture section navigable from header', async ({ page }) => {
    // Additional test: Verify navigation to architecture section works
    const architectureLink = page.locator('a[href="#architecture"]');

    if (await architectureLink.count() > 0) {
      await architectureLink.first().click();

      // Wait for scroll animation
      await page.waitForTimeout(500);

      // Check if architecture section is in viewport
      const architectureSection = page.locator(SELECTORS.architecture);
      await expect(architectureSection).toBeInViewport();
    }
  });
});
