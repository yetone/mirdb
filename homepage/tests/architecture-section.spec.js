// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Architecture Diagram Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Architecture section exists with heading', async ({ page }) => {
    // Test Case 1: Query for architecture section on the page
    // Expected: Architecture section exists with heading

    // Look for architecture section by id, data-testid, or class
    const architectureSection = page.locator('#architecture, [data-testid="architecture"], .architecture, section:has(h2:text-matches("architecture|how it works", "i"))');
    await expect(architectureSection).toBeVisible();

    // Verify there is a heading for the architecture section
    const heading = architectureSection.locator('h2');
    await expect(heading).toBeVisible();

    const headingText = await heading.textContent();
    const lowerText = headingText?.toLowerCase() || '';
    expect(lowerText).toMatch(/architecture|how it works/i);
  });

  test('TC2: Visual diagram element exists in architecture section', async ({ page }) => {
    // Test Case 2: Query for image or SVG element in architecture section
    // Expected: Visual diagram element exists

    const architectureSection = page.locator('#architecture, [data-testid="architecture"], .architecture, section:has(h2:text-matches("architecture|how it works", "i"))');
    await expect(architectureSection).toBeVisible();

    // Look for visual diagram element (image or SVG)
    const visualDiagram = architectureSection.locator('img, svg, [data-testid="architecture-diagram"]');
    await expect(visualDiagram.first()).toBeVisible();
  });

  test('TC3: Key LSM tree concepts are mentioned or labeled', async ({ page }) => {
    // Test Case 3: Query architecture section for LSM tree terms (WAL, Memtable, SSTable)
    // Expected: Key LSM tree concepts are mentioned or labeled

    const architectureSection = page.locator('#architecture, [data-testid="architecture"], .architecture, section:has(h2:text-matches("architecture|how it works", "i"))');
    await expect(architectureSection).toBeVisible();

    // Get all text content in the architecture section
    const sectionText = await architectureSection.textContent();
    const lowerText = sectionText?.toLowerCase() || '';

    // Check for key LSM tree concepts
    const hasWAL = lowerText.includes('wal') || lowerText.includes('write-ahead log') || lowerText.includes('write ahead log');
    const hasMemtable = lowerText.includes('memtable') || lowerText.includes('mem table');
    const hasSSTable = lowerText.includes('sstable') || lowerText.includes('ss table') || lowerText.includes('sorted string table');

    // At least 2 of the 3 key concepts should be present
    const conceptsPresent = [hasWAL, hasMemtable, hasSSTable].filter(Boolean).length;
    expect(conceptsPresent).toBeGreaterThanOrEqual(2);
  });

  test('TC4: Diagram has descriptive alt text for accessibility', async ({ page }) => {
    // Test Case 4: Verify diagram has alt text for accessibility
    // Expected: Image/diagram has descriptive alt attribute

    const architectureSection = page.locator('#architecture, [data-testid="architecture"], .architecture, section:has(h2:text-matches("architecture|how it works", "i"))');
    await expect(architectureSection).toBeVisible();

    // Check for img with alt attribute
    const imgWithAlt = architectureSection.locator('img[alt]');
    const svgWithRole = architectureSection.locator('svg[role="img"][aria-label], svg[aria-labelledby]');

    // Either we have an img with alt or an SVG with proper accessibility attributes
    const imgCount = await imgWithAlt.count();
    const svgCount = await svgWithRole.count();

    expect(imgCount + svgCount).toBeGreaterThanOrEqual(1);

    if (imgCount > 0) {
      const altText = await imgWithAlt.first().getAttribute('alt');
      // Alt text should be descriptive (at least 10 characters)
      expect(altText?.length).toBeGreaterThanOrEqual(10);
    }

    if (svgCount > 0) {
      const ariaLabel = await svgWithRole.first().getAttribute('aria-label');
      const ariaLabelledBy = await svgWithRole.first().getAttribute('aria-labelledby');
      // Either aria-label or aria-labelledby should be present
      expect(ariaLabel || ariaLabelledBy).toBeTruthy();
    }
  });
});
