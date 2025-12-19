import { test, expect } from '@playwright/test';

test.describe('Architecture Overview Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Architecture section exists with proper heading', async ({ page }) => {
    // Test Case 1: Query for architecture section in DOM
    // Expected: Section with 'architecture' heading or id exists
    const architectureSection = page.locator('#architecture, [data-testid="architecture"]');
    await expect(architectureSection).toBeVisible();

    // Check for architecture heading
    const heading = architectureSection.locator('h2');
    await expect(heading).toBeVisible();
    const headingText = await heading.textContent();
    expect(headingText?.toLowerCase()).toContain('architecture');
  });

  test('TC2: Visual diagram element is present', async ({ page }) => {
    // Test Case 2: Query for image/svg/diagram element in architecture section
    // Expected: Visual diagram element (img, svg, or canvas) is present
    const architectureSection = page.locator('#architecture, [data-testid="architecture"]');

    // Look for visual diagram elements - img, svg, canvas, or a diagram container with role="img"
    const diagramElements = architectureSection.locator('img, svg, canvas, [role="img"], .architecture-diagram, .arch-diagram');
    const count = await diagramElements.count();
    expect(count).toBeGreaterThanOrEqual(1);
  });

  test('TC3: Diagram has alt text for accessibility', async ({ page }) => {
    // Test Case 3: Verify diagram has alt text for accessibility
    // Expected: Diagram image has descriptive alt attribute
    const architectureSection = page.locator('#architecture, [data-testid="architecture"]');

    // Find the diagram element
    const diagramElement = architectureSection.locator('img[alt], [role="img"][aria-label], .architecture-diagram[aria-label]');
    await expect(diagramElement.first()).toBeVisible();

    // Check for accessible description
    const hasAlt = await diagramElement.first().getAttribute('alt') || await diagramElement.first().getAttribute('aria-label');
    expect(hasAlt).toBeTruthy();
    expect(hasAlt!.length).toBeGreaterThan(10); // Should be descriptive
  });

  test('TC4: WAL (Write-Ahead Log) is mentioned', async ({ page }) => {
    // Test Case 4: Search for WAL/Write-Ahead Log text
    // Expected: WAL or Write-Ahead Log is mentioned in architecture section
    const architectureSection = page.locator('#architecture, [data-testid="architecture"]');
    const sectionText = await architectureSection.textContent();
    const lowerText = sectionText?.toLowerCase() || '';

    // Check for WAL or Write-Ahead Log mention
    const hasWAL = lowerText.includes('wal') || lowerText.includes('write-ahead log') || lowerText.includes('write ahead log');
    expect(hasWAL).toBeTruthy();
  });

  test('TC5: Memtable concept is explained', async ({ page }) => {
    // Test Case 5: Search for memtable text
    // Expected: Memtable concept is explained
    const architectureSection = page.locator('#architecture, [data-testid="architecture"]');
    const sectionText = await architectureSection.textContent();
    const lowerText = sectionText?.toLowerCase() || '';

    // Check for memtable mention
    expect(lowerText).toContain('memtable');
  });

  test('TC6: SSTable storage is explained', async ({ page }) => {
    // Test Case 6: Search for SSTable text
    // Expected: SSTable storage is explained
    const architectureSection = page.locator('#architecture, [data-testid="architecture"]');
    const sectionText = await architectureSection.textContent();
    const lowerText = sectionText?.toLowerCase() || '';

    // Check for SSTable mention
    expect(lowerText).toContain('sstable');
  });

  test('TC7: Background compaction is mentioned as LSM benefit', async ({ page }) => {
    // Test Case 7: Search for compaction text
    // Expected: Background compaction is mentioned as LSM benefit
    const architectureSection = page.locator('#architecture, [data-testid="architecture"]');
    const sectionText = await architectureSection.textContent();
    const lowerText = sectionText?.toLowerCase() || '';

    // Check for compaction mention
    expect(lowerText).toContain('compaction');
  });
});
