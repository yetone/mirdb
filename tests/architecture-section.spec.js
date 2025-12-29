// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Architecture Overview Section E2E Tests
 * Tests REQ-5 from PRD: Display architecture overview with visual diagram explaining LSM tree data flow
 */

test.describe('Architecture Overview Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('http://localhost:8080');
  });

  // Test Case 1: Architecture section exists with heading containing 'Architecture' or 'How it Works'
  test('TC1: Architecture section exists with appropriate heading', async ({ page }) => {
    // Find the architecture section by ID (most specific)
    const architectureSection = page.locator('section#architecture');
    await expect(architectureSection).toBeVisible();

    // Find heading with 'Architecture' or 'How it Works'
    const heading = architectureSection.locator('h2, h3').first();
    await expect(heading).toBeVisible();
    const headingText = await heading.textContent();
    const hasArchitectureText = /architecture|how it works/i.test(headingText || '');
    expect(hasArchitectureText).toBeTruthy();
  });

  // Test Case 2: SVG or image element exists showing LSM tree structure
  test('TC2: Diagram element exists showing LSM tree structure', async ({ page }) => {
    const architectureSection = page.locator('section#architecture');
    await expect(architectureSection).toBeVisible();

    // Check for SVG or img element with architecture-related content
    const diagram = architectureSection.locator('svg.architecture-diagram, img.architecture-diagram, svg[aria-label*="LSM"], img[alt*="LSM"]').first();
    await expect(diagram).toBeVisible();
  });

  // Test Case 3: Diagram has descriptive alt text for accessibility
  test('TC3: Diagram has descriptive alt text for accessibility', async ({ page }) => {
    const architectureSection = page.locator('section#architecture');
    await expect(architectureSection).toBeVisible();

    // Check for SVG with aria-label or img with alt text
    const svgDiagram = architectureSection.locator('svg[aria-label]');
    const imgDiagram = architectureSection.locator('img[alt]');

    // At least one should have descriptive text
    const svgCount = await svgDiagram.count();
    const imgCount = await imgDiagram.count();

    expect(svgCount + imgCount).toBeGreaterThan(0);

    if (svgCount > 0) {
      const ariaLabel = await svgDiagram.first().getAttribute('aria-label');
      expect(ariaLabel).toBeTruthy();
      expect(ariaLabel?.length).toBeGreaterThan(10);
    }

    if (imgCount > 0 && svgCount === 0) {
      const altText = await imgDiagram.first().getAttribute('alt');
      expect(altText).toBeTruthy();
      expect(altText?.length).toBeGreaterThan(10);
    }
  });

  // Test Case 4: Text content mentions 'WAL' or 'Write-Ahead Log' for durability
  test('TC4: Text mentions WAL or Write-Ahead Log', async ({ page }) => {
    const architectureSection = page.locator('section#architecture');
    await expect(architectureSection).toBeVisible();

    const sectionText = await architectureSection.textContent();
    const hasWAL = /WAL|Write[- ]?Ahead[- ]?Log/i.test(sectionText || '');
    expect(hasWAL).toBeTruthy();
  });

  // Test Case 5: Text content mentions 'compaction' process
  test('TC5: Text mentions compaction process', async ({ page }) => {
    const architectureSection = page.locator('section#architecture');
    await expect(architectureSection).toBeVisible();

    const sectionText = await architectureSection.textContent();
    const hasCompaction = /compaction/i.test(sectionText || '');
    expect(hasCompaction).toBeTruthy();
  });
});
