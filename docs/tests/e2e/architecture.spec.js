// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E tests for Architecture Diagram Section
 * Owner: Scenario 4
 *
 * Test cases:
 * - Architecture section presence
 * - Diagram element visibility
 * - Accessibility (alt text / ARIA label)
 * - Key components mentioned (Memtable, SSTable, WAL)
 * - Dark mode visibility
 */

test.describe('Architecture Diagram Section', () => {
  test.beforeEach(async ({ page }) => {
    // Use './' or empty string for relative URL resolution with baseURL
    await page.goto('./');
    await page.waitForLoadState('domcontentloaded');
  });

  test('architecture section exists with heading', async ({ page }) => {
    // Test case 1: Check architecture section exists with heading 'Architecture' or 'How It Works'
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    const heading = architectureSection.locator('h2').first();
    await expect(heading).toBeVisible();

    // Heading should be 'Architecture' or 'How It Works'
    const headingText = await heading.textContent();
    expect(headingText).toMatch(/Architecture|How It Works/i);
  });

  test('diagram is present and visible', async ({ page }) => {
    // Test case 2: Verify SVG or image element showing architecture is visible
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Check for SVG diagram with architecture-diagram class or role="img"
    const svgDiagram = architectureSection.locator('svg.architecture-diagram').first();
    const svgCount = await svgDiagram.count();

    if (svgCount > 0) {
      await expect(svgDiagram).toBeVisible();
    } else {
      // Fallback: check for any SVG with role="img" or img element
      const altDiagram = architectureSection.locator('svg[role="img"], img');
      await expect(altDiagram.first()).toBeVisible();
    }
  });

  test('diagram has appropriate accessibility attributes', async ({ page }) => {
    // Test case 3: Check diagram has appropriate alt text or ARIA label describing LSM tree components
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Check for SVG with aria-label or role="img"
    const svgDiagram = architectureSection.locator('svg[role="img"]').first();
    const svgCount = await svgDiagram.count();

    if (svgCount > 0) {
      // SVG should have aria-label describing the architecture
      const ariaLabel = await svgDiagram.getAttribute('aria-label');
      expect(ariaLabel).toBeTruthy();
      expect(ariaLabel.toLowerCase()).toMatch(/lsm|architecture|diagram/i);

      // SVG should have title element for accessibility
      const titleElement = svgDiagram.locator('title').first();
      const titleCount = await titleElement.count();
      expect(titleCount).toBeGreaterThan(0);
    } else {
      // If image, check for alt text
      const imgDiagram = architectureSection.locator('img').first();
      const imgCount = await imgDiagram.count();
      if (imgCount > 0) {
        const altText = await imgDiagram.getAttribute('alt');
        expect(altText).toBeTruthy();
        expect(altText.toLowerCase()).toMatch(/lsm|architecture|memtable|sstable|wal/i);
      }
    }
  });

  test('diagram mentions key components: Memtable, SSTable, WAL', async ({ page }) => {
    // Test case 4: Verify text or labels reference Memtable, SSTable, WAL
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    const sectionText = await architectureSection.textContent();
    const lowerText = sectionText.toLowerCase();

    // Check for Memtable mention
    expect(lowerText).toContain('memtable');

    // Check for SSTable mention
    expect(lowerText).toContain('sstable');

    // Check for WAL or Write-Ahead Log mention
    expect(lowerText.match(/wal|write-ahead log/i)).toBeTruthy();
  });

  test('diagram remains visible in dark mode', async ({ page }) => {
    // Test case 5: Test diagram visibility in dark theme
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Enable dark mode by adding 'dark' class to html element
    await page.evaluate(() => {
      document.documentElement.classList.add('dark');
    });

    // Wait for potential transitions
    await page.waitForTimeout(300);

    // Check SVG diagram visibility in dark mode
    const svgDiagram = architectureSection.locator('svg.architecture-diagram').first();
    const svgCount = await svgDiagram.count();

    if (svgCount > 0) {
      await expect(svgDiagram).toBeVisible();

      // Verify SVG has visible bounding box (not completely hidden)
      const svgBox = await svgDiagram.boundingBox();
      expect(svgBox).toBeTruthy();
      expect(svgBox.width).toBeGreaterThan(0);
      expect(svgBox.height).toBeGreaterThan(0);
    } else {
      // If image, just check visibility
      const imgDiagram = architectureSection.locator('img').first();
      const imgCount = await imgDiagram.count();
      if (imgCount > 0) {
        await expect(imgDiagram).toBeVisible();
      }
    }
  });
});
