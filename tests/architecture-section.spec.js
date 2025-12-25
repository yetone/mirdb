// @ts-check
const { test, expect } = require('@playwright/test');

// Only run on chromium since it's the only browser installed
test.use({ browserName: 'chromium' });

/**
 * Test Suite: Architecture Diagram Section
 *
 * This test suite validates that the architecture section of the MirDB homepage
 * displays a visual diagram showing the LSM tree data flow correctly.
 *
 * Requirements tested:
 * - Visual diagram showing LSM tree data flow is present
 * - Diagram includes WAL, Memtable, Immutable Memtables, and SSTables
 * - Architecture diagram has descriptive alt text for accessibility
 * - Brief explanation of each component is provided alongside diagram
 */

test.describe('Architecture Diagram Section', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to the homepage
    await page.goto('file://' + process.cwd() + '/index.html');

    // Wait for the architecture section to be visible
    await page.waitForSelector('#architecture');
  });

  test('Test Case 1: Visual diagram showing LSM tree data flow is present', async ({ page }) => {
    // Navigate to architecture section
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Verify the section has proper heading
    const sectionHeading = page.locator('#architecture-title');
    await expect(sectionHeading).toBeVisible();
    await expect(sectionHeading).toContainText('Architecture');

    // Find the architecture diagram container
    const diagramContainer = page.locator('[data-testid="architecture-diagram"]');
    await expect(diagramContainer).toBeVisible();

    // Verify the diagram is displaying visual content (SVG or img)
    const visualElement = diagramContainer.locator('svg, img');
    await expect(visualElement).toBeVisible();
  });

  test('Test Case 2: Diagram includes WAL, Memtable, Immutable Memtables, and SSTables', async ({ page }) => {
    // Navigate to architecture section
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Find the architecture diagram
    const diagramContainer = page.locator('[data-testid="architecture-diagram"]');
    await expect(diagramContainer).toBeVisible();

    // Verify components are present by checking SVG text content
    const svgContent = await diagramContainer.locator('svg').innerHTML();

    // Verify WAL component is present
    expect(svgContent).toContain('>WAL<');

    // Verify Memtable component is present
    expect(svgContent).toContain('>Memtable<');

    // Verify Immutable Memtables component is present
    expect(svgContent).toMatch(/Immutable/);

    // Verify SSTables component is present
    expect(svgContent).toMatch(/SSTable/);
  });

  test('Test Case 3: Architecture diagram has descriptive alt text for accessibility', async ({ page }) => {
    // Navigate to architecture section
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Find the architecture diagram
    const diagramContainer = page.locator('[data-testid="architecture-diagram"]');
    await expect(diagramContainer).toBeVisible();

    // Check for accessible description - could be via aria-label, aria-describedby, title, or role
    const svgElement = diagramContainer.locator('svg');
    const imgElement = diagramContainer.locator('img');

    // Check if it's an SVG with title or aria-label
    if (await svgElement.count() > 0) {
      const ariaLabel = await svgElement.getAttribute('aria-label');
      const titleElement = await svgElement.locator('title').textContent();
      const roleAttr = await svgElement.getAttribute('role');

      // SVG should have either aria-label or a title element
      const hasAccessibleDescription =
        (ariaLabel && ariaLabel.length > 10) ||
        (titleElement && titleElement.length > 10);

      expect(hasAccessibleDescription).toBeTruthy();

      // Check that the role is set for accessibility
      if (roleAttr) {
        expect(roleAttr).toBe('img');
      }
    }

    // Check if it's an img with alt text
    if (await imgElement.count() > 0) {
      const altText = await imgElement.getAttribute('alt');
      expect(altText).toBeTruthy();
      expect(altText.length).toBeGreaterThan(10);

      // Verify alt text is descriptive and mentions key components
      expect(altText.toLowerCase()).toMatch(/lsm|architecture|data flow/);
    }
  });

  test('Test Case 4: Brief explanation of each component is provided alongside diagram', async ({ page }) => {
    // Navigate to architecture section
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Find the component explanations container
    const explanationsContainer = page.locator('[data-testid="architecture-explanations"]');
    await expect(explanationsContainer).toBeVisible();

    // Verify WAL explanation is present
    const walExplanation = explanationsContainer.locator('[data-testid="wal-explanation"]');
    await expect(walExplanation).toBeVisible();
    const walText = await walExplanation.textContent();
    expect(walText.toLowerCase()).toMatch(/write-ahead log|wal|durability|recovery/);

    // Verify Memtable explanation is present
    const memtableExplanation = explanationsContainer.locator('[data-testid="memtable-explanation"]');
    await expect(memtableExplanation).toBeVisible();
    const memtableText = await memtableExplanation.textContent();
    expect(memtableText.toLowerCase()).toMatch(/memory|in-memory|skip list|active/);

    // Verify Immutable Memtables explanation is present
    const immutableExplanation = explanationsContainer.locator('[data-testid="immutable-explanation"]');
    await expect(immutableExplanation).toBeVisible();
    const immutableText = await immutableExplanation.textContent();
    expect(immutableText.toLowerCase()).toMatch(/immutable|flush|compaction|queue/);

    // Verify SSTables explanation is present
    const sstableExplanation = explanationsContainer.locator('[data-testid="sstable-explanation"]');
    await expect(sstableExplanation).toBeVisible();
    const sstableText = await sstableExplanation.textContent();
    expect(sstableText.toLowerCase()).toMatch(/sorted|disk|level|persist/);
  });

  test('Architecture section has proper accessibility structure', async ({ page }) => {
    // Verify the section has proper aria-labelledby
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toHaveAttribute('aria-labelledby', 'architecture-title');

    // Verify the heading exists and is properly labeled
    const sectionHeading = page.locator('#architecture-title');
    await expect(sectionHeading).toBeVisible();
    await expect(sectionHeading).toContainText('Architecture');

    // Verify the section uses proper semantic HTML structure
    const tagName = await architectureSection.evaluate(el => el.tagName.toLowerCase());
    expect(tagName).toBe('section');
  });

  test('Architecture diagram is visually displayed within the architecture section', async ({ page }) => {
    // Navigate to architecture section
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Scroll the architecture section into view
    await architectureSection.scrollIntoViewIfNeeded();

    // Verify the diagram container has visual dimensions
    const diagramContainer = page.locator('[data-testid="architecture-diagram"]');
    const boundingBox = await diagramContainer.boundingBox();

    // Verify the diagram has meaningful dimensions (not zero)
    expect(boundingBox).toBeTruthy();
    expect(boundingBox.width).toBeGreaterThan(100);
    expect(boundingBox.height).toBeGreaterThan(100);
  });

  test('Data flow arrows or connections are visible in the diagram', async ({ page }) => {
    // Navigate to architecture section
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Find the architecture diagram
    const diagramContainer = page.locator('[data-testid="architecture-diagram"]');
    await expect(diagramContainer).toBeVisible();

    // Check for visual flow indicators (arrows, paths, or flow text)
    // The diagram should show the flow: WAL -> Memtable -> Immutable -> SSTables
    const svgElement = diagramContainer.locator('svg');

    if (await svgElement.count() > 0) {
      // Check for arrow elements (path, polygon, line, or marker)
      const flowIndicators = svgElement.locator('path, polygon, line, marker, text:has-text("→")');
      const indicatorCount = await flowIndicators.count();
      expect(indicatorCount).toBeGreaterThan(0);
    }
  });
});
