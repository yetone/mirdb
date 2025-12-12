import { test, expect } from '@playwright/test';

/**
 * E2E Tests for Architecture Diagram Display
 * Scenario: Verify that the architecture overview section displays a visual diagram showing data flow
 * Tests the presence, visibility, and accessibility of the architecture diagram
 */

test.describe('Architecture Diagram Display', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to homepage before each test
    await page.goto('/');
  });

  /**
   * Test Case 1: Check for architecture section
   * Input: Check for architecture section
   * Expected: Architecture section is present on the page
   */
  test('should display architecture section on the page', async ({ page }) => {
    // Verify the architecture section exists
    const architectureSection = page.locator('[data-testid="architecture-section"]');
    await expect(architectureSection).toBeVisible();

    // Verify the architecture section has a proper heading
    const architectureHeading = page.locator('[data-testid="architecture-heading"]');
    await expect(architectureHeading).toBeVisible();

    // Verify heading text contains relevant content about architecture
    const headingText = await architectureHeading.textContent();
    expect(headingText?.toLowerCase()).toMatch(/architecture/);
  });

  /**
   * Test Case 2: Verify architecture diagram exists
   * Input: Verify architecture diagram exists
   * Expected: Visual diagram or image is displayed showing data flow
   */
  test('should display visual diagram showing data flow', async ({ page }) => {
    // Verify the architecture diagram container exists
    const diagramContainer = page.locator('[data-testid="architecture-diagram"]');
    await expect(diagramContainer).toBeVisible();

    // Verify diagram contains visual elements (SVG or image)
    // Check for either an SVG diagram or an image element
    const svgDiagram = diagramContainer.locator('svg');
    const imgDiagram = diagramContainer.locator('img');

    // At least one visual representation should be present
    const hasSvg = await svgDiagram.count() > 0;
    const hasImg = await imgDiagram.count() > 0;
    expect(hasSvg || hasImg).toBe(true);

    // Verify the diagram contains key components of LSM tree architecture
    // Check for presence of WAL, Memtable, SSTable text labels
    const diagramText = await diagramContainer.textContent();
    expect(diagramText?.toLowerCase()).toMatch(/(wal|write-ahead|write ahead)/);
    expect(diagramText?.toLowerCase()).toMatch(/memtable/);
    expect(diagramText?.toLowerCase()).toMatch(/sstable/);
  });

  /**
   * Test Case 3: Check diagram accessibility
   * Input: Check diagram accessibility
   * Expected: Diagram has alt text or accessible description
   */
  test('should have accessible diagram with alt text or aria description', async ({ page }) => {
    // Verify the architecture diagram container exists
    const diagramContainer = page.locator('[data-testid="architecture-diagram"]');
    await expect(diagramContainer).toBeVisible();

    // Check for accessibility attributes
    // Option 1: aria-label on the container or diagram element
    // Option 2: aria-labelledby pointing to a description
    // Option 3: alt text on image element
    // Option 4: role="img" with aria-label

    const containerAriaLabel = await diagramContainer.getAttribute('aria-label');
    const containerAriaLabelledBy = await diagramContainer.getAttribute('aria-labelledby');
    const containerRole = await diagramContainer.getAttribute('role');

    // Check for img alt text
    const imgElement = diagramContainer.locator('img');
    let imgAlt = null;
    if (await imgElement.count() > 0) {
      imgAlt = await imgElement.getAttribute('alt');
    }

    // Check for SVG accessibility
    const svgElement = diagramContainer.locator('svg');
    let svgAriaLabel = null;
    let svgTitle = null;
    if (await svgElement.count() > 0) {
      svgAriaLabel = await svgElement.getAttribute('aria-label');
      // Check for title element inside SVG
      const titleElement = svgElement.locator('title');
      if (await titleElement.count() > 0) {
        svgTitle = await titleElement.textContent();
      }
    }

    // Check for accessible description element
    const accessibleDescription = page.locator('[data-testid="architecture-diagram-description"]');
    const hasDescription = await accessibleDescription.count() > 0;

    // At least one accessibility mechanism should be present
    const hasAccessibility =
      containerAriaLabel !== null ||
      containerAriaLabelledBy !== null ||
      (containerRole === 'img' && containerAriaLabel !== null) ||
      (imgAlt !== null && imgAlt.length > 0) ||
      svgAriaLabel !== null ||
      (svgTitle !== null && svgTitle.length > 0) ||
      hasDescription;

    expect(hasAccessibility).toBe(true);
  });

  /**
   * Additional Test: Verify diagram shows correct data flow components
   * Validates the diagram displays the complete LSM tree data flow
   */
  test('should display complete LSM tree architecture components', async ({ page }) => {
    // Verify the architecture section is visible
    const architectureSection = page.locator('[data-testid="architecture-section"]');
    await expect(architectureSection).toBeVisible();

    // Get the full section text content to verify all key components
    const sectionText = await architectureSection.textContent();

    // Verify key components are mentioned:
    // Write path components
    expect(sectionText?.toLowerCase()).toMatch(/(write|request)/);

    // WAL component
    expect(sectionText?.toLowerCase()).toMatch(/(wal|write-ahead|write ahead)/);

    // Memtable component
    expect(sectionText?.toLowerCase()).toMatch(/memtable/);

    // Immutable memtables (or "imm" reference)
    expect(sectionText?.toLowerCase()).toMatch(/(immutable|imm)/);

    // SSTable component
    expect(sectionText?.toLowerCase()).toMatch(/sstable/);
  });
});
