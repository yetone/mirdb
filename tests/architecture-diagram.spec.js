// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for Architecture Diagram Display (Scenario 5)
 * Verifies that a visual representation of the LSM tree architecture is displayed as specified in REQ-6
 */

test.describe('Architecture Diagram Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Check for architecture diagram image
   * Expected: SVG or image element with architecture diagram is present
   */
  test('should display SVG architecture diagram', async ({ page }) => {
    // Navigate to architecture section
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Check that a visual diagram (SVG) is displayed
    const architectureDiagram = page.locator('[data-testid="architecture-diagram"]');
    await expect(architectureDiagram).toBeVisible();

    // Verify SVG element is present
    const svgElement = page.locator('[data-testid="architecture-svg"]');
    await expect(svgElement).toBeVisible();

    // Verify it's actually an SVG element
    const tagName = await svgElement.evaluate(el => el.tagName.toLowerCase());
    expect(tagName).toBe('svg');

    // Verify the SVG has proper role for accessibility
    await expect(svgElement).toHaveAttribute('role', 'img');
  });

  /**
   * Test Case 2: Verify diagram has alt text
   * Expected: Image has descriptive alt text for accessibility
   */
  test('should have descriptive alt text for accessibility', async ({ page }) => {
    // Navigate to architecture section
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Get the SVG element
    const svgElement = page.locator('[data-testid="architecture-svg"]');
    await expect(svgElement).toBeVisible();

    // Check for aria-labelledby attribute (references title and desc)
    const ariaLabelledby = await svgElement.getAttribute('aria-labelledby');
    expect(ariaLabelledby).toBeTruthy();
    expect(ariaLabelledby).toContain('architecture-diagram-title');
    expect(ariaLabelledby).toContain('architecture-diagram-desc');

    // Verify the title element exists and has meaningful content
    // Note: SVG <title> elements are not visually visible but provide accessibility
    const titleElement = page.locator('#architecture-diagram-title');
    await expect(titleElement).toBeAttached();
    const titleText = await titleElement.textContent();
    expect(titleText).toBeTruthy();
    expect(titleText.toLowerCase()).toContain('architecture');

    // Verify the desc element exists and has meaningful content
    const descElement = page.locator('#architecture-diagram-desc');
    await expect(descElement).toBeAttached();
    const descText = await descElement.textContent();
    expect(descText).toBeTruthy();
    expect(descText.length).toBeGreaterThan(50); // Should be a descriptive text
  });

  /**
   * Test Case 3: Check architecture section text content
   * Expected: Text mentions memtable, SSTable, and compaction concepts
   */
  test('should mention memtable, SSTable, and compaction concepts', async ({ page }) => {
    // Navigate to architecture section
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Get all text content from the architecture section
    const sectionText = await architectureSection.textContent();
    const textLower = sectionText.toLowerCase();

    // Verify memtable is mentioned
    const hasMemtable = textLower.includes('memtable');
    expect(hasMemtable).toBeTruthy();

    // Verify SSTable is mentioned
    const hasSSTable = textLower.includes('sstable') || textLower.includes('sst');
    expect(hasSSTable).toBeTruthy();

    // Verify compaction is mentioned
    const hasCompaction = textLower.includes('compaction');
    expect(hasCompaction).toBeTruthy();
  });

  /**
   * Additional test: Verify diagram shows key LSM tree components
   * Checks that the SVG contains WAL, Memtable, and SSTable components
   */
  test('should display key LSM tree components in diagram', async ({ page }) => {
    // Navigate to architecture section
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Check for WAL component in diagram
    const walComponent = page.locator('[data-component="wal"]');
    await expect(walComponent).toBeVisible();

    // Check for Memtable component in diagram
    const memtableComponent = page.locator('[data-component="memtable"]');
    await expect(memtableComponent).toBeVisible();

    // Check for SSTable component in diagram
    const sstableComponent = page.locator('[data-component="sstable"]');
    await expect(sstableComponent).toBeVisible();

    // Check for Level 0 SSTs component
    const level0Component = page.locator('[data-component="level0-sst"]');
    await expect(level0Component).toBeVisible();
  });

  /**
   * Test navigation to architecture section via anchor link
   */
  test('should be able to navigate to architecture section', async ({ page }) => {
    // Click on architecture link in footer
    await page.click('a[href="#architecture"]');

    // Verify architecture section is visible and in view
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Verify the diagram is visible after navigation
    const svgElement = page.locator('[data-testid="architecture-svg"]');
    await expect(svgElement).toBeVisible();
  });

  /**
   * Test that architecture description explains the write path
   */
  test('should explain write path in architecture description', async ({ page }) => {
    // Navigate to architecture section
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Get description text
    const descriptionSection = page.locator('.architecture-description');
    await expect(descriptionSection).toBeVisible();

    const descriptionText = await descriptionSection.textContent();
    const textLower = descriptionText.toLowerCase();

    // Verify write path explanation
    expect(textLower).toContain('write');
    expect(textLower).toContain('wal');
    expect(textLower).toContain('durability');
  });
});
