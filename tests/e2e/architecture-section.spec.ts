import { test, expect } from '@playwright/test';

/**
 * E2E Tests for MirDB Homepage Architecture Section
 *
 * These tests verify that the architecture section displays:
 * - A visual diagram (img, svg, or canvas) showing data flow
 * - Accessible alt text for the diagram
 * - LSM-tree structure and benefits explanation
 * - Key component mentions (WAL, Memtable, SSTable, compaction)
 */

test.describe('Architecture Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Check for architecture diagram image or SVG', async ({ page }) => {
    // Navigate to architecture section
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Check for visual diagram element (img, svg, or canvas)
    const diagram = architectureSection.locator('img, svg, canvas, [data-testid="architecture-diagram"]');
    await expect(diagram.first()).toBeVisible();

    // Verify the diagram shows data flow architecture
    // It should be within the architecture-diagram container
    const diagramContainer = architectureSection.locator('.architecture-diagram');
    await expect(diagramContainer).toBeVisible();

    // Check that there's a visual representation (SVG is inline or img exists)
    const svgElement = architectureSection.locator('svg');
    const imgElement = architectureSection.locator('img');

    const hasSvg = await svgElement.count() > 0;
    const hasImg = await imgElement.count() > 0;

    expect(hasSvg || hasImg).toBeTruthy();
  });

  test('TC2: Verify diagram has alt text for accessibility', async ({ page }) => {
    // Navigate to architecture section
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Check for accessibility - either:
    // 1. SVG with title/desc or aria-label
    // 2. img with alt attribute
    // 3. Container with role and aria-label

    // Check for img with alt text
    const imgWithAlt = architectureSection.locator('img[alt]');
    const imgAltCount = await imgWithAlt.count();

    // Check for SVG with accessibility attributes
    const svgWithTitle = architectureSection.locator('svg title, svg[aria-label], svg[role="img"]');
    const svgAccessibleCount = await svgWithTitle.count();

    // Check for container with aria-label
    const containerWithAriaLabel = architectureSection.locator('[aria-label*="architecture"], [aria-label*="diagram"], [aria-label*="data flow"]');
    const containerAriaCount = await containerWithAriaLabel.count();

    // At least one accessibility method should be present
    const hasAccessibility = imgAltCount > 0 || svgAccessibleCount > 0 || containerAriaCount > 0;
    expect(hasAccessibility).toBeTruthy();

    // Verify the alt/aria text is descriptive (not empty or generic)
    if (imgAltCount > 0) {
      const altText = await imgWithAlt.first().getAttribute('alt');
      expect(altText).toBeTruthy();
      expect(altText!.length).toBeGreaterThan(10); // Descriptive alt text should be substantial
    }

    if (containerAriaCount > 0) {
      const ariaLabel = await containerWithAriaLabel.first().getAttribute('aria-label');
      expect(ariaLabel).toBeTruthy();
      expect(ariaLabel!.length).toBeGreaterThan(10);
    }
  });

  test('TC3: Check for LSM-tree explanation', async ({ page }) => {
    // Navigate to architecture section
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Get all text content from the architecture section
    const sectionText = await architectureSection.textContent();

    // Verify LSM-tree is mentioned
    expect(sectionText?.toLowerCase()).toContain('lsm');

    // Verify there's an explanation of the structure or benefits
    // Look for keywords that indicate explanation
    const hasExplanation =
      sectionText?.toLowerCase().includes('write') ||
      sectionText?.toLowerCase().includes('optimized') ||
      sectionText?.toLowerCase().includes('performance') ||
      sectionText?.toLowerCase().includes('durability') ||
      sectionText?.toLowerCase().includes('storage') ||
      sectionText?.toLowerCase().includes('architecture');

    expect(hasExplanation).toBeTruthy();

    // Verify explanatory text is present (paragraph or description element)
    const explanatoryText = architectureSection.locator('p, .section-description, .architecture-description');
    await expect(explanatoryText.first()).toBeVisible();
  });

  test('TC4: Verify key component mentions', async ({ page }) => {
    // Navigate to architecture section
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Get all text content from the architecture section
    const sectionText = await architectureSection.textContent();
    const textLower = sectionText?.toLowerCase() || '';

    // Verify WAL is mentioned
    const hasWAL = textLower.includes('wal') || textLower.includes('write-ahead log') || textLower.includes('write ahead log');
    expect(hasWAL).toBeTruthy();

    // Verify Memtable is mentioned
    const hasMemtable = textLower.includes('memtable') || textLower.includes('mem table') || textLower.includes('memory table');
    expect(hasMemtable).toBeTruthy();

    // Verify SSTable is mentioned
    const hasSSTable = textLower.includes('sstable') || textLower.includes('ss table') || textLower.includes('sorted string table');
    expect(hasSSTable).toBeTruthy();

    // Verify compaction is mentioned
    const hasCompaction = textLower.includes('compaction') || textLower.includes('compact');
    expect(hasCompaction).toBeTruthy();
  });
});
