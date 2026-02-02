/**
 * Architecture Section E2E Tests
 * Owner: Scenario 4 - Architecture Overview Section
 *
 * Tests:
 * - Architecture diagram presence
 * - Diagram accessibility (alt text)
 * - LSM-tree components mentioned
 * - Diagram loads without errors
 */

import { test, expect } from '@playwright/test';

test.describe('Architecture Overview Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Architecture section contains a diagram element (image or SVG)', async ({ page }) => {
    // Navigate to architecture section
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Check for diagram presence - could be img or inline SVG
    const diagram = page.locator('#architecture img.architecture-diagram, #architecture svg');
    await expect(diagram.first()).toBeVisible();

    // Verify it's an image element with SVG source
    const img = page.locator('#architecture img.architecture-diagram');
    const imgCount = await img.count();

    if (imgCount > 0) {
      const src = await img.getAttribute('src');
      expect(src).toContain('architecture');
    }
  });

  test('TC2: Architecture diagram has descriptive alt text for accessibility', async ({ page }) => {
    // Find the architecture diagram image
    const diagram = page.locator('#architecture img.architecture-diagram');
    await expect(diagram).toBeVisible();

    // Get alt text
    const altText = await diagram.getAttribute('alt');

    // Verify alt text exists and is descriptive
    expect(altText).toBeTruthy();
    expect(altText.length).toBeGreaterThan(50); // Should be descriptive, not just "diagram"

    // Alt text should mention key architecture concepts
    const altTextLower = altText.toLowerCase();
    expect(altTextLower).toMatch(/lsm-tree|architecture|memtable|sstable/i);
  });

  test('TC3: Architecture section mentions memtable, immutable memtables, and SSTable levels', async ({ page }) => {
    // Get the architecture section
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Get all text content from the section
    const sectionText = await architectureSection.textContent();
    const textLower = sectionText.toLowerCase();

    // Verify memtable is mentioned
    expect(textLower).toContain('memtable');

    // Verify immutable memtables are mentioned
    expect(textLower).toMatch(/immutable.*memtable|immutable/i);

    // Verify SSTable levels are mentioned
    expect(textLower).toMatch(/sstable|sst|level/i);
  });

  test('TC4: Architecture diagram loads correctly without errors', async ({ page }) => {
    // Set up request failure listener
    const failedRequests = [];
    page.on('requestfailed', request => {
      if (request.url().includes('architecture')) {
        failedRequests.push(request.url());
      }
    });

    // Navigate to page
    await page.goto('/');

    // Wait for architecture section to be visible
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Find the diagram
    const diagram = page.locator('#architecture img.architecture-diagram');
    await expect(diagram).toBeVisible();

    // Verify the image is visible and rendered (SVGs don't have naturalWidth like raster images)
    // Check that the element is visible and has a bounding box
    const boundingBox = await diagram.boundingBox();
    expect(boundingBox).toBeTruthy();
    expect(boundingBox.width).toBeGreaterThan(0);
    expect(boundingBox.height).toBeGreaterThan(0);

    // Verify the image src is correct
    const src = await diagram.getAttribute('src');
    expect(src).toContain('architecture.svg');

    // No failed requests for architecture images
    expect(failedRequests.filter(url => url.includes('architecture'))).toHaveLength(0);
  });

  test('Architecture section has proper heading structure', async ({ page }) => {
    // Check main heading
    const heading = page.locator('#architecture-heading');
    await expect(heading).toBeVisible();

    // Verify it's an h2 element
    const tagName = await heading.evaluate(el => el.tagName.toLowerCase());
    expect(tagName).toBe('h2');

    // Verify content
    await expect(heading).toContainText('Architecture');
  });

  test('Architecture section has aria-labelledby attribute', async ({ page }) => {
    const architectureSection = page.locator('#architecture');

    // Check aria-labelledby attribute
    await expect(architectureSection).toHaveAttribute('aria-labelledby', 'architecture-heading');
  });

  test('Architecture components section displays key LSM-tree components', async ({ page }) => {
    // Check for component cards
    const components = page.locator('.architecture-component');

    // Should have at least 3 components (Memtable, Immutable Memtables, SSTable Levels)
    await expect(components).toHaveCount(3);

    // Check first component
    const firstComponentTitle = await components.first().locator('.component-title').textContent();
    expect(firstComponentTitle).toBeTruthy();
  });

  test('Architecture diagram has proper width and height attributes', async ({ page }) => {
    const diagram = page.locator('#architecture img.architecture-diagram');
    await expect(diagram).toBeVisible();

    // Check width and height attributes exist (helps with layout stability)
    const width = await diagram.getAttribute('width');
    const height = await diagram.getAttribute('height');

    expect(width).toBeTruthy();
    expect(height).toBeTruthy();
  });

  test('Architecture diagram has lazy loading attribute', async ({ page }) => {
    const diagram = page.locator('#architecture img.architecture-diagram');
    await expect(diagram).toBeVisible();

    // Check for lazy loading attribute
    const loading = await diagram.getAttribute('loading');
    expect(loading).toBe('lazy');
  });

  test('Can scroll to architecture section via navigation', async ({ page }) => {
    // Scroll to top first
    await page.evaluate(() => window.scrollTo(0, 0));

    // Navigate using anchor link
    await page.goto('/#architecture');

    // Wait for architecture section to be in view
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeInViewport();
  });
});
