// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Architecture Diagram', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 1: Architecture diagram image or SVG exists', async ({ page }) => {
    // Query for architecture diagram (SVG or img)
    const architectureSection = page.locator('[data-testid="architecture-section"]');
    await expect(architectureSection).toBeVisible();

    const diagram = page.locator('[data-testid="architecture-diagram"]');
    await expect(diagram).toBeVisible();
  });

  test('Test Case 2: Diagram has descriptive alt text for accessibility', async ({ page }) => {
    // Check diagram accessibility - either through aria-label, aria-labelledby, or title
    const diagram = page.locator('[data-testid="architecture-diagram"]');
    await expect(diagram).toBeVisible();

    // For SVG, check for title element or aria-label/aria-labelledby
    const svgTitle = diagram.locator('title');
    const hasSvgTitle = await svgTitle.count() > 0;

    if (hasSvgTitle) {
      const titleText = await svgTitle.textContent();
      expect(titleText).toBeTruthy();
      expect(titleText.length).toBeGreaterThan(10);
    } else {
      // Check for aria-label or aria-labelledby
      const ariaLabel = await diagram.getAttribute('aria-label');
      const ariaLabelledBy = await diagram.getAttribute('aria-labelledby');
      expect(ariaLabel || ariaLabelledBy).toBeTruthy();

      if (ariaLabel) {
        expect(ariaLabel.length).toBeGreaterThan(10);
      }
    }
  });

  test('Test Case 3: Diagram image loads without errors', async ({ page }) => {
    // Verify the diagram is visible and properly rendered
    const diagram = page.locator('[data-testid="architecture-diagram"]');
    await expect(diagram).toBeVisible();

    // Check that SVG has content (paths, rects, text elements, etc.)
    const svgElements = diagram.locator('path, rect, text, line, ellipse, circle, polygon, polyline');
    const elementCount = await svgElements.count();
    expect(elementCount).toBeGreaterThan(0);

    // Verify bounding box exists (diagram is rendered)
    const boundingBox = await diagram.boundingBox();
    expect(boundingBox).not.toBeNull();
    expect(boundingBox.width).toBeGreaterThan(100);
    expect(boundingBox.height).toBeGreaterThan(100);
  });

  test('Test Case 4: Diagram scales appropriately on different screen sizes', async ({ page }) => {
    const diagram = page.locator('[data-testid="architecture-diagram"]');
    await expect(diagram).toBeVisible();

    // Get initial bounding box at desktop size (default is 1280x720)
    const desktopBoundingBox = await diagram.boundingBox();
    expect(desktopBoundingBox).not.toBeNull();
    expect(desktopBoundingBox.width).toBeGreaterThan(0);
    expect(desktopBoundingBox.height).toBeGreaterThan(0);

    // Test tablet viewport (768px)
    await page.setViewportSize({ width: 768, height: 1024 });
    await expect(diagram).toBeVisible();
    const tabletBoundingBox = await diagram.boundingBox();
    expect(tabletBoundingBox).not.toBeNull();
    expect(tabletBoundingBox.width).toBeGreaterThan(0);
    expect(tabletBoundingBox.height).toBeGreaterThan(0);

    // Test mobile viewport (375px)
    await page.setViewportSize({ width: 375, height: 667 });
    await expect(diagram).toBeVisible();
    const mobileBoundingBox = await diagram.boundingBox();
    expect(mobileBoundingBox).not.toBeNull();
    expect(mobileBoundingBox.width).toBeGreaterThan(0);
    expect(mobileBoundingBox.height).toBeGreaterThan(0);

    // Verify diagram is responsive - on smaller screens it should fit within viewport
    // Mobile width should fit within mobile viewport (accounting for padding)
    expect(mobileBoundingBox.width).toBeLessThanOrEqual(375);
    // Diagram should maintain proper dimensions across all sizes
    expect(mobileBoundingBox.width).toBeLessThanOrEqual(tabletBoundingBox.width);
  });
});
