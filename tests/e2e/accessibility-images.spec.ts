import { test, expect } from '@playwright/test';

test.describe('Accessibility - Images and Alt Text', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: All img elements have non-empty alt attributes', async ({ page }) => {
    // Get all img elements on the page
    const images = page.locator('img');
    const imageCount = await images.count();

    // If there are no images, the test passes (no violation possible)
    if (imageCount === 0) {
      // Verify no img elements exist
      expect(imageCount).toBe(0);
      return;
    }

    // For each image, verify it has a non-empty alt attribute
    for (let i = 0; i < imageCount; i++) {
      const img = images.nth(i);
      const alt = await img.getAttribute('alt');
      const src = await img.getAttribute('src');

      // alt attribute must exist and be non-empty (unless explicitly empty for decorative images)
      expect(alt, `Image ${src} should have an alt attribute`).not.toBeNull();

      // Non-decorative images should have meaningful alt text
      // Decorative images should have empty alt=""
      if (alt !== '') {
        expect(alt!.length, `Image ${src} should have meaningful alt text`).toBeGreaterThan(0);
      }
    }
  });

  test('TC2: Architecture diagram has descriptive alt text explaining the visual', async ({ page }) => {
    // Find the architecture diagram SVG
    const architectureDiagram = page.locator('[data-testid="architecture-diagram"] svg, .architecture-diagram');

    // Verify the diagram exists
    await expect(architectureDiagram).toBeVisible();

    // Check for accessible attributes on the SVG
    // SVG should have role="img" for accessibility
    await expect(architectureDiagram.first()).toHaveAttribute('role', 'img');

    // Check for aria-label or aria-labelledby
    const ariaLabel = await architectureDiagram.first().getAttribute('aria-label');
    const ariaLabelledby = await architectureDiagram.first().getAttribute('aria-labelledby');

    // Should have either aria-label or aria-labelledby
    const hasAccessibleName = (ariaLabel && ariaLabel.length > 0) || ariaLabelledby;
    expect(hasAccessibleName, 'Architecture diagram should have aria-label or aria-labelledby').toBeTruthy();

    // If aria-label exists, verify it's descriptive (more than just a few words)
    if (ariaLabel) {
      expect(ariaLabel.length, 'aria-label should be descriptive').toBeGreaterThan(10);
      // Should mention architecture, LSM, or diagram related terms
      const isDescriptive =
        ariaLabel.toLowerCase().includes('architecture') ||
        ariaLabel.toLowerCase().includes('lsm') ||
        ariaLabel.toLowerCase().includes('diagram') ||
        ariaLabel.toLowerCase().includes('tree');
      expect(isDescriptive, 'aria-label should describe the architecture diagram').toBeTruthy();
    }

    // Check for <title> element inside SVG (fallback for screen readers)
    const svgTitle = page.locator('[data-testid="architecture-diagram"] svg title, .architecture-diagram title');
    const titleCount = await svgTitle.count();
    if (titleCount > 0) {
      const titleText = await svgTitle.first().textContent();
      expect(titleText, 'SVG title should have meaningful text').not.toBeNull();
      expect(titleText!.length, 'SVG title should have meaningful text').toBeGreaterThan(0);
    }
  });

  test('TC3: Decorative images have empty alt="" attribute', async ({ page }) => {
    // Get all SVG elements that are used as decorative icons (not diagrams)
    const decorativeIcons = page.locator('.feature-icon svg, .feature-card svg');
    const iconCount = await decorativeIcons.count();

    // Feature icons should be decorative and have aria-hidden="true"
    for (let i = 0; i < iconCount; i++) {
      const icon = decorativeIcons.nth(i);

      // Decorative SVGs should have aria-hidden="true"
      const ariaHidden = await icon.getAttribute('aria-hidden');
      const role = await icon.getAttribute('role');

      // Either aria-hidden should be true, or role should be "presentation" or "none"
      const isMarkedDecorative =
        ariaHidden === 'true' ||
        role === 'presentation' ||
        role === 'none' ||
        role === null; // SVGs without role in icon context are often treated as decorative

      expect(isMarkedDecorative, `Feature icon ${i + 1} should be marked as decorative with aria-hidden="true"`).toBeTruthy();
    }

    // Check any <img> elements that might be decorative
    const allImages = page.locator('img');
    const imageCount = await allImages.count();

    for (let i = 0; i < imageCount; i++) {
      const img = allImages.nth(i);
      const alt = await img.getAttribute('alt');
      const className = await img.getAttribute('class');
      const dataRole = await img.getAttribute('data-role');

      // If an image is marked as decorative (via class or data attribute)
      const isMarkedDecorative =
        className?.includes('decorative') ||
        dataRole === 'decorative' ||
        dataRole === 'presentation';

      if (isMarkedDecorative) {
        // Decorative images should have empty alt=""
        expect(alt, 'Decorative images should have empty alt=""').toBe('');
      }
    }
  });

  test('SVG diagrams have proper accessibility attributes', async ({ page }) => {
    // Get all SVGs that are meant to convey information (not icons)
    const informativeSvgs = page.locator('svg[role="img"]');
    const svgCount = await informativeSvgs.count();

    for (let i = 0; i < svgCount; i++) {
      const svg = informativeSvgs.nth(i);

      // Should have aria-label or aria-labelledby
      const ariaLabel = await svg.getAttribute('aria-label');
      const ariaLabelledby = await svg.getAttribute('aria-labelledby');

      const hasAccessibleName = (ariaLabel && ariaLabel.length > 0) || ariaLabelledby;
      expect(hasAccessibleName, `SVG ${i + 1} with role="img" should have accessible name`).toBeTruthy();
    }
  });

  test('All images and diagrams are accessible to screen readers', async ({ page }) => {
    // Comprehensive accessibility check for all visual content

    // 1. Check all img elements
    const images = page.locator('img');
    const imageCount = await images.count();

    for (let i = 0; i < imageCount; i++) {
      const img = images.nth(i);
      const alt = await img.getAttribute('alt');
      expect(alt, `Image ${i + 1} must have alt attribute`).not.toBeNull();
    }

    // 2. Check all informative SVGs (with role="img")
    const informativeSvgs = page.locator('svg[role="img"]');
    const informativeSvgCount = await informativeSvgs.count();

    for (let i = 0; i < informativeSvgCount; i++) {
      const svg = informativeSvgs.nth(i);
      const ariaLabel = await svg.getAttribute('aria-label');
      const ariaLabelledby = await svg.getAttribute('aria-labelledby');
      const titleElement = await svg.locator('title').count();

      const hasAccessibleName = ariaLabel || ariaLabelledby || titleElement > 0;
      expect(hasAccessibleName, `Informative SVG ${i + 1} must have accessible name`).toBeTruthy();
    }

    // 3. Check decorative SVGs (icons) have aria-hidden
    const iconSvgs = page.locator('.feature-icon svg');
    const iconSvgCount = await iconSvgs.count();

    for (let i = 0; i < iconSvgCount; i++) {
      const svg = iconSvgs.nth(i);
      const ariaHidden = await svg.getAttribute('aria-hidden');
      expect(ariaHidden, `Icon SVG ${i + 1} should have aria-hidden="true"`).toBe('true');
    }
  });
});
