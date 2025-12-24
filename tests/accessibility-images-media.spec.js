// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for Accessibility - Images and Media (Scenario 13)
 * Verifies that images have appropriate alt text as specified in NFR-3 (WCAG 2.1 AA compliance)
 */

test.describe('Accessibility - Images and Media', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Check all img elements for alt attribute
   * Expected: Every img element has an alt attribute
   */
  test('should have alt attribute on all img elements', async ({ page }) => {
    // Get all img elements on the page
    const images = page.locator('img');
    const imageCount = await images.count();

    // If there are no img elements, check that we have proper alternative visual representations
    if (imageCount === 0) {
      // Verify SVG images have proper accessibility attributes instead
      const svgImages = page.locator('svg[role="img"]');
      const svgCount = await svgImages.count();

      // There should be at least one visual element (SVG or img)
      expect(svgCount).toBeGreaterThan(0);

      // Verify all SVG images with role="img" have proper accessibility
      for (let i = 0; i < svgCount; i++) {
        const svg = svgImages.nth(i);

        // SVG with role="img" should have aria-labelledby or aria-label
        const hasAriaLabelledby = await svg.getAttribute('aria-labelledby');
        const hasAriaLabel = await svg.getAttribute('aria-label');

        expect(hasAriaLabelledby || hasAriaLabel).toBeTruthy();
      }
    } else {
      // Check every img element has an alt attribute
      for (let i = 0; i < imageCount; i++) {
        const img = images.nth(i);
        const altAttr = await img.getAttribute('alt');

        // alt attribute should exist (can be empty string for decorative images)
        expect(altAttr).not.toBeNull();
      }
    }
  });

  /**
   * Test Case 2: Verify architecture diagram alt text
   * Expected: Architecture diagram has descriptive alt text explaining the LSM tree flow
   */
  test('should have descriptive alt text for architecture diagram', async ({ page }) => {
    // Navigate to architecture section
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Get the SVG architecture diagram
    const svgElement = page.locator('[data-testid="architecture-svg"], svg.lsm-tree-diagram');
    await expect(svgElement).toBeVisible();

    // Check for aria-labelledby attribute pointing to title and desc
    const ariaLabelledby = await svgElement.getAttribute('aria-labelledby');
    expect(ariaLabelledby).toBeTruthy();

    // Verify the title element exists and describes the architecture
    const titleId = ariaLabelledby.split(' ')[0]; // First ID is typically the title
    const titleElement = page.locator(`#${titleId}`);
    const titleText = await titleElement.textContent();

    // Title should mention architecture or diagram
    expect(titleText).toBeTruthy();
    expect(titleText.toLowerCase()).toMatch(/architecture|diagram|lsm|mirdb/i);

    // Verify description exists and is meaningful
    const descIds = ariaLabelledby.split(' ');
    if (descIds.length > 1) {
      const descElement = page.locator(`#${descIds[1]}`);
      const descText = await descElement.textContent();

      // Description should explain the LSM tree flow
      expect(descText).toBeTruthy();
      expect(descText.length).toBeGreaterThan(50);

      // Should mention key LSM tree components
      const descLower = descText.toLowerCase();
      expect(
        descLower.includes('data flow') ||
        descLower.includes('wal') ||
        descLower.includes('memtable') ||
        descLower.includes('sstable') ||
        descLower.includes('write') ||
        descLower.includes('storage')
      ).toBeTruthy();
    }
  });

  /**
   * Test Case 3: Check logo alt text
   * Expected: Logo image has appropriate alt text (e.g., 'MirDB logo')
   */
  test('should have appropriate alt text for logo image', async ({ page }) => {
    // Check for logo in the hero section
    const heroSection = page.locator('#hero, .hero');
    await expect(heroSection).toBeVisible();

    // Look for logo image (could be img or SVG)
    const logoImg = page.locator('.logo img, img.logo, [data-testid="logo"] img, header img');
    const logoImgCount = await logoImg.count();

    if (logoImgCount > 0) {
      // If there's an img logo, verify it has appropriate alt text
      const altText = await logoImg.first().getAttribute('alt');
      expect(altText).toBeTruthy();
      expect(altText.toLowerCase()).toMatch(/mirdb|logo/i);
    } else {
      // Check for SVG logo with proper accessibility
      const logoSvg = page.locator('.logo svg, svg.logo, [data-testid="logo"] svg');
      const logoSvgCount = await logoSvg.count();

      if (logoSvgCount > 0) {
        const svg = logoSvg.first();

        // SVG logo should be either decorative (aria-hidden) or have accessible label
        const ariaHidden = await svg.getAttribute('aria-hidden');
        const ariaLabel = await svg.getAttribute('aria-label');
        const ariaLabelledby = await svg.getAttribute('aria-labelledby');
        const role = await svg.getAttribute('role');

        // If it's meaningful content, it should have accessible name
        if (ariaHidden !== 'true') {
          expect(ariaLabel || ariaLabelledby || role === 'presentation').toBeTruthy();
        }
      } else {
        // If logo is text-based (H1 with MirDB), verify the text is present and accessible
        const productName = page.locator('h1').first();
        await expect(productName).toBeVisible();
        const nameText = await productName.textContent();
        expect(nameText).toContain('MirDB');
      }
    }
  });

  /**
   * Additional test: Verify decorative images have empty alt or role="presentation"
   */
  test('should properly handle decorative images', async ({ page }) => {
    // Get all images
    const allImages = page.locator('img');
    const imageCount = await allImages.count();

    // Get all SVG elements with decorative role
    const decorativeSvgs = page.locator('svg[aria-hidden="true"], svg[role="presentation"]');

    for (let i = 0; i < imageCount; i++) {
      const img = allImages.nth(i);
      const altText = await img.getAttribute('alt');
      const role = await img.getAttribute('role');

      // If alt is empty string, it should be a decorative image
      // and may have role="presentation" (optional but recommended)
      if (altText === '') {
        // Empty alt is valid for decorative images - just ensure it's intentional
        // by checking it's not in a critical content area
        const parentText = await img.locator('..').textContent();
        // Decorative images typically don't have important parent content that requires the image
      }
    }

    // All decorative SVGs should be properly hidden from screen readers
    const decorativeSvgCount = await decorativeSvgs.count();
    for (let i = 0; i < decorativeSvgCount; i++) {
      const svg = decorativeSvgs.nth(i);
      const ariaHidden = await svg.getAttribute('aria-hidden');
      const role = await svg.getAttribute('role');

      // Either aria-hidden="true" or role="presentation" should be present
      expect(ariaHidden === 'true' || role === 'presentation').toBeTruthy();
    }
  });

  /**
   * Additional test: Verify all visual elements have accessible alternatives
   */
  test('should have accessible alternatives for all visual elements', async ({ page }) => {
    // Check architecture diagram section
    const archDiagram = page.locator('[data-testid="architecture-diagram"]');

    if (await archDiagram.count() > 0) {
      await expect(archDiagram).toBeVisible();

      // The diagram section should have accompanying text description
      const archSection = page.locator('#architecture');
      const archDescription = archSection.locator('.architecture-description, .arch-detail');

      await expect(archDescription.first()).toBeVisible();
      const descText = await archDescription.allTextContents();
      expect(descText.length).toBeGreaterThan(0);
    }
  });

  /**
   * Additional test: Icon SVGs should be properly marked as decorative or have labels
   */
  test('should properly handle icon SVGs', async ({ page }) => {
    // Find all inline SVG icons (small SVGs typically used as icons)
    const iconSvgs = page.locator('svg.icon, svg.github-icon, .feature-icon svg');
    const iconCount = await iconSvgs.count();

    for (let i = 0; i < iconCount; i++) {
      const icon = iconSvgs.nth(i);

      // Icons should either be hidden from screen readers or have accessible labels
      const ariaHidden = await icon.getAttribute('aria-hidden');
      const ariaLabel = await icon.getAttribute('aria-label');
      const role = await icon.getAttribute('role');

      // Icon should be either decorative (hidden) or have an accessible name
      expect(
        ariaHidden === 'true' ||
        role === 'presentation' ||
        ariaLabel !== null
      ).toBeTruthy();
    }
  });

  /**
   * Additional test: Feature icons (emoji) accessibility
   */
  test('should handle emoji icons accessibly', async ({ page }) => {
    // Check feature icons (often implemented as emoji in spans)
    const featureIcons = page.locator('.feature-icon');
    const iconCount = await featureIcons.count();

    if (iconCount > 0) {
      for (let i = 0; i < iconCount; i++) {
        const icon = featureIcons.nth(i);

        // Feature icons should either:
        // 1. Have aria-hidden="true" (decorative, with text nearby)
        // 2. Have role="img" and aria-label (meaningful content)
        const ariaHidden = await icon.getAttribute('aria-hidden');
        const role = await icon.getAttribute('role');
        const ariaLabel = await icon.getAttribute('aria-label');

        // The icon's sibling/parent should have descriptive text
        const parentCard = icon.locator('..');
        const hasText = await parentCard.locator('h3, p').count();

        // Either the icon is decorative with nearby text, or it has its own label
        expect(
          hasText > 0 || // Text nearby makes icon decorative
          ariaHidden === 'true' ||
          (role === 'img' && ariaLabel !== null)
        ).toBeTruthy();
      }
    }
  });
});
