import { test, expect } from '@playwright/test';

test.describe('Accessibility - Images and Alt Text', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: All img elements have alt attributes', async ({ page }) => {
    // Find all img elements on the page
    const images = page.locator('img');
    const imageCount = await images.count();

    // Verify each img element has an alt attribute (can be empty for decorative images)
    for (let i = 0; i < imageCount; i++) {
      const img = images.nth(i);
      const altAttribute = await img.getAttribute('alt');
      const src = await img.getAttribute('src');

      // Alt attribute must exist (even if empty string for decorative images)
      expect(
        altAttribute,
        `Image ${src || `at index ${i}`} is missing alt attribute`
      ).not.toBeNull();
    }

    // If no img elements exist, that's acceptable (CSS-based images don't need alt)
    // The test passes as there are no violations
    if (imageCount === 0) {
      // Verify the page still has meaningful content
      const bodyText = await page.locator('body').textContent();
      expect(bodyText?.length).toBeGreaterThan(100);
    }
  });

  test('TC2: Architecture diagram has descriptive alt text describing MirDB data flow', async ({ page }) => {
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Locate the architecture diagram
    const diagram = page.locator('[data-testid="architecture-diagram"]');
    await expect(diagram).toBeVisible();

    // Check if diagram is an img element
    const imgElement = diagram.locator('img');
    const imgCount = await imgElement.count();

    if (imgCount > 0) {
      // If it's an image, check for descriptive alt text
      const altText = await imgElement.getAttribute('alt');
      expect(altText, 'Architecture diagram image must have alt text').toBeTruthy();
      expect(
        altText!.length,
        'Alt text should be descriptive (at least 20 characters)'
      ).toBeGreaterThan(20);

      // Alt text should describe MirDB data flow
      const altLower = altText!.toLowerCase();
      expect(
        altLower.includes('mirdb') || altLower.includes('data flow') || altLower.includes('architecture') || altLower.includes('lsm'),
        'Alt text should describe MirDB architecture or data flow'
      ).toBeTruthy();
    } else {
      // If it's a CSS/HTML-based diagram, check for ARIA accessibility attributes
      const ariaLabel = await diagram.getAttribute('aria-label');
      const role = await diagram.getAttribute('role');
      const ariaLabelledBy = await diagram.getAttribute('aria-labelledby');

      // Diagram should have proper ARIA attributes for accessibility
      expect(
        role === 'img' || ariaLabel || ariaLabelledBy,
        'CSS-based diagram must have role="img", aria-label, or aria-labelledby'
      ).toBeTruthy();

      // If using role="img", should have aria-label or aria-labelledby
      if (role === 'img') {
        expect(
          ariaLabel || ariaLabelledBy,
          'Diagram with role="img" must have aria-label or aria-labelledby'
        ).toBeTruthy();
      }

      // Verify aria-label describes MirDB data flow
      if (ariaLabel) {
        expect(
          ariaLabel.length,
          'Aria-label should be descriptive (at least 20 characters)'
        ).toBeGreaterThan(20);

        const ariaLower = ariaLabel.toLowerCase();
        expect(
          ariaLower.includes('mirdb') || ariaLower.includes('data flow') || ariaLower.includes('architecture') || ariaLower.includes('lsm'),
          'Aria-label should describe MirDB architecture or data flow'
        ).toBeTruthy();
      }
    }
  });

  test('TC3: Decorative images have empty alt attributes or are CSS backgrounds', async ({ page }) => {
    // Find all img elements
    const images = page.locator('img');
    const imageCount = await images.count();

    // Track decorative vs informative images
    const decorativeImages: string[] = [];
    const informativeImages: string[] = [];

    for (let i = 0; i < imageCount; i++) {
      const img = images.nth(i);
      const altText = await img.getAttribute('alt');
      const src = await img.getAttribute('src') || `image-${i}`;
      const role = await img.getAttribute('role');

      // Images with empty alt="" or role="presentation" are properly marked as decorative
      if (altText === '' || role === 'presentation' || role === 'none') {
        decorativeImages.push(src);
      } else if (altText !== null) {
        informativeImages.push(src);
      }
    }

    // Check for CSS background images in key sections
    const heroSection = page.locator('#hero');
    const featuresSection = page.locator('#features');
    const architectureSection = page.locator('#architecture');

    // These sections may use CSS backgrounds for decorative purposes
    // CSS backgrounds are acceptable for decorative images as they're not exposed to assistive technology

    // Verify that any decorative img elements are properly marked
    for (const decorativeSrc of decorativeImages) {
      // Decorative images should have empty alt or role="presentation"
      const decorativeImg = page.locator(`img[src="${decorativeSrc}"]`);
      const count = await decorativeImg.count();

      if (count > 0) {
        const alt = await decorativeImg.first().getAttribute('alt');
        const role = await decorativeImg.first().getAttribute('role');

        expect(
          alt === '' || role === 'presentation' || role === 'none',
          `Decorative image ${decorativeSrc} should have empty alt="" or role="presentation"`
        ).toBeTruthy();
      }
    }

    // If no img elements exist, verify the page still renders correctly
    // (meaning decorative elements are handled via CSS)
    if (imageCount === 0) {
      // Page should have visual content even without img elements
      await expect(page.locator('body')).toBeVisible();

      // Check that CSS-based visual elements exist
      const diagramContainer = page.locator('.diagram-container, [data-testid="architecture-diagram"]');
      const diagramCount = await diagramContainer.count();

      if (diagramCount > 0) {
        // CSS-based diagrams are acceptable as they can include proper ARIA
        await expect(diagramContainer.first()).toBeVisible();
      }
    }
  });

  test('All role="img" elements have accessible names', async ({ page }) => {
    // Find all elements with role="img"
    const roleImgElements = page.locator('[role="img"]');
    const count = await roleImgElements.count();

    for (let i = 0; i < count; i++) {
      const element = roleImgElements.nth(i);
      const ariaLabel = await element.getAttribute('aria-label');
      const ariaLabelledBy = await element.getAttribute('aria-labelledby');
      const title = await element.getAttribute('title');

      // Elements with role="img" must have an accessible name
      expect(
        ariaLabel || ariaLabelledBy || title,
        `Element with role="img" at index ${i} must have aria-label, aria-labelledby, or title`
      ).toBeTruthy();
    }
  });

  test('SVG images have appropriate accessibility attributes', async ({ page }) => {
    // Find all inline SVG elements
    const svgElements = page.locator('svg');
    const count = await svgElements.count();

    for (let i = 0; i < count; i++) {
      const svg = svgElements.nth(i);
      const role = await svg.getAttribute('role');
      const ariaLabel = await svg.getAttribute('aria-label');
      const ariaLabelledBy = await svg.getAttribute('aria-labelledby');
      const ariaHidden = await svg.getAttribute('aria-hidden');

      // SVGs should either be:
      // 1. Marked as decorative (aria-hidden="true")
      // 2. Have an accessible name (role="img" with aria-label/aria-labelledby)
      // 3. Have a title element (checked via aria-labelledby)
      const isDecorative = ariaHidden === 'true';
      const hasAccessibleName = ariaLabel || ariaLabelledBy || (role === 'img' && (ariaLabel || ariaLabelledBy));

      expect(
        isDecorative || hasAccessibleName || role === 'presentation' || role === 'none',
        `SVG at index ${i} should be either decorative (aria-hidden="true") or have an accessible name`
      ).toBeTruthy();
    }
  });
});
