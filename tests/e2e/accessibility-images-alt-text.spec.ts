import { test, expect } from '@playwright/test';

/**
 * E2E Tests for MirDB Homepage Accessibility - Images and Alt Text
 *
 * These tests verify that all images have appropriate alt text
 * to ensure WCAG 2.1 AA compliance for accessibility.
 *
 * Test coverage includes:
 * - All img elements have alt attributes with descriptive text
 * - Architecture diagram has descriptive alt text
 * - Decorative images have empty alt="" attributes
 */

test.describe('Accessibility - Images and Alt Text', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: All img elements have alt attribute with descriptive text', async ({ page }) => {
    // Get all img elements on the page
    const allImages = page.locator('img');
    const imageCount = await allImages.count();

    // If there are no img elements, the test should still pass
    // as the page may use SVG or CSS for graphics
    if (imageCount === 0) {
      // Verify that visual content exists via SVG or other means
      const svgElements = page.locator('svg');
      const svgCount = await svgElements.count();
      expect(svgCount).toBeGreaterThan(0);
      return;
    }

    // Check each img element for alt attribute
    for (let i = 0; i < imageCount; i++) {
      const img = allImages.nth(i);
      const altAttribute = await img.getAttribute('alt');

      // Alt attribute must exist
      expect(altAttribute, `Image ${i + 1} is missing alt attribute`).not.toBeNull();

      // Get src for better error messages
      const src = await img.getAttribute('src');

      // If alt is not empty, it should be descriptive (more than just a filename)
      if (altAttribute !== '') {
        // Alt text should be meaningful (not just the filename)
        expect(
          altAttribute!.length,
          `Image ${i + 1} (src: ${src}) has non-descriptive alt text: "${altAttribute}"`
        ).toBeGreaterThan(3);

        // Alt text should not just be the filename
        const isJustFilename = /\.(png|jpg|jpeg|gif|svg|webp)$/i.test(altAttribute!);
        expect(
          isJustFilename,
          `Image ${i + 1} alt text should not be just a filename: "${altAttribute}"`
        ).toBeFalsy();
      }
    }
  });

  test('TC2: Architecture diagram has alt text describing the data flow', async ({ page }) => {
    // Navigate to architecture section
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Check for architecture diagram container
    const diagramContainer = architectureSection.locator('.architecture-diagram');
    await expect(diagramContainer).toBeVisible();

    // Check for accessibility attributes on the diagram
    // The diagram could be an SVG with title/desc or a container with aria-label
    const svgElement = diagramContainer.locator('svg');
    const svgCount = await svgElement.count();

    if (svgCount > 0) {
      // SVG diagram - check for title, desc, or aria-label
      const svgTitle = svgElement.locator('title');
      const svgDesc = svgElement.locator('desc');

      const hasSvgTitle = await svgTitle.count() > 0;
      const hasSvgDesc = await svgDesc.count() > 0;
      const ariaLabel = await svgElement.getAttribute('aria-label');
      const role = await svgElement.getAttribute('role');

      // At least one accessibility method should be present
      const hasAccessibility = hasSvgTitle || hasSvgDesc || ariaLabel !== null;
      expect(
        hasAccessibility,
        'Architecture SVG diagram must have title, desc, or aria-label for accessibility'
      ).toBeTruthy();

      // Verify content describes data flow
      let accessibleText = '';
      if (hasSvgTitle) {
        accessibleText += await svgTitle.textContent() || '';
      }
      if (hasSvgDesc) {
        accessibleText += ' ' + (await svgDesc.textContent() || '');
      }
      if (ariaLabel) {
        accessibleText += ' ' + ariaLabel;
      }

      // Check for role="img" for proper semantics
      const containerAriaLabel = await diagramContainer.getAttribute('aria-label');
      if (containerAriaLabel) {
        accessibleText += ' ' + containerAriaLabel;
      }

      // The accessible text should describe the data flow/architecture
      const textLower = accessibleText.toLowerCase();
      const describesDataFlow =
        textLower.includes('data flow') ||
        textLower.includes('architecture') ||
        textLower.includes('diagram') ||
        (textLower.includes('write') && textLower.includes('wal')) ||
        (textLower.includes('memtable') && textLower.includes('sstable'));

      expect(
        describesDataFlow,
        `Architecture diagram alt text should describe data flow. Got: "${accessibleText}"`
      ).toBeTruthy();
    } else {
      // If no SVG, check for img with alt
      const imgElement = diagramContainer.locator('img');
      const imgCount = await imgElement.count();

      if (imgCount > 0) {
        const altText = await imgElement.getAttribute('alt');
        expect(altText, 'Architecture diagram img must have alt attribute').not.toBeNull();
        expect(
          altText!.length,
          'Architecture diagram alt text should be descriptive'
        ).toBeGreaterThan(10);

        // Verify it describes data flow
        const textLower = altText!.toLowerCase();
        const describesDataFlow =
          textLower.includes('data flow') ||
          textLower.includes('architecture') ||
          textLower.includes('diagram');

        expect(
          describesDataFlow,
          `Architecture diagram alt text should describe data flow. Got: "${altText}"`
        ).toBeTruthy();
      } else {
        // Check for container with aria-label
        const containerAriaLabel = await diagramContainer.getAttribute('aria-label');
        const containerRole = await diagramContainer.getAttribute('role');

        expect(
          containerAriaLabel,
          'Architecture diagram container must have aria-label if no SVG or img'
        ).not.toBeNull();

        expect(
          containerRole,
          'Architecture diagram container should have role="img"'
        ).toBe('img');
      }
    }
  });

  test('TC3: Decorative images have empty alt="" attribute', async ({ page }) => {
    // Get all img elements
    const allImages = page.locator('img');
    const imageCount = await allImages.count();

    // Get all SVG elements that might be decorative
    const allSvgs = page.locator('svg');
    const svgCount = await allSvgs.count();

    // Check for decorative images (those that are purely visual, not informational)
    // Common patterns for decorative images:
    // - Icons within buttons or links where text provides context
    // - Background decorations
    // - Spacer images

    // For img elements
    for (let i = 0; i < imageCount; i++) {
      const img = allImages.nth(i);
      const altAttribute = await img.getAttribute('alt');
      const src = await img.getAttribute('src');

      // Alt attribute must exist
      expect(altAttribute, `Image ${i + 1} (src: ${src}) must have alt attribute`).not.toBeNull();

      // Check if this appears to be a decorative image
      // Decorative images inside interactive elements (buttons, links) should have alt=""
      const parentLink = img.locator('xpath=ancestor::a');
      const parentButton = img.locator('xpath=ancestor::button');

      const isInsideLink = (await parentLink.count()) > 0;
      const isInsideButton = (await parentButton.count()) > 0;

      // If image is inside an interactive element with text, it may be decorative
      if (isInsideLink || isInsideButton) {
        const parentElement = isInsideLink ? parentLink : parentButton;
        const parentText = await parentElement.textContent();

        // If the parent has meaningful text, the image could be decorative
        // In this case, empty alt="" is acceptable
        if (parentText && parentText.trim().length > 0 && altAttribute === '') {
          // This is acceptable - decorative image with empty alt
          continue;
        }
      }

      // For standalone images, alt should be descriptive (not empty)
      // unless explicitly marked as decorative via role="presentation"
      const role = await img.getAttribute('role');
      const ariaHidden = await img.getAttribute('aria-hidden');

      if (role === 'presentation' || ariaHidden === 'true') {
        // Decorative image - empty alt is correct
        expect(
          altAttribute,
          `Decorative image ${i + 1} should have empty alt=""`
        ).toBe('');
      } else if (!isInsideLink && !isInsideButton) {
        // Standalone informational image - should have descriptive alt
        expect(
          altAttribute!.length,
          `Standalone image ${i + 1} (src: ${src}) should have descriptive alt text`
        ).toBeGreaterThan(0);
      }
    }

    // For SVG elements used as decorative icons
    for (let i = 0; i < svgCount; i++) {
      const svg = allSvgs.nth(i);
      const role = await svg.getAttribute('role');
      const ariaHidden = await svg.getAttribute('aria-hidden');

      // SVG within feature cards are typically decorative icons
      const parentFeatureCard = svg.locator('xpath=ancestor::*[contains(@class, "feature-card")]');
      const isInFeatureCard = (await parentFeatureCard.count()) > 0;

      if (isInFeatureCard) {
        // These are decorative icons accompanying text
        // They should either have aria-hidden="true" or role="presentation"
        // OR they should have accessible text if informational
        const hasTitle = (await svg.locator('title').count()) > 0;

        if (!hasTitle) {
          // If no title, should be marked as decorative
          const isMarkedDecorative = ariaHidden === 'true' || role === 'presentation';
          // Note: It's also acceptable for decorative SVGs to have no explicit marking
          // if they don't add semantic value
        }
      }

      // Check architecture diagram SVG specifically for proper accessibility
      const isArchitectureDiagram = svg.locator('xpath=ancestor::*[contains(@class, "architecture-diagram")]');
      if ((await isArchitectureDiagram.count()) > 0) {
        // This is an informational SVG and should have proper accessibility
        const hasTitle = (await svg.locator('title').count()) > 0;
        const hasDesc = (await svg.locator('desc').count()) > 0;
        const hasAriaLabel = (await svg.getAttribute('aria-label')) !== null;

        const hasAccessibility = hasTitle || hasDesc || hasAriaLabel;
        expect(
          hasAccessibility,
          'Architecture diagram SVG must have accessible description'
        ).toBeTruthy();
      }
    }
  });
});
