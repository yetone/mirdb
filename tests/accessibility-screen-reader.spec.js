// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Accessibility - Screen Reader Compatibility', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Page has exactly one h1 element (product name)', async ({ page }) => {
    // Check that there is exactly one h1 element on the page
    const h1Elements = page.locator('h1');
    const h1Count = await h1Elements.count();
    expect(h1Count).toBe(1);

    // Verify the h1 contains the product name
    const h1Text = await h1Elements.first().textContent();
    expect(h1Text).toContain('MirDB');
  });

  test('TC2: Headings follow logical order (h1 -> h2 -> h3) without skipping levels', async ({ page }) => {
    // Get all heading elements in document order
    const headings = page.locator('h1, h2, h3, h4, h5, h6');
    const headingCount = await headings.count();

    expect(headingCount).toBeGreaterThan(0);

    // Track heading levels to ensure no skipping
    let previousLevel = 0;
    const headingLevels = [];

    for (let i = 0; i < headingCount; i++) {
      const heading = headings.nth(i);
      const tagName = await heading.evaluate(el => el.tagName.toLowerCase());
      const level = parseInt(tagName.replace('h', ''), 10);
      headingLevels.push({ level, tagName });
    }

    // Verify heading hierarchy doesn't skip levels
    // First heading should be h1
    expect(headingLevels[0].level).toBe(1);

    // Each subsequent heading should not skip more than one level going deeper
    for (let i = 1; i < headingLevels.length; i++) {
      const current = headingLevels[i].level;
      const previous = headingLevels[i - 1].level;

      // When going to a deeper level (higher number), should not skip
      // e.g., h1 -> h3 is invalid (skips h2), but h1 -> h2 -> h3 is valid
      // Going back up to a higher level is always allowed (h3 -> h1)
      if (current > previous) {
        // We're going deeper, ensure we don't skip more than 1 level
        const skipAmount = current - previous;
        expect(skipAmount).toBeLessThanOrEqual(1);
      }
    }
  });

  test('TC3: Logo has descriptive alt text', async ({ page }) => {
    // Find the logo image
    const logo = page.locator('img.logo, img[alt*="logo" i], img[alt*="MirDB" i], .hero img').first();
    await expect(logo).toBeVisible();

    // Check that alt attribute exists and is descriptive
    const altText = await logo.getAttribute('alt');
    expect(altText).toBeTruthy();
    expect(altText.length).toBeGreaterThan(0);

    // Verify alt text is meaningful (contains either "MirDB" or "logo")
    const meaningfulAlt = altText.toLowerCase().includes('mirdb') ||
                          altText.toLowerCase().includes('logo');
    expect(meaningfulAlt).toBe(true);
  });

  test('TC4: Feature icons have appropriate alt text or are decorative', async ({ page }) => {
    // Check the features section icons
    const featureSection = page.locator('[data-testid="features"], .features, #features').first();
    await expect(featureSection).toBeVisible();

    // Get all icons in the features section (SVG or img elements)
    const featureCards = page.locator('.feature-card, [data-testid="feature-card"]');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThan(0);

    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i);

      // Check for SVG icons (should have aria-hidden="true" or aria-label for decorative/meaningful)
      const svgIcon = card.locator('svg').first();
      const svgExists = await svgIcon.count() > 0;

      if (svgExists) {
        // Decorative icons should be hidden from screen readers
        // OR have appropriate aria-label if meaningful
        const ariaHidden = await svgIcon.getAttribute('aria-hidden');
        const ariaLabel = await svgIcon.getAttribute('aria-label');
        const role = await svgIcon.getAttribute('role');

        // Icon is acceptable if:
        // 1. It has aria-hidden="true" (decorative)
        // 2. It has an aria-label (meaningful)
        // 3. It has role="img" with aria-label
        // 4. The parent has descriptive text (h3) which provides context
        const hasH3Context = await card.locator('h3').count() > 0;

        const isAccessible = ariaHidden === 'true' ||
                            (ariaLabel && ariaLabel.length > 0) ||
                            hasH3Context;
        expect(isAccessible).toBe(true);
      }

      // Check for img icons
      const imgIcon = card.locator('img').first();
      const imgExists = await imgIcon.count() > 0;

      if (imgExists) {
        const altText = await imgIcon.getAttribute('alt');
        // Alt text should exist (can be empty string for decorative)
        expect(altText !== null).toBe(true);
      }
    }
  });

  test('TC5: Buttons have accessible names via text content or aria-label', async ({ page }) => {
    // Get all interactive button elements (buttons and links styled as buttons)
    const buttons = page.locator('button, a.btn, a[class*="btn-"], [role="button"]');
    const buttonCount = await buttons.count();

    expect(buttonCount).toBeGreaterThan(0);

    for (let i = 0; i < buttonCount; i++) {
      const button = buttons.nth(i);

      // Get accessible name through various methods
      const textContent = await button.textContent();
      const ariaLabel = await button.getAttribute('aria-label');
      const ariaLabelledBy = await button.getAttribute('aria-labelledby');
      const title = await button.getAttribute('title');

      // Button should have an accessible name via one of these methods:
      // 1. Text content (not empty after trimming)
      // 2. aria-label attribute
      // 3. aria-labelledby reference
      // 4. title attribute (less preferred but acceptable)
      const hasAccessibleName =
        (textContent && textContent.trim().length > 0) ||
        (ariaLabel && ariaLabel.length > 0) ||
        (ariaLabelledBy && ariaLabelledBy.length > 0) ||
        (title && title.length > 0);

      expect(hasAccessibleName).toBe(true);
    }
  });

  test('Verify all images have alt attributes', async ({ page }) => {
    // Get all images on the page
    const images = page.locator('img');
    const imageCount = await images.count();

    for (let i = 0; i < imageCount; i++) {
      const img = images.nth(i);
      const src = await img.getAttribute('src');
      const altAttr = await img.getAttribute('alt');

      // All images must have alt attribute (can be empty for decorative)
      expect(altAttr !== null).toBe(true);
    }
  });

  test('Interactive elements are focusable', async ({ page }) => {
    // Check that main interactive elements can receive focus
    const interactiveElements = page.locator('a[href], button, [tabindex]:not([tabindex="-1"])');
    const count = await interactiveElements.count();

    expect(count).toBeGreaterThan(0);

    // Sample a few elements to verify they can be focused
    const sampleSize = Math.min(count, 5);
    for (let i = 0; i < sampleSize; i++) {
      const element = interactiveElements.nth(i);
      await element.focus();
      await expect(element).toBeFocused();
    }
  });

  test('Page has proper language attribute', async ({ page }) => {
    // Check that html element has lang attribute for screen readers
    const htmlLang = await page.locator('html').getAttribute('lang');
    expect(htmlLang).toBeTruthy();
    expect(htmlLang.length).toBeGreaterThan(0);
  });
});
