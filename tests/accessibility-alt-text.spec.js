// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = 'file://' + path.resolve(__dirname, '../index.html');

test.describe('Accessibility - Image Alt Text (NFR-3)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(indexPath);
  });

  // Test Case 1: Check logo image for alt attribute
  // Input: Check logo image for alt attribute
  // Expected: Logo image has descriptive alt text (e.g., 'MirDB Logo')
  test('TC1: Logo image has descriptive alt text', async ({ page }) => {
    // Locate the logo image by its ID
    const logo = page.locator('#logo');

    // Verify the logo is visible
    await expect(logo).toBeVisible();

    // Verify the logo is an img element
    const tagName = await logo.evaluate(el => el.tagName.toLowerCase());
    expect(tagName).toBe('img');

    // Verify the alt attribute exists and is not empty
    const altText = await logo.getAttribute('alt');
    expect(altText).toBeTruthy();
    expect(altText.length).toBeGreaterThan(0);

    // Verify the alt text is descriptive (contains 'MirDB' or 'Logo')
    expect(altText.toLowerCase()).toMatch(/mirdb|logo/i);

    // Verify the specific expected alt text
    expect(altText).toBe('MirDB Logo');
  });

  // Test Case 2: Check all img elements for alt attributes
  // Input: Check all img elements for alt attributes
  // Expected: All images have alt attribute (meaningful or empty for decorative)
  test('TC2: All images have alt attributes (meaningful or empty for decorative)', async ({ page }) => {
    // Get all img elements on the page
    const images = page.locator('img');
    const imageCount = await images.count();

    // Verify there are images on the page
    expect(imageCount).toBeGreaterThan(0);

    // Check each image for alt attribute
    for (let i = 0; i < imageCount; i++) {
      const image = images.nth(i);
      const src = await image.getAttribute('src');
      const alt = await image.getAttribute('alt');

      // Alt attribute must exist (can be empty for decorative images)
      expect(alt).not.toBeNull();

      // If the image appears to be functional (not decorative), alt should be meaningful
      // Decorative images can have alt="" but not null/undefined
      if (alt !== '') {
        // Meaningful alt text should have some content
        expect(alt.length).toBeGreaterThan(0);
      }

      // Log for debugging purposes
      console.log(`Image ${i + 1}: src="${src}", alt="${alt}"`);
    }
  });

  // Test Case 3: Verify feature icons have appropriate alt text
  // Input: Verify feature icons have appropriate alt text
  // Expected: Feature icons have descriptive alt or are marked as decorative
  test('TC3: Feature icons are appropriately handled for accessibility', async ({ page }) => {
    // Get all feature cards
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();

    // Verify there are feature cards
    expect(cardCount).toBeGreaterThan(0);

    // Check each feature card
    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i);

      // Check for the feature icon element
      const featureIcon = card.locator('.feature-icon');
      const iconExists = await featureIcon.count() > 0;

      if (iconExists) {
        // Feature icons in this implementation use Unicode emoji characters
        // inside div elements (not img tags), which is accessible
        const iconContent = await featureIcon.textContent();
        expect(iconContent.length).toBeGreaterThan(0);

        // Check if there are any img elements within the feature icon
        const imgInIcon = card.locator('.feature-icon img');
        const imgCount = await imgInIcon.count();

        // If there are img elements, they should have alt attributes
        for (let j = 0; j < imgCount; j++) {
          const img = imgInIcon.nth(j);
          const alt = await img.getAttribute('alt');

          // Alt must exist (can be empty for purely decorative icons)
          expect(alt).not.toBeNull();
        }
      }

      // Verify each feature card has an accessible heading
      const heading = card.locator('h3');
      await expect(heading).toBeVisible();
      const headingText = await heading.textContent();
      expect(headingText.length).toBeGreaterThan(0);
    }
  });

  // Additional test: Verify specific images have appropriate alt text content
  test('All specific images have meaningful and descriptive alt text', async ({ page }) => {
    // Test the demo GIF
    const demoGif = page.locator('.demo-gif');
    const demoAlt = await demoGif.getAttribute('alt');
    expect(demoAlt).toBeTruthy();
    expect(demoAlt).toContain('MirDB');
    expect(demoAlt.toLowerCase()).toMatch(/usage|demonstration|demo/i);

    // Test the GitHub stars badge
    const footerLinks = page.locator('.footer-links');
    const footerImages = footerLinks.locator('img');
    const footerImageCount = await footerImages.count();

    expect(footerImageCount).toBeGreaterThan(0);

    // Check each footer image (badges)
    for (let i = 0; i < footerImageCount; i++) {
      const img = footerImages.nth(i);
      const alt = await img.getAttribute('alt');
      const src = await img.getAttribute('src');

      // Alt should be descriptive for badges
      expect(alt).toBeTruthy();
      expect(alt.length).toBeGreaterThan(0);

      // GitHub badge should mention stars or GitHub
      if (src.includes('github')) {
        expect(alt.toLowerCase()).toMatch(/github|star/i);
      }

      // CircleCI badge should mention build or CI
      if (src.includes('circleci')) {
        expect(alt.toLowerCase()).toMatch(/circleci|build|status/i);
      }
    }
  });

  // Additional test: Verify no images have generic or placeholder alt text
  test('No images have generic or placeholder alt text', async ({ page }) => {
    const images = page.locator('img');
    const imageCount = await images.count();

    // List of generic/placeholder alt texts that should not be used
    const genericAltTexts = [
      'image',
      'img',
      'photo',
      'picture',
      'icon',
      'placeholder',
      'untitled',
      'unknown',
      'null',
      'undefined'
    ];

    for (let i = 0; i < imageCount; i++) {
      const image = images.nth(i);
      const alt = await image.getAttribute('alt');

      // If alt is not empty, it should not be a generic placeholder
      if (alt && alt.trim() !== '') {
        const lowerAlt = alt.toLowerCase().trim();

        for (const generic of genericAltTexts) {
          expect(lowerAlt).not.toBe(generic);
        }
      }
    }
  });

  // Additional test: Verify alt text doesn't start with redundant phrases
  test('Alt text does not start with redundant phrases', async ({ page }) => {
    const images = page.locator('img');
    const imageCount = await images.count();

    // Phrases that are redundant in alt text
    const redundantPhrases = [
      'image of',
      'picture of',
      'photo of',
      'graphic of',
      'icon of'
    ];

    for (let i = 0; i < imageCount; i++) {
      const image = images.nth(i);
      const alt = await image.getAttribute('alt');

      if (alt && alt.trim() !== '') {
        const lowerAlt = alt.toLowerCase().trim();

        for (const phrase of redundantPhrases) {
          expect(lowerAlt.startsWith(phrase)).toBe(false);
        }
      }
    }
  });
});
