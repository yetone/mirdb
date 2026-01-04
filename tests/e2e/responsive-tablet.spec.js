// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Test suite for tablet responsive design (NFR-1)
 * Verifies page renders correctly on tablet devices at various viewports
 */

test.describe('Responsive Design - Tablet', () => {

  test.describe('Tablet Portrait (768x1024)', () => {
    test.use({ viewport: { width: 768, height: 1024 } });

    test('page adapts to tablet portrait mode with appropriate layout', async ({ page }) => {
      await page.goto('/');

      // Page should be visible and not have horizontal overflow
      const body = page.locator('body');
      await expect(body).toBeVisible();

      // Check viewport width is respected
      const bodyBox = await body.boundingBox();
      expect(bodyBox.width).toBeLessThanOrEqual(768);

      // Hero section should be visible and properly sized
      const hero = page.locator('.hero');
      await expect(hero).toBeVisible();
      const heroBox = await hero.boundingBox();
      expect(heroBox.width).toBeLessThanOrEqual(768);

      // Hero title should be visible
      const heroTitle = page.locator('.hero h1');
      await expect(heroTitle).toBeVisible();
      await expect(heroTitle).toContainText('MirDB');

      // Tagline should be visible
      const tagline = page.locator('.hero .tagline');
      await expect(tagline).toBeVisible();

      // CTA buttons should be visible and accessible
      const ctaButtons = page.locator('.cta-buttons .cta');
      await expect(ctaButtons.first()).toBeVisible();

      // Navigation should be present
      const navbar = page.locator('.navbar');
      await expect(navbar).toBeVisible();

      // Features section should be visible
      const features = page.locator('#features');
      await expect(features).toBeVisible();

      // Getting started section should be visible
      const gettingStarted = page.locator('#getting-started');
      await expect(gettingStarted).toBeVisible();

      // Footer should be visible
      const footer = page.locator('footer');
      await expect(footer).toBeVisible();

      // No horizontal scrollbar (content fits within viewport)
      const scrollWidth = await page.evaluate(() => document.documentElement.scrollWidth);
      const clientWidth = await page.evaluate(() => document.documentElement.clientWidth);
      expect(scrollWidth).toBeLessThanOrEqual(clientWidth + 1); // Allow 1px tolerance
    });
  });

  test.describe('Tablet Landscape (1024x768)', () => {
    test.use({ viewport: { width: 1024, height: 768 } });

    test('page adapts to tablet landscape mode with appropriate layout', async ({ page }) => {
      await page.goto('/');

      // Page should be visible
      const body = page.locator('body');
      await expect(body).toBeVisible();

      // Check viewport width is respected
      const bodyBox = await body.boundingBox();
      expect(bodyBox.width).toBeLessThanOrEqual(1024);

      // Hero section should be visible and properly sized
      const hero = page.locator('.hero');
      await expect(hero).toBeVisible();
      const heroBox = await hero.boundingBox();
      expect(heroBox.width).toBeLessThanOrEqual(1024);

      // Hero content should be readable
      const heroTitle = page.locator('.hero h1');
      await expect(heroTitle).toBeVisible();
      await expect(heroTitle).toContainText('MirDB');

      // Description text should be visible
      const description = page.locator('.hero .description');
      await expect(description).toBeVisible();

      // Navigation links should be visible in landscape
      const navLinks = page.locator('.nav-links');
      await expect(navLinks).toBeVisible();

      // Features section should be present
      const features = page.locator('#features');
      await expect(features).toBeVisible();

      // Commands section should be visible
      const commands = page.locator('.commands');
      await expect(commands).toBeVisible();

      // No horizontal scrollbar
      const scrollWidth = await page.evaluate(() => document.documentElement.scrollWidth);
      const clientWidth = await page.evaluate(() => document.documentElement.clientWidth);
      expect(scrollWidth).toBeLessThanOrEqual(clientWidth + 1);
    });
  });

  test.describe('Feature Cards Layout', () => {

    test('feature cards reflow to appropriate layout on tablet portrait (768px)', async ({ page }) => {
      await page.setViewportSize({ width: 768, height: 1024 });
      await page.goto('/');

      // Feature grid should be visible
      const featureGrid = page.locator('.feature-grid');
      await expect(featureGrid).toBeVisible();

      // Get all feature cards
      const featureCards = page.locator('.feature-card');
      const cardCount = await featureCards.count();
      expect(cardCount).toBe(4);

      // All cards should be visible
      for (let i = 0; i < cardCount; i++) {
        await expect(featureCards.nth(i)).toBeVisible();
      }

      // Cards should use grid layout (check they have reasonable widths)
      // At 768px with minmax(250px, 1fr), we expect 2 columns
      const firstCardBox = await featureCards.first().boundingBox();
      const secondCardBox = await featureCards.nth(1).boundingBox();

      // Cards should have appropriate width for tablet
      // With 2rem padding on each side (32px each) and 2rem gap, max card width ~340px
      expect(firstCardBox.width).toBeGreaterThan(200);
      expect(firstCardBox.width).toBeLessThan(400);

      // Check if cards are in 2-column layout (second card on same row)
      // or stacked layout (second card below first)
      const isTwoColumn = Math.abs(firstCardBox.y - secondCardBox.y) < 10;
      const isStacked = secondCardBox.y > firstCardBox.y + firstCardBox.height - 10;

      // Either layout is acceptable for tablet
      expect(isTwoColumn || isStacked).toBeTruthy();
    });

    test('feature cards reflow to appropriate layout on tablet landscape (1024px)', async ({ page }) => {
      await page.setViewportSize({ width: 1024, height: 768 });
      await page.goto('/');

      // Feature grid should be visible
      const featureGrid = page.locator('.feature-grid');
      await expect(featureGrid).toBeVisible();

      // Get all feature cards
      const featureCards = page.locator('.feature-card');
      const cardCount = await featureCards.count();
      expect(cardCount).toBe(4);

      // All cards should be visible
      for (let i = 0; i < cardCount; i++) {
        await expect(featureCards.nth(i)).toBeVisible();
      }

      // At 1024px, we might get 3 or even 4 columns depending on container padding
      const firstCardBox = await featureCards.first().boundingBox();
      const secondCardBox = await featureCards.nth(1).boundingBox();

      // Cards should have reasonable width
      expect(firstCardBox.width).toBeGreaterThan(200);
      expect(firstCardBox.width).toBeLessThan(500);

      // In landscape, cards should be in a row (2, 3, or 4 column layout)
      // Second card should be on the same row as first
      const isSameRow = Math.abs(firstCardBox.y - secondCardBox.y) < 10;
      expect(isSameRow).toBeTruthy();
    });

    test('feature cards content remains readable at tablet sizes', async ({ page }) => {
      await page.setViewportSize({ width: 768, height: 1024 });
      await page.goto('/');

      const featureCards = page.locator('.feature-card');

      // Check each card has visible heading and description
      const cardTitles = [
        'Memcached Compatibility',
        'Persistent Storage',
        'LSM Tree Architecture',
        'Built with Rust'
      ];

      for (let i = 0; i < cardTitles.length; i++) {
        const card = featureCards.nth(i);
        const heading = card.locator('h3');
        const paragraph = card.locator('p');

        await expect(heading).toBeVisible();
        await expect(heading).toContainText(cardTitles[i]);
        await expect(paragraph).toBeVisible();
      }
    });
  });
});
