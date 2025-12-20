/**
 * E2E Tests for Image Loading Failures - Error Handling
 * Scenario: Error Handling - Image Loading Failures
 * Test Case 1: Block image loading and check page rendering
 * Validates that page remains functional with alt text displayed for images when images fail to load
 */

const { test, expect } = require('@playwright/test');
const path = require('path');

test.describe('Error Handling - Image Loading Failures', () => {
  const htmlPath = `file://${path.resolve(__dirname, '../../index.html')}`;

  /**
   * Test Case 1: Block image loading and check page rendering
   * Expected: Page remains functional with alt text displayed for images
   */
  test.describe('Test Case 1: Page Functionality with Blocked Images', () => {
    test('page should remain fully functional when images fail to load', async ({ page }) => {
      // Block all image requests to simulate image loading failures
      await page.route('**/*.{png,jpg,jpeg,gif,webp,svg}', route => route.abort());

      // Navigate to the page
      await page.goto(htmlPath);

      // Wait for page to be fully loaded
      await page.waitForLoadState('domcontentloaded');

      // Verify page title is correct
      const title = await page.title();
      expect(title).toContain('MirDB');

      // Verify main content sections are present and visible
      const hero = await page.locator('.hero').first();
      await expect(hero).toBeVisible();

      // Check h1 heading
      const h1 = await page.locator('h1').first();
      await expect(h1).toContainText('MirDB');
    });

    test('navigation should be fully functional without images', async ({ page }) => {
      // Block all image requests
      await page.route('**/*.{png,jpg,jpeg,gif,webp}', route => route.abort());

      await page.goto(htmlPath);
      await page.waitForLoadState('domcontentloaded');

      // Check navigation links are accessible
      const navLinks = await page.locator('nav a');
      const count = await navLinks.count();

      expect(count).toBeGreaterThan(0);

      // Each nav link should have visible text
      for (let i = 0; i < count; i++) {
        const link = navLinks.nth(i);
        await expect(link).toBeVisible();
        const text = await link.textContent();
        expect(text.trim().length).toBeGreaterThan(0);
      }
    });

    test('CTA buttons should remain clickable when images fail', async ({ page }) => {
      // Block all image requests
      await page.route('**/*.{png,jpg,jpeg,gif,webp}', route => route.abort());

      await page.goto(htmlPath);
      await page.waitForLoadState('domcontentloaded');

      // Check primary CTA button
      const ctaButtons = await page.locator('.btn, a.btn-primary, a.btn-secondary');
      const buttonCount = await ctaButtons.count();

      expect(buttonCount).toBeGreaterThan(0);

      // Verify buttons are visible and have text
      for (let i = 0; i < buttonCount; i++) {
        const btn = ctaButtons.nth(i);
        await expect(btn).toBeVisible();
      }
    });

    test('all content sections should remain visible without images', async ({ page }) => {
      // Block all image requests
      await page.route('**/*.{png,jpg,jpeg,gif,webp}', route => route.abort());

      await page.goto(htmlPath);
      await page.waitForLoadState('domcontentloaded');

      // Check for key sections
      const sections = await page.locator('section');
      const sectionCount = await sections.count();

      expect(sectionCount).toBeGreaterThan(0);

      // Each section should be visible
      for (let i = 0; i < sectionCount; i++) {
        const section = sections.nth(i);
        await expect(section).toBeVisible();
      }
    });
  });

  test.describe('Alt Text Accessibility with Failed Images', () => {
    test('SVG logos should maintain aria-label when displayed', async ({ page }) => {
      await page.goto(htmlPath);
      await page.waitForLoadState('domcontentloaded');

      // Check hero logo SVG has aria-label
      const heroLogo = await page.locator('svg.hero-logo');
      const count = await heroLogo.count();

      if (count > 0) {
        const ariaLabel = await heroLogo.getAttribute('aria-label');
        expect(ariaLabel).toBeTruthy();
        expect(ariaLabel.toLowerCase()).toContain('logo');
      }
    });

    test('all img elements should have alt attributes', async ({ page }) => {
      await page.goto(htmlPath);
      await page.waitForLoadState('domcontentloaded');

      // Get all img elements
      const images = await page.locator('img');
      const imgCount = await images.count();

      // Check each img has an alt attribute
      for (let i = 0; i < imgCount; i++) {
        const img = images.nth(i);
        const alt = await img.getAttribute('alt');
        expect(alt).not.toBeNull();
      }
    });
  });

  test.describe('Layout Stability with Missing Images', () => {
    test('page layout should not break when images fail to load', async ({ page }) => {
      // Block all image requests
      await page.route('**/*.{png,jpg,jpeg,gif,webp}', route => route.abort());

      await page.goto(htmlPath);
      await page.waitForLoadState('domcontentloaded');

      // Check that the page has no horizontal scroll overflow (broken layout indicator)
      const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
      const viewportWidth = await page.evaluate(() => window.innerWidth);

      // Page should not overflow horizontally
      expect(bodyWidth).toBeLessThanOrEqual(viewportWidth + 20); // Allow small margin

      // Footer should be present at bottom
      const footer = await page.locator('footer');
      await expect(footer).toBeVisible();
    });

    test('hero section should maintain structure without images', async ({ page }) => {
      // Block all image requests
      await page.route('**/*.{png,jpg,jpeg,gif,webp}', route => route.abort());

      await page.goto(htmlPath);
      await page.waitForLoadState('domcontentloaded');

      // Hero content should still be properly aligned
      const heroContent = await page.locator('.hero-content, .hero').first();
      await expect(heroContent).toBeVisible();

      // Check that hero h1 is visible
      const heroTitle = await page.locator('.hero h1');
      await expect(heroTitle).toBeVisible();

      // Check that tagline is visible
      const tagline = await page.locator('.hero-tagline');
      await expect(tagline).toBeVisible();
    });

    test('SVG images should remain visible (inline SVGs not affected by image blocking)', async ({ page }) => {
      // Block raster image requests (SVGs are inline, not blocked)
      await page.route('**/*.{png,jpg,jpeg,gif,webp}', route => route.abort());

      await page.goto(htmlPath);
      await page.waitForLoadState('domcontentloaded');

      // Inline SVGs should still be present
      const svgs = await page.locator('svg');
      const svgCount = await svgs.count();

      expect(svgCount).toBeGreaterThan(0);

      // Check hero logo SVG is visible
      const heroLogo = await page.locator('.hero-logo');
      await expect(heroLogo).toBeVisible();
    });
  });

  test.describe('Graceful Degradation Experience', () => {
    test('user can still understand page purpose when images fail', async ({ page }) => {
      // Block all images
      await page.route('**/*.{png,jpg,jpeg,gif,webp}', route => route.abort());

      await page.goto(htmlPath);
      await page.waitForLoadState('domcontentloaded');

      // Check that key information is conveyed through text
      const pageText = await page.locator('body').textContent();

      // Essential information should be available as text
      expect(pageText.toLowerCase()).toContain('mirdb');
      expect(pageText.toLowerCase()).toContain('persistent');
      expect(pageText.toLowerCase()).toContain('key-value');
      expect(pageText.toLowerCase()).toContain('memcached');
    });

    test('feature cards should be readable without icons', async ({ page }) => {
      // Block all images
      await page.route('**/*.{png,jpg,jpeg,gif,webp}', route => route.abort());

      await page.goto(htmlPath);
      await page.waitForLoadState('domcontentloaded');

      // Check feature cards exist and have text content
      const featureCards = await page.locator('.feature-card, [data-feature]');
      const cardCount = await featureCards.count();

      if (cardCount > 0) {
        for (let i = 0; i < cardCount; i++) {
          const card = featureCards.nth(i);
          await expect(card).toBeVisible();

          // Each card should have title and description text
          const cardText = await card.textContent();
          expect(cardText.trim().length).toBeGreaterThan(10);
        }
      }
    });
  });
});
