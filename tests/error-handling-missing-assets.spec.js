// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Error Handling - Missing Assets
 *
 * These tests verify that the MirDB homepage gracefully handles missing image assets
 * by ensuring alt text displays properly and the layout doesn't break when images
 * fail to load due to network issues or missing files.
 */

test.describe('Error Handling - Missing Assets', () => {

  test.describe('Test Case 1: Missing logo.gif', () => {

    test('should display alt text gracefully when logo image fails to load', async ({ page }) => {
      // Block the logo-placeholder.svg request to simulate missing asset
      await page.route('**/assets/logo-placeholder.svg', route => route.abort('failed'));

      await page.goto('/');

      // The hero logo should still be present in the DOM
      const heroLogo = page.locator('.hero-logo');
      await expect(heroLogo).toBeVisible();

      // Verify alt text is present and meaningful
      const altText = await heroLogo.getAttribute('alt');
      expect(altText).toBeTruthy();
      expect(altText.toLowerCase()).toContain('mirdb');
    });

    test('should maintain layout integrity when logo fails to load', async ({ page }) => {
      // Block logo asset
      await page.route('**/assets/logo-placeholder.svg', route => route.abort('failed'));

      await page.goto('/');

      // Hero section should still be visible and properly structured
      const heroSection = page.locator('.hero');
      await expect(heroSection).toBeVisible();

      // Hero title should remain visible
      const heroTitle = page.locator('.hero h1');
      await expect(heroTitle).toBeVisible();
      await expect(heroTitle).toHaveText('MirDB');

      // Tagline should remain visible
      const tagline = page.locator('.hero-tagline');
      await expect(tagline).toBeVisible();

      // CTA buttons should remain functional
      const primaryCta = page.locator('.hero-ctas .btn-primary');
      await expect(primaryCta).toBeVisible();
      await expect(primaryCta).toHaveAttribute('href', '#quick-start');

      const secondaryCta = page.locator('.hero-ctas .btn-secondary');
      await expect(secondaryCta).toBeVisible();
    });

    test('should not cause overflow or layout shifts when logo fails to load', async ({ page }) => {
      // Block logo asset
      await page.route('**/assets/logo-placeholder.svg', route => route.abort('failed'));

      await page.goto('/');

      // Check that body doesn't have horizontal overflow
      const bodyOverflow = await page.evaluate(() => {
        const body = document.body;
        return body.scrollWidth <= window.innerWidth;
      });
      expect(bodyOverflow).toBe(true);

      // Hero section should have proper dimensions
      const heroBox = await page.locator('.hero').boundingBox();
      expect(heroBox).not.toBeNull();
      expect(heroBox.width).toBeGreaterThan(0);
      expect(heroBox.height).toBeGreaterThan(0);
    });

    test('should have proper dimensions defined for logo to prevent layout shift', async ({ page }) => {
      await page.route('**/assets/logo-placeholder.svg', route => route.abort('failed'));

      await page.goto('/');

      const heroLogo = page.locator('.hero-logo');

      // Check that width and height attributes are present
      const width = await heroLogo.getAttribute('width');
      const height = await heroLogo.getAttribute('height');

      expect(width).toBeTruthy();
      expect(height).toBeTruthy();
    });
  });

  test.describe('Test Case 2: Missing usage.gif', () => {

    test('should display alt text when usage demo gif fails to load', async ({ page }) => {
      // Block the usage.gif request
      await page.route('**/assets/usage.gif', route => route.abort('failed'));

      await page.goto('/');

      // The demo gif should still be in the DOM
      const demoGif = page.locator('.demo-gif');
      await expect(demoGif).toBeVisible();

      // Verify alt text is present and descriptive
      const altText = await demoGif.getAttribute('alt');
      expect(altText).toBeTruthy();
      expect(altText.toLowerCase()).toContain('demonstration');
    });

    test('should keep demo section usable when usage gif fails to load', async ({ page }) => {
      // Block the usage.gif request
      await page.route('**/assets/usage.gif', route => route.abort('failed'));

      await page.goto('/');

      // Demo section should still be visible
      const demoSection = page.locator('#demo');
      await expect(demoSection).toBeVisible();

      // Section title should be visible
      const demoTitle = page.locator('#demo-title');
      await expect(demoTitle).toBeVisible();
      await expect(demoTitle).toHaveText('See It in Action');

      // Demo intro text should be visible
      const demoIntro = page.locator('.demo-intro');
      await expect(demoIntro).toBeVisible();
    });

    test('should maintain section layout when usage gif fails to load', async ({ page }) => {
      // Block usage.gif
      await page.route('**/assets/usage.gif', route => route.abort('failed'));

      await page.goto('/');

      // Demo section should have proper dimensions
      const demoBox = await page.locator('#demo').boundingBox();
      expect(demoBox).not.toBeNull();
      expect(demoBox.width).toBeGreaterThan(0);
      expect(demoBox.height).toBeGreaterThan(50); // Should have some minimum height
    });

    test('should have dimensions defined for usage gif to prevent layout shift', async ({ page }) => {
      await page.route('**/assets/usage.gif', route => route.abort('failed'));

      await page.goto('/');

      const demoGif = page.locator('.demo-gif');

      // Check that width and height attributes are present
      const width = await demoGif.getAttribute('width');
      const height = await demoGif.getAttribute('height');

      expect(width).toBeTruthy();
      expect(height).toBeTruthy();
    });
  });

  test.describe('Test Case 3: Missing CI badge image', () => {

    test('should fail gracefully when CircleCI badge fails to load', async ({ page }) => {
      // Block the CircleCI badge request
      await page.route('**/circleci.com/**', route => route.abort('failed'));

      await page.goto('/');

      // The badge images should still be in the DOM
      const badges = page.locator('img[alt*="CircleCI"]');
      const badgeCount = await badges.count();
      expect(badgeCount).toBeGreaterThan(0);

      // Each badge should have proper alt text
      for (let i = 0; i < badgeCount; i++) {
        const badge = badges.nth(i);
        const altText = await badge.getAttribute('alt');
        expect(altText).toBeTruthy();
        expect(altText.toLowerCase()).toContain('circleci');
      }
    });

    test('should not break layout when CI badge fails to load', async ({ page }) => {
      // Block CircleCI badge
      await page.route('**/circleci.com/**', route => route.abort('failed'));

      await page.goto('/');

      // Hero badge section should not cause layout issues
      const heroBadge = page.locator('.hero-badge');
      await expect(heroBadge).toBeVisible();

      const heroBadgeBox = await heroBadge.boundingBox();
      expect(heroBadgeBox).not.toBeNull();
      // Badge container should have reasonable dimensions
      expect(heroBadgeBox.width).toBeGreaterThan(0);
    });

    test('should maintain footer layout when CI badge fails to load', async ({ page }) => {
      // Block CircleCI badge
      await page.route('**/circleci.com/**', route => route.abort('failed'));

      await page.goto('/');

      // Footer should still be properly laid out
      const footer = page.locator('.footer');
      await expect(footer).toBeVisible();

      // Footer links should be visible
      const footerLinks = page.locator('.footer-links');
      await expect(footerLinks).toBeVisible();

      // GitHub link in footer should still work
      const githubLink = page.locator('.footer-links a[href*="github.com"]');
      await expect(githubLink).toBeVisible();
      await expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
    });

    test('should keep badge links functional when image fails', async ({ page }) => {
      // Block CircleCI badge image
      await page.route('**/circleci.com/**svg**', route => route.abort('failed'));

      await page.goto('/');

      // The badge links should still be functional
      const badgeLink = page.locator('.hero-badge a');
      await expect(badgeLink).toBeVisible();
      await expect(badgeLink).toHaveAttribute('href', 'https://circleci.com/gh/yetone/mirdb');
    });

    test('should have dimensions defined for CI badges to prevent layout shift', async ({ page }) => {
      await page.route('**/circleci.com/**', route => route.abort('failed'));

      await page.goto('/');

      // Check hero badge
      const heroBadgeImg = page.locator('.hero-badge img');
      const heroWidth = await heroBadgeImg.getAttribute('width');
      const heroHeight = await heroBadgeImg.getAttribute('height');

      expect(heroWidth).toBeTruthy();
      expect(heroHeight).toBeTruthy();

      // Check footer badge
      const footerBadgeImg = page.locator('.footer-links img');
      const footerWidth = await footerBadgeImg.getAttribute('width');
      const footerHeight = await footerBadgeImg.getAttribute('height');

      expect(footerWidth).toBeTruthy();
      expect(footerHeight).toBeTruthy();
    });
  });

  test.describe('Multiple Assets Missing', () => {

    test('should handle all images failing gracefully', async ({ page }) => {
      // Block all image assets
      await page.route('**/assets/**', route => route.abort('failed'));
      await page.route('**/circleci.com/**', route => route.abort('failed'));

      await page.goto('/');

      // Page should still be functional
      const main = page.locator('main');
      await expect(main).toBeVisible();

      // All sections should be visible
      await expect(page.locator('.hero')).toBeVisible();
      await expect(page.locator('#features')).toBeVisible();
      await expect(page.locator('#demo')).toBeVisible();
      await expect(page.locator('#quick-start')).toBeVisible();
      await expect(page.locator('.footer')).toBeVisible();
    });

    test('should maintain navigation functionality when assets fail', async ({ page }) => {
      // Block all assets
      await page.route('**/assets/**', route => route.abort('failed'));
      await page.route('**/circleci.com/**', route => route.abort('failed'));

      await page.goto('/');

      // Navigation should still work
      const navLinks = page.locator('.nav-links a');
      const navCount = await navLinks.count();
      expect(navCount).toBeGreaterThan(0);

      // Internal links should work
      const featuresLink = page.locator('.nav-links a[href="#features"]');
      await expect(featuresLink).toBeVisible();

      // Skip link should work
      const skipLink = page.locator('.skip-link');
      await expect(skipLink).toHaveAttribute('href', '#main-content');
    });

    test('should keep page scrollable when all images fail', async ({ page }) => {
      // Block all assets
      await page.route('**/assets/**', route => route.abort('failed'));
      await page.route('**/circleci.com/**', route => route.abort('failed'));

      await page.goto('/');

      // Page should be scrollable
      const scrollHeight = await page.evaluate(() => document.body.scrollHeight);
      const viewportHeight = await page.evaluate(() => window.innerHeight);

      // Page content should extend beyond viewport (indicating scrollable content)
      expect(scrollHeight).toBeGreaterThan(viewportHeight);
    });
  });

  test.describe('Accessibility with Missing Assets', () => {

    test('should maintain accessible alt text when images fail', async ({ page }) => {
      // Block all assets
      await page.route('**/assets/**', route => route.abort('failed'));
      await page.route('**/circleci.com/**', route => route.abort('failed'));

      await page.goto('/');

      // All images should have alt text
      const images = page.locator('img');
      const imgCount = await images.count();

      for (let i = 0; i < imgCount; i++) {
        const img = images.nth(i);
        const altText = await img.getAttribute('alt');
        expect(altText).toBeTruthy();
        expect(altText.length).toBeGreaterThan(0);
      }
    });

    test('should keep ARIA landmarks functional with missing assets', async ({ page }) => {
      // Block all assets
      await page.route('**/assets/**', route => route.abort('failed'));
      await page.route('**/circleci.com/**', route => route.abort('failed'));

      await page.goto('/');

      // Main content landmark should be accessible
      const mainContent = page.locator('#main-content');
      await expect(mainContent).toBeVisible();

      // Navigation should be properly labeled
      const nav = page.locator('nav[role="navigation"]');
      await expect(nav).toHaveAttribute('aria-label');
    });
  });
});
