/**
 * Asset Loading E2E Tests
 * Owner: Scenario 17 - Asset Loading - Logo and GIF
 *
 * Tests for verifying logo and usage GIF assets load correctly in the browser,
 * have proper alt text for accessibility, and handle failures gracefully.
 */

const { test, expect } = require('@playwright/test');

test.describe('Asset Loading - Logo and GIF', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for page to fully load
    await page.waitForLoadState('networkidle');
  });

  test.describe('TC1: Logo displays in hero section', () => {
    test('logo from /assets/logo.gif displays in hero section', async ({ page }) => {
      // Find the hero section
      const heroSection = page.locator('#hero');
      await expect(heroSection).toBeVisible();

      // Find the hero logo image
      const heroLogo = heroSection.locator('img.hero-logo');
      await expect(heroLogo).toBeVisible();

      // Verify the src attribute points to logo.gif
      const src = await heroLogo.getAttribute('src');
      expect(src).toContain('logo.gif');
    });

    test('logo image loads successfully (not broken)', async ({ page }) => {
      const heroLogo = page.locator('#hero img.hero-logo');
      await expect(heroLogo).toBeVisible();

      // Check that the image actually loaded (naturalWidth > 0 means it loaded)
      const isLoaded = await heroLogo.evaluate((img) => {
        return img.complete && img.naturalWidth > 0;
      });
      expect(isLoaded).toBe(true);
    });

    test('logo has proper dimensions', async ({ page }) => {
      const heroLogo = page.locator('#hero img.hero-logo');
      await expect(heroLogo).toBeVisible();

      // Get the bounding box to verify visible size
      const box = await heroLogo.boundingBox();
      expect(box.width).toBeGreaterThan(0);
      expect(box.height).toBeGreaterThan(0);
    });
  });

  test.describe('TC2: Usage GIF displays in Usage section', () => {
    test('usage GIF from /assets/usage.gif displays in Usage section', async ({ page }) => {
      // Scroll to usage section to trigger lazy loading
      const usageSection = page.locator('#usage');
      await usageSection.scrollIntoViewIfNeeded();
      await expect(usageSection).toBeVisible();

      // Find the usage GIF image
      const usageGif = usageSection.locator('img.usage__gif');
      await expect(usageGif).toBeVisible();

      // Verify the src attribute points to usage.gif
      const src = await usageGif.getAttribute('src');
      expect(src).toContain('usage.gif');
    });

    test('usage GIF image loads successfully (not broken)', async ({ page }) => {
      // Scroll to usage section first
      const usageSection = page.locator('#usage');
      await usageSection.scrollIntoViewIfNeeded();

      const usageGif = page.locator('#usage img.usage__gif');
      await expect(usageGif).toBeVisible();

      // Wait a moment for lazy-loaded image to complete loading
      await page.waitForTimeout(500);

      // Check that the image actually loaded
      const isLoaded = await usageGif.evaluate((img) => {
        return img.complete && img.naturalWidth > 0;
      });
      expect(isLoaded).toBe(true);
    });

    test('usage GIF is in proper container', async ({ page }) => {
      const usageSection = page.locator('#usage');
      await usageSection.scrollIntoViewIfNeeded();

      // Check that the GIF is inside the demo container
      const demoContainer = usageSection.locator('.usage__demo-container');
      await expect(demoContainer).toBeVisible();

      const usageGif = demoContainer.locator('img.usage__gif');
      await expect(usageGif).toBeVisible();
    });
  });

  test.describe('TC3: Alt text accessibility', () => {
    test('hero logo has meaningful alt text', async ({ page }) => {
      const heroLogo = page.locator('#hero img.hero-logo');
      const altText = await heroLogo.getAttribute('alt');

      expect(altText).not.toBeNull();
      expect(altText.length).toBeGreaterThan(0);
      // Alt text should contain meaningful description related to the logo
      expect(altText.toLowerCase()).toMatch(/logo|mirdb/i);
    });

    test('usage GIF has meaningful alt text', async ({ page }) => {
      const usageSection = page.locator('#usage');
      await usageSection.scrollIntoViewIfNeeded();

      const usageGif = usageSection.locator('img.usage__gif');
      const altText = await usageGif.getAttribute('alt');

      expect(altText).not.toBeNull();
      expect(altText.length).toBeGreaterThan(10); // Should be descriptive
      // Alt text should describe the demonstration
      expect(altText.toLowerCase()).toMatch(/terminal|demo|usage|command|mirdb/i);
    });

    test('navigation logo has meaningful alt text', async ({ page }) => {
      const navLogo = page.locator('nav img');
      const altText = await navLogo.getAttribute('alt');

      expect(altText).not.toBeNull();
      expect(altText.length).toBeGreaterThan(0);
    });

    test('footer logo has meaningful alt text', async ({ page }) => {
      // Scroll to footer
      const footer = page.locator('footer');
      await footer.scrollIntoViewIfNeeded();

      const footerLogo = footer.locator('img');
      const altText = await footerLogo.getAttribute('alt');

      expect(altText).not.toBeNull();
      expect(altText.length).toBeGreaterThan(0);
    });
  });

  test.describe('TC4: Fallback behavior when images fail', () => {
    test('page layout remains intact when logo fails to load', async ({ page }) => {
      // Block logo image requests
      await page.route('**/logo.gif', (route) => {
        route.abort();
      });

      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Hero section should still be visible and properly laid out
      const heroSection = page.locator('#hero');
      await expect(heroSection).toBeVisible();

      // Hero title should still be visible
      const heroTitle = heroSection.locator('h1');
      await expect(heroTitle).toBeVisible();
      await expect(heroTitle).toContainText('MirDB');

      // CTA buttons should still be visible
      const ctaButtons = heroSection.locator('.btn');
      await expect(ctaButtons.first()).toBeVisible();
    });

    test('alt text is displayed when logo fails to load', async ({ page }) => {
      // Block logo image requests
      await page.route('**/logo.gif', (route) => {
        route.abort();
      });

      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // The hero logo element should still exist with alt attribute
      const heroLogo = page.locator('#hero img.hero-logo');
      await expect(heroLogo).toHaveAttribute('alt');

      // Alt text should be meaningful
      const altText = await heroLogo.getAttribute('alt');
      expect(altText.length).toBeGreaterThan(0);
    });

    test('page layout remains intact when usage GIF fails to load', async ({ page }) => {
      // Block usage GIF requests
      await page.route('**/usage.gif', (route) => {
        route.abort();
      });

      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Scroll to usage section
      const usageSection = page.locator('#usage');
      await usageSection.scrollIntoViewIfNeeded();

      // Usage section should still be visible and properly laid out
      await expect(usageSection).toBeVisible();

      // Section title should still be visible
      const usageTitle = usageSection.locator('h2');
      await expect(usageTitle).toBeVisible();
      await expect(usageTitle).toContainText('Simple as Memcached');

      // Code blocks should still be visible
      const codeBlocks = usageSection.locator('.code-block');
      await expect(codeBlocks.first()).toBeVisible();
    });

    test('alt text is displayed when usage GIF fails to load', async ({ page }) => {
      // Block usage GIF requests
      await page.route('**/usage.gif', (route) => {
        route.abort();
      });

      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Scroll to usage section
      const usageSection = page.locator('#usage');
      await usageSection.scrollIntoViewIfNeeded();

      // The usage GIF element should still exist with alt attribute
      const usageGif = usageSection.locator('img.usage__gif');
      await expect(usageGif).toHaveAttribute('alt');

      // Alt text should be meaningful
      const altText = await usageGif.getAttribute('alt');
      expect(altText.length).toBeGreaterThan(10);
    });

    test('navigation still works when images fail', async ({ page }) => {
      // Block all image requests
      await page.route('**/*.gif', (route) => {
        route.abort();
      });

      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Main navigation should still be visible (use specific selector for main nav)
      const mainNav = page.locator('nav#main-nav');
      await expect(mainNav).toBeVisible();

      // Navigation links should still work
      const featuresLink = mainNav.locator('a[href="#features"]');
      await expect(featuresLink).toBeVisible();

      // Click on features link and verify scroll
      await featuresLink.click();

      // Features section should be visible after click
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeInViewport();
    });

    test('all sections remain accessible when images blocked', async ({ page }) => {
      // Block all image requests
      await page.route('**/*.gif', (route) => {
        route.abort();
      });

      await page.goto('/');
      await page.waitForLoadState('domcontentloaded');

      // Verify all major sections are still visible and accessible
      const sections = ['#hero', '#features', '#usage', '#architecture', '#getting-started'];

      for (const sectionId of sections) {
        const section = page.locator(sectionId);
        await section.scrollIntoViewIfNeeded();
        await expect(section).toBeVisible();
      }

      // Footer should also be visible
      const footer = page.locator('footer');
      await footer.scrollIntoViewIfNeeded();
      await expect(footer).toBeVisible();
    });
  });

  test.describe('Performance considerations', () => {
    test('usage GIF has lazy loading attribute', async ({ page }) => {
      const usageGif = page.locator('#usage img.usage__gif');
      const loading = await usageGif.getAttribute('loading');
      expect(loading).toBe('lazy');
    });

    test('hero logo does not have lazy loading (above fold)', async ({ page }) => {
      const heroLogo = page.locator('#hero img.hero-logo');
      const loading = await heroLogo.getAttribute('loading');
      // Hero logo should not have lazy loading since it's above the fold
      expect(loading).not.toBe('lazy');
    });
  });
});
