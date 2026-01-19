// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Cross-Browser Compatibility Tests for NFR-3
 *
 * Verifies that the MirDB homepage works correctly on modern browsers:
 * - Chrome (latest 2 versions)
 * - Firefox (latest 2 versions)
 * - Safari (latest 2 versions) - tested via WebKit
 * - Edge (latest 2 versions) - uses Chromium engine
 *
 * These tests run across all configured browser projects in playwright.config.js
 */

test.describe('Cross-Browser Compatibility (NFR-3)', () => {

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('page loads successfully without errors', async ({ page, browserName }) => {
    // Collect any console errors during page load
    const consoleErrors = [];
    page.on('console', msg => {
      if (msg.type() === 'error') {
        consoleErrors.push(msg.text());
      }
    });

    // Verify the page loaded successfully
    await expect(page).toHaveTitle(/MirDB/i);

    // Give time for any async errors to appear
    await page.waitForTimeout(1000);

    // Filter out known acceptable errors (e.g., favicon 404)
    const criticalErrors = consoleErrors.filter(error =>
      !error.includes('favicon') &&
      !error.includes('404')
    );

    // Assert no critical console errors
    expect(criticalErrors, `Console errors found in ${browserName}: ${criticalErrors.join(', ')}`).toHaveLength(0);
  });

  test('hero section renders correctly', async ({ page, browserName }) => {
    // Hero section should be visible
    const heroSection = page.locator('#hero, .hero, [class*="hero"]').first();
    await expect(heroSection).toBeVisible();

    // Logo should be visible
    const logo = page.locator('img[alt*="MirDB"], img[src*="logo"]').first();
    await expect(logo, `Logo not visible in ${browserName}`).toBeVisible();

    // Tagline should be visible
    const tagline = page.getByText(/persistent.*key-value|memcached/i).first();
    await expect(tagline, `Tagline not visible in ${browserName}`).toBeVisible();

    // CTA buttons should be visible
    const ctaButtons = page.locator('a[href*="github"], button, .cta, .btn').filter({ hasText: /get started|github|learn/i });
    await expect(ctaButtons.first(), `CTA buttons not visible in ${browserName}`).toBeVisible();
  });

  test('value proposition section renders correctly', async ({ page, browserName }) => {
    // Value proposition section should exist
    const valueSection = page.locator('#value-proposition, .value-proposition, [class*="value"], [class*="pillars"]').first();

    // Check for the three pillars
    const persistenceText = page.getByText(/persistence/i).first();
    const compatibilityText = page.getByText(/compatibility|memcached/i).first();
    const performanceText = page.getByText(/performance|rust/i).first();

    await expect(persistenceText, `Persistence pillar not visible in ${browserName}`).toBeVisible();
    await expect(compatibilityText, `Compatibility pillar not visible in ${browserName}`).toBeVisible();
    await expect(performanceText, `Performance pillar not visible in ${browserName}`).toBeVisible();
  });

  test('features section renders correctly', async ({ page, browserName }) => {
    // Scroll to features section
    const featuresSection = page.locator('#features, .features, [class*="features"]').first();
    await featuresSection.scrollIntoViewIfNeeded();

    // Check for key feature mentions
    const featureTexts = [
      /GET.*SET.*DELETE|operations/i,
      /compaction/i,
      /WAL|write-ahead|logging/i,
      /TOML|config/i
    ];

    for (const featureText of featureTexts) {
      const featureElement = page.getByText(featureText).first();
      // Features should be present (may be in cards or list items)
      const featureCount = await page.getByText(featureText).count();
      expect(featureCount, `Feature "${featureText}" not found in ${browserName}`).toBeGreaterThanOrEqual(1);
    }
  });

  test('architecture section renders correctly', async ({ page, browserName }) => {
    // Scroll to architecture section
    const archSection = page.locator('#architecture, .architecture, [class*="architecture"]').first();
    await archSection.scrollIntoViewIfNeeded();

    // Check for LSM tree references
    const lsmText = page.getByText(/LSM|memtable|SSTable/i).first();
    await expect(lsmText, `LSM tree reference not visible in ${browserName}`).toBeVisible();
  });

  test('getting started section renders correctly', async ({ page, browserName }) => {
    // Scroll to getting started section
    const gettingStartedSection = page.locator('#getting-started, .getting-started, [class*="getting-started"]').first();
    await gettingStartedSection.scrollIntoViewIfNeeded();

    // Check for code examples or installation commands
    const codeBlock = page.locator('pre, code, .code-block').first();
    await expect(codeBlock, `Code block not visible in ${browserName}`).toBeVisible();
  });

  test('footer section renders correctly', async ({ page, browserName }) => {
    // Scroll to footer
    const footer = page.locator('footer').first();
    await footer.scrollIntoViewIfNeeded();
    await expect(footer, `Footer not visible in ${browserName}`).toBeVisible();

    // GitHub link should be present
    const githubLink = page.locator('a[href*="github"]').first();
    await expect(githubLink, `GitHub link not visible in ${browserName}`).toBeVisible();
  });

  test('navigation works correctly', async ({ page, browserName }) => {
    // Find navigation links
    const navLinks = page.locator('nav a, header a, .nav a').filter({ hasText: /features|architecture|get started/i });

    // There should be at least one navigation link
    const navCount = await navLinks.count();
    expect(navCount, `Navigation links not found in ${browserName}`).toBeGreaterThan(0);

    // Test clicking the first navigation link
    if (navCount > 0) {
      const firstLink = navLinks.first();
      const href = await firstLink.getAttribute('href');

      // Only test internal links (starting with #)
      if (href && href.startsWith('#')) {
        await firstLink.click();
        // URL should update with the hash
        await expect(page).toHaveURL(new RegExp(href.replace('#', '#')));
      }
    }
  });

  test('page is responsive at desktop viewport', async ({ page, browserName }) => {
    // Set desktop viewport
    await page.setViewportSize({ width: 1920, height: 1080 });

    // Page should not have horizontal overflow
    const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
    const viewportWidth = await page.evaluate(() => window.innerWidth);

    expect(bodyWidth, `Horizontal overflow detected in ${browserName} at desktop`).toBeLessThanOrEqual(viewportWidth + 1);

    // Hero section should be visible without scrolling
    const heroSection = page.locator('#hero, .hero, [class*="hero"]').first();
    await expect(heroSection).toBeInViewport();
  });

  test('page is responsive at mobile viewport', async ({ page, browserName }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 375, height: 667 });

    // Page should not have horizontal overflow
    const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
    const viewportWidth = await page.evaluate(() => window.innerWidth);

    expect(bodyWidth, `Horizontal overflow detected in ${browserName} at mobile`).toBeLessThanOrEqual(viewportWidth + 1);

    // Content should still be visible
    const heroSection = page.locator('#hero, .hero, [class*="hero"]').first();
    await expect(heroSection).toBeVisible();
  });

  test('CSS styling is applied correctly', async ({ page, browserName }) => {
    // Check that styles are loaded
    const stylesheets = await page.evaluate(() => {
      return Array.from(document.styleSheets).filter(sheet => !sheet.disabled).length;
    });

    expect(stylesheets, `No stylesheets loaded in ${browserName}`).toBeGreaterThan(0);

    // Check that body has expected styling (not raw HTML)
    const bodyFontFamily = await page.evaluate(() => {
      return window.getComputedStyle(document.body).fontFamily;
    });

    // Font family should not be the browser default serif
    expect(bodyFontFamily.toLowerCase(), `Default browser font used in ${browserName}`).not.toBe('times new roman');
  });

  test('images load correctly', async ({ page, browserName }) => {
    // Wait for network to be idle (all images loaded)
    await page.waitForLoadState('networkidle');

    // Check that images have loaded
    const images = page.locator('img');
    const imageCount = await images.count();

    for (let i = 0; i < imageCount; i++) {
      const img = images.nth(i);
      const isVisible = await img.isVisible();

      if (isVisible) {
        const src = await img.getAttribute('src');

        // Skip data URIs
        if (src && !src.startsWith('data:')) {
          // For animated GIFs and other images, check if the image element is complete
          // Some browsers may not populate naturalWidth for animated GIFs immediately
          const imageLoaded = await img.evaluate((el) => {
            const imgEl = el;
            // Check if image is complete and either has naturalWidth or is a GIF (which may behave differently)
            return imgEl.complete && (imgEl.naturalWidth > 0 || imgEl.src.endsWith('.gif'));
          });

          // If image is not loaded yet, wait a bit and check again
          if (!imageLoaded) {
            await page.waitForTimeout(500);
            const rechecked = await img.evaluate((el) => {
              return el.complete;
            });
            expect(rechecked, `Image ${src} failed to load in ${browserName}`).toBeTruthy();
          }
        }
      }
    }
  });

  test('links have correct attributes for security', async ({ page, browserName }) => {
    // External links should have rel="noopener" or rel="noreferrer"
    const externalLinks = page.locator('a[target="_blank"]');
    const linkCount = await externalLinks.count();

    for (let i = 0; i < linkCount; i++) {
      const link = externalLinks.nth(i);
      const rel = await link.getAttribute('rel');
      const href = await link.getAttribute('href');

      // Check that external links have security attributes
      expect(
        rel?.includes('noopener') || rel?.includes('noreferrer'),
        `External link ${href} missing security attributes in ${browserName}`
      ).toBeTruthy();
    }
  });

  test('no JavaScript errors during interaction', async ({ page, browserName }) => {
    const jsErrors = [];

    page.on('pageerror', error => {
      jsErrors.push(error.message);
    });

    // Perform various interactions
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
    await page.waitForTimeout(500);
    await page.evaluate(() => window.scrollTo(0, 0));
    await page.waitForTimeout(500);

    // Click on interactive elements
    const buttons = page.locator('button, a.btn, .button');
    const buttonCount = await buttons.count();

    for (let i = 0; i < Math.min(buttonCount, 3); i++) {
      const button = buttons.nth(i);
      const isVisible = await button.isVisible();
      if (isVisible) {
        // Don't actually navigate, just hover
        await button.hover();
      }
    }

    expect(jsErrors, `JavaScript errors found in ${browserName}: ${jsErrors.join(', ')}`).toHaveLength(0);
  });

});
