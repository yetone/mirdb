// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Cross-Browser Compatibility Tests
 *
 * This test suite verifies the MirDB homepage works correctly across major browsers:
 * - Chrome (Chromium)
 * - Firefox
 * - Safari (WebKit)
 * - Edge (Chromium-based, shares engine with Chrome)
 *
 * Each browser test verifies:
 * 1. Page loads successfully without errors
 * 2. All main sections are visible and rendered correctly
 * 3. Interactive elements (links, navigation) work properly
 * 4. No JavaScript console errors occur
 * 5. CSS styling is applied correctly
 */

// Helper function to collect console errors during page load
async function collectConsoleErrors(page) {
  const errors = [];
  page.on('console', msg => {
    if (msg.type() === 'error') {
      errors.push(msg.text());
    }
  });
  page.on('pageerror', err => {
    errors.push(err.message);
  });
  return errors;
}

// Test suite for comprehensive cross-browser feature verification
test.describe('Cross-Browser Compatibility - Feature Verification', () => {

  test.beforeEach(async ({ page }) => {
    // Navigate to the homepage before each test
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  test('page loads successfully and has correct title', async ({ page }) => {
    await expect(page).toHaveTitle(/MirDB/);
  });

  test('hero section renders correctly', async ({ page }) => {
    // Check hero section elements
    const hero = page.locator('#hero');
    await expect(hero).toBeVisible();

    // Check logo
    const logo = page.locator('[data-testid="logo"]');
    await expect(logo).toBeVisible();

    // Check main heading
    const h1 = page.locator('h1');
    await expect(h1).toContainText('MirDB');

    // Check tagline
    const tagline = page.locator('.tagline');
    await expect(tagline).toContainText('Persistent Key-Value Store');

    // Check CTA buttons
    const ctaButtons = page.locator('.cta-buttons .btn');
    await expect(ctaButtons).toHaveCount(2);
  });

  test('features section renders all feature items', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Check all 6 features are present
    const featureItems = page.locator('.feature-item');
    await expect(featureItems).toHaveCount(6);

    // Verify specific features by data attribute
    const features = [
      'memcached-protocol',
      'persistence',
      'lsm-tree',
      'async-io',
      'configurable',
      'caching'
    ];

    for (const feature of features) {
      const featureItem = page.locator(`[data-feature="${feature}"]`);
      await expect(featureItem).toBeVisible();
    }
  });

  test('architecture diagram renders correctly', async ({ page }) => {
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Check SVG diagram is present
    const diagram = page.locator('[data-testid="architecture-svg"]');
    await expect(diagram).toBeVisible();

    // Verify diagram components
    const components = [
      'write-request',
      'wal',
      'memtable',
      'imm-memtable',
      'level0-sst',
      'sstable',
      'storage'
    ];

    for (const component of components) {
      const node = page.locator(`[data-component="${component}"]`);
      await expect(node).toBeVisible();
    }
  });

  test('getting started section renders code examples', async ({ page }) => {
    const gettingStarted = page.locator('#getting-started');
    await expect(gettingStarted).toBeVisible();

    // Check code blocks are present
    const codeBlocks = gettingStarted.locator('.code-block');
    await expect(codeBlocks.first()).toBeVisible();

    // Verify configuration example is present
    const configCode = gettingStarted.locator('code:has-text("addr")');
    await expect(configCode).toBeVisible();
  });

  test('configuration table renders correctly', async ({ page }) => {
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Check table exists
    const table = page.locator('.config-table');
    await expect(table).toBeVisible();

    // Verify table has header and body rows
    const headerRows = table.locator('thead tr');
    await expect(headerRows).toHaveCount(1);

    const bodyRows = table.locator('tbody tr');
    const count = await bodyRows.count();
    expect(count).toBeGreaterThanOrEqual(7);
  });

  test('footer renders with all required links', async ({ page }) => {
    const footer = page.locator('[data-testid="footer"]');
    await expect(footer).toBeVisible();

    // Check footer links
    const footerLinks = page.locator('[data-testid="footer-links"] a');
    const linkCount = await footerLinks.count();
    expect(linkCount).toBeGreaterThanOrEqual(6);

    // Verify specific important links
    const docsLink = page.locator('[data-testid="footer-docs-link"]');
    await expect(docsLink).toBeVisible();

    const githubLink = page.locator('[data-testid="footer-github-link"]');
    await expect(githubLink).toBeVisible();

    const licenseLink = page.locator('[data-testid="footer-license-link"]');
    await expect(licenseLink).toBeVisible();
  });

  test('navigation links work correctly', async ({ page }) => {
    // Test internal navigation links
    const navLinks = page.locator('.header-nav a[href^="#"]');
    const linkCount = await navLinks.count();

    for (let i = 0; i < linkCount; i++) {
      const link = navLinks.nth(i);
      const href = await link.getAttribute('href');

      // Click the link
      await link.click();

      // Verify the target section exists and is in viewport (with some tolerance for animation)
      const targetId = href.replace('#', '');
      const targetSection = page.locator(`#${targetId}`);
      await expect(targetSection).toBeVisible();
    }
  });

  test('skip link for accessibility is present', async ({ page }) => {
    const skipLink = page.locator('.skip-link');
    await expect(skipLink).toHaveAttribute('href', '#main-content');

    // Skip link should have proper text
    await expect(skipLink).toContainText('Skip to main content');
  });

  test('CSS styles are applied correctly', async ({ page }) => {
    // Check hero has a background applied (gradient or solid color)
    const hero = page.locator('#hero');
    const heroBackgroundImage = await hero.evaluate(el =>
      window.getComputedStyle(el).backgroundImage
    );
    // Hero should have a background gradient applied (linear-gradient)
    expect(heroBackgroundImage).toMatch(/linear-gradient|rgb/);

    // Check feature items have styling
    const featureItem = page.locator('.feature-item').first();
    const featureItemPadding = await featureItem.evaluate(el =>
      window.getComputedStyle(el).padding
    );
    // Feature items should have padding
    expect(featureItemPadding).not.toBe('0px');

    // Check code blocks have monospace font
    const codeBlock = page.locator('.code-block code').first();
    const fontFamily = await codeBlock.evaluate(el =>
      window.getComputedStyle(el).fontFamily
    );
    // Should contain monospace font
    expect(fontFamily.toLowerCase()).toMatch(/mono|courier/);
  });

  test('images load correctly', async ({ page }) => {
    // Check logo image loads
    const logo = page.locator('[data-testid="logo"]');
    const logoSrc = await logo.getAttribute('src');
    expect(logoSrc).toBeTruthy();

    // Wait for image to load by checking natural dimensions
    const logoLoaded = await logo.evaluate((img) => {
      const imgElement = img;
      return imgElement.complete && imgElement.naturalHeight !== 0;
    });
    expect(logoLoaded).toBe(true);
  });

  test('external links have proper attributes', async ({ page }) => {
    // Check that external links have target="_blank" and rel="noopener"
    const externalLinks = page.locator('a[href^="https://"]');
    const count = await externalLinks.count();

    for (let i = 0; i < count; i++) {
      const link = externalLinks.nth(i);
      await expect(link).toHaveAttribute('target', '_blank');
      await expect(link).toHaveAttribute('rel', /noopener/);
    }
  });
});

// Test suite for console error detection
test.describe('Cross-Browser Compatibility - Console Error Detection', () => {

  test('no JavaScript console errors on page load', async ({ page }) => {
    const errors = [];

    // Collect console errors
    page.on('console', msg => {
      if (msg.type() === 'error') {
        errors.push(msg.text());
      }
    });

    page.on('pageerror', err => {
      errors.push(err.message);
    });

    // Navigate to page
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Allow some time for any delayed errors
    await page.waitForTimeout(1000);

    // Assert no errors occurred
    expect(errors).toHaveLength(0);
  });

  test('no JavaScript errors during user interactions', async ({ page }) => {
    const errors = [];

    page.on('console', msg => {
      if (msg.type() === 'error') {
        errors.push(msg.text());
      }
    });

    page.on('pageerror', err => {
      errors.push(err.message);
    });

    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Perform various interactions
    // Click navigation links
    const navLinks = page.locator('.header-nav a[href^="#"]');
    const navCount = await navLinks.count();
    for (let i = 0; i < navCount; i++) {
      await navLinks.nth(i).click();
      await page.waitForTimeout(100);
    }

    // Scroll through the page
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
    await page.waitForTimeout(500);
    await page.evaluate(() => window.scrollTo(0, 0));
    await page.waitForTimeout(500);

    // Assert no errors occurred
    expect(errors).toHaveLength(0);
  });
});

// Test suite for page performance and load times
test.describe('Cross-Browser Compatibility - Performance', () => {

  test('page loads within acceptable time', async ({ page }) => {
    const startTime = Date.now();

    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    const loadTime = Date.now() - startTime;

    // Page should load within 3 seconds (3000ms) per NFR-2
    expect(loadTime).toBeLessThan(3000);
  });

  test('page resources load successfully', async ({ page }) => {
    const failedResources = [];

    page.on('requestfailed', request => {
      failedResources.push({
        url: request.url(),
        failure: request.failure()?.errorText
      });
    });

    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // No resources should fail to load
    expect(failedResources).toHaveLength(0);
  });
});

// Test suite for responsive design across browsers
test.describe('Cross-Browser Compatibility - Responsive Design', () => {

  test('page displays correctly at desktop viewport', async ({ page }) => {
    await page.setViewportSize({ width: 1920, height: 1080 });
    await page.goto('/');

    // Check hero section is visible
    const hero = page.locator('#hero');
    await expect(hero).toBeVisible();

    // Features grid should be in row layout at desktop
    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();
  });

  test('page displays correctly at tablet viewport', async ({ page }) => {
    await page.setViewportSize({ width: 768, height: 1024 });
    await page.goto('/');

    // Content should still be visible
    const hero = page.locator('#hero');
    await expect(hero).toBeVisible();

    const features = page.locator('#features');
    await expect(features).toBeVisible();
  });

  test('page displays correctly at mobile viewport', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');

    // Content should be visible without horizontal scroll
    const hero = page.locator('#hero');
    await expect(hero).toBeVisible();

    // Check there's no horizontal overflow
    const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
    const viewportWidth = await page.evaluate(() => window.innerWidth);

    // Body should not be wider than viewport (allowing small tolerance)
    expect(bodyWidth).toBeLessThanOrEqual(viewportWidth + 5);
  });
});
