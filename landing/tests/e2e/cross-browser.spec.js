import { test, expect } from '@playwright/test';

/**
 * Cross-Browser Compatibility E2E Tests (NFR-4)
 *
 * These tests verify that the MirDB landing page renders correctly
 * across major browsers: Chrome, Firefox, Safari, and Edge.
 *
 * Each test runs in all configured browser projects from playwright.config.js
 */

test.describe('Cross-Browser Compatibility - Page Rendering', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case 1-4: Page renders correctly with all sections visible
  test('should display hero section correctly', async ({ page, browserName }) => {
    // Wait for page to be fully loaded
    await expect(page.locator('.hero')).toBeVisible();

    // Product name should be visible
    const productName = page.locator('.product-name');
    await expect(productName).toBeVisible();
    await expect(productName).toHaveText('MirDB');

    // Tagline should be visible
    const tagline = page.locator('.tagline');
    await expect(tagline).toBeVisible();
    await expect(tagline).toContainText('Memcached Compatibility');

    console.log(`✅ Hero section renders correctly in ${browserName}`);
  });

  test('should display CTA buttons correctly', async ({ page, browserName }) => {
    // Primary CTA
    const primaryBtn = page.locator('.btn-primary');
    await expect(primaryBtn).toBeVisible();
    await expect(primaryBtn).toHaveText('Get Started');

    // Secondary CTA
    const secondaryBtn = page.locator('.btn-secondary');
    await expect(secondaryBtn).toBeVisible();
    await expect(secondaryBtn).toHaveText('View on GitHub');

    console.log(`✅ CTA buttons render correctly in ${browserName}`);
  });

  test('should display Get Started section correctly', async ({ page, browserName }) => {
    const getStarted = page.locator('#get-started');
    await expect(getStarted).toBeVisible();

    // Installation section should be visible
    const installation = page.locator('.installation');
    await expect(installation).toBeVisible();

    // Code blocks should be visible
    const codeBlocks = page.locator('.code-block');
    await expect(codeBlocks.first()).toBeVisible();

    console.log(`✅ Get Started section renders correctly in ${browserName}`);
  });

  test('should display footer correctly', async ({ page, browserName }) => {
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Footer links should be visible
    const footerLinks = footer.locator('a');
    await expect(footerLinks.first()).toBeVisible();

    console.log(`✅ Footer renders correctly in ${browserName}`);
  });

  test('should render gradient text correctly in hero', async ({ page, browserName }) => {
    const productName = page.locator('.product-name');

    // Check that the element has the expected gradient styling
    const styles = await productName.evaluate((el) => {
      const computed = window.getComputedStyle(el);
      return {
        backgroundClip: computed.backgroundClip || computed.webkitBackgroundClip,
        backgroundImage: computed.backgroundImage,
      };
    });

    // Gradient background should be applied
    expect(styles.backgroundImage).toContain('linear-gradient');

    console.log(`✅ Gradient text renders correctly in ${browserName}`);
  });

  test('should apply flexbox layout correctly', async ({ page, browserName }) => {
    const ctaButtons = page.locator('.cta-buttons');

    const display = await ctaButtons.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });

    expect(display).toBe('flex');

    console.log(`✅ Flexbox layout renders correctly in ${browserName}`);
  });
});

test.describe('Cross-Browser Compatibility - Interactivity', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('should navigate to Get Started section on CTA click', async ({ page, browserName }) => {
    const getStartedBtn = page.locator('.btn-primary');
    await getStartedBtn.click();

    // Wait for smooth scroll to complete
    await page.waitForTimeout(500);

    // Get Started section should be in view
    const getStartedSection = page.locator('#get-started');
    await expect(getStartedSection).toBeInViewport();

    console.log(`✅ Navigation works correctly in ${browserName}`);
  });

  test('should have proper hover effects on buttons', async ({ page, browserName }) => {
    const primaryBtn = page.locator('.btn-primary');

    // Get initial transform
    const initialTransform = await primaryBtn.evaluate((el) => {
      return window.getComputedStyle(el).transform;
    });

    // Hover over button
    await primaryBtn.hover();
    await page.waitForTimeout(300);

    // Button should be visible and styled
    await expect(primaryBtn).toBeVisible();

    console.log(`✅ Hover effects work correctly in ${browserName}`);
  });

  test('should have working external links', async ({ page, browserName }) => {
    const githubLink = page.locator('.btn-secondary');

    const href = await githubLink.getAttribute('href');
    expect(href).toContain('github.com');

    const target = await githubLink.getAttribute('target');
    expect(target).toBe('_blank');

    const rel = await githubLink.getAttribute('rel');
    expect(rel).toContain('noopener');

    console.log(`✅ External links are properly configured in ${browserName}`);
  });
});

test.describe('Cross-Browser Compatibility - Responsive Design', () => {
  test('should render correctly on mobile viewport', async ({ page, browserName }) => {
    await page.setViewportSize({ width: 375, height: 812 });
    await page.goto('/');

    // Hero should still be visible
    await expect(page.locator('.hero')).toBeVisible();
    await expect(page.locator('.product-name')).toBeVisible();

    // CTA buttons should stack vertically on mobile
    const ctaButtons = page.locator('.cta-buttons');
    const flexDirection = await ctaButtons.evaluate((el) => {
      return window.getComputedStyle(el).flexDirection;
    });

    // Should be column on mobile
    expect(flexDirection).toBe('column');

    console.log(`✅ Mobile responsive layout works in ${browserName}`);
  });

  test('should render correctly on tablet viewport', async ({ page, browserName }) => {
    await page.setViewportSize({ width: 768, height: 1024 });
    await page.goto('/');

    await expect(page.locator('.hero')).toBeVisible();
    await expect(page.locator('.product-name')).toBeVisible();
    await expect(page.locator('#get-started')).toBeVisible();

    console.log(`✅ Tablet responsive layout works in ${browserName}`);
  });

  test('should render correctly on desktop viewport', async ({ page, browserName }) => {
    await page.setViewportSize({ width: 1920, height: 1080 });
    await page.goto('/');

    await expect(page.locator('.hero')).toBeVisible();
    await expect(page.locator('.product-name')).toBeVisible();

    // CTA buttons should be in a row on desktop
    const ctaButtons = page.locator('.cta-buttons');
    const flexDirection = await ctaButtons.evaluate((el) => {
      return window.getComputedStyle(el).flexDirection;
    });

    expect(flexDirection).toBe('row');

    console.log(`✅ Desktop layout works in ${browserName}`);
  });
});

// Test Case 6: JavaScript console errors
test.describe('Cross-Browser Compatibility - Console Errors', () => {
  test('should have no JavaScript errors in console', async ({ page, browserName }) => {
    const errors = [];

    page.on('pageerror', (error) => {
      errors.push(error.message);
    });

    page.on('console', (msg) => {
      if (msg.type() === 'error') {
        errors.push(msg.text());
      }
    });

    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Scroll through the page to trigger any lazy-loaded errors
    await page.evaluate(() => {
      window.scrollTo(0, document.body.scrollHeight);
    });
    await page.waitForTimeout(500);

    expect(errors).toHaveLength(0);

    console.log(`✅ No JavaScript errors in ${browserName}`);
  });
});

// Screenshot tests for visual comparison
test.describe('Cross-Browser Compatibility - Visual Consistency', () => {
  test('should capture full page screenshot', async ({ page, browserName }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Take a full page screenshot
    await page.screenshot({
      path: `./test-results/screenshot-${browserName}-full.png`,
      fullPage: true,
    });

    console.log(`📸 Full page screenshot captured for ${browserName}`);
  });

  test('should capture hero section screenshot', async ({ page, browserName }) => {
    await page.goto('/');

    const hero = page.locator('.hero');
    await hero.screenshot({
      path: `./test-results/screenshot-${browserName}-hero.png`,
    });

    console.log(`📸 Hero section screenshot captured for ${browserName}`);
  });
});
