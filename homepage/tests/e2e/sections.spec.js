/**
 * Content Sections E2E Tests
 * Owner: Scenarios 2-9, 19 - All content sections
 *
 * Expected tests:
 * - Hero section content and CTA
 * - Features section cards and usage.gif
 * - Quick Start code blocks
 * - Architecture documentation
 * - API Reference commands
 * - Configuration parameters
 * - Performance information
 * - Contributing guidelines
 * - Asset loading (logo.gif, usage.gif)
 */

const { test, expect } = require('@playwright/test');

test.describe('Hero Section (Scenario 2)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Hero section contains clear value proposition', async ({ page }) => {
    // Verify hero section exists
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Check for value proposition elements
    const heroTitle = page.locator('.hero__title');
    await expect(heroTitle).toBeVisible();
    await expect(heroTitle).toContainText('MirDB');

    // Check tagline mentions key value prop
    const tagline = page.locator('.hero__tagline');
    await expect(tagline).toBeVisible();
    await expect(tagline).toContainText('Persistent Key-Value Store');
    await expect(tagline).toContainText('Memcached');

    // Check description provides additional context
    const description = page.locator('.hero__description');
    await expect(description).toBeVisible();
  });

  test('TC2: Primary CTA button is visible and clickable', async ({ page }) => {
    // Find the CTA button
    const ctaButton = page.locator('.hero__cta');
    await expect(ctaButton).toBeVisible();

    // Verify it's labeled appropriately (Quick Start or similar)
    await expect(ctaButton).toContainText(/Quick Start/i);

    // Verify it's clickable (has href attribute)
    const href = await ctaButton.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href).toContain('quick-start');
  });

  test('TC3: Quick Start CTA navigates to Quick Start section', async ({ page }) => {
    // Click the CTA button
    const ctaButton = page.locator('.hero__cta');
    await ctaButton.click();

    // Wait for navigation/scroll
    await page.waitForTimeout(500);

    // Verify we're at the Quick Start section
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeInViewport();
  });

  test('TC4: Logo.gif is loaded and visible in hero section', async ({ page }) => {
    // Find the hero logo
    const heroLogo = page.locator('.hero__logo');
    await expect(heroLogo).toBeVisible();

    // Verify it's an image with the correct source
    const src = await heroLogo.getAttribute('src');
    expect(src).toContain('logo.gif');

    // Verify the image has loaded (naturalWidth > 0)
    const isLoaded = await heroLogo.evaluate((img) => {
      return img.complete && img.naturalWidth > 0;
    });
    expect(isLoaded).toBe(true);
  });

  test('TC5: Scroll indicator exists in hero section', async ({ page }) => {
    // Find the scroll indicator
    const scrollIndicator = page.locator('.scroll-indicator');
    await expect(scrollIndicator).toBeVisible();

    // Verify it has visual elements (text and/or arrow)
    const scrollText = page.locator('.scroll-indicator__text');
    const scrollArrow = page.locator('.scroll-indicator__arrow');

    // At least one should be visible
    const hasText = await scrollText.isVisible();
    const hasArrow = await scrollArrow.isVisible();
    expect(hasText || hasArrow).toBe(true);
  });
});

// Placeholder tests for other sections (to be implemented by respective scenarios)
test.describe('Features Section (Scenario 3)', () => {
  test.skip('Features section displays capability cards', async ({ page }) => {
    // To be implemented by Scenario 3
  });
});

test.describe('Quick Start Section (Scenario 4)', () => {
  test.skip('Quick Start section shows installation commands', async ({ page }) => {
    // To be implemented by Scenario 4
  });
});

test.describe('Architecture Section (Scenario 5)', () => {
  test.skip('Architecture section explains LSM Tree design', async ({ page }) => {
    // To be implemented by Scenario 5
  });
});

test.describe('API Reference Section (Scenario 6)', () => {
  test.skip('API section documents Memcached commands', async ({ page }) => {
    // To be implemented by Scenario 6
  });
});

test.describe('Configuration Section (Scenario 7)', () => {
  test.skip('Configuration section shows parameters', async ({ page }) => {
    // To be implemented by Scenario 7
  });
});

test.describe('Performance Section (Scenario 8)', () => {
  test.skip('Performance section displays benchmarks', async ({ page }) => {
    // To be implemented by Scenario 8
  });
});

test.describe('Contributing Section (Scenario 9)', () => {
  test.skip('Contributing section shows guidelines', async ({ page }) => {
    // To be implemented by Scenario 9
  });
});

test.describe('Asset Integration (Scenario 19)', () => {
  test.skip('Usage.gif is loaded in features section', async ({ page }) => {
    // To be implemented by Scenario 19
  });
});
