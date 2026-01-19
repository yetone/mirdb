// @ts-check
const { test, expect } = require('@playwright/test');
const fs = require('fs');
const path = require('path');

/**
 * Unit tests for Asset Optimization
 * Test Case: Images are optimized for web delivery (appropriate file sizes)
 */

test.describe('Asset Optimization', () => {

  // Define maximum acceptable file sizes for web delivery
  // These are reasonable limits for web assets
  const MAX_LOGO_SIZE_MB = 5; // 5MB max for logo (animated gifs can be larger)
  const MAX_USAGE_GIF_SIZE_MB = 10; // 10MB max for usage demo gif

  const assetsDir = path.join(__dirname, '..', 'assets');

  /**
   * Test Case 3: Images are optimized for web delivery
   * Input: Check asset optimization
   * Expected: Images are optimized for web delivery (appropriate file sizes)
   */
  test('TC3: Logo.gif has appropriate file size for web delivery', async () => {
    const logoPath = path.join(assetsDir, 'logo.gif');

    // Verify the file exists
    expect(fs.existsSync(logoPath)).toBe(true);

    // Get file stats
    const stats = fs.statSync(logoPath);
    const fileSizeMB = stats.size / (1024 * 1024);

    // Verify file size is within acceptable range
    // File should not be empty and should not exceed max size
    expect(stats.size).toBeGreaterThan(0);
    expect(fileSizeMB).toBeLessThanOrEqual(MAX_LOGO_SIZE_MB);

    // Log the actual size for reference
    console.log(`logo.gif size: ${fileSizeMB.toFixed(2)} MB`);
  });

  test('TC3b: Usage.gif has appropriate file size for web delivery', async () => {
    const usagePath = path.join(assetsDir, 'usage.gif');

    // Verify the file exists
    expect(fs.existsSync(usagePath)).toBe(true);

    // Get file stats
    const stats = fs.statSync(usagePath);
    const fileSizeMB = stats.size / (1024 * 1024);

    // Verify file size is within acceptable range
    expect(stats.size).toBeGreaterThan(0);
    expect(fileSizeMB).toBeLessThanOrEqual(MAX_USAGE_GIF_SIZE_MB);

    // Log the actual size for reference
    console.log(`usage.gif size: ${fileSizeMB.toFixed(2)} MB`);
  });

  test('All asset files exist in the assets directory', async () => {
    const logoPath = path.join(assetsDir, 'logo.gif');
    const usagePath = path.join(assetsDir, 'usage.gif');

    // Verify both expected assets exist
    expect(fs.existsSync(logoPath)).toBe(true);
    expect(fs.existsSync(usagePath)).toBe(true);
  });

  test('Asset files are valid GIF format', async () => {
    const logoPath = path.join(assetsDir, 'logo.gif');
    const usagePath = path.join(assetsDir, 'usage.gif');

    // GIF files start with 'GIF87a' or 'GIF89a'
    const gifMagicNumbers = ['GIF87a', 'GIF89a'];

    // Check logo.gif format
    const logoBuffer = fs.readFileSync(logoPath);
    const logoHeader = logoBuffer.slice(0, 6).toString('ascii');
    expect(gifMagicNumbers).toContain(logoHeader);

    // Check usage.gif format
    const usageBuffer = fs.readFileSync(usagePath);
    const usageHeader = usageBuffer.slice(0, 6).toString('ascii');
    expect(gifMagicNumbers).toContain(usageHeader);
  });

  test('Assets have reasonable dimensions for web display', async ({ page }) => {
    // Navigate to homepage to test rendered dimensions
    await page.goto('/');

    // Check logo dimensions on page
    const logo = page.locator('[data-testid="hero-logo"]');
    await expect(logo).toBeVisible();

    // Wait for logo image to load
    await page.waitForFunction(
      (selector) => {
        const img = document.querySelector(selector);
        return img && img.complete && img.naturalWidth > 0;
      },
      '[data-testid="hero-logo"]',
      { timeout: 30000 }
    );

    // Get natural dimensions
    const logoDimensions = await logo.evaluate((img) => ({
      naturalWidth: img.naturalWidth,
      naturalHeight: img.naturalHeight
    }));

    // Logo should have reasonable dimensions (not absurdly large)
    expect(logoDimensions.naturalWidth).toBeGreaterThan(0);
    expect(logoDimensions.naturalWidth).toBeLessThan(3000); // Max 3000px width
    expect(logoDimensions.naturalHeight).toBeGreaterThan(0);
    expect(logoDimensions.naturalHeight).toBeLessThan(3000); // Max 3000px height

    // Check usage.gif dimensions
    await page.locator('#getting-started').scrollIntoViewIfNeeded();
    const usageGif = page.locator('img[src*="usage.gif"]');
    await expect(usageGif).toBeVisible();

    // Wait for usage.gif to load
    await page.waitForFunction(
      (selector) => {
        const img = document.querySelector(selector);
        return img && img.complete && img.naturalWidth > 0;
      },
      'img[src*="usage.gif"]',
      { timeout: 30000 }
    );

    const usageDimensions = await usageGif.evaluate((img) => ({
      naturalWidth: img.naturalWidth,
      naturalHeight: img.naturalHeight
    }));

    expect(usageDimensions.naturalWidth).toBeGreaterThan(0);
    expect(usageDimensions.naturalWidth).toBeLessThan(3000);
    expect(usageDimensions.naturalHeight).toBeGreaterThan(0);
    expect(usageDimensions.naturalHeight).toBeLessThan(3000);
  });

});
