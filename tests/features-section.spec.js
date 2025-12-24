// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for Features Section Display (Scenario 2)
 * Verifies that key product features are presented clearly as specified in REQ-2
 */

test.describe('Features Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Check features section for memcached compatibility
   * Expected: Feature mentioning 'memcached protocol' or 'memcached compatible' is present
   */
  test('should display memcached protocol compatibility feature', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Check for memcached compatibility feature
    const memcachedFeature = page.locator('[data-feature="memcached-protocol"]');
    await expect(memcachedFeature).toBeVisible();

    // Verify the feature mentions memcached protocol or memcached compatible
    const featureText = await memcachedFeature.textContent();
    const hasMemcachedProtocol = featureText.toLowerCase().includes('memcached protocol');
    const hasMemcachedCompatible = featureText.toLowerCase().includes('memcached compatible');

    expect(hasMemcachedProtocol || hasMemcachedCompatible).toBeTruthy();
  });

  /**
   * Test Case 2: Check features section for persistence
   * Expected: Feature mentioning 'persistent' or 'persistence' and 'SSTable' is present
   */
  test('should display persistence feature with SSTable mention', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Check for persistence feature
    const persistenceFeature = page.locator('[data-feature="persistence"]');
    await expect(persistenceFeature).toBeVisible();

    // Verify the feature mentions persistence and SSTable
    const featureText = await persistenceFeature.textContent();
    const hasPersistent = featureText.toLowerCase().includes('persist');
    const hasSSTable = featureText.toLowerCase().includes('sstable');

    expect(hasPersistent).toBeTruthy();
    expect(hasSSTable).toBeTruthy();
  });

  /**
   * Test Case 3: Check features section for LSM tree
   * Expected: Feature mentioning 'LSM tree' or 'Log-Structured Merge' is present
   */
  test('should display LSM tree architecture feature', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Check for LSM tree feature
    const lsmTreeFeature = page.locator('[data-feature="lsm-tree"]');
    await expect(lsmTreeFeature).toBeVisible();

    // Verify the feature mentions LSM tree or Log-Structured Merge
    const featureText = await lsmTreeFeature.textContent();
    const hasLSMTree = featureText.toLowerCase().includes('lsm tree') ||
                       featureText.toLowerCase().includes('lsm-tree') ||
                       featureText.toLowerCase().includes('log-structured merge');

    expect(hasLSMTree).toBeTruthy();
  });

  /**
   * Test Case 4: Verify at least 3 key features are displayed
   * Expected: Minimum of 3 distinct feature items are visible in the features section
   */
  test('should display at least 3 key features', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Count the number of feature items
    const featureItems = page.locator('.feature-item');
    const featureCount = await featureItems.count();

    // Verify at least 3 features are displayed
    expect(featureCount).toBeGreaterThanOrEqual(3);

    // Verify all feature items are visible
    for (let i = 0; i < Math.min(featureCount, 3); i++) {
      await expect(featureItems.nth(i)).toBeVisible();
    }
  });

  /**
   * Additional test: Verify Tokio async I/O feature is present
   * This covers step 5 of the scenario
   */
  test('should display async I/O with Tokio feature', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Check for async I/O feature
    const asyncFeature = page.locator('[data-feature="async-io"]');
    await expect(asyncFeature).toBeVisible();

    // Verify the feature mentions Tokio
    const featureText = await asyncFeature.textContent();
    const hasTokio = featureText.toLowerCase().includes('tokio');
    const hasAsync = featureText.toLowerCase().includes('async');

    expect(hasTokio).toBeTruthy();
    expect(hasAsync).toBeTruthy();
  });

  /**
   * Test navigation to features section via anchor link
   */
  test('should be able to navigate to features section', async ({ page }) => {
    // Click on features link in footer
    await page.click('a[href="#features"]');

    // Verify features section is visible and in view
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();
  });
});
