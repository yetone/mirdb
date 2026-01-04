// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Features Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('should display Memcached Compatibility feature', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();

    // Check for Memcached Compatibility feature card
    const memcachedFeature = page.locator('[data-feature="memcached-compatibility"]');
    await expect(memcachedFeature).toBeVisible();

    // Verify it describes drop-in replacement for existing memcached clients
    const description = memcachedFeature.locator('.feature-description, p');
    await expect(description).toContainText(/drop-in replacement|memcached client/i);
  });

  test('should display Persistent Storage feature', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();

    // Check for Persistent Storage feature card
    const persistentFeature = page.locator('[data-feature="persistent-storage"]');
    await expect(persistentFeature).toBeVisible();

    // Verify it describes data survival using SSTable-based storage
    const description = persistentFeature.locator('.feature-description, p');
    await expect(description).toContainText(/SSTable|survives|restart|persist/i);
  });

  test('should display LSM Tree Architecture feature', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();

    // Check for LSM Tree Architecture feature card
    const lsmFeature = page.locator('[data-feature="lsm-tree"]');
    await expect(lsmFeature).toBeVisible();

    // Verify it describes efficient write performance with background compaction
    const description = lsmFeature.locator('.feature-description, p');
    await expect(description).toContainText(/write|compaction|efficient/i);
  });

  test('should display Async Networking feature', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();

    // Check for Async Networking feature card
    const asyncFeature = page.locator('[data-feature="async-networking"]');
    await expect(asyncFeature).toBeVisible();

    // Verify it describes Tokio-based async I/O
    const description = asyncFeature.locator('.feature-description, p');
    await expect(description).toContainText(/Tokio|async|I\/O/i);
  });
});
