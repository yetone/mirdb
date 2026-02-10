/**
 * Features Section E2E Tests
 * Owner: Scenario 2 - Features Section Display
 *
 * End-to-end tests for the MirDB homepage features section.
 *
 * Expected test coverage:
 * - Features section contains Memcached protocol compatibility text
 * - Features section contains persistence/durability keywords
 * - Features section contains LSM/architecture keywords
 * - At least 3 distinct feature items are visible
 * - Features are displayed in a visually organized manner
 *
 * Requirements traced:
 * - REQ-2: Homepage shall showcase key features and capabilities
 * - USR-1: User can identify at least three key features
 */

const { test, expect } = require('@playwright/test');

test.describe('Features Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case 1: Search features section for 'Memcached' text
  test('features section contains Memcached protocol compatibility text', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    const memcachedText = featuresSection.getByText(/memcached/i);
    await expect(memcachedText.first()).toBeVisible();

    // Verify it mentions protocol compatibility
    const sectionText = await featuresSection.textContent();
    expect(sectionText.toLowerCase()).toMatch(/memcached.*protocol|protocol.*memcached|memcached.*compatible|compatible.*memcached/i);
  });

  // Test Case 2: Search features section for persistence keywords
  test('features section contains persistence keywords', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    const sectionText = await featuresSection.textContent();
    const lowerText = sectionText.toLowerCase();

    // Check for persistence-related keywords
    const hasPersistenceKeyword =
      lowerText.includes('persist') ||
      lowerText.includes('durable') ||
      lowerText.includes('durability') ||
      lowerText.includes('sstable');

    expect(hasPersistenceKeyword).toBeTruthy();
  });

  // Test Case 3: Search features section for architecture keywords
  test('features section contains LSM architecture keywords', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    const sectionText = await featuresSection.textContent();
    const lowerText = sectionText.toLowerCase();

    // Check for LSM-related keywords
    const hasArchitectureKeyword =
      lowerText.includes('lsm') ||
      lowerText.includes('memtable') ||
      lowerText.includes('compaction') ||
      lowerText.includes('log-structured');

    expect(hasArchitectureKeyword).toBeTruthy();
  });

  // Test Case 4: Count feature items displayed
  test('at least 3 distinct feature items are visible', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Look for feature cards or list items
    const featureItems = featuresSection.locator('.feature-card, .feature-item, .feature');
    const count = await featureItems.count();

    expect(count).toBeGreaterThanOrEqual(3);
  });

  // Test Case 6: Check feature cards/list styling
  test('features are displayed in a visually organized manner', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Check that feature items exist and are visible
    const featureItems = featuresSection.locator('.feature-card, .feature-item, .feature');
    const count = await featureItems.count();
    expect(count).toBeGreaterThanOrEqual(1);

    // Check that features are displayed as cards, grid, or list
    const firstFeature = featureItems.first();
    await expect(firstFeature).toBeVisible();

    // Verify the container uses grid or flex layout (visually organized)
    const featuresContainer = featuresSection.locator('.features-grid, .features-list, .features-container');
    await expect(featuresContainer).toBeVisible();
  });
});
