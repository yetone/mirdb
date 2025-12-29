import { test, expect } from '@playwright/test';

test.describe('Key Features Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('features section contains Memcached compatibility content', async ({ page }) => {
    // Test Case 1: Check features section for Memcached compatibility content
    // Expected: Features section contains text about Memcached protocol compatibility

    const featuresSection = page.locator('[data-testid="features-section"]');
    await expect(featuresSection).toBeVisible();

    // Check for Memcached compatibility content
    const memcachedFeature = featuresSection.locator('[data-testid="feature-memcached"]');
    await expect(memcachedFeature).toBeVisible();

    // Verify it mentions Memcached protocol compatibility
    await expect(memcachedFeature).toContainText(/[Mm]emcached/);
    await expect(memcachedFeature).toContainText(/protocol|compatible|compatibility|drop-in/i);
  });

  test('features section contains persistence content', async ({ page }) => {
    // Test Case 2: Check features section for persistence content
    // Expected: Features section contains text about data persistence and SSTable storage

    const featuresSection = page.locator('[data-testid="features-section"]');
    await expect(featuresSection).toBeVisible();

    // Check for persistence feature content
    const persistenceFeature = featuresSection.locator('[data-testid="feature-persistence"]');
    await expect(persistenceFeature).toBeVisible();

    // Verify it mentions persistence and SSTable
    await expect(persistenceFeature).toContainText(/persist|persistence|persistent/i);
    await expect(persistenceFeature).toContainText(/SSTable|disk|storage/i);
  });

  test('features section contains LSM architecture content', async ({ page }) => {
    // Test Case 3: Check features section for LSM architecture content
    // Expected: Features section contains text about LSM tree architecture

    const featuresSection = page.locator('[data-testid="features-section"]');
    await expect(featuresSection).toBeVisible();

    // Check for LSM architecture feature content
    const lsmFeature = featuresSection.locator('[data-testid="feature-lsm"]');
    await expect(lsmFeature).toBeVisible();

    // Verify it mentions LSM tree architecture
    await expect(lsmFeature).toContainText(/LSM/i);
    await expect(lsmFeature).toContainText(/tree|architecture|write-optimized/i);
  });

  test('features section displays 3-4 key differentiators in a grid layout', async ({ page }) => {
    // Test Case 4: Verify features grid layout with 3-4 items
    // Expected: Features section displays 3-4 key differentiators in a grid layout

    const featuresSection = page.locator('[data-testid="features-section"]');
    await expect(featuresSection).toBeVisible();

    // Check for features grid
    const featuresGrid = featuresSection.locator('[data-testid="features-grid"]');
    await expect(featuresGrid).toBeVisible();

    // Count the number of feature items
    const featureItems = featuresGrid.locator('[data-testid^="feature-"]');
    const count = await featureItems.count();

    // Verify there are 3-4 feature items
    expect(count).toBeGreaterThanOrEqual(3);
    expect(count).toBeLessThanOrEqual(4);

    // Verify grid layout CSS is applied (display: grid)
    const gridStyle = await featuresGrid.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return style.display;
    });
    expect(gridStyle).toBe('grid');
  });

  test('features section is navigable', async ({ page }) => {
    // Verify that the features section can be reached via navigation
    const featuresSection = page.locator('[data-testid="features-section"]');

    // Check that the section has an id for anchor links
    const sectionId = await featuresSection.getAttribute('id');
    expect(sectionId).toBeTruthy();
  });
});
