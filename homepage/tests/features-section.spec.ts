import { test, expect } from '@playwright/test';

test.describe('Features Section Content', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: displays memcached protocol compatibility feature', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Check for memcached protocol compatibility feature
    const memcachedFeature = featuresSection.locator('[data-feature="memcached"]');
    await expect(memcachedFeature).toBeVisible();

    // Verify icon is present
    const icon = memcachedFeature.locator('[data-icon]');
    await expect(icon).toBeVisible();

    // Verify title is present and contains relevant text
    const title = memcachedFeature.locator('[data-title]');
    await expect(title).toBeVisible();
    await expect(title).toContainText(/memcached|protocol|compatibility/i);

    // Verify description mentions compatibility with existing clients
    const description = memcachedFeature.locator('[data-description]');
    await expect(description).toBeVisible();
    await expect(description).toContainText(/client|compatible|existing/i);
  });

  test('TC2: displays persistence feature', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Check for persistence feature
    const persistenceFeature = featuresSection.locator('[data-feature="persistence"]');
    await expect(persistenceFeature).toBeVisible();

    // Verify icon is present
    const icon = persistenceFeature.locator('[data-icon]');
    await expect(icon).toBeVisible();

    // Verify title is present
    const title = persistenceFeature.locator('[data-title]');
    await expect(title).toBeVisible();
    await expect(title).toContainText(/persist|durable|storage/i);

    // Verify description mentions SSTables and durability
    const description = persistenceFeature.locator('[data-description]');
    await expect(description).toBeVisible();
    await expect(description).toContainText(/SSTable|disk|restart|durability/i);
  });

  test('TC3: displays LSM tree architecture feature', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Check for LSM tree architecture feature
    const lsmFeature = featuresSection.locator('[data-feature="lsm"]');
    await expect(lsmFeature).toBeVisible();

    // Verify icon is present
    const icon = lsmFeature.locator('[data-icon]');
    await expect(icon).toBeVisible();

    // Verify title is present
    const title = lsmFeature.locator('[data-title]');
    await expect(title).toBeVisible();
    await expect(title).toContainText(/LSM|tree|architecture/i);

    // Verify description explains LSM tree benefits
    const description = lsmFeature.locator('[data-description]');
    await expect(description).toBeVisible();
    await expect(description).toContainText(/write|performance|compaction|memtable/i);
  });

  test('TC4: displays Rust performance feature', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Check for Rust performance feature
    const rustFeature = featuresSection.locator('[data-feature="rust"]');
    await expect(rustFeature).toBeVisible();

    // Verify icon is present
    const icon = rustFeature.locator('[data-icon]');
    await expect(icon).toBeVisible();

    // Verify title is present
    const title = rustFeature.locator('[data-title]');
    await expect(title).toBeVisible();
    await expect(title).toContainText(/rust|performance|fast/i);

    // Verify description mentions performance and memory safety
    const description = rustFeature.locator('[data-description]');
    await expect(description).toBeVisible();
    await expect(description).toContainText(/memory|safe|performance|speed/i);
  });

  test('TC5: all features have accompanying icons', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Get all feature cards
    const featureCards = featuresSection.locator('[data-feature]');
    const count = await featureCards.count();

    // Ensure we have at least 4 features
    expect(count).toBeGreaterThanOrEqual(4);

    // Verify each feature has an icon
    for (let i = 0; i < count; i++) {
      const card = featureCards.nth(i);
      const icon = card.locator('[data-icon]');
      await expect(icon).toBeVisible();

      // Icon should contain an SVG or image element
      const hasSvgOrImg = await card.locator('[data-icon] svg, [data-icon] img').count();
      expect(hasSvgOrImg).toBeGreaterThan(0);
    }
  });

  test('features section has proper structure and styling', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Check section has a heading
    const heading = featuresSection.locator('h2');
    await expect(heading).toBeVisible();
    await expect(heading).toContainText(/feature/i);

    // Check that features are displayed in a grid or list layout
    const featureCards = featuresSection.locator('[data-feature]');
    const count = await featureCards.count();
    expect(count).toBe(4);
  });
});
