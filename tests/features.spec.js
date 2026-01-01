const { test, expect } = require('@playwright/test');

test.describe('Key Features Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Memcached compatibility feature is displayed with description of protocol support', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();

    // Query for Memcached compatibility feature element
    const memcachedFeature = page.locator('[data-testid="feature-memcached"]');

    // Verify feature card exists
    await expect(memcachedFeature).toBeVisible();

    // Verify title
    const title = memcachedFeature.locator('h3');
    await expect(title).toHaveText('Memcached Compatible');

    // Verify description mentions protocol support
    const description = memcachedFeature.locator('p');
    await expect(description).toContainText('memcached');
    await expect(description).toContainText('protocol');

    // Verify description mentions supported commands
    await expect(description).toContainText('SET');
    await expect(description).toContainText('GET');
  });

  test('TC2: Data persistence feature is displayed highlighting durability unlike standard memcached', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();

    // Query for persistence feature element
    const persistenceFeature = page.locator('[data-testid="feature-persistence"]');

    // Verify feature card exists
    await expect(persistenceFeature).toBeVisible();

    // Verify title
    const title = persistenceFeature.locator('h3');
    await expect(title).toHaveText('Data Persistence');

    // Verify description highlights durability unlike standard memcached
    const description = persistenceFeature.locator('p');
    await expect(description).toContainText('Unlike standard memcached');
    await expect(description).toContainText('survives restarts');

    // Verify mentions durability features
    await expect(description).toContainText('Write-Ahead Log');
    await expect(description).toContainText('durability');
  });

  test('TC3: High performance feature with LSM tree architecture explanation is displayed', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();

    // Query for performance/LSM tree feature element
    const performanceFeature = page.locator('[data-testid="feature-performance"]');

    // Verify feature card exists
    await expect(performanceFeature).toBeVisible();

    // Verify title
    const title = performanceFeature.locator('h3');
    await expect(title).toHaveText('High Performance');

    // Verify description mentions LSM tree architecture
    const description = performanceFeature.locator('p');
    await expect(description).toContainText('LSM tree');
    await expect(description).toContainText('architecture');

    // Verify performance benefits are mentioned
    await expect(description).toContainText('write throughput');
    await expect(description).toContainText('compaction');
  });

  test('TC4: At least 3 feature cards are present in the features section', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();

    // Count total number of feature cards in features section
    const featuresGrid = page.locator('[data-testid="features-grid"]');
    await expect(featuresGrid).toBeVisible();

    const featureCards = featuresGrid.locator('.feature-card');
    const count = await featureCards.count();

    // Verify at least 3 feature cards are present
    expect(count).toBeGreaterThanOrEqual(3);

    // Verify all three specific feature cards exist
    await expect(page.locator('[data-testid="feature-memcached"]')).toBeVisible();
    await expect(page.locator('[data-testid="feature-persistence"]')).toBeVisible();
    await expect(page.locator('[data-testid="feature-performance"]')).toBeVisible();
  });
});
