// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Features Section Completeness', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Memcached Protocol feature is displayed', async ({ page }) => {
    // Test Case 1: Query features section for Memcached-related content
    // Expected: Feature card/element exists with 'Memcached' or 'memcached protocol' text
    const featuresSection = page.locator('#features, [data-testid="features"], .features');
    await expect(featuresSection).toBeVisible();

    // Look for a feature card containing Memcached-related text
    const memcachedFeature = featuresSection.locator('.feature-card, [data-testid="feature-card"], article, div').filter({
      hasText: /memcached/i
    }).first();

    await expect(memcachedFeature).toBeVisible();

    // Verify the content mentions protocol or compatibility
    const featureText = await memcachedFeature.textContent();
    const lowerText = featureText?.toLowerCase() || '';
    expect(lowerText).toMatch(/memcached/i);
  });

  test('TC2: Data Persistence feature is displayed', async ({ page }) => {
    // Test Case 2: Query features section for persistence-related content
    // Expected: Feature card/element exists with 'persistence' or 'persistent' or 'survives restarts' text
    const featuresSection = page.locator('#features, [data-testid="features"], .features');
    await expect(featuresSection).toBeVisible();

    // Look for a feature card containing persistence-related text
    const persistenceFeature = featuresSection.locator('.feature-card, [data-testid="feature-card"], article, div').filter({
      hasText: /persist|survives? restarts?/i
    }).first();

    await expect(persistenceFeature).toBeVisible();

    // Verify the content mentions persistence
    const featureText = await persistenceFeature.textContent();
    const lowerText = featureText?.toLowerCase() || '';
    const hasPersistence = lowerText.includes('persist') || lowerText.includes('survives restart');
    expect(hasPersistence).toBeTruthy();
  });

  test('TC3: LSM Tree Architecture feature is displayed', async ({ page }) => {
    // Test Case 3: Query features section for LSM tree-related content
    // Expected: Feature card/element exists with 'LSM' or 'LSM tree' text
    const featuresSection = page.locator('#features, [data-testid="features"], .features');
    await expect(featuresSection).toBeVisible();

    // Look for a feature card containing LSM-related text
    const lsmFeature = featuresSection.locator('.feature-card, [data-testid="feature-card"], article, div').filter({
      hasText: /lsm/i
    }).first();

    await expect(lsmFeature).toBeVisible();

    // Verify the content mentions LSM
    const featureText = await lsmFeature.textContent();
    expect(featureText?.toLowerCase()).toMatch(/lsm/i);
  });

  test('TC4: Rust Performance feature is displayed', async ({ page }) => {
    // Test Case 4: Query features section for Rust-related content
    // Expected: Feature card/element exists with 'Rust' text mentioning performance or safety
    const featuresSection = page.locator('#features, [data-testid="features"], .features');
    await expect(featuresSection).toBeVisible();

    // Look for a feature card containing Rust-related text
    const rustFeature = featuresSection.locator('.feature-card, [data-testid="feature-card"], article, div').filter({
      hasText: /rust/i
    }).first();

    await expect(rustFeature).toBeVisible();

    // Verify the content mentions Rust with performance or safety
    const featureText = await rustFeature.textContent();
    const lowerText = featureText?.toLowerCase() || '';
    const hasRust = lowerText.includes('rust');
    const hasPerformanceOrSafety = lowerText.includes('performance') || lowerText.includes('safe') || lowerText.includes('fast');
    expect(hasRust && hasPerformanceOrSafety).toBeTruthy();
  });

  test('TC5: At least 4 feature cards are present', async ({ page }) => {
    // Test Case 5: Count the number of feature cards/elements in features section
    // Expected: At least 4 distinct feature cards are present
    const featuresSection = page.locator('#features, [data-testid="features"], .features');
    await expect(featuresSection).toBeVisible();

    // Count feature cards
    const featureCards = featuresSection.locator('.feature-card, [data-testid="feature-card"]');
    const count = await featureCards.count();

    expect(count).toBeGreaterThanOrEqual(4);
  });
});
