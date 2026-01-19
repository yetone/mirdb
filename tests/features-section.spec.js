// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Features Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: At least 4 feature cards are present', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features, section.features, [data-testid="features"]').first();
    await expect(featuresSection).toBeVisible();

    // Count feature cards
    const featureCards = featuresSection.locator('.feature-card, .feature, [data-testid="feature-card"]');
    const count = await featureCards.count();

    expect(count).toBeGreaterThanOrEqual(4);
  });

  test('TC2: Memcached Protocol feature card is displayed with icon', async ({ page }) => {
    const featuresSection = page.locator('#features, section.features, [data-testid="features"]').first();
    await expect(featuresSection).toBeVisible();

    // Check for Memcached Protocol feature
    const memcachedFeature = featuresSection.locator('.feature-card, .feature, [data-testid="feature-card"]').filter({
      hasText: /memcached.*protocol|tokio.*memcached/i
    }).first();
    await expect(memcachedFeature).toBeVisible();

    // Check for icon
    const icon = memcachedFeature.locator('svg, img, .icon, [data-testid="feature-icon"]').first();
    await expect(icon).toBeVisible();
  });

  test('TC3: Skiplist Memtable feature card is displayed with icon', async ({ page }) => {
    const featuresSection = page.locator('#features, section.features, [data-testid="features"]').first();
    await expect(featuresSection).toBeVisible();

    // Check for Skiplist Memtable feature
    const skiplistFeature = featuresSection.locator('.feature-card, .feature, [data-testid="feature-card"]').filter({
      hasText: /memtable/i
    }).filter({
      hasText: /skiplist|skip.*list/i
    }).first();
    await expect(skiplistFeature).toBeVisible();

    // Check for icon
    const icon = skiplistFeature.locator('svg, img, .icon, [data-testid="feature-icon"]').first();
    await expect(icon).toBeVisible();
  });

  test('TC4: Minor Compaction feature card is displayed with icon', async ({ page }) => {
    const featuresSection = page.locator('#features, section.features, [data-testid="features"]').first();
    await expect(featuresSection).toBeVisible();

    // Check for Minor Compaction feature
    const minorCompactionFeature = featuresSection.locator('.feature-card, .feature, [data-testid="feature-card"]').filter({
      hasText: /minor.*compaction/i
    }).first();
    await expect(minorCompactionFeature).toBeVisible();

    // Check for icon
    const icon = minorCompactionFeature.locator('svg, img, .icon, [data-testid="feature-icon"]').first();
    await expect(icon).toBeVisible();
  });

  test('TC5: Major Compaction feature card is displayed with icon', async ({ page }) => {
    const featuresSection = page.locator('#features, section.features, [data-testid="features"]').first();
    await expect(featuresSection).toBeVisible();

    // Check for Major Compaction feature
    const majorCompactionFeature = featuresSection.locator('.feature-card, .feature, [data-testid="feature-card"]').filter({
      hasText: /major.*compaction/i
    }).first();
    await expect(majorCompactionFeature).toBeVisible();

    // Check for icon
    const icon = majorCompactionFeature.locator('svg, img, .icon, [data-testid="feature-icon"]').first();
    await expect(icon).toBeVisible();
  });

  test('TC6: Each feature card contains a brief description', async ({ page }) => {
    const featuresSection = page.locator('#features, section.features, [data-testid="features"]').first();
    await expect(featuresSection).toBeVisible();

    const featureCards = featuresSection.locator('.feature-card, .feature, [data-testid="feature-card"]');
    const count = await featureCards.count();

    expect(count).toBeGreaterThanOrEqual(4);

    // Check each feature card has a description
    for (let i = 0; i < count; i++) {
      const card = featureCards.nth(i);
      const description = card.locator('.description, .feature-description, p').first();
      await expect(description).toBeVisible();

      // Verify description has meaningful text (at least 20 characters)
      const text = await description.textContent();
      expect(text.length).toBeGreaterThanOrEqual(20);
    }
  });
});
