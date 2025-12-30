// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Key Features Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Section displays at least 5 key features', async ({ page }) => {
    // Navigate to Features section
    const featuresSection = page.locator('#features, [data-testid="features-section"]');
    await expect(featuresSection).toBeVisible();

    // Verify section heading
    const heading = featuresSection.locator('h2');
    await expect(heading).toContainText('Key Features');

    // Check that all 5 key features are displayed
    const featureCards = featuresSection.locator('.feature-card, [data-testid="feature-card"]');
    await expect(featureCards).toHaveCount(5);

    // Verify feature names are present
    const memcachedFeature = featuresSection.locator('h3:has-text("Memcached Protocol")');
    await expect(memcachedFeature).toBeVisible();

    const persistentFeature = featuresSection.locator('h3:has-text("Persistent Storage")');
    await expect(persistentFeature).toBeVisible();

    const lsmFeature = featuresSection.locator('h3:has-text("LSM Tree Engine")');
    await expect(lsmFeature).toBeVisible();

    const asyncFeature = featuresSection.locator('h3:has-text("Async I/O")');
    await expect(asyncFeature).toBeVisible();

    const configurableFeature = featuresSection.locator('h3:has-text("Configurable")');
    await expect(configurableFeature).toBeVisible();
  });

  test('TC2: Memcached Protocol feature card mentions drop-in compatibility', async ({ page }) => {
    // Navigate to Features section
    const featuresSection = page.locator('#features, [data-testid="features-section"]');
    await expect(featuresSection).toBeVisible();

    // Find Memcached Protocol feature card
    const featureCards = featuresSection.locator('.feature-card, [data-testid="feature-card"]');
    const memcachedCard = featureCards.filter({ hasText: 'Memcached Protocol' });
    await expect(memcachedCard).toBeVisible();

    // Verify description mentions 'drop-in compatibility with existing memcached clients'
    const description = memcachedCard.locator('p');
    await expect(description).toContainText('drop-in compatibility with existing memcached clients', { ignoreCase: true });
  });

  test('TC3: Persistent Storage feature card mentions data survives restarts via SSTable', async ({ page }) => {
    // Navigate to Features section
    const featuresSection = page.locator('#features, [data-testid="features-section"]');
    await expect(featuresSection).toBeVisible();

    // Find Persistent Storage feature card
    const featureCards = featuresSection.locator('.feature-card, [data-testid="feature-card"]');
    const persistentCard = featureCards.filter({ hasText: 'Persistent Storage' });
    await expect(persistentCard).toBeVisible();

    // Verify description mentions 'data survives restarts via SSTable file format'
    const description = persistentCard.locator('p');
    await expect(description).toContainText('data survives restarts via SSTable file format', { ignoreCase: true });
  });

  test('TC4: LSM Tree Engine feature card mentions high-performance writes with background compaction', async ({ page }) => {
    // Navigate to Features section
    const featuresSection = page.locator('#features, [data-testid="features-section"]');
    await expect(featuresSection).toBeVisible();

    // Find LSM Tree Engine feature card
    const featureCards = featuresSection.locator('.feature-card, [data-testid="feature-card"]');
    const lsmCard = featureCards.filter({ hasText: 'LSM Tree Engine' });
    await expect(lsmCard).toBeVisible();

    // Verify description mentions 'high-performance writes with background compaction'
    const description = lsmCard.locator('p');
    await expect(description).toContainText('high-performance writes with background compaction', { ignoreCase: true });
  });

  test('TC5: Each feature card displays a recognizable icon', async ({ page }) => {
    // Navigate to Features section
    const featuresSection = page.locator('#features, [data-testid="features-section"]');
    await expect(featuresSection).toBeVisible();

    // Get all feature cards
    const featureCards = featuresSection.locator('.feature-card, [data-testid="feature-card"]');
    const cardCount = await featureCards.count();

    // Each card should have at least 5 features
    expect(cardCount).toBeGreaterThanOrEqual(5);

    // Verify each feature card has an icon element
    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i);
      const icon = card.locator('.feature-icon, [data-testid="feature-icon"], svg, img, span[aria-hidden="true"]');
      await expect(icon).toBeVisible();

      // Verify icon has content (not empty)
      const iconContent = await icon.textContent();
      const hasIconClass = await icon.getAttribute('class');
      const hasSvg = await card.locator('svg').count();
      const hasImg = await card.locator('img').count();

      // Icon should have text content (emoji), be an SVG, be an image, or have a class indicating an icon
      const hasValidIcon = (iconContent && iconContent.trim().length > 0) ||
                           hasSvg > 0 ||
                           hasImg > 0 ||
                           (hasIconClass && (hasIconClass.includes('icon') || hasIconClass.includes('fa-') || hasIconClass.includes('bi-')));

      expect(hasValidIcon).toBeTruthy();
    }
  });
});
