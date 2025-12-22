const { test, expect } = require('@playwright/test');

test.describe('Features Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('should display at least 3 feature cards', async ({ page }) => {
    // Test Case 1: Count feature cards in features section
    const featuresSection = page.locator('#features, [data-testid="features-section"], .features-section');
    await expect(featuresSection).toBeVisible();

    const featureCards = page.locator('.feature-card, [data-testid="feature-card"]');
    const count = await featureCards.count();

    expect(count).toBeGreaterThanOrEqual(3);
  });

  test('should display Memcached Protocol feature', async ({ page }) => {
    // Test Case 2: Check for 'Memcached Protocol' feature
    const memcachedFeature = page.locator('.feature-card, [data-testid="feature-card"]').filter({
      hasText: /memcached/i
    });

    await expect(memcachedFeature).toBeVisible();

    // Verify it has an icon
    const icon = memcachedFeature.locator('.feature-icon, [data-testid="feature-icon"]').first();
    await expect(icon).toBeVisible();

    // Verify it has a description
    const description = memcachedFeature.locator('.feature-description, p');
    await expect(description).toBeVisible();
    const descText = await description.textContent();
    expect(descText.length).toBeGreaterThan(10);
  });

  test('should display Persistent Storage or SSTables feature', async ({ page }) => {
    // Test Case 3: Check for 'Persistent Storage' or 'SSTables' feature
    const persistentFeature = page.locator('.feature-card, [data-testid="feature-card"]').filter({
      hasText: /persistent|sstable/i
    });

    await expect(persistentFeature).toBeVisible();

    // Verify it has an icon
    const icon = persistentFeature.locator('.feature-icon, [data-testid="feature-icon"]').first();
    await expect(icon).toBeVisible();

    // Verify it has a description
    const description = persistentFeature.locator('.feature-description, p');
    await expect(description).toBeVisible();
  });

  test('should display LSM Tree Architecture feature', async ({ page }) => {
    // Test Case 4: Check for 'LSM Tree Architecture' feature
    const lsmFeature = page.locator('.feature-card, [data-testid="feature-card"]').filter({
      hasText: /lsm/i
    });

    await expect(lsmFeature).toBeVisible();

    // Verify it has an icon
    const icon = lsmFeature.locator('.feature-icon, [data-testid="feature-icon"]').first();
    await expect(icon).toBeVisible();

    // Verify it has a description
    const description = lsmFeature.locator('.feature-description, p');
    await expect(description).toBeVisible();
  });

  test('should have visual icons on all feature cards', async ({ page }) => {
    // Test Case 5: Verify each feature card has visual icon
    const featureCards = page.locator('.feature-card, [data-testid="feature-card"]');
    const count = await featureCards.count();

    expect(count).toBeGreaterThanOrEqual(3);

    for (let i = 0; i < count; i++) {
      const card = featureCards.nth(i);
      const icon = card.locator('.feature-icon, [data-testid="feature-icon"]').first();
      await expect(icon).toBeVisible();
    }
  });

  test('should display features in grid layout on desktop', async ({ page }) => {
    // Test Case 6 (Part 1): Verify grid layout on desktop
    const featuresGrid = page.locator('.features-grid, [data-testid="features-grid"], #features .grid');
    await expect(featuresGrid).toBeVisible();

    // Check that grid layout is applied
    const gridStyle = await featuresGrid.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        display: style.display,
        gridTemplateColumns: style.gridTemplateColumns
      };
    });

    // On desktop, should be grid with multiple columns
    expect(gridStyle.display).toBe('grid');
    // Grid should have more than one column on desktop
    expect(gridStyle.gridTemplateColumns).not.toBe('none');
  });
});

test.describe('Features Section Responsiveness', () => {
  test('should stack features vertically on mobile', async ({ page }) => {
    // Test Case 6 (Part 2): Verify features stack vertically on mobile
    // This test runs in mobile viewport as defined in playwright.config.js
    await page.goto('/');

    const featuresGrid = page.locator('.features-grid, [data-testid="features-grid"], #features .grid');
    await expect(featuresGrid).toBeVisible();

    const featureCards = page.locator('.feature-card, [data-testid="feature-card"]');
    const count = await featureCards.count();

    if (count >= 2) {
      const firstCard = featureCards.first();
      const secondCard = featureCards.nth(1);

      const firstBox = await firstCard.boundingBox();
      const secondBox = await secondCard.boundingBox();

      // On mobile, cards should stack vertically (second card below first)
      // Either they're stacked (y positions differ significantly) or side by side
      // In mobile view, we expect vertical stacking
      const viewportSize = page.viewportSize();
      if (viewportSize && viewportSize.width < 768) {
        // On mobile, second card should be below first card
        expect(secondBox.y).toBeGreaterThanOrEqual(firstBox.y + firstBox.height - 10);
      }
    }
  });
});
