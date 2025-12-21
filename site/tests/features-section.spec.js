const { test, expect } = require('@playwright/test');

/**
 * E2E tests for Features Section Display (Scenario 2)
 * Tests verify that key features are displayed with icons and clear descriptions
 * Requirements: REQ-2, REQ-3, US-2
 */

test.describe('Features Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case 1: Features section is present with identifiable container
  test('TC1: should have features section element present', async ({ page }) => {
    const featuresSection = page.locator('#features, [data-section="features"], section.features');
    await expect(featuresSection).toBeVisible();
  });

  // Test Case 2: At least 4 distinct feature cards are displayed
  test('TC2: should display at least 4 distinct feature cards', async ({ page }) => {
    const featureCards = page.locator('.feature-card, [data-testid="feature-card"], .features .card');
    await expect(featureCards).toHaveCount(4);
  });

  // Test Case 3: Memcached Protocol Compatible feature is present with icon and description
  test('TC3: should display Memcached Protocol Compatible feature with icon and description', async ({ page }) => {
    const memcachedFeature = page.locator('[data-feature="memcached"]');
    await expect(memcachedFeature).toBeVisible();

    // Verify it has text about memcached protocol/compatibility
    const featureText = await memcachedFeature.textContent();
    expect(featureText.toLowerCase()).toContain('memcached');
    expect(featureText.toLowerCase()).toMatch(/protocol|compatible|drop-in|replacement|client/);

    // Verify icon is present (use .first() since multiple matches are ok for icon check)
    const icon = memcachedFeature.locator('svg').first();
    await expect(icon).toBeVisible();
  });

  // Test Case 4: Persistent Storage feature is present
  test('TC4: should display Persistent Storage feature describing data persistence with SSTables', async ({ page }) => {
    const persistentFeature = page.locator('[data-feature="persistent"], .feature-card:has-text("Persistent")');
    await expect(persistentFeature).toBeVisible();

    // Verify it has text about persistence and SSTables
    const featureText = await persistentFeature.textContent();
    expect(featureText.toLowerCase()).toContain('persistent');
    expect(featureText.toLowerCase()).toMatch(/sstable|restart|surviv|durable|disk/);
  });

  // Test Case 5: High Performance feature is present
  test('TC5: should display High Performance feature describing LSM-tree architecture and Tokio async I/O', async ({ page }) => {
    const performanceFeature = page.locator('[data-feature="performance"]');
    await expect(performanceFeature).toBeVisible();

    // Verify it has text about LSM-tree or Tokio
    const featureText = await performanceFeature.textContent();
    expect(featureText.toLowerCase()).toContain('performance');
    expect(featureText.toLowerCase()).toMatch(/lsm|tokio|async|fast|efficient/);
  });

  // Test Case 6: Configurable feature is present
  test('TC6: should display Configurable feature describing configuration options', async ({ page }) => {
    const configurableFeature = page.locator('[data-feature="configurable"], .feature-card:has-text("Configurable")');
    await expect(configurableFeature).toBeVisible();

    // Verify it has text about configuration options
    const featureText = await configurableFeature.textContent();
    expect(featureText.toLowerCase()).toContain('configur');
    expect(featureText.toLowerCase()).toMatch(/option|setting|tun|customiz/);
  });

  // Test Case 7: Each feature card contains an icon element
  test('TC7: should have icons present in each feature card', async ({ page }) => {
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();

    expect(cardCount).toBeGreaterThanOrEqual(4);

    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i);
      // Use svg.first() to avoid strict mode violation with multiple icon elements
      const icon = card.locator('svg').first();
      await expect(icon).toBeVisible();
    }
  });
});
