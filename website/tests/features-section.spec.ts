import { test, expect } from '@playwright/test';

/**
 * E2E Tests for Features Section Display
 * Scenario: Verify that the features section presents key MirDB capabilities
 * including memcached protocol support, persistence, and LSM tree architecture
 */

test.describe('Features Section Display', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to homepage before each test
    await page.goto('/');
  });

  /**
   * Test Case 1: Check for Memcached Protocol feature
   * Input: Check for Memcached Protocol feature
   * Expected: Feature card displays information about memcached protocol compatibility
   */
  test('should display Memcached Protocol feature card with compatibility information', async ({ page }) => {
    // Verify the features section exists
    const featuresSection = page.locator('[data-testid="features-section"]');
    await expect(featuresSection).toBeVisible();

    // Verify the memcached protocol feature card exists
    const memcachedFeature = page.locator('[data-testid="feature-memcached"]');
    await expect(memcachedFeature).toBeVisible();

    // Verify the feature card contains relevant content about memcached protocol
    const featureText = await memcachedFeature.textContent();
    expect(featureText).toBeTruthy();
    expect(featureText?.toLowerCase()).toMatch(/memcached/);
    expect(featureText?.toLowerCase()).toMatch(/(protocol|compatible|compatibility|client)/);
  });

  /**
   * Test Case 2: Check for Persistence feature
   * Input: Check for Persistence feature
   * Expected: Feature card displays information about SSTable-based persistence
   */
  test('should display Persistence feature card with SSTable information', async ({ page }) => {
    // Verify the features section exists
    const featuresSection = page.locator('[data-testid="features-section"]');
    await expect(featuresSection).toBeVisible();

    // Verify the persistence feature card exists
    const persistenceFeature = page.locator('[data-testid="feature-persistence"]');
    await expect(persistenceFeature).toBeVisible();

    // Verify the feature card contains relevant content about persistence/SSTables
    const featureText = await persistenceFeature.textContent();
    expect(featureText).toBeTruthy();
    expect(featureText?.toLowerCase()).toMatch(/(persist|storage|sstable|durable|durability|disk)/);
  });

  /**
   * Test Case 3: Check for LSM Tree feature
   * Input: Check for LSM Tree feature
   * Expected: Feature card displays information about LSM tree architecture
   */
  test('should display LSM Tree feature card with architecture information', async ({ page }) => {
    // Verify the features section exists
    const featuresSection = page.locator('[data-testid="features-section"]');
    await expect(featuresSection).toBeVisible();

    // Verify the LSM tree feature card exists
    const lsmFeature = page.locator('[data-testid="feature-lsm"]');
    await expect(lsmFeature).toBeVisible();

    // Verify the feature card contains relevant content about LSM tree architecture
    const featureText = await lsmFeature.textContent();
    expect(featureText).toBeTruthy();
    expect(featureText?.toLowerCase()).toMatch(/(lsm|log-structured|architecture|tree)/);
  });

  /**
   * Test Case 4: Check for Rust/Performance feature
   * Input: Check for Rust/Performance feature
   * Expected: Feature card mentions Rust implementation and high performance
   */
  test('should display Rust/Performance feature card with performance information', async ({ page }) => {
    // Verify the features section exists
    const featuresSection = page.locator('[data-testid="features-section"]');
    await expect(featuresSection).toBeVisible();

    // Verify the performance feature card exists
    const performanceFeature = page.locator('[data-testid="feature-performance"]');
    await expect(performanceFeature).toBeVisible();

    // Verify the feature card mentions Rust and performance
    const featureText = await performanceFeature.textContent();
    expect(featureText).toBeTruthy();
    expect(featureText?.toLowerCase()).toMatch(/rust/);
    expect(featureText?.toLowerCase()).toMatch(/(performance|fast|speed|efficient|high-performance)/);
  });

  /**
   * Test Case 5: Verify features grid layout
   * Input: Verify features grid layout
   * Expected: Features are displayed in a grid layout (3-4 cards) with consistent styling
   */
  test('should display features in a grid layout with 3-4 cards', async ({ page }) => {
    // Verify the features section exists
    const featuresSection = page.locator('[data-testid="features-section"]');
    await expect(featuresSection).toBeVisible();

    // Verify the features grid container exists
    const featuresGrid = page.locator('[data-testid="features-grid"]');
    await expect(featuresGrid).toBeVisible();

    // Verify there are 3-4 feature cards in the grid
    // Use specific feature card testids to avoid counting title/description elements
    const featureCardIds = ['feature-memcached', 'feature-persistence', 'feature-lsm', 'feature-performance'];
    const featureCards = page.locator('[data-testid="features-grid"] > div');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(3);
    expect(cardCount).toBeLessThanOrEqual(4);

    // Verify each feature card has a title and description
    for (const featureId of featureCardIds.slice(0, cardCount)) {
      const card = page.locator(`[data-testid="${featureId}"]`);

      // Check that card has a title element
      const title = card.locator(`[data-testid="${featureId}-title"]`);
      await expect(title).toBeVisible();

      // Check that card has a description element
      const description = card.locator(`[data-testid="${featureId}-description"]`);
      await expect(description).toBeVisible();
    }

    // Verify grid has display: grid property (responsive grid layout)
    const gridDisplay = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });
    expect(gridDisplay).toBe('grid');
  });
});
