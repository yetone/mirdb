// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for Features Section (Scenario 2)
 *
 * Validates the features grid displays all key MirDB features
 * with icons, titles, and descriptions.
 */

test.describe('Features Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case 1: Query features section for feature cards
  test('should display 5-6 feature card elements with class .feature-card', async ({ page }) => {
    const featureCards = page.locator('.feature-card');
    const count = await featureCards.count();

    expect(count).toBeGreaterThanOrEqual(5);
    expect(count).toBeLessThanOrEqual(6);
  });

  // Test Case 2: Check feature - Memcached Protocol Compatibility
  test('should display Memcached Protocol feature card with icon, title, and description', async ({ page }) => {
    const featuresSection = page.locator('#features');

    // Find the feature card containing Memcached Protocol
    const memcachedCard = featuresSection.locator('.feature-card', {
      has: page.locator('.feature-title', { hasText: /Memcached Protocol/i })
    });

    await expect(memcachedCard).toBeVisible();

    // Verify it has an icon
    const icon = memcachedCard.locator('.feature-icon');
    await expect(icon).toBeVisible();

    // Verify title
    const title = memcachedCard.locator('.feature-title');
    await expect(title).toHaveText(/Memcached Protocol/i);

    // Verify description mentions compatibility
    const description = memcachedCard.locator('.feature-description');
    await expect(description).toContainText(/compatib/i);
  });

  // Test Case 3: Check feature - Persistence (LSM Tree)
  test('should display Persistence/LSM Tree feature card with durable storage description', async ({ page }) => {
    const featuresSection = page.locator('#features');

    // Find the feature card mentioning Persistence or LSM
    const persistenceCard = featuresSection.locator('.feature-card', {
      has: page.locator('.feature-title', { hasText: /Persistence|LSM/i })
    });

    await expect(persistenceCard).toBeVisible();

    // Verify it has an icon
    const icon = persistenceCard.locator('.feature-icon');
    await expect(icon).toBeVisible();

    // Verify description mentions durable storage
    const description = persistenceCard.locator('.feature-description');
    await expect(description).toContainText(/durable|storage|persist/i);
  });

  // Test Case 4: Check feature - TTL Support
  test('should display TTL Support feature card mentioning expiration', async ({ page }) => {
    const featuresSection = page.locator('#features');

    // Find the feature card mentioning TTL
    const ttlCard = featuresSection.locator('.feature-card', {
      has: page.locator('.feature-title', { hasText: /TTL/i })
    });

    await expect(ttlCard).toBeVisible();

    // Verify it has an icon
    const icon = ttlCard.locator('.feature-icon');
    await expect(icon).toBeVisible();

    // Verify description mentions TTL or expiration
    const description = ttlCard.locator('.feature-description');
    await expect(description).toContainText(/expir|TTL/i);
  });

  // Test Case 5: Check feature - Fault Tolerance (WAL)
  test('should display Fault Tolerance feature card mentioning Write-Ahead Log or crash recovery', async ({ page }) => {
    const featuresSection = page.locator('#features');

    // Find the feature card mentioning Fault Tolerance
    const faultToleranceCard = featuresSection.locator('.feature-card', {
      has: page.locator('.feature-title', { hasText: /Fault Tolerance/i })
    });

    await expect(faultToleranceCard).toBeVisible();

    // Verify it has an icon
    const icon = faultToleranceCard.locator('.feature-icon');
    await expect(icon).toBeVisible();

    // Verify description mentions WAL or crash recovery
    const description = faultToleranceCard.locator('.feature-description');
    await expect(description).toContainText(/Write-Ahead|WAL|crash|recover/i);
  });

  // Test Case 6: Check feature - Compaction
  test('should display Compaction feature card mentioning storage optimization', async ({ page }) => {
    const featuresSection = page.locator('#features');

    // Find the feature card mentioning Compaction
    const compactionCard = featuresSection.locator('.feature-card', {
      has: page.locator('.feature-title', { hasText: /Compaction/i })
    });

    await expect(compactionCard).toBeVisible();

    // Verify it has an icon
    const icon = compactionCard.locator('.feature-icon');
    await expect(icon).toBeVisible();

    // Verify description mentions compaction or optimization
    const description = compactionCard.locator('.feature-description');
    await expect(description).toContainText(/compaction|optimi|merg|clean/i);
  });

  // Test Case 7: Hover over feature card shows hover state with transition <= 200ms
  test('should display hover state with transition duration <= 200ms', async ({ page }) => {
    const featureCard = page.locator('.feature-card').first();

    // Get the computed transition-duration before hover
    const transitionDuration = await featureCard.evaluate((el) => {
      const style = window.getComputedStyle(el);
      const duration = style.transitionDuration;
      // Parse duration - could be in format "0.2s" or "200ms"
      if (duration.includes('ms')) {
        return parseFloat(duration);
      } else if (duration.includes('s')) {
        return parseFloat(duration) * 1000;
      }
      return 0;
    });

    expect(transitionDuration).toBeLessThanOrEqual(200);

    // Hover over the card and verify visual change
    await featureCard.hover();

    // Verify transform or box-shadow is applied on hover
    const transform = await featureCard.evaluate((el) => {
      return window.getComputedStyle(el).transform;
    });

    // Should have some transform applied (not "none")
    expect(transform).not.toBe('none');
  });

  // Test Case 8: Verify features grid layout (2-3 columns on desktop)
  test('should display features in responsive grid with 2-3 columns on desktop', async ({ page }) => {
    // Set viewport to desktop size
    await page.setViewportSize({ width: 1200, height: 800 });

    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    // Check that the grid has the correct column configuration
    const gridColumns = await featuresGrid.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return style.gridTemplateColumns;
    });

    // Count number of columns by splitting on space (each column has a width value)
    const columnCount = gridColumns.split(' ').filter(val => val.length > 0 && val !== 'none').length;

    expect(columnCount).toBeGreaterThanOrEqual(2);
    expect(columnCount).toBeLessThanOrEqual(3);
  });

  // Additional test: Verify all feature cards have required structure
  test('each feature card should have icon, title, and description', async ({ page }) => {
    const featureCards = page.locator('.feature-card');
    const count = await featureCards.count();

    for (let i = 0; i < count; i++) {
      const card = featureCards.nth(i);

      // Each card should have an icon
      const icon = card.locator('.feature-icon');
      await expect(icon).toBeVisible();

      // Each card should have a title
      const title = card.locator('.feature-title');
      await expect(title).toBeVisible();
      const titleText = await title.textContent();
      expect(titleText?.trim().length).toBeGreaterThan(0);

      // Each card should have a description
      const description = card.locator('.feature-description');
      await expect(description).toBeVisible();
      const descText = await description.textContent();
      expect(descText?.trim().length).toBeGreaterThan(0);
    }
  });

  // Responsive test: Verify grid changes on tablet
  test('should display features in 2-column grid on tablet', async ({ page }) => {
    await page.setViewportSize({ width: 900, height: 800 });

    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    const gridColumns = await featuresGrid.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return style.gridTemplateColumns;
    });

    const columnCount = gridColumns.split(' ').filter(val => val.length > 0 && val !== 'none').length;
    expect(columnCount).toBe(2);
  });

  // Responsive test: Verify grid changes on mobile
  test('should display features in single column on mobile', async ({ page }) => {
    await page.setViewportSize({ width: 480, height: 800 });

    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    const gridColumns = await featuresGrid.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return style.gridTemplateColumns;
    });

    const columnCount = gridColumns.split(' ').filter(val => val.length > 0 && val !== 'none').length;
    expect(columnCount).toBe(1);
  });
});
