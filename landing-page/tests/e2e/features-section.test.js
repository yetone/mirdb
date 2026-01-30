/**
 * MirDB Landing Page - Features Section E2E Tests
 * Owner: Scenario 2 - Features Section Grid
 *
 * Test cases:
 * 1. Count feature cards in features section - Exactly 4 feature cards are present
 * 2. Check for Memcached Compatibility feature - explains that existing memcached clients work without modification
 * 3. Check for Persistent Storage feature - mentions LSM-tree architecture and SSTable-based storage
 * 4. Check for High Performance feature - mentions skip-list memtable and compaction strategies
 * 5. Check for Configurable feature - mentions configuration options for deployment scenarios
 * 6. Verify feature cards have icons or visual indicators - each feature card has an associated icon
 */

const { test, expect } = require('@playwright/test');
const { setupPage } = require('../helpers/test-utils');

test.describe('Features Section Grid', () => {
  test.beforeEach(async ({ page }) => {
    await setupPage(page);
  });

  test('TC1: Count feature cards - exactly 4 feature cards are present', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Count feature cards
    const featureCards = featuresSection.locator('.feature-card');
    await expect(featureCards).toHaveCount(4);
  });

  test('TC2: Memcached Compatibility feature - explains clients work without modification', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Find the Memcached Compatible feature card
    const memcachedCard = featuresSection.locator('.feature-card', {
      has: page.locator('h3:has-text("Memcached Compatible")')
    });
    await expect(memcachedCard).toBeVisible();

    // Check for description about existing clients working without modification
    const description = memcachedCard.locator('.feature-description');
    await expect(description).toContainText(/existing memcached clients work without modification/i);
  });

  test('TC3: Persistent Storage feature - mentions LSM-tree and SSTable-based storage', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Find the Persistent Storage feature card
    const persistentCard = featuresSection.locator('.feature-card', {
      has: page.locator('h3:has-text("Persistent Storage")')
    });
    await expect(persistentCard).toBeVisible();

    // Check for description mentioning LSM-tree and SSTable
    const description = persistentCard.locator('.feature-description');
    await expect(description).toContainText(/LSM-tree/i);
    await expect(description).toContainText(/SSTable/i);
  });

  test('TC4: High Performance feature - mentions skip-list memtable and compaction strategies', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Find the High Performance feature card
    const performanceCard = featuresSection.locator('.feature-card', {
      has: page.locator('h3:has-text("High Performance")')
    });
    await expect(performanceCard).toBeVisible();

    // Check for description mentioning skip-list and compaction
    const description = performanceCard.locator('.feature-description');
    await expect(description).toContainText(/skip-list/i);
    await expect(description).toContainText(/compaction/i);
  });

  test('TC5: Configurable feature - mentions configuration options for deployment scenarios', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Find the Configurable feature card
    const configurableCard = featuresSection.locator('.feature-card', {
      has: page.locator('h3:has-text("Configurable")')
    });
    await expect(configurableCard).toBeVisible();

    // Check for description mentioning configuration options and deployment
    const description = configurableCard.locator('.feature-description');
    await expect(description).toContainText(/configuration options/i);
    await expect(description).toContainText(/deployment/i);
  });

  test('TC6: Feature cards have icons or visual indicators', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Get all feature cards
    const featureCards = featuresSection.locator('.feature-card');
    const count = await featureCards.count();

    // Each feature card should have an icon
    for (let i = 0; i < count; i++) {
      const card = featureCards.nth(i);
      const icon = card.locator('.feature-icon');
      await expect(icon).toBeVisible();

      // Check that the icon contains an SVG element
      const svg = icon.locator('svg');
      await expect(svg).toBeVisible();
    }
  });

  test('Features section is accessible from navigation', async ({ page }) => {
    // Click on Features link in navigation
    const featuresLink = page.locator('.nav-link[href="#features"]');
    await expect(featuresLink).toBeVisible();
    await featuresLink.click();

    // Verify we navigated to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();
  });

  test('Feature cards have proper heading structure', async ({ page }) => {
    const featuresSection = page.locator('#features');

    // Check for section heading
    const sectionTitle = featuresSection.locator('#features-title');
    await expect(sectionTitle).toBeVisible();
    await expect(sectionTitle).toHaveText('Key Features');

    // Each card should have an h3 heading
    const cardHeadings = featuresSection.locator('.feature-card h3');
    await expect(cardHeadings).toHaveCount(4);
  });

  test('Feature cards have proper ARIA labels for icons', async ({ page }) => {
    const featuresSection = page.locator('#features');
    const featureCards = featuresSection.locator('.feature-card');
    const count = await featureCards.count();

    for (let i = 0; i < count; i++) {
      const card = featureCards.nth(i);
      const iconContainer = card.locator('.feature-icon');

      // Icon container should have aria-hidden
      await expect(iconContainer).toHaveAttribute('aria-hidden', 'true');
    }
  });
});
