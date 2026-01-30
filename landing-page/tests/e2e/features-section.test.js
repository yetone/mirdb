/**
 * MirDB Landing Page - Features Section E2E Tests
 * Owner: Scenario 2 - Features Section Grid
 *
 * Tests verify:
 * - Exactly 4 feature cards are present
 * - Memcached Compatibility feature card content
 * - Persistent Storage feature card content
 * - High Performance feature card content
 * - Configurable feature card content
 * - Each feature card has an icon or visual indicator
 */

const { test, expect } = require('@playwright/test');
const { setupPage } = require('../helpers/test-utils');

test.describe('Features Section Grid', () => {
  test.beforeEach(async ({ page }) => {
    await setupPage(page);
  });

  test('TC-1: Exactly 4 feature cards are present', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Count feature cards
    const featureCards = page.locator('.feature-card');
    await expect(featureCards).toHaveCount(4);
  });

  test('TC-2: Memcached Compatibility feature explains clients work without modification', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Find the Memcached Compatible feature card
    const memcachedCard = page.locator('.feature-card', { has: page.locator('h3', { hasText: /Memcached Compatible/i }) });
    await expect(memcachedCard).toBeVisible();

    // Verify the description mentions clients work without modification
    const description = memcachedCard.locator('.feature-description');
    await expect(description).toContainText(/existing memcached clients work without modification/i);
  });

  test('TC-3: Persistent Storage feature mentions LSM-tree architecture and SSTable-based storage', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Find the Persistent Storage feature card
    const persistentCard = page.locator('.feature-card', { has: page.locator('h3', { hasText: /Persistent Storage/i }) });
    await expect(persistentCard).toBeVisible();

    // Verify the description mentions LSM-tree and SSTable
    const description = persistentCard.locator('.feature-description');
    await expect(description).toContainText(/LSM-tree/i);
    await expect(description).toContainText(/SSTable/i);
  });

  test('TC-4: High Performance feature mentions skip-list memtable and compaction strategies', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Find the High Performance feature card
    const performanceCard = page.locator('.feature-card', { has: page.locator('h3', { hasText: /High Performance/i }) });
    await expect(performanceCard).toBeVisible();

    // Verify the description mentions skip-list memtable and compaction
    const description = performanceCard.locator('.feature-description');
    await expect(description).toContainText(/skip-list memtable/i);
    await expect(description).toContainText(/compaction/i);
  });

  test('TC-5: Configurable feature mentions configuration options for deployment scenarios', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Find the Configurable feature card
    const configCard = page.locator('.feature-card', { has: page.locator('h3', { hasText: /Configurable/i }) });
    await expect(configCard).toBeVisible();

    // Verify the description mentions configuration options and deployment
    const description = configCard.locator('.feature-description');
    await expect(description).toContainText(/configuration options/i);
    await expect(description).toContainText(/deployment/i);
  });

  test('TC-6: Each feature card has an icon or visual indicator', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Get all feature cards
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();

    // Each card should have exactly 4 cards
    expect(cardCount).toBe(4);

    // Verify each card has a feature icon
    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i);
      const icon = card.locator('.feature-icon');
      await expect(icon).toBeVisible();

      // Verify the icon contains an SVG element
      const svg = icon.locator('svg');
      await expect(svg).toBeVisible();
    }
  });

  test('Features section is accessible via navigation', async ({ page }) => {
    // Click on Features nav link
    const featuresLink = page.locator('.nav-links a[href="#features"]');
    await featuresLink.click();

    // Verify the features section is in viewport
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();
  });

  test('Feature cards have proper ARIA labels for accessibility', async ({ page }) => {
    // Verify the features section has proper aria-labelledby
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toHaveAttribute('aria-labelledby', 'features-title');

    // Verify each feature card is an article with aria-labelledby
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();

    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i);
      await expect(card).toHaveAttribute('aria-labelledby');
    }
  });
});
