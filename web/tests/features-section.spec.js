// @ts-check
import { test, expect } from '@playwright/test';

test.describe('Key Features Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 1: Features section exists and is visible', async ({ page }) => {
    // Query features section
    const featuresSection = page.locator('#features, [data-testid="features-section"], section.features');
    await expect(featuresSection).toBeVisible();
  });

  test('Test Case 2: 3-4 feature cards are displayed', async ({ page }) => {
    // Count feature cards
    const featureCards = page.locator('.feature-card, [data-testid="feature-card"]');
    const count = await featureCards.count();
    expect(count).toBeGreaterThanOrEqual(3);
    expect(count).toBeLessThanOrEqual(4);
  });

  test('Test Case 3: Memcached Protocol feature card exists with icon', async ({ page }) => {
    // Check for Memcached Protocol feature
    const memcachedCard = page.locator('.feature-card, [data-testid="feature-card"]').filter({
      hasText: /memcached/i
    });
    await expect(memcachedCard).toBeVisible();

    // Verify icon exists (SVG or img element)
    const icon = memcachedCard.locator('svg').first();
    await expect(icon).toBeVisible();
  });

  test('Test Case 4: Persistent Storage feature card with LSM Tree exists', async ({ page }) => {
    // Check for Persistent Storage feature
    const storageCard = page.locator('.feature-card, [data-testid="feature-card"]').filter({
      hasText: /persistent|storage|lsm/i
    });
    await expect(storageCard).toBeVisible();

    // Verify icon exists
    const icon = storageCard.locator('svg').first();
    await expect(icon).toBeVisible();
  });

  test('Test Case 5: High Performance feature card highlighting Rust exists', async ({ page }) => {
    // Check for High Performance feature with Rust mention
    const performanceCard = page.locator('.feature-card, [data-testid="feature-card"]').filter({
      hasText: /performance|rust|fast/i
    });
    await expect(performanceCard).toBeVisible();

    // Verify icon exists
    const icon = performanceCard.locator('svg').first();
    await expect(icon).toBeVisible();
  });

  test('Test Case 6: Easy Configuration feature card exists', async ({ page }) => {
    // Check for Easy Configuration feature
    const configCard = page.locator('.feature-card, [data-testid="feature-card"]').filter({
      hasText: /config|easy|simple/i
    });
    await expect(configCard).toBeVisible();

    // Verify icon exists
    const icon = configCard.locator('svg').first();
    await expect(icon).toBeVisible();
  });
});
