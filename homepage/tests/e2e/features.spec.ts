/**
 * Features Section E2E Tests
 * Owner: Scenario 3 - Features Section Display
 *
 * Tests:
 * - Feature cards count (4 cards)
 * - Individual feature content (title, description)
 * - Feature icons presence
 * - Responsive grid layout (2x2 desktop, 1 column mobile)
 */
import { test, expect } from '@playwright/test';

test.describe('Features Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case 1: Query features section for feature cards - Exactly 4 feature cards are present
  test('should display exactly 4 feature cards', async ({ page }) => {
    const featureCards = page.locator('[data-testid="feature-card"]');
    await expect(featureCards).toHaveCount(4);
  });

  // Test Case 2: Check for Memcached Protocol feature card
  test('should display Memcached Protocol feature card with correct content', async ({ page }) => {
    const memcachedCard = page.locator('.feature-card').filter({ hasText: 'Memcached Protocol' });
    await expect(memcachedCard).toBeVisible();

    const title = memcachedCard.locator('.feature-title');
    await expect(title).toHaveText('Memcached Protocol');

    const description = memcachedCard.locator('.feature-description');
    await expect(description).toContainText('protocol compatibility');
  });

  // Test Case 3: Check for Durable Storage feature card
  test('should display Durable Storage feature card with correct content', async ({ page }) => {
    const storageCard = page.locator('.feature-card').filter({ hasText: 'Durable Storage' });
    await expect(storageCard).toBeVisible();

    const title = storageCard.locator('.feature-title');
    await expect(title).toHaveText('Durable Storage');

    const description = storageCard.locator('.feature-description');
    await expect(description).toContainText('SSTable');
  });

  // Test Case 4: Check for LSM Tree Architecture feature card
  test('should display LSM Tree feature card with correct content', async ({ page }) => {
    const lsmCard = page.locator('.feature-card').filter({ hasText: 'LSM Tree' });
    await expect(lsmCard).toBeVisible();

    const title = lsmCard.locator('.feature-title');
    await expect(title).toHaveText('LSM Tree');

    const description = lsmCard.locator('.feature-description');
    await expect(description).toContainText('write performance');
  });

  // Test Case 5: Check for Rust Powered feature card
  test('should display Rust Powered feature card with correct content', async ({ page }) => {
    const rustCard = page.locator('.feature-card').filter({ hasText: 'Rust Powered' });
    await expect(rustCard).toBeVisible();

    const title = rustCard.locator('.feature-title');
    await expect(title).toHaveText('Rust Powered');

    const description = rustCard.locator('.feature-description');
    await expect(description).toContainText('Memory-safe');
  });

  // Test Case 6: Verify each feature card has an icon element
  test('should display icons on all feature cards', async ({ page }) => {
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();

    expect(cardCount).toBe(4);

    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i);
      const icon = card.locator('.feature-icon img, .feature-icon svg');
      await expect(icon).toBeVisible();
    }
  });

  // Test Case 7: Check grid layout on desktop (1024px width)
  test('should display features in 2x2 grid on desktop', async ({ page }) => {
    await page.setViewportSize({ width: 1024, height: 768 });

    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    // Check grid has 2 columns using computed styles
    const gridTemplateColumns = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).gridTemplateColumns;
    });

    // Should have 2 columns (computed as pixel values)
    const columnCount = gridTemplateColumns.split(' ').filter(c => c.includes('px') || c.includes('fr')).length;
    expect(columnCount).toBe(2);
  });

  // Test Case 8: Check grid layout on mobile (375px width)
  test('should display features in single column on mobile', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 });

    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    // Check grid has 1 column on mobile
    const gridTemplateColumns = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).gridTemplateColumns;
    });

    // Should have 1 column on mobile
    const columnCount = gridTemplateColumns.split(' ').filter(c => c.includes('px') || c.includes('fr')).length;
    expect(columnCount).toBe(1);
  });
});
