/**
 * Features Display Tests
 * Owner: Scenario 2 - Key Features Display
 *
 * Test cases:
 * - Features section heading exists
 * - All 7 features are displayed (Memcached, LSM-tree, Skip-list, WAL, SSTable, Compaction, TTL)
 * - Feature cards have proper structure
 */

import { test, expect } from '@playwright/test';

test.describe('Key Features Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  // Test Case 1: Check for Memcached protocol feature card
  test('should display Memcached protocol feature card', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Look for a feature card containing both 'Memcached' and 'protocol'
    const memcachedCard = featuresSection.locator('.feature-card').filter({
      hasText: /Memcached/i
    }).filter({
      hasText: /protocol/i
    });

    await expect(memcachedCard).toBeVisible();
    await expect(memcachedCard).toContainText('Memcached');
    await expect(memcachedCard).toContainText('protocol');
  });

  // Test Case 2: Check for LSM-tree feature card
  test('should display LSM-tree feature card', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Look for a feature card containing 'LSM-tree' or 'Log-Structured Merge'
    const lsmCard = featuresSection.locator('.feature-card').filter({
      hasText: /LSM-tree|Log-Structured Merge/i
    });

    await expect(lsmCard).toBeVisible();
  });

  // Test Case 3: Check for Skip-list memtable feature card
  test('should display Skip-list memtable feature card', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Look for a feature card containing 'Skip-list' or 'memtable'
    const skipListCard = featuresSection.locator('.feature-card').filter({
      hasText: /Skip-list|memtable/i
    });

    await expect(skipListCard).toBeVisible();
  });

  // Test Case 4: Check for WAL durability feature card
  test('should display WAL durability feature card', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Look for a feature card containing 'WAL' or 'Write-Ahead Log' or 'durability'
    const walCard = featuresSection.locator('.feature-card').filter({
      hasText: /WAL|Write-Ahead Log|durability/i
    });

    await expect(walCard).toBeVisible();
  });

  // Test Case 5: Check for SSTable feature card
  test('should display SSTable feature card', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Look for a feature card with 'SSTable' in its heading (h3)
    const sstableCard = featuresSection.locator('.feature-card').filter({
      has: page.locator('h3', { hasText: /SSTable/i })
    });

    await expect(sstableCard).toBeVisible();
    await expect(sstableCard).toContainText('SSTable');
  });

  // Test Case 6: Check for compaction feature card
  test('should display compaction feature card', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Look for a feature card containing 'compaction'
    const compactionCard = featuresSection.locator('.feature-card').filter({
      hasText: /compaction/i
    });

    await expect(compactionCard).toBeVisible();
  });

  // Test Case 7: Check for TTL feature card
  test('should display TTL feature card', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Look for a feature card containing 'TTL' or 'time-to-live'
    const ttlCard = featuresSection.locator('.feature-card').filter({
      hasText: /TTL|time-to-live/i
    });

    await expect(ttlCard).toBeVisible();
  });

  // Test Case 8: Verify features section has proper heading
  test('should have features section with proper h2 heading', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Check for h2 heading with text like 'Features' or 'Key Features'
    const heading = featuresSection.locator('h2');
    await expect(heading).toBeVisible();
    await expect(heading).toContainText(/Features|Key Features/i);
  });

  // Additional test: Verify all 7 feature cards are present
  test('should display exactly 7 feature cards', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    const featureCards = featuresSection.locator('.feature-card');
    await expect(featureCards).toHaveCount(7);
  });

  // Additional test: Each feature card has a title (h3) and description (p)
  test('each feature card should have title and description', async ({ page }) => {
    const featuresSection = page.locator('#features');
    const featureCards = featuresSection.locator('.feature-card');
    const count = await featureCards.count();

    for (let i = 0; i < count; i++) {
      const card = featureCards.nth(i);
      const title = card.locator('h3');
      const description = card.locator('p');

      await expect(title).toBeVisible();
      await expect(description).toBeVisible();
    }
  });
});
