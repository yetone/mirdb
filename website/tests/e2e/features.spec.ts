/**
 * Features Section E2E Tests
 * Owner: Scenario 2 - Feature Overview Section
 *
 * Tests the features section displaying four key MirDB capabilities:
 * - Tokio with Memcached Protocol
 * - Memtable with Skiplist
 * - Minor Compaction
 * - Major Compaction
 */

import { test, expect } from '@playwright/test';

test.describe('Feature Overview Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('features section exists with appropriate semantic heading (h2)', async ({ page }) => {
    // Test Case 1: Query for features section element
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Check for h2 heading
    const heading = featuresSection.locator('h2');
    await expect(heading).toBeVisible();
    await expect(heading).toHaveText('Key Features');
  });

  test('exactly 4 feature items are present', async ({ page }) => {
    // Test Case 2: Count feature items in the features section
    const featureCards = page.locator('#features .feature-card');
    await expect(featureCards).toHaveCount(4);
  });

  test('tokio with memcached protocol feature exists with description', async ({ page }) => {
    // Test Case 3: Query for feature containing 'tokio' or 'memcached protocol'
    const featuresSection = page.locator('#features');

    // Find the feature card with tokio/memcached content
    const tokioFeature = featuresSection.locator('.feature-card').filter({
      has: page.locator('.feature-title', { hasText: /tokio|memcached/i })
    });

    await expect(tokioFeature).toBeVisible();

    // Verify it has a description
    const description = tokioFeature.locator('.feature-description');
    await expect(description).toBeVisible();
    await expect(description).not.toBeEmpty();
  });

  test('memtable with skiplist feature exists with description', async ({ page }) => {
    // Test Case 4: Query for feature containing 'memtable' or 'skiplist'
    const featuresSection = page.locator('#features');

    // Find the feature card with memtable/skiplist content
    const memtableFeature = featuresSection.locator('.feature-card').filter({
      has: page.locator('.feature-title', { hasText: /memtable|skiplist/i })
    });

    await expect(memtableFeature).toBeVisible();

    // Verify it has a description
    const description = memtableFeature.locator('.feature-description');
    await expect(description).toBeVisible();
    await expect(description).not.toBeEmpty();
  });

  test('minor compaction feature exists with description', async ({ page }) => {
    // Test Case 5: Query for feature containing 'minor compaction'
    const featuresSection = page.locator('#features');

    // Find the feature card with minor compaction content
    const minorCompactionFeature = featuresSection.locator('.feature-card').filter({
      has: page.locator('.feature-title', { hasText: /minor compaction/i })
    });

    await expect(minorCompactionFeature).toBeVisible();

    // Verify it has a description
    const description = minorCompactionFeature.locator('.feature-description');
    await expect(description).toBeVisible();
    await expect(description).not.toBeEmpty();
  });

  test('major compaction feature exists with description', async ({ page }) => {
    // Test Case 6: Query for feature containing 'major compaction'
    const featuresSection = page.locator('#features');

    // Find the feature card with major compaction content
    const majorCompactionFeature = featuresSection.locator('.feature-card').filter({
      has: page.locator('.feature-title', { hasText: /major compaction/i })
    });

    await expect(majorCompactionFeature).toBeVisible();

    // Verify it has a description
    const description = majorCompactionFeature.locator('.feature-description');
    await expect(description).toBeVisible();
    await expect(description).not.toBeEmpty();
  });

  test('features have distinct boundaries (spacing, borders, or cards)', async ({ page }) => {
    // Test Case 7: Check visual separation between features
    const featureCards = page.locator('#features .feature-card');

    // Verify we have exactly 4 cards
    await expect(featureCards).toHaveCount(4);

    // Check each card has visual styling that creates separation
    for (let i = 0; i < 4; i++) {
      const card = featureCards.nth(i);

      // Check card has border or background to create visual separation
      const border = await card.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return style.border || style.borderWidth;
      });

      const backgroundColor = await card.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return style.backgroundColor;
      });

      const padding = await card.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return parseFloat(style.padding) ||
               parseFloat(style.paddingTop) + parseFloat(style.paddingBottom);
      });

      // Card should have at least one of: border, distinct background, or padding
      const hasBorder = border && !border.includes('0px');
      const hasBackground = backgroundColor && backgroundColor !== 'rgba(0, 0, 0, 0)';
      const hasPadding = padding > 0;

      expect(hasBorder || hasBackground || hasPadding).toBeTruthy();
    }

    // Check that the grid has gap spacing between cards
    const featuresGrid = page.locator('#features .features-grid');
    const gridGap = await featuresGrid.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return style.gap || style.gridGap || style.columnGap;
    });

    // Grid should have some gap/spacing
    expect(gridGap).toBeTruthy();
    expect(gridGap).not.toBe('0px');
  });
});
