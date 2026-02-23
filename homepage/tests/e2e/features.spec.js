// @ts-check
import { test, expect } from '@playwright/test';

/**
 * Features Section E2E Tests
 * Owner: Scenario 3 - Features Section
 *
 * End-to-end tests for features section:
 * - Section is present and visible
 * - All implemented features are listed
 * - Feature descriptions are visible
 * - Proper feature categorization
 */

test.describe('Features Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Features section is present and visible', async ({ page }) => {
    // Check Features section exists
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Check section has proper heading
    const heading = page.locator('#features-heading');
    await expect(heading).toBeVisible();
    await expect(heading).toContainText('Features');
  });

  test('TC2: Tokio async runtime feature is listed with description', async ({ page }) => {
    const featuresSection = page.locator('#features');

    // Check for Tokio feature
    const tokioFeature = featuresSection.locator('.feature-card', {
      has: page.locator('.feature-title:has-text("Tokio")')
    });
    await expect(tokioFeature).toBeVisible();

    // Check feature has description mentioning async
    const description = tokioFeature.locator('.feature-description');
    await expect(description).toBeVisible();
    await expect(description).toContainText('async');
  });

  test('TC3: Skip-list memtable feature is listed', async ({ page }) => {
    const featuresSection = page.locator('#features');

    // Check for Skip-list memtable feature
    const skipListFeature = featuresSection.locator('.feature-card', {
      has: page.locator('.feature-title:has-text("Skip-List")')
    });
    await expect(skipListFeature).toBeVisible();

    // Check feature has description mentioning memtable
    const description = skipListFeature.locator('.feature-description');
    await expect(description).toBeVisible();
    await expect(description).toContainText('memtable');
  });

  test('TC4: Minor compaction feature is listed', async ({ page }) => {
    const featuresSection = page.locator('#features');

    // Check for Minor compaction feature
    const minorCompactionFeature = featuresSection.locator('.feature-card', {
      has: page.locator('.feature-title:has-text("Minor Compaction")')
    });
    await expect(minorCompactionFeature).toBeVisible();

    // Check feature has description
    const description = minorCompactionFeature.locator('.feature-description');
    await expect(description).toBeVisible();
    await expect(description).not.toBeEmpty();
  });

  test('TC5: Major compaction feature is listed', async ({ page }) => {
    const featuresSection = page.locator('#features');

    // Check for Major compaction feature
    const majorCompactionFeature = featuresSection.locator('.feature-card', {
      has: page.locator('.feature-title:has-text("Major Compaction")')
    });
    await expect(majorCompactionFeature).toBeVisible();

    // Check feature has description
    const description = majorCompactionFeature.locator('.feature-description');
    await expect(description).toBeVisible();
    await expect(description).not.toBeEmpty();
  });

  test('TC6: Write-ahead log feature is listed for durability', async ({ page }) => {
    const featuresSection = page.locator('#features');

    // Check for Write-Ahead Log feature
    const walFeature = featuresSection.locator('.feature-card', {
      has: page.locator('.feature-title:has-text("Write-Ahead Log")')
    });
    await expect(walFeature).toBeVisible();

    // Check feature has description mentioning durability or recovery
    const description = walFeature.locator('.feature-description');
    await expect(description).toBeVisible();
    await expect(description).toContainText(/durable|durability|recovery/i);
  });

  test('TC7: Memcached protocol feature is listed', async ({ page }) => {
    const featuresSection = page.locator('#features');

    // Check for Memcached protocol feature
    const memcachedFeature = featuresSection.locator('.feature-card', {
      has: page.locator('.feature-title:has-text("Memcached")')
    });
    await expect(memcachedFeature).toBeVisible();

    // Check feature has description mentioning protocol or compatibility
    const description = memcachedFeature.locator('.feature-description');
    await expect(description).toBeVisible();
    await expect(description).toContainText(/protocol|compatibility|compatible/i);
  });

  test('All six features are displayed', async ({ page }) => {
    const featuresSection = page.locator('#features');
    const features = featuresSection.locator('.feature-card');

    // Should have exactly 6 implemented features
    await expect(features).toHaveCount(6);
  });

  test('Features section follows Quick Start section', async ({ page }) => {
    // Get bounding boxes to verify order
    const quickstartSection = page.locator('#quickstart');
    const featuresSection = page.locator('#features');

    const quickstartBox = await quickstartSection.boundingBox();
    const featuresBox = await featuresSection.boundingBox();

    expect(quickstartBox).not.toBeNull();
    expect(featuresBox).not.toBeNull();

    // Features should be below Quick Start (higher Y value)
    expect(featuresBox.y).toBeGreaterThan(quickstartBox.y);
  });

  test('Features section has accessible structure', async ({ page }) => {
    const featuresSection = page.locator('section#features');

    // Check for aria-labelledby
    await expect(featuresSection).toHaveAttribute('aria-labelledby', 'features-heading');

    // Check each feature is an article
    const featureArticles = featuresSection.locator('article.feature-card');
    await expect(featureArticles).toHaveCount(6);

    // Each feature should have a heading
    for (let i = 0; i < 6; i++) {
      const featureHeading = featureArticles.nth(i).locator('h3.feature-title');
      await expect(featureHeading).toBeVisible();
    }
  });
});
