// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for Features Section Display
 * Scenario: Verify that core features are showcased with visual explanations as specified in REQ-3
 */

test.describe('Features Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 1: Check for GET/SET/DELETE operations feature', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();

    // Find the feature card for GET/SET/DELETE operations
    const operationsCard = page.locator('[data-testid="feature-operations"]');
    await expect(operationsCard).toBeVisible();

    // Verify the card displays GET/SET/DELETE operations with TTL support
    const cardTitle = operationsCard.locator('h3');
    await expect(cardTitle).toContainText('GET/SET/DELETE');

    const cardDescription = operationsCard.locator('p');
    await expect(cardDescription).toContainText('TTL');
    await expect(cardDescription).toContainText('Time-To-Live');

    // Verify visual representation (icon)
    const icon = operationsCard.locator('.feature-icon svg');
    await expect(icon).toBeVisible();
  });

  test('Test Case 2: Check for background compaction feature', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();

    // Find the feature card for background compaction
    const compactionCard = page.locator('[data-testid="feature-compaction"]');
    await expect(compactionCard).toBeVisible();

    // Verify the card mentions minor and major compaction
    const cardTitle = compactionCard.locator('h3');
    await expect(cardTitle).toContainText('Compaction');

    const cardDescription = compactionCard.locator('p');
    await expect(cardDescription).toContainText('minor');
    await expect(cardDescription).toContainText('major');
    await expect(cardDescription).toContainText('compaction');

    // Verify visual representation (icon)
    const icon = compactionCard.locator('.feature-icon svg');
    await expect(icon).toBeVisible();
  });

  test('Test Case 3: Check for write-ahead logging feature', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();

    // Find the feature card for WAL
    const walCard = page.locator('[data-testid="feature-wal"]');
    await expect(walCard).toBeVisible();

    // Verify the card explains WAL for crash recovery
    const cardTitle = walCard.locator('h3');
    await expect(cardTitle).toContainText('Write-Ahead Logging');

    const cardDescription = walCard.locator('p');
    await expect(cardDescription).toContainText('WAL');
    await expect(cardDescription).toContainText('crash recovery');

    // Verify visual representation (icon)
    const icon = walCard.locator('.feature-icon svg');
    await expect(icon).toBeVisible();
  });

  test('Test Case 4: Check for TOML configuration feature', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();

    // Find the feature card for TOML configuration
    const tomlCard = page.locator('[data-testid="feature-toml"]');
    await expect(tomlCard).toBeVisible();

    // Verify the card mentions configurable via TOML
    const cardTitle = tomlCard.locator('h3');
    await expect(cardTitle).toContainText('TOML');
    await expect(cardTitle).toContainText('Configuration');

    const cardDescription = tomlCard.locator('p');
    await expect(cardDescription).toContainText('TOML');
    await expect(cardDescription).toContainText('configuration');

    // Verify visual representation (icon)
    const icon = tomlCard.locator('.feature-icon svg');
    await expect(icon).toBeVisible();
  });

  test('Test Case 5: Check for storage levels feature', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();

    // Find the feature card for storage levels
    const storageLevelsCard = page.locator('[data-testid="feature-storage-levels"]');
    await expect(storageLevelsCard).toBeVisible();

    // Verify the card describes multiple storage levels with optimized merging
    const cardTitle = storageLevelsCard.locator('h3');
    await expect(cardTitle).toContainText('Storage Levels');

    const cardDescription = storageLevelsCard.locator('p');
    await expect(cardDescription).toContainText('LSM');
    await expect(cardDescription).toContainText('level');
    await expect(cardDescription).toContainText('merging');

    // Verify visual representation (icon)
    const icon = storageLevelsCard.locator('.feature-icon svg');
    await expect(icon).toBeVisible();
  });

  test('Features section follows value proposition section', async ({ page }) => {
    // Verify the features section comes after the value proposition section
    const valuePropSection = page.locator('#value-proposition');
    const featuresSection = page.locator('#features');

    await expect(valuePropSection).toBeVisible();
    await expect(featuresSection).toBeVisible();

    // Get bounding boxes to verify order
    const valuePropBox = await valuePropSection.boundingBox();
    const featuresBox = await featuresSection.boundingBox();

    expect(valuePropBox).toBeTruthy();
    expect(featuresBox).toBeTruthy();

    // Features section should be below value proposition section
    expect(featuresBox.y).toBeGreaterThan(valuePropBox.y);
  });

  test('All feature cards have visual icons', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();

    // Check all feature cards have icons
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();

    expect(cardCount).toBeGreaterThanOrEqual(5);

    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i);
      const icon = card.locator('.feature-icon svg');
      await expect(icon).toBeVisible();
    }
  });
});
