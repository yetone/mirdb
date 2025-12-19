// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = 'file://' + path.resolve(__dirname, '../index.html');

test.describe('Features Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(indexPath);
  });

  // Test Case 1: Query DOM for features section
  test('features section exists with proper id or aria-label', async ({ page }) => {
    // Check for section with id 'features' or appropriate aria-label
    const featuresSection = page.locator('section#features, [aria-label*="features" i], #features');
    await expect(featuresSection).toBeVisible();
  });

  // Test Case 2: Count feature cards in features section
  test('at least 3 feature cards are present', async ({ page }) => {
    const featuresSection = page.locator('#features');
    const featureCards = featuresSection.locator('.feature-card, .feature, [class*="card"]');
    await expect(featureCards).toHaveCount(await featureCards.count());
    const count = await featureCards.count();
    expect(count).toBeGreaterThanOrEqual(3);
  });

  // Test Case 3: Search for memcached-related content
  test('memcached compatibility feature is present', async ({ page }) => {
    const featuresSection = page.locator('#features');
    const textContent = await featuresSection.textContent();
    const lowerText = textContent.toLowerCase();

    // Check for 'memcached' and 'compatible' or 'protocol'
    expect(lowerText).toContain('memcached');
    expect(lowerText.includes('compatible') || lowerText.includes('protocol')).toBeTruthy();
  });

  // Test Case 4: Search for persistence-related content
  test('persistence feature is present', async ({ page }) => {
    const featuresSection = page.locator('#features');
    const textContent = await featuresSection.textContent();
    const lowerText = textContent.toLowerCase();

    // Check for 'persistent' or 'persistence' and 'storage' or 'sstable'
    const hasPersistence = lowerText.includes('persistent') || lowerText.includes('persistence');
    const hasStorage = lowerText.includes('storage') || lowerText.includes('sstable');
    expect(hasPersistence).toBeTruthy();
    expect(hasStorage).toBeTruthy();
  });

  // Test Case 5: Search for LSM architecture content
  test('LSM architecture feature is present', async ({ page }) => {
    const featuresSection = page.locator('#features');
    const textContent = await featuresSection.textContent();
    const lowerText = textContent.toLowerCase();

    // Check for 'LSM' and 'architecture' or 'tree'
    expect(lowerText).toContain('lsm');
    expect(lowerText.includes('architecture') || lowerText.includes('tree')).toBeTruthy();
  });

  // Test Case 6: Verify each feature card has title and description
  test('each feature card contains heading and description', async ({ page }) => {
    const featuresSection = page.locator('#features');
    const featureCards = featuresSection.locator('.feature-card');
    const count = await featureCards.count();

    expect(count).toBeGreaterThanOrEqual(3);

    for (let i = 0; i < count; i++) {
      const card = featureCards.nth(i);
      // Check for heading element (h2, h3, h4, or element with heading role)
      const heading = card.locator('h2, h3, h4, [role="heading"]');
      await expect(heading).toBeVisible();

      // Check for paragraph/description text
      const description = card.locator('p, .description, [class*="desc"]');
      await expect(description).toBeVisible();
    }
  });
});
