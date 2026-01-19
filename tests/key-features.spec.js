// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = 'file://' + path.resolve(__dirname, '../index.html');

test.describe('Key Features Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(indexPath);
  });

  // Test Case 1: Count feature card elements in features section (3-4 feature cards)
  test('TC1: 3-4 feature cards are present in the features section', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Count feature cards
    const featureCards = page.locator('#features .feature-card');
    const count = await featureCards.count();

    // Verify 3-4 feature cards
    expect(count).toBeGreaterThanOrEqual(3);
    expect(count).toBeLessThanOrEqual(4);
  });

  // Test Case 2: Check for Memcached Protocol Compatibility feature
  test('TC2: Feature card with Memcached protocol mention exists', async ({ page }) => {
    // Find the features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Look for feature card mentioning Memcached
    const memcachedFeature = page.locator('.feature-card').filter({
      hasText: /memcached/i
    });

    await expect(memcachedFeature).toBeVisible();

    // Verify it has a title mentioning Memcached
    const title = memcachedFeature.locator('h3');
    await expect(title).toBeVisible();
    const titleText = await title.textContent();
    expect(titleText.toLowerCase()).toContain('memcached');

    // Verify it has a description
    const description = memcachedFeature.locator('p');
    await expect(description).toBeVisible();
    const descText = await description.textContent();
    expect(descText.length).toBeGreaterThan(10);
  });

  // Test Case 3: Check for Persistent Storage feature
  test('TC3: Feature card mentioning Persistent Storage or SSTables exists', async ({ page }) => {
    // Find the features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Look for feature card mentioning Persistent Storage or SSTables
    const persistentFeature = page.locator('.feature-card').filter({
      hasText: /persistent|sstable/i
    });

    await expect(persistentFeature).toBeVisible();

    // Verify it has a title mentioning Persistent or Storage
    const title = persistentFeature.locator('h3');
    await expect(title).toBeVisible();
    const titleText = await title.textContent();
    const lowerTitle = titleText.toLowerCase();
    expect(lowerTitle.includes('persistent') || lowerTitle.includes('storage')).toBeTruthy();

    // Verify description mentions SSTables
    const description = persistentFeature.locator('p');
    await expect(description).toBeVisible();
    const descText = await description.textContent();
    expect(descText.toLowerCase()).toContain('sstable');
  });

  // Test Case 4: Check for LSM Tree Architecture feature
  test('TC4: Feature card mentioning LSM Tree architecture exists', async ({ page }) => {
    // Find the features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Look for feature card mentioning LSM Tree
    const lsmFeature = page.locator('.feature-card').filter({
      hasText: /lsm\s*tree/i
    });

    await expect(lsmFeature).toBeVisible();

    // Verify it has a title mentioning LSM Tree
    const title = lsmFeature.locator('h3');
    await expect(title).toBeVisible();
    const titleText = await title.textContent();
    expect(titleText.toLowerCase()).toContain('lsm');

    // Verify description exists
    const description = lsmFeature.locator('p');
    await expect(description).toBeVisible();
    const descText = await description.textContent();
    expect(descText.length).toBeGreaterThan(10);
  });

  // Test Case 5: Verify each feature card has an icon
  test('TC5: All feature cards contain a visual icon element', async ({ page }) => {
    // Find all feature cards
    const featureCards = page.locator('#features .feature-card');
    const count = await featureCards.count();

    // Verify each card has an icon
    for (let i = 0; i < count; i++) {
      const card = featureCards.nth(i);

      // Check for icon (either .feature-icon div or svg element)
      const iconDiv = card.locator('.feature-icon');
      const svgIcon = card.locator('svg');

      // At least one should be visible
      const hasIconDiv = await iconDiv.count() > 0;
      const hasSvg = await svgIcon.count() > 0;

      expect(hasIconDiv || hasSvg).toBeTruthy();

      if (hasIconDiv) {
        await expect(iconDiv).toBeVisible();
        // Verify the icon has content (either text emoji or svg)
        const iconContent = await iconDiv.textContent();
        const innerSvg = await iconDiv.locator('svg').count();
        expect(iconContent.trim().length > 0 || innerSvg > 0).toBeTruthy();
      }
    }
  });

  // Additional test: Features section is accessible via scroll
  test('Features section is visible after scrolling past hero', async ({ page }) => {
    // Set viewport
    await page.setViewportSize({ width: 1280, height: 720 });

    // Scroll to features section
    await page.locator('#features').scrollIntoViewIfNeeded();

    // Verify features section is visible
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Verify the heading is visible
    const heading = page.locator('#features h2');
    await expect(heading).toBeVisible();
    await expect(heading).toHaveText('Key Features');
  });
});
