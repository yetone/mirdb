/**
 * Features Section E2E Tests
 * Owner: Scenario 2 - Features Section Display
 *
 * Tests the features section displays all key MirDB features
 * with proper icons and descriptions per REQ-3.
 */

const { test, expect } = require('@playwright/test');

test.describe('Features Section Display (Scenario 2)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case 1: Features section exists with heading
  test('TC1: Features section exists with heading "Features" or similar', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Check for a features heading (could be visible or sr-only)
    const featuresHeading = page.locator('#features-title, .features__title, .features h2');
    await expect(featuresHeading).toHaveCount(1);

    const headingText = await featuresHeading.textContent();
    expect(headingText.toLowerCase()).toContain('features');
  });

  // Test Case 2: Between 4 and 6 feature cards are present
  test('TC2: Between 4 and 6 feature cards are present', async ({ page }) => {
    const featureCards = page.locator('.features__card, .feature-card');
    const count = await featureCards.count();

    expect(count).toBeGreaterThanOrEqual(4);
    expect(count).toBeLessThanOrEqual(6);
  });

  // Test Case 3: Persistent Storage feature exists
  test('TC3: Feature card with title/description about persistent storage exists', async ({ page }) => {
    const featuresSection = page.locator('#features');
    const sectionText = await featuresSection.textContent();

    // Check for persistent storage mention (case-insensitive)
    expect(sectionText.toLowerCase()).toContain('persistent');

    // Verify there's a card with this content
    const persistentCard = page.locator('.features__card, .feature-card').filter({
      hasText: /persistent/i
    });
    await expect(persistentCard.first()).toBeVisible();
  });

  // Test Case 4: Memcached Protocol feature exists
  test('TC4: Feature card with title/description about Memcached protocol compatibility exists', async ({ page }) => {
    const featuresSection = page.locator('#features');
    const sectionText = await featuresSection.textContent();

    // Check for Memcached mention
    expect(sectionText.toLowerCase()).toContain('memcached');

    // Verify there's a card with this content
    const memcachedCard = page.locator('.features__card, .feature-card').filter({
      hasText: /memcached/i
    });
    await expect(memcachedCard.first()).toBeVisible();
  });

  // Test Case 5: LSM-tree feature exists
  test('TC5: Feature card with title/description about LSM-tree storage engine exists', async ({ page }) => {
    const featuresSection = page.locator('#features');
    const sectionText = await featuresSection.textContent();

    // Check for LSM-tree mention
    expect(sectionText.toLowerCase()).toContain('lsm');

    // Verify there's a card with this content
    const lsmCard = page.locator('.features__card, .feature-card').filter({
      hasText: /lsm/i
    });
    await expect(lsmCard.first()).toBeVisible();
  });

  // Test Case 6: Skip-list feature exists
  test('TC6: Feature card with title/description about skip-list memtable exists', async ({ page }) => {
    const featuresSection = page.locator('#features');
    const sectionText = await featuresSection.textContent();

    // Check for skip-list mention
    expect(sectionText.toLowerCase()).toContain('skip');

    // Verify there's a card with this content
    const skipListCard = page.locator('.features__card, .feature-card').filter({
      hasText: /skip/i
    });
    await expect(skipListCard.first()).toBeVisible();
  });

  // Test Case 7: Compaction feature exists
  test('TC7: Feature card with title/description about compaction support exists', async ({ page }) => {
    const featuresSection = page.locator('#features');
    const sectionText = await featuresSection.textContent();

    // Check for compaction mention
    expect(sectionText.toLowerCase()).toContain('compaction');

    // Verify there's a card with this content
    const compactionCard = page.locator('.features__card, .feature-card').filter({
      hasText: /compaction/i
    });
    await expect(compactionCard.first()).toBeVisible();
  });

  // Test Case 8: Each feature card has an associated icon
  test('TC8: Each feature card has an associated icon (SVG or image)', async ({ page }) => {
    const featureCards = page.locator('.features__card, .feature-card');
    const count = await featureCards.count();

    expect(count).toBeGreaterThan(0);

    // Check each card has an icon
    for (let i = 0; i < count; i++) {
      const card = featureCards.nth(i);
      const icon = card.locator('svg, img, .features__icon, .feature-icon');
      await expect(icon.first()).toBeVisible();
    }
  });

  // Additional test: Feature cards have titles
  test('Each feature card has a title element', async ({ page }) => {
    const featureCards = page.locator('.features__card, .feature-card');
    const count = await featureCards.count();

    for (let i = 0; i < count; i++) {
      const card = featureCards.nth(i);
      const title = card.locator('h3, .features__card-title, .feature-title');
      await expect(title.first()).toBeVisible();
    }
  });

  // Additional test: Feature cards have descriptions
  test('Each feature card has a description element', async ({ page }) => {
    const featureCards = page.locator('.features__card, .feature-card');
    const count = await featureCards.count();

    for (let i = 0; i < count; i++) {
      const card = featureCards.nth(i);
      const description = card.locator('p, .features__card-description, .feature-description');
      await expect(description.first()).toBeVisible();
    }
  });
});
