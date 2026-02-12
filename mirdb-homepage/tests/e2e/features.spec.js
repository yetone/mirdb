/**
 * Features Section E2E Tests
 * Owner: Scenario 2 - Features Section Display
 *
 * Tests for validating that all key MirDB features are displayed
 * as specified in REQ-3 and US-2.
 */

const { test, expect } = require('@playwright/test');

test.describe('Features Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  test('features section exists with correct id', async ({ page }) => {
    // Test case 1: Check for features section element
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Verify it's a section element
    const tagName = await featuresSection.evaluate(el => el.tagName.toLowerCase());
    expect(tagName).toBe('section');
  });

  test('features section has heading', async ({ page }) => {
    const heading = page.locator('#features h2');
    await expect(heading).toBeVisible();
    await expect(heading).toContainText('Features');
  });

  test('Memcached protocol feature is displayed', async ({ page }) => {
    // Test case 2: Search for Memcached protocol feature
    const featuresSection = page.locator('#features');
    const featureText = await featuresSection.textContent();

    expect(featureText.toLowerCase()).toContain('memcached');
    expect(featureText.toLowerCase()).toContain('protocol');
  });

  test('persistent storage feature is displayed', async ({ page }) => {
    // Test case 3: Search for persistent storage feature
    const featuresSection = page.locator('#features');
    const featureText = await featuresSection.textContent();

    expect(featureText.toLowerCase()).toContain('persistent');
    expect(featureText.toLowerCase()).toContain('storage');
  });

  test('skip-list memtable feature is displayed', async ({ page }) => {
    // Test case 4: Search for skip-list memtable feature
    const featuresSection = page.locator('#features');
    const featureText = await featuresSection.textContent();

    const hasSkipList = featureText.toLowerCase().includes('skip-list') ||
                        featureText.toLowerCase().includes('skiplist');
    expect(hasSkipList).toBe(true);
    expect(featureText.toLowerCase()).toContain('memtable');
  });

  test('compaction feature is displayed', async ({ page }) => {
    // Test case 5: Search for compaction feature
    const featuresSection = page.locator('#features');
    const featureText = await featuresSection.textContent();

    expect(featureText.toLowerCase()).toContain('compaction');
  });

  test('Rust implementation is mentioned', async ({ page }) => {
    // Test case 6: Search for Rust implementation mention
    const featuresSection = page.locator('#features');
    const featureText = await featuresSection.textContent();

    expect(featureText).toContain('Rust');
  });

  test('features are displayed in grid layout', async ({ page }) => {
    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    // Check that multiple feature cards exist
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(5);
  });

  test('each feature card has title and description', async ({ page }) => {
    const featureCards = page.locator('.feature-card');
    const count = await featureCards.count();

    for (let i = 0; i < count; i++) {
      const card = featureCards.nth(i);
      const title = card.locator('h3');
      const description = card.locator('p');

      await expect(title).toBeVisible();
      await expect(description).toBeVisible();

      const titleText = await title.textContent();
      const descText = await description.textContent();

      expect(titleText.length).toBeGreaterThan(0);
      expect(descText.length).toBeGreaterThan(0);
    }
  });

  test('features section is navigable via anchor', async ({ page }) => {
    await page.goto('/#features');
    await page.waitForLoadState('domcontentloaded');

    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();
  });

  test('all five key features are present', async ({ page }) => {
    const featuresSection = page.locator('#features');
    const text = await featuresSection.textContent();
    const lowerText = text.toLowerCase();

    // Verify all 5 required features
    const features = [
      { name: 'Memcached Protocol', check: () => lowerText.includes('memcached') && lowerText.includes('protocol') },
      { name: 'Persistent Storage', check: () => lowerText.includes('persistent') && lowerText.includes('storage') },
      { name: 'Skip-list Memtable', check: () => (lowerText.includes('skip-list') || lowerText.includes('skiplist')) && lowerText.includes('memtable') },
      { name: 'Compaction', check: () => lowerText.includes('compaction') },
      { name: 'Rust', check: () => text.includes('Rust') }
    ];

    for (const feature of features) {
      expect(feature.check(), `Feature "${feature.name}" should be present`).toBe(true);
    }
  });
});
