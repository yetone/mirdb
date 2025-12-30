// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = 'file://' + path.resolve(__dirname, '..', 'index.html');

test.describe('Key Features Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(indexPath);
  });

  test('TC1: Features section contains at least 3 distinct feature elements', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Verify at least 3 feature cards exist
    const featureCards = page.locator('.feature-card');
    const count = await featureCards.count();
    expect(count).toBeGreaterThanOrEqual(3);

    // Verify each feature card is visually distinct (has unique data-feature attribute)
    const features = await featureCards.evaluateAll(cards =>
      cards.map(card => card.getAttribute('data-feature'))
    );
    const uniqueFeatures = new Set(features);
    expect(uniqueFeatures.size).toBeGreaterThanOrEqual(3);
  });

  test('TC2: Memcached compatibility feature is present with correct content', async ({ page }) => {
    // Locate the features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Find the Memcached feature card
    const memcachedCard = page.locator('.feature-card[data-feature="memcached"]');
    await expect(memcachedCard).toBeVisible();

    // Verify the title contains "Memcached"
    const title = memcachedCard.locator('.feature-title');
    await expect(title).toContainText('Memcached');

    // Verify the description mentions client compatibility
    const description = memcachedCard.locator('.feature-description');
    const descriptionText = await description.textContent();
    expect(descriptionText).toContain('client');
    expect(descriptionText.toLowerCase()).toContain('memcached');
  });

  test('TC3: Persistence feature is present with SSTable/storage mention', async ({ page }) => {
    // Locate the features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Find the Persistence feature card
    const persistenceCard = page.locator('.feature-card[data-feature="persistence"]');
    await expect(persistenceCard).toBeVisible();

    // Verify the title contains "Persistent" or "Persistence"
    const title = persistenceCard.locator('.feature-title');
    const titleText = await title.textContent();
    const hasPersistenceInTitle = titleText.toLowerCase().includes('persist');
    expect(hasPersistenceInTitle).toBe(true);

    // Verify the description mentions SSTable or storage
    const description = persistenceCard.locator('.feature-description');
    const descriptionText = await description.textContent();
    const hasSSTable = descriptionText.includes('SSTable');
    const hasStorage = descriptionText.toLowerCase().includes('storage');
    expect(hasSSTable || hasStorage).toBe(true);
  });

  test('TC4: LSM tree feature is present with performance/Tokio mention', async ({ page }) => {
    // Locate the features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Find the LSM feature card
    const lsmCard = page.locator('.feature-card[data-feature="lsm"]');
    await expect(lsmCard).toBeVisible();

    // Verify the title contains "LSM"
    const title = lsmCard.locator('.feature-title');
    const titleText = await title.textContent();
    expect(titleText).toContain('LSM');

    // Verify the description mentions performance or Tokio
    const description = lsmCard.locator('.feature-description');
    const descriptionText = await description.textContent();
    const hasPerformance = descriptionText.toLowerCase().includes('performance');
    const hasTokio = descriptionText.includes('Tokio');
    expect(hasPerformance || hasTokio).toBe(true);
  });
});
