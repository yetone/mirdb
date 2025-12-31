import { test, expect } from '@playwright/test';

test.describe('Key Features Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Check for Memcached Protocol feature', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Check for Memcached Protocol feature card
    const featureCards = featuresSection.locator('.feature-card');
    const memcachedCard = featureCards.filter({ hasText: /Memcached Protocol/i });
    await expect(memcachedCard).toBeVisible();

    // Verify description mentions compatibility with standard memcached clients
    const description = memcachedCard.locator('p');
    await expect(description).toContainText(/compatible|compatibility|standard.*memcached.*client|memcached.*client/i);
  });

  test('TC2: Check for Disk Persistence feature', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Check for Persistence feature card
    const featureCards = featuresSection.locator('.feature-card');
    const persistenceCard = featureCards.filter({ hasText: /Persistence/i });
    await expect(persistenceCard).toBeVisible();

    // Verify description mentions SSTables and durability
    const description = persistenceCard.locator('p');
    await expect(description).toContainText(/SSTable|durability|durable|persist/i);
  });

  test('TC3: Check for LSM Tree Architecture feature', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Check for LSM Tree feature card
    const featureCards = featuresSection.locator('.feature-card');
    const lsmTreeCard = featureCards.filter({ hasText: /LSM Tree/i });
    await expect(lsmTreeCard).toBeVisible();

    // Verify description mentions multi-level compaction
    const description = lsmTreeCard.locator('p');
    await expect(description).toContainText(/compaction|multi-level|level/i);
  });

  test('TC4: Check for Async Networking feature', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Check for Async Networking feature card
    const featureCards = featuresSection.locator('.feature-card');
    const asyncCard = featureCards.filter({ hasText: /Async.*Networking|Networking/i });
    await expect(asyncCard).toBeVisible();

    // Verify description mentions Tokio
    const description = asyncCard.locator('p');
    await expect(description).toContainText(/Tokio/i);
  });

  test('TC5: Check for Configurable Parameters feature', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Check for Configurable Parameters feature card
    const featureCards = featuresSection.locator('.feature-card');
    const configCard = featureCards.filter({ hasText: /Configurable|Parameters|Configuration/i });
    await expect(configCard).toBeVisible();

    // Verify description mentions configurable performance parameters
    const description = configCard.locator('p');
    await expect(description).toContainText(/configurable|parameters|performance|tuning|customize/i);
  });

  test('TC6: Count total number of feature items', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Count feature cards
    const featureCards = featuresSection.locator('.feature-card');
    await expect(featureCards).toHaveCount(5);
  });
});
