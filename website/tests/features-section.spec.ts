import { test, expect } from '@playwright/test';

/**
 * E2E Tests for Key Features Section
 * Tests validate that the features section displays all key MirDB capabilities
 * with clear descriptions as per PRD requirements.
 */

test.describe('Key Features Section', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to the homepage
    await page.goto('file://' + process.cwd() + '/index.html');
  });

  test('TC1: Check for Memcached Protocol Compatibility feature', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Find the Memcached Protocol Compatibility feature card
    const memcachedCard = page.locator('.feature-card').filter({
      has: page.locator('text=/memcached protocol/i')
    });

    await expect(memcachedCard).toBeVisible();

    // Verify title exists
    const cardTitle = memcachedCard.locator('.feature-title, h3');
    await expect(cardTitle).toBeVisible();
    await expect(cardTitle).toContainText(/memcached/i);

    // Verify description about memcached protocol support
    const cardDescription = memcachedCard.locator('.feature-description, p');
    await expect(cardDescription).toBeVisible();
    await expect(cardDescription).toContainText(/memcached|protocol|compatibility|client/i);
  });

  test('TC2: Check for Persistent Storage feature', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Find the Persistent Storage feature card
    const storageCard = page.locator('.feature-card').filter({
      has: page.locator('text=/persistent storage|sstable/i')
    });

    await expect(storageCard).toBeVisible();

    // Verify title exists
    const cardTitle = storageCard.locator('.feature-title, h3');
    await expect(cardTitle).toBeVisible();
    await expect(cardTitle).toContainText(/persistent|storage/i);

    // Verify description mentions SSTables and durable storage
    const cardDescription = storageCard.locator('.feature-description, p');
    await expect(cardDescription).toBeVisible();
    await expect(cardDescription).toContainText(/sstable|durable|persist/i);
  });

  test('TC3: Check for LSM Tree Architecture feature', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Find the LSM Tree Architecture feature card
    const lsmCard = page.locator('.feature-card').filter({
      has: page.locator('text=/lsm tree|lsm-tree/i')
    });

    await expect(lsmCard).toBeVisible();

    // Verify title exists
    const cardTitle = lsmCard.locator('.feature-title, h3');
    await expect(cardTitle).toBeVisible();
    await expect(cardTitle).toContainText(/lsm/i);

    // Verify description explains LSM tree design for write optimization
    const cardDescription = lsmCard.locator('.feature-description, p');
    await expect(cardDescription).toBeVisible();
    await expect(cardDescription).toContainText(/write|optim|throughput|performance/i);
  });

  test('TC4: Check for Async Networking feature', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Find the Async Networking feature card
    const asyncCard = page.locator('.feature-card').filter({
      has: page.locator('text=/async|tokio|networking/i')
    });

    await expect(asyncCard).toBeVisible();

    // Verify title exists
    const cardTitle = asyncCard.locator('.feature-title, h3');
    await expect(cardTitle).toBeVisible();
    await expect(cardTitle).toContainText(/async|networking/i);

    // Verify description mentions Tokio-based async networking
    const cardDescription = asyncCard.locator('.feature-description, p');
    await expect(cardDescription).toBeVisible();
    await expect(cardDescription).toContainText(/tokio|async|concurrent|performance/i);
  });

  test('TC5: Verify feature card layout consistency', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Get all feature cards
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();

    // Expect at least 4 feature cards (the 4 main features)
    expect(cardCount).toBeGreaterThanOrEqual(4);

    // Verify each card has icon, title, and description arranged consistently
    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i);

      // Check for icon container with icon inside (feature-icon class wraps svg)
      const iconContainer = card.locator('.feature-icon');
      await expect(iconContainer).toBeVisible();

      // Check for title
      const title = card.locator('.feature-title');
      await expect(title).toBeVisible();

      // Check for description
      const description = card.locator('.feature-description');
      await expect(description).toBeVisible();
    }
  });
});
