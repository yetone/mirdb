// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const pageUrl = 'file://' + path.join(__dirname, '..', 'index.html');

test.describe('Key Features Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(pageUrl);
  });

  test('TC1: Memcached Protocol Compatible feature card is displayed', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Find the memcached feature card
    const memcachedCard = page.locator('[data-testid="feature-memcached"]');
    await expect(memcachedCard).toBeVisible();

    // Verify title
    const title = memcachedCard.locator('h3');
    await expect(title).toHaveText('Memcached Protocol Compatible');

    // Verify description contains information about existing client compatibility
    const description = memcachedCard.locator('p');
    await expect(description).toContainText('existing memcached clients');
    await expect(description).toContainText('seamless');
  });

  test('TC2: Persistent Storage feature card is displayed', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Find the persistent storage feature card
    const persistentCard = page.locator('[data-testid="feature-persistent"]');
    await expect(persistentCard).toBeVisible();

    // Verify title
    const title = persistentCard.locator('h3');
    await expect(title).toHaveText('Persistent Storage');

    // Verify description mentions data surviving restarts and SSTables
    const description = persistentCard.locator('p');
    await expect(description).toContainText('survives restarts');
    await expect(description).toContainText('SSTables');
  });

  test('TC3: LSM Tree Architecture feature card is displayed', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Find the LSM tree feature card
    const lsmCard = page.locator('[data-testid="feature-lsm"]');
    await expect(lsmCard).toBeVisible();

    // Verify title
    const title = lsmCard.locator('h3');
    await expect(title).toHaveText('LSM Tree Architecture');

    // Verify description mentions write performance and compaction
    const description = lsmCard.locator('p');
    await expect(description).toContainText('write performance');
    await expect(description).toContainText('compaction');
  });

  test('TC4: Feature cards have visual elements (icons)', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Check that all three feature cards have icons
    const featureCards = [
      page.locator('[data-testid="feature-memcached"]'),
      page.locator('[data-testid="feature-persistent"]'),
      page.locator('[data-testid="feature-lsm"]')
    ];

    for (const card of featureCards) {
      await expect(card).toBeVisible();

      // Check for feature-icon div containing SVG
      const icon = card.locator('.feature-icon');
      await expect(icon).toBeVisible();

      // Verify there's an SVG inside the icon container
      const svg = icon.locator('svg');
      await expect(svg).toBeVisible();
    }
  });

  test('Features section has correct heading', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    const heading = featuresSection.locator('h2');
    await expect(heading).toHaveText('Key Features');
  });

  test('Features section contains three feature cards', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    const featureCards = featuresSection.locator('.feature-card');
    await expect(featureCards).toHaveCount(3);
  });
});
