import { test, expect } from '@playwright/test';

test.describe('Features Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Check for Memcached protocol feature', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Find the Memcached feature card
    const memcachedCard = page.locator('[data-feature="memcached"]');
    await expect(memcachedCard).toBeVisible();

    // Verify title contains 'Memcached Compatible'
    const title = memcachedCard.locator('h3');
    await expect(title).toHaveText('Memcached Compatible');

    // Verify description mentions client compatibility
    const description = memcachedCard.locator('p');
    await expect(description).toContainText('existing memcached clients');
    await expect(description).toContainText('Drop-in replacement');
  });

  test('TC2: Check for persistence feature', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Find the persistence feature card
    const persistenceCard = page.locator('[data-feature="persistence"]');
    await expect(persistenceCard).toBeVisible();

    // Verify title contains 'Built-in Persistence'
    const title = persistenceCard.locator('h3');
    await expect(title).toHaveText('Built-in Persistence');

    // Verify description mentions SSTables
    const description = persistenceCard.locator('p');
    await expect(description).toContainText('SSTables');
    await expect(description).toContainText('durability');
  });

  test('TC3: Check for LSM-tree feature', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Find the LSM-tree feature card
    const lsmTreeCard = page.locator('[data-feature="lsm-tree"]');
    await expect(lsmTreeCard).toBeVisible();

    // Verify title contains 'LSM-Tree'
    const title = lsmTreeCard.locator('h3');
    await expect(title).toHaveText('LSM-Tree Architecture');

    // Verify description mentions write optimization
    const description = lsmTreeCard.locator('p');
    await expect(description).toContainText('Write-optimized');
    await expect(description).toContainText('compaction');
  });

  test('TC4: Check for configuration feature', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Find the configuration feature card
    const configCard = page.locator('[data-feature="configuration"]');
    await expect(configCard).toBeVisible();

    // Verify title contains 'Configuration'
    const title = configCard.locator('h3');
    await expect(title).toContainText('Configuration');

    // Verify description mentions TOML
    const description = configCard.locator('p');
    await expect(description).toContainText('TOML');
    await expect(description).toContainText('defaults');
  });

  test('TC5: Verify feature layout structure', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Verify features grid exists
    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    // Verify all 4 feature cards exist with consistent styling
    const featureCards = page.locator('.feature-card');
    await expect(featureCards).toHaveCount(4);

    // Verify grid layout is applied (check CSS grid display)
    const gridDisplay = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });
    expect(gridDisplay).toBe('grid');

    // Verify each card has consistent structure (icon, title, description)
    for (let i = 0; i < 4; i++) {
      const card = featureCards.nth(i);
      await expect(card.locator('.feature-icon')).toBeVisible();
      await expect(card.locator('h3')).toBeVisible();
      await expect(card.locator('p')).toBeVisible();
    }

    // Verify cards have consistent styling (border, padding via box shadow)
    const firstCard = featureCards.first();
    const cardBorder = await firstCard.evaluate((el) => {
      return window.getComputedStyle(el).borderWidth;
    });
    expect(cardBorder).toBe('1px');
  });
});
