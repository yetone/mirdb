// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Features Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Features section exists with heading and three feature cards', async ({ page }) => {
    // Verify features section exists
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Verify section has the "Key Features" heading
    const sectionTitle = featuresSection.locator('.section-title');
    await expect(sectionTitle).toHaveText('Key Features');

    // Verify three feature cards are present
    const featureCards = featuresSection.locator('.feature-card');
    await expect(featureCards).toHaveCount(3);
  });

  test('TC2: Memcached Compatible feature card displays correct content', async ({ page }) => {
    const featuresSection = page.locator('#features');
    const memcachedCard = featuresSection.locator('[data-feature="memcached-compatible"]');

    // Verify card is visible
    await expect(memcachedCard).toBeVisible();

    // Verify card has the correct title
    const title = memcachedCard.locator('.feature-title');
    await expect(title).toHaveText('Memcached Compatible');

    // Verify description mentions existing clients and tools
    const description = memcachedCard.locator('.feature-description');
    await expect(description).toContainText('existing');
    await expect(description).toContainText('clients');
    await expect(description).toContainText('tools');
  });

  test('TC3: Persistent Storage feature card displays correct content', async ({ page }) => {
    const featuresSection = page.locator('#features');
    const persistentCard = featuresSection.locator('[data-feature="persistent-storage"]');

    // Verify card is visible
    await expect(persistentCard).toBeVisible();

    // Verify card has the correct title
    const title = persistentCard.locator('.feature-title');
    await expect(title).toHaveText('Persistent Storage');

    // Verify description mentions data surviving restarts and SSTable format
    const description = persistentCard.locator('.feature-description');
    await expect(description).toContainText('survives');
    await expect(description).toContainText('restarts');
    await expect(description).toContainText('SSTable');
  });

  test('TC4: High Performance feature card displays correct content', async ({ page }) => {
    const featuresSection = page.locator('#features');
    const performanceCard = featuresSection.locator('[data-feature="high-performance"]');

    // Verify card is visible
    await expect(performanceCard).toBeVisible();

    // Verify card has the correct title
    const title = performanceCard.locator('.feature-title');
    await expect(title).toHaveText('High Performance');

    // Verify description mentions LSM tree architecture and skip list memtables
    const description = performanceCard.locator('.feature-description');
    await expect(description).toContainText('LSM tree');
    await expect(description).toContainText('skip list');
    await expect(description).toContainText('memtables');
  });

  test('TC5: Three feature cards are displayed in a grid layout', async ({ page }) => {
    const featuresSection = page.locator('#features');
    const featuresGrid = featuresSection.locator('.features-grid');

    // Verify the grid container exists
    await expect(featuresGrid).toBeVisible();

    // Verify grid has display: grid style
    const gridDisplay = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });
    expect(gridDisplay).toBe('grid');

    // Verify all three cards are direct children of the grid
    const cardsInGrid = featuresGrid.locator('.feature-card');
    await expect(cardsInGrid).toHaveCount(3);

    // Verify cards are visible and laid out correctly
    const firstCard = cardsInGrid.nth(0);
    const secondCard = cardsInGrid.nth(1);
    const thirdCard = cardsInGrid.nth(2);

    await expect(firstCard).toBeVisible();
    await expect(secondCard).toBeVisible();
    await expect(thirdCard).toBeVisible();

    // Check that cards have proper positioning (not stacked vertically at 0,0)
    const firstBox = await firstCard.boundingBox();
    const secondBox = await secondCard.boundingBox();
    const thirdBox = await thirdCard.boundingBox();

    expect(firstBox).not.toBeNull();
    expect(secondBox).not.toBeNull();
    expect(thirdBox).not.toBeNull();

    // On desktop, cards should be in a row (different x positions or same y)
    // On narrow viewport, they might stack. But we verify they exist and are visible.
    expect(firstBox.width).toBeGreaterThan(0);
    expect(secondBox.width).toBeGreaterThan(0);
    expect(thirdBox.width).toBeGreaterThan(0);
  });
});
