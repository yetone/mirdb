/**
 * E2E tests for Features Section.
 * Owner: Scenario 3 - Features Section
 *
 * Tests:
 * - Features section renders with correct heading
 * - At least 4 feature cards are present
 * - Each feature card has correct title and description
 * - Grid layout is applied at desktop width
 */

const { test, expect } = require('@playwright/test');

const filePath = 'file://' + process.cwd() + '/index.html';

test.describe('Features Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(filePath);
  });

  test('Features section exists with h2 heading "Features"', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    const heading = featuresSection.locator('h2');
    await expect(heading).toBeVisible();
    await expect(heading).toHaveText('Features');
  });

  test('At least 4 feature cards are present', async ({ page }) => {
    const featureCards = page.locator('#features .feature-card');
    await expect(featureCards).toHaveCount(4);
  });

  test('Memcached Protocol feature card exists with correct content', async ({ page }) => {
    const card = page.locator('[data-testid="feature-memcached"]');
    await expect(card).toBeVisible();

    const title = card.locator('h3');
    await expect(title).toContainText('Memcached');

    const description = card.locator('p');
    await expect(description).toContainText('seamlessly');
  });

  test('Persistence feature card exists with correct content', async ({ page }) => {
    const card = page.locator('[data-testid="feature-persistence"]');
    await expect(card).toBeVisible();

    const title = card.locator('h3');
    await expect(title).toContainText('Persistence');

    const description = card.locator('p');
    await expect(description).toContainText('SSTable');
  });

  test('LSM Tree feature card exists with correct content', async ({ page }) => {
    const card = page.locator('[data-testid="feature-lsm-tree"]');
    await expect(card).toBeVisible();

    const title = card.locator('h3');
    await expect(title).toContainText('LSM');

    const description = card.locator('p');
    await expect(description).toContainText('compaction');
  });

  test('Async Networking feature card exists with correct content', async ({ page }) => {
    const card = page.locator('[data-testid="feature-async-networking"]');
    await expect(card).toBeVisible();

    const title = card.locator('h3');
    await expect(title).toContainText('Async');

    const description = card.locator('p');
    await expect(description).toContainText('Tokio');
  });

  test('Feature cards display in a grid layout at desktop width', async ({ page }) => {
    await page.setViewportSize({ width: 1280, height: 800 });

    const grid = page.locator('#features .features-grid');
    await expect(grid).toBeVisible();

    const gridStyles = await grid.evaluate((el) => {
      const computed = window.getComputedStyle(el);
      return {
        display: computed.display,
        gridTemplateColumns: computed.gridTemplateColumns,
      };
    });

    expect(gridStyles.display).toBe('grid');

    // Verify at least 2 columns (split into multiple track values)
    const columns = gridStyles.gridTemplateColumns.split(' ');
    expect(columns.length).toBeGreaterThanOrEqual(2);
  });
});
