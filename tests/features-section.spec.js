// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Key Features Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Features section contains three distinct feature cards/columns', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Check for three-column grid layout
    const featuresGrid = featuresSection.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    // Verify three distinct feature cards exist
    const featureCards = featuresSection.locator('.feature-card');
    await expect(featureCards).toHaveCount(3);

    // Verify each card has a title and description
    for (let i = 0; i < 3; i++) {
      const card = featureCards.nth(i);
      await expect(card.locator('h3')).toBeVisible();
      await expect(card.locator('p')).toBeVisible();
    }
  });

  test('TC2: Memcached compatibility feature card is present with correct content', async ({ page }) => {
    const featuresSection = page.locator('#features');

    // Find the memcached feature card
    const memcachedCard = featuresSection.locator('.feature-card[data-feature="memcached"]');
    await expect(memcachedCard).toBeVisible();

    // Verify title contains 'Memcached'
    const title = memcachedCard.locator('h3');
    await expect(title).toContainText('Memcached');

    // Verify description mentions existing clients/tooling compatibility
    const description = memcachedCard.locator('p');
    const descText = await description.textContent();
    expect(descText).toMatch(/existing clients|tooling|client/i);
  });

  test('TC3: Persistence feature card is present with correct content', async ({ page }) => {
    const featuresSection = page.locator('#features');

    // Find the persistent storage feature card
    const persistentCard = featuresSection.locator('.feature-card[data-feature="persistent"]');
    await expect(persistentCard).toBeVisible();

    // Verify title contains 'Persistent'
    const title = persistentCard.locator('h3');
    await expect(title).toContainText('Persistent');

    // Verify description mentions data surviving restarts and SSTable storage
    const description = persistentCard.locator('p');
    const descText = await description.textContent();
    expect(descText).toMatch(/survives? restarts?/i);
    expect(descText).toMatch(/SSTable/i);
  });

  test('TC4: Performance feature card is present with correct content', async ({ page }) => {
    const featuresSection = page.locator('#features');

    // Find the performance feature card
    const performanceCard = featuresSection.locator('.feature-card[data-feature="performance"]');
    await expect(performanceCard).toBeVisible();

    // Verify title contains 'Performance'
    const title = performanceCard.locator('h3');
    await expect(title).toContainText('Performance');

    // Verify description mentions LSM tree architecture
    const description = performanceCard.locator('p');
    const descText = await description.textContent();
    expect(descText).toMatch(/LSM tree/i);
  });

  test('Features section has three-column layout on desktop', async ({ page }) => {
    // Set desktop viewport
    await page.setViewportSize({ width: 1200, height: 800 });

    const featuresGrid = page.locator('.features-grid');

    // Check that the grid is using 3 columns
    const gridStyle = await featuresGrid.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        display: style.display,
        gridTemplateColumns: style.gridTemplateColumns,
      };
    });

    expect(gridStyle.display).toBe('grid');
    // gridTemplateColumns should have 3 values (3 columns)
    const columnCount = gridStyle.gridTemplateColumns.split(' ').length;
    expect(columnCount).toBe(3);
  });
});
