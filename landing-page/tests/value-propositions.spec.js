// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

test.describe('Three Core Value Propositions Display', () => {
  test.beforeEach(async ({ page }) => {
    const filePath = path.resolve(__dirname, '../index.html');
    await page.goto(`file://${filePath}`);
  });

  // Test Case 1: Exactly 3 value proposition cards/sections are displayed
  test('TC1: Exactly 3 value proposition cards are displayed', async ({ page }) => {
    // Locate the features section
    const featuresSection = page.locator('[data-testid="features-section"]');
    await expect(featuresSection).toBeVisible();

    // Count distinct value proposition cards
    const featureCards = featuresSection.locator('[data-testid^="feature-card-"]');
    await expect(featureCards).toHaveCount(3);
  });

  // Test Case 2: Page contains content explaining Memcached protocol compatibility
  test('TC2: Page contains Memcached compatibility content', async ({ page }) => {
    const featuresSection = page.locator('[data-testid="features-section"]');
    await expect(featuresSection).toBeVisible();

    // Look for Memcached-related content
    const pageContent = await page.locator('body').textContent();

    // Check for Memcached keyword
    expect(pageContent.toLowerCase()).toContain('memcached');

    // Check for protocol compatibility explanation
    const hasCompatibilityContent =
      pageContent.toLowerCase().includes('protocol') ||
      pageContent.toLowerCase().includes('compatible') ||
      pageContent.toLowerCase().includes('compatibility');
    expect(hasCompatibilityContent).toBeTruthy();

    // Verify the specific Memcached feature card exists
    const memcachedCard = featuresSection.locator('[data-testid="feature-card-memcached"]');
    await expect(memcachedCard).toBeVisible();
  });

  // Test Case 3: Page contains content about persistent storage, SSTable, or durable data
  test('TC3: Page contains persistence-related content', async ({ page }) => {
    const featuresSection = page.locator('[data-testid="features-section"]');
    await expect(featuresSection).toBeVisible();

    const pageContent = await page.locator('body').textContent();

    // Check for persistence-related keywords
    const hasPersistenceContent =
      pageContent.toLowerCase().includes('persist') ||
      pageContent.toLowerCase().includes('sstable') ||
      pageContent.toLowerCase().includes('durable') ||
      pageContent.toLowerCase().includes('storage') ||
      pageContent.toLowerCase().includes('disk');
    expect(hasPersistenceContent).toBeTruthy();

    // Verify the specific persistence feature card exists
    const persistenceCard = featuresSection.locator('[data-testid="feature-card-persistence"]');
    await expect(persistenceCard).toBeVisible();
  });

  // Test Case 4: Page contains content about LSM tree architecture or storage engine
  test('TC4: Page contains LSM tree architecture content', async ({ page }) => {
    const featuresSection = page.locator('[data-testid="features-section"]');
    await expect(featuresSection).toBeVisible();

    const pageContent = await page.locator('body').textContent();

    // Check for LSM tree related keywords
    const hasLsmContent =
      pageContent.toLowerCase().includes('lsm') ||
      pageContent.toLowerCase().includes('log-structured') ||
      pageContent.toLowerCase().includes('storage engine') ||
      pageContent.toLowerCase().includes('architecture');
    expect(hasLsmContent).toBeTruthy();

    // Verify the specific LSM feature card exists
    const lsmCard = featuresSection.locator('[data-testid="feature-card-lsm"]');
    await expect(lsmCard).toBeVisible();
  });

  // Test Case 5: Features are displayed in a three-column grid layout on desktop
  test('TC5: Features are displayed in three-column grid layout on desktop', async ({ page }) => {
    // Set viewport to desktop size
    await page.setViewportSize({ width: 1920, height: 1080 });

    const featuresSection = page.locator('[data-testid="features-section"]');
    await expect(featuresSection).toBeVisible();

    // Get the features grid container
    const featuresGrid = featuresSection.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    // Verify grid layout using CSS
    const gridStyle = await featuresGrid.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        display: style.display,
        gridTemplateColumns: style.gridTemplateColumns
      };
    });

    // Check that it's a grid layout
    expect(gridStyle.display).toBe('grid');

    // Check for three columns (either explicit 3 columns or auto-fit with min-max)
    // The gridTemplateColumns should have 3 values or a repeat pattern resulting in 3 columns
    const columns = gridStyle.gridTemplateColumns.split(' ').filter(col => col && col !== '0px');
    expect(columns.length).toBe(3);

    // Verify all three feature cards are in view and positioned horizontally
    const featureCards = featuresSection.locator('[data-testid^="feature-card-"]');
    const cardCount = await featureCards.count();
    expect(cardCount).toBe(3);

    // Get bounding boxes for all cards to verify horizontal layout
    const boundingBoxes = [];
    for (let i = 0; i < cardCount; i++) {
      const box = await featureCards.nth(i).boundingBox();
      expect(box).not.toBeNull();
      boundingBoxes.push(box);
    }

    // Verify cards are roughly on the same vertical level (within 50px tolerance)
    const firstCardY = boundingBoxes[0].y;
    for (const box of boundingBoxes) {
      expect(Math.abs(box.y - firstCardY)).toBeLessThan(50);
    }

    // Verify cards are positioned horizontally (each card's x position increases)
    for (let i = 1; i < boundingBoxes.length; i++) {
      expect(boundingBoxes[i].x).toBeGreaterThan(boundingBoxes[i - 1].x);
    }
  });

  // Additional test: Each feature card has an icon
  test('TC6: Each feature card has an icon or illustration', async ({ page }) => {
    const featuresSection = page.locator('[data-testid="features-section"]');
    await expect(featuresSection).toBeVisible();

    const featureCards = featuresSection.locator('[data-testid^="feature-card-"]');
    const cardCount = await featureCards.count();

    expect(cardCount).toBe(3);

    // Each card should have an icon element
    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i);
      const icon = card.locator('.feature-icon');
      await expect(icon).toBeVisible();
    }
  });
});
