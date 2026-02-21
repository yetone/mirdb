// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E tests for Features Section
 * Owner: Scenario 3
 *
 * Test cases:
 * - Features section heading
 * - Feature card count (minimum 3)
 * - Memcached, Persistence, LSM Tree features present
 * - Responsive grid layout at different viewports
 */

test.describe('Features Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('features section exists with heading', async ({ page }) => {
    // Test case 1: Check features section exists with heading
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    const heading = featuresSection.locator('h2');
    await expect(heading).toBeVisible();
    await expect(heading).toHaveText(/Features/i);
  });

  test('at least 3 feature cards are displayed', async ({ page }) => {
    // Test case 2: Count feature cards
    const featureCards = page.locator('.feature-card');
    await expect(featureCards).toHaveCount(3);
  });

  test('Memcached protocol feature is present', async ({ page }) => {
    // Test case 3: Verify Memcached protocol feature
    const memcachedCard = page.locator('[data-testid="feature-card-memcached"]');
    await expect(memcachedCard).toBeVisible();

    // Check title mentions Memcached
    const title = memcachedCard.locator('h3');
    await expect(title).toContainText(/Memcached/i);

    // Check description mentions protocol or client compatibility
    const description = memcachedCard.locator('p');
    const descText = await description.textContent();
    expect(descText.toLowerCase()).toMatch(/protocol|client/i);
  });

  test('Persistence feature is present', async ({ page }) => {
    // Test case 4: Verify Persistence feature
    const persistenceCard = page.locator('[data-testid="feature-card-persistence"]');
    await expect(persistenceCard).toBeVisible();

    // Check title mentions Persistence
    const title = persistenceCard.locator('h3');
    await expect(title).toContainText(/Persistence/i);

    // Check description mentions persistence, disk, or SSTable
    const description = persistenceCard.locator('p');
    const descText = await description.textContent();
    expect(descText.toLowerCase()).toMatch(/persist|disk|sstable/i);
  });

  test('LSM Tree feature is present', async ({ page }) => {
    // Test case 5: Verify LSM Tree feature
    const lsmCard = page.locator('[data-testid="feature-card-lsm"]');
    await expect(lsmCard).toBeVisible();

    // Check title mentions LSM Tree
    const title = lsmCard.locator('h3');
    await expect(title).toContainText(/LSM Tree/i);

    // Check description mentions LSM tree or Log-Structured Merge
    const description = lsmCard.locator('p');
    const descText = await description.textContent();
    expect(descText.toLowerCase()).toMatch(/lsm|log-structured merge/i);
  });

  test('feature cards stack vertically on mobile (320px)', async ({ page }) => {
    // Test case 6: Test responsive grid layout at 320px width
    await page.setViewportSize({ width: 320, height: 800 });

    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();

    // Get positions of all cards
    const positions = [];
    for (let i = 0; i < cardCount; i++) {
      const box = await featureCards.nth(i).boundingBox();
      positions.push(box);
    }

    // Verify cards are stacked vertically (each card starts at similar X position)
    // and they're positioned below each other (Y increases)
    for (let i = 1; i < positions.length; i++) {
      // Cards should be at similar X position (stacked vertically)
      expect(Math.abs(positions[i].x - positions[0].x)).toBeLessThan(20);
      // Each subsequent card should be below the previous one
      expect(positions[i].y).toBeGreaterThan(positions[i - 1].y);
    }
  });

  test('feature cards display in 2-column grid on tablet (768px)', async ({ page }) => {
    // Test case 7: Test responsive grid layout at 768px width
    await page.setViewportSize({ width: 768, height: 1024 });
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();

    // Get positions of first two cards
    const card1Box = await featureCards.nth(0).boundingBox();
    const card2Box = await featureCards.nth(1).boundingBox();

    // In a 2-column layout, first two cards should be side by side
    // Card 2 should be to the right of Card 1 (different X position)
    expect(card2Box.x).toBeGreaterThan(card1Box.x);

    // First two cards should be on the same row (similar Y position)
    expect(Math.abs(card2Box.y - card1Box.y)).toBeLessThan(20);

    // If there's a third card, it should be on a new row
    if (cardCount > 2) {
      const card3Box = await featureCards.nth(2).boundingBox();
      expect(card3Box.y).toBeGreaterThan(card1Box.y);
    }
  });

  test('feature cards display in 3-column grid on desktop (1920px)', async ({ page }) => {
    // Test case 8: Test responsive grid layout at 1920px width
    await page.setViewportSize({ width: 1920, height: 1080 });
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();

    // Get positions of all three cards
    const card1Box = await featureCards.nth(0).boundingBox();
    const card2Box = await featureCards.nth(1).boundingBox();
    const card3Box = await featureCards.nth(2).boundingBox();

    // In a 3-column layout, all three cards should be side by side
    // Each card should be to the right of the previous one
    expect(card2Box.x).toBeGreaterThan(card1Box.x);
    expect(card3Box.x).toBeGreaterThan(card2Box.x);

    // All three cards should be on the same row (similar Y position)
    expect(Math.abs(card2Box.y - card1Box.y)).toBeLessThan(20);
    expect(Math.abs(card3Box.y - card1Box.y)).toBeLessThan(20);
  });

  test('feature cards have icons', async ({ page }) => {
    // Additional test: verify each feature card has an icon
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();

    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i);
      const icon = card.locator('.feature-icon svg, .feature-icon img');
      await expect(icon).toBeVisible();
    }
  });

  test('feature cards have title and description', async ({ page }) => {
    // Additional test: verify each feature card has title and description
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();

    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i);
      const title = card.locator('h3');
      const description = card.locator('p');

      await expect(title).toBeVisible();
      await expect(description).toBeVisible();

      // Title should not be empty
      const titleText = await title.textContent();
      expect(titleText.trim().length).toBeGreaterThan(0);

      // Description should not be empty
      const descText = await description.textContent();
      expect(descText.trim().length).toBeGreaterThan(0);
    }
  });
});
