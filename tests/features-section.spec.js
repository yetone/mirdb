// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Test Case 1: Check for features section container
 * Expected: Features section exists with at least 3 feature cards
 */
test('features section exists with at least 3 feature cards', async ({ page }) => {
  await page.goto('/');

  // Check that features section exists
  const featuresSection = page.locator('#features, section.features, [data-testid="features-section"]');
  await expect(featuresSection).toBeVisible();

  // Check for at least 3 feature cards
  const featureCards = page.locator('.feature-card, [data-testid="feature-card"]');
  await expect(featureCards).toHaveCount(3);
});

/**
 * Test Case 2: Verify Memcached feature card content
 * Expected: Card with title containing 'Memcached' and description mentioning 'protocol' or 'compatible'
 */
test('Memcached feature card has correct content', async ({ page }) => {
  await page.goto('/');

  // Find the Memcached feature card
  const memcachedCard = page.locator('.feature-card, [data-testid="feature-card"]').filter({
    has: page.locator('h2, h3, .feature-title').filter({ hasText: /Memcached/i })
  });

  await expect(memcachedCard).toBeVisible();

  // Check description mentions 'protocol' or 'compatible'
  const description = memcachedCard.locator('p, .feature-description');
  const descText = await description.textContent();
  expect(descText?.toLowerCase()).toMatch(/protocol|compatible/i);
});

/**
 * Test Case 3: Verify Persistence feature card content
 * Expected: Card with title containing 'Persistent' and description mentioning 'SSTable' or 'disk'
 */
test('Persistence feature card has correct content', async ({ page }) => {
  await page.goto('/');

  // Find the Persistence feature card
  const persistenceCard = page.locator('.feature-card, [data-testid="feature-card"]').filter({
    has: page.locator('h2, h3, .feature-title').filter({ hasText: /Persistent/i })
  });

  await expect(persistenceCard).toBeVisible();

  // Check description mentions 'SSTable' or 'disk'
  const description = persistenceCard.locator('p, .feature-description');
  const descText = await description.textContent();
  expect(descText?.toLowerCase()).toMatch(/sstable|disk/i);
});

/**
 * Test Case 4: Verify Performance feature card content
 * Expected: Card with title containing 'Performance' and description mentioning 'LSM' or 'async'
 */
test('Performance feature card has correct content', async ({ page }) => {
  await page.goto('/');

  // Find the Performance feature card
  const performanceCard = page.locator('.feature-card, [data-testid="feature-card"]').filter({
    has: page.locator('h2, h3, .feature-title').filter({ hasText: /Performance/i })
  });

  await expect(performanceCard).toBeVisible();

  // Check description mentions 'LSM' or 'async'
  const description = performanceCard.locator('p, .feature-description');
  const descText = await description.textContent();
  expect(descText?.toLowerCase()).toMatch(/lsm|async/i);
});

/**
 * Test Case 5: Check feature card structure
 * Expected: Each feature card contains an icon/image element, heading, and paragraph
 */
test('each feature card has icon, heading, and paragraph', async ({ page }) => {
  await page.goto('/');

  const featureCards = page.locator('.feature-card, [data-testid="feature-card"]');
  const count = await featureCards.count();

  expect(count).toBeGreaterThanOrEqual(3);

  // Check each card has required elements
  for (let i = 0; i < count; i++) {
    const card = featureCards.nth(i);

    // Check for icon (svg, img, or element with icon class)
    const icon = card.locator('svg, img, .feature-icon, [data-testid="feature-icon"]').first();
    await expect(icon).toBeVisible();

    // Check for heading (h2, h3, or element with title class)
    const heading = card.locator('h2, h3, .feature-title');
    await expect(heading).toBeVisible();

    // Check for paragraph/description
    const paragraph = card.locator('p, .feature-description');
    await expect(paragraph).toBeVisible();
  }
});
