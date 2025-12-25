// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Test Suite: Features Section Display
 *
 * This test suite validates that the features section of the MirDB homepage
 * displays the correct feature cards with proper content as defined in the PRD.
 *
 * Features tested:
 * - Memcached Protocol Compatibility
 * - Persistent Storage with SSTables
 * - LSM Tree Architecture
 * - Async Networking with Tokio
 */

test.describe('Features Section Display', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to the homepage
    await page.goto('file://' + process.cwd() + '/index.html');

    // Wait for the features section to be visible
    await page.waitForSelector('#features');
  });

  test('Test Case 1: Memcached Protocol Compatibility card is present with description', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Find the Memcached compatibility feature card
    const memcachedCard = page.locator('[data-testid="feature-card-memcached"]');
    await expect(memcachedCard).toBeVisible();

    // Verify the card has the correct heading
    const cardHeading = memcachedCard.locator('h3');
    await expect(cardHeading).toHaveText('Memcached Protocol Compatibility');

    // Verify the card has a description mentioning key aspects
    const cardDescription = memcachedCard.locator('p');
    await expect(cardDescription).toBeVisible();

    // Verify the description mentions Memcached protocol and compatibility
    const descriptionText = await cardDescription.textContent();
    expect(descriptionText).toContain('Memcached');
    expect(descriptionText).toContain('GET');
    expect(descriptionText).toContain('SET');
  });

  test('Test Case 2: Persistent Storage with SSTables card is present', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Find the persistence feature card
    const persistenceCard = page.locator('[data-testid="feature-card-persistence"]');
    await expect(persistenceCard).toBeVisible();

    // Verify the card has the correct heading
    const cardHeading = persistenceCard.locator('h3');
    await expect(cardHeading).toHaveText('Persistent Storage with SSTables');

    // Verify the card has a description
    const cardDescription = persistenceCard.locator('p');
    await expect(cardDescription).toBeVisible();

    // Verify the description mentions SSTables and persistence
    const descriptionText = await cardDescription.textContent();
    expect(descriptionText).toContain('SSTables');
    expect(descriptionText).toContain('persist');
  });

  test('Test Case 3: LSM Tree Architecture card is present', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Find the LSM architecture feature card
    const lsmCard = page.locator('[data-testid="feature-card-lsm"]');
    await expect(lsmCard).toBeVisible();

    // Verify the card has the correct heading
    const cardHeading = lsmCard.locator('h3');
    await expect(cardHeading).toHaveText('LSM Tree Architecture');

    // Verify the card has a description
    const cardDescription = lsmCard.locator('p');
    await expect(cardDescription).toBeVisible();

    // Verify the description mentions LSM tree and key components
    const descriptionText = await cardDescription.textContent();
    expect(descriptionText).toContain('LSM');
    expect(descriptionText).toContain('Memtable');
    expect(descriptionText).toContain('compaction');
  });

  test('Test Case 4: Async Networking with Tokio card is present', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Find the async networking feature card
    const asyncCard = page.locator('[data-testid="feature-card-async"]');
    await expect(asyncCard).toBeVisible();

    // Verify the card has the correct heading
    const cardHeading = asyncCard.locator('h3');
    await expect(cardHeading).toHaveText('Async Networking with Tokio');

    // Verify the card has a description
    const cardDescription = asyncCard.locator('p');
    await expect(cardDescription).toBeVisible();

    // Verify the description mentions Tokio and async features
    const descriptionText = await cardDescription.textContent();
    expect(descriptionText).toContain('Tokio');
    expect(descriptionText).toContain('async');
  });

  test('Test Case 5: Features section contains 3-4 feature cards', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Get the features grid container
    const featuresGrid = page.locator('[data-testid="features-grid"]');
    await expect(featuresGrid).toBeVisible();

    // Count all feature cards in the grid
    const featureCards = featuresGrid.locator('.feature-card');
    const cardCount = await featureCards.count();

    // Verify there are between 3 and 4 feature cards (inclusive)
    expect(cardCount).toBeGreaterThanOrEqual(3);
    expect(cardCount).toBeLessThanOrEqual(4);

    // Log the actual count for clarity
    console.log(`Found ${cardCount} feature cards in the features section`);
  });

  test('Features section has proper accessibility structure', async ({ page }) => {
    // Verify the section has proper aria-labelledby
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toHaveAttribute('aria-labelledby', 'features-title');

    // Verify the heading exists and is properly labeled
    const sectionHeading = page.locator('#features-title');
    await expect(sectionHeading).toBeVisible();
    await expect(sectionHeading).toHaveText('Key Features');

    // Verify each feature card is an article element
    const featureCards = page.locator('.feature-card');
    const count = await featureCards.count();

    for (let i = 0; i < count; i++) {
      const card = featureCards.nth(i);
      const tagName = await card.evaluate(el => el.tagName.toLowerCase());
      expect(tagName).toBe('article');
    }
  });

  test('Feature cards have proper structure with icon, heading, and description', async ({ page }) => {
    const featureCards = page.locator('.feature-card');
    const count = await featureCards.count();

    for (let i = 0; i < count; i++) {
      const card = featureCards.nth(i);

      // Each card should have an icon
      const icon = card.locator('.feature-icon');
      await expect(icon).toBeVisible();

      // Each card should have a heading (h3)
      const heading = card.locator('h3');
      await expect(heading).toBeVisible();
      const headingText = await heading.textContent();
      expect(headingText?.length).toBeGreaterThan(0);

      // Each card should have a description (p)
      const description = card.locator('p');
      await expect(description).toBeVisible();
      const descText = await description.textContent();
      expect(descText?.length).toBeGreaterThan(0);
    }
  });
});
