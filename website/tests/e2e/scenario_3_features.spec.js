/**
 * E2E Tests for Scenario 3: Features Section
 *
 * Tests verify that the features section displays 3-4 feature cards
 * with icons and descriptions for MirDB's key features.
 */
const { test, expect } = require('@playwright/test');

test.describe('Features Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 1: Features section is present with appropriate heading', async ({ page }) => {
    // Locate the features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Check for the "Features" heading
    const heading = featuresSection.locator('h2');
    await expect(heading).toBeVisible();
    await expect(heading).toContainText('Features');
  });

  test('Test Case 2: Section contains 3-4 feature cards', async ({ page }) => {
    // Locate all feature cards within the features section
    const featureCards = page.locator('#features [data-testid="feature-card"]');
    const count = await featureCards.count();

    // Verify there are between 3 and 4 feature cards
    expect(count).toBeGreaterThanOrEqual(3);
    expect(count).toBeLessThanOrEqual(4);
  });

  test('Test Case 3: Memcached Protocol feature card is present', async ({ page }) => {
    const featuresSection = page.locator('#features');

    // Check for Memcached Protocol feature
    const memcachedCard = featuresSection.locator('.feature-card', { hasText: 'Memcached Protocol' });
    await expect(memcachedCard).toBeVisible();

    // Verify card has an icon
    const icon = memcachedCard.locator('.feature-icon svg, .feature-icon img');
    await expect(icon).toBeVisible();

    // Verify card has a description
    const description = memcachedCard.locator('.feature-description');
    await expect(description).toBeVisible();
    await expect(description).not.toBeEmpty();
  });

  test('Test Case 4: SSTables/Data Persistence feature card is present', async ({ page }) => {
    const featuresSection = page.locator('#features');

    // Check for SSTables or persistence-related feature
    const sstablesCard = featuresSection.locator('.feature-card', { hasText: /SSTable|Persistence/i });
    await expect(sstablesCard).toBeVisible();

    // Verify card has an icon
    const icon = sstablesCard.locator('.feature-icon svg, .feature-icon img');
    await expect(icon).toBeVisible();

    // Verify card has a description
    const description = sstablesCard.locator('.feature-description');
    await expect(description).toBeVisible();
    await expect(description).not.toBeEmpty();
  });

  test('Test Case 5: LSM Tree Architecture feature card is present', async ({ page }) => {
    const featuresSection = page.locator('#features');

    // Check for LSM Tree feature
    const lsmTreeCard = featuresSection.locator('.feature-card', { hasText: /LSM Tree/i });
    await expect(lsmTreeCard).toBeVisible();

    // Verify card has an icon
    const icon = lsmTreeCard.locator('.feature-icon svg, .feature-icon img');
    await expect(icon).toBeVisible();

    // Verify card has a description
    const description = lsmTreeCard.locator('.feature-description');
    await expect(description).toBeVisible();
    await expect(description).not.toBeEmpty();
  });

  test('Test Case 6: Async/Tokio/Performance feature card is present', async ({ page }) => {
    const featuresSection = page.locator('#features');

    // Check for Async Performance feature using title text (more specific to avoid matching LSM Tree's "performance" in description)
    const asyncCard = featuresSection.locator('.feature-card').filter({
      has: page.locator('.feature-title', { hasText: /Async/i })
    });
    await expect(asyncCard).toBeVisible();

    // Verify card has an icon
    const icon = asyncCard.locator('.feature-icon svg, .feature-icon img');
    await expect(icon).toBeVisible();

    // Verify card has a description mentioning Tokio
    const description = asyncCard.locator('.feature-description');
    await expect(description).toBeVisible();
    await expect(description).toContainText(/Tokio/i);
  });

  test('All feature cards have proper structure (icon, title, description)', async ({ page }) => {
    const featureCards = page.locator('#features [data-testid="feature-card"]');
    const count = await featureCards.count();

    for (let i = 0; i < count; i++) {
      const card = featureCards.nth(i);

      // Check icon is present
      const icon = card.locator('.feature-icon');
      await expect(icon).toBeVisible();

      // Check title is present
      const title = card.locator('.feature-title');
      await expect(title).toBeVisible();
      await expect(title).not.toBeEmpty();

      // Check description is present
      const description = card.locator('.feature-description');
      await expect(description).toBeVisible();
      await expect(description).not.toBeEmpty();
    }
  });

  test('Feature cards are in a responsive grid layout', async ({ page }) => {
    const grid = page.locator('#features [data-testid="feature-cards-grid"]');
    await expect(grid).toBeVisible();

    // Check grid has proper CSS grid class
    await expect(grid).toHaveClass(/grid/);
  });
});
