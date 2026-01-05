// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Feature Highlights with Descriptions Tests (REQ-3)
 *
 * This test suite verifies that feature highlights are displayed with clear descriptions
 * as specified in the product requirements document.
 */

test.describe('Feature Highlights with Descriptions (REQ-3)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Memcached Compatibility Feature Card
   * Verifies that the memcached compatibility feature has both a title and description
   * explaining drop-in compatibility with existing clients.
   */
  test('TC1: Memcached compatibility feature has title and description explaining drop-in compatibility', async ({ page }) => {
    // Locate the features section
    const featuresSection = page.locator('[data-testid="features-section"]');
    await expect(featuresSection).toBeVisible();

    // Find the memcached feature card
    const memcachedCard = page.locator('[data-testid~="feature-card-memcached"]');
    await expect(memcachedCard).toBeVisible();

    // Verify the feature has a title
    const title = memcachedCard.locator('[data-testid="feature-title-memcached"]');
    await expect(title).toBeVisible();
    await expect(title).toHaveText(/Memcached Protocol/i);

    // Verify the feature has a description
    const description = memcachedCard.locator('[data-testid="feature-description-memcached"]');
    await expect(description).toBeVisible();

    // Verify the description explains drop-in compatibility with existing clients
    const descriptionText = await description.textContent();
    expect(descriptionText).toMatch(/drop-in compatibility/i);
    expect(descriptionText).toMatch(/existing.*clients/i);
  });

  /**
   * Test Case 2: Persistent Storage Feature Card
   * Verifies that the persistent storage feature has both a title and description
   * explaining that data survives restarts.
   */
  test('TC2: Persistent storage feature has title and description explaining data survives restarts', async ({ page }) => {
    // Locate the features section
    const featuresSection = page.locator('[data-testid="features-section"]');
    await expect(featuresSection).toBeVisible();

    // Find the persistent storage feature card
    const persistentCard = page.locator('[data-testid~="feature-card-persistent"]');
    await expect(persistentCard).toBeVisible();

    // Verify the feature has a title
    const title = persistentCard.locator('[data-testid="feature-title-persistent"]');
    await expect(title).toBeVisible();
    await expect(title).toHaveText(/Persistent Storage/i);

    // Verify the feature has a description
    const description = persistentCard.locator('[data-testid="feature-description-persistent"]');
    await expect(description).toBeVisible();

    // Verify the description explains data survives restarts
    const descriptionText = await description.textContent();
    expect(descriptionText).toMatch(/survives.*restarts/i);
  });

  /**
   * Test Case 3: LSM Architecture Feature Card
   * Verifies that the LSM tree architecture feature has both a title and description
   * explaining efficient write-heavy workloads.
   */
  test('TC3: LSM architecture feature has title and description explaining efficient write-heavy workloads', async ({ page }) => {
    // Locate the features section
    const featuresSection = page.locator('[data-testid="features-section"]');
    await expect(featuresSection).toBeVisible();

    // Find the LSM architecture feature card
    const lsmCard = page.locator('[data-testid~="feature-card-lsm"]');
    await expect(lsmCard).toBeVisible();

    // Verify the feature has a title
    const title = lsmCard.locator('[data-testid="feature-title-lsm"]');
    await expect(title).toBeVisible();
    await expect(title).toHaveText(/LSM Tree Architecture/i);

    // Verify the feature has a description
    const description = lsmCard.locator('[data-testid="feature-description-lsm"]');
    await expect(description).toBeVisible();

    // Verify the description explains efficient write-heavy workloads
    const descriptionText = await description.textContent();
    expect(descriptionText).toMatch(/write-heavy workloads/i);
    expect(descriptionText).toMatch(/efficient/i);
  });

  /**
   * Test Case 4: Feature Descriptions Are Readable
   * Verifies that all feature descriptions are at least 10 words each
   * and explain benefits clearly.
   */
  test('TC4: Feature descriptions are at least 10 words each and explain benefits clearly', async ({ page }) => {
    // Locate the features section
    const featuresSection = page.locator('[data-testid="features-section"]');
    await expect(featuresSection).toBeVisible();

    // Find all feature descriptions
    const featureDescriptions = [
      page.locator('[data-testid="feature-description-memcached"]'),
      page.locator('[data-testid="feature-description-persistent"]'),
      page.locator('[data-testid="feature-description-lsm"]')
    ];

    for (const descriptionElement of featureDescriptions) {
      await expect(descriptionElement).toBeVisible();

      const text = await descriptionElement.textContent();

      // Count words (split by whitespace and filter out empty strings)
      const wordCount = text.trim().split(/\s+/).filter(word => word.length > 0).length;

      // Verify description has at least 10 words
      expect(wordCount).toBeGreaterThanOrEqual(10);

      // Verify description is not just placeholder text
      expect(text.length).toBeGreaterThan(50);
    }
  });

  /**
   * Additional test: Features grid is properly structured
   * Verifies that the features section has the correct structure with all three feature cards
   */
  test('Features section contains exactly three feature cards', async ({ page }) => {
    // Locate the features grid
    const featuresGrid = page.locator('[data-testid="features-grid"]');
    await expect(featuresGrid).toBeVisible();

    // Verify there are exactly 3 feature cards
    const featureCards = featuresGrid.locator('.feature-card');
    await expect(featureCards).toHaveCount(3);
  });

  /**
   * Additional test: Features section has proper heading
   */
  test('Features section has "Key Features" heading', async ({ page }) => {
    const featuresHeading = page.locator('#features-heading');
    await expect(featuresHeading).toBeVisible();
    await expect(featuresHeading).toHaveText('Key Features');
  });
});
