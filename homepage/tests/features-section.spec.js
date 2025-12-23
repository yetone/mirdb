// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Features Section Display E2E Tests
 *
 * Scenario: Verify that key features are presented clearly as specified in REQ-2
 * including persistence, Memcached compatibility, LSM tree architecture, and async Rust
 */

test.describe('Features Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Memcached Protocol Compatibility feature card
   * Input: Inspect features section for Memcached Protocol Compatibility card
   * Expected: Feature card for Memcached Protocol Compatibility is displayed with icon, title, and description
   */
  test('should display Memcached Protocol Compatibility feature card with icon, title, and description', async ({ page }) => {
    // Navigate to features section
    await page.locator('#features').scrollIntoViewIfNeeded();

    // Find the Memcached feature card
    const memcachedCard = page.locator('[data-testid="feature-memcached"]');

    // Verify the card is visible
    await expect(memcachedCard).toBeVisible();

    // Verify the icon is present
    const icon = memcachedCard.locator('.feature-icon');
    await expect(icon).toBeVisible();
    const iconSvg = icon.locator('svg');
    await expect(iconSvg).toBeVisible();

    // Verify the title contains the expected text
    const title = memcachedCard.locator('.feature-title');
    await expect(title).toBeVisible();
    await expect(title).toContainText('Memcached Protocol Compatibility');

    // Verify the description is present and not empty
    const description = memcachedCard.locator('.feature-description');
    await expect(description).toBeVisible();
    const descriptionText = await description.textContent();
    expect(descriptionText.length).toBeGreaterThan(0);
    expect(descriptionText).toContain('memcached');
  });

  /**
   * Test Case 2: Persistent Storage feature card
   * Input: Inspect features section for Persistent Storage card
   * Expected: Feature card for Persistent Storage with SSTables is displayed with icon, title, and description
   */
  test('should display Persistent Storage with SSTables feature card with icon, title, and description', async ({ page }) => {
    // Navigate to features section
    await page.locator('#features').scrollIntoViewIfNeeded();

    // Find the Persistent Storage feature card
    const persistentCard = page.locator('[data-testid="feature-persistent"]');

    // Verify the card is visible
    await expect(persistentCard).toBeVisible();

    // Verify the icon is present
    const icon = persistentCard.locator('.feature-icon');
    await expect(icon).toBeVisible();
    const iconSvg = icon.locator('svg');
    await expect(iconSvg).toBeVisible();

    // Verify the title contains the expected text
    const title = persistentCard.locator('.feature-title');
    await expect(title).toBeVisible();
    await expect(title).toContainText('Persistent Storage');
    await expect(title).toContainText('SSTables');

    // Verify the description mentions SSTables and persistence
    const description = persistentCard.locator('.feature-description');
    await expect(description).toBeVisible();
    const descriptionText = await description.textContent();
    expect(descriptionText.length).toBeGreaterThan(0);
    expect(descriptionText.toLowerCase()).toContain('persist');
  });

  /**
   * Test Case 3: LSM Tree Architecture feature card
   * Input: Inspect features section for LSM Tree Architecture card
   * Expected: Feature card for LSM Tree Architecture is displayed with icon, title, and description
   */
  test('should display LSM Tree Architecture feature card with icon, title, and description', async ({ page }) => {
    // Navigate to features section
    await page.locator('#features').scrollIntoViewIfNeeded();

    // Find the LSM Tree feature card
    const lsmCard = page.locator('[data-testid="feature-lsm"]');

    // Verify the card is visible
    await expect(lsmCard).toBeVisible();

    // Verify the icon is present
    const icon = lsmCard.locator('.feature-icon');
    await expect(icon).toBeVisible();
    const iconSvg = icon.locator('svg');
    await expect(iconSvg).toBeVisible();

    // Verify the title contains the expected text
    const title = lsmCard.locator('.feature-title');
    await expect(title).toBeVisible();
    await expect(title).toContainText('LSM Tree Architecture');

    // Verify the description mentions LSM tree concepts
    const description = lsmCard.locator('.feature-description');
    await expect(description).toBeVisible();
    const descriptionText = await description.textContent();
    expect(descriptionText.length).toBeGreaterThan(0);
    expect(descriptionText.toLowerCase()).toContain('memtable');
  });

  /**
   * Test Case 4: Async Rust feature card
   * Input: Inspect features section for Async Rust card
   * Expected: Feature card for Async Rust with Tokio is displayed with icon, title, and description
   */
  test('should display Async Rust with Tokio feature card with icon, title, and description', async ({ page }) => {
    // Navigate to features section
    await page.locator('#features').scrollIntoViewIfNeeded();

    // Find the Async Rust feature card
    const asyncRustCard = page.locator('[data-testid="feature-async-rust"]');

    // Verify the card is visible
    await expect(asyncRustCard).toBeVisible();

    // Verify the icon is present
    const icon = asyncRustCard.locator('.feature-icon');
    await expect(icon).toBeVisible();
    const iconSvg = icon.locator('svg');
    await expect(iconSvg).toBeVisible();

    // Verify the title contains the expected text
    const title = asyncRustCard.locator('.feature-title');
    await expect(title).toBeVisible();
    await expect(title).toContainText('Async Rust');
    await expect(title).toContainText('Tokio');

    // Verify the description mentions Tokio and async
    const description = asyncRustCard.locator('.feature-description');
    await expect(description).toBeVisible();
    const descriptionText = await description.textContent();
    expect(descriptionText.length).toBeGreaterThan(0);
    expect(descriptionText.toLowerCase()).toContain('tokio');
  });

  /**
   * Test Case 5: Features visible in first viewport
   * Input: Check 3-4 features visible in first viewport after hero
   * Expected: At least 3 key feature highlights are visible within first scroll on desktop
   */
  test('should display at least 3 feature cards visible after first scroll on desktop', async ({ page }) => {
    // Set desktop viewport
    await page.setViewportSize({ width: 1280, height: 800 });

    // Navigate to features section (simulating first scroll after hero)
    await page.locator('#features').scrollIntoViewIfNeeded();

    // Wait for the features section to be visible
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Get all feature cards
    const featureCards = page.locator('.feature-card');

    // Verify at least 4 feature cards exist
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(4);

    // Verify at least 3 feature cards are visible in the viewport
    let visibleCount = 0;
    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i);
      const isVisible = await card.isVisible();
      if (isVisible) {
        // Check if the card is in the viewport
        const boundingBox = await card.boundingBox();
        if (boundingBox) {
          const viewportHeight = 800;
          const scrollY = await page.evaluate(() => window.scrollY);
          const cardTop = boundingBox.y + scrollY;
          const cardBottom = cardTop + boundingBox.height;
          const viewportTop = scrollY;
          const viewportBottom = scrollY + viewportHeight;

          // Card is considered visible if at least part of it is in the viewport
          if (cardTop < viewportBottom && cardBottom > viewportTop) {
            visibleCount++;
          }
        }
      }
    }

    // At least 3 features should be visible
    expect(visibleCount).toBeGreaterThanOrEqual(3);
  });

  /**
   * Additional test: Verify features section navigation works
   */
  test('should navigate to features section when clicking Features link', async ({ page }) => {
    // Click on Features navigation link
    await page.click('a[href="#features"]');

    // Verify the features section is visible
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Verify the section title
    const sectionTitle = featuresSection.locator('.section-title');
    await expect(sectionTitle).toContainText('Key Features');
  });

  /**
   * Additional test: Verify all four feature cards have distinct styling
   */
  test('should display all four feature cards with visual distinction', async ({ page }) => {
    // Navigate to features section
    await page.locator('#features').scrollIntoViewIfNeeded();

    // Get all feature cards
    const featureCards = page.locator('.feature-card');

    // Verify exactly 4 feature cards exist
    await expect(featureCards).toHaveCount(4);

    // Verify each card has the required structure
    for (let i = 0; i < 4; i++) {
      const card = featureCards.nth(i);
      await expect(card).toBeVisible();

      // Verify each card has an icon container
      const icon = card.locator('.feature-icon');
      await expect(icon).toBeVisible();

      // Verify each card has a title
      const title = card.locator('.feature-title');
      await expect(title).toBeVisible();

      // Verify each card has a description
      const description = card.locator('.feature-description');
      await expect(description).toBeVisible();
    }
  });
});
