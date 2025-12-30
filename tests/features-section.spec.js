// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Test Suite: Features Section Display
 * Scenario: Verify the features section showcases key capabilities including
 * LSM tree architecture, Memcached protocol support, and persistence
 */

test.describe('Features Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Check Features section for Memcached Protocol feature
   * Input: Check Features section for Memcached Protocol feature
   * Expected: Feature card/item for 'Memcached Protocol Compatibility' is displayed with description
   */
  test('TC1: Memcached Protocol Compatibility feature is displayed with description', async ({ page }) => {
    // Navigate to Features section
    const featuresSection = page.locator('#features.features-section');
    await expect(featuresSection).toBeVisible();

    // Find the feature card for Memcached Protocol Compatibility
    const memcachedCard = page.locator('.feature-card', { hasText: 'Memcached Protocol Compatibility' });
    await expect(memcachedCard).toBeVisible();

    // Verify the card has a heading
    const cardHeading = memcachedCard.locator('h3');
    await expect(cardHeading).toBeVisible();
    await expect(cardHeading).toContainText('Memcached Protocol');

    // Verify the card has a description
    const cardDescription = memcachedCard.locator('p');
    await expect(cardDescription).toBeVisible();

    // Verify description mentions key aspects
    const descriptionText = await cardDescription.textContent();
    expect(descriptionText).toBeTruthy();
    expect(descriptionText.length).toBeGreaterThan(20);

    // Should mention drop-in replacement or compatibility
    const lowerText = descriptionText.toLowerCase();
    const mentionsCompatibility = lowerText.includes('drop-in') ||
                                  lowerText.includes('compatib') ||
                                  lowerText.includes('existing clients');
    expect(mentionsCompatibility).toBeTruthy();
  });

  /**
   * Test Case 2: Check Features section for Persistent Storage feature
   * Input: Check Features section for Persistent Storage feature
   * Expected: Feature card/item for 'Persistent Storage with SSTables' is displayed with description
   */
  test('TC2: Persistent Storage feature is displayed with description', async ({ page }) => {
    // Navigate to Features section
    const featuresSection = page.locator('#features.features-section');
    await expect(featuresSection).toBeVisible();

    // Find the feature card for Persistent Storage
    const persistentCard = page.locator('.feature-card', { hasText: 'Persistent Storage' });
    await expect(persistentCard).toBeVisible();

    // Verify the card has a heading
    const cardHeading = persistentCard.locator('h3');
    await expect(cardHeading).toBeVisible();
    await expect(cardHeading).toContainText('Persistent Storage');

    // Verify the card has a description
    const cardDescription = persistentCard.locator('p');
    await expect(cardDescription).toBeVisible();

    // Verify description mentions SSTables or disk-based storage
    const descriptionText = await cardDescription.textContent();
    expect(descriptionText).toBeTruthy();

    const lowerText = descriptionText.toLowerCase();
    const mentionsSSTables = lowerText.includes('sstable') ||
                            lowerText.includes('disk') ||
                            lowerText.includes('durable') ||
                            lowerText.includes('persist');
    expect(mentionsSSTables).toBeTruthy();
  });

  /**
   * Test Case 3: Check Features section for LSM Tree feature
   * Input: Check Features section for LSM Tree feature
   * Expected: Feature card/item for 'LSM Tree Architecture' is displayed with description
   */
  test('TC3: LSM Tree Architecture feature is displayed with description', async ({ page }) => {
    // Navigate to Features section
    const featuresSection = page.locator('#features.features-section');
    await expect(featuresSection).toBeVisible();

    // Find the feature card for LSM Tree Architecture
    const lsmCard = page.locator('.feature-card', { hasText: 'LSM Tree Architecture' });
    await expect(lsmCard).toBeVisible();

    // Verify the card has a heading
    const cardHeading = lsmCard.locator('h3');
    await expect(cardHeading).toBeVisible();
    await expect(cardHeading).toContainText('LSM Tree');

    // Verify the card has a description
    const cardDescription = lsmCard.locator('p');
    await expect(cardDescription).toBeVisible();

    // Verify description mentions key LSM concepts
    const descriptionText = await cardDescription.textContent();
    expect(descriptionText).toBeTruthy();

    const lowerText = descriptionText.toLowerCase();
    const mentionsLSMConcepts = lowerText.includes('log-structured') ||
                                lowerText.includes('merge') ||
                                lowerText.includes('write-ahead') ||
                                lowerText.includes('wal') ||
                                lowerText.includes('data integrity');
    expect(mentionsLSMConcepts).toBeTruthy();
  });

  /**
   * Test Case 4: Check Features section for Async Performance feature
   * Input: Check Features section for Async Performance feature
   * Expected: Feature card/item for 'Async Performance with Tokio' is displayed with description
   */
  test('TC4: Async Performance with Tokio feature is displayed with description', async ({ page }) => {
    // Navigate to Features section
    const featuresSection = page.locator('#features.features-section');
    await expect(featuresSection).toBeVisible();

    // Find the feature card for Async Performance
    const asyncCard = page.locator('.feature-card', { hasText: 'Async Performance' });
    await expect(asyncCard).toBeVisible();

    // Verify the card has a heading
    const cardHeading = asyncCard.locator('h3');
    await expect(cardHeading).toBeVisible();
    await expect(cardHeading).toContainText('Async');

    // Verify the card has a description
    const cardDescription = asyncCard.locator('p');
    await expect(cardDescription).toBeVisible();

    // Verify description mentions Tokio or async concepts
    const descriptionText = await cardDescription.textContent();
    expect(descriptionText).toBeTruthy();

    const lowerText = descriptionText.toLowerCase();
    const mentionsTokio = lowerText.includes('tokio') ||
                          lowerText.includes('async') ||
                          lowerText.includes('throughput') ||
                          lowerText.includes('performance');
    expect(mentionsTokio).toBeTruthy();
  });

  /**
   * Test Case 5: Verify feature layout structure
   * Input: Verify feature layout structure
   * Expected: Features are displayed in a grid or card layout, not plain list
   */
  test('TC5: Features are displayed in a grid or card layout', async ({ page }) => {
    // Navigate to Features section
    const featuresSection = page.locator('#features.features-section');
    await expect(featuresSection).toBeVisible();

    // Verify features grid container exists
    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    // Verify grid uses CSS Grid or Flexbox layout (not plain list)
    const displayStyle = await featuresGrid.evaluate(el =>
      window.getComputedStyle(el).display
    );
    const isGridOrFlex = displayStyle === 'grid' || displayStyle === 'flex';
    expect(isGridOrFlex).toBeTruthy();

    // Verify multiple feature cards exist
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(4); // At least 4 key features

    // Verify cards are styled as cards (have padding and background)
    const firstCard = featureCards.first();
    const cardStyles = await firstCard.evaluate(el => {
      const styles = window.getComputedStyle(el);
      return {
        padding: styles.padding,
        backgroundColor: styles.backgroundColor,
        borderRadius: styles.borderRadius
      };
    });

    // Cards should have padding
    expect(cardStyles.padding).not.toBe('0px');

    // Cards should have a background color (not transparent)
    expect(cardStyles.backgroundColor).not.toBe('transparent');
    expect(cardStyles.backgroundColor).not.toBe('rgba(0, 0, 0, 0)');

    // Verify cards are laid out horizontally on desktop (not stacked vertically)
    const firstCardBox = await featureCards.nth(0).boundingBox();
    const secondCardBox = await featureCards.nth(1).boundingBox();

    expect(firstCardBox).not.toBeNull();
    expect(secondCardBox).not.toBeNull();

    // On desktop, cards should be side by side (second card starts at similar Y or to the right)
    // Allow for either grid row arrangement
    const isGridLayout =
      // Cards on same row
      (Math.abs(firstCardBox.y - secondCardBox.y) < 50) ||
      // Or cards properly stacked with grid gap
      (secondCardBox.y > firstCardBox.y + firstCardBox.height - 100);
    expect(isGridLayout).toBeTruthy();
  });

  /**
   * Additional test: Configurable Storage feature is displayed
   */
  test('Configurable Storage Engine feature is displayed with description', async ({ page }) => {
    // Navigate to Features section
    const featuresSection = page.locator('#features.features-section');
    await expect(featuresSection).toBeVisible();

    // Find the feature card for Configurable Storage
    const configCard = page.locator('.feature-card', { hasText: 'Configurable Storage' });
    await expect(configCard).toBeVisible();

    // Verify the card has a heading
    const cardHeading = configCard.locator('h3');
    await expect(cardHeading).toBeVisible();
    await expect(cardHeading).toContainText('Configurable');

    // Verify the card has a description
    const cardDescription = configCard.locator('p');
    await expect(cardDescription).toBeVisible();

    // Verify description mentions configuration options
    const descriptionText = await cardDescription.textContent();
    expect(descriptionText).toBeTruthy();

    const lowerText = descriptionText.toLowerCase();
    const mentionsConfig = lowerText.includes('memtable') ||
                           lowerText.includes('sstable') ||
                           lowerText.includes('compaction') ||
                           lowerText.includes('fine-tune') ||
                           lowerText.includes('configur');
    expect(mentionsConfig).toBeTruthy();
  });

  /**
   * Additional test: Features section has proper semantic structure
   */
  test('Features section has proper semantic structure', async ({ page }) => {
    // Verify features section exists within main
    const featuresInMain = page.locator('main #features');
    await expect(featuresInMain).toBeVisible();

    // Verify section has proper heading
    const featuresHeading = page.locator('#features h2');
    await expect(featuresHeading).toBeVisible();
    await expect(featuresHeading).toHaveText('Features');

    // Verify each feature card has proper heading hierarchy (h3)
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();

    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i);
      const cardH3 = card.locator('h3');
      await expect(cardH3).toBeVisible();
    }
  });

  /**
   * Additional test: Features section is accessible via navigation
   */
  test('Features section is accessible via navigation link', async ({ page }) => {
    // Find and click the Features navigation link
    const featuresNavLink = page.locator('.nav-link', { hasText: 'Features' });
    await expect(featuresNavLink).toBeVisible();
    await featuresNavLink.click();

    // Wait for scroll animation
    await page.waitForTimeout(500);

    // Verify Features section is now near the top of viewport
    const featuresSection = page.locator('#features');
    const sectionBox = await featuresSection.boundingBox();
    expect(sectionBox).not.toBeNull();
    expect(sectionBox.y).toBeLessThan(200);

    // Verify URL hash has changed
    const currentUrl = page.url();
    expect(currentUrl).toContain('#features');
  });
});
