// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * User Story 2 - Explore Features
 *
 * As DevOps Diana, I want to see the key features and architecture of MirDB,
 * so that I can assess its reliability and performance characteristics.
 *
 * Scenario: Verify users can explore and understand MirDB features
 */
test.describe('User Story 2 - Explore Features', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Features section is easily reachable via scroll or navigation
   * Input: Scroll to features section
   * Expected: Features section is easily reachable via scroll or navigation
   */
  test('TC1: Features section is easily reachable via scroll or navigation', async ({ page }) => {
    // Test navigation link to features section
    const featuresNavLink = page.locator('nav a[href="#features"]');
    await expect(featuresNavLink).toBeVisible();
    await expect(featuresNavLink).toHaveText('Features');

    // Click navigation link
    await featuresNavLink.click();

    // Wait for smooth scroll and verify features section is visible
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();
    await expect(featuresSection).toBeInViewport();

    // Also verify features section has an id attribute for anchor navigation
    await expect(featuresSection).toHaveAttribute('id', 'features');

    // Verify smooth scrolling behavior is enabled (html has scroll-behavior: smooth)
    const htmlElement = page.locator('html');
    const scrollBehavior = await htmlElement.evaluate((el) =>
      window.getComputedStyle(el).scrollBehavior
    );
    expect(scrollBehavior).toBe('smooth');
  });

  /**
   * Test Case 2: At least 6 feature cards are displayed
   * Input: Count feature cards
   * Expected: At least 6 feature cards are displayed
   */
  test('TC2: At least 6 feature cards are displayed', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Count feature cards
    const featureCards = page.locator('#features .feature-card');
    const count = await featureCards.count();

    // Verify at least 6 feature cards
    expect(count).toBeGreaterThanOrEqual(6);

    // Verify each card is visible
    for (let i = 0; i < count; i++) {
      await expect(featureCards.nth(i)).toBeVisible();
    }
  });

  /**
   * Test Case 3: Each feature card has title and benefit description
   * Input: Check feature card content
   * Expected: Each feature card has title and benefit description
   */
  test('TC3: Each feature card has title and benefit description', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Get all feature cards
    const featureCards = page.locator('#features .feature-card');
    const count = await featureCards.count();

    // Verify each card has a title (h3) and description (p)
    for (let i = 0; i < count; i++) {
      const card = featureCards.nth(i);

      // Verify title exists and has content
      const title = card.locator('h3');
      await expect(title).toBeVisible();
      const titleText = await title.textContent();
      expect(titleText).toBeTruthy();
      expect(titleText.length).toBeGreaterThan(0);

      // Verify description exists and has meaningful content
      const description = card.locator('p');
      await expect(description).toBeVisible();
      const descriptionText = await description.textContent();
      expect(descriptionText).toBeTruthy();
      // Each description should be at least 30 characters for meaningful benefit description
      expect(descriptionText.length).toBeGreaterThan(30);
    }

    // Verify specific expected features are present
    const expectedFeatures = [
      'Memcached Protocol',
      'Persistent Storage',
      'LSM Tree Architecture',
      'Async Networking',
      'Write-Ahead Logging',
      'Configurable Compaction'
    ];

    for (const feature of expectedFeatures) {
      const featureTitle = page.locator(`#features .feature-card h3:has-text("${feature}")`);
      await expect(featureTitle).toBeVisible();
    }
  });

  /**
   * Test Case 4: LSM tree architecture is explained with compaction strategy
   * Input: Find LSM tree architecture info
   * Expected: LSM tree architecture is explained with compaction strategy
   */
  test('TC4: LSM tree architecture is explained with compaction strategy', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Find LSM Tree Architecture feature card
    const lsmCard = page.locator('[data-testid="feature-lsm-tree"]');
    await expect(lsmCard).toBeVisible();

    // Verify title
    const title = lsmCard.locator('h3');
    await expect(title).toHaveText('LSM Tree Architecture');

    // Verify description explains LSM tree architecture
    const description = lsmCard.locator('p');
    await expect(description).toBeVisible();
    const descriptionText = await description.textContent();

    // Should mention Log-Structured Merge-tree
    expect(descriptionText).toContain('Log-Structured Merge-tree');

    // Should mention compaction (the compaction strategy)
    expect(descriptionText.toLowerCase()).toContain('compaction');

    // Also check for configurable compaction feature card
    const compactionCard = page.locator('[data-testid="feature-compaction"]');
    await expect(compactionCard).toBeVisible();

    const compactionTitle = compactionCard.locator('h3');
    await expect(compactionTitle).toHaveText('Configurable Compaction');

    const compactionDescription = compactionCard.locator('p');
    await expect(compactionDescription).toBeVisible();
    const compactionText = await compactionDescription.textContent();

    // Should explain compaction strategies
    expect(compactionText.toLowerCase()).toContain('compaction');
    expect(compactionText.toLowerCase()).toContain('strategies');
  });

  /**
   * Additional test: Features section has proper heading
   */
  test('Features section has proper heading structure', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Verify section heading
    const heading = featuresSection.locator('h2');
    await expect(heading).toHaveText('Key Features');

    // Verify section description
    const sectionDescription = featuresSection.locator('.section-description');
    await expect(sectionDescription).toBeVisible();
    await expect(sectionDescription).toContainText('Rust');
  });

  /**
   * Additional test: Feature cards are visually organized in a grid
   */
  test('Feature cards are organized in a responsive grid layout', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Verify features grid container exists
    const featuresGrid = page.locator('#features .features-grid');
    await expect(featuresGrid).toBeVisible();

    // Verify grid display is used
    const displayStyle = await featuresGrid.evaluate((el) =>
      window.getComputedStyle(el).display
    );
    expect(displayStyle).toBe('grid');
  });
});
