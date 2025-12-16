// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for MirDB Homepage Features Section Display
 *
 * Test Case 1: Features section exists with proper heading
 * Test Case 2: Memcached protocol feature card is displayed
 * Test Case 3: LSM-tree storage feature card is displayed
 * Test Case 4: WAL (Write-Ahead Logging) feature card is displayed
 * Test Case 5: Compaction feature card is displayed
 * Test Case 6: At least 4 feature cards are displayed
 */

test.describe('Features Section Display', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to homepage before each test
    await page.goto('/');
  });

  test('Test Case 1: Features section is rendered with proper heading', async ({ page }) => {
    // Verify the features section exists
    const featuresSection = page.getByTestId('features-section');
    await expect(featuresSection).toBeVisible();

    // Verify the features heading is displayed
    const featuresHeading = page.getByTestId('features-heading');
    await expect(featuresHeading).toBeVisible();

    // Verify the heading contains "Features" text
    const headingText = await featuresHeading.textContent();
    expect(headingText?.toLowerCase()).toContain('features');
  });

  test('Test Case 2: Feature card for Memcached protocol compatibility is displayed', async ({ page }) => {
    // Scroll to features section
    const featuresSection = page.getByTestId('features-section');
    await featuresSection.scrollIntoViewIfNeeded();

    // Find the Memcached protocol feature card
    const memcachedFeature = page.getByTestId('feature-memcached');
    await expect(memcachedFeature).toBeVisible();

    // Verify it has a title
    const featureTitle = memcachedFeature.locator('[data-testid="feature-title"]');
    await expect(featureTitle).toBeVisible();

    // Verify the title mentions Memcached
    const titleText = await featureTitle.textContent();
    expect(titleText?.toLowerCase()).toContain('memcached');

    // Verify it has a description
    const featureDescription = memcachedFeature.locator('[data-testid="feature-description"]');
    await expect(featureDescription).toBeVisible();

    // Verify it has an icon
    const featureIcon = memcachedFeature.locator('[data-testid="feature-icon"]');
    await expect(featureIcon).toBeVisible();
  });

  test('Test Case 3: Feature card for LSM-tree style persistent storage is displayed', async ({ page }) => {
    // Scroll to features section
    const featuresSection = page.getByTestId('features-section');
    await featuresSection.scrollIntoViewIfNeeded();

    // Find the LSM-tree storage feature card
    const lsmTreeFeature = page.getByTestId('feature-lsm-tree');
    await expect(lsmTreeFeature).toBeVisible();

    // Verify it has a title
    const featureTitle = lsmTreeFeature.locator('[data-testid="feature-title"]');
    await expect(featureTitle).toBeVisible();

    // Verify the title mentions LSM-tree or storage
    const titleText = await featureTitle.textContent();
    expect(titleText?.toLowerCase()).toMatch(/lsm[-\s]?tree|storage/);

    // Verify it has a description
    const featureDescription = lsmTreeFeature.locator('[data-testid="feature-description"]');
    await expect(featureDescription).toBeVisible();

    // Verify it has an icon
    const featureIcon = lsmTreeFeature.locator('[data-testid="feature-icon"]');
    await expect(featureIcon).toBeVisible();
  });

  test('Test Case 4: Feature card for Write-Ahead Logging (durability) is displayed', async ({ page }) => {
    // Scroll to features section
    const featuresSection = page.getByTestId('features-section');
    await featuresSection.scrollIntoViewIfNeeded();

    // Find the WAL feature card
    const walFeature = page.getByTestId('feature-wal');
    await expect(walFeature).toBeVisible();

    // Verify it has a title
    const featureTitle = walFeature.locator('[data-testid="feature-title"]');
    await expect(featureTitle).toBeVisible();

    // Verify the title mentions WAL or Write-Ahead Logging
    const titleText = await featureTitle.textContent();
    expect(titleText?.toLowerCase()).toMatch(/wal|write[-\s]?ahead|logging/);

    // Verify it has a description
    const featureDescription = walFeature.locator('[data-testid="feature-description"]');
    await expect(featureDescription).toBeVisible();

    // Verify it has an icon
    const featureIcon = walFeature.locator('[data-testid="feature-icon"]');
    await expect(featureIcon).toBeVisible();
  });

  test('Test Case 5: Feature card for minor and major compaction support is displayed', async ({ page }) => {
    // Scroll to features section
    const featuresSection = page.getByTestId('features-section');
    await featuresSection.scrollIntoViewIfNeeded();

    // Find the compaction feature card
    const compactionFeature = page.getByTestId('feature-compaction');
    await expect(compactionFeature).toBeVisible();

    // Verify it has a title
    const featureTitle = compactionFeature.locator('[data-testid="feature-title"]');
    await expect(featureTitle).toBeVisible();

    // Verify the title mentions compaction
    const titleText = await featureTitle.textContent();
    expect(titleText?.toLowerCase()).toContain('compaction');

    // Verify it has a description
    const featureDescription = compactionFeature.locator('[data-testid="feature-description"]');
    await expect(featureDescription).toBeVisible();

    // Verify it has an icon
    const featureIcon = compactionFeature.locator('[data-testid="feature-icon"]');
    await expect(featureIcon).toBeVisible();
  });

  test('Test Case 6: At least 4 feature cards are displayed', async ({ page }) => {
    // Scroll to features section
    const featuresSection = page.getByTestId('features-section');
    await featuresSection.scrollIntoViewIfNeeded();

    // Count all feature cards
    const featureCards = page.locator('[data-testid^="feature-"][data-testid$="-card"], [data-testid^="feature-"]:not([data-testid="features-section"]):not([data-testid="features-heading"]):not([data-testid="feature-title"]):not([data-testid="feature-description"]):not([data-testid="feature-icon"])');

    // Alternative: count by class or role
    const featureCardsByClass = page.locator('.feature-card');

    // Get count and verify at least 4 cards
    const count = await featureCardsByClass.count();
    expect(count).toBeGreaterThanOrEqual(4);
  });
});
