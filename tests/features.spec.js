// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Test Suite: Features Section
 * Scenario: Verify the features section highlights core capabilities including
 * LSM Tree architecture, Async I/O, configuration, and compaction
 */

test.describe('Features Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: LSM Tree Architecture feature is displayed with description
   * Input: Check features section for LSM Tree
   * Expected: LSM Tree Architecture feature is displayed with description
   */
  test('TC1: LSM Tree Architecture feature is displayed with description', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();

    // Find the LSM Tree feature card
    const lsmTreeFeature = page.locator('[data-feature="lsm-tree"]');
    await expect(lsmTreeFeature).toBeVisible();

    // Verify title
    const title = lsmTreeFeature.locator('h3');
    await expect(title).toHaveText('LSM Tree Architecture');

    // Verify description exists and contains meaningful content
    const description = lsmTreeFeature.locator('p');
    await expect(description).toBeVisible();
    await expect(description).toContainText('Log-Structured Merge-tree');
    await expect(description).toContainText('memtables');
    await expect(description).toContainText('SSTable');
  });

  /**
   * Test Case 2: Async I/O with Tokio feature is displayed with description
   * Input: Check features section for Async I/O
   * Expected: Async I/O with Tokio feature is displayed with description
   */
  test('TC2: Async I/O with Tokio feature is displayed with description', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();

    // Find the Async I/O feature card
    const asyncIOFeature = page.locator('[data-feature="async-io"]');
    await expect(asyncIOFeature).toBeVisible();

    // Verify title
    const title = asyncIOFeature.locator('h3');
    await expect(title).toHaveText('Async I/O with Tokio');

    // Verify description exists and contains meaningful content
    const description = asyncIOFeature.locator('p');
    await expect(description).toBeVisible();
    await expect(description).toContainText('Tokio');
    await expect(description).toContainText('asynchronous');
  });

  /**
   * Test Case 3: Configurable Parameters feature is displayed with description
   * Input: Check features section for configuration
   * Expected: Configurable Parameters feature is displayed with description
   */
  test('TC3: Configurable Parameters feature is displayed with description', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();

    // Find the Configurable Parameters feature card
    const configFeature = page.locator('[data-feature="configurable"]');
    await expect(configFeature).toBeVisible();

    // Verify title
    const title = configFeature.locator('h3');
    await expect(title).toHaveText('Configurable Parameters');

    // Verify description exists and contains meaningful content
    const description = configFeature.locator('p');
    await expect(description).toBeVisible();
    await expect(description).toContainText('TOML');
    await expect(description).toContainText('configuration');
  });

  /**
   * Test Case 4: Background Compaction feature is displayed with description
   * Input: Check features section for compaction
   * Expected: Background Compaction feature is displayed with description
   */
  test('TC4: Background Compaction feature is displayed with description', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();

    // Find the Background Compaction feature card
    const compactionFeature = page.locator('[data-feature="compaction"]');
    await expect(compactionFeature).toBeVisible();

    // Verify title
    const title = compactionFeature.locator('h3');
    await expect(title).toHaveText('Background Compaction');

    // Verify description exists and contains meaningful content
    const description = compactionFeature.locator('p');
    await expect(description).toBeVisible();
    await expect(description).toContainText('compaction');
    await expect(description).toContainText('memtables');
    await expect(description).toContainText('SSTables');
  });

  /**
   * Test: Features section is accessible via navigation
   * Verifies that the features section can be navigated to and displays all 4 feature cards
   */
  test('Features section is accessible via navigation', async ({ page }) => {
    // Click on Features link in navigation
    await page.click('a[href="#features"]');

    // Verify the features section is now visible
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();

    // Verify all 4 feature cards are present
    const featureCards = page.locator('.feature-card');
    await expect(featureCards).toHaveCount(4);
  });
});
