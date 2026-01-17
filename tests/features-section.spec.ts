import { test, expect } from '@playwright/test';

/**
 * E2E Tests for Key Differentiators and Features Section
 *
 * This test suite verifies that the features section displays all key differentiators
 * including Rust performance, persistence via SSTables, and memcached protocol compatibility.
 *
 * Requirements: REQ-2 and REQ-4
 */

test.describe('Key Differentiators and Features Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 1: Check for Memcached Protocol Compatible feature', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Find the Memcached Protocol Compatible feature card
    const featureCard = page.locator('[data-feature="memcached-protocol"]');
    await expect(featureCard).toBeVisible();

    // Verify the feature heading
    const heading = featureCard.locator('h3');
    await expect(heading).toContainText('Memcached Protocol Compatible');

    // Verify the description mentions protocol compatibility
    const description = featureCard.locator('p');
    await expect(description).toContainText('memcached');
    await expect(description).toContainText('protocol');

    // Verify supported commands are mentioned
    await expect(description).toContainText('SET');
    await expect(description).toContainText('GET');
    await expect(description).toContainText('DELETE');
  });

  test('Test Case 2: Check for Persistent Storage feature', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Find the Persistent Storage feature card
    const featureCard = page.locator('[data-feature="persistent-storage"]');
    await expect(featureCard).toBeVisible();

    // Verify the feature heading
    const heading = featureCard.locator('h3');
    await expect(heading).toContainText('Persistent Storage');
    await expect(heading).toContainText('SSTables');

    // Verify the description mentions persistence and SSTables
    const description = featureCard.locator('p');
    await expect(description).toContainText('persists');
    await expect(description).toContainText('SSTables');
    await expect(description).toContainText('Sorted String Tables');
  });

  test('Test Case 3: Check for LSM-Tree Architecture feature', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Find the LSM-Tree Architecture feature card
    const featureCard = page.locator('[data-feature="lsm-tree"]');
    await expect(featureCard).toBeVisible();

    // Verify the feature heading
    const heading = featureCard.locator('h3');
    await expect(heading).toContainText('LSM-Tree');
    await expect(heading).toContainText('Architecture');

    // Verify the description mentions LSM-tree storage architecture
    const description = featureCard.locator('p');
    await expect(description).toContainText('Log-Structured Merge-tree');
    await expect(description).toContainText('memtable');
    await expect(description).toContainText('SSTables');
  });

  test('Test Case 4: Check for High Performance feature', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Find the High Performance feature card
    const featureCard = page.locator('[data-feature="high-performance"]');
    await expect(featureCard).toBeVisible();

    // Verify the feature heading
    const heading = featureCard.locator('h3');
    await expect(heading).toContainText('High Performance');
    await expect(heading).toContainText('Rust');

    // Verify the description highlights Rust performance benefits
    const description = featureCard.locator('p');
    await expect(description).toContainText('Rust');
    await expect(description).toContainText('performance');
    await expect(description).toContainText('Tokio');
  });

  test('Test Case 5: Check for WAL feature', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Find the WAL feature card
    const featureCard = page.locator('[data-feature="wal"]');
    await expect(featureCard).toBeVisible();

    // Verify the feature heading mentions Write-Ahead Log
    const heading = featureCard.locator('h3');
    await expect(heading).toContainText('Write-Ahead Log');

    // Verify the description mentions durability and crash recovery
    const description = featureCard.locator('p');
    await expect(description).toContainText('durability');
    await expect(description).toContainText('crash recovery');
  });

  test('Test Case 6: Check for Compaction feature', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Find the Compaction feature card
    const featureCard = page.locator('[data-feature="compaction"]');
    await expect(featureCard).toBeVisible();

    // Verify the feature heading mentions configurable compaction
    const heading = featureCard.locator('h3');
    await expect(heading).toContainText('Configurable Compaction');

    // Verify the description mentions compaction details
    const description = featureCard.locator('p');
    await expect(description).toContainText('compaction');
    await expect(description).toContainText('minor compaction');
    await expect(description).toContainText('major compaction');
  });

  test('Test Case 7: Check for Skip List Memtable feature', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Find the Skip List Memtable feature card
    const featureCard = page.locator('[data-feature="skip-list"]');
    await expect(featureCard).toBeVisible();

    // Verify the feature heading mentions skip list memtable
    const heading = featureCard.locator('h3');
    await expect(heading).toContainText('Skip List');
    await expect(heading).toContainText('Memtable');

    // Verify the description mentions skip list implementation
    const description = featureCard.locator('p');
    await expect(description).toContainText('skip list');
    await expect(description).toContainText('O(log n)');
  });

  test('Features section should be clearly visible and organized', async ({ page }) => {
    // Verify features section exists and has proper structure
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Verify section has a heading
    const sectionHeading = featuresSection.locator('h2');
    await expect(sectionHeading).toContainText('Key Features');

    // Verify section has a subtitle
    const subtitle = featuresSection.locator('.section-subtitle');
    await expect(subtitle).toBeVisible();

    // Verify features grid contains all 7 feature cards
    const featureCards = featuresSection.locator('.feature-card');
    await expect(featureCards).toHaveCount(7);

    // Verify all feature cards have icons
    const featureIcons = featuresSection.locator('.feature-icon');
    await expect(featureIcons).toHaveCount(7);
  });

  test('Each feature card should have proper visual structure', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Check each feature card has icon, heading, and description
    const featureCards = featuresSection.locator('.feature-card');
    const count = await featureCards.count();

    for (let i = 0; i < count; i++) {
      const card = featureCards.nth(i);

      // Each card should have an icon
      const icon = card.locator('.feature-icon');
      await expect(icon).toBeVisible();

      // Each card should have a heading
      const heading = card.locator('h3');
      await expect(heading).toBeVisible();

      // Each card should have a description
      const description = card.locator('p');
      await expect(description).toBeVisible();
    }
  });
});
