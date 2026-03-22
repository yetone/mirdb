/**
 * Features Section E2E Tests
 * Owners: Scenario 3 (Features), Scenario 4 (Usage Example)
 *
 * Test groups:
 * - Features grid layout
 * - Feature count and content
 * - Feature descriptions
 * - Usage GIF or code snippet presence
 * - Usage content relevance
 */

import { test, expect } from '@playwright/test';
import { scrollToSection } from './utils';

test.describe('Features Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case 1: Query features section for feature item elements
  // Expected: Features section contains between 3 and 6 feature items
  test('features section contains between 3 and 6 feature items', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    const featureItems = featuresSection.locator('.feature-item');
    const count = await featureItems.count();

    expect(count).toBeGreaterThanOrEqual(3);
    expect(count).toBeLessThanOrEqual(6);
  });

  // Test Case 2: Extract text content from each feature item
  // Expected: Each feature has a title and description text
  test('each feature has a title and description text', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await scrollToSection(page, '#features');

    const featureItems = featuresSection.locator('.feature-item');
    const count = await featureItems.count();

    for (let i = 0; i < count; i++) {
      const featureItem = featureItems.nth(i);

      // Check for title
      const title = featureItem.locator('.feature-title');
      await expect(title).toBeVisible();
      const titleText = await title.textContent();
      expect(titleText).toBeTruthy();
      expect(titleText!.length).toBeGreaterThan(0);

      // Check for description
      const description = featureItem.locator('.feature-description');
      await expect(description).toBeVisible();
      const descText = await description.textContent();
      expect(descText).toBeTruthy();
      expect(descText!.length).toBeGreaterThan(10); // Description should be meaningful
    }
  });

  // Test Case 3: Check for Memcached protocol mention
  // Expected: At least one feature mentions Memcached protocol compatibility
  test('at least one feature mentions Memcached protocol compatibility', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await scrollToSection(page, '#features');

    const sectionText = await featuresSection.textContent();
    const hasMemcached = sectionText?.toLowerCase().includes('memcached');
    const hasProtocol = sectionText?.toLowerCase().includes('protocol');

    expect(hasMemcached).toBeTruthy();
    expect(hasProtocol).toBeTruthy();
  });

  // Test Case 4: Check for persistence/durability mention
  // Expected: At least one feature mentions persistence or data durability
  test('at least one feature mentions persistence or data durability', async ({ page }) => {
    const featuresSection = page.locator('#features');
    await scrollToSection(page, '#features');

    const sectionText = await featuresSection.textContent();
    const lowerText = sectionText?.toLowerCase() || '';

    const hasPersistence = lowerText.includes('persist');
    const hasDurability = lowerText.includes('durabil');
    const hasStorage = lowerText.includes('storage') && lowerText.includes('disk');

    expect(hasPersistence || hasDurability || hasStorage).toBeTruthy();
  });

  // Additional test: Features section has proper heading
  test('features section has proper heading', async ({ page }) => {
    const featuresHeading = page.locator('#features-heading');
    await expect(featuresHeading).toBeVisible();

    const headingText = await featuresHeading.textContent();
    expect(headingText).toBeTruthy();
  });

  // Additional test: Features grid is responsive
  test('features grid layout is visible', async ({ page }) => {
    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();
  });
});
