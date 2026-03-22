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

test.describe('Usage Example Display (Scenario 4)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case 1: Locate usage example section and check for GIF or code block
  // Expected: Section contains either an img element (GIF) or code/pre element with example commands
  test('usage section contains GIF or code block', async ({ page }) => {
    const usageSection = page.locator('#usage');
    await expect(usageSection).toBeVisible();
    await scrollToSection(page, '#usage');

    // Check for either GIF or code block
    const usageGif = usageSection.locator('img.usage-gif, img[id="usage-gif"]');
    const usageCode = usageSection.locator('pre.usage-code, code#usage-code-snippet');

    const hasGif = await usageGif.count() > 0;
    const hasCode = await usageCode.count() > 0;

    // At least one should be present
    expect(hasGif || hasCode).toBeTruthy();
  });

  // Test Case 2: If GIF present, verify src attribute
  // Expected: GIF source points to usage.gif asset and loads successfully
  test('usage GIF src points to usage.gif asset', async ({ page }) => {
    await scrollToSection(page, '#usage');
    const usageGif = page.locator('#usage-gif');

    // GIF should be present
    await expect(usageGif).toBeVisible();

    // Check src attribute contains usage.gif
    const src = await usageGif.getAttribute('src');
    expect(src).toBeTruthy();
    expect(src).toContain('usage.gif');
  });

  // Test Case 3: If code snippet present, extract content
  // Expected: Code contains Memcached-style commands (get, set, etc.)
  test('code snippet contains Memcached-style commands', async ({ page }) => {
    await scrollToSection(page, '#usage');
    const codeSnippet = page.locator('#usage-code-snippet');

    // Code snippet should be present
    await expect(codeSnippet).toBeVisible();

    // Extract text content
    const codeText = await codeSnippet.textContent();
    expect(codeText).toBeTruthy();

    const lowerCode = codeText!.toLowerCase();

    // Should contain Memcached-style commands
    const hasSet = lowerCode.includes('set');
    const hasGet = lowerCode.includes('get');
    const hasDelete = lowerCode.includes('delete');

    expect(hasSet).toBeTruthy();
    expect(hasGet).toBeTruthy();
    expect(hasDelete).toBeTruthy();
  });

  // Additional test: Usage section has proper heading
  test('usage section has proper heading', async ({ page }) => {
    await scrollToSection(page, '#usage');
    const usageHeading = page.locator('#usage-heading');

    await expect(usageHeading).toBeVisible();

    const headingText = await usageHeading.textContent();
    expect(headingText).toBeTruthy();
    expect(headingText!.toLowerCase()).toContain('usage');
  });

  // Additional test: Usage GIF has alt text for accessibility
  test('usage GIF has descriptive alt text', async ({ page }) => {
    await scrollToSection(page, '#usage');
    const usageGif = page.locator('#usage-gif');

    await expect(usageGif).toBeVisible();

    const altText = await usageGif.getAttribute('alt');
    expect(altText).toBeTruthy();
    expect(altText!.length).toBeGreaterThan(10); // Should be descriptive
  });

  // Additional test: Usage GIF loads successfully
  test('usage GIF loads successfully', async ({ page }) => {
    await scrollToSection(page, '#usage');
    const usageGif = page.locator('#usage-gif');

    await expect(usageGif).toBeVisible();

    // Check if image loaded (naturalWidth > 0)
    const naturalWidth = await usageGif.evaluate((img: HTMLImageElement) => img.naturalWidth);
    expect(naturalWidth).toBeGreaterThan(0);
  });
});
