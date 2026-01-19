// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = 'file://' + path.resolve(__dirname, '../index.html');

test.describe('Async I/O Feature Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(indexPath);
  });

  // Test Case 1: Search features section for Async I/O mention
  test('TC1: Feature card or text mentions Async I/O or Tokio', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Look for feature card mentioning Async I/O or Tokio
    const asyncFeature = page.locator('.feature-card').filter({
      hasText: /async\s*i\/o|tokio/i
    });

    await expect(asyncFeature).toBeVisible();

    // Verify the card contains either "Async I/O" or "Tokio"
    const cardText = await asyncFeature.textContent();
    const hasAsyncIO = /async\s*i\/o/i.test(cardText);
    const hasTokio = /tokio/i.test(cardText);

    expect(hasAsyncIO || hasTokio).toBeTruthy();
  });

  // Test Case 2: Verify Async feature has description
  test('TC2: Async I/O feature has explanatory description text', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Find the Async I/O / Tokio feature card
    const asyncFeature = page.locator('.feature-card').filter({
      hasText: /async\s*i\/o|tokio/i
    });

    await expect(asyncFeature).toBeVisible();

    // Verify it has a title (h3)
    const title = asyncFeature.locator('h3');
    await expect(title).toBeVisible();
    const titleText = await title.textContent();

    // Title should mention Async I/O or Tokio
    const titleHasAsyncIO = /async\s*i\/o/i.test(titleText);
    const titleHasTokio = /tokio/i.test(titleText);
    expect(titleHasAsyncIO || titleHasTokio).toBeTruthy();

    // Verify it has a description paragraph
    const description = asyncFeature.locator('p');
    await expect(description).toBeVisible();
    const descText = await description.textContent();

    // Description should be meaningful (at least 20 characters)
    expect(descText.trim().length).toBeGreaterThan(20);

    // Description should provide context about async/Tokio/performance
    const descLower = descText.toLowerCase();
    const hasRelevantContent =
      descLower.includes('rust') ||
      descLower.includes('tokio') ||
      descLower.includes('async') ||
      descLower.includes('performance') ||
      descLower.includes('high-performance') ||
      descLower.includes('runtime');

    expect(hasRelevantContent).toBeTruthy();
  });

  // Additional: Verify the feature card has an icon
  test('Async I/O feature card has visual icon', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Find the Async I/O / Tokio feature card
    const asyncFeature = page.locator('.feature-card').filter({
      hasText: /async\s*i\/o|tokio/i
    });

    await expect(asyncFeature).toBeVisible();

    // Check for icon element
    const iconDiv = asyncFeature.locator('.feature-icon');
    await expect(iconDiv).toBeVisible();

    // Verify the icon has content
    const iconContent = await iconDiv.textContent();
    expect(iconContent.trim().length).toBeGreaterThan(0);
  });
});
