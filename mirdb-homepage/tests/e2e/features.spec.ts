/**
 * Features Section E2E Tests.
 * Owner: Scenario 2 - Features Showcase Section
 *
 * Tests:
 * - Feature cards presence
 * - Tokio feature content
 * - Memtable feature content
 * - Compaction features content
 * - Minimum 4 feature cards
 */

import { test, expect } from '@playwright/test';

test.describe('Features Showcase Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test("TC1: Feature card exists mentioning tokio or async runtime", async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Query for tokio/async content within feature cards
    const featureCards = featuresSection.locator('[data-testid^="feature-card"]');
    const tokioCard = featureCards.filter({ hasText: /tokio|async/i });

    // Verify at least one card mentions tokio or async
    await expect(tokioCard.first()).toBeVisible();
    await expect(tokioCard.first()).toContainText(/tokio|async/i);
  });

  test("TC2: Feature card exists mentioning memtable or skiplist", async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Query for memtable/skiplist content within feature cards
    const featureCards = featuresSection.locator('[data-testid^="feature-card"]');
    const memtableCard = featureCards.filter({ hasText: /memtable|skiplist/i });

    // Verify at least one card mentions memtable or skiplist
    await expect(memtableCard.first()).toBeVisible();
    await expect(memtableCard.first()).toContainText(/memtable|skiplist/i);
  });

  test("TC3: Feature cards exist for both minor and major compaction", async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Query for compaction content within feature cards
    const featureCards = featuresSection.locator('[data-testid^="feature-card"]');

    // Check for minor compaction card
    const minorCompactionCard = featureCards.filter({ hasText: /minor compaction/i });
    await expect(minorCompactionCard.first()).toBeVisible();

    // Check for major compaction card
    const majorCompactionCard = featureCards.filter({ hasText: /major compaction/i });
    await expect(majorCompactionCard.first()).toBeVisible();
  });

  test("TC4: At least 4 feature cards are displayed", async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Count total feature cards
    const featureCards = featuresSection.locator('[data-testid^="feature-card"]');
    const count = await featureCards.count();

    // Verify at least 4 feature cards
    expect(count).toBeGreaterThanOrEqual(4);
  });

  test('Features section has proper heading', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Verify heading exists
    const heading = featuresSection.locator('h2');
    await expect(heading).toBeVisible();
    await expect(heading).toContainText('Key Features');
  });

  test('Feature cards are displayed in a grid layout', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Verify grid container exists
    const grid = featuresSection.locator('[data-testid="features-grid"]');
    await expect(grid).toBeVisible();
  });

  test('Each feature card has a title and description', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Get all feature cards
    const featureCards = featuresSection.locator('[data-testid^="feature-card"]');
    const count = await featureCards.count();

    // Verify each card has a title (h3) and description (p)
    for (let i = 0; i < count; i++) {
      const card = featureCards.nth(i);
      const title = card.locator('h3');
      const description = card.locator('p');

      await expect(title).toBeVisible();
      await expect(description).toBeVisible();
    }
  });

  test('Features section uses semantic HTML', async ({ page }) => {
    // Verify features section uses a section element
    const section = page.locator('section#features');
    await expect(section).toBeVisible();

    // Verify feature cards use article elements
    const articles = section.locator('article');
    const count = await articles.count();
    expect(count).toBeGreaterThanOrEqual(4);
  });
});
