/**
 * Features Section E2E Tests
 * Owner: Scenario 2 - Features Section Display
 *
 * Tests for:
 * - Features section visibility
 * - Feature cards display and count
 * - Feature card content (icon, title, description)
 * - Grid layout responsiveness
 */
import { test, expect } from '@playwright/test';
import { SELECTORS, VIEWPORTS } from '../fixtures/test-data';

// Feature data for validation
const EXPECTED_FEATURES = [
  { id: 'persistence', title: 'Disk Persistence' },
  { id: 'lsm-tree', title: 'LSM Tree Architecture' },
  { id: 'memcached-protocol', title: 'Memcached Protocol' },
  { id: 'compression', title: 'Snappy Compression' },
  { id: 'bloom-filters', title: 'Bloom Filters' },
  { id: 'async-io', title: 'Tokio Async Runtime' },
];

test.describe('Features Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Features section is visible with heading', async ({ page }) => {
    // Scroll to features section
    const featuresSection = page.locator(SELECTORS.features);
    await featuresSection.scrollIntoViewIfNeeded();

    // Verify section is visible
    await expect(featuresSection).toBeVisible();

    // Verify section has Features heading
    const heading = featuresSection.locator('.features-title');
    await expect(heading).toBeVisible();
    await expect(heading).toContainText('Features');
  });

  test('TC2: At least 4 feature cards are displayed', async ({ page }) => {
    // Scroll to features section
    const featuresSection = page.locator(SELECTORS.features);
    await featuresSection.scrollIntoViewIfNeeded();

    // Count feature cards
    const featureCards = featuresSection.locator('.feature-card');
    const cardCount = await featureCards.count();

    // Verify at least 4 cards exist (Persistence, LSM Tree, Memcached Protocol, Compression)
    expect(cardCount).toBeGreaterThanOrEqual(4);

    // Verify all expected features are present
    for (const feature of EXPECTED_FEATURES) {
      const card = featuresSection.locator(`[data-feature="${feature.id}"]`);
      await expect(card).toBeVisible();
    }
  });

  test('TC3: Each feature card contains icon, title, and description', async ({ page }) => {
    // Scroll to features section
    const featuresSection = page.locator(SELECTORS.features);
    await featuresSection.scrollIntoViewIfNeeded();

    // Get all feature cards
    const featureCards = featuresSection.locator('.feature-card');
    const cardCount = await featureCards.count();

    // Verify each card has required elements
    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i);

      // Check for icon/visual
      const icon = card.locator('.feature-icon');
      await expect(icon).toBeVisible();

      // Check for title
      const title = card.locator('.feature-title');
      await expect(title).toBeVisible();
      const titleText = await title.textContent();
      expect(titleText).toBeTruthy();
      expect(titleText!.length).toBeGreaterThan(0);

      // Check for description
      const description = card.locator('.feature-description');
      await expect(description).toBeVisible();
      const descText = await description.textContent();
      expect(descText).toBeTruthy();
      expect(descText!.length).toBeGreaterThan(10); // Description should be meaningful
    }
  });

  test('TC4: Features grid adapts to different viewport sizes', async ({ page }) => {
    const featuresSection = page.locator(SELECTORS.features);

    // Test desktop viewport
    await page.setViewportSize(VIEWPORTS.desktop);
    await featuresSection.scrollIntoViewIfNeeded();
    await expect(featuresSection).toBeVisible();

    const grid = featuresSection.locator('.features-grid');
    await expect(grid).toBeVisible();

    // Verify grid layout exists
    const gridStyle = await grid.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        display: style.display,
        gridTemplateColumns: style.gridTemplateColumns,
      };
    });
    expect(gridStyle.display).toBe('grid');

    // Test tablet viewport
    await page.setViewportSize(VIEWPORTS.tablet);
    await featuresSection.scrollIntoViewIfNeeded();
    await expect(featuresSection).toBeVisible();

    // Test mobile viewport
    await page.setViewportSize(VIEWPORTS.mobile);
    await featuresSection.scrollIntoViewIfNeeded();
    await expect(featuresSection).toBeVisible();

    // All feature cards should still be visible on mobile
    const featureCards = featuresSection.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(4);
  });

  test('TC5: Feature cards have proper semantic structure', async ({ page }) => {
    const featuresSection = page.locator(SELECTORS.features);
    await featuresSection.scrollIntoViewIfNeeded();

    // Verify section heading structure
    const sectionHeading = featuresSection.locator('h2.features-title');
    await expect(sectionHeading).toBeVisible();

    // Verify feature cards use article elements for semantic meaning
    const articleCards = featuresSection.locator('article.feature-card');
    const articleCount = await articleCards.count();
    expect(articleCount).toBeGreaterThanOrEqual(4);

    // Verify feature titles use h3 elements
    const featureTitles = featuresSection.locator('h3.feature-title');
    const titleCount = await featureTitles.count();
    expect(titleCount).toBe(articleCount);
  });

  test('TC6: Core MirDB features are displayed correctly', async ({ page }) => {
    const featuresSection = page.locator(SELECTORS.features);
    await featuresSection.scrollIntoViewIfNeeded();

    // Verify specific core features are present with correct content
    const coreFeatures = [
      { id: 'persistence', text: 'Disk Persistence' },
      { id: 'lsm-tree', text: 'LSM Tree' },
      { id: 'memcached-protocol', text: 'Memcached Protocol' },
      { id: 'compression', text: 'Compression' },
    ];

    for (const feature of coreFeatures) {
      const card = featuresSection.locator(`[data-feature="${feature.id}"]`);
      await expect(card).toBeVisible();

      const title = card.locator('.feature-title');
      await expect(title).toContainText(feature.text);
    }
  });
});
