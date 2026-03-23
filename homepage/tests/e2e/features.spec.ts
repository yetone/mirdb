/**
 * Features Section E2E Tests
 * Owner: Scenario 2 - Features Section Display
 *
 * Test cases:
 * - Persistent storage feature listed
 * - Memcached protocol feature listed
 * - Skiplist memtable feature listed
 * - Compaction feature listed
 * - Each feature has description
 * - Features displayed in organized layout
 */

import { test, expect } from '@playwright/test';
import { scrollToSection, selectors } from './test-utils';

test.describe('Features Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Persistent storage feature is listed with a brief, clear description', async ({ page }) => {
    // Navigate to features section
    await scrollToSection(page, 'features');

    // Check for persistent storage feature
    const persistentStorageCard = page.locator('[data-feature="persistent-storage"]');
    await expect(persistentStorageCard).toBeVisible();

    // Check title
    const title = persistentStorageCard.locator('.feature-title');
    await expect(title).toContainText('Persistent Storage');

    // Check description exists and has meaningful content
    const description = persistentStorageCard.locator('.feature-description');
    await expect(description).toBeVisible();
    const descText = await description.textContent();
    expect(descText).toBeTruthy();
    expect(descText!.trim().length).toBeGreaterThan(20); // Ensure it's a meaningful description

    // Verify description mentions key aspects of persistent storage
    await expect(description).toContainText(/disk|durable|durability|persist/i);
  });

  test('TC2: Memcached protocol compatibility feature is listed with description', async ({ page }) => {
    // Navigate to features section
    await scrollToSection(page, 'features');

    // Check for Memcached protocol feature
    const memcachedCard = page.locator('[data-feature="memcached-protocol"]');
    await expect(memcachedCard).toBeVisible();

    // Check title
    const title = memcachedCard.locator('.feature-title');
    await expect(title).toContainText('Memcached Protocol');

    // Check description exists and has meaningful content
    const description = memcachedCard.locator('.feature-description');
    await expect(description).toBeVisible();
    const descText = await description.textContent();
    expect(descText).toBeTruthy();
    expect(descText!.trim().length).toBeGreaterThan(20);

    // Verify description mentions compatibility/protocol aspects
    await expect(description).toContainText(/compatib|protocol|client/i);
  });

  test('TC3: Skiplist-based memtable feature is listed with description', async ({ page }) => {
    // Navigate to features section
    await scrollToSection(page, 'features');

    // Check for skiplist memtable feature
    const skiplistCard = page.locator('[data-feature="skiplist-memtable"]');
    await expect(skiplistCard).toBeVisible();

    // Check title
    const title = skiplistCard.locator('.feature-title');
    await expect(title).toContainText('Skiplist');

    // Check description exists and has meaningful content
    const description = skiplistCard.locator('.feature-description');
    await expect(description).toBeVisible();
    const descText = await description.textContent();
    expect(descText).toBeTruthy();
    expect(descText!.trim().length).toBeGreaterThan(20);

    // Verify description mentions memory/performance aspects
    await expect(description).toContainText(/memory|fast|performance|write|concurrent/i);
  });

  test('TC4: Compaction capabilities feature is listed with description', async ({ page }) => {
    // Navigate to features section
    await scrollToSection(page, 'features');

    // Check for compaction feature
    const compactionCard = page.locator('[data-feature="compaction"]');
    await expect(compactionCard).toBeVisible();

    // Check title
    const title = compactionCard.locator('.feature-title');
    await expect(title).toContainText('Compaction');

    // Check description exists and has meaningful content
    const description = compactionCard.locator('.feature-description');
    await expect(description).toBeVisible();
    const descText = await description.textContent();
    expect(descText).toBeTruthy();
    expect(descText!.trim().length).toBeGreaterThan(20);

    // Verify description mentions storage/optimization aspects
    await expect(description).toContainText(/background|optimiz|disk|space/i);
  });

  test('TC5: Features are displayed in a visually organized layout (cards, grid, or list format)', async ({ page }) => {
    // Navigate to features section
    await scrollToSection(page, 'features');

    // Check features section exists
    const featuresSection = page.locator(selectors.features.section);
    await expect(featuresSection).toBeVisible();

    // Check section has a title
    const sectionTitle = featuresSection.locator('.section-heading, h2');
    await expect(sectionTitle).toContainText('Features');

    // Check features grid/container exists
    const featuresGrid = page.locator(selectors.features.grid);
    await expect(featuresGrid).toBeVisible();

    // Check that we have exactly 4 feature cards
    const featureCards = page.locator(selectors.features.card);
    await expect(featureCards).toHaveCount(4);

    // Verify each card has the expected structure
    for (let i = 0; i < 4; i++) {
      const card = featureCards.nth(i);

      // Each card should have an icon, title, and description
      const icon = card.locator('.feature-icon');
      const title = card.locator('.feature-title');
      const description = card.locator('.feature-description');

      await expect(icon).toBeVisible();
      await expect(title).toBeVisible();
      await expect(description).toBeVisible();
    }

    // Verify the grid displays as a proper grid/flex layout
    const gridDisplay = await featuresGrid.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        display: styles.display,
        gridTemplateColumns: styles.gridTemplateColumns,
      };
    });

    // Should be using grid layout
    expect(gridDisplay.display).toBe('grid');
    // Grid should have template columns defined
    expect(gridDisplay.gridTemplateColumns).not.toBe('none');
  });

  test('All required features are present per REQ-3', async ({ page }) => {
    // This is a summary test that ensures all 4 features from REQ-3 are present
    await scrollToSection(page, 'features');

    const cards = page.locator(selectors.features.card);
    const count = await cards.count();
    expect(count).toBe(4);

    // Check data-feature attributes are present for all required features
    const featureNames: string[] = [];
    for (let i = 0; i < count; i++) {
      const dataFeature = await cards.nth(i).getAttribute('data-feature');
      if (dataFeature) {
        featureNames.push(dataFeature);
      }
    }

    expect(featureNames).toContain('persistent-storage');
    expect(featureNames).toContain('memcached-protocol');
    expect(featureNames).toContain('skiplist-memtable');
    expect(featureNames).toContain('compaction');

    // Each feature should have both title and description
    for (let i = 0; i < count; i++) {
      const card = cards.nth(i);
      const title = await card.locator('.feature-title').textContent();
      const description = await card.locator('.feature-description').textContent();
      expect(title!.trim().length).toBeGreaterThan(0);
      expect(description!.trim().length).toBeGreaterThan(20);
    }
  });

  test('Features section is accessible via navigation', async ({ page }) => {
    // Click on Features navigation link
    const navLink = page.locator('a[href="#features"]').first();
    await expect(navLink).toBeVisible();
    await navLink.click();

    // Wait for scroll animation
    await page.waitForTimeout(500);

    // Features section should be visible in viewport
    const featuresSection = page.locator(selectors.features.section);
    await expect(featuresSection).toBeInViewport();
  });

  test('Feature cards have semantic structure', async ({ page }) => {
    await scrollToSection(page, 'features');

    // Check that feature cards use article elements for semantic structure
    const featureCards = page.locator(selectors.features.card);

    for (let i = 0; i < 4; i++) {
      const card = featureCards.nth(i);
      const tagName = await card.evaluate(el => el.tagName.toLowerCase());
      expect(tagName).toBe('article');
    }

    // Check that the features grid has role="list"
    const featuresGrid = page.locator(selectors.features.grid);
    const role = await featuresGrid.getAttribute('role');
    expect(role).toBe('list');

    // Check that feature cards have role="listitem"
    const firstCard = featureCards.first();
    const cardRole = await firstCard.getAttribute('role');
    expect(cardRole).toBe('listitem');
  });
});
