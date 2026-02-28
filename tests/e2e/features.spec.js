/**
 * Features Section E2E Tests
 * Owner: Scenario 2 - Features Section Display
 *
 * Test cases:
 * - Memcached Protocol feature card
 * - Persistent Storage feature card
 * - LSM Tree Architecture feature card
 * - Grid layout at 1024px viewport
 * - Single column at 375px viewport
 */

const { test, expect } = require('@playwright/test');
const { VIEWPORTS, waitForPageLoad } = require('./test-utils');

test.describe('Features Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await waitForPageLoad(page);
  });

  test('TC1: Memcached Protocol feature card is displayed with icon and description', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();

    // Find the Memcached Protocol feature card
    const memcachedCard = page.locator('.feature-card').filter({
      has: page.locator('.feature-title', { hasText: 'Memcached Protocol' })
    });

    // Verify card exists
    await expect(memcachedCard).toBeVisible();

    // Verify icon is present
    const icon = memcachedCard.locator('.feature-icon');
    await expect(icon).toBeVisible();

    // Verify description mentions protocol compatibility
    const description = memcachedCard.locator('.feature-description');
    await expect(description).toBeVisible();
    await expect(description).toContainText(/memcached|protocol|compatible|drop-in/i);
  });

  test('TC2: Persistent Storage feature card is displayed with icon and description', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();

    // Find the Persistent Storage feature card
    const storageCard = page.locator('.feature-card').filter({
      has: page.locator('.feature-title', { hasText: 'Persistent Storage' })
    });

    // Verify card exists
    await expect(storageCard).toBeVisible();

    // Verify icon is present
    const icon = storageCard.locator('.feature-icon');
    await expect(icon).toBeVisible();

    // Verify description mentions data persistence
    const description = storageCard.locator('.feature-description');
    await expect(description).toBeVisible();
    await expect(description).toContainText(/persist|data|durability|survives|restart/i);
  });

  test('TC3: LSM Tree Architecture feature card is displayed with icon and description', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();

    // Find the LSM Tree Architecture feature card
    const lsmCard = page.locator('.feature-card').filter({
      has: page.locator('.feature-title', { hasText: 'LSM Tree Architecture' })
    });

    // Verify card exists
    await expect(lsmCard).toBeVisible();

    // Verify icon is present
    const icon = lsmCard.locator('.feature-icon');
    await expect(icon).toBeVisible();

    // Verify description mentions LSM tree implementation
    const description = lsmCard.locator('.feature-description');
    await expect(description).toBeVisible();
    await expect(description).toContainText(/LSM|log-structured|merge|efficient|write/i);
  });

  test('TC4: Features display in three-column grid layout at 1024px viewport', async ({ page }) => {
    // Set desktop viewport (1024px width)
    await page.setViewportSize({ width: 1024, height: 768 });

    // Navigate to features section
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();

    // Get the features grid
    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    // Check grid layout using computed styles
    const gridStyle = await featuresGrid.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        display: styles.display,
        gridTemplateColumns: styles.gridTemplateColumns
      };
    });

    // Verify it's a grid
    expect(gridStyle.display).toBe('grid');

    // Verify three columns (gridTemplateColumns should show 3 column values)
    const columns = gridStyle.gridTemplateColumns.split(' ').filter(c => c && c !== '0px');
    expect(columns.length).toBe(3);

    // Verify all three cards are visible
    const cards = page.locator('.feature-card');
    await expect(cards).toHaveCount(3);

    // Check that cards are arranged horizontally
    const cardPositions = await cards.evaluateAll((cards) => {
      return cards.map(card => {
        const rect = card.getBoundingClientRect();
        return { top: rect.top, left: rect.left };
      });
    });

    // All cards should have similar top positions (same row)
    const topPositions = cardPositions.map(p => p.top);
    const maxTopDiff = Math.max(...topPositions) - Math.min(...topPositions);
    expect(maxTopDiff).toBeLessThan(10); // Allow small variance

    // Cards should have different left positions (different columns)
    const leftPositions = cardPositions.map(p => p.left);
    const uniqueLeftPositions = [...new Set(leftPositions.map(l => Math.round(l / 10)))];
    expect(uniqueLeftPositions.length).toBe(3);
  });

  test('TC5: Features stack in single-column layout at 375px viewport', async ({ page }) => {
    // Set mobile viewport (375px width)
    await page.setViewportSize(VIEWPORTS.mobile);

    // Navigate to features section
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();

    // Get the features grid
    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    // Check grid layout
    const gridStyle = await featuresGrid.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        display: styles.display,
        gridTemplateColumns: styles.gridTemplateColumns
      };
    });

    // Verify it's still a grid
    expect(gridStyle.display).toBe('grid');

    // Verify single column (gridTemplateColumns should show 1 column value)
    const columns = gridStyle.gridTemplateColumns.split(' ').filter(c => c && c !== '0px');
    expect(columns.length).toBe(1);

    // Verify all three cards are visible
    const cards = page.locator('.feature-card');
    await expect(cards).toHaveCount(3);

    // Check that cards are stacked vertically
    const cardPositions = await cards.evaluateAll((cards) => {
      return cards.map(card => {
        const rect = card.getBoundingClientRect();
        return { top: rect.top, left: rect.left, width: rect.width };
      });
    });

    // All cards should have similar left positions (single column)
    const leftPositions = cardPositions.map(p => Math.round(p.left));
    const uniqueLeftPositions = [...new Set(leftPositions)];
    expect(uniqueLeftPositions.length).toBe(1);

    // Cards should have different top positions (stacked)
    const topPositions = cardPositions.map(p => p.top);
    expect(topPositions[1]).toBeGreaterThan(topPositions[0]);
    expect(topPositions[2]).toBeGreaterThan(topPositions[1]);
  });

  test('Features section is accessible via navigation', async ({ page }) => {
    // Features section should have proper ID for navigation
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Scroll to features via hash navigation
    await page.goto('/#features');
    await waitForPageLoad(page);

    // Verify features section is visible
    await expect(featuresSection).toBeInViewport();
  });

  test('Feature cards have proper accessibility attributes', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();

    // Check that feature cards are article elements
    const cards = page.locator('.feature-card');
    await expect(cards).toHaveCount(3);

    // Check each card has proper structure
    for (let i = 0; i < 3; i++) {
      const card = cards.nth(i);

      // Verify icon has aria-hidden
      const icon = card.locator('.feature-icon');
      await expect(icon).toHaveAttribute('aria-hidden', 'true');

      // Verify title is an h3
      const title = card.locator('.feature-title');
      await expect(title).toBeVisible();

      // Verify description paragraph
      const description = card.locator('.feature-description');
      await expect(description).toBeVisible();
    }
  });
});
