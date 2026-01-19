// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for Value Proposition Section Display
 * Verifies that the three key value propositions (persistence, memcached compatibility, Rust performance)
 * are clearly presented as specified in REQ-2
 */

test.describe('Value Proposition Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 1: Check for Persistence pillar', async ({ page }) => {
    // Navigate to value proposition section
    const valuePropositionSection = page.locator('#value-proposition, .value-proposition, [data-section="value-proposition"]');
    await expect(valuePropositionSection).toBeVisible();

    // Find the Persistence pillar card/column
    const persistencePillar = page.locator('[data-pillar="persistence"], .pillar-persistence, .pillar:has-text("Persistence")').first();
    await expect(persistencePillar).toBeVisible();

    // Check for heading
    const persistenceHeading = persistencePillar.locator('h2, h3, .pillar-title');
    await expect(persistenceHeading).toContainText(/Persistence/i);

    // Check for LSM tree-based storage mention
    const persistenceContent = persistencePillar.locator('.pillar-description, p, .pillar-content');
    await expect(persistenceContent).toContainText(/LSM/i);

    // Check for WAL/durability mention
    await expect(persistenceContent).toContainText(/WAL|durability|write-ahead/i);
  });

  test('Test Case 2: Check for Compatibility pillar', async ({ page }) => {
    // Navigate to value proposition section
    const valuePropositionSection = page.locator('#value-proposition, .value-proposition, [data-section="value-proposition"]');
    await expect(valuePropositionSection).toBeVisible();

    // Find the Compatibility pillar card/column
    const compatibilityPillar = page.locator('[data-pillar="compatibility"], .pillar-compatibility, .pillar:has-text("Compatibility")').first();
    await expect(compatibilityPillar).toBeVisible();

    // Check for heading
    const compatibilityHeading = compatibilityPillar.locator('h2, h3, .pillar-title');
    await expect(compatibilityHeading).toContainText(/Compatibility/i);

    // Check for drop-in memcached protocol support mention
    const compatibilityContent = compatibilityPillar.locator('.pillar-description, p, .pillar-content');
    await expect(compatibilityContent).toContainText(/memcached/i);
    await expect(compatibilityContent).toContainText(/drop-in|protocol/i);
  });

  test('Test Case 3: Check for Performance pillar', async ({ page }) => {
    // Navigate to value proposition section
    const valuePropositionSection = page.locator('#value-proposition, .value-proposition, [data-section="value-proposition"]');
    await expect(valuePropositionSection).toBeVisible();

    // Find the Performance pillar card/column
    const performancePillar = page.locator('[data-pillar="performance"], .pillar-performance, .pillar:has-text("Performance")').first();
    await expect(performancePillar).toBeVisible();

    // Check for heading
    const performanceHeading = performancePillar.locator('h2, h3, .pillar-title');
    await expect(performanceHeading).toContainText(/Performance/i);

    // Check for Rust-powered async I/O with Tokio mention
    const performanceContent = performancePillar.locator('.pillar-description, p, .pillar-content');
    await expect(performanceContent).toContainText(/Rust/i);
    await expect(performanceContent).toContainText(/Tokio|async/i);
  });

  test('Test Case 4: Verify visual consistency of pillars', async ({ page }) => {
    // Navigate to value proposition section
    const valuePropositionSection = page.locator('#value-proposition, .value-proposition, [data-section="value-proposition"]');
    await expect(valuePropositionSection).toBeVisible();

    // Get all three pillar elements
    const pillars = page.locator('.pillar, .value-pillar, [data-pillar]');
    await expect(pillars).toHaveCount(3);

    // Get bounding boxes for all pillars
    const pillar1 = pillars.nth(0);
    const pillar2 = pillars.nth(1);
    const pillar3 = pillars.nth(2);

    const box1 = await pillar1.boundingBox();
    const box2 = await pillar2.boundingBox();
    const box3 = await pillar3.boundingBox();

    // Verify all pillars have consistent sizing (width should be similar within 10px tolerance)
    expect(box1).not.toBeNull();
    expect(box2).not.toBeNull();
    expect(box3).not.toBeNull();

    if (box1 && box2 && box3) {
      // Check consistent width (within 10px tolerance)
      expect(Math.abs(box1.width - box2.width)).toBeLessThan(10);
      expect(Math.abs(box2.width - box3.width)).toBeLessThan(10);
      expect(Math.abs(box1.width - box3.width)).toBeLessThan(10);

      // Check consistent height (within 50px tolerance as content may vary slightly)
      expect(Math.abs(box1.height - box2.height)).toBeLessThan(50);
      expect(Math.abs(box2.height - box3.height)).toBeLessThan(50);
      expect(Math.abs(box1.height - box3.height)).toBeLessThan(50);

      // Check alignment - all pillars should have the same y position (horizontal alignment within 5px)
      expect(Math.abs(box1.y - box2.y)).toBeLessThan(5);
      expect(Math.abs(box2.y - box3.y)).toBeLessThan(5);
    }

    // Verify all pillars have consistent styling by checking they share common CSS classes
    for (let i = 0; i < 3; i++) {
      const pillar = pillars.nth(i);
      await expect(pillar).toBeVisible();
      // Check each pillar has required structure
      await expect(pillar.locator('h2, h3, .pillar-title')).toBeVisible();
      await expect(pillar.locator('.pillar-description, p, .pillar-content')).toBeVisible();
    }
  });
});
