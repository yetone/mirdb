/**
 * Technical Specifications E2E Tests
 * Owner: Scenario 7 - Technical Specifications Display
 *
 * Tests:
 * - Specifications section visibility
 * - All five technical specifications displayed with correct values
 * - Grid layout for specifications display
 */

import { test, expect } from '@playwright/test';

test.describe('Technical Specifications Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('specifications section is visible', async ({ page }) => {
    const specsSection = page.locator('#specifications');
    await expect(specsSection).toBeVisible();
  });

  test('specifications section has correct heading', async ({ page }) => {
    const heading = page.locator('#specifications-heading');
    await expect(heading).toBeVisible();
    await expect(heading).toHaveText('Technical Specifications');
  });

  test('listen address shows 0.0.0.0:12333', async ({ page }) => {
    const specCard = page.locator('[data-spec="listen-address"]');
    await expect(specCard).toBeVisible();

    const specValue = page.locator('[data-testid="spec-listen-address"]');
    await expect(specValue).toHaveText('0.0.0.0:12333');
  });

  test('max LSM levels shows 7', async ({ page }) => {
    const specCard = page.locator('[data-spec="max-lsm-levels"]');
    await expect(specCard).toBeVisible();

    const specValue = page.locator('[data-testid="spec-max-lsm-levels"]');
    await expect(specValue).toHaveText('7');
  });

  test('SSTable max size shows 100MB', async ({ page }) => {
    const specCard = page.locator('[data-spec="sstable-max-size"]');
    await expect(specCard).toBeVisible();

    const specValue = page.locator('[data-testid="spec-sstable-max-size"]');
    await expect(specValue).toHaveText('100MB');
  });

  test('Memtable max size shows 4MB', async ({ page }) => {
    const specCard = page.locator('[data-spec="memtable-max-size"]');
    await expect(specCard).toBeVisible();

    const specValue = page.locator('[data-testid="spec-memtable-max-size"]');
    await expect(specValue).toHaveText('4MB');
  });

  test('Block size shows 4KB', async ({ page }) => {
    const specCard = page.locator('[data-spec="block-size"]');
    await expect(specCard).toBeVisible();

    const specValue = page.locator('[data-testid="spec-block-size"]');
    await expect(specValue).toHaveText('4KB');
  });

  test('specifications displayed in organized grid format', async ({ page }) => {
    // Check that the specifications grid exists
    const specsGrid = page.locator('.specifications-grid');
    await expect(specsGrid).toBeVisible();

    // Verify all 5 spec cards are present
    const specCards = page.locator('.spec-card');
    await expect(specCards).toHaveCount(5);

    // Verify grid layout is applied (using CSS grid)
    const gridDisplay = await specsGrid.evaluate(el =>
      window.getComputedStyle(el).display
    );
    expect(gridDisplay).toBe('grid');
  });

  test('all specification cards have icons', async ({ page }) => {
    const specCards = page.locator('.spec-card');
    const count = await specCards.count();

    for (let i = 0; i < count; i++) {
      const icon = specCards.nth(i).locator('.spec-icon');
      await expect(icon).toBeVisible();
    }
  });

  test('all specification cards have labels and values', async ({ page }) => {
    const specCards = page.locator('.spec-card');
    const count = await specCards.count();

    for (let i = 0; i < count; i++) {
      const label = specCards.nth(i).locator('.spec-label');
      const value = specCards.nth(i).locator('.spec-value');
      await expect(label).toBeVisible();
      await expect(value).toBeVisible();
    }
  });

  test('specifications section is accessible via navigation', async ({ page }) => {
    // Navigate to the specifications section by scrolling
    const specsSection = page.locator('#specifications');
    await specsSection.scrollIntoViewIfNeeded();

    // Verify the section is in viewport
    await expect(specsSection).toBeInViewport();
  });
});
