/**
 * Features Section E2E Tests
 * Owner: Scenario 4 - Features Section Display
 *
 * Tests:
 * - Feature cards are visible and properly displayed
 * - Hover effects work on desktop
 * - Cards are interactive
 */

import { test, expect } from '@playwright/test';

test.describe('Test Case 5: Feature Card Hover Effects', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Scroll to features section
    await page.locator('#features').scrollIntoViewIfNeeded();
  });

  test('should display feature cards on the page', async ({ page }) => {
    const featureCards = page.locator('.feature-card');
    await expect(featureCards).toHaveCount(4);
  });

  test('should show hover effect when hovering over feature card on desktop', async ({ page, isMobile }) => {
    test.skip(isMobile, 'Hover effects are desktop only');

    const firstCard = page.locator('.feature-card').first();

    // Get initial transform value
    const initialTransform = await firstCard.evaluate((el) => {
      return window.getComputedStyle(el).transform;
    });

    // Hover over the card
    await firstCard.hover();

    // Wait for transition
    await page.waitForTimeout(300);

    // Get transform after hover
    const hoverTransform = await firstCard.evaluate((el) => {
      return window.getComputedStyle(el).transform;
    });

    // The transform should change (translateY(-4px) creates a matrix with different values)
    expect(initialTransform).not.toBe(hoverTransform);
  });

  test('should have smooth transition on hover', async ({ page, isMobile }) => {
    test.skip(isMobile, 'Hover effects are desktop only');

    const firstCard = page.locator('.feature-card').first();

    // Verify transition property is set
    const transition = await firstCard.evaluate((el) => {
      return window.getComputedStyle(el).transition;
    });

    expect(transition).toContain('transform');
  });

  test('should change box-shadow on hover', async ({ page, isMobile }) => {
    test.skip(isMobile, 'Hover effects are desktop only');

    const firstCard = page.locator('.feature-card').first();

    // Get initial box-shadow
    const initialShadow = await firstCard.evaluate((el) => {
      return window.getComputedStyle(el).boxShadow;
    });

    // Hover over the card
    await firstCard.hover();

    // Wait for transition
    await page.waitForTimeout(300);

    // Get box-shadow after hover
    const hoverShadow = await firstCard.evaluate((el) => {
      return window.getComputedStyle(el).boxShadow;
    });

    // Box shadow should change on hover (becomes larger/more prominent)
    expect(hoverShadow).not.toBe(initialShadow);
  });

  test('should have all feature cards visible in viewport after scrolling', async ({ page }) => {
    const featureCards = page.locator('.feature-card');
    const count = await featureCards.count();

    for (let i = 0; i < count; i++) {
      const card = featureCards.nth(i);
      await expect(card).toBeVisible();
    }
  });

  test('should display icon, title, and description for each card', async ({ page }) => {
    const featureCards = page.locator('.feature-card');
    const count = await featureCards.count();

    for (let i = 0; i < count; i++) {
      const card = featureCards.nth(i);

      // Check icon
      const icon = card.locator('.feature-card__icon svg');
      await expect(icon).toBeVisible();

      // Check title
      const title = card.locator('.feature-card__title');
      await expect(title).toBeVisible();
      const titleText = await title.textContent();
      expect(titleText.trim().length).toBeGreaterThan(0);

      // Check description
      const description = card.locator('.feature-card__description');
      await expect(description).toBeVisible();
      const descText = await description.textContent();
      expect(descText.trim().length).toBeGreaterThan(0);
    }
  });

  test('should have proper visual hierarchy with centered text', async ({ page }) => {
    const firstCard = page.locator('.feature-card').first();

    const textAlign = await firstCard.evaluate((el) => {
      return window.getComputedStyle(el).textAlign;
    });

    expect(textAlign).toBe('center');
  });
});

test.describe('Features Section Layout', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.locator('#features').scrollIntoViewIfNeeded();
  });

  test('should display features section with proper heading', async ({ page }) => {
    const heading = page.locator('.features__title');
    await expect(heading).toBeVisible();
    await expect(heading).toHaveText('Features');
  });

  test('should have feature cards in a grid layout', async ({ page }) => {
    const grid = page.locator('.features__grid');
    await expect(grid).toBeVisible();

    const display = await grid.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });

    expect(display).toBe('grid');
  });

  test('should have between 3 and 5 feature cards', async ({ page }) => {
    const featureCards = page.locator('.feature-card');
    const count = await featureCards.count();

    expect(count).toBeGreaterThanOrEqual(3);
    expect(count).toBeLessThanOrEqual(5);
  });
});
