import { test, expect } from '@playwright/test';

/**
 * E2E Tests for Project Status Badges
 *
 * This test suite verifies that project status badges (CI status, version) are displayed correctly.
 * Badges typically show CI pipeline status (e.g., CircleCI) and version information.
 *
 * Requirements: REQ-8 - Display project status badges (CI status, version)
 */

test.describe('Project Status Badges', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 1: Check for CI status badge', async ({ page }) => {
    // The CI badge should be visible in the badges section
    const badgesSection = page.locator('[data-section="badges"]');
    await expect(badgesSection).toBeVisible();

    // Find the CI badge (CircleCI or equivalent)
    const ciBadge = page.locator('[data-badge="ci"]');
    await expect(ciBadge).toBeVisible();

    // The badge should be a link to the CI service
    const badgeLink = ciBadge.locator('a');
    await expect(badgeLink).toBeVisible();

    // The link should point to a CI service (CircleCI)
    const href = await badgeLink.getAttribute('href');
    expect(href).toContain('circleci');

    // The badge should contain an image
    const badgeImage = ciBadge.locator('img');
    await expect(badgeImage).toBeVisible();

    // The image should have appropriate alt text
    const altText = await badgeImage.getAttribute('alt');
    expect(altText).toBeTruthy();
    expect(altText?.toLowerCase()).toMatch(/ci|build|status|circleci/);
  });

  test('Test Case 2: Check for version badge', async ({ page }) => {
    // The badges section should be visible
    const badgesSection = page.locator('[data-section="badges"]');
    await expect(badgesSection).toBeVisible();

    // Find the version badge
    const versionBadge = page.locator('[data-badge="version"]');
    await expect(versionBadge).toBeVisible();

    // The version badge should contain an image
    const badgeImage = versionBadge.locator('img');
    await expect(badgeImage).toBeVisible();

    // The image should have appropriate alt text mentioning version
    const altText = await badgeImage.getAttribute('alt');
    expect(altText).toBeTruthy();
    expect(altText?.toLowerCase()).toMatch(/version/);
  });

  test('Test Case 3: Verify badge images load successfully', async ({ page }) => {
    // The badges section should be visible
    const badgesSection = page.locator('[data-section="badges"]');
    await expect(badgesSection).toBeVisible();

    // Get all badge images
    const badgeImages = page.locator('[data-section="badges"] img');
    const count = await badgeImages.count();

    // There should be at least 2 badges (CI and version)
    expect(count).toBeGreaterThanOrEqual(2);

    // Verify each badge image loads successfully (no broken images)
    for (let i = 0; i < count; i++) {
      const img = badgeImages.nth(i);
      await expect(img).toBeVisible();

      // Check that the image has a valid src attribute
      const src = await img.getAttribute('src');
      expect(src).toBeTruthy();

      // Verify the image has loaded by checking its naturalWidth
      // (broken images have naturalWidth of 0)
      const naturalWidth = await img.evaluate((el: HTMLImageElement) => el.naturalWidth);
      expect(naturalWidth).toBeGreaterThan(0);
    }
  });

  test('Badges section should be prominently positioned in hero area', async ({ page }) => {
    // The badges section should exist within or near the hero section
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    // Badges should be in the hero section
    const badgesInHero = heroSection.locator('[data-section="badges"]');
    await expect(badgesInHero).toBeVisible();
  });

  test('Badge images should have appropriate dimensions', async ({ page }) => {
    const badgesSection = page.locator('[data-section="badges"]');
    await expect(badgesSection).toBeVisible();

    const badgeImages = page.locator('[data-section="badges"] img');
    const count = await badgeImages.count();

    for (let i = 0; i < count; i++) {
      const img = badgeImages.nth(i);
      const boundingBox = await img.boundingBox();

      // Badge images should have reasonable dimensions
      // Shields.io badges are typically around 80-150px wide and ~20px tall
      expect(boundingBox).not.toBeNull();
      if (boundingBox) {
        expect(boundingBox.width).toBeGreaterThan(50);
        expect(boundingBox.height).toBeGreaterThan(10);
      }
    }
  });

  test('CI badge link should open in new tab', async ({ page }) => {
    const ciBadge = page.locator('[data-badge="ci"] a');
    await expect(ciBadge).toBeVisible();

    // Check that the link has target="_blank" for opening in new tab
    const target = await ciBadge.getAttribute('target');
    expect(target).toBe('_blank');

    // Check for rel="noopener" for security
    const rel = await ciBadge.getAttribute('rel');
    expect(rel).toContain('noopener');
  });
});
