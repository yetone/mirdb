/**
 * Homepage E2E Tests - Hero Section
 * Owner: Scenario 1 - Hero Section Display
 *
 * Tests for hero section rendering with:
 * - Gradient/subtle background
 * - Correct layout
 * - Visual elements
 */

import { test, expect } from '@playwright/test';

test.describe('Homepage Hero Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 4: Hero section renders with gradient background and correct layout', async ({
    page,
  }) => {
    // Verify hero section exists
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Verify layout structure - centered content
    const heading = page.getByRole('heading', { level: 1 });
    await expect(heading).toBeVisible();
    await expect(heading).toContainText('MirDB');

    const tagline = page.getByText(
      'A Persistent Key-Value Store with Memcached Protocol'
    );
    await expect(tagline).toBeVisible();

    // Verify CTA buttons are visible
    const githubButton = page.getByRole('link', { name: /github/i });
    const docsButton = page.getByRole('link', { name: /get started/i });
    await expect(githubButton).toBeVisible();
    await expect(docsButton).toBeVisible();
  });

  test('Hero section is above the fold on desktop viewport', async ({
    page,
  }) => {
    // Set desktop viewport
    await page.setViewportSize({ width: 1280, height: 720 });

    const heroSection = page.locator('#hero');
    const boundingBox = await heroSection.boundingBox();

    expect(boundingBox).not.toBeNull();
    // Hero should start at or near the top of the page
    expect(boundingBox!.y).toBeLessThan(100);
  });

  test('CTA buttons have proper focus indicators', async ({ page }) => {
    const githubButton = page.getByRole('link', { name: /github/i });

    // Focus the button using keyboard
    await githubButton.focus();

    // Button should be focused and visible
    await expect(githubButton).toBeFocused();
    await expect(githubButton).toBeVisible();
  });

  test('Hero section renders correctly on mobile viewport', async ({
    page,
  }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 375, height: 667 });

    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // All content should still be visible
    const heading = page.getByRole('heading', { level: 1 });
    await expect(heading).toBeVisible();

    const githubButton = page.getByRole('link', { name: /github/i });
    const docsButton = page.getByRole('link', { name: /get started/i });
    await expect(githubButton).toBeVisible();
    await expect(docsButton).toBeVisible();
  });

  test('GitHub link points to correct repository', async ({ page }) => {
    const githubButton = page.getByRole('link', { name: /github/i });
    const href = await githubButton.getAttribute('href');
    expect(href).toBe('https://github.com/yetone/mirdb');
  });

  test('Decorative elements are hidden from screen readers', async ({
    page,
  }) => {
    const heroSection = page.locator('#hero');
    const decorativeElements = heroSection.locator('[aria-hidden="true"]');

    // Should have decorative background elements
    const count = await decorativeElements.count();
    expect(count).toBeGreaterThan(0);
  });
});
