/**
 * Hero Section E2E Tests - Badge Tests
 * Owner: Scenario 5 - CI/CD Status Badges
 *
 * Tests:
 * - Badge image display and alt text
 * - Badge link to CI dashboard
 * - Badge image source
 */
import { test, expect } from '@playwright/test';

test.describe('CI/CD Status Badges', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('should display CircleCI status badge image', async ({ page }) => {
    // Wait for the page to load
    await page.waitForLoadState('domcontentloaded');

    // Find badge image by test id
    const badgeImage = page.locator('[data-testid="status-badge-image"]');
    await expect(badgeImage).toBeVisible();

    // Verify src contains the CircleCI badge URL
    const src = await badgeImage.getAttribute('src');
    expect(src).toContain('atompunk.yetone.fun/github/yetone/mirdb');
  });

  test('should have descriptive alt text on badge image', async ({ page }) => {
    // Wait for the page to load
    await page.waitForLoadState('domcontentloaded');

    // Find badge image
    const badgeImage = page.locator('[data-testid="status-badge-image"]');
    await expect(badgeImage).toBeVisible();

    // Verify alt text is descriptive (e.g., 'Build Status')
    const altText = await badgeImage.getAttribute('alt');
    expect(altText).toBeTruthy();
    expect(altText?.length).toBeGreaterThan(0);
    // Alt text should describe the badge's purpose
    expect(altText?.toLowerCase()).toMatch(/build|status|ci/i);
  });

  test('should wrap badge in link pointing to CircleCI dashboard', async ({ page }) => {
    // Wait for the page to load
    await page.waitForLoadState('domcontentloaded');

    // Find badge link
    const badgeLink = page.locator('[data-testid="status-badge"]');
    await expect(badgeLink).toBeVisible();

    // Verify it's an anchor element
    const tagName = await badgeLink.evaluate(el => el.tagName);
    expect(tagName).toBe('A');

    // Verify href points to CircleCI
    const href = await badgeLink.getAttribute('href');
    expect(href).toContain('circleci.com/gh/yetone/mirdb');
  });

  test('should have proper security attributes on badge link', async ({ page }) => {
    // Wait for the page to load
    await page.waitForLoadState('domcontentloaded');

    // Find badge link
    const badgeLink = page.locator('[data-testid="status-badge"]');
    await expect(badgeLink).toBeVisible();

    // Verify security attributes
    await expect(badgeLink).toHaveAttribute('target', '_blank');
    await expect(badgeLink).toHaveAttribute('rel', 'noopener noreferrer');
  });

  test('should render badge image wrapped in anchor tag with correct structure', async ({ page }) => {
    // Wait for the page to load
    await page.waitForLoadState('domcontentloaded');

    // Find badge link and image
    const badgeLink = page.locator('[data-testid="status-badge"]');
    const badgeImage = page.locator('[data-testid="status-badge-image"]');

    await expect(badgeLink).toBeVisible();
    await expect(badgeImage).toBeVisible();

    // Verify image is inside the link
    const imageInLink = badgeLink.locator('[data-testid="status-badge-image"]');
    await expect(imageInLink).toBeVisible();

    // Verify the link has CircleCI URL
    const href = await badgeLink.getAttribute('href');
    expect(href).toContain('circleci.com');

    // Verify the image has the badge URL
    const src = await badgeImage.getAttribute('src');
    expect(src).toContain('atompunk.yetone.fun');
  });
});
