/**
 * Status Badges Tests
 * Owner: Scenario 5 - Status Badges Display
 *
 * Test cases:
 * - CircleCI badge image present
 * - CircleCI badge is clickable
 * - Version badge/text displayed
 * - Badges have proper alt text
 */

import { test, expect } from '@playwright/test';

test.describe('Status Badges Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  test('TC1: CircleCI badge image exists with correct src', async ({ page }) => {
    // Find the badges section
    const badgesSection = page.locator('.badges-section');
    await expect(badgesSection).toBeVisible();

    // Find the CircleCI badge image
    const circleciBadge = page.locator('.badge-image');
    await expect(circleciBadge).toBeVisible();

    // Verify the src contains circleci
    const src = await circleciBadge.getAttribute('src');
    expect(src).toBeTruthy();
    expect(src!.toLowerCase()).toContain('circleci');
  });

  test('TC2: CircleCI badge is clickable and links to CircleCI project page', async ({ page }) => {
    // Find the CircleCI badge link
    const badgeLink = page.locator('.badge-link');
    await expect(badgeLink).toBeVisible();

    // Verify the link points to CircleCI project page
    const href = await badgeLink.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href).toContain('circleci.com');
    expect(href).toContain('yetone/mirdb');

    // Verify the link opens in a new tab (target="_blank")
    const target = await badgeLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify rel attribute for security
    const rel = await badgeLink.getAttribute('rel');
    expect(rel).toContain('noopener');
  });

  test('TC3: Version information (0.1.0) is displayed on the page', async ({ page }) => {
    // Find the version badge
    const versionBadge = page.locator('.version-badge');
    await expect(versionBadge).toBeVisible();

    // Check for version label
    const versionLabel = page.locator('.version-badge-label');
    await expect(versionLabel).toBeVisible();
    await expect(versionLabel).toContainText('version');

    // Check for version value
    const versionValue = page.locator('.version-badge-value');
    await expect(versionValue).toBeVisible();
    const versionText = await versionValue.textContent();
    expect(versionText).toBeTruthy();
    // Should contain version number (0.1.0)
    expect(versionText).toMatch(/\d+\.\d+\.\d+/);
    expect(versionText).toContain('0.1.0');
  });

  test('TC4: Badge images have descriptive alt text for accessibility', async ({ page }) => {
    // Find the CircleCI badge image
    const circleciBadge = page.locator('.badge-image');
    await expect(circleciBadge).toBeVisible();

    // Verify alt text is present and descriptive
    const altText = await circleciBadge.getAttribute('alt');
    expect(altText).toBeTruthy();
    expect(altText!.length).toBeGreaterThan(5); // Not just empty or minimal text

    // Alt text should describe what the badge represents
    const altLower = altText!.toLowerCase();
    expect(
      altLower.includes('circleci') ||
      altLower.includes('build') ||
      altLower.includes('status')
    ).toBe(true);

    // Verify version badge has aria-label for accessibility
    const versionBadge = page.locator('.version-badge');
    const ariaLabel = await versionBadge.getAttribute('aria-label');
    expect(ariaLabel).toBeTruthy();
    expect(ariaLabel!.toLowerCase()).toContain('version');
  });

  test('Badges section is positioned after CTA buttons', async ({ page }) => {
    // Verify badges section exists within hero container
    const heroContainer = page.locator('.hero-container');
    await expect(heroContainer).toBeVisible();

    // Verify both CTA and badges sections exist within hero
    const ctaSection = heroContainer.locator('.hero-cta');
    const badgesSection = heroContainer.locator('.badges-section');

    await expect(ctaSection).toBeVisible();
    await expect(badgesSection).toBeVisible();

    // Verify badges appear after CTA (check vertical position)
    const ctaBox = await ctaSection.boundingBox();
    const badgesBox = await badgesSection.boundingBox();

    expect(ctaBox).toBeTruthy();
    expect(badgesBox).toBeTruthy();
    // Badges should be below CTA buttons
    expect(badgesBox!.y).toBeGreaterThan(ctaBox!.y);
  });
});
