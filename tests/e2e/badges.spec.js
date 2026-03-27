/**
 * Project Status Badges E2E Tests
 * Owner: Scenario 6 - Project Status Badges
 *
 * Tests:
 * - CircleCI badge visibility
 * - Badge links to CI pipeline
 * - Badge accessibility
 */
import { test, expect } from '@playwright/test';

test.describe('Project Status Badges Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('CircleCI status badge is present and visible', async ({ page }) => {
    // Navigate to status section
    const statusSection = page.locator('#status');
    await expect(statusSection).toBeVisible();

    // Check for CircleCI badge image
    const circleCIBadge = statusSection.locator('img[alt="Build Status"]');
    await expect(circleCIBadge).toBeVisible();

    // Verify badge image source is correct
    const badgeSrc = await circleCIBadge.getAttribute('src');
    expect(badgeSrc).toContain('circleci.com');
    expect(badgeSrc).toContain('yetone/mirdb');
  });

  test('CircleCI badge links to CI pipeline page', async ({ page }) => {
    const statusSection = page.locator('#status');

    // Find the badge link
    const badgeLink = statusSection.locator('a[href*="circleci.com"]');
    await expect(badgeLink).toBeVisible();

    // Verify href points to CircleCI pipeline
    const href = await badgeLink.getAttribute('href');
    expect(href).toBe('https://circleci.com/gh/yetone/mirdb');

    // Verify link opens in new tab
    const target = await badgeLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify rel attribute for security
    const rel = await badgeLink.getAttribute('rel');
    expect(rel).toContain('noopener');
  });

  test('Status section has proper heading', async ({ page }) => {
    const heading = page.locator('#status h2');
    await expect(heading).toHaveText('Project Status');
  });

  test('Badge has accessible aria-label', async ({ page }) => {
    const statusSection = page.locator('#status');
    const badgeLink = statusSection.locator('a.status-badge');

    // Verify aria-label exists
    const ariaLabel = await badgeLink.getAttribute('aria-label');
    expect(ariaLabel).toBeTruthy();
    expect(ariaLabel.toLowerCase()).toContain('circleci');
  });
});
