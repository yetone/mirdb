/**
 * Status Section E2E Tests
 * Owner: Scenario 5 - Status Section with Badges
 *
 * Tests:
 * - Status section presence
 * - CircleCI badge display
 * - Badge link functionality
 * - Feature checklist display
 */

// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Status Section with Badges', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Status section exists with id="status"', async ({ page }) => {
    // Test Case 1: Check Status section presence
    const statusSection = page.locator('section#status');
    await expect(statusSection).toBeVisible();
    await expect(statusSection).toHaveAttribute('id', 'status');
  });

  test('CircleCI status badge image is displayed', async ({ page }) => {
    // Test Case 2: Verify CircleCI badge presence
    const statusSection = page.locator('section#status');
    const circleCiBadge = statusSection.locator('img[alt*="CircleCI"], img[alt*="Build Status"], img.status-badge');
    await expect(circleCiBadge).toBeVisible();

    // Verify the badge has a valid src attribute
    const src = await circleCiBadge.getAttribute('src');
    expect(src).toBeTruthy();
    expect(src).toContain('circleci');
  });

  test('CircleCI badge links to build status page and opens in new tab', async ({ page }) => {
    // Test Case 3: Click CircleCI badge
    const statusSection = page.locator('section#status');
    const badgeLink = statusSection.locator('a').filter({ has: page.locator('img[alt*="CircleCI"], img[alt*="Build Status"], img.status-badge') });

    await expect(badgeLink).toBeVisible();

    // Verify the link opens in new tab
    const target = await badgeLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify rel attribute for security
    const rel = await badgeLink.getAttribute('rel');
    expect(rel).toContain('noopener');

    // Verify link points to CircleCI
    const href = await badgeLink.getAttribute('href');
    expect(href).toContain('circleci');
  });

  test('Feature checklist shows implemented features with checkmarks and planned features distinctly', async ({ page }) => {
    // Test Case 4: Verify feature checklist display
    const statusSection = page.locator('section#status');

    // Check for feature checklist container
    const featureChecklist = statusSection.locator('.status-checklist, .feature-checklist, ul.checklist');
    await expect(featureChecklist).toBeVisible();

    // Check for implemented features (should have checkmarks/completed indicators)
    const implementedFeatures = statusSection.locator('.status-feature--implemented, .feature-item--implemented, li.implemented, [data-status="implemented"]');
    const implementedCount = await implementedFeatures.count();
    expect(implementedCount).toBeGreaterThan(0);

    // Check for planned features (should be visually distinct)
    const plannedFeatures = statusSection.locator('.status-feature--planned, .feature-item--planned, li.planned, [data-status="planned"]');
    const plannedCount = await plannedFeatures.count();
    expect(plannedCount).toBeGreaterThan(0);

    // Verify visual distinction - implemented should have checkmark indicators
    const firstImplemented = implementedFeatures.first();
    const checkmarkIcon = firstImplemented.locator('svg, .checkmark, .check-icon, span[aria-label*="check"]');
    await expect(checkmarkIcon.or(firstImplemented.locator(':text("✓"), :text("✔")'))).toBeVisible();
  });

  test('Status section has proper heading', async ({ page }) => {
    // Additional test for section structure
    const statusSection = page.locator('section#status');
    const heading = statusSection.locator('h2');
    await expect(heading).toBeVisible();
    await expect(heading).toContainText(/status|project health/i);
  });

  test('Badge image has proper alt text for accessibility', async ({ page }) => {
    const statusSection = page.locator('section#status');
    const badge = statusSection.locator('img').first();
    const altText = await badge.getAttribute('alt');
    expect(altText).toBeTruthy();
    expect(altText.length).toBeGreaterThan(0);
  });
});
