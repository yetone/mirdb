// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Project Status Badges (REQ-4)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 1: CircleCI build status badge is displayed', async ({ page }) => {
    // Query for build status badge
    const statusBadges = page.locator('[data-testid="status-badges"]');
    await expect(statusBadges).toBeVisible();

    const buildBadge = page.locator('[data-testid="badge-build-status"]');
    await expect(buildBadge).toBeVisible();

    // Verify it contains a CircleCI badge image
    const buildBadgeImg = page.locator('[data-testid="badge-build-status-img"]');
    await expect(buildBadgeImg).toBeVisible();
    await expect(buildBadgeImg).toHaveAttribute('alt', 'CircleCI Build Status');

    // Verify badge image src points to CircleCI
    const imgSrc = await buildBadgeImg.getAttribute('src');
    expect(imgSrc).toContain('circleci');
  });

  test('Test Case 2: Version badge is displayed', async ({ page }) => {
    // Query for version badge
    const versionBadge = page.locator('[data-testid="badge-version"]');
    await expect(versionBadge).toBeVisible();

    // Verify it contains a version badge image
    const versionBadgeImg = page.locator('[data-testid="badge-version-img"]');
    await expect(versionBadgeImg).toBeVisible();

    // Verify the alt text indicates version
    const altText = await versionBadgeImg.getAttribute('alt');
    expect(altText).toMatch(/version/i);

    // Verify badge image src contains version info
    const imgSrc = await versionBadgeImg.getAttribute('src');
    expect(imgSrc).toContain('version');
  });

  test('Test Case 3: License badge is displayed', async ({ page }) => {
    // Query for license badge
    const licenseBadge = page.locator('[data-testid="badge-license"]');
    await expect(licenseBadge).toBeVisible();

    // Verify it contains a license badge image
    const licenseBadgeImg = page.locator('[data-testid="badge-license-img"]');
    await expect(licenseBadgeImg).toBeVisible();

    // Verify the alt text indicates license
    const altText = await licenseBadgeImg.getAttribute('alt');
    expect(altText).toMatch(/license/i);

    // Verify badge image src contains license info
    const imgSrc = await licenseBadgeImg.getAttribute('src');
    expect(imgSrc).toContain('license');
  });

  test('Test Case 4: Build status badge links to CircleCI dashboard', async ({ page }) => {
    // Click build status badge and verify it links to CircleCI
    const buildBadge = page.locator('[data-testid="badge-build-status"]');
    await expect(buildBadge).toBeVisible();

    // Verify the badge links to CircleCI
    await expect(buildBadge).toHaveAttribute('href', 'https://circleci.com/gh/yetone/mirdb');

    // Verify proper link attributes for external link
    await expect(buildBadge).toHaveAttribute('target', '_blank');
    await expect(buildBadge).toHaveAttribute('rel', 'noopener noreferrer');
  });

  test('Test Case 5: All badge images load correctly without broken image icons', async ({ page }) => {
    // Verify all badge images in the status badges section are properly configured
    const badgeImages = page.locator('[data-testid="status-badges"] img');
    const count = await badgeImages.count();

    // Expect at least 3 badge images (build, version, license)
    expect(count).toBeGreaterThanOrEqual(3);

    // Verify each image is visible and has proper attributes
    for (let i = 0; i < count; i++) {
      const img = badgeImages.nth(i);
      await expect(img).toBeVisible();

      // Verify image has src attribute with valid URL
      const src = await img.getAttribute('src');
      expect(src).toBeTruthy();
      expect(src.length).toBeGreaterThan(0);
      expect(src).toMatch(/^https?:\/\//);

      // Verify image has alt attribute for accessibility
      const alt = await img.getAttribute('alt');
      expect(alt).toBeTruthy();
      expect(alt.length).toBeGreaterThan(0);
    }

    // Verify specific badge images exist with correct sources
    const buildBadgeImg = page.locator('[data-testid="badge-build-status-img"]');
    const versionBadgeImg = page.locator('[data-testid="badge-version-img"]');
    const licenseBadgeImg = page.locator('[data-testid="badge-license-img"]');

    // Check that all three required badges are present
    await expect(buildBadgeImg).toBeVisible();
    await expect(versionBadgeImg).toBeVisible();
    await expect(licenseBadgeImg).toBeVisible();

    // Verify each badge has a valid image source URL
    const buildSrc = await buildBadgeImg.getAttribute('src');
    const versionSrc = await versionBadgeImg.getAttribute('src');
    const licenseSrc = await licenseBadgeImg.getAttribute('src');

    // Build badge should point to CircleCI
    expect(buildSrc).toContain('circleci');

    // Version badge should be from shields.io and contain version info
    expect(versionSrc).toContain('img.shields.io');
    expect(versionSrc).toContain('version');

    // License badge should be from shields.io and contain license info
    expect(licenseSrc).toContain('img.shields.io');
    expect(licenseSrc).toContain('license');
  });
});
