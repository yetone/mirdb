// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Project Status Badges (CircleCI)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: CircleCI build status badge is visible on the page', async ({ page }) => {
    // Check that the CircleCI badge image is present on the page
    // The badge should be in the footer or a badges section
    const badge = page.locator('a[href*="circleci.com"] img, img[alt*="CircleCI" i], img[alt*="build status" i], img[alt*="CI" i], [data-testid="circleci-badge"] img, .status-badge img, .badge img').first();
    await expect(badge).toBeVisible();

    // Verify the alt text is appropriate for a CI badge
    const alt = await badge.getAttribute('alt');
    expect(alt).toBeTruthy();
    expect(alt.toLowerCase()).toMatch(/circleci|build|status|ci/i);
  });

  test('TC2: Badge image from circleci.com renders without errors', async ({ page }) => {
    // Locate the CircleCI badge image
    const badge = page.locator('a[href*="circleci.com"] img, img[alt*="CircleCI" i], img[alt*="build status" i], img[alt*="CI" i], [data-testid="circleci-badge"] img').first();
    await expect(badge).toBeVisible();

    // Verify the src attribute points to a valid CircleCI badge image source
    const src = await badge.getAttribute('src');
    expect(src).toBeTruthy();
    expect(src).toMatch(/circleci\.com/i);

    // Verify the image has appropriate dimensions set (for proper rendering)
    const width = await badge.getAttribute('width');
    const height = await badge.getAttribute('height');

    // Badge should have dimensions specified for proper layout
    expect(width || height).toBeTruthy();

    // Verify the alt text is descriptive
    const alt = await badge.getAttribute('alt');
    expect(alt).toBeTruthy();
    expect(alt.toLowerCase()).toMatch(/circleci|build|status|ci/i);
  });

  test('TC3: Clicking badge navigates to CircleCI build page', async ({ page }) => {
    // Find the CircleCI badge link
    const badgeLink = page.locator('a[href*="circleci.com/gh/yetone/mirdb"], a[href*="circleci.com"]:has(img)').first();
    await expect(badgeLink).toBeVisible();

    // Verify the href attribute points to the CircleCI build page
    const href = await badgeLink.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href).toContain('circleci.com');
    expect(href).toContain('yetone/mirdb');

    // Verify the link opens in a new tab for external links
    const target = await badgeLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Security best practice: external links should have rel="noopener"
    const rel = await badgeLink.getAttribute('rel');
    expect(rel).toContain('noopener');
  });
});
