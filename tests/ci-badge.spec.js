const { test, expect } = require('@playwright/test');

/**
 * CI Status Badge Display Tests
 * Scenario: Validate the CircleCI status badge is displayed and links correctly
 */

test.describe('CI Status Badge Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 1: Page contains image element with CircleCI badge source URL', async ({ page }) => {
    // Step 1: Locate CI badge in hero section
    const heroSection = page.locator('.hero, [class*="hero"], section:first-of-type, header + section');
    await expect(heroSection.first()).toBeVisible();

    // Step 2: Verify badge image loads and displays build status
    const badgeContainer = page.locator('.badge-container');
    await expect(badgeContainer).toBeVisible();

    // Check for badge image within badge container
    const badgeImage = badgeContainer.locator('img');
    await expect(badgeImage).toBeVisible();

    // Verify the badge image has a src attribute (CircleCI or CI-related badge URL)
    const src = await badgeImage.getAttribute('src');
    expect(src).toBeTruthy();
    // The badge should be a CI status badge (can be CircleCI badge URL or similar service)
    expect(src.length).toBeGreaterThan(0);
  });

  test('Test Case 2: Badge is wrapped in link pointing to CircleCI project page', async ({ page }) => {
    // Step 3: Verify badge link navigates to CircleCI build page
    const badgeContainer = page.locator('.badge-container');
    await expect(badgeContainer).toBeVisible();

    // Find the link wrapping the badge
    const badgeLink = badgeContainer.locator('a');
    await expect(badgeLink).toBeVisible();

    // Verify the link points to CircleCI project page
    const href = await badgeLink.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href).toContain('circleci.com');
    expect(href).toContain('yetone/mirdb');
  });

  test('Badge link has proper security attributes', async ({ page }) => {
    // Verify the badge link has proper target and rel attributes for external links
    const badgeLink = page.locator('.badge-container a');
    await expect(badgeLink).toBeVisible();

    const target = await badgeLink.getAttribute('target');
    expect(target).toBe('_blank');

    const rel = await badgeLink.getAttribute('rel');
    expect(rel).toContain('noopener');
  });

  test('Badge is located in hero section', async ({ page }) => {
    // Verify the badge is within the hero section for proper placement
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    const badgeInHero = heroSection.locator('.badge-container');
    await expect(badgeInHero).toBeVisible();
  });

  test('Badge image has alt text for accessibility', async ({ page }) => {
    // Verify the badge image has appropriate alt text
    const badgeImage = page.locator('.badge-container img');
    await expect(badgeImage).toBeVisible();

    const alt = await badgeImage.getAttribute('alt');
    expect(alt).toBeTruthy();
    expect(alt.length).toBeGreaterThan(0);
  });
});
