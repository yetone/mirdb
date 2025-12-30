const { test, expect } = require('@playwright/test');

/**
 * User Story 5 (US-5): Project Engagement Validation Tests
 *
 * Scenario: Validate acceptance criteria for US-5 - Contributor can access GitHub and CI status
 *
 * As a Contributor, I want to easily access the GitHub repository and CI status,
 * so that I can evaluate the project health and contribute.
 *
 * Acceptance Criteria:
 * - Given I am on the homepage
 * - When I look for project links
 * - Then I find a clear link to the GitHub repository
 * - And I can see the current CI/CD build status
 */

test.describe('US-5: Project Engagement - GitHub and CI Status Access', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: GitHub Link Validation
   * Input: Click GitHub link
   * Expected: Link navigates to https://github.com/yetone/mirdb
   * Type: e2e
   */
  test('Test Case 1: GitHub link navigates to https://github.com/yetone/mirdb', async ({ page }) => {
    // Step 1: Find GitHub link - should be easily discoverable
    // Check multiple locations: hero section CTA, header navigation, footer

    // Check hero section "View on GitHub" button
    const heroGitHubLink = page.locator('.hero a').filter({ hasText: /GitHub/i });
    await expect(heroGitHubLink.first()).toBeVisible();

    // Step 2: Verify link destination - should link to yetone/mirdb
    const heroHref = await heroGitHubLink.first().getAttribute('href');
    expect(heroHref).toBe('https://github.com/yetone/mirdb');

    // Also verify header navigation GitHub link
    const headerGitHubLink = page.locator('header nav a').filter({ hasText: /GitHub/i });
    await expect(headerGitHubLink.first()).toBeVisible();

    const headerHref = await headerGitHubLink.first().getAttribute('href');
    expect(headerHref).toBe('https://github.com/yetone/mirdb');

    // Verify the links have proper external link attributes
    const target = await heroGitHubLink.first().getAttribute('target');
    expect(target).toBe('_blank');

    const rel = await heroGitHubLink.first().getAttribute('rel');
    expect(rel).toContain('noopener');
  });

  /**
   * Test Case 2: CI Badge Visibility Validation
   * Input: Verify CI badge visibility
   * Expected: CircleCI status badge is visible on the page
   * Type: e2e
   */
  test('Test Case 2: CircleCI status badge is visible on the page', async ({ page }) => {
    // Step 3: Find CI status - locate and verify CI/CD build status badge

    // Locate the badge container in the hero section
    const badgeContainer = page.locator('.badge-container');
    await expect(badgeContainer).toBeVisible();

    // Verify the CI badge image is visible
    const badgeImage = badgeContainer.locator('img');
    await expect(badgeImage).toBeVisible();

    // Verify the badge has proper alt text for accessibility
    const altText = await badgeImage.getAttribute('alt');
    expect(altText).toBeTruthy();
    expect(altText.toLowerCase()).toContain('circleci');

    // Verify the badge links to CircleCI project page
    const badgeLink = badgeContainer.locator('a');
    await expect(badgeLink).toBeVisible();

    const href = await badgeLink.getAttribute('href');
    expect(href).toContain('circleci.com');
    expect(href).toContain('yetone/mirdb');

    // Verify the badge is displayed in the hero section (prominent placement)
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    const badgeInHero = heroSection.locator('.badge-container');
    await expect(badgeInHero).toBeVisible();
  });

  /**
   * Additional test: Multiple GitHub link access points
   * Verifies contributors can find GitHub links in multiple locations
   */
  test('GitHub repository links are accessible from multiple locations', async ({ page }) => {
    const expectedGitHubUrl = 'https://github.com/yetone/mirdb';

    // Hero section "View on GitHub" button
    const heroCTA = page.locator('.hero a[href*="github"]');
    await expect(heroCTA.first()).toBeVisible();
    expect(await heroCTA.first().getAttribute('href')).toBe(expectedGitHubUrl);

    // Header navigation
    const headerNav = page.locator('header nav a[href*="github"]');
    await expect(headerNav.first()).toBeVisible();
    expect(await headerNav.first().getAttribute('href')).toBe(expectedGitHubUrl);

    // Footer links
    const footerLinks = page.locator('footer a[href*="github"]');
    const footerCount = await footerLinks.count();
    expect(footerCount).toBeGreaterThanOrEqual(1);

    // Verify at least one footer link points to the main repo
    let foundMainRepoLink = false;
    for (let i = 0; i < footerCount; i++) {
      const href = await footerLinks.nth(i).getAttribute('href');
      if (href === expectedGitHubUrl) {
        foundMainRepoLink = true;
        break;
      }
    }
    expect(foundMainRepoLink).toBe(true);
  });

  /**
   * Additional test: CI badge shows current build status
   * Verifies the badge image loads properly
   */
  test('CI badge image loads and displays build status', async ({ page }) => {
    const badgeImage = page.locator('.badge-container img');
    await expect(badgeImage).toBeVisible();

    // Verify the image has a valid src attribute
    const src = await badgeImage.getAttribute('src');
    expect(src).toBeTruthy();
    expect(src.length).toBeGreaterThan(0);

    // Verify image has explicit dimensions for layout stability
    const width = await badgeImage.getAttribute('width');
    const height = await badgeImage.getAttribute('height');
    expect(width).toBeTruthy();
    expect(height).toBeTruthy();
  });
});
