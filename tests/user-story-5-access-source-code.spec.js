// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * User Story 5 - Access Source Code Tests (REQ-7)
 *
 * As a Curious Carl, I want to easily access the GitHub repository,
 * so that I can explore the source code and contribute.
 *
 * Acceptance Criteria:
 * - Given I want to view the source code
 * - When I click the GitHub link in the navigation or hero section
 * - Then I am directed to the MirDB GitHub repository in a new tab
 */

const GITHUB_REPO_URL = 'https://github.com/yetone/mirdb';

test.describe('User Story 5 - Access Source Code', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Find GitHub link in navigation
   * Input: Find GitHub link in navigation
   * Expected: GitHub link is prominently displayed in navigation bar
   * Type: e2e
   */
  test('TC1: GitHub link is prominently displayed in navigation bar', async ({ page }) => {
    // Step 1: Find GitHub link - Locate link to GitHub repository
    // Context: Should be visible in navigation or hero

    // Query for navigation bar
    const navbar = page.locator('nav');
    await expect(navbar).toBeVisible();

    // Query for GitHub link in navigation bar
    const navGithubLink = page.locator('nav a[data-testid="nav-github-link"]');

    // Verify GitHub link is visible in the navigation
    await expect(navGithubLink).toBeVisible();

    // Verify link text contains 'GitHub'
    await expect(navGithubLink).toHaveText('GitHub');

    // Verify link has valid href attribute pointing to GitHub repository
    const href = await navGithubLink.getAttribute('href');
    expect(href).toBe(GITHUB_REPO_URL);

    // Verify the link is prominently displayed (not hidden or tiny)
    const boundingBox = await navGithubLink.boundingBox();
    expect(boundingBox).not.toBeNull();
    expect(boundingBox.width).toBeGreaterThan(30); // Reasonably sized
    expect(boundingBox.height).toBeGreaterThan(10);
  });

  /**
   * Test Case 2: Click GitHub link
   * Input: Click GitHub link
   * Expected: Link opens MirDB GitHub repository in new tab
   * Type: e2e
   */
  test('TC2: GitHub link opens MirDB repository in new tab', async ({ page, context }) => {
    // Step 2: Click GitHub link - Click the link to access repository
    // Context: Should open in new tab

    // Find the navigation GitHub link
    const navGithubLink = page.locator('nav a[data-testid="nav-github-link"]');
    await expect(navGithubLink).toBeVisible();

    // Listen for new page (new tab)
    const newPagePromise = context.waitForEvent('page');

    // Click the GitHub link
    await navGithubLink.click();

    // Wait for the new page/tab to open
    const newPage = await newPagePromise;

    // Verify the new page URL is the MirDB GitHub repository
    await newPage.waitForLoadState('domcontentloaded');
    const newPageUrl = newPage.url();
    expect(newPageUrl).toContain('github.com/yetone/mirdb');

    // Clean up - close the new tab
    await newPage.close();
  });

  /**
   * Test Case 3: Check GitHub link target attribute
   * Input: Check GitHub link target attribute
   * Expected: Link has target='_blank' attribute
   * Type: unit
   */
  test('TC3: GitHub link has target="_blank" attribute', async ({ page }) => {
    // Check all GitHub links have target="_blank" attribute

    // Navigation GitHub link
    const navGithubLink = page.locator('nav a[data-testid="nav-github-link"]');
    await expect(navGithubLink).toBeVisible();
    const navTarget = await navGithubLink.getAttribute('target');
    expect(navTarget).toBe('_blank');

    // Hero section GitHub link
    const heroGithubLink = page.locator('#hero a[data-testid="hero-github-link"]');
    await expect(heroGithubLink).toBeVisible();
    const heroTarget = await heroGithubLink.getAttribute('target');
    expect(heroTarget).toBe('_blank');

    // Footer GitHub link
    const footerGithubLink = page.locator('footer a[data-testid="footer-github-link"]');
    await expect(footerGithubLink).toBeVisible();
    const footerTarget = await footerGithubLink.getAttribute('target');
    expect(footerTarget).toBe('_blank');

    // Verify rel attribute for security (noopener noreferrer)
    const navRel = await navGithubLink.getAttribute('rel');
    expect(navRel).toContain('noopener');
    expect(navRel).toContain('noreferrer');

    const heroRel = await heroGithubLink.getAttribute('rel');
    expect(heroRel).toContain('noopener');
    expect(heroRel).toContain('noreferrer');

    const footerRel = await footerGithubLink.getAttribute('rel');
    expect(footerRel).toContain('noopener');
    expect(footerRel).toContain('noreferrer');
  });

  /**
   * Additional Test: Hero section also has GitHub access
   * Ensures users can access GitHub from multiple locations
   */
  test('TC-Additional: Hero section View on GitHub button works', async ({ page, context }) => {
    // Find the hero GitHub link
    const heroGithubLink = page.locator('#hero a[data-testid="hero-github-link"]');
    await expect(heroGithubLink).toBeVisible();

    // Verify button text
    await expect(heroGithubLink).toHaveText('View on GitHub');

    // Verify href
    const href = await heroGithubLink.getAttribute('href');
    expect(href).toBe(GITHUB_REPO_URL);

    // Verify it opens in new tab
    const newPagePromise = context.waitForEvent('page');
    await heroGithubLink.click();
    const newPage = await newPagePromise;
    await newPage.waitForLoadState('domcontentloaded');
    expect(newPage.url()).toContain('github.com/yetone/mirdb');
    await newPage.close();
  });
});
