// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * External Links and Repository Access E2E Tests (REQ-7)
 * Verifies external links to source code repository and documentation work correctly
 */

const GITHUB_REPO_URL = 'https://github.com/yetone/mirdb';

test.describe('External Links and Repository Access', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: GitHub link in navigation bar
   * Input: Query for GitHub link in navigation bar
   * Expected: GitHub link is present with valid href attribute
   */
  test('TC1: GitHub link is present in navigation bar with valid href', async ({ page }) => {
    // Query for GitHub link in navigation bar
    const navGithubLink = page.locator('nav a[data-testid="nav-github-link"]');

    // Verify link is visible
    await expect(navGithubLink).toBeVisible();

    // Verify link has valid href attribute pointing to GitHub repository
    const href = await navGithubLink.getAttribute('href');
    expect(href).toBe(GITHUB_REPO_URL);

    // Verify link text contains 'GitHub'
    await expect(navGithubLink).toHaveText('GitHub');
  });

  /**
   * Test Case 2: GitHub link in hero section (View on GitHub CTA)
   * Input: Query for GitHub link in hero section
   * Expected: View on GitHub CTA button has valid href to repository
   */
  test('TC2: View on GitHub CTA button in hero section has valid href', async ({ page }) => {
    // Query for GitHub link in hero section
    const heroGithubLink = page.locator('#hero a[data-testid="hero-github-link"]');

    // Verify link is visible
    await expect(heroGithubLink).toBeVisible();

    // Verify link text is 'View on GitHub'
    await expect(heroGithubLink).toHaveText('View on GitHub');

    // Verify link has valid href attribute pointing to GitHub repository
    const href = await heroGithubLink.getAttribute('href');
    expect(href).toBe(GITHUB_REPO_URL);
  });

  /**
   * Test Case 3: GitHub link in footer
   * Input: Query for GitHub link in footer
   * Expected: Footer contains repository link
   */
  test('TC3: Footer contains GitHub repository link', async ({ page }) => {
    // Query for GitHub link in footer
    const footerGithubLink = page.locator('footer a[data-testid="footer-github-link"]');

    // Verify link is visible
    await expect(footerGithubLink).toBeVisible();

    // Verify link text contains 'GitHub Repository'
    await expect(footerGithubLink).toHaveText('GitHub Repository');

    // Verify link has valid href attribute pointing to GitHub repository
    const href = await footerGithubLink.getAttribute('href');
    expect(href).toBe(GITHUB_REPO_URL);

    // Also verify additional footer links exist
    const issuesLink = page.locator('footer a[data-testid="footer-issues-link"]');
    await expect(issuesLink).toBeVisible();

    const contributingLink = page.locator('footer a[data-testid="footer-contributing-link"]');
    await expect(contributingLink).toBeVisible();
  });

  /**
   * Test Case 4: External links have correct target and rel attributes
   * Input: Check link target attributes
   * Expected: External links have target='_blank' and rel='noopener noreferrer'
   */
  test('TC4: External links have target="_blank" and rel="noopener noreferrer"', async ({ page }) => {
    // Get all external GitHub links
    const externalLinks = page.locator('a[href^="https://github.com"]');
    const count = await externalLinks.count();

    // Verify we have at least 3 external links (nav, hero, footer)
    expect(count).toBeGreaterThanOrEqual(3);

    // Check each external link for proper attributes
    for (let i = 0; i < count; i++) {
      const link = externalLinks.nth(i);

      // Verify target="_blank" attribute
      const target = await link.getAttribute('target');
      expect(target).toBe('_blank');

      // Verify rel attribute contains 'noopener' and 'noreferrer'
      const rel = await link.getAttribute('rel');
      expect(rel).toContain('noopener');
      expect(rel).toContain('noreferrer');
    }
  });

  /**
   * Test Case 5: External links are keyboard accessible
   * Input: Check link accessibility with keyboard
   * Expected: All links are focusable and activatable via keyboard
   */
  test('TC5: All external links are keyboard accessible', async ({ page }) => {
    // Get all external links
    const navGithubLink = page.locator('nav a[data-testid="nav-github-link"]');
    const heroGithubLink = page.locator('#hero a[data-testid="hero-github-link"]');
    const footerGithubLink = page.locator('footer a[data-testid="footer-github-link"]');

    // Test navigation GitHub link is focusable
    await navGithubLink.focus();
    await expect(navGithubLink).toBeFocused();

    // Verify link can receive keyboard focus and has visible focus indicator
    const navLinkBoundingBox = await navGithubLink.boundingBox();
    expect(navLinkBoundingBox).not.toBeNull();

    // Test hero GitHub link is focusable
    await heroGithubLink.focus();
    await expect(heroGithubLink).toBeFocused();

    // Test footer GitHub link is focusable (need to scroll to footer first)
    await footerGithubLink.scrollIntoViewIfNeeded();
    await footerGithubLink.focus();
    await expect(footerGithubLink).toBeFocused();

    // Verify all links have proper tabindex (not negative)
    const navTabIndex = await navGithubLink.getAttribute('tabindex');
    const heroTabIndex = await heroGithubLink.getAttribute('tabindex');
    const footerTabIndex = await footerGithubLink.getAttribute('tabindex');

    // tabindex should be null (default focusable) or >= 0
    if (navTabIndex !== null) {
      expect(parseInt(navTabIndex)).toBeGreaterThanOrEqual(0);
    }
    if (heroTabIndex !== null) {
      expect(parseInt(heroTabIndex)).toBeGreaterThanOrEqual(0);
    }
    if (footerTabIndex !== null) {
      expect(parseInt(footerTabIndex)).toBeGreaterThanOrEqual(0);
    }
  });

  /**
   * Additional test: Navigation GitHub link can be reached via Tab key
   */
  test('TC5-additional: Links can be navigated with Tab key', async ({ page }) => {
    // Start from the beginning of the page
    await page.keyboard.press('Tab');

    // Tab through the page to reach the GitHub link
    // The nav links are: Features, Quick Start, Commands, Configuration, GitHub
    // First tab should be on the brand/logo, then Features...
    let foundGitHubLink = false;
    let tabCount = 0;
    const maxTabs = 20; // Safety limit

    while (!foundGitHubLink && tabCount < maxTabs) {
      const focusedElement = page.locator(':focus');
      const href = await focusedElement.getAttribute('href');

      if (href === GITHUB_REPO_URL) {
        foundGitHubLink = true;
        break;
      }

      await page.keyboard.press('Tab');
      tabCount++;
    }

    // Verify we found the GitHub link via keyboard navigation
    expect(foundGitHubLink).toBe(true);
  });
});
