// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E tests for GitHub Repository Navigation
 * Scenario: Verify that navigation to GitHub repository is available as specified in REQ-8
 *
 * Tests cover:
 * 1. GitHub link presence in hero section or navigation
 * 2. GitHub link presence in footer section
 * 3. GitHub link navigation to correct repository URL
 */

test.describe('GitHub Repository Navigation', () => {

  test.beforeEach(async ({ page }) => {
    // Navigate to the homepage
    await page.goto('/');
  });

  /**
   * Test Case 1: Check for GitHub link in header/hero
   * Input: Check for GitHub link in header/hero
   * Expected: GitHub repository link is present in hero section or navigation
   */
  test('TC1: GitHub repository link is present in hero section or navigation', async ({ page }) => {
    // Check for GitHub link in navigation bar
    const navGitHubLink = page.locator('nav a[href*="github.com"]');
    const navGitHubLinkVisible = await navGitHubLink.count() > 0;

    // Check for GitHub link in hero section CTAs
    const heroSection = page.locator('[data-testid="hero-section"], .hero, header.hero, #hero');
    const heroCTAGitHubLink = heroSection.locator('a[href*="github.com"]');
    const heroCTAGitHubLinkVisible = await heroCTAGitHubLink.count() > 0;

    // At least one GitHub link should be in the navigation or hero section
    expect(navGitHubLinkVisible || heroCTAGitHubLinkVisible).toBeTruthy();

    // Verify the link contains correct GitHub URL pattern
    let gitHubLink;
    if (navGitHubLinkVisible) {
      gitHubLink = navGitHubLink.first();
    } else {
      gitHubLink = heroCTAGitHubLink.first();
    }

    await expect(gitHubLink).toBeVisible();

    // Verify link text or aria-label contains GitHub reference
    const linkText = await gitHubLink.textContent();
    const linkHref = await gitHubLink.getAttribute('href');

    expect(linkHref).toContain('github.com');
    expect(linkText.toLowerCase()).toMatch(/github|view on github|source/);
  });

  /**
   * Test Case 2: Check for GitHub link in footer
   * Input: Check for GitHub link in footer
   * Expected: GitHub repository link is present in footer section
   */
  test('TC2: GitHub repository link is present in footer section', async ({ page }) => {
    // Locate the footer section
    const footer = page.locator('footer, .footer');
    await expect(footer).toBeVisible();

    // Find GitHub link in footer
    const footerGitHubLink = footer.locator('a[href*="github.com"]');
    await expect(footerGitHubLink.first()).toBeVisible();

    // Verify the link points to GitHub
    const href = await footerGitHubLink.first().getAttribute('href');
    expect(href).toContain('github.com');

    // Verify footer has a link specifically to the repository (not just issues)
    const repoLink = footer.locator('a[href*="github.com"]:not([href*="/issues"]):not([href*="/pull"])');
    const repoLinkCount = await repoLink.count();
    expect(repoLinkCount).toBeGreaterThan(0);
  });

  /**
   * Test Case 3: Click GitHub link
   * Input: Click GitHub link
   * Expected: Link navigates to correct GitHub repository URL
   */
  test('TC3: GitHub link navigates to correct GitHub repository URL', async ({ page, context }) => {
    // Find any GitHub link on the page (prefer hero/nav area)
    const heroNav = page.locator('nav, [data-testid="hero-section"], .hero, header.hero, #hero');
    let gitHubLink = heroNav.locator('a[href*="github.com"]').first();

    // Fall back to any GitHub link if not found in hero/nav
    if (await gitHubLink.count() === 0) {
      gitHubLink = page.locator('a[href*="github.com"]').first();
    }

    await expect(gitHubLink).toBeVisible();

    // Get the href before clicking
    const expectedUrl = await gitHubLink.getAttribute('href');
    expect(expectedUrl).toContain('github.com');

    // Verify the URL contains the expected repository path pattern
    // Should point to a valid GitHub repository URL format: github.com/{owner}/{repo}
    const urlPattern = /github\.com\/[^\/]+\/[^\/]+/;
    expect(expectedUrl).toMatch(urlPattern);

    // Check if link opens in new tab (target="_blank")
    const target = await gitHubLink.getAttribute('target');
    const hasRelNoopener = await gitHubLink.getAttribute('rel');

    if (target === '_blank') {
      // If opens in new tab, verify rel="noopener" for security
      expect(hasRelNoopener).toContain('noopener');

      // Listen for new page to open
      const pagePromise = context.waitForEvent('page');
      await gitHubLink.click();
      const newPage = await pagePromise;

      // Wait for navigation
      await newPage.waitForLoadState('domcontentloaded');

      // Verify the new page URL is the GitHub repository
      const newPageUrl = newPage.url();
      expect(newPageUrl).toContain('github.com');
    } else {
      // If same tab navigation, verify href is correct
      expect(expectedUrl).toMatch(urlPattern);
    }
  });

  /**
   * Additional test: Verify all GitHub links point to the same repository
   */
  test('All GitHub repository links point to consistent repository', async ({ page }) => {
    // Get all GitHub links that point to the repository (not issues, PRs, etc.)
    const allGitHubRepoLinks = page.locator('a[href*="github.com"]:not([href*="/issues"]):not([href*="/pull"]):not([href*="/blob"])');

    const linkCount = await allGitHubRepoLinks.count();
    expect(linkCount).toBeGreaterThan(0);

    // Extract the base repository URL from all links
    const repoUrls = new Set();
    for (let i = 0; i < linkCount; i++) {
      const href = await allGitHubRepoLinks.nth(i).getAttribute('href');
      // Extract base repo URL (github.com/owner/repo)
      const match = href.match(/(github\.com\/[^\/]+\/[^\/\?#]+)/);
      if (match) {
        repoUrls.add(match[1]);
      }
    }

    // All links should point to the same repository
    expect(repoUrls.size).toBeLessThanOrEqual(1);
  });

});
