// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Test Suite: Navigation Links
 *
 * Verifies navigation links to documentation, GitHub repository, and community resources (REQ-7)
 */

test.describe('Navigation Links', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Check GitHub repository link
   * Input: Check GitHub repository link
   * Expected: Link to MirDB GitHub repository is present and valid
   */
  test('should have a valid GitHub repository link', async ({ page }) => {
    // Check for GitHub link in navigation (specifically the one labeled "GitHub")
    const navGitHubLink = page.locator('.nav-links a[href="https://github.com/yetone/mirdb"]');
    await expect(navGitHubLink).toBeVisible();

    // Verify the href is the correct GitHub URL
    const href = await navGitHubLink.getAttribute('href');
    expect(href).toBe('https://github.com/yetone/mirdb');

    // Verify link text contains "GitHub"
    const linkText = await navGitHubLink.textContent();
    expect(linkText?.toLowerCase()).toContain('github');
  });

  /**
   * Test Case 2: Check documentation link
   * Input: Check documentation link
   * Expected: Link to documentation is present (may be to GitHub README or docs site)
   */
  test('should have documentation links accessible', async ({ page }) => {
    // Documentation link may point to GitHub README or a dedicated docs site
    // Check for documentation in navigation or body
    const docsLinks = page.locator('a[href*="github.com/yetone/mirdb"], a[href*="docs"], a[href*="documentation"]');

    // At minimum, the GitHub link serves as documentation
    const count = await docsLinks.count();
    expect(count).toBeGreaterThan(0);

    // Verify the main GitHub link is present (README serves as documentation)
    const gitHubLink = page.locator('a[href="https://github.com/yetone/mirdb"]').first();
    await expect(gitHubLink).toBeVisible();

    // The link should be clickable
    const href = await gitHubLink.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href).toContain('github.com/yetone/mirdb');
  });

  /**
   * Test Case 3: Check footer links
   * Input: Check footer links
   * Expected: Footer contains links to GitHub, license information, and other relevant resources
   */
  test('should have footer with GitHub and other relevant links', async ({ page }) => {
    // Scroll to footer to ensure it's in view
    const footer = page.locator('footer');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Check for GitHub link in footer
    const footerGitHubLink = page.locator('footer a[href*="github.com/yetone/mirdb"]').first();
    await expect(footerGitHubLink).toBeVisible();

    // Check for license information (MIT License mentioned in footer)
    const footerContent = await footer.textContent();
    expect(footerContent?.toLowerCase()).toMatch(/license|mit/i);

    // Check for "Report Issue" or similar community link
    const issueLink = page.locator('footer a[href*="issues"]');
    await expect(issueLink).toBeVisible();
    const issueHref = await issueLink.getAttribute('href');
    expect(issueHref).toContain('github.com/yetone/mirdb/issues');
  });

  /**
   * Test Case 4: Verify external links open in new tab
   * Input: Verify external links open in new tab
   * Expected: External links have target='_blank' and rel='noopener noreferrer' attributes
   */
  test('should have external links with proper security attributes', async ({ page }) => {
    // Get all external links (GitHub links)
    const externalLinks = page.locator('a[href^="https://github.com"]');
    const count = await externalLinks.count();

    // Ensure there are external links to test
    expect(count).toBeGreaterThan(0);

    // Check each external link for proper attributes
    for (let i = 0; i < count; i++) {
      const link = externalLinks.nth(i);
      const href = await link.getAttribute('href');
      const target = await link.getAttribute('target');
      const rel = await link.getAttribute('rel');

      // All external links should open in new tab
      expect(target).toBe('_blank');

      // All external links should have noopener noreferrer for security
      expect(rel).toContain('noopener');
      expect(rel).toContain('noreferrer');
    }
  });
});
