// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Test Suite: GitHub Repository Links
 * Scenario: Verify that links to the GitHub repository are present and functional
 * across header, hero section, and footer
 */

test.describe('GitHub Repository Links', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Check header for GitHub link
   * Input: Check header for GitHub link
   * Expected: Header navigation contains GitHub link
   */
  test('TC1: Header navigation contains GitHub link', async ({ page }) => {
    // Verify header element exists
    const header = page.locator('header.header');
    await expect(header).toBeVisible();

    // Verify navigation element exists
    const nav = page.locator('nav.main-nav');
    await expect(nav).toBeVisible();

    // Find the GitHub link in header navigation
    const githubLink = page.locator('nav.main-nav .nav-link', { hasText: 'GitHub' });
    await expect(githubLink).toBeVisible();

    // Verify GitHub link has correct class for styling
    const linkClasses = await githubLink.getAttribute('class');
    expect(linkClasses).toContain('github-link');

    // Verify the link points to a GitHub URL
    const href = await githubLink.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href).toContain('github.com');
    expect(href).toContain('mirdb');

    // Verify it's specifically in the navigation list
    const navListGithubLink = page.locator('.nav-list a[href*="github.com"]');
    await expect(navListGithubLink).toBeVisible();
  });

  /**
   * Test Case 2: Check hero section for GitHub CTA
   * Input: Check hero section for GitHub CTA
   * Expected: Hero section contains 'View on GitHub' or similar link
   */
  test('TC2: Hero section contains View on GitHub link', async ({ page }) => {
    // Verify hero section exists
    const heroSection = page.locator('#hero.hero-section');
    await expect(heroSection).toBeVisible();

    // Verify hero CTA container exists
    const heroCta = page.locator('#hero .hero-cta');
    await expect(heroCta).toBeVisible();

    // Find the GitHub CTA link in hero section (secondary button)
    const githubLink = page.locator('#hero .btn-secondary');
    await expect(githubLink).toBeVisible();

    // Verify the link text contains GitHub reference
    const linkText = await githubLink.textContent();
    expect(linkText).toBeTruthy();
    const lowerText = linkText.toLowerCase();
    expect(lowerText).toContain('github');

    // Alternative patterns: "View on GitHub", "GitHub", "Source on GitHub"
    const containsViewOnGithub = lowerText.includes('view on github');
    const containsGithub = lowerText.includes('github');
    expect(containsViewOnGithub || containsGithub).toBeTruthy();

    // Verify the link points to GitHub
    const href = await githubLink.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href).toContain('github.com');
    expect(href).toContain('mirdb');
  });

  /**
   * Test Case 3: Check footer for GitHub link
   * Input: Check footer for GitHub link
   * Expected: Footer contains link to GitHub repository
   */
  test('TC3: Footer contains link to GitHub repository', async ({ page }) => {
    // Verify footer element exists
    const footer = page.locator('footer.footer');
    await expect(footer).toBeVisible();

    // Find GitHub link in footer
    const footerGithubLink = page.locator('footer a[href*="github.com/yetone/mirdb"]').first();
    await expect(footerGithubLink).toBeVisible();

    // Verify the link text
    const linkText = await footerGithubLink.textContent();
    expect(linkText).toBeTruthy();

    // The footer should have a GitHub link (either main repo or issues)
    const href = await footerGithubLink.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href).toContain('github.com');
    expect(href).toContain('mirdb');

    // Verify footer nav exists
    const footerNav = page.locator('footer .footer-nav');
    await expect(footerNav).toBeVisible();

    // Count GitHub-related links in footer
    const footerGithubLinks = page.locator('footer a[href*="github.com"]');
    const linkCount = await footerGithubLinks.count();
    expect(linkCount).toBeGreaterThanOrEqual(1);
  });

  /**
   * Test Case 4: Click GitHub link and verify destination
   * Input: Click GitHub link and verify destination
   * Expected: Link navigates to valid GitHub repository page
   */
  test('TC4: GitHub link navigates to valid GitHub repository', async ({ page }) => {
    // Test the header GitHub link
    const headerGithubLink = page.locator('nav.main-nav .github-link');
    await expect(headerGithubLink).toBeVisible();

    // Verify href points to GitHub repository
    const href = await headerGithubLink.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href).toBe('https://github.com/yetone/mirdb');

    // Click the GitHub link and verify it opens a new page
    const [newPage] = await Promise.all([
      page.context().waitForEvent('page'),
      headerGithubLink.click()
    ]);

    // Verify new page was opened
    expect(newPage).toBeTruthy();

    // Verify the new page URL is the GitHub repository
    const newPageUrl = newPage.url();
    expect(newPageUrl).toContain('github.com');
    expect(newPageUrl).toContain('yetone/mirdb');

    // Close the new page
    await newPage.close();
  });

  /**
   * Test Case 5: Verify GitHub links open in new tab
   * Input: Verify GitHub links open in new tab
   * Expected: GitHub links have target='_blank' or equivalent behavior
   */
  test('TC5: GitHub links have target=_blank for new tab behavior', async ({ page }) => {
    // Check header GitHub link
    const headerGithubLink = page.locator('nav.main-nav .github-link');
    await expect(headerGithubLink).toHaveAttribute('target', '_blank');
    await expect(headerGithubLink).toHaveAttribute('rel', 'noopener noreferrer');

    // Check hero section GitHub link
    const heroGithubLink = page.locator('#hero .btn-secondary');
    await expect(heroGithubLink).toHaveAttribute('target', '_blank');
    await expect(heroGithubLink).toHaveAttribute('rel', 'noopener noreferrer');

    // Check footer GitHub links
    const footerGithubLinks = page.locator('footer a[href*="github.com"]');
    const count = await footerGithubLinks.count();

    for (let i = 0; i < count; i++) {
      const link = footerGithubLinks.nth(i);
      await expect(link).toHaveAttribute('target', '_blank');
      await expect(link).toHaveAttribute('rel', 'noopener noreferrer');
    }
  });

  /**
   * Additional test: All GitHub links point to consistent repository URL
   */
  test('All GitHub links point to consistent repository URL', async ({ page }) => {
    const expectedRepoUrl = 'https://github.com/yetone/mirdb';

    // Header GitHub link
    const headerGithubLink = page.locator('nav.main-nav .github-link');
    await expect(headerGithubLink).toHaveAttribute('href', expectedRepoUrl);

    // Hero section GitHub link
    const heroGithubLink = page.locator('#hero .btn-secondary');
    await expect(heroGithubLink).toHaveAttribute('href', expectedRepoUrl);

    // Footer main GitHub link
    const footerGithubLink = page.locator('footer a[href="https://github.com/yetone/mirdb"]');
    await expect(footerGithubLink).toBeVisible();
  });

  /**
   * Additional test: GitHub links have proper security attributes
   */
  test('GitHub links have proper security attributes', async ({ page }) => {
    // All external GitHub links should have rel="noopener noreferrer"
    const allGithubLinks = page.locator('a[href*="github.com"]');
    const count = await allGithubLinks.count();

    expect(count).toBeGreaterThanOrEqual(3); // At least header, hero, and footer

    for (let i = 0; i < count; i++) {
      const link = allGithubLinks.nth(i);
      const rel = await link.getAttribute('rel');
      expect(rel).toBeTruthy();
      expect(rel).toContain('noopener');
      expect(rel).toContain('noreferrer');
    }
  });

  /**
   * Additional test: GitHub links are accessible
   */
  test('GitHub links are keyboard accessible', async ({ page }) => {
    // Verify GitHub links can be focused
    const headerGithubLink = page.locator('nav.main-nav .github-link');

    // Focus the link using keyboard navigation
    await headerGithubLink.focus();
    await expect(headerGithubLink).toBeFocused();

    // Verify focused state is visually distinguishable (has focus outline or similar)
    const outline = await headerGithubLink.evaluate(el => {
      const style = window.getComputedStyle(el);
      return style.outline || style.outlineStyle;
    });
    // Link should have some focus indicator (either default or custom)
    expect(outline).toBeDefined();
  });
});
