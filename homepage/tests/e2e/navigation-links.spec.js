// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for Navigation and External Links
 * Scenario: Verify that navigation to documentation, GitHub repository, and community resources works correctly as per REQ-6
 */

test.describe('Navigation and External Links', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to homepage before each test
    await page.goto('/');
  });

  /**
   * Test Case 1: GitHub repository link navigation
   * Input: Click GitHub repository link
   * Expected: Link navigates to MirDB GitHub repository in new tab
   */
  test('TC1: GitHub repository link navigates to MirDB GitHub repository in new tab', async ({ page, context }) => {
    // Verify GitHub button exists in hero section
    const githubBtn = page.locator('[data-testid="github-btn"]');
    await expect(githubBtn).toBeVisible();
    await expect(githubBtn).toContainText('View on GitHub');

    // Verify the link href points to the GitHub repository
    const href = await githubBtn.getAttribute('href');
    expect(href).toBe('https://github.com/yetone/mirdb');

    // Verify it opens in new tab (target="_blank")
    const target = await githubBtn.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify security attribute for external link
    const rel = await githubBtn.getAttribute('rel');
    expect(rel).toContain('noopener');
    expect(rel).toContain('noreferrer');

    // Test that clicking opens a new tab with the correct URL
    const [newPage] = await Promise.all([
      context.waitForEvent('page'),
      githubBtn.click()
    ]);

    // Verify the new page URL
    expect(newPage.url()).toBe('https://github.com/yetone/mirdb');

    // Close the new page
    await newPage.close();
  });

  /**
   * Test Case 2: Documentation link navigation
   * Input: Click documentation link
   * Expected: Link navigates to MirDB documentation
   */
  test('TC2: Documentation link navigates to MirDB documentation', async ({ page, context }) => {
    // Scroll to getting started section where docs links are located
    await page.locator('#getting-started').scrollIntoViewIfNeeded();

    // Verify primary documentation link exists
    const docsLink = page.locator('[data-testid="docs-link"]');
    await expect(docsLink).toBeVisible();
    await expect(docsLink).toContainText('Documentation');

    // Verify the link href points to documentation
    const href = await docsLink.getAttribute('href');
    expect(href).toBe('https://github.com/yetone/mirdb#readme');

    // Verify it opens in new tab
    const target = await docsLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify security attributes
    const rel = await docsLink.getAttribute('rel');
    expect(rel).toContain('noopener');
    expect(rel).toContain('noreferrer');

    // Test that clicking opens a new tab with the correct URL
    const [newPage] = await Promise.all([
      context.waitForEvent('page'),
      docsLink.click()
    ]);

    // Verify the new page URL contains the expected path
    expect(newPage.url()).toContain('github.com/yetone/mirdb');

    // Close the new page
    await newPage.close();
  });

  /**
   * Test Case 2b: Wiki link navigation
   * Note: GitHub may redirect to main repo if wiki doesn't exist, so we verify
   * the link attributes are correct and it opens in a new tab pointing to the repo
   */
  test('TC2b: Wiki link navigates to MirDB wiki', async ({ page, context }) => {
    // Scroll to getting started section
    await page.locator('#getting-started').scrollIntoViewIfNeeded();

    // Verify wiki link exists
    const wikiLink = page.locator('[data-testid="wiki-link"]');
    await expect(wikiLink).toBeVisible();
    await expect(wikiLink).toContainText('Wiki');

    // Verify the link href points to wiki
    const href = await wikiLink.getAttribute('href');
    expect(href).toBe('https://github.com/yetone/mirdb/wiki');

    // Verify it opens in new tab
    const target = await wikiLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify security attributes
    const rel = await wikiLink.getAttribute('rel');
    expect(rel).toContain('noopener');
    expect(rel).toContain('noreferrer');

    // Test that clicking opens a new tab (GitHub may redirect if wiki doesn't exist)
    const [newPage] = await Promise.all([
      context.waitForEvent('page'),
      wikiLink.click()
    ]);

    // Verify the new page URL is GitHub (may redirect from wiki to main repo if wiki doesn't exist)
    expect(newPage.url()).toContain('github.com/yetone/mirdb');

    // Close the new page
    await newPage.close();
  });

  /**
   * Test Case 3: Smooth scroll navigation
   * Input: Test smooth scroll navigation
   * Expected: Clicking navigation items smoothly scrolls to corresponding section
   */
  test('TC3: Clicking navigation items smoothly scrolls to corresponding section', async ({ page }) => {
    // Ensure we start at the top of the page
    await page.evaluate(() => window.scrollTo(0, 0));
    await page.waitForTimeout(100);

    // Verify Get Started button navigates to getting-started section
    const getStartedBtn = page.locator('[data-testid="get-started-btn"]');
    await expect(getStartedBtn).toBeVisible();

    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);
    expect(initialScrollY).toBeLessThan(100); // Should be near top

    // Click Get Started button
    await getStartedBtn.click();

    // Wait for smooth scroll animation to complete
    await page.waitForTimeout(600);

    // Verify we've scrolled down
    const afterScrollY = await page.evaluate(() => window.scrollY);
    expect(afterScrollY).toBeGreaterThan(initialScrollY);

    // Verify the getting-started section is now in viewport
    const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');
    await expect(gettingStartedSection).toBeInViewport();
  });

  /**
   * Test Case 3b: Internal footer navigation link
   */
  test('TC3b: Footer Documentation link scrolls to getting-started section', async ({ page }) => {
    // Scroll to footer
    await page.locator('.footer').scrollIntoViewIfNeeded();

    // Find the internal documentation link in footer
    const footerDocsLink = page.locator('.footer-nav a[href="#getting-started"]');
    await expect(footerDocsLink).toBeVisible();
    await expect(footerDocsLink).toContainText('Documentation');

    // Click and verify smooth scroll
    await footerDocsLink.click();

    // Wait for smooth scroll
    await page.waitForTimeout(600);

    // Verify the getting-started section is now in viewport
    const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');
    await expect(gettingStartedSection).toBeInViewport();
  });

  /**
   * Test Case: Footer GitHub link
   */
  test('TC-Footer: Footer GitHub link has correct URL and security attributes', async ({ page, context }) => {
    // Scroll to footer
    await page.locator('.footer').scrollIntoViewIfNeeded();

    // Find the GitHub link in footer
    const footerGithubLink = page.locator('.footer-nav a[href="https://github.com/yetone/mirdb"]');
    await expect(footerGithubLink).toBeVisible();
    await expect(footerGithubLink).toContainText('GitHub');

    // Verify it opens in new tab with security attributes
    const target = await footerGithubLink.getAttribute('target');
    expect(target).toBe('_blank');

    const rel = await footerGithubLink.getAttribute('rel');
    expect(rel).toContain('noopener');
  });
});
