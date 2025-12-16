// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for US-4: Access Project Resources
 *
 * User Story: As a Contributor, I want to easily find the GitHub repository and documentation,
 * so that I can explore the codebase and contribute.
 *
 * Acceptance Criteria:
 * - Given I want to contribute to MirDB
 * - When I look for project resources
 * - Then I find clearly visible links to GitHub repository
 * - And I can access technical documentation
 *
 * Related Requirements: REQ-6, REQ-7
 */

test.describe('US-4: Access Project Resources', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to homepage before each test
    await page.goto('/');
  });

  /**
   * Test Case 1: GitHub repository link is clearly visible (header, hero, or footer)
   * Input: Check for visible GitHub link
   * Expected: GitHub repository link is clearly visible (header, hero, or footer)
   */
  test('TC1: GitHub repository link is clearly visible on homepage', async ({ page }) => {
    // Check for GitHub link in hero section (most prominent placement)
    const heroGithubBtn = page.locator('[data-testid="github-btn"]');
    await expect(heroGithubBtn).toBeVisible();
    await expect(heroGithubBtn).toContainText('GitHub');

    // Verify the hero GitHub link is in a prominent position
    const heroBtnBox = await heroGithubBtn.boundingBox();
    expect(heroBtnBox).toBeTruthy();
    // The button should be within the top viewport area (within 800px from top)
    expect(heroBtnBox.y).toBeLessThan(800);

    // Also verify GitHub link exists in footer for additional visibility
    const footerGithubLink = page.locator('[data-testid="footer-github-link"]');
    await expect(footerGithubLink).toBeVisible();
    await expect(footerGithubLink).toContainText('GitHub');

    // Verify project status badge also links to GitHub
    const statusBadge = page.locator('[data-testid="project-status-badge"]');
    await expect(statusBadge).toBeVisible();
    const statusBadgeHref = await statusBadge.getAttribute('href');
    expect(statusBadgeHref).toContain('github.com/yetone/mirdb');
  });

  /**
   * Test Case 2: Click GitHub link and verify navigation to repository
   * Input: Click GitHub link
   * Expected: Link navigates to MirDB GitHub repository
   */
  test('TC2: GitHub link navigates to MirDB GitHub repository', async ({ page, context }) => {
    // Test the hero GitHub button
    const heroGithubBtn = page.locator('[data-testid="github-btn"]');
    await expect(heroGithubBtn).toBeVisible();

    // Verify the href attribute points to correct GitHub repository
    const href = await heroGithubBtn.getAttribute('href');
    expect(href).toBe('https://github.com/yetone/mirdb');

    // Verify target="_blank" for external link behavior
    const target = await heroGithubBtn.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify security attributes for external link
    const rel = await heroGithubBtn.getAttribute('rel');
    expect(rel).toContain('noopener');
    expect(rel).toContain('noreferrer');

    // Test that clicking opens a new tab with the correct URL
    const [newPage] = await Promise.all([
      context.waitForEvent('page'),
      heroGithubBtn.click()
    ]);

    // Verify the new page URL points to MirDB GitHub repository
    expect(newPage.url()).toBe('https://github.com/yetone/mirdb');

    // Close the new page
    await newPage.close();

    // Also verify footer GitHub link navigates correctly
    await page.locator('.footer').scrollIntoViewIfNeeded();
    const footerGithubLink = page.locator('[data-testid="footer-github-link"]');

    const footerHref = await footerGithubLink.getAttribute('href');
    expect(footerHref).toBe('https://github.com/yetone/mirdb');

    const footerTarget = await footerGithubLink.getAttribute('target');
    expect(footerTarget).toBe('_blank');
  });

  /**
   * Test Case 3: Technical documentation is accessible via link on homepage
   * Input: Check for documentation access
   * Expected: Technical documentation is accessible via link on homepage
   */
  test('TC3: Technical documentation is accessible via link on homepage', async ({ page, context }) => {
    // Scroll to getting-started section where documentation links are located
    await page.locator('#getting-started').scrollIntoViewIfNeeded();

    // Verify primary documentation link exists and is visible
    const docsLink = page.locator('[data-testid="docs-link"]');
    await expect(docsLink).toBeVisible();
    await expect(docsLink).toContainText('Documentation');

    // Verify the documentation link href points to documentation
    const docsHref = await docsLink.getAttribute('href');
    expect(docsHref).toBe('https://github.com/yetone/mirdb#readme');

    // Verify docs link opens in new tab with security attributes
    const docsTarget = await docsLink.getAttribute('target');
    expect(docsTarget).toBe('_blank');

    const docsRel = await docsLink.getAttribute('rel');
    expect(docsRel).toContain('noopener');
    expect(docsRel).toContain('noreferrer');

    // Test that clicking documentation link opens correct URL
    const [docsPage] = await Promise.all([
      context.waitForEvent('page'),
      docsLink.click()
    ]);

    // Verify the new page URL contains the repository documentation
    expect(docsPage.url()).toContain('github.com/yetone/mirdb');

    await docsPage.close();

    // Also verify Wiki link for additional documentation access
    const wikiLink = page.locator('[data-testid="wiki-link"]');
    await expect(wikiLink).toBeVisible();
    await expect(wikiLink).toContainText('Wiki');

    const wikiHref = await wikiLink.getAttribute('href');
    expect(wikiHref).toBe('https://github.com/yetone/mirdb/wiki');

    // Verify footer documentation links
    await page.locator('.footer').scrollIntoViewIfNeeded();

    // Internal documentation link (scrolls to getting-started)
    const footerInternalDocsLink = page.locator('[data-testid="footer-docs-internal-link"]');
    await expect(footerInternalDocsLink).toBeVisible();
    await expect(footerInternalDocsLink).toContainText('Documentation');

    const internalHref = await footerInternalDocsLink.getAttribute('href');
    expect(internalHref).toBe('#getting-started');

    // External full documentation link
    const footerExternalDocsLink = page.locator('[data-testid="footer-docs-link"]');
    await expect(footerExternalDocsLink).toBeVisible();

    const externalHref = await footerExternalDocsLink.getAttribute('href');
    expect(externalHref).toContain('github.com/yetone/mirdb');
    expect(externalHref.toLowerCase()).toContain('readme');
  });

  /**
   * Additional Test: All project resource links are functional and properly secured
   */
  test('TC-Additional: All project resource links have proper security attributes', async ({ page }) => {
    // Check all external links have proper security attributes
    const externalLinks = [
      { testId: 'github-btn', expectedHref: 'https://github.com/yetone/mirdb' },
      { testId: 'docs-link', expectedHref: 'https://github.com/yetone/mirdb#readme' },
      { testId: 'wiki-link', expectedHref: 'https://github.com/yetone/mirdb/wiki' },
      { testId: 'footer-github-link', expectedHref: 'https://github.com/yetone/mirdb' },
      { testId: 'footer-docs-link', expectedHref: 'https://github.com/yetone/mirdb#readme' },
    ];

    for (const { testId, expectedHref } of externalLinks) {
      const link = page.locator(`[data-testid="${testId}"]`);

      // Scroll to element if needed
      await link.scrollIntoViewIfNeeded();

      // Verify href
      const href = await link.getAttribute('href');
      expect(href).toBe(expectedHref);

      // Verify target="_blank" for new tab
      const target = await link.getAttribute('target');
      expect(target).toBe('_blank');

      // Verify security attributes
      const rel = await link.getAttribute('rel');
      expect(rel).toContain('noopener');
      expect(rel).toContain('noreferrer');
    }
  });

  /**
   * Additional Test: Project resources are easily discoverable without scrolling (hero section)
   */
  test('TC-Additional: GitHub link is accessible without scrolling from hero section', async ({ page }) => {
    // Verify user lands on page with GitHub link visible without scrolling
    const githubBtn = page.locator('[data-testid="github-btn"]');

    // Wait for hero section to be fully visible
    await expect(githubBtn).toBeVisible();

    // Verify the button is in the viewport without scrolling
    await expect(githubBtn).toBeInViewport();

    // Verify button is clearly labeled
    const buttonText = await githubBtn.textContent();
    expect(buttonText?.toLowerCase()).toContain('github');
  });
});
