/**
 * External Links E2E Tests
 * Owner: Scenario 10 - External Link Verification
 *
 * E2E tests for verifying external links open correctly in new tabs
 * and navigate to the expected destinations.
 */

const { test, expect } = require('@playwright/test');

test.describe('External Link Verification', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('GitHub link in hero opens in new tab with correct URL', async ({ page, context }) => {
    // Test case 5: Click GitHub link and verify navigation
    // New tab opens to GitHub repository page

    // Get the "View on GitHub" button in hero section
    const githubLink = page.locator('.cta-buttons a[href="https://github.com/yetone/mirdb"]');

    // Verify the link exists
    await expect(githubLink).toBeVisible();

    // Verify href attribute
    await expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');

    // Verify target="_blank" for new tab
    await expect(githubLink).toHaveAttribute('target', '_blank');

    // Verify security attributes
    const rel = await githubLink.getAttribute('rel');
    expect(rel).toContain('noopener');
    expect(rel).toContain('noreferrer');

    // Test that clicking opens a new tab (using popup event)
    const [newPage] = await Promise.all([
      context.waitForEvent('page'),
      githubLink.click()
    ]);

    // Verify the new page URL
    await newPage.waitForLoadState();
    expect(newPage.url()).toContain('github.com/yetone/mirdb');

    // Close the new page
    await newPage.close();
  });

  test('GitHub link in footer opens in new tab', async ({ page, context }) => {
    // Get the GitHub link in footer section
    const footerGithubLink = page.locator('footer a[href="https://github.com/yetone/mirdb"]');

    // Verify the link exists and is visible
    await expect(footerGithubLink).toBeVisible();

    // Verify attributes
    await expect(footerGithubLink).toHaveAttribute('target', '_blank');
    const rel = await footerGithubLink.getAttribute('rel');
    expect(rel).toContain('noopener');
    expect(rel).toContain('noreferrer');

    // Test that clicking opens a new tab
    const [newPage] = await Promise.all([
      context.waitForEvent('page'),
      footerGithubLink.click()
    ]);

    // Verify the new page URL
    await newPage.waitForLoadState();
    expect(newPage.url()).toContain('github.com/yetone/mirdb');

    await newPage.close();
  });

  test('CircleCI badge link opens in new tab', async ({ page, context }) => {
    // Get the CircleCI badge link
    const circleCILink = page.locator('footer a[href="https://circleci.com/gh/yetone/mirdb"]');

    // Verify the link exists and is visible
    await expect(circleCILink).toBeVisible();

    // Verify the badge image is inside the link
    const badgeImg = circleCILink.locator('img');
    await expect(badgeImg).toBeVisible();

    // Verify attributes
    await expect(circleCILink).toHaveAttribute('target', '_blank');
    const rel = await circleCILink.getAttribute('rel');
    expect(rel).toContain('noopener');
    expect(rel).toContain('noreferrer');

    // Test that clicking opens a new tab
    const [newPage] = await Promise.all([
      context.waitForEvent('page'),
      circleCILink.click()
    ]);

    // Verify the new page URL contains circleci
    await newPage.waitForLoadState();
    expect(newPage.url()).toContain('circleci.com');

    await newPage.close();
  });

  test('Author link opens in new tab', async ({ page, context }) => {
    // Get the author link
    const authorLink = page.locator('.author a[href="https://github.com/yetone"]');

    // Verify the link exists and is visible
    await expect(authorLink).toBeVisible();

    // Verify attributes
    await expect(authorLink).toHaveAttribute('target', '_blank');
    const rel = await authorLink.getAttribute('rel');
    expect(rel).toContain('noopener');
    expect(rel).toContain('noreferrer');

    // Test that clicking opens a new tab
    const [newPage] = await Promise.all([
      context.waitForEvent('page'),
      authorLink.click()
    ]);

    // Verify the new page URL
    await newPage.waitForLoadState();
    expect(newPage.url()).toContain('github.com/yetone');

    await newPage.close();
  });

  test('all external links have security attributes', async ({ page }) => {
    // Get all external links (starting with http)
    const externalLinks = page.locator('a[href^="http"]');
    const count = await externalLinks.count();

    // Verify we have external links
    expect(count).toBeGreaterThan(0);

    // Check each link has proper attributes
    for (let i = 0; i < count; i++) {
      const link = externalLinks.nth(i);
      await expect(link).toHaveAttribute('target', '_blank');

      const rel = await link.getAttribute('rel');
      expect(rel).toContain('noopener');
      expect(rel).toContain('noreferrer');
    }
  });
});
