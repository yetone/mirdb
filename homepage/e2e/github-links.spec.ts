import { test, expect } from '@playwright/test';

const GITHUB_REPO_URL = 'https://github.com/mirdb/mirdb';

test.describe('GitHub Repository Links', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('header contains link to GitHub repository', async ({ page }) => {
    // Test Case 1: Check header for GitHub repository link
    const headerGitHubLink = page.locator('header a[href*="github.com"]');
    await expect(headerGitHubLink).toBeVisible();
    await expect(headerGitHubLink).toHaveAttribute('href', GITHUB_REPO_URL);
  });

  test('footer contains link to GitHub repository', async ({ page }) => {
    // Test Case 2: Check footer for GitHub repository link
    const footerGitHubLink = page.locator('footer a[href*="github.com"]');
    await expect(footerGitHubLink).toBeVisible();
    await expect(footerGitHubLink).toHaveAttribute('href', GITHUB_REPO_URL);
  });

  test('GitHub link opens in new tab (target="_blank")', async ({ page }) => {
    // Test Case 3: Verify GitHub link has target='_blank'
    const headerGitHubLink = page.locator('header a[href*="github.com"]');
    await expect(headerGitHubLink).toHaveAttribute('target', '_blank');

    const footerGitHubLink = page.locator('footer a[href*="github.com"]');
    await expect(footerGitHubLink).toHaveAttribute('target', '_blank');
  });
});
