import { test, expect } from '@playwright/test';

const GITHUB_REPO_URL = 'https://github.com/mirdb/mirdb';

test.describe('GitHub Repository Links', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('navigation contains link to GitHub repository', async ({ page }) => {
    // Test Case 1: Check sticky navigation for GitHub repository link
    const navGitHubLink = page.locator('.sticky-nav a[href*="github.com"], nav a[href*="github.com"]');
    await expect(navGitHubLink).toBeVisible();
    await expect(navGitHubLink).toHaveAttribute('href', GITHUB_REPO_URL);
  });

  test('footer contains link to GitHub repository', async ({ page }) => {
    // Test Case 2: Check footer for GitHub repository link (use .github-link class for the main repo link)
    const footerGitHubLink = page.locator('footer a.github-link[href*="github.com"]');
    await expect(footerGitHubLink).toBeVisible();
    await expect(footerGitHubLink).toHaveAttribute('href', GITHUB_REPO_URL);
  });

  test('GitHub link opens in new tab (target="_blank")', async ({ page }) => {
    // Test Case 3: Verify GitHub link has target='_blank'
    const navGitHubLink = page.locator('.sticky-nav a[href*="github.com"], nav a[href*="github.com"]');
    await expect(navGitHubLink).toHaveAttribute('target', '_blank');

    // Use .github-link class for the main repo link
    const footerGitHubLink = page.locator('footer a.github-link[href*="github.com"]');
    await expect(footerGitHubLink).toHaveAttribute('target', '_blank');
  });
});
