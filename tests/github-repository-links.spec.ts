import { test, expect } from '@playwright/test';

test.describe('GitHub Repository Links', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('page contains at least one link to GitHub repository', async ({ page }) => {
    // Test Case 1: Find GitHub link elements on page
    // Expected: Page contains at least one link to GitHub repository

    const githubLinks = page.locator('a[href*="github.com"]');
    const count = await githubLinks.count();

    expect(count).toBeGreaterThanOrEqual(1);

    // Verify at least one link points to the MirDB repository
    const firstLink = githubLinks.first();
    await expect(firstLink).toBeVisible();
    const href = await firstLink.getAttribute('href');
    expect(href).toContain('github.com');
  });

  test('footer contains link to MirDB GitHub repository', async ({ page }) => {
    // Test Case 2: Verify GitHub link in footer
    // Expected: Footer contains link to MirDB GitHub repository

    const footer = page.locator('footer[data-testid="footer"]');
    await expect(footer).toBeVisible();

    const footerGithubLink = footer.locator('a[href*="github.com/yetone/mirdb"]');
    await expect(footerGithubLink).toBeVisible();

    const href = await footerGithubLink.getAttribute('href');
    expect(href).toBe('https://github.com/yetone/mirdb');
  });

  test('GitHub links have valid href pointing to repository URL', async ({ page }) => {
    // Test Case 3: Verify GitHub link href attribute
    // Expected: GitHub links have valid href pointing to repository URL

    const githubLinks = page.locator('a[href*="github.com/yetone/mirdb"]');
    const count = await githubLinks.count();

    expect(count).toBeGreaterThanOrEqual(1);

    // Check all GitHub links have the correct URL format
    for (let i = 0; i < count; i++) {
      const link = githubLinks.nth(i);
      const href = await link.getAttribute('href');

      // Verify href is a valid GitHub URL
      expect(href).toBe('https://github.com/yetone/mirdb');

      // Verify the link opens in a new tab (target="_blank")
      const target = await link.getAttribute('target');
      expect(target).toBe('_blank');

      // Verify security attributes for external links
      const rel = await link.getAttribute('rel');
      expect(rel).toContain('noopener');
    }
  });

  test('hero section contains GitHub link with proper attributes', async ({ page }) => {
    // Additional test for hero section GitHub link
    const heroGithubLink = page.locator('[data-testid="hero-cta-github"]');
    await expect(heroGithubLink).toBeVisible();

    const href = await heroGithubLink.getAttribute('href');
    expect(href).toBe('https://github.com/yetone/mirdb');

    const target = await heroGithubLink.getAttribute('target');
    expect(target).toBe('_blank');

    await expect(heroGithubLink).toContainText(/GitHub/i);
  });
});
