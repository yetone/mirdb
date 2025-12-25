import { test, expect } from '@playwright/test';

test.describe('Repository Link Accessibility', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Repository link is visible in the hero section', async ({ page }) => {
    // Check for GitHub/repository link in hero section
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    // Find the GitHub link within hero section
    const githubLink = page.locator('[data-testid="github-link"]');
    await expect(githubLink).toBeVisible();

    // Verify the link text contains "GitHub" or "View on GitHub"
    const linkText = await githubLink.textContent();
    expect(linkText?.toLowerCase()).toContain('github');

    // Verify link href points to GitHub repository
    const href = await githubLink.getAttribute('href');
    expect(href).toBe('https://github.com/yetone/mirdb');
  });

  test('TC2: Repository link is present in the header/navigation', async ({ page }) => {
    // Check for repository link in header/navigation
    const navGithubLink = page.locator('[data-testid="nav-github-link"]');
    await expect(navGithubLink).toBeVisible();

    // Verify the link text
    const linkText = await navGithubLink.textContent();
    expect(linkText?.toLowerCase()).toContain('github');

    // Verify link href points to GitHub repository
    const href = await navGithubLink.getAttribute('href');
    expect(href).toBe('https://github.com/yetone/mirdb');

    // Verify it opens in a new tab (target="_blank")
    const target = await navGithubLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify it has security attribute for external links
    const rel = await navGithubLink.getAttribute('rel');
    expect(rel).toContain('noopener');
  });

  test('TC3: Repository link opens in a new tab with correct URL', async ({ page, context }) => {
    // Get the hero GitHub link
    const githubLink = page.locator('[data-testid="github-link"]');
    await expect(githubLink).toBeVisible();

    // Verify link has target="_blank" attribute
    const target = await githubLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify link has rel="noopener" for security
    const rel = await githubLink.getAttribute('rel');
    expect(rel).toContain('noopener');

    // Verify the href points to MirDB repository
    const href = await githubLink.getAttribute('href');
    expect(href).toBe('https://github.com/yetone/mirdb');

    // Test that clicking opens a new page (simulated via newPage promise)
    const pagePromise = context.waitForEvent('page');
    await githubLink.click();
    const newPage = await pagePromise;

    // Verify the new page URL is the GitHub repository
    const newPageUrl = newPage.url();
    expect(newPageUrl).toContain('github.com/yetone/mirdb');
  });

  test('TC4: Repository link is present in the footer', async ({ page }) => {
    // Check for repository link in footer
    const footer = page.locator('[data-testid="footer"]');
    await expect(footer).toBeVisible();

    // Find the GitHub link in footer
    const footerGithubLink = page.locator('[data-testid="footer-repo-link"]');
    await expect(footerGithubLink).toBeVisible();

    // Verify link text
    const linkText = await footerGithubLink.textContent();
    expect(linkText?.toLowerCase()).toContain('github');

    // Verify link href points to GitHub repository
    const href = await footerGithubLink.getAttribute('href');
    expect(href).toBe('https://github.com/yetone/mirdb');

    // Verify it opens in a new tab (target="_blank")
    const target = await footerGithubLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify it has security attribute for external links
    const rel = await footerGithubLink.getAttribute('rel');
    expect(rel).toContain('noopener');
  });

  test('All repository links are accessible from any scroll position', async ({ page }) => {
    // Navigation GitHub link should always be visible (fixed nav)
    const navGithubLink = page.locator('[data-testid="nav-github-link"]');

    // Scroll to different sections and verify nav link is still accessible
    await page.locator('#features').scrollIntoViewIfNeeded();
    await expect(navGithubLink).toBeVisible();

    await page.locator('#getting-started').scrollIntoViewIfNeeded();
    await expect(navGithubLink).toBeVisible();

    await page.locator('#configuration').scrollIntoViewIfNeeded();
    await expect(navGithubLink).toBeVisible();
  });
});
