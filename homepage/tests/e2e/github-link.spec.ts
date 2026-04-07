/**
 * GitHub Repository Link E2E Tests
 * Owner: Scenario 5 - GitHub Repository Link
 *
 * Test coverage:
 * - Header contains GitHub link
 * - Footer contains GitHub link
 * - GitHub links open in new tab with proper attributes
 * - GitHub links point to valid GitHub URL
 */
import { test, expect } from '@playwright/test';

const GITHUB_URL = 'https://github.com/mirdb/mirdb';
const BASE_URL = 'http://127.0.0.1:1111';

test.describe('GitHub Repository Links', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(BASE_URL);
  });

  test('TC1: Header navigation contains link to GitHub repository', async ({ page }) => {
    // Find GitHub link in header navigation
    const headerNav = page.locator('header nav');
    await expect(headerNav).toBeVisible();

    const githubLink = headerNav.locator('a[href*="github.com"]');
    await expect(githubLink).toBeVisible();
    await expect(githubLink).toContainText('GitHub');

    // Verify the link points to the correct GitHub repository
    const href = await githubLink.getAttribute('href');
    expect(href).toBe(GITHUB_URL);
  });

  test('TC2: Footer contains link to GitHub repository', async ({ page }) => {
    // Find GitHub link in footer
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    const githubLink = footer.locator('a[href*="github.com"]');
    await expect(githubLink).toBeVisible();

    // Verify the link text indicates GitHub
    const linkText = await githubLink.textContent();
    expect(linkText?.toLowerCase()).toContain('github');

    // Verify the link points to the correct GitHub repository
    const href = await githubLink.getAttribute('href');
    expect(href).toBe(GITHUB_URL);
  });

  test('TC3: GitHub links open in new tab with proper security attributes', async ({ page }) => {
    // Check header GitHub link attributes
    const headerGithubLink = page.locator('header nav a[href*="github.com"]');
    await expect(headerGithubLink).toHaveAttribute('target', '_blank');
    await expect(headerGithubLink).toHaveAttribute('rel', /noopener/);

    // Check footer GitHub link attributes
    const footerGithubLink = page.locator('footer a[href*="github.com"]');
    await expect(footerGithubLink).toHaveAttribute('target', '_blank');
    await expect(footerGithubLink).toHaveAttribute('rel', /noopener/);

    // Verify both links point to valid GitHub URL pattern
    const headerHref = await headerGithubLink.getAttribute('href');
    const footerHref = await footerGithubLink.getAttribute('href');

    expect(headerHref).toMatch(/^https:\/\/github\.com\//);
    expect(footerHref).toMatch(/^https:\/\/github\.com\//);
  });
});
