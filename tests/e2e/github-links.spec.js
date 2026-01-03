// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * GitHub Repository Link Tests
 * Scenario: Verify the homepage provides working links to the source code repository
 */
test.describe('GitHub Repository Links', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case 1: Search for GitHub link in hero section
  test('TC1: hero section contains link to GitHub repository', async ({ page }) => {
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    // Find GitHub link in hero section
    const githubLink = heroSection.locator('a[href*="github.com"]');
    await expect(githubLink).toBeVisible();

    // Verify the link points to the MirDB repository
    const href = await githubLink.getAttribute('href');
    expect(href).toContain('github.com/yetone/mirdb');

    // Verify link text contains GitHub
    const linkText = await githubLink.textContent();
    expect(linkText?.toLowerCase()).toContain('github');
  });

  // Test Case 2: Search for GitHub link in footer
  test('TC2: footer contains link to GitHub repository', async ({ page }) => {
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Find GitHub link in footer
    const githubLink = footer.locator('a[href*="github.com/yetone/mirdb"]').first();
    await expect(githubLink).toBeVisible();

    // Verify the link points to the MirDB repository
    const href = await githubLink.getAttribute('href');
    expect(href).toContain('github.com/yetone/mirdb');

    // Verify link text contains GitHub or is related to the repo
    const linkText = await githubLink.textContent();
    expect(linkText).toBeTruthy();
  });

  // Test Case 4 (E2E part): Verify external links have proper security attributes
  test('TC4: external GitHub links have target=_blank and rel=noopener', async ({ page }) => {
    // Check hero section GitHub link
    const heroSection = page.locator('.hero');
    const heroGithubLink = heroSection.locator('a[href*="github.com"]');
    await expect(heroGithubLink).toHaveAttribute('target', '_blank');
    const heroRel = await heroGithubLink.getAttribute('rel');
    expect(heroRel).toContain('noopener');

    // Check footer GitHub links
    const footer = page.locator('footer');
    const footerGithubLinks = footer.locator('a[href*="github.com"]');
    const count = await footerGithubLinks.count();

    for (let i = 0; i < count; i++) {
      const link = footerGithubLinks.nth(i);
      await expect(link).toHaveAttribute('target', '_blank');
      const rel = await link.getAttribute('rel');
      expect(rel).toContain('noopener');
    }
  });
});
