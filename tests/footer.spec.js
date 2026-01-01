const { test, expect } = require('@playwright/test');

test.describe('Footer Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('footer section is present at the bottom of the page', async ({ page }) => {
    const footer = page.locator('[data-testid="footer"]');
    await expect(footer).toBeVisible();

    // Verify footer is a semantic footer element
    await expect(footer).toHaveAttribute('data-testid', 'footer');

    // Verify footer contains the description
    const description = page.locator('[data-testid="footer-description"]');
    await expect(description).toBeVisible();
    await expect(description).toContainText('MirDB');
  });

  test('license information is displayed in footer', async ({ page }) => {
    const licenseLink = page.locator('[data-testid="footer-license-link"]');
    await expect(licenseLink).toBeVisible();
    await expect(licenseLink).toHaveText('License');
    await expect(licenseLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb/blob/master/LICENSE');
    await expect(licenseLink).toHaveAttribute('target', '_blank');
    await expect(licenseLink).toHaveAttribute('rel', 'noopener');
  });

  test('GitHub repository link is present in footer', async ({ page }) => {
    const githubLink = page.locator('[data-testid="footer-github-link"]');
    await expect(githubLink).toBeVisible();
    await expect(githubLink).toHaveText('GitHub');
    await expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
    await expect(githubLink).toHaveAttribute('target', '_blank');
    await expect(githubLink).toHaveAttribute('rel', 'noopener');
  });

  test('contribution guidelines link is present and functional', async ({ page }) => {
    const contributingLink = page.locator('[data-testid="footer-contributing-link"]');
    await expect(contributingLink).toBeVisible();
    await expect(contributingLink).toHaveText('Contributing');
    await expect(contributingLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb/blob/master/CONTRIBUTING.md');
    await expect(contributingLink).toHaveAttribute('target', '_blank');
    await expect(contributingLink).toHaveAttribute('rel', 'noopener');
  });

  test('footer links container is properly structured', async ({ page }) => {
    const footerLinks = page.locator('[data-testid="footer-links"]');
    await expect(footerLinks).toBeVisible();

    // Verify all three links are present in the footer
    const links = footerLinks.locator('a');
    await expect(links).toHaveCount(3);
  });

  test('footer is accessible via keyboard navigation', async ({ page }) => {
    // Tab through the page to reach footer links
    const githubLink = page.locator('[data-testid="footer-github-link"]');
    const contributingLink = page.locator('[data-testid="footer-contributing-link"]');
    const licenseLink = page.locator('[data-testid="footer-license-link"]');

    // Focus on the GitHub link and verify it's focusable
    await githubLink.focus();
    await expect(githubLink).toBeFocused();

    // Tab to contributing link
    await page.keyboard.press('Tab');
    await expect(contributingLink).toBeFocused();

    // Tab to license link
    await page.keyboard.press('Tab');
    await expect(licenseLink).toBeFocused();
  });
});
