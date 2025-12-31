import { test, expect } from '@playwright/test';

test.describe('Footer Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Footer contains link to GitHub repository that is clickable', async ({ page }) => {
    // Navigate to footer by scrolling to the bottom of the page
    const footerSection = page.locator('[data-testid="footer-section"]');
    await footerSection.scrollIntoViewIfNeeded();
    await expect(footerSection).toBeVisible();

    // Check for GitHub repository link in footer
    const githubLink = page.locator('[data-testid="footer-github-link"]');
    await expect(githubLink).toBeVisible();
    await expect(githubLink).toHaveText('GitHub');

    // Verify the link is clickable (has href attribute)
    await expect(githubLink).toHaveAttribute('href', /github\.com/);
  });

  test('TC2: GitHub link navigates to the MirDB GitHub repository page', async ({ page }) => {
    // Navigate to footer
    const footerSection = page.locator('[data-testid="footer-section"]');
    await footerSection.scrollIntoViewIfNeeded();

    // Click GitHub link in footer
    const githubLink = page.locator('[data-testid="footer-github-link"]');
    await expect(githubLink).toBeVisible();

    // Verify the link points to the MirDB GitHub repository
    await expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');

    // Verify it opens in a new tab (target="_blank")
    await expect(githubLink).toHaveAttribute('target', '_blank');

    // Verify it has security attributes for external links
    await expect(githubLink).toHaveAttribute('rel', /noopener/);
    await expect(githubLink).toHaveAttribute('rel', /noreferrer/);
  });

  test('TC3: Footer contains link to documentation', async ({ page }) => {
    // Navigate to footer
    const footerSection = page.locator('[data-testid="footer-section"]');
    await footerSection.scrollIntoViewIfNeeded();
    await expect(footerSection).toBeVisible();

    // Check for documentation link in footer
    const docsLink = page.locator('[data-testid="footer-docs-link"]');
    await expect(docsLink).toBeVisible();
    await expect(docsLink).toHaveText('Documentation');

    // Verify the link is present and has href attribute
    await expect(docsLink).toHaveAttribute('href', /.+/);
  });

  test('TC4: Footer displays license type (MIT)', async ({ page }) => {
    // Navigate to footer
    const footerSection = page.locator('[data-testid="footer-section"]');
    await footerSection.scrollIntoViewIfNeeded();
    await expect(footerSection).toBeVisible();

    // Check for license information in footer
    const licenseInfo = page.locator('[data-testid="footer-license"]');
    await expect(licenseInfo).toBeVisible();

    // Verify license type is displayed (MIT in this case)
    await expect(licenseInfo).toContainText('MIT');
  });
});
