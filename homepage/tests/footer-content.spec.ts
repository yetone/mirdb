import { test, expect } from '@playwright/test';

test.describe('Footer Content', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Footer contains a functional link to the source code repository', async ({ page }) => {
    // Navigate to footer
    const footer = page.locator('[data-testid="footer"]');
    await expect(footer).toBeVisible();

    // Check for repository link
    const repoLink = page.locator('[data-testid="footer-repo-link"]');
    await expect(repoLink).toBeVisible();

    // Verify link points to GitHub repository
    const href = await repoLink.getAttribute('href');
    expect(href).toBe('https://github.com/yetone/mirdb');

    // Verify it opens in a new tab (target="_blank")
    const target = await repoLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify it has security attribute for external links
    const rel = await repoLink.getAttribute('rel');
    expect(rel).toContain('noopener');
  });

  test('TC2: Footer displays license type and/or link to license details', async ({ page }) => {
    // Navigate to footer
    const footer = page.locator('[data-testid="footer"]');
    await expect(footer).toBeVisible();

    // Check for license information
    const licenseInfo = page.locator('[data-testid="footer-license"]');
    await expect(licenseInfo).toBeVisible();

    // Verify license text contains license type (MIT, Apache, ISC, etc.) or "License"
    const licenseText = await licenseInfo.textContent();
    expect(licenseText).toBeTruthy();

    // Check that license information is present (either as text or link)
    const containsLicenseInfo = /license|MIT|Apache|GPL|ISC|BSD/i.test(licenseText || '');
    expect(containsLicenseInfo).toBe(true);
  });

  test('TC3: Footer contains navigation links to major page sections', async ({ page }) => {
    // Navigate to footer
    const footer = page.locator('[data-testid="footer"]');
    await expect(footer).toBeVisible();

    // Check for footer navigation
    const footerNav = page.locator('[data-testid="footer-nav"]');
    await expect(footerNav).toBeVisible();

    // Check for navigation links to major sections
    const featuresLink = footerNav.locator('a[href="#features"]');
    await expect(featuresLink).toBeVisible();
    await expect(featuresLink).toHaveText(/features/i);

    const gettingStartedLink = footerNav.locator('a[href="#getting-started"]');
    await expect(gettingStartedLink).toBeVisible();
    await expect(gettingStartedLink).toHaveText(/getting started/i);

    const configLink = footerNav.locator('a[href="#configuration"]');
    await expect(configLink).toBeVisible();
    await expect(configLink).toHaveText(/configuration/i);
  });

  test('Footer navigation links are functional', async ({ page }) => {
    // Navigate to footer
    const footer = page.locator('[data-testid="footer"]');
    await footer.scrollIntoViewIfNeeded();

    // Click Features link and verify navigation
    const footerNav = page.locator('[data-testid="footer-nav"]');
    const featuresLink = footerNav.locator('a[href="#features"]');
    await featuresLink.click();
    await expect(page).toHaveURL(/#features/);

    // Verify Features section is visible (using id attribute since that's the anchor target)
    const featuresSection = page.locator('section#features');
    await expect(featuresSection).toBeInViewport();
  });
});
