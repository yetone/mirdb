// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Footer Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Page has a visible footer section', async ({ page }) => {
    // Check that the footer element exists and is visible
    const footer = page.locator('footer, [data-testid="footer"]').first();
    await expect(footer).toBeVisible();

    // Verify footer has the expected data-testid attribute
    await expect(page.locator('[data-testid="footer"]')).toBeVisible();
  });

  test('TC2: Footer contains link to GitHub repository', async ({ page }) => {
    // Get the footer section
    const footer = page.locator('[data-testid="footer"]');
    await expect(footer).toBeVisible();

    // Check that there's a GitHub link in the footer
    const footerGithubLink = footer.locator('a[href*="github.com/yetone/mirdb"]');
    await expect(footerGithubLink).toBeVisible();

    // Verify the href attribute points to the correct GitHub repository
    const href = await footerGithubLink.getAttribute('href');
    expect(href).toBe('https://github.com/yetone/mirdb');

    // Verify the link opens in a new tab with proper security attributes
    const target = await footerGithubLink.getAttribute('target');
    expect(target).toBe('_blank');

    const rel = await footerGithubLink.getAttribute('rel');
    expect(rel).toContain('noopener');
  });

  test('TC3: Footer displays or links to license information', async ({ page }) => {
    // Get the footer section
    const footer = page.locator('[data-testid="footer"]');
    await expect(footer).toBeVisible();

    // Check for license information text in the footer
    // The footer should contain license information - either as text or a link
    const licenseText = footer.locator('.license, .footer-license, [class*="license"]');
    await expect(licenseText).toBeVisible();

    // Verify the license text mentions "MIT License"
    const licenseContent = await licenseText.textContent();
    expect(licenseContent).toContain('MIT License');
  });
});
