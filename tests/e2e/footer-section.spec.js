// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Footer Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case 2: Footer contains link to source repository
  test('TC2: footer contains link to source repository', async ({ page }) => {
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Check for GitHub repository link
    const githubLink = footer.locator('a', { hasText: 'GitHub' });
    await expect(githubLink).toBeVisible();

    const href = await githubLink.getAttribute('href');
    expect(href).toContain('github.com/yetone/mirdb');

    // Check that it opens in a new tab
    await expect(githubLink).toHaveAttribute('target', '_blank');

    // Check for security attribute
    const rel = await githubLink.getAttribute('rel');
    expect(rel).toContain('noopener');
  });

  // Test Case 3: Footer contains link to documentation
  test('TC3: footer contains link to documentation', async ({ page }) => {
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Check for Documentation link
    const docsLink = footer.locator('a', { hasText: 'Documentation' });
    await expect(docsLink).toBeVisible();

    const href = await docsLink.getAttribute('href');
    expect(href).toBeTruthy();
    // Documentation link should point to the README or docs
    expect(href).toMatch(/github\.com.*readme|docs/i);

    // Check that it opens in a new tab
    await expect(docsLink).toHaveAttribute('target', '_blank');
  });

  // Test Case 4: Footer mentions license or links to license
  test('TC4: footer contains license information', async ({ page }) => {
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Check for License link
    const licenseLink = footer.locator('a', { hasText: 'License' });
    await expect(licenseLink).toBeVisible();

    const href = await licenseLink.getAttribute('href');
    expect(href).toContain('LICENSE');

    // Also check that the copyright text mentions the license
    const copyrightText = await footer.locator('p').first().textContent();
    expect(copyrightText?.toLowerCase()).toContain('mit');
  });

  // Test Case 5: Footer contains copyright notice
  test('TC5: footer contains copyright notice', async ({ page }) => {
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Check for copyright text
    const copyrightParagraph = footer.locator('p').first();
    await expect(copyrightParagraph).toBeVisible();

    const copyrightText = await copyrightParagraph.textContent();
    // Check for copyright symbol or word
    expect(copyrightText).toMatch(/©|copyright/i);
    // Check for year
    expect(copyrightText).toMatch(/20\d{2}/);
    // Check for project name
    expect(copyrightText?.toLowerCase()).toContain('mirdb');
  });

  // Additional test: Footer links have proper accessibility attributes
  test('Footer links have proper accessibility attributes', async ({ page }) => {
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    const footerNav = footer.locator('.footer-links');
    await expect(footerNav).toBeVisible();

    // All external links should have rel="noopener"
    const externalLinks = footerNav.locator('a[target="_blank"]');
    const count = await externalLinks.count();

    for (let i = 0; i < count; i++) {
      const link = externalLinks.nth(i);
      const rel = await link.getAttribute('rel');
      expect(rel).toContain('noopener');
    }
  });
});
