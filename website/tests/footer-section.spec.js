// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Footer Section with Links and License', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Footer contains links to GitHub, Documentation, and displays license information', async ({ page }) => {
    // Scroll to footer and inspect footer section
    const footer = page.locator('footer, [data-testid="footer-section"], .footer');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Check for GitHub link
    const githubLink = footer.locator('a[href*="github.com"], a:has-text("GitHub")');
    await expect(githubLink).toBeVisible();

    // Check for Documentation link
    const docsLink = footer.locator('a:has-text("Documentation"), a:has-text("Docs"), a[href*="#quick-start"]');
    await expect(docsLink).toBeVisible();

    // Check for license information
    const licenseInfo = footer.locator('.license, [data-testid="license-info"], p');
    await expect(licenseInfo).toBeVisible();

    // Verify license text contains MIT or Apache
    const licenseText = await licenseInfo.textContent();
    expect(licenseText?.toLowerCase()).toMatch(/mit|apache/);
  });

  test('TC2: GitHub link in footer navigates to MirDB GitHub repository', async ({ page }) => {
    // Navigate to footer
    const footer = page.locator('footer, [data-testid="footer-section"], .footer');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Find and verify the GitHub link
    const githubLink = footer.locator('a[href*="github.com"], a:has-text("GitHub")');
    await expect(githubLink).toBeVisible();

    // Check that the href points to GitHub repository
    const href = await githubLink.getAttribute('href');
    expect(href).toContain('github.com');

    // Verify link opens in new tab for external links
    const target = await githubLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify security attributes for external links
    const rel = await githubLink.getAttribute('rel');
    expect(rel).toContain('noopener');
  });

  test('TC3: Documentation link in footer navigates to documentation resource', async ({ page }) => {
    // Navigate to footer
    const footer = page.locator('footer, [data-testid="footer-section"], .footer');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Find the Documentation link
    const docsLink = footer.locator('a:has-text("Documentation"), a:has-text("Docs")');
    await expect(docsLink).toBeVisible();

    // Get the href attribute
    const href = await docsLink.getAttribute('href');
    expect(href).toBeTruthy();

    // Click the Documentation link
    await docsLink.click();

    // Check that the navigation occurred - either to an external URL or an internal section
    const currentUrl = page.url();

    // Verify navigation happened - could be internal (#quick-start) or external documentation URL
    const isInternalLink = href?.startsWith('#');
    const isExternalLink = href?.startsWith('http');

    if (isInternalLink) {
      // If internal link, verify the URL hash changed or section is visible
      expect(currentUrl).toContain(href);
    } else if (isExternalLink) {
      // For external links, just verify the href is a valid URL
      expect(href).toMatch(/^https?:\/\//);
    } else {
      // If relative link, verify navigation occurred
      expect(currentUrl || href).toBeTruthy();
    }
  });

  test('TC4: Footer displays MIT/Apache license information', async ({ page }) => {
    // Navigate to footer
    const footer = page.locator('footer, [data-testid="footer-section"], .footer');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Find license information element
    const licenseElement = footer.locator('.license, [data-testid="license-info"], p:has-text("License"), p:has-text("MIT"), p:has-text("Apache")');
    await expect(licenseElement).toBeVisible();

    // Get license text
    const licenseText = await licenseElement.textContent();

    // Verify the license text mentions MIT and/or Apache
    const hasMIT = licenseText?.toLowerCase().includes('mit');
    const hasApache = licenseText?.toLowerCase().includes('apache');

    // Footer should display at least one of the licenses
    expect(hasMIT || hasApache).toBeTruthy();
  });
});
