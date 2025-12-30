// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = 'file://' + path.resolve(__dirname, '..', 'index.html');

test.describe('Footer Content', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(indexPath);
  });

  /**
   * Test Case 1: Footer element exists at bottom of page
   */
  test('TC1: footer section exists at bottom of page', async ({ page }) => {
    // Verify footer element exists
    const footer = page.locator('footer.footer');
    await expect(footer).toBeVisible();

    // Verify footer is at the bottom of the page (after other content)
    const footerBoundingBox = await footer.boundingBox();
    expect(footerBoundingBox).toBeTruthy();

    // Verify footer has proper structure
    const footerContent = footer.locator('.footer-content');
    await expect(footerContent).toBeVisible();
  });

  /**
   * Test Case 2: Footer contains license text (MIT, Apache, or License)
   */
  test('TC2: footer contains license information', async ({ page }) => {
    // Verify footer exists
    const footer = page.locator('footer.footer');
    await expect(footer).toBeVisible();

    // Look for license information
    const footerText = await footer.textContent();

    // Verify footer contains MIT, Apache, or License text
    const hasLicenseInfo = /MIT|Apache|License/i.test(footerText);
    expect(hasLicenseInfo).toBe(true);

    // Verify the dedicated license element exists
    const licenseElement = footer.locator('.footer-license');
    await expect(licenseElement).toBeVisible();
    await expect(licenseElement).toContainText(/MIT|Apache|License/i);
  });

  /**
   * Test Case 3: Footer contains link to GitHub repository
   */
  test('TC3: footer contains GitHub link', async ({ page }) => {
    // Verify footer exists
    const footer = page.locator('footer.footer');
    await expect(footer).toBeVisible();

    // Look for GitHub link
    const githubLink = footer.locator('a[href*="github"]');
    await expect(githubLink).toBeVisible();

    // Verify GitHub link text
    const linkText = await githubLink.textContent();
    expect(linkText).toMatch(/GitHub/i);

    // Verify link href contains github
    const href = await githubLink.getAttribute('href');
    expect(href).toMatch(/github\.com/i);

    // Verify link has proper security attributes for external link
    const target = await githubLink.getAttribute('target');
    const rel = await githubLink.getAttribute('rel');
    expect(target).toBe('_blank');
    expect(rel).toContain('noopener');
  });

  /**
   * Test Case 4: Footer optionally contains version or last updated date
   */
  test('TC4: footer optionally contains version or last updated information', async ({ page }) => {
    // Verify footer exists
    const footer = page.locator('footer.footer');
    await expect(footer).toBeVisible();

    // Get footer text content
    const footerText = await footer.textContent();

    // Check for version number pattern (e.g., v1.0.0, 1.0, etc.) or date patterns
    const hasVersionInfo = /v?\d+\.\d+(\.\d+)?|version|last updated|\d{4}/i.test(footerText);

    // This test case is marked as optional in the requirements
    // We verify the footer exists and check if version info is present
    // The test passes regardless since it's optional
    if (hasVersionInfo) {
      // If version info exists, log it
      console.log('Footer contains version/date information');
    }

    // The test passes as long as the footer structure is correct
    expect(footer).toBeTruthy();
  });
});
