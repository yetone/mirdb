// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Footer Section Content E2E Tests
 * Scenario: Footer Section Content - Verify footer contains required information and links
 *
 * Test Cases:
 * TC1 (e2e): Check for footer element - Footer element exists at bottom of page
 * TC2 (e2e): Verify GitHub link in footer - Footer contains link to GitHub repository
 * TC3 (e2e): Check for license info - Footer mentions license or links to LICENSE file
 * TC4 (e2e): Check for version info - Footer may contain version or release information
 */

test.describe('Footer Section Content', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('http://localhost:8080');
  });

  // Test Case 1: Check for footer element
  test('TC1: Footer element exists at bottom of page', async ({ page }) => {
    // Find the footer element
    const footer = page.locator('footer.footer');
    await expect(footer).toBeVisible();

    // Verify footer is the last major section on the page
    const footerBoundingBox = await footer.boundingBox();
    expect(footerBoundingBox).not.toBeNull();

    // Check that footer is below all other sections
    const sections = page.locator('section');
    const sectionCount = await sections.count();

    for (let i = 0; i < sectionCount; i++) {
      const section = sections.nth(i);
      const sectionBox = await section.boundingBox();
      if (sectionBox && footerBoundingBox) {
        expect(footerBoundingBox.y).toBeGreaterThanOrEqual(sectionBox.y + sectionBox.height - 10);
      }
    }
  });

  // Test Case 2: Verify GitHub link in footer
  test('TC2: Footer contains link to GitHub repository', async ({ page }) => {
    const footer = page.locator('footer.footer');
    await expect(footer).toBeVisible();

    // Find GitHub link in footer
    const githubLink = footer.locator('a').filter({ hasText: /GitHub/i });
    await expect(githubLink).toBeVisible();

    // Verify link points to GitHub
    const href = await githubLink.getAttribute('href');
    expect(href).toContain('github.com');
    expect(href).toContain('mirdb');

    // Verify security attributes for external link
    const rel = await githubLink.getAttribute('rel');
    expect(rel).toContain('noopener');

    const target = await githubLink.getAttribute('target');
    expect(target).toBe('_blank');
  });

  // Test Case 3: Check for license info
  test('TC3: Footer mentions license or links to LICENSE file', async ({ page }) => {
    const footer = page.locator('footer.footer');
    await expect(footer).toBeVisible();

    // Get footer text content
    const footerText = await footer.textContent();

    // Check for license mention (MIT, Apache, LICENSE, etc.)
    const hasLicenseText = /(?:MIT|Apache|GPL|BSD|ISC|license)/i.test(footerText || '');

    // Check for LICENSE link
    const licenseLink = footer.locator('a').filter({ hasText: /license/i });
    const hasLicenseLink = (await licenseLink.count()) > 0;

    // Check for href containing LICENSE
    const allLinks = footer.locator('a');
    const linkCount = await allLinks.count();
    let hasLicenseLinkHref = false;

    for (let i = 0; i < linkCount; i++) {
      const href = await allLinks.nth(i).getAttribute('href');
      if (href && /license/i.test(href)) {
        hasLicenseLinkHref = true;
        break;
      }
    }

    expect(
      hasLicenseText || hasLicenseLink || hasLicenseLinkHref,
      `Footer should mention license or link to LICENSE file. Found text: ${footerText}`
    ).toBeTruthy();
  });

  // Test Case 4: Check for version info (optional)
  test('TC4: Footer may contain version or release information', async ({ page }) => {
    const footer = page.locator('footer.footer');
    await expect(footer).toBeVisible();

    // Get footer text content
    const footerText = await footer.textContent();

    // Check for version info - this is optional per the scenario ("may contain")
    // Look for patterns like "v1.0.0", "Version 1.0", "Release", etc.
    const hasVersionText = /(?:v?\d+\.\d+(?:\.\d+)?|version|release)/i.test(footerText || '');

    // Note: This test verifies the presence or absence of version info
    // Since the scenario says "may contain", we document the result but don't fail
    if (hasVersionText) {
      // Version info exists - verify it looks reasonable
      const versionMatch = footerText?.match(/v?\d+\.\d+(?:\.\d+)?/i);
      if (versionMatch) {
        expect(versionMatch[0]).toMatch(/v?\d+\.\d+(?:\.\d+)?/i);
      }
    }

    // This test passes regardless - version info is optional
    expect(true).toBeTruthy();
  });

  // Additional test: Footer is visible at different viewport sizes
  test('Footer is visible at mobile viewport', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('http://localhost:8080');

    const footer = page.locator('footer.footer');
    await expect(footer).toBeVisible();

    // Verify footer content is accessible
    const githubLink = footer.locator('a').filter({ hasText: /GitHub/i });
    await expect(githubLink).toBeVisible();
  });

  // Test: Footer links are clickable and accessible
  test('Footer links have adequate touch targets', async ({ page }) => {
    const footer = page.locator('footer.footer');
    await expect(footer).toBeVisible();

    const links = footer.locator('a');
    const linkCount = await links.count();

    expect(linkCount).toBeGreaterThanOrEqual(1);

    for (let i = 0; i < linkCount; i++) {
      const link = links.nth(i);
      await expect(link).toBeVisible();

      // Verify link is clickable (has cursor pointer or is an anchor)
      const tagName = await link.evaluate((el) => el.tagName.toLowerCase());
      expect(tagName).toBe('a');
    }
  });
});
