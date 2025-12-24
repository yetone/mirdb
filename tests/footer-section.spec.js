// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Footer Section E2E Tests (REQ-9)
 * Verifies the footer displays licensing and contribution information
 */

test.describe('Footer Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Footer section is present at bottom of page', async ({ page }) => {
    // Query for footer element
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Verify footer exists in the DOM
    const footerCount = await footer.count();
    expect(footerCount).toBe(1);

    // Scroll to footer to ensure it's at the bottom of the page
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeInViewport();
  });

  test('TC2: License information is displayed in footer', async ({ page }) => {
    // Query for license information in footer
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Look for license text (could be MIT, ISC, Apache, etc.)
    const licenseText = footer.locator('text=/license/i').first();
    await expect(licenseText).toBeVisible();

    // Verify specific license type is mentioned (MIT based on package.json)
    const footerText = await footer.textContent();
    expect(footerText?.toLowerCase()).toMatch(/mit|license|licensed/i);
  });

  test('TC3: Link to source code repository is present in footer', async ({ page }) => {
    // Query for repository link in footer
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Look for GitHub repository link
    const repoLink = footer.locator('a[href*="github.com"]');
    await expect(repoLink.first()).toBeVisible();

    // Verify the link contains the MirDB repository URL
    const href = await repoLink.first().getAttribute('href');
    expect(href).toContain('github.com');
    expect(href?.toLowerCase()).toContain('mirdb');
  });

  test('TC4: Link to contribution guidelines is present in footer', async ({ page }) => {
    // Query for contribution guidelines link in footer
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Look for contributing link - could be text or href containing "contribut"
    const contributingLink = footer.locator('a').filter({
      has: page.locator('text=/contribut/i')
    }).or(footer.locator('a[href*="contribut" i]'));

    await expect(contributingLink.first()).toBeVisible();

    // Verify the link text or href mentions contributing
    const linkText = await contributingLink.first().textContent();
    const linkHref = await contributingLink.first().getAttribute('href');

    const hasContributingReference =
      linkText?.toLowerCase().includes('contribut') ||
      linkHref?.toLowerCase().includes('contribut');

    expect(hasContributingReference).toBe(true);
  });
});
