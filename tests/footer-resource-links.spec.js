// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = path.resolve(__dirname, '../index.html');

/**
 * Footer and Resource Links Test Suite
 * Scenario 17: Verify the footer contains links to documentation, source code, and community resources
 */
test.describe('Footer and Resource Links', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(`file://${indexPath}`);
  });

  /**
   * Test Case 1: Check footer for GitHub/source code link
   * Input: Check footer for GitHub/source code link
   * Expected: Link to source code repository present in footer
   */
  test('TC1: Footer contains GitHub/source code link', async ({ page }) => {
    // Scroll to footer to ensure it's visible
    const footer = page.locator('[data-testid="footer"]');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Verify GitHub Repository link is present in footer
    const githubLink = page.locator('[data-testid="footer-github-link"]');
    await expect(githubLink).toBeVisible();

    // Verify the link text indicates it's a source code link
    const linkText = await githubLink.textContent();
    expect(linkText.toLowerCase()).toContain('github');

    // Verify the href points to a valid GitHub repository
    const href = await githubLink.getAttribute('href');
    expect(href).toContain('github.com');
    expect(href).toContain('mirdb');
  });

  /**
   * Test Case 2: Check footer for documentation link
   * Input: Check footer for documentation link
   * Expected: Link to documentation present in footer (or stated as coming soon)
   */
  test('TC2: Footer contains documentation link', async ({ page }) => {
    // Scroll to footer
    const footer = page.locator('[data-testid="footer"]');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Verify documentation link is present in footer
    const docsLink = page.locator('[data-testid="footer-docs-link"]');
    await expect(docsLink).toBeVisible();

    // Verify the link text contains documentation-related text
    const linkText = await docsLink.textContent();
    expect(linkText.toLowerCase()).toMatch(/doc|documentation|readme|guide/i);

    // Verify the href is a valid URL
    const href = await docsLink.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href.length).toBeGreaterThan(0);
  });

  /**
   * Test Case 3: Check footer for license information
   * Input: Check footer for license information
   * Expected: License type displayed or linked in footer
   */
  test('TC3: Footer displays license information', async ({ page }) => {
    // Scroll to footer
    const footer = page.locator('[data-testid="footer"]');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Check for license text in footer
    const footerBottom = page.locator('[data-testid="footer-bottom"]');
    await expect(footerBottom).toBeVisible();

    // Verify license information is present
    const licenseElement = page.locator('[data-testid="footer-license"]');
    await expect(licenseElement).toBeVisible();

    const licenseText = await licenseElement.textContent();
    expect(licenseText.toLowerCase()).toContain('license');

    // Verify license link exists and points to license info
    const licenseLink = page.locator('[data-testid="footer-license-link"]');
    await expect(licenseLink).toBeVisible();

    const licenseHref = await licenseLink.getAttribute('href');
    expect(licenseHref).toContain('license');
  });

  /**
   * Test Case 4: Verify footer links open correctly
   * Input: Verify footer links open correctly
   * Expected: External links open in new tabs with target='_blank'
   */
  test('TC4: External footer links have target="_blank"', async ({ page }) => {
    // Scroll to footer
    const footer = page.locator('[data-testid="footer"]');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Check GitHub link opens in new tab
    const githubLink = page.locator('[data-testid="footer-github-link"]');
    const githubTarget = await githubLink.getAttribute('target');
    expect(githubTarget).toBe('_blank');

    // Verify rel="noopener noreferrer" for security
    const githubRel = await githubLink.getAttribute('rel');
    expect(githubRel).toContain('noopener');
    expect(githubRel).toContain('noreferrer');

    // Check documentation link opens in new tab
    const docsLink = page.locator('[data-testid="footer-docs-link"]');
    const docsTarget = await docsLink.getAttribute('target');
    expect(docsTarget).toBe('_blank');

    // Check issues link opens in new tab
    const issuesLink = page.locator('[data-testid="footer-issues-link"]');
    const issuesTarget = await issuesLink.getAttribute('target');
    expect(issuesTarget).toBe('_blank');

    // Check crates.io link opens in new tab
    const cratesLink = page.locator('[data-testid="footer-crates-link"]');
    const cratesTarget = await cratesLink.getAttribute('target');
    expect(cratesTarget).toBe('_blank');

    // Check contributors link opens in new tab
    const contributorsLink = page.locator('[data-testid="footer-contributors-link"]');
    const contributorsTarget = await contributorsLink.getAttribute('target');
    expect(contributorsTarget).toBe('_blank');

    // Check license link opens in new tab
    const licenseLink = page.locator('[data-testid="footer-license-link"]');
    const licenseTarget = await licenseLink.getAttribute('target');
    expect(licenseTarget).toBe('_blank');
  });

  /**
   * Test Case 5: Check footer semantic HTML
   * Input: Check footer semantic HTML
   * Expected: Footer uses semantic footer element
   */
  test('TC5: Footer uses semantic HTML footer element', async ({ page }) => {
    // Verify footer element exists and uses semantic <footer> tag
    const footer = page.locator('footer[data-testid="footer"]');
    await expect(footer).toBeVisible();

    // Verify footer has role="contentinfo" for accessibility
    const role = await footer.getAttribute('role');
    expect(role).toBe('contentinfo');

    // Verify the footer is a semantic <footer> element
    const tagName = await footer.evaluate(el => el.tagName.toLowerCase());
    expect(tagName).toBe('footer');
  });

  /**
   * Additional test: Footer has proper heading structure
   */
  test('Footer has proper heading structure', async ({ page }) => {
    const footer = page.locator('[data-testid="footer"]');
    await footer.scrollIntoViewIfNeeded();

    // Check for section headings in footer
    const headings = footer.locator('h3');
    const headingCount = await headings.count();
    expect(headingCount).toBeGreaterThanOrEqual(2);

    // Verify Resources heading
    const resourcesSection = page.locator('[data-testid="footer-resources"]');
    const resourcesHeading = resourcesSection.locator('h3');
    await expect(resourcesHeading).toHaveText('Resources');

    // Verify Community heading
    const communitySection = page.locator('[data-testid="footer-community"]');
    const communityHeading = communitySection.locator('h3');
    await expect(communityHeading).toHaveText('Community');
  });

  /**
   * Additional test: All footer links have valid href attributes
   */
  test('All footer links have valid href attributes', async ({ page }) => {
    const footer = page.locator('[data-testid="footer"]');
    await footer.scrollIntoViewIfNeeded();

    // Get all links in footer
    const links = footer.locator('a');
    const linkCount = await links.count();
    expect(linkCount).toBeGreaterThanOrEqual(5);

    // Verify each link has a valid href
    for (let i = 0; i < linkCount; i++) {
      const link = links.nth(i);
      const href = await link.getAttribute('href');
      expect(href).toBeTruthy();
      expect(href.length).toBeGreaterThan(0);
      // Verify href is a valid URL or anchor
      expect(href).toMatch(/^(https?:\/\/|#)/);
    }
  });

  /**
   * Additional test: Footer links are keyboard accessible
   */
  test('Footer links are keyboard accessible', async ({ page }) => {
    const footer = page.locator('[data-testid="footer"]');
    await footer.scrollIntoViewIfNeeded();

    // Get the first link in the Resources section
    const resourcesLinks = page.locator('[data-testid="footer-resources"] a');
    const firstLink = resourcesLinks.first();

    // Focus on the first link
    await firstLink.focus();
    await expect(firstLink).toBeFocused();

    // Tab through links and verify they receive focus
    await page.keyboard.press('Tab');
    const secondLink = resourcesLinks.nth(1);
    await expect(secondLink).toBeFocused();
  });

  /**
   * Additional test: Footer branding is present
   */
  test('Footer contains branding information', async ({ page }) => {
    const footer = page.locator('[data-testid="footer"]');
    await footer.scrollIntoViewIfNeeded();

    // Check for footer brand/logo area
    const footerBrand = footer.locator('.footer-brand');
    await expect(footerBrand).toBeVisible();

    // Verify MirDB brand name
    const footerLogo = footerBrand.locator('.footer-logo');
    await expect(footerLogo).toHaveText('MirDB');

    // Verify tagline is present
    const brandText = await footerBrand.textContent();
    expect(brandText.toLowerCase()).toContain('key-value store');
  });

  /**
   * Additional test: Footer is at the bottom of the page
   */
  test('Footer is positioned at the bottom of the page', async ({ page }) => {
    const footer = page.locator('[data-testid="footer"]');

    // Get page height and footer position
    const footerBox = await footer.boundingBox();
    const pageHeight = await page.evaluate(() => document.documentElement.scrollHeight);

    // Footer should be near the bottom of the page
    expect(footerBox).toBeTruthy();
    const footerBottom = footerBox.y + footerBox.height;

    // Footer bottom should be approximately at page bottom (within 10px tolerance)
    expect(footerBottom).toBeGreaterThanOrEqual(pageHeight - 10);
  });
});
