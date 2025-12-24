// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for Footer Section Display (Scenario 16)
 * Description: Verify that the footer contains appropriate links and project status
 * information as specified in REQ-10
 *
 * Test Cases:
 * TC1: Check footer for documentation link
 * TC2: Check footer for GitHub link
 * TC3: Check footer for license information
 * TC4: Check for project roadmap mention (Raft consensus)
 */

test.describe('Footer Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Check footer for documentation link
   * Input: Check footer for documentation link
   * Expected: Footer contains link to documentation
   */
  test('TC1: Footer contains link to documentation', async ({ page }) => {
    // Step 1: Navigate to footer (scroll to bottom)
    const footer = page.locator('[data-testid="footer"]');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Step 2: Verify documentation link exists in footer
    const docsLink = footer.locator('[data-testid="footer-docs-link"]');
    await expect(docsLink).toBeVisible();

    // Verify link text contains "Documentation"
    await expect(docsLink).toHaveText(/documentation/i);

    // Verify link has valid href
    const href = await docsLink.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href.length).toBeGreaterThan(0);

    // Verify link opens in new tab (external link best practice)
    const target = await docsLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify link has rel="noopener" for security
    const rel = await docsLink.getAttribute('rel');
    expect(rel).toMatch(/noopener/);
  });

  /**
   * Test Case 2: Check footer for GitHub link
   * Input: Check footer for GitHub link
   * Expected: Footer contains link to GitHub repository
   */
  test('TC2: Footer contains link to GitHub repository', async ({ page }) => {
    // Step 1: Navigate to footer
    const footer = page.locator('[data-testid="footer"]');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Step 2: Verify GitHub link exists in footer
    const githubLink = footer.locator('[data-testid="footer-github-link"]');
    await expect(githubLink).toBeVisible();

    // Verify link text contains "GitHub"
    await expect(githubLink).toHaveText(/github/i);

    // Verify link points to GitHub
    const href = await githubLink.getAttribute('href');
    expect(href).toMatch(/^https:\/\/github\.com\//);

    // Verify link opens in new tab
    const target = await githubLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify link has rel="noopener" for security
    const rel = await githubLink.getAttribute('rel');
    expect(rel).toMatch(/noopener/);
  });

  /**
   * Test Case 3: Check footer for license information
   * Input: Check footer for license information
   * Expected: Footer mentions or links to project license
   */
  test('TC3: Footer mentions or links to project license', async ({ page }) => {
    // Step 1: Navigate to footer
    const footer = page.locator('[data-testid="footer"]');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Step 2: Verify license link or mention exists in footer
    const licenseLink = footer.locator('[data-testid="footer-license-link"]');
    await expect(licenseLink).toBeVisible();

    // Verify link text contains "License"
    await expect(licenseLink).toHaveText(/license/i);

    // Verify link has valid href
    const href = await licenseLink.getAttribute('href');
    expect(href).toBeTruthy();

    // Verify link opens in new tab
    const target = await licenseLink.getAttribute('target');
    expect(target).toBe('_blank');

    // Verify link has rel="noopener" for security
    const rel = await licenseLink.getAttribute('rel');
    expect(rel).toMatch(/noopener/);
  });

  /**
   * Test Case 4: Check for project roadmap mention
   * Input: Check for project roadmap mention
   * Expected: Footer or page mentions upcoming features (e.g., Raft consensus)
   */
  test('TC4: Footer or page mentions upcoming features (Raft consensus)', async ({ page }) => {
    // Step 1: Navigate to footer
    const footer = page.locator('[data-testid="footer"]');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Step 2: Verify roadmap/upcoming features mention exists
    const roadmapNote = footer.locator('[data-testid="footer-roadmap"]');
    await expect(roadmapNote).toBeVisible();

    // Verify note mentions Raft consensus
    await expect(roadmapNote).toContainText(/raft consensus/i);

    // Verify note indicates it's coming soon (roadmap item)
    await expect(roadmapNote).toContainText(/coming soon/i);
  });

  /**
   * Additional test: Footer is visible at the bottom of the page
   */
  test('Footer is positioned at the bottom of the page', async ({ page }) => {
    // Scroll to the very bottom of the page
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));

    // Verify footer is visible
    const footer = page.locator('[data-testid="footer"]');
    await expect(footer).toBeVisible();

    // Verify footer is the last major element in the document
    const footerElement = page.locator('body > footer');
    await expect(footerElement).toBeVisible();
  });

  /**
   * Additional test: Footer links section is present and contains multiple links
   */
  test('Footer contains multiple navigation links', async ({ page }) => {
    const footer = page.locator('[data-testid="footer"]');
    await footer.scrollIntoViewIfNeeded();

    const footerLinks = footer.locator('[data-testid="footer-links"] a');
    const linkCount = await footerLinks.count();

    // Footer should contain at least 5 links (Features, Architecture, Getting Started, Configuration, Documentation, GitHub, License)
    expect(linkCount).toBeGreaterThanOrEqual(5);
  });

  /**
   * Additional test: Footer external links use HTTPS
   */
  test('Footer external links use HTTPS protocol', async ({ page }) => {
    const footer = page.locator('[data-testid="footer"]');
    await footer.scrollIntoViewIfNeeded();

    // Get all external links (starting with http)
    const externalLinks = footer.locator('a[href^="http"]');
    const count = await externalLinks.count();

    for (let i = 0; i < count; i++) {
      const link = externalLinks.nth(i);
      const href = await link.getAttribute('href');

      // All external links should use HTTPS
      expect(href, `External link should use HTTPS: ${href}`).toMatch(/^https:\/\//);
    }
  });

  /**
   * Additional test: Footer internal links work correctly
   */
  test('Footer internal anchor links point to existing sections', async ({ page }) => {
    const footer = page.locator('[data-testid="footer"]');
    await footer.scrollIntoViewIfNeeded();

    // Get all internal anchor links
    const anchorLinks = footer.locator('a[href^="#"]');
    const count = await anchorLinks.count();

    for (let i = 0; i < count; i++) {
      const link = anchorLinks.nth(i);
      const href = await link.getAttribute('href');

      if (href && href.startsWith('#')) {
        const targetId = href.slice(1);
        const targetElement = page.locator(`#${targetId}`);

        // Verify target element exists
        const exists = await targetElement.count() > 0;
        expect(exists, `Anchor ${href} should point to existing element`).toBeTruthy();
      }
    }
  });

  /**
   * Additional test: Footer copyright notice is present
   */
  test('Footer contains copyright notice', async ({ page }) => {
    const footer = page.locator('[data-testid="footer"]');
    await footer.scrollIntoViewIfNeeded();

    const copyright = footer.locator('.copyright');
    await expect(copyright).toBeVisible();
    await expect(copyright).toContainText(/©|copyright/i);
    await expect(copyright).toContainText(/mirdb/i);
  });

  /**
   * Additional test: Footer mentions open source status
   */
  test('Footer mentions open source status', async ({ page }) => {
    const footer = page.locator('[data-testid="footer"]');
    await footer.scrollIntoViewIfNeeded();

    // Footer note should mention open source
    const footerNote = footer.locator('[data-testid="footer-roadmap"]');
    await expect(footerNote).toContainText(/open source/i);
  });
});
