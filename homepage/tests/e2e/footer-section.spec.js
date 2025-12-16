// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Footer Section E2E Tests
 *
 * REQ-7: Include footer with project links, license information, and contact details
 *
 * This test file validates that the footer section contains all required elements:
 * - GitHub repository link
 * - Documentation link
 * - License information
 * - Project status badge
 */

test.describe('Footer Section Content (Scenario 7)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Scroll to footer section
    await page.locator('[data-testid="footer-section"]').scrollIntoViewIfNeeded();
  });

  /**
   * Test Case 1: Check for GitHub repository link in footer
   * Expected: Footer contains clickable GitHub repository link
   */
  test('TC1: Footer contains clickable GitHub repository link', async ({ page }) => {
    // Find the GitHub link in footer navigation
    const githubLink = page.locator('[data-testid="footer-github-link"]');

    // Verify the link is visible
    await expect(githubLink).toBeVisible();

    // Verify it contains correct text
    await expect(githubLink).toContainText('GitHub');

    // Verify the href points to the GitHub repository
    const href = await githubLink.getAttribute('href');
    expect(href).toBe('https://github.com/yetone/mirdb');

    // Verify it opens in new tab with security attributes
    const target = await githubLink.getAttribute('target');
    expect(target).toBe('_blank');

    const rel = await githubLink.getAttribute('rel');
    expect(rel).toContain('noopener');
    expect(rel).toContain('noreferrer');

    // Verify the link is clickable (has pointer cursor)
    await expect(githubLink).toHaveCSS('cursor', 'pointer');
  });

  /**
   * Test Case 2: Check for documentation link in footer
   * Expected: Footer contains clickable documentation link
   */
  test('TC2: Footer contains clickable documentation link', async ({ page }) => {
    // Check for internal documentation link (scrolls to getting-started)
    const internalDocsLink = page.locator('[data-testid="footer-docs-internal-link"]');
    await expect(internalDocsLink).toBeVisible();
    await expect(internalDocsLink).toContainText('Documentation');

    // Verify internal link points to getting-started section
    const internalHref = await internalDocsLink.getAttribute('href');
    expect(internalHref).toBe('#getting-started');

    // Check for external full documentation link
    const externalDocsLink = page.locator('[data-testid="footer-docs-link"]');
    await expect(externalDocsLink).toBeVisible();
    await expect(externalDocsLink).toContainText('Docs');

    // Verify the external link points to GitHub documentation
    const href = await externalDocsLink.getAttribute('href');
    expect(href).toContain('github.com/yetone/mirdb');
    expect(href).toContain('readme');

    // Verify external link opens in new tab with security attributes
    const target = await externalDocsLink.getAttribute('target');
    expect(target).toBe('_blank');

    const rel = await externalDocsLink.getAttribute('rel');
    expect(rel).toContain('noopener');
    expect(rel).toContain('noreferrer');
  });

  /**
   * Test Case 3: Check for license information
   * Expected: Footer displays license information or link to license
   */
  test('TC3: Footer displays license information or link to license', async ({ page }) => {
    // Check for license text in copyright section
    const copyrightSection = page.locator('[data-testid="footer-copyright"]');
    await expect(copyrightSection).toBeVisible();
    await expect(copyrightSection).toContainText('MIT');

    // Check for dedicated license link in navigation
    const licenseNavLink = page.locator('[data-testid="footer-license-link"]');
    await expect(licenseNavLink).toBeVisible();
    await expect(licenseNavLink).toContainText('License');

    // Verify license link points to MIT license
    const navLinkHref = await licenseNavLink.getAttribute('href');
    expect(navLinkHref).toContain('opensource.org/licenses/MIT');

    // Check for clickable license link in copyright text
    const licenseTextLink = page.locator('[data-testid="footer-license-text-link"]');
    await expect(licenseTextLink).toBeVisible();
    await expect(licenseTextLink).toContainText('MIT License');

    // Verify license text link is clickable
    const textLinkHref = await licenseTextLink.getAttribute('href');
    expect(textLinkHref).toContain('opensource.org/licenses/MIT');
  });

  /**
   * Test Case 4: Check for project status badge
   * Expected: Project status badge is displayed (if applicable)
   */
  test('TC4: Project status badge is displayed', async ({ page }) => {
    // Find the badges container
    const badgesContainer = page.locator('[data-testid="footer-badges"]');
    await expect(badgesContainer).toBeVisible();

    // Check for project status badge
    const statusBadgeLink = page.locator('[data-testid="project-status-badge"]');
    await expect(statusBadgeLink).toBeVisible();

    // Verify status badge image exists
    const statusBadgeImg = page.locator('[data-testid="status-badge-img"]');
    await expect(statusBadgeImg).toBeVisible();

    // Verify badge has alt text for accessibility
    const altText = await statusBadgeImg.getAttribute('alt');
    expect(altText).toBeTruthy();
    expect(altText.toLowerCase()).toContain('status');

    // Check for license badge as well
    const licenseBadgeLink = page.locator('[data-testid="license-badge-link"]');
    await expect(licenseBadgeLink).toBeVisible();

    const licenseBadgeImg = page.locator('[data-testid="license-badge-img"]');
    await expect(licenseBadgeImg).toBeVisible();

    // Verify license badge has alt text
    const licenseAltText = await licenseBadgeImg.getAttribute('alt');
    expect(licenseAltText).toBeTruthy();
    expect(licenseAltText.toLowerCase()).toContain('license');
  });

  /**
   * Additional E2E Test: Footer navigation has proper structure
   */
  test('TC-Footer-Nav: Footer navigation has proper structure with all required links', async ({ page }) => {
    const footerNav = page.locator('[data-testid="footer-nav"]');
    await expect(footerNav).toBeVisible();

    // Get all navigation links
    const navLinks = footerNav.locator('a');
    const linkCount = await navLinks.count();

    // Should have at least 3 links: GitHub, Documentation, License
    expect(linkCount).toBeGreaterThanOrEqual(3);

    // Verify all links have proper security attributes for external links
    for (let i = 0; i < linkCount; i++) {
      const link = navLinks.nth(i);
      const href = await link.getAttribute('href');

      if (href && href.startsWith('http')) {
        const target = await link.getAttribute('target');
        const rel = await link.getAttribute('rel');

        expect(target).toBe('_blank');
        expect(rel).toContain('noopener');
      }
    }
  });

  /**
   * Additional E2E Test: Footer badges are clickable and link to correct destinations
   */
  test('TC-Footer-Badges: Footer badges are clickable with correct destinations', async ({ page }) => {
    // Status badge should link to GitHub
    const statusBadge = page.locator('[data-testid="project-status-badge"]');
    const statusHref = await statusBadge.getAttribute('href');
    expect(statusHref).toContain('github.com/yetone/mirdb');

    // License badge should link to MIT license
    const licenseBadge = page.locator('[data-testid="license-badge-link"]');
    const licenseHref = await licenseBadge.getAttribute('href');
    expect(licenseHref).toContain('opensource.org/licenses/MIT');

    // Both should open in new tab
    expect(await statusBadge.getAttribute('target')).toBe('_blank');
    expect(await licenseBadge.getAttribute('target')).toBe('_blank');
  });

  /**
   * Additional E2E Test: Footer copyright text is present and correct
   */
  test('TC-Footer-Copyright: Footer displays copyright with year and project name', async ({ page }) => {
    const copyright = page.locator('[data-testid="footer-copyright"]');
    await expect(copyright).toBeVisible();

    // Should contain copyright symbol or text
    const text = await copyright.textContent();
    expect(text).toMatch(/©|copyright/i);

    // Should contain year
    expect(text).toMatch(/20\d{2}/);

    // Should contain project name
    expect(text.toLowerCase()).toContain('mirdb');
  });
});
