// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * External Link Security Tests
 *
 * This test suite verifies that all external links on the MirDB homepage
 * have proper security attributes to prevent security vulnerabilities.
 *
 * Security concerns addressed:
 * - rel="noopener" prevents the new page from accessing window.opener
 * - rel="noreferrer" prevents the Referer header from being sent
 * - target="_blank" ensures links open in new tab (best practice for external links)
 * - No sensitive data should be passed in URL parameters
 */

test.describe('External Link Security', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Check GitHub link attributes
   * Expected: GitHub link has rel='noopener noreferrer' and target='_blank'
   */
  test('should have GitHub link with proper security attributes', async ({ page }) => {
    // Find the main GitHub link in the navigation header
    const header = page.locator('header');
    const githubNavLink = header.locator('a[href*="github.com/yetone/mirdb"]:not([href*="#"]):not([href*="issues"]):not([href*="readme"])');

    await expect(githubNavLink.first()).toBeVisible();

    // Verify target="_blank"
    const target = await githubNavLink.first().getAttribute('target');
    expect(target).toBe('_blank');

    // Verify rel attribute contains both noopener and noreferrer
    const rel = await githubNavLink.first().getAttribute('rel');
    expect(rel).toBeTruthy();
    expect(rel).toContain('noopener');
    expect(rel).toContain('noreferrer');

    // Verify the href points to the correct repository
    const href = await githubNavLink.first().getAttribute('href');
    expect(href).toContain('github.com/yetone/mirdb');
  });

  /**
   * Test Case 2: Check all external links
   * Expected: All external links have rel='noopener noreferrer'
   */
  test('should have all external links with rel="noopener noreferrer"', async ({ page }) => {
    // Find all anchor tags that link to external sites (not starting with / or #)
    const allLinks = await page.locator('a[href^="http"], a[href^="https"]').all();

    expect(allLinks.length).toBeGreaterThan(0);

    const externalLinkResults = [];

    for (const link of allLinks) {
      const href = await link.getAttribute('href');
      const rel = await link.getAttribute('rel');
      const target = await link.getAttribute('target');
      const linkText = await link.textContent();

      // Store results for reporting
      externalLinkResults.push({
        href,
        rel,
        target,
        linkText: linkText?.trim(),
        hasNoopener: rel?.includes('noopener') ?? false,
        hasNoreferrer: rel?.includes('noreferrer') ?? false,
        opensInNewTab: target === '_blank'
      });

      // Verify rel attribute contains both security values
      expect(rel, `Link "${href}" is missing rel attribute`).toBeTruthy();
      expect(rel, `Link "${href}" is missing noopener`).toContain('noopener');
      expect(rel, `Link "${href}" is missing noreferrer`).toContain('noreferrer');

      // Verify external links open in new tab
      expect(target, `Link "${href}" should open in new tab`).toBe('_blank');
    }

    // Log summary for debugging
    console.log(`Verified ${externalLinkResults.length} external links have proper security attributes`);
  });

  /**
   * Test Case 3: Verify no sensitive data in URL params
   * Expected: External links do not pass sensitive information
   */
  test('should not pass sensitive data in external link URL parameters', async ({ page }) => {
    // Find all external links
    const allExternalLinks = await page.locator('a[href^="http"], a[href^="https"]').all();

    expect(allExternalLinks.length).toBeGreaterThan(0);

    // List of sensitive parameter names that should not be present
    const sensitiveParamPatterns = [
      /password/i,
      /passwd/i,
      /secret/i,
      /token/i,
      /api[-_]?key/i,
      /auth/i,
      /session/i,
      /cookie/i,
      /credential/i,
      /private/i,
      /ssn/i,
      /credit[-_]?card/i,
      /card[-_]?number/i,
      /cvv/i,
      /pin/i,
      /email/i,
      /user[-_]?id/i,
      /account/i
    ];

    for (const link of allExternalLinks) {
      const href = await link.getAttribute('href');

      // Parse URL to check query parameters
      let url;
      try {
        url = new URL(href);
      } catch (e) {
        // Skip malformed URLs
        continue;
      }

      // Check for sensitive data in query parameters
      const searchParams = url.searchParams;
      for (const [paramName, paramValue] of searchParams.entries()) {
        for (const pattern of sensitiveParamPatterns) {
          expect(
            pattern.test(paramName),
            `Link "${href}" contains potentially sensitive parameter: ${paramName}`
          ).toBe(false);
        }
      }

      // Check for sensitive data in URL path (common patterns)
      const pathLower = url.pathname.toLowerCase();
      expect(
        pathLower.includes('/password') || pathLower.includes('/secret'),
        `Link "${href}" contains potentially sensitive path segment`
      ).toBe(false);
    }
  });

  /**
   * Additional Test: Verify hero section GitHub button has security attributes
   */
  test('should have hero section GitHub button with proper security attributes', async ({ page }) => {
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    // Find the "View on GitHub" button in hero section
    const githubButton = heroSection.locator('a:has-text("GitHub")');
    await expect(githubButton.first()).toBeVisible();

    // Verify security attributes
    const target = await githubButton.first().getAttribute('target');
    expect(target).toBe('_blank');

    const rel = await githubButton.first().getAttribute('rel');
    expect(rel).toContain('noopener');
    expect(rel).toContain('noreferrer');
  });

  /**
   * Additional Test: Verify footer external links have security attributes
   */
  test('should have footer external links with proper security attributes', async ({ page }) => {
    const footer = page.locator('footer');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Find all external links in footer
    const footerLinks = await footer.locator('a[href^="http"], a[href^="https"]').all();

    expect(footerLinks.length).toBeGreaterThan(0);

    for (const link of footerLinks) {
      const href = await link.getAttribute('href');
      const rel = await link.getAttribute('rel');
      const target = await link.getAttribute('target');

      // Verify security attributes
      expect(rel, `Footer link "${href}" is missing rel attribute`).toBeTruthy();
      expect(rel, `Footer link "${href}" is missing noopener`).toContain('noopener');
      expect(rel, `Footer link "${href}" is missing noreferrer`).toContain('noreferrer');
      expect(target, `Footer link "${href}" should open in new tab`).toBe('_blank');
    }
  });

  /**
   * Additional Test: Verify project status section GitHub link has security attributes
   */
  test('should have status section GitHub link with proper security attributes', async ({ page }) => {
    const statusSection = page.locator('#status, [data-testid="status-section"]');
    await statusSection.scrollIntoViewIfNeeded();
    await expect(statusSection).toBeVisible();

    // Find GitHub link in status section
    const githubLink = statusSection.locator('a[href*="github.com"]');
    await expect(githubLink.first()).toBeVisible();

    // Verify security attributes
    const target = await githubLink.first().getAttribute('target');
    expect(target).toBe('_blank');

    const rel = await githubLink.first().getAttribute('rel');
    expect(rel).toContain('noopener');
    expect(rel).toContain('noreferrer');
  });

  /**
   * Additional Test: Documentation link security check
   */
  test('should have Documentation link with proper security attributes', async ({ page }) => {
    const header = page.locator('header');
    const docsLink = header.locator('a:has-text("Documentation")');

    await expect(docsLink).toBeVisible();

    const href = await docsLink.getAttribute('href');

    // If documentation link is external (GitHub readme)
    if (href?.startsWith('http')) {
      const target = await docsLink.getAttribute('target');
      expect(target).toBe('_blank');

      const rel = await docsLink.getAttribute('rel');
      expect(rel).toContain('noopener');
      expect(rel).toContain('noreferrer');
    }
  });

  /**
   * Additional Test: Count and verify all external links
   */
  test('should identify and secure all external links on the page', async ({ page }) => {
    // Get count of all external links
    const externalLinks = await page.locator('a[href^="http"], a[href^="https"]').all();

    // Log the count for verification
    console.log(`Found ${externalLinks.length} external links on the page`);

    // Verify we found a reasonable number of external links
    expect(externalLinks.length).toBeGreaterThanOrEqual(5);

    // Collect all link data for comprehensive verification
    const linkData = [];
    for (const link of externalLinks) {
      const href = await link.getAttribute('href');
      const rel = await link.getAttribute('rel');
      const target = await link.getAttribute('target');

      linkData.push({ href, rel, target });

      // All external links must have security attributes
      expect(rel).toBeTruthy();
      expect(rel).toContain('noopener');
      expect(rel).toContain('noreferrer');
      expect(target).toBe('_blank');
    }

    // Verify all links pass security check
    const secureLinks = linkData.filter(l =>
      l.rel?.includes('noopener') &&
      l.rel?.includes('noreferrer') &&
      l.target === '_blank'
    );

    expect(secureLinks.length).toBe(externalLinks.length);
  });
});
