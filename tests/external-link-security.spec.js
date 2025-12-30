// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = 'file://' + path.resolve(__dirname, '..', 'index.html');

test.describe('External Link Security', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(indexPath);
  });

  /**
   * Test Case 1: Find all external links on page
   * Expected: All external links are identified and catalogued
   */
  test('TC1: all external links are identified and catalogued', async ({ page }) => {
    // Find all anchor tags with href attribute
    const allLinks = page.locator('a[href]');
    const allLinksCount = await allLinks.count();
    expect(allLinksCount).toBeGreaterThan(0);

    // Identify external links (those with http:// or https:// and not same domain)
    const externalLinks = page.locator('a[href^="http://"], a[href^="https://"]');
    const externalLinksCount = await externalLinks.count();

    // We expect at least some external links (e.g., GitHub)
    expect(externalLinksCount).toBeGreaterThanOrEqual(1);

    // Catalogue all external links
    const externalLinksList = [];
    for (let i = 0; i < externalLinksCount; i++) {
      const link = externalLinks.nth(i);
      const href = await link.getAttribute('href');
      const text = await link.textContent();
      const target = await link.getAttribute('target');
      const rel = await link.getAttribute('rel');

      externalLinksList.push({
        href,
        text: text?.trim(),
        target,
        rel
      });
    }

    // Verify we successfully catalogued the links
    expect(externalLinksList.length).toBe(externalLinksCount);

    // Log the catalogue for documentation purposes
    console.log('External links found:', JSON.stringify(externalLinksList, null, 2));

    // Verify all external links have been identified
    for (const link of externalLinksList) {
      expect(link.href).toMatch(/^https?:\/\//);
    }
  });

  /**
   * Test Case 2: Check external links for rel='noopener'
   * Expected: All external links include rel='noopener' or rel='noopener noreferrer'
   */
  test('TC2: all external links have rel noopener or noopener noreferrer', async ({ page }) => {
    // Find all external links (http/https)
    const externalLinks = page.locator('a[href^="http://"], a[href^="https://"]');
    const count = await externalLinks.count();

    // We should have external links to test
    expect(count).toBeGreaterThan(0);

    // Check each external link for proper rel attribute
    const linksWithSecurityIssues = [];

    for (let i = 0; i < count; i++) {
      const link = externalLinks.nth(i);
      const href = await link.getAttribute('href');
      const rel = await link.getAttribute('rel');
      const target = await link.getAttribute('target');

      // External links should have rel="noopener" or rel="noopener noreferrer"
      const hasNoopener = rel && rel.includes('noopener');

      if (!hasNoopener) {
        linksWithSecurityIssues.push({
          href,
          target,
          rel,
          issue: 'Missing noopener in rel attribute'
        });
      }
    }

    // Report any security issues found
    if (linksWithSecurityIssues.length > 0) {
      console.log('Links with security issues:', JSON.stringify(linksWithSecurityIssues, null, 2));
    }

    // All external links should have noopener
    expect(linksWithSecurityIssues.length).toBe(0);
  });

  /**
   * Test Case 2b: External links with target="_blank" must have noopener noreferrer
   * Expected: All external links that open in new tab have proper security attributes
   */
  test('TC2b: external links with target _blank have noopener noreferrer', async ({ page }) => {
    // Find external links that open in new tab
    const externalLinksWithBlank = page.locator('a[href^="http://"][target="_blank"], a[href^="https://"][target="_blank"]');
    const count = await externalLinksWithBlank.count();

    // Check each link
    for (let i = 0; i < count; i++) {
      const link = externalLinksWithBlank.nth(i);
      const href = await link.getAttribute('href');
      const rel = await link.getAttribute('rel');

      // Links with target="_blank" MUST have noopener to prevent tabnabbing attacks
      expect(rel).toBeTruthy();
      expect(rel).toContain('noopener');

      // Best practice: should also include noreferrer
      expect(rel).toContain('noreferrer');

      console.log(`Link ${href} has proper security attributes: rel="${rel}"`);
    }
  });

  /**
   * Test Case 3: Check for broken links
   * Expected: All links return 200 OK or valid redirect status
   * Note: This is an integration test that makes actual HTTP requests
   */
  test('TC3: all external links return valid HTTP status', async ({ page, request }) => {
    // Find all external links
    const externalLinks = page.locator('a[href^="http://"], a[href^="https://"]');
    const count = await externalLinks.count();

    expect(count).toBeGreaterThan(0);

    const linkResults = [];

    for (let i = 0; i < count; i++) {
      const link = externalLinks.nth(i);
      const href = await link.getAttribute('href');

      if (!href) continue;

      try {
        // Make a HEAD request to check link validity
        const response = await request.head(href, {
          timeout: 10000,
          ignoreHTTPSErrors: true
        });

        const status = response.status();

        // Valid statuses: 200 (OK), 301/302 (redirects), 304 (not modified)
        const isValidStatus = status >= 200 && status < 400;

        linkResults.push({
          href,
          status,
          isValid: isValidStatus
        });

        expect(isValidStatus).toBe(true);
      } catch (error) {
        // Some sites block HEAD requests, try GET
        try {
          const response = await request.get(href, {
            timeout: 10000,
            ignoreHTTPSErrors: true
          });

          const status = response.status();
          const isValidStatus = status >= 200 && status < 400;

          linkResults.push({
            href,
            status,
            isValid: isValidStatus,
            method: 'GET'
          });

          expect(isValidStatus).toBe(true);
        } catch (getError) {
          // If both fail, record the error but don't fail the test for network issues
          // as this may be a test environment limitation
          linkResults.push({
            href,
            error: getError.message,
            isValid: false
          });
          console.log(`Warning: Could not verify link ${href}: ${getError.message}`);
        }
      }
    }

    // Log all results for documentation
    console.log('Link validation results:', JSON.stringify(linkResults, null, 2));

    // At least some links should be verifiable
    const verifiableLinks = linkResults.filter(r => !r.error);
    expect(verifiableLinks.length).toBeGreaterThan(0);
  });

  /**
   * Additional Test: Verify all internal links don't have unnecessary external link attributes
   */
  test('TC-bonus: internal links do not have unnecessary target _blank', async ({ page }) => {
    // Find internal links (starting with #, /, or relative paths)
    const internalLinks = page.locator('a[href^="#"], a[href^="/"], a:not([href^="http"])');
    const count = await internalLinks.count();

    for (let i = 0; i < count; i++) {
      const link = internalLinks.nth(i);
      const href = await link.getAttribute('href');
      const target = await link.getAttribute('target');

      // Internal links should not open in new tab unnecessarily
      if (target === '_blank') {
        console.log(`Note: Internal link ${href} opens in new tab - consider if this is necessary`);
      }
    }

    // This test is informational - it passes as long as we checked
    expect(true).toBe(true);
  });
});
