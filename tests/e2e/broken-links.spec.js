// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Test Suite: Broken Links Validation
 *
 * Scenario: Error Handling - Broken Links
 * Verifies no broken links exist on the page:
 * 1. No empty href attributes
 * 2. Internal anchor links point to existing elements
 * 3. External links return 200 OK status
 */

test.describe('Broken Links Validation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Scan all href attributes
   * Input: Scan all href attributes
   * Expected: No empty href attributes
   */
  test('should have no empty href attributes', async ({ page }) => {
    // Get all anchor elements with href attribute
    const links = await page.locator('a[href]').all();

    expect(links.length).toBeGreaterThan(0);

    const emptyHrefLinks = [];

    for (const link of links) {
      const href = await link.getAttribute('href');
      const text = await link.textContent();

      // Check for empty, whitespace-only, or just "#" href values
      if (!href || href.trim() === '' || href === '#') {
        emptyHrefLinks.push({
          text: text?.trim() || '[no text]',
          href: href || '[empty]'
        });
      }
    }

    // Verify no empty href links
    expect(emptyHrefLinks).toHaveLength(0);
  });

  /**
   * Test Case 2: Check internal anchor links
   * Input: Check internal anchor links
   * Expected: Internal anchor links (#section) point to existing elements
   */
  test('should have internal anchor links pointing to existing elements', async ({ page }) => {
    // Get all internal anchor links (starting with #)
    const anchorLinks = await page.locator('a[href^="#"]').all();

    const brokenLinks = [];

    for (const link of anchorLinks) {
      const href = await link.getAttribute('href');
      const text = await link.textContent();

      // Skip lone "#" - that's checked in test 1
      if (href === '#') continue;

      // Extract the target ID
      const targetId = href.substring(1);

      // Check if element with that ID exists
      // Use escaped selector for special characters in IDs
      const escapedId = targetId.replace(/([!"#$%&'()*+,./:;<=>?@[\\\]^`{|}~])/g, '\\$1');
      const targetElement = page.locator(`#${escapedId}`);
      const count = await targetElement.count();

      if (count === 0) {
        brokenLinks.push({
          text: text?.trim() || '[no text]',
          href: href,
          targetId: targetId
        });
      }
    }

    // Verify no broken anchor links
    expect(brokenLinks).toHaveLength(0);
  });

  /**
   * Test Case 3: Verify external link validity
   * Input: Verify external link validity
   * Expected: External links return 200 OK status
   */
  test('should have valid external links that return 200 OK status', async ({ page, request }) => {
    // Get all external links (http:// or https://)
    const externalLinks = await page.locator('a[href^="https://"], a[href^="http://"]').all();

    expect(externalLinks.length).toBeGreaterThan(0);

    // Collect unique URLs to avoid duplicate requests
    const uniqueUrls = new Set();

    for (const link of externalLinks) {
      const href = await link.getAttribute('href');
      if (href) {
        uniqueUrls.add(href);
      }
    }

    const brokenLinks = [];
    const validLinks = [];

    // Check each unique URL
    for (const url of uniqueUrls) {
      try {
        const response = await request.head(url, {
          timeout: 10000,
          ignoreHTTPSErrors: true
        });

        // Accept 2xx and 3xx status codes as valid
        if (response.status() >= 200 && response.status() < 400) {
          validLinks.push({ url, status: response.status() });
        } else {
          brokenLinks.push({ url, status: response.status() });
        }
      } catch (error) {
        // If HEAD fails, try GET request as some servers don't support HEAD
        try {
          const response = await request.get(url, {
            timeout: 10000,
            ignoreHTTPSErrors: true
          });

          if (response.status() >= 200 && response.status() < 400) {
            validLinks.push({ url, status: response.status() });
          } else {
            brokenLinks.push({ url, status: response.status() });
          }
        } catch (getError) {
          brokenLinks.push({ url, status: 'error', error: getError.message });
        }
      }
    }

    // Log the results
    console.log(`\nExternal Link Validation Results:`);
    console.log(`  Valid links: ${validLinks.length}`);
    console.log(`  Broken links: ${brokenLinks.length}`);

    if (brokenLinks.length > 0) {
      console.log('\nBroken links:');
      brokenLinks.forEach(link => {
        console.log(`  - ${link.url} (status: ${link.status})`);
      });
    }

    // Verify no broken external links
    expect(brokenLinks).toHaveLength(0);
  });

  /**
   * Additional Test: Verify all links are properly formatted
   */
  test('should have all links with proper format', async ({ page }) => {
    const allLinks = await page.locator('a[href]').all();

    const malformedLinks = [];

    for (const link of allLinks) {
      const href = await link.getAttribute('href');
      const text = await link.textContent();

      if (href) {
        // Check for malformed URLs
        // Valid patterns: #anchor, /path, http://, https://, mailto:, tel:
        const validPatterns = [
          /^#/, // anchor links
          /^\//, // absolute paths
          /^https?:\/\//, // http/https URLs
          /^mailto:/, // email links
          /^tel:/, // telephone links
          /^javascript:void/, // placeholder links (less ideal but valid)
        ];

        const isValid = validPatterns.some(pattern => pattern.test(href));

        if (!isValid) {
          malformedLinks.push({
            text: text?.trim() || '[no text]',
            href: href
          });
        }
      }
    }

    // Verify no malformed links
    expect(malformedLinks).toHaveLength(0);
  });
});
