// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Test Suite: Broken Links Validation
 *
 * Verifies no broken links exist on the page:
 * - No empty href attributes
 * - Internal anchor links point to existing elements
 * - External links return valid HTTP responses
 */

test.describe('Broken Links Validation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: No empty href attributes
   * Input: Scan all href attributes
   * Expected: No empty href attributes
   */
  test('should have no empty href attributes', async ({ page }) => {
    // Get all anchor elements
    const allLinks = await page.locator('a[href]').all();
    const emptyHrefLinks = [];

    for (const link of allLinks) {
      const href = await link.getAttribute('href');
      const text = await link.textContent();

      // Check for empty, whitespace-only, or just "#" href
      if (!href || href.trim() === '' || href === '#') {
        emptyHrefLinks.push({
          text: text?.trim() || '[no text]',
          href: href || '[empty]'
        });
      }
    }

    // Assert no empty hrefs
    expect(emptyHrefLinks, `Found empty href attributes: ${JSON.stringify(emptyHrefLinks)}`).toHaveLength(0);
  });

  /**
   * Test Case 2: Internal anchor links point to existing elements
   * Input: Check internal anchor links
   * Expected: Internal anchor links (#section) point to existing elements
   */
  test('should have valid internal anchor links pointing to existing elements', async ({ page }) => {
    // Get all internal anchor links (starting with #)
    const internalLinks = await page.locator('a[href^="#"]').all();
    const brokenAnchors = [];
    let validCount = 0;

    for (const link of internalLinks) {
      const href = await link.getAttribute('href');
      const text = await link.textContent();

      // Skip if just "#" (tested separately)
      if (href === '#') {
        continue;
      }

      // Extract target id (remove #)
      const targetId = href.substring(1);

      // Check if element with this id exists (use data-testid or id attribute matching)
      const targetElement = await page.locator(`[id="${targetId}"]`).count();

      if (targetElement > 0) {
        validCount++;
      } else {
        brokenAnchors.push({
          text: text?.trim() || '[no text]',
          href: href,
          targetId: targetId
        });
      }
    }

    // Assert no broken anchors
    expect(brokenAnchors, `Found broken anchor links: ${JSON.stringify(brokenAnchors)}`).toHaveLength(0);

    // Log success info
    console.log(`Validated ${validCount} internal anchor links`);
  });

  /**
   * Test Case 3: External links return valid HTTP responses
   * Input: Verify external link validity
   * Expected: External links return 200 OK status
   *
   * Note: This is an integration test that makes real HTTP requests
   */
  test('should have external links that return valid HTTP responses', async ({ page, request }) => {
    // Get all external links (http/https)
    const externalLinks = await page.locator('a[href^="https://"], a[href^="http://"]').all();
    const linkResults = [];
    const uniqueUrls = new Set();

    // Collect unique URLs to avoid duplicate requests
    for (const link of externalLinks) {
      const href = await link.getAttribute('href');
      if (href) {
        uniqueUrls.add(href);
      }
    }

    // Test each unique URL
    for (const url of uniqueUrls) {
      try {
        const response = await request.head(url, {
          timeout: 10000, // 10 second timeout
          ignoreHTTPSErrors: true
        });

        linkResults.push({
          url: url,
          status: response.status(),
          ok: response.ok()
        });
      } catch (error) {
        // If HEAD fails, try GET (some servers don't support HEAD)
        try {
          const response = await request.get(url, {
            timeout: 10000,
            ignoreHTTPSErrors: true
          });

          linkResults.push({
            url: url,
            status: response.status(),
            ok: response.ok()
          });
        } catch (getError) {
          linkResults.push({
            url: url,
            status: 0,
            ok: false,
            error: getError.message
          });
        }
      }
    }

    // Filter failed links
    const failedLinks = linkResults.filter(result => !result.ok);

    // Log results
    console.log(`Checked ${linkResults.length} unique external URLs`);
    linkResults.forEach(result => {
      console.log(`  ${result.ok ? '✓' : '✗'} ${result.url} - ${result.status}`);
    });

    // Assert all links are valid (2xx status)
    expect(failedLinks, `Found broken external links: ${JSON.stringify(failedLinks)}`).toHaveLength(0);
  });

  /**
   * Additional: Verify anchor links scroll to correct sections
   */
  test('should scroll to correct section when clicking internal anchor links', async ({ page }) => {
    // Test navigation to features section
    const featuresLink = page.locator('a[href="#features"]').first();
    await featuresLink.click();

    // Wait for scroll to complete
    await page.waitForTimeout(500);

    // Verify the features section is in viewport
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();
    await expect(featuresSection).toBeInViewport();

    // Test navigation to getting-started section
    const gettingStartedLink = page.locator('a[href="#getting-started"]').first();
    await gettingStartedLink.click();

    // Wait for scroll to complete
    await page.waitForTimeout(500);

    // Verify the getting-started section is in viewport
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();
    await expect(gettingStartedSection).toBeInViewport();
  });
});
