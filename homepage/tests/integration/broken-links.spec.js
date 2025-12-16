// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Integration Tests for Broken Links
 * Scenario: Verify that all links return valid HTTP responses (no 404s) as per REQ-6
 */

test.describe('Broken Links Check', () => {
  /**
   * Test Case 5: Check for broken links
   * Input: Check for broken links
   * Expected: All links return valid HTTP responses (no 404s)
   */
  test('TC5: All external links return valid HTTP responses (no 404s)', async ({ page, request }) => {
    // Navigate to homepage
    await page.goto('/');

    // Get all external links
    const externalLinks = page.locator('a[href^="http"]');
    const count = await externalLinks.count();

    // Collect all unique hrefs
    const hrefs = new Set();
    for (let i = 0; i < count; i++) {
      const href = await externalLinks.nth(i).getAttribute('href');
      if (href) {
        hrefs.add(href);
      }
    }

    // Verify we have external links to test
    expect(hrefs.size).toBeGreaterThan(0);

    // Test each unique URL
    const results = [];
    for (const href of hrefs) {
      try {
        const response = await request.head(href, {
          timeout: 10000,
          ignoreHTTPSErrors: true
        });

        results.push({
          url: href,
          status: response.status(),
          ok: response.ok() || response.status() === 301 || response.status() === 302 || response.status() === 307 || response.status() === 308
        });
      } catch (error) {
        // If HEAD fails, try GET request
        try {
          const response = await request.get(href, {
            timeout: 10000,
            ignoreHTTPSErrors: true
          });

          results.push({
            url: href,
            status: response.status(),
            ok: response.ok() || response.status() === 301 || response.status() === 302 || response.status() === 307 || response.status() === 308
          });
        } catch (getError) {
          results.push({
            url: href,
            status: 0,
            ok: false,
            error: getError.message
          });
        }
      }
    }

    // Log results for debugging
    console.log('Link check results:');
    results.forEach(r => {
      console.log(`  ${r.ok ? '✓' : '✗'} ${r.url} - Status: ${r.status}${r.error ? ` Error: ${r.error}` : ''}`);
    });

    // Verify no broken links (404s or request failures)
    const brokenLinks = results.filter(r => !r.ok);
    expect(brokenLinks, `Found ${brokenLinks.length} broken link(s): ${brokenLinks.map(l => `${l.url} (${l.status})`).join(', ')}`).toHaveLength(0);
  });

  /**
   * Test Case 5b: Verify internal anchor links point to existing elements
   */
  test('TC5b: All internal anchor links point to existing elements', async ({ page }) => {
    // Navigate to homepage
    await page.goto('/');

    // Get all internal anchor links
    const internalLinks = page.locator('a[href^="#"]');
    const count = await internalLinks.count();

    // Verify we have internal links to test
    expect(count).toBeGreaterThan(0);

    // Collect all anchor targets
    const anchors = [];
    for (let i = 0; i < count; i++) {
      const href = await internalLinks.nth(i).getAttribute('href');
      if (href && href.startsWith('#') && href.length > 1) {
        anchors.push(href);
      }
    }

    // Verify each anchor target exists in the DOM
    const missingTargets = [];
    for (const anchor of anchors) {
      const targetElement = page.locator(anchor);
      const exists = await targetElement.count() > 0;
      if (!exists) {
        missingTargets.push(anchor);
      }
    }

    // Verify no missing anchor targets
    expect(missingTargets, `Found ${missingTargets.length} broken anchor link(s): ${missingTargets.join(', ')}`).toHaveLength(0);
  });

  /**
   * Test Case 5c: Verify GitHub repository URL format is correct
   */
  test('TC5c: GitHub URLs have correct format', async ({ page }) => {
    // Navigate to homepage
    await page.goto('/');

    // Get all GitHub links
    const githubLinks = page.locator('a[href*="github.com/yetone/mirdb"]');
    const count = await githubLinks.count();

    // Verify we have GitHub links
    expect(count).toBeGreaterThan(0);

    for (let i = 0; i < count; i++) {
      const href = await githubLinks.nth(i).getAttribute('href');

      // Verify URL format
      expect(href).toMatch(/^https:\/\/github\.com\/yetone\/mirdb/);

      // Verify it's a valid GitHub URL pattern
      expect(href).not.toContain('//github.com//');
      expect(href).not.toContain('github.com/yetone/mirdb/yetone');
    }
  });

  /**
   * Test: Verify no javascript: URLs exist
   */
  test('TC-NoJavaScript: No javascript: URLs in links', async ({ page }) => {
    await page.goto('/');

    const jsLinks = page.locator('a[href^="javascript:"]');
    const count = await jsLinks.count();

    expect(count, 'Should not have any javascript: URLs in links').toBe(0);
  });

  /**
   * Test: Verify no mailto: links without proper email
   */
  test('TC-MailtoValid: No malformed mailto: links', async ({ page }) => {
    await page.goto('/');

    const mailtoLinks = page.locator('a[href^="mailto:"]');
    const count = await mailtoLinks.count();

    for (let i = 0; i < count; i++) {
      const href = await mailtoLinks.nth(i).getAttribute('href');
      // If mailto links exist, verify they have valid email format
      if (href) {
        const email = href.replace('mailto:', '').split('?')[0];
        expect(email).toMatch(/.+@.+\..+/);
      }
    }
  });
});
