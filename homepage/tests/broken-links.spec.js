// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Broken Links Error Handling Tests
 *
 * This test suite verifies that all links on the MirDB homepage are valid
 * and do not result in errors or broken navigation.
 *
 * Scenario: Error Handling - Broken Links
 * - Test all internal anchor links navigate correctly
 * - Test all external links return valid responses
 * - Verify no 404 errors occur
 */

test.describe('Error Handling - Broken Links', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Click all internal anchor links
   * Expected: All internal links navigate to correct sections
   * Type: e2e
   */
  test('should navigate to correct sections when clicking internal anchor links', async ({ page }) => {
    // Find all internal anchor links (starting with #)
    const internalLinks = await page.locator('a[href^="#"]').all();

    expect(internalLinks.length).toBeGreaterThan(0);

    const linkResults = [];

    for (const link of internalLinks) {
      const href = await link.getAttribute('href');
      const linkText = await link.textContent();

      // Skip skip-link which may not be visible
      if (href === '#main-content') {
        // Verify main content target exists
        const targetElement = page.locator('#main-content');
        const targetExists = await targetElement.count() > 0;
        linkResults.push({
          href,
          linkText: linkText?.trim(),
          status: targetExists ? 'pass' : 'fail',
          error: targetExists ? null : 'Target element #main-content not found'
        });
        continue;
      }

      // Verify the link is visible before clicking
      const isVisible = await link.isVisible();
      if (!isVisible) {
        // Some links may be hidden (skip link) - just verify target exists
        const targetId = href.replace('#', '');
        const targetElement = page.locator(`#${targetId}, [id="${targetId}"]`);
        const targetExists = await targetElement.count() > 0;
        linkResults.push({
          href,
          linkText: linkText?.trim(),
          status: targetExists ? 'pass' : 'fail',
          error: targetExists ? null : `Target element ${href} not found`
        });
        continue;
      }

      // Click the link
      await link.click();

      // Wait for navigation/scroll
      await page.waitForTimeout(500);

      // Verify URL contains the expected hash
      const url = page.url();
      expect(url, `Link "${href}" should update URL hash`).toContain(href);

      // Verify target section exists
      const targetId = href.replace('#', '');
      const targetElement = page.locator(`#${targetId}, [id="${targetId}"]`);
      const targetExists = await targetElement.count() > 0;

      expect(targetExists, `Target element for "${href}" should exist`).toBe(true);

      linkResults.push({
        href,
        linkText: linkText?.trim(),
        status: targetExists && url.includes(href) ? 'pass' : 'fail',
        error: null
      });

      // Go back to top for next link test
      await page.goto('/');
    }

    // Verify all internal links passed
    const failedLinks = linkResults.filter(r => r.status === 'fail');
    expect(failedLinks.length, `Failed links: ${JSON.stringify(failedLinks)}`).toBe(0);

    console.log(`Verified ${linkResults.length} internal anchor links`);
  });

  /**
   * Test Case 2: Verify GitHub repository link validity
   * Expected: GitHub link returns 200 response and displays repository
   * Type: integration
   */
  test('should have valid GitHub repository link that returns 200', async ({ page, request }) => {
    // Find the main GitHub link
    const githubLinks = await page.locator('a[href*="github.com/yetone/mirdb"]').all();

    expect(githubLinks.length).toBeGreaterThan(0);

    // Get the main repository URL (not issues or readme)
    const mainGithubLink = await page.locator('a[href="https://github.com/yetone/mirdb"]').first();
    const href = await mainGithubLink.getAttribute('href');

    expect(href).toBe('https://github.com/yetone/mirdb');

    // Test the URL validity by making a HEAD request
    const response = await request.head(href);

    // Verify successful response (2xx status)
    expect(response.status(), `GitHub link ${href} should return successful status`).toBeLessThan(400);
    expect(response.status()).toBeGreaterThanOrEqual(200);

    console.log(`GitHub repository link ${href} returned status ${response.status()}`);
  });

  /**
   * Test Case 3: Verify documentation link validity
   * Expected: Documentation link returns valid page
   * Type: integration
   */
  test('should have valid Documentation link that returns valid response', async ({ page, request }) => {
    // Find the Documentation link (points to GitHub readme)
    const docsLink = page.locator('a:has-text("Documentation")').first();
    await expect(docsLink).toBeVisible();

    const href = await docsLink.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href).toContain('github.com/yetone/mirdb');

    // Test the URL validity by making a HEAD request
    const response = await request.head(href);

    // Verify successful response (2xx status)
    expect(response.status(), `Documentation link ${href} should return successful status`).toBeLessThan(400);
    expect(response.status()).toBeGreaterThanOrEqual(200);

    console.log(`Documentation link ${href} returned status ${response.status()}`);
  });

  /**
   * Test Case 4: Check for 404 errors
   * Expected: No internal links result in 404 errors
   * Type: e2e
   */
  test('should not have any internal links that result in 404 errors', async ({ page }) => {
    // Collect all internal anchor links
    const internalLinks = await page.locator('a[href^="#"]').all();

    expect(internalLinks.length).toBeGreaterThan(0);

    const brokenLinks = [];

    for (const link of internalLinks) {
      const href = await link.getAttribute('href');
      const linkText = await link.textContent();

      // Extract the target ID
      const targetId = href.replace('#', '');

      // Check if target element exists in the DOM
      const targetElement = page.locator(`#${targetId}, [id="${targetId}"]`);
      const targetCount = await targetElement.count();

      if (targetCount === 0) {
        brokenLinks.push({
          href,
          linkText: linkText?.trim(),
          error: `Target element with id="${targetId}" not found (would result in 404-like behavior)`
        });
      }
    }

    // Also check internal page links (href="/..." or relative paths without protocol)
    const internalPageLinks = await page.locator('a[href^="/"]').all();

    for (const link of internalPageLinks) {
      const href = await link.getAttribute('href');
      const linkText = await link.textContent();

      // Skip the root path
      if (href === '/') continue;

      // These would result in 404 on a static single-page site
      // unless there's a corresponding file or the server handles it
      // For this single-page site, only "/" should be valid
      brokenLinks.push({
        href,
        linkText: linkText?.trim(),
        error: `Internal page link "${href}" may result in 404 on static hosting`
      });
    }

    // Verify no broken internal links
    expect(brokenLinks.length, `Broken links found: ${JSON.stringify(brokenLinks, null, 2)}`).toBe(0);

    console.log(`Verified ${internalLinks.length} internal links have valid targets (no 404 errors)`);
  });

  /**
   * Additional Test: Verify all external links return valid responses
   */
  test('should have all external links return valid HTTP responses', async ({ page, request }) => {
    // Find all external links
    const externalLinks = await page.locator('a[href^="http"], a[href^="https"]').all();

    expect(externalLinks.length).toBeGreaterThan(0);

    const linkResults = [];
    const uniqueUrls = new Set();

    // Collect unique URLs to avoid duplicate requests
    for (const link of externalLinks) {
      const href = await link.getAttribute('href');
      uniqueUrls.add(href);
    }

    // Test each unique external URL
    for (const url of uniqueUrls) {
      try {
        const response = await request.head(url, {
          timeout: 10000, // 10 second timeout
          ignoreHTTPSErrors: true
        });

        const status = response.status();

        // Accept 2xx and 3xx as valid (redirects are okay)
        const isValid = status >= 200 && status < 400;

        linkResults.push({
          url,
          status,
          valid: isValid,
          error: isValid ? null : `HTTP ${status}`
        });
      } catch (error) {
        linkResults.push({
          url,
          status: null,
          valid: false,
          error: error.message
        });
      }
    }

    // Check for any invalid links
    const invalidLinks = linkResults.filter(r => !r.valid);

    // Log results
    console.log(`Tested ${linkResults.length} unique external URLs`);
    console.log(`Valid: ${linkResults.filter(r => r.valid).length}`);
    console.log(`Invalid: ${invalidLinks.length}`);

    if (invalidLinks.length > 0) {
      console.log('Invalid links:', JSON.stringify(invalidLinks, null, 2));
    }

    // All external links should be valid
    expect(invalidLinks.length, `Invalid external links: ${JSON.stringify(invalidLinks)}`).toBe(0);
  });

  /**
   * Additional Test: Verify anchor links have corresponding target sections
   */
  test('should have all navigation anchor links point to existing sections', async ({ page }) => {
    // Get navigation anchor links
    const navLinks = await page.locator('.nav-links a[href^="#"], .hero a[href^="#"]').all();

    expect(navLinks.length).toBeGreaterThan(0);

    for (const link of navLinks) {
      const href = await link.getAttribute('href');
      const linkText = await link.textContent();
      const targetId = href.replace('#', '');

      // Verify the target section exists
      const targetSection = page.locator(`#${targetId}`);
      const sectionExists = await targetSection.count() > 0;

      expect(
        sectionExists,
        `Navigation link "${linkText?.trim()}" (${href}) should point to an existing section`
      ).toBe(true);

      // Verify the section is a proper landmark or content area
      if (sectionExists) {
        const targetElement = await targetSection.first();
        const tagName = await targetElement.evaluate(el => el.tagName.toLowerCase());

        // Target should be a section, div, main, or similar container
        const validTags = ['section', 'div', 'main', 'article', 'aside', 'nav'];
        expect(
          validTags.includes(tagName),
          `Target "${targetId}" should be a content container (got ${tagName})`
        ).toBe(true);
      }
    }
  });

  /**
   * Additional Test: Verify footer links are valid
   */
  test('should have valid footer links', async ({ page, request }) => {
    // Scroll to footer
    const footer = page.locator('footer');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Get all footer links
    const footerLinks = await footer.locator('a').all();

    expect(footerLinks.length).toBeGreaterThan(0);

    for (const link of footerLinks) {
      const href = await link.getAttribute('href');
      const linkText = await link.textContent();

      // External links should be tested
      if (href.startsWith('http')) {
        try {
          const response = await request.head(href, {
            timeout: 10000,
            ignoreHTTPSErrors: true
          });

          expect(
            response.status(),
            `Footer link "${linkText?.trim()}" (${href}) should return valid status`
          ).toBeLessThan(400);
        } catch (error) {
          // If HEAD fails, try GET (some servers don't support HEAD)
          try {
            const response = await request.get(href, {
              timeout: 10000,
              ignoreHTTPSErrors: true
            });

            expect(
              response.status(),
              `Footer link "${linkText?.trim()}" (${href}) should return valid status`
            ).toBeLessThan(400);
          } catch (getError) {
            throw new Error(`Footer link "${href}" failed: ${getError.message}`);
          }
        }
      }
    }
  });

  /**
   * Additional Test: Verify GitHub Issues link is valid
   */
  test('should have valid GitHub Issues link', async ({ page, request }) => {
    // Find the Issues link in footer
    const issuesLink = page.locator('a[href*="github.com/yetone/mirdb/issues"]');
    const linkCount = await issuesLink.count();

    expect(linkCount).toBeGreaterThan(0);

    const href = await issuesLink.first().getAttribute('href');
    expect(href).toBe('https://github.com/yetone/mirdb/issues');

    // Verify the link returns a valid response
    const response = await request.head(href, {
      timeout: 10000,
      ignoreHTTPSErrors: true
    });

    expect(response.status(), `Issues link should return valid status`).toBeLessThan(400);

    console.log(`GitHub Issues link ${href} returned status ${response.status()}`);
  });
});
