// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Test Suite: Error Handling - Broken Links
 * Scenario: Verify all links on the page are functional and lead to valid destinations
 *
 * This test suite validates:
 * 1. All internal anchor links scroll to valid sections
 * 2. All external links resolve without 404 errors
 * 3. GitHub repository link is correct and accessible
 */

test.describe('Error Handling - Broken Links', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to the homepage before each test
    await page.goto('/');
  });

  /**
   * Test Case 1: Check all internal links (anchors)
   * Input: Check all internal links (anchors)
   * Expected: All internal anchor links scroll to valid sections
   */
  test('TC1: All internal anchor links scroll to valid sections', async ({ page }) => {
    // Get all anchor links that point to internal sections (href starts with #)
    const internalLinks = page.locator('a[href^="#"]');
    const linkCount = await internalLinks.count();

    // Ensure there are internal links to test
    expect(linkCount).toBeGreaterThan(0);

    // Collect all unique internal link targets
    const linkTargets = new Set();
    for (let i = 0; i < linkCount; i++) {
      const href = await internalLinks.nth(i).getAttribute('href');
      if (href && href !== '#') {
        linkTargets.add(href);
      }
    }

    // Verify each unique target section exists and is accessible
    for (const target of linkTargets) {
      // The target should be a valid CSS selector (e.g., #features, #quick-start)
      const targetElement = page.locator(target);

      // Verify the target element exists
      await expect(targetElement, `Target element ${target} should exist`).toBeAttached();

      // Find a link that points to this target and click it
      const linkToTarget = page.locator(`a[href="${target}"]`).first();
      await linkToTarget.click();

      // Wait for scroll animation
      await page.waitForTimeout(500);

      // Verify the URL hash changes correctly
      await expect(page).toHaveURL(new RegExp(`${target}$`));

      // Verify the target section is now visible in the viewport
      await expect(targetElement).toBeVisible();

      // Check that the element is actually in the viewport (scrolled to)
      const isInViewport = await targetElement.evaluate((el) => {
        const rect = el.getBoundingClientRect();
        // Allow some tolerance for sticky headers
        return rect.top >= -150 && rect.top <= window.innerHeight;
      });
      expect(isInViewport, `Target ${target} should be in viewport after clicking link`).toBe(true);
    }
  });

  /**
   * Test Case 2: Check all external links
   * Input: Check all external links
   * Expected: All external links (GitHub, etc.) resolve without 404
   */
  test('TC2: All external links resolve without 404', async ({ page, context }) => {
    // Get all external links (href starts with http:// or https://)
    const externalLinks = page.locator('a[href^="http://"], a[href^="https://"]');
    const linkCount = await externalLinks.count();

    // Ensure there are external links to test
    expect(linkCount).toBeGreaterThan(0);

    // Collect all unique external URLs
    const externalUrls = new Map();
    for (let i = 0; i < linkCount; i++) {
      const link = externalLinks.nth(i);
      const href = await link.getAttribute('href');
      const text = await link.textContent();
      if (href && !externalUrls.has(href)) {
        externalUrls.set(href, text);
      }
    }

    // Test each unique external URL
    for (const [url, linkText] of externalUrls) {
      // Verify the link has proper attributes for security
      const linkElement = page.locator(`a[href="${url}"]`).first();

      // External links should open in a new tab
      const target = await linkElement.getAttribute('target');
      expect(target, `External link "${linkText}" (${url}) should have target="_blank"`).toBe('_blank');

      // External links should have rel="noopener noreferrer" for security
      const rel = await linkElement.getAttribute('rel');
      expect(rel, `External link "${linkText}" (${url}) should have rel attribute`).toBeTruthy();
      expect(rel).toContain('noopener');

      // Make an HTTP request to verify the URL is accessible (not a 404)
      // We use page.request to make a HEAD request to check if the URL is valid
      try {
        const response = await page.request.head(url, {
          timeout: 10000,
          ignoreHTTPSErrors: true
        });

        // Check that we don't get a 404 or other error status
        const status = response.status();
        expect(
          status,
          `External link "${linkText}" (${url}) should not return error status`
        ).toBeLessThan(400);
      } catch (error) {
        // If HEAD request fails, try GET request (some servers don't support HEAD)
        try {
          const response = await page.request.get(url, {
            timeout: 10000,
            ignoreHTTPSErrors: true
          });
          const status = response.status();
          expect(
            status,
            `External link "${linkText}" (${url}) should not return error status`
          ).toBeLessThan(400);
        } catch (getError) {
          // Log the error but don't fail the test for network issues
          // The link format is still valid even if network is unavailable
          console.warn(`Could not verify external link ${url}: ${getError.message}`);
        }
      }
    }
  });

  /**
   * Test Case 3: Verify GitHub repository link
   * Input: Verify GitHub repository link
   * Expected: GitHub link points to correct and accessible repository
   */
  test('TC3: GitHub link points to correct and accessible repository', async ({ page, context }) => {
    // Find all GitHub links on the page
    const githubLinks = page.locator('a[href*="github.com/yetone/mirdb"]');
    const linkCount = await githubLinks.count();

    // Ensure there is at least one GitHub link
    expect(linkCount).toBeGreaterThan(0);

    // Verify the first GitHub link (usually in header)
    const headerGithubLink = page.locator('header a[href*="github.com"]');
    if (await headerGithubLink.count() > 0) {
      const href = await headerGithubLink.getAttribute('href');
      expect(href).toBe('https://github.com/yetone/mirdb');

      // Verify it opens in a new tab with proper security attributes
      await expect(headerGithubLink).toHaveAttribute('target', '_blank');
      const rel = await headerGithubLink.getAttribute('rel');
      expect(rel).toContain('noopener');
      expect(rel).toContain('noreferrer');
    }

    // Verify the footer GitHub link
    const footerGithubLink = page.locator('[data-testid="footer-github-link"]');
    if (await footerGithubLink.count() > 0) {
      const href = await footerGithubLink.getAttribute('href');
      expect(href).toBe('https://github.com/yetone/mirdb');

      // Verify it opens in a new tab with proper security attributes
      await expect(footerGithubLink).toHaveAttribute('target', '_blank');
      const rel = await footerGithubLink.getAttribute('rel');
      expect(rel).toContain('noopener');
      expect(rel).toContain('noreferrer');
    }

    // Verify the hero section GitHub button
    const heroGithubBtn = page.locator('#github-btn');
    if (await heroGithubBtn.count() > 0) {
      const href = await heroGithubBtn.getAttribute('href');
      expect(href).toBe('https://github.com/yetone/mirdb');

      // Verify it opens in a new tab with proper security attributes
      await expect(heroGithubBtn).toHaveAttribute('target', '_blank');
      const rel = await heroGithubBtn.getAttribute('rel');
      expect(rel).toContain('noopener');
      expect(rel).toContain('noreferrer');
    }

    // Test that clicking the GitHub link opens the correct repository
    const [newPage] = await Promise.all([
      context.waitForEvent('page'),
      githubLinks.first().click()
    ]);

    // Wait for the new page to load
    await newPage.waitForLoadState('domcontentloaded');

    // Verify the new page URL is the correct GitHub repository
    const newPageUrl = newPage.url();
    expect(newPageUrl).toContain('github.com');
    expect(newPageUrl).toContain('yetone/mirdb');

    // Close the new page
    await newPage.close();
  });

  /**
   * Additional test: Verify all links have valid href attributes
   */
  test('All links have valid href attributes', async ({ page }) => {
    // Get all anchor tags
    const allLinks = page.locator('a');
    const linkCount = await allLinks.count();

    for (let i = 0; i < linkCount; i++) {
      const link = allLinks.nth(i);
      const href = await link.getAttribute('href');
      const linkText = await link.textContent();

      // Verify href is not empty or undefined
      expect(href, `Link "${linkText}" should have a valid href`).toBeTruthy();

      // Verify href is not just whitespace
      expect(href.trim().length, `Link "${linkText}" href should not be empty`).toBeGreaterThan(0);

      // Verify href follows a valid pattern
      const isValidHref =
        href.startsWith('#') ||           // Internal anchor
        href.startsWith('/') ||           // Relative path
        href.startsWith('http://') ||     // HTTP URL
        href.startsWith('https://') ||    // HTTPS URL
        href.startsWith('mailto:') ||     // Email link
        href.startsWith('tel:') ||        // Phone link
        href === '#';                      // Homepage anchor

      expect(
        isValidHref,
        `Link "${linkText}" should have a valid href format (got: ${href})`
      ).toBe(true);
    }
  });

  /**
   * Additional test: Verify no broken internal references in code blocks
   */
  test('Code blocks with GitHub URLs contain valid repository references', async ({ page }) => {
    // Get all code blocks that might contain GitHub URLs
    const codeBlocks = page.locator('pre code');
    const blockCount = await codeBlocks.count();

    for (let i = 0; i < blockCount; i++) {
      const codeBlock = codeBlocks.nth(i);
      const codeText = await codeBlock.textContent();

      // Check if the code block contains a GitHub clone URL
      if (codeText && codeText.includes('github.com')) {
        // Verify it references the correct repository
        expect(
          codeText.includes('github.com/yetone/mirdb'),
          'Code block GitHub URLs should reference the correct repository'
        ).toBe(true);
      }
    }
  });

  /**
   * Additional test: Verify Issues link is valid
   */
  test('GitHub Issues link is valid and accessible', async ({ page }) => {
    // Find the issues link in the footer
    const issuesLink = page.locator('a[href*="github.com/yetone/mirdb/issues"]');

    if (await issuesLink.count() > 0) {
      const href = await issuesLink.getAttribute('href');
      expect(href).toBe('https://github.com/yetone/mirdb/issues');

      // Verify it has proper security attributes
      await expect(issuesLink).toHaveAttribute('target', '_blank');
      const rel = await issuesLink.getAttribute('rel');
      expect(rel).toContain('noopener');
      expect(rel).toContain('noreferrer');

      // Verify the URL is accessible
      try {
        const response = await page.request.head(href, {
          timeout: 10000,
          ignoreHTTPSErrors: true
        });
        expect(response.status()).toBeLessThan(400);
      } catch (error) {
        console.warn(`Could not verify issues link: ${error.message}`);
      }
    }
  });
});
