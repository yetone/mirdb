// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Test Suite: Link Validation
 * Scenario: Verify all links on the page are functional and lead to valid destinations
 * This includes internal anchor links, external links, and proper link attributes
 */

test.describe('Link Validation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Check all internal anchor links
   * Input: Check all internal anchor links
   * Expected: All internal links (e.g., #features, #getting-started) navigate to existing sections
   * Type: e2e
   */
  test('TC1: All internal anchor links navigate to existing sections', async ({ page }) => {
    // Collect all internal anchor links (href starts with #)
    const internalLinks = page.locator('a[href^="#"]');
    const linkCount = await internalLinks.count();

    // We should have at least some internal links
    expect(linkCount).toBeGreaterThan(0);

    // Collect all unique anchor targets
    const anchorTargets = new Set();
    for (let i = 0; i < linkCount; i++) {
      const href = await internalLinks.nth(i).getAttribute('href');
      if (href && href !== '#') {
        anchorTargets.add(href);
      }
    }

    // Verify each internal link points to an existing section
    for (const anchor of anchorTargets) {
      const targetId = anchor.replace('#', '');
      const targetSection = page.locator(`#${targetId}`);
      await expect(targetSection, `Section ${anchor} should exist`).toBeVisible();
    }

    // Test clicking each unique anchor link
    for (const anchor of anchorTargets) {
      // Find the first link with this anchor
      const link = page.locator(`a[href="${anchor}"]`).first();
      await expect(link).toBeVisible();

      // Click the link
      await link.click();

      // Wait for scroll animation
      await page.waitForTimeout(300);

      // Verify the target section is visible and near the top of viewport
      const targetId = anchor.replace('#', '');
      const targetSection = page.locator(`#${targetId}`);
      await expect(targetSection).toBeVisible();

      // The target section should be near the top of the viewport
      const sectionBox = await targetSection.boundingBox();
      expect(sectionBox).not.toBeNull();
      expect(sectionBox.y).toBeLessThan(250); // Allow some tolerance for sticky headers

      // Go back to top for next iteration
      await page.evaluate(() => window.scrollTo(0, 0));
      await page.waitForTimeout(100);
    }
  });

  /**
   * Test Case 2: Verify GitHub link is valid
   * Input: Verify GitHub link is valid
   * Expected: GitHub repository link returns 200 status code
   * Type: integration
   */
  test('TC2: GitHub repository link returns 200 status code', async ({ page, request }) => {
    // Find the GitHub link in the navigation
    const githubLink = page.locator('nav.main-nav .github-link');
    await expect(githubLink).toBeVisible();

    // Get the href
    const href = await githubLink.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href).toContain('github.com');

    // Make a HEAD request to verify the link is valid
    const response = await request.head(href);
    expect(response.status()).toBe(200);
  });

  /**
   * Test Case 3: Check for broken links
   * Input: Check for broken links
   * Expected: No links return 404 or other error status codes
   * Type: integration
   */
  test('TC3: No links return 404 or other error status codes', async ({ page, request }) => {
    // Get all links on the page
    const allLinks = page.locator('a[href]');
    const linkCount = await allLinks.count();

    expect(linkCount).toBeGreaterThan(0);

    // Collect unique external URLs to test
    const externalUrls = new Set();
    for (let i = 0; i < linkCount; i++) {
      const href = await allLinks.nth(i).getAttribute('href');
      // Only check external URLs (http/https)
      if (href && (href.startsWith('http://') || href.startsWith('https://'))) {
        externalUrls.add(href);
      }
    }

    // Verify each external URL returns a valid status code (not 4xx or 5xx)
    const brokenLinks = [];
    for (const url of externalUrls) {
      try {
        const response = await request.head(url, {
          timeout: 10000,
          ignoreHTTPSErrors: true
        });
        const status = response.status();
        if (status >= 400) {
          brokenLinks.push({ url, status });
        }
      } catch (error) {
        // If HEAD fails, try GET
        try {
          const response = await request.get(url, {
            timeout: 10000,
            ignoreHTTPSErrors: true
          });
          const status = response.status();
          if (status >= 400) {
            brokenLinks.push({ url, status });
          }
        } catch (getError) {
          brokenLinks.push({ url, status: 'unreachable', error: getError.message });
        }
      }
    }

    // Assert no broken links found
    expect(brokenLinks, `Broken links found: ${JSON.stringify(brokenLinks)}`).toHaveLength(0);
  });

  /**
   * Test Case 4: Verify external links open in new tab
   * Input: Verify external links open in new tab
   * Expected: External links have target='_blank' and rel='noopener noreferrer'
   * Type: unit
   */
  test('TC4: External links have target=_blank and rel=noopener noreferrer', async ({ page }) => {
    // Get all external links (http/https)
    const externalLinks = page.locator('a[href^="http"]');
    const linkCount = await externalLinks.count();

    // We should have at least some external links
    expect(linkCount).toBeGreaterThan(0);

    // Verify each external link has proper attributes
    const linksWithoutProperAttributes = [];
    for (let i = 0; i < linkCount; i++) {
      const link = externalLinks.nth(i);
      const href = await link.getAttribute('href');
      const target = await link.getAttribute('target');
      const rel = await link.getAttribute('rel');

      // Check target='_blank'
      if (target !== '_blank') {
        linksWithoutProperAttributes.push({
          href,
          issue: `missing or incorrect target attribute (got: ${target})`
        });
      }

      // Check rel contains 'noopener' and 'noreferrer'
      if (!rel || !rel.includes('noopener') || !rel.includes('noreferrer')) {
        linksWithoutProperAttributes.push({
          href,
          issue: `missing or incorrect rel attribute (got: ${rel})`
        });
      }
    }

    // Assert all external links have proper attributes
    expect(
      linksWithoutProperAttributes,
      `External links with improper attributes: ${JSON.stringify(linksWithoutProperAttributes)}`
    ).toHaveLength(0);
  });

  /**
   * Additional test: Verify specific internal anchor links exist and work
   */
  test('Specific navigation anchor links work correctly', async ({ page }) => {
    // Define expected anchor links based on the navigation
    const expectedAnchors = [
      { anchor: '#features', expectedHeading: 'Features' },
      { anchor: '#getting-started', expectedHeading: 'Getting Started' },
      { anchor: '#documentation', expectedHeading: 'Documentation' }
    ];

    for (const { anchor, expectedHeading } of expectedAnchors) {
      // Find and click the navigation link
      const navLink = page.locator(`.nav-link[href="${anchor}"]`);
      await expect(navLink, `Navigation link for ${anchor} should exist`).toBeVisible();

      await navLink.click();
      await page.waitForTimeout(300);

      // Verify the section heading is visible
      const targetId = anchor.replace('#', '');
      const heading = page.locator(`#${targetId} h2`);
      await expect(heading).toBeVisible();
      await expect(heading).toHaveText(expectedHeading);

      // Reset scroll
      await page.evaluate(() => window.scrollTo(0, 0));
      await page.waitForTimeout(100);
    }
  });

  /**
   * Additional test: All links have non-empty href
   */
  test('All links have non-empty href attributes', async ({ page }) => {
    const allLinks = page.locator('a');
    const linkCount = await allLinks.count();

    expect(linkCount).toBeGreaterThan(0);

    const emptyHrefLinks = [];
    for (let i = 0; i < linkCount; i++) {
      const link = allLinks.nth(i);
      const href = await link.getAttribute('href');

      // Check for empty or null href
      if (!href || href.trim() === '') {
        const text = await link.textContent();
        emptyHrefLinks.push({ text: text?.trim() || 'unknown', index: i });
      }
    }

    expect(
      emptyHrefLinks,
      `Links with empty href: ${JSON.stringify(emptyHrefLinks)}`
    ).toHaveLength(0);
  });

  /**
   * Additional test: Internal links don't have target=_blank
   */
  test('Internal anchor links do not have target=_blank', async ({ page }) => {
    const internalLinks = page.locator('a[href^="#"]');
    const linkCount = await internalLinks.count();

    for (let i = 0; i < linkCount; i++) {
      const link = internalLinks.nth(i);
      const href = await link.getAttribute('href');
      const target = await link.getAttribute('target');

      // Internal links should not open in new tab
      if (target === '_blank') {
        throw new Error(`Internal link ${href} should not have target="_blank"`);
      }
    }
  });

  /**
   * Additional test: Links are keyboard accessible and focusable
   */
  test('All links are keyboard accessible', async ({ page }) => {
    const allLinks = page.locator('a[href]');
    const linkCount = await allLinks.count();

    expect(linkCount).toBeGreaterThan(0);

    // Test that at least the first few links can be focused via keyboard
    const linksToTest = Math.min(linkCount, 5);
    for (let i = 0; i < linksToTest; i++) {
      const link = allLinks.nth(i);
      await link.scrollIntoViewIfNeeded();
      await link.focus();
      await expect(link).toBeFocused();
    }
  });
});
