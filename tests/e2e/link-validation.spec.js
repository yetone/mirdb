// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Helper function to escape CSS selectors (since CSS.escape is not available in Node.js)
 */
function cssEscape(str) {
  return str.replace(/([!"#$%&'()*+,./:;<=>?@[\\\]^`{|}~])/g, '\\$1');
}

/**
 * E2E tests for Link Validation
 * Scenario: Verify all links on the homepage are valid and functional
 */
test.describe('Link Validation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case 1: Check all internal anchor links
  test('TC1: all internal anchor links point to existing IDs on page', async ({ page }) => {
    // Get all internal anchor links
    const internalLinks = await page.locator('a[href^="#"]').all();
    const errors = [];

    for (const link of internalLinks) {
      const href = await link.getAttribute('href');
      if (href && href !== '#' && href !== '') {
        const targetId = href.substring(1);
        const targetElement = page.locator(`#${cssEscape(targetId)}`);
        const exists = await targetElement.count();

        if (exists === 0) {
          const linkText = await link.textContent();
          errors.push({ href, linkText, targetId });
        }
      }
    }

    expect(errors).toEqual([]);
  });

  // Test Case 2: Verify GitHub repository link is valid
  test('TC2: GitHub repository link returns 200 or redirects properly', async ({ request }) => {
    const response = await request.get('https://github.com/yetone/mirdb', {
      maxRedirects: 5,
    });

    // GitHub should return 200 or redirect to valid page
    expect([200, 301, 302]).toContain(response.status());
  });

  // Test Case 3: Check for empty href attributes
  test('TC3: no links have empty or hash-only href values', async ({ page }) => {
    const allLinks = await page.locator('a[href]').all();
    const invalidLinks = [];

    for (const link of allLinks) {
      const href = await link.getAttribute('href');
      if (href === '' || href === '#') {
        const linkText = await link.textContent();
        invalidLinks.push({ href, linkText });
      }
    }

    expect(invalidLinks).toEqual([]);
  });

  // Test Case 4: Verify documentation links work
  test('TC4: documentation links are valid and accessible', async ({ request }) => {
    // Test the main documentation link (GitHub README)
    const readmeResponse = await request.get('https://github.com/yetone/mirdb#readme', {
      maxRedirects: 5,
    });

    // Accept 200, redirects, or 429 (rate limiting)
    expect([200, 301, 302, 429]).toContain(readmeResponse.status());

    // Test the memcached protocol reference link
    const protocolResponse = await request.get(
      'https://github.com/memcached/memcached/blob/master/doc/protocol.txt',
      { maxRedirects: 5 }
    );

    // Accept 200, redirects, or 429 (rate limiting - GitHub may rate limit automated requests)
    expect([200, 301, 302, 429]).toContain(protocolResponse.status());
  });

  // Additional E2E tests for link functionality
  test('internal anchor link scrolls to correct section', async ({ page }) => {
    // Click on Get Started button
    const getStartedLink = page.locator('a[href="#quickstart"]');
    await expect(getStartedLink).toBeVisible();
    await getStartedLink.click();

    // Wait for scroll and verify quickstart section is visible in viewport
    await page.waitForTimeout(500); // Allow scroll animation
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeInViewport();
  });

  test('all GitHub links in page are valid URLs', async ({ page }) => {
    const githubLinks = await page.locator('a[href*="github.com"]').all();
    expect(githubLinks.length).toBeGreaterThan(0);

    // Validate GitHub link URL format
    const githubUrlPattern = /^https:\/\/github\.com\/[a-zA-Z0-9_-]+\/[a-zA-Z0-9_-]+(\/[^#]*)?(#.*)?$/;

    for (const link of githubLinks) {
      const href = await link.getAttribute('href');
      expect(href).toMatch(githubUrlPattern);
      // Verify link is pointing to the expected yetone/mirdb repo or memcached
      expect(href).toMatch(/github\.com\/(yetone\/mirdb|memcached\/memcached)/);
    }
  });

  test('footer links are all valid and clickable', async ({ page }) => {
    const footerLinks = await page.locator('footer a[href]').all();
    expect(footerLinks.length).toBeGreaterThan(0);

    for (const link of footerLinks) {
      await expect(link).toBeVisible();
      const href = await link.getAttribute('href');
      expect(href).toBeTruthy();
      expect(href).not.toBe('#');
      expect(href).not.toBe('');
    }
  });
});
