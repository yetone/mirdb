const { test, expect } = require('@playwright/test');

/**
 * External Link Validation Tests
 * Scenario: Validate all external links open correctly and have proper attributes
 *
 * Test Cases:
 * 1. GitHub links have target='_blank' and rel='noopener noreferrer' (unit test)
 * 2. All external links return 200 status codes (integration test)
 */

// Define all expected external links on the page
const EXPECTED_EXTERNAL_LINKS = [
  { selector: 'header nav a[href*="github.com"]', url: 'https://github.com/yetone/mirdb', description: 'Header GitHub link' },
  { selector: '.hero-cta a[href*="github.com"]', url: 'https://github.com/yetone/mirdb', description: 'Hero GitHub button' },
  { selector: '.badge-container a[href*="circleci.com"]', url: 'https://circleci.com/gh/yetone/mirdb', description: 'CircleCI badge link' },
  { selector: 'footer a[href="https://github.com/yetone/mirdb"]', url: 'https://github.com/yetone/mirdb', description: 'Footer GitHub link' },
  { selector: 'footer a[href*="github.com/yetone/mirdb/issues"]', url: 'https://github.com/yetone/mirdb/issues', description: 'Footer Issues link' },
];

test.describe('External Link Validation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('Test Case 1: GitHub links have proper security attributes', () => {
    test('All external links have target="_blank" attribute', async ({ page }) => {
      // Find all external links (links starting with http:// or https://)
      const externalLinks = page.locator('a[href^="http://"], a[href^="https://"]');
      const count = await externalLinks.count();

      expect(count).toBeGreaterThan(0);

      // Check each external link has target="_blank"
      for (let i = 0; i < count; i++) {
        const link = externalLinks.nth(i);
        const href = await link.getAttribute('href');
        const target = await link.getAttribute('target');

        expect(target, `Link ${href} should have target="_blank"`).toBe('_blank');
      }
    });

    test('All external links have rel="noopener noreferrer" attribute', async ({ page }) => {
      // Find all external links
      const externalLinks = page.locator('a[href^="http://"], a[href^="https://"]');
      const count = await externalLinks.count();

      expect(count).toBeGreaterThan(0);

      // Check each external link has proper rel attribute
      for (let i = 0; i < count; i++) {
        const link = externalLinks.nth(i);
        const href = await link.getAttribute('href');
        const rel = await link.getAttribute('rel');

        expect(rel, `Link ${href} should have rel attribute containing "noopener"`).toContain('noopener');
        expect(rel, `Link ${href} should have rel attribute containing "noreferrer"`).toContain('noreferrer');
      }
    });

    test('Header GitHub link has correct security attributes', async ({ page }) => {
      const headerGithubLink = page.locator('header nav a[href*="github.com"]');
      await expect(headerGithubLink).toBeVisible();

      const href = await headerGithubLink.getAttribute('href');
      expect(href).toBe('https://github.com/yetone/mirdb');

      const target = await headerGithubLink.getAttribute('target');
      expect(target).toBe('_blank');

      const rel = await headerGithubLink.getAttribute('rel');
      expect(rel).toContain('noopener');
      expect(rel).toContain('noreferrer');
    });

    test('Hero GitHub button has correct security attributes', async ({ page }) => {
      const heroGithubLink = page.locator('.hero-cta a[href*="github.com"]');
      await expect(heroGithubLink).toBeVisible();

      const href = await heroGithubLink.getAttribute('href');
      expect(href).toBe('https://github.com/yetone/mirdb');

      const target = await heroGithubLink.getAttribute('target');
      expect(target).toBe('_blank');

      const rel = await heroGithubLink.getAttribute('rel');
      expect(rel).toContain('noopener');
      expect(rel).toContain('noreferrer');
    });

    test('CircleCI badge link has correct security attributes', async ({ page }) => {
      const circleCILink = page.locator('.badge-container a[href*="circleci.com"]');
      await expect(circleCILink).toBeVisible();

      const href = await circleCILink.getAttribute('href');
      expect(href).toContain('circleci.com');

      const target = await circleCILink.getAttribute('target');
      expect(target).toBe('_blank');

      const rel = await circleCILink.getAttribute('rel');
      expect(rel).toContain('noopener');
      expect(rel).toContain('noreferrer');
    });

    test('Footer external links have correct security attributes', async ({ page }) => {
      const footerExternalLinks = page.locator('footer a[href^="https://"]');
      const count = await footerExternalLinks.count();

      expect(count).toBeGreaterThanOrEqual(3); // At least GitHub, Issues, License links

      for (let i = 0; i < count; i++) {
        const link = footerExternalLinks.nth(i);
        const href = await link.getAttribute('href');
        const target = await link.getAttribute('target');
        const rel = await link.getAttribute('rel');

        expect(target, `Footer link ${href} should have target="_blank"`).toBe('_blank');
        expect(rel, `Footer link ${href} should have rel containing "noopener"`).toContain('noopener');
        expect(rel, `Footer link ${href} should have rel containing "noreferrer"`).toContain('noreferrer');
      }
    });
  });

  test.describe('Test Case 2: External link reachability', () => {
    // Note: This integration test verifies links are reachable
    // We use HEAD requests to minimize bandwidth while verifying connectivity

    test('GitHub repository link is reachable', async ({ request }) => {
      const response = await request.head('https://github.com/yetone/mirdb');
      // GitHub may return 200 or 301/302 for redirects
      expect([200, 301, 302]).toContain(response.status());
    });

    test('GitHub issues link is reachable', async ({ request }) => {
      const response = await request.head('https://github.com/yetone/mirdb/issues');
      expect([200, 301, 302]).toContain(response.status());
    });

    test('GitHub main repository link is reachable (used for license)', async ({ request }) => {
      // The repository doesn't have a dedicated LICENSE file, so the license link points to the main repo
      const response = await request.head('https://github.com/yetone/mirdb');
      expect([200, 301, 302]).toContain(response.status());
    });

    test('CircleCI project link is reachable', async ({ request }) => {
      const response = await request.head('https://circleci.com/gh/yetone/mirdb');
      // CircleCI may return various success codes
      expect([200, 301, 302, 303]).toContain(response.status());
    });

    test('All external links on page are reachable', async ({ page, request }) => {
      // Find all external links
      const externalLinks = page.locator('a[href^="https://"]');
      const count = await externalLinks.count();

      expect(count).toBeGreaterThan(0);

      const checkedUrls = new Set();
      const failedLinks = [];

      for (let i = 0; i < count; i++) {
        const link = externalLinks.nth(i);
        const href = await link.getAttribute('href');

        // Skip if we've already checked this URL
        if (checkedUrls.has(href)) continue;
        checkedUrls.add(href);

        try {
          const response = await request.head(href, { timeout: 10000 });
          // Accept success codes and redirects
          if (![200, 201, 204, 301, 302, 303, 307, 308].includes(response.status())) {
            failedLinks.push({ url: href, status: response.status() });
          }
        } catch (error) {
          failedLinks.push({ url: href, error: error.message });
        }
      }

      expect(failedLinks, `Failed links: ${JSON.stringify(failedLinks)}`).toHaveLength(0);
    });
  });

  test.describe('External links identification', () => {
    test('Page contains expected external links', async ({ page }) => {
      // Verify all expected external links are present
      for (const expectedLink of EXPECTED_EXTERNAL_LINKS) {
        const link = page.locator(`a[href="${expectedLink.url}"]`).first();
        await expect(link, `${expectedLink.description} should be present`).toBeVisible();
      }
    });

    test('External links point to correct GitHub domain', async ({ page }) => {
      const githubLinks = page.locator('a[href*="github.com"]');
      const count = await githubLinks.count();

      expect(count).toBeGreaterThanOrEqual(4); // At least 4 GitHub links expected

      for (let i = 0; i < count; i++) {
        const link = githubLinks.nth(i);
        const href = await link.getAttribute('href');
        expect(href).toMatch(/^https:\/\/github\.com\//);
      }
    });

    test('External links point to correct project (yetone/mirdb)', async ({ page }) => {
      const githubLinks = page.locator('a[href*="github.com/yetone/mirdb"]');
      const count = await githubLinks.count();

      expect(count).toBeGreaterThanOrEqual(4);

      for (let i = 0; i < count; i++) {
        const link = githubLinks.nth(i);
        const href = await link.getAttribute('href');
        expect(href).toContain('github.com/yetone/mirdb');
      }
    });
  });
});
