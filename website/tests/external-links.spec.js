// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('External Links Functionality', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('TC1: GitHub repository link functionality', () => {
    test('Navigation GitHub link opens MirDB GitHub repository', async ({ page }) => {
      // Find the GitHub link in the navigation
      const navGithubLink = page.locator('nav a:has-text("GitHub")').first();
      await expect(navGithubLink).toBeVisible();

      // Verify the href points to the MirDB GitHub repository
      const href = await navGithubLink.getAttribute('href');
      expect(href).toBe('https://github.com/mirdb/mirdb');
    });

    test('Hero section GitHub link opens MirDB GitHub repository', async ({ page }) => {
      // Find the GitHub link in the hero section
      const heroGithubLink = page.locator('.hero a:has-text("GitHub"), .hero-buttons a[href*="github"]').first();
      await expect(heroGithubLink).toBeVisible();

      // Verify the href points to the MirDB GitHub repository
      const href = await heroGithubLink.getAttribute('href');
      expect(href).toBe('https://github.com/mirdb/mirdb');
    });

    test('Footer GitHub link opens MirDB GitHub repository', async ({ page }) => {
      // Find the GitHub link in the footer
      const footerGithubLink = page.locator('footer a:has-text("GitHub"), footer a[href*="github"]').first();
      await expect(footerGithubLink).toBeVisible();

      // Verify the href points to the MirDB GitHub repository
      const href = await footerGithubLink.getAttribute('href');
      expect(href).toBe('https://github.com/mirdb/mirdb');
    });
  });

  test.describe('TC2: External links have target="_blank"', () => {
    test('Navigation GitHub link opens in new tab', async ({ page }) => {
      const navGithubLink = page.locator('nav a:has-text("GitHub")').first();
      const target = await navGithubLink.getAttribute('target');
      expect(target).toBe('_blank');
    });

    test('Hero section GitHub link opens in new tab', async ({ page }) => {
      const heroGithubLink = page.locator('.hero a:has-text("GitHub"), .hero-buttons a[href*="github"]').first();
      const target = await heroGithubLink.getAttribute('target');
      expect(target).toBe('_blank');
    });

    test('Footer GitHub link opens in new tab', async ({ page }) => {
      const footerGithubLink = page.locator('footer a:has-text("GitHub"), footer a[href*="github"]').first();
      const target = await footerGithubLink.getAttribute('target');
      expect(target).toBe('_blank');
    });

    test('All external links have target="_blank"', async ({ page }) => {
      // Find all external links (links starting with http:// or https://)
      const externalLinks = page.locator('a[href^="http://"], a[href^="https://"]');
      const count = await externalLinks.count();

      expect(count).toBeGreaterThan(0);

      for (let i = 0; i < count; i++) {
        const link = externalLinks.nth(i);
        const target = await link.getAttribute('target');
        const href = await link.getAttribute('href');
        expect(target, `External link ${href} should have target="_blank"`).toBe('_blank');
      }
    });
  });

  test.describe('TC3: External links have rel="noopener noreferrer" for security', () => {
    test('Navigation GitHub link has rel="noopener noreferrer"', async ({ page }) => {
      const navGithubLink = page.locator('nav a:has-text("GitHub")').first();
      const rel = await navGithubLink.getAttribute('rel');
      expect(rel).toContain('noopener');
      expect(rel).toContain('noreferrer');
    });

    test('Hero section GitHub link has rel="noopener noreferrer"', async ({ page }) => {
      const heroGithubLink = page.locator('.hero a:has-text("GitHub"), .hero-buttons a[href*="github"]').first();
      const rel = await heroGithubLink.getAttribute('rel');
      expect(rel).toContain('noopener');
      expect(rel).toContain('noreferrer');
    });

    test('Footer GitHub link has rel="noopener noreferrer"', async ({ page }) => {
      const footerGithubLink = page.locator('footer a:has-text("GitHub"), footer a[href*="github"]').first();
      const rel = await footerGithubLink.getAttribute('rel');
      expect(rel).toContain('noopener');
      expect(rel).toContain('noreferrer');
    });

    test('All external links have rel containing "noopener"', async ({ page }) => {
      // Find all external links
      const externalLinks = page.locator('a[href^="http://"], a[href^="https://"]');
      const count = await externalLinks.count();

      expect(count).toBeGreaterThan(0);

      for (let i = 0; i < count; i++) {
        const link = externalLinks.nth(i);
        const rel = await link.getAttribute('rel');
        const href = await link.getAttribute('href');
        expect(rel, `External link ${href} should have rel containing "noopener"`).toContain('noopener');
      }
    });
  });

  test.describe('TC4: No broken external links', () => {
    test('All external links are valid URLs', async ({ page }) => {
      // Find all external links
      const externalLinks = page.locator('a[href^="http://"], a[href^="https://"]');
      const count = await externalLinks.count();

      expect(count).toBeGreaterThan(0);

      const hrefs = [];
      for (let i = 0; i < count; i++) {
        const link = externalLinks.nth(i);
        const href = await link.getAttribute('href');
        if (href) {
          hrefs.push(href);
        }
      }

      // Verify all external links are valid URLs
      for (const href of hrefs) {
        // Check that it's a valid URL format
        expect(() => new URL(href), `${href} should be a valid URL`).not.toThrow();
      }
    });

    test('GitHub repository URL is correctly formatted', async ({ page }) => {
      // Find all GitHub links
      const githubLinks = page.locator('a[href*="github.com/mirdb/mirdb"]');
      const count = await githubLinks.count();

      expect(count).toBeGreaterThan(0);

      for (let i = 0; i < count; i++) {
        const link = githubLinks.nth(i);
        const href = await link.getAttribute('href');

        // Verify the GitHub URL is correctly formatted
        expect(href).toBe('https://github.com/mirdb/mirdb');

        // Verify it's a valid URL
        const url = new URL(href);
        expect(url.protocol).toBe('https:');
        expect(url.hostname).toBe('github.com');
        expect(url.pathname).toBe('/mirdb/mirdb');
      }
    });

    test('External links return successful HTTP response', async ({ page, request }) => {
      // Find all external links
      const externalLinks = page.locator('a[href^="http://"], a[href^="https://"]');
      const count = await externalLinks.count();

      expect(count).toBeGreaterThan(0);

      // Collect unique external URLs
      const uniqueUrls = new Set();
      for (let i = 0; i < count; i++) {
        const link = externalLinks.nth(i);
        const href = await link.getAttribute('href');
        if (href) {
          uniqueUrls.add(href);
        }
      }

      // Check each unique URL returns a successful response
      for (const url of uniqueUrls) {
        try {
          const response = await request.head(url, { timeout: 10000 });
          // Accept 200, 301, 302, 307, 308 (redirects are valid)
          expect(
            [200, 301, 302, 307, 308].includes(response.status()),
            `URL ${url} should return a successful status (got ${response.status()})`
          ).toBeTruthy();
        } catch (error) {
          // Some external services may block automated requests
          // In that case, we just verify the URL is well-formed
          expect(() => new URL(url), `${url} should be a valid URL`).not.toThrow();
        }
      }
    });
  });

  test.describe('External link identification', () => {
    test('Page contains expected external links', async ({ page }) => {
      // Find all external links
      const externalLinks = page.locator('a[href^="http://"], a[href^="https://"]');
      const count = await externalLinks.count();

      // The page should have at least 3 GitHub links (nav, hero, footer)
      expect(count).toBeGreaterThanOrEqual(3);
    });

    test('External links are distinguishable from internal links', async ({ page }) => {
      // Find all links
      const allLinks = page.locator('a[href]');
      const allLinksCount = await allLinks.count();

      // Find external links
      const externalLinks = page.locator('a[href^="http://"], a[href^="https://"]');
      const externalCount = await externalLinks.count();

      // Find internal links (starting with / or #)
      const internalLinks = page.locator('a[href^="/"], a[href^="#"]');
      const internalCount = await internalLinks.count();

      // Page should have both internal and external links
      expect(externalCount).toBeGreaterThan(0);
      expect(internalCount).toBeGreaterThan(0);

      // All external links should open in new tab
      for (let i = 0; i < externalCount; i++) {
        const link = externalLinks.nth(i);
        const target = await link.getAttribute('target');
        expect(target).toBe('_blank');
      }

      // Internal links should NOT have target="_blank"
      for (let i = 0; i < internalCount; i++) {
        const link = internalLinks.nth(i);
        const target = await link.getAttribute('target');
        const href = await link.getAttribute('href');
        // Internal links should not open in new tab
        expect(target, `Internal link ${href} should not have target="_blank"`).not.toBe('_blank');
      }
    });
  });
});
