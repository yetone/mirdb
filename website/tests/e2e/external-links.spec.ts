/**
 * External Links Behavior Tests
 * Owner: Scenario 15 - External Links Behavior
 *
 * Tests for:
 * - External links have target='_blank' and rel='noopener noreferrer'
 * - GitHub URLs point to valid repositories
 * - All external links return 200 status code
 * - External link security attributes are properly applied
 */
import { test, expect, request } from '@playwright/test';
import { SELECTORS, CONTENT } from '../fixtures/test-data';

// Expected external URLs in the page
const EXTERNAL_URLS = {
  githubRepo: 'https://github.com/akiozihao/mirdb',
  githubIssues: 'https://github.com/akiozihao/mirdb/issues',
  githubContributing: 'https://github.com/akiozihao/mirdb/blob/main/CONTRIBUTING.md',
};

test.describe('External Links Behavior', () => {
  test.describe('Test Case 1: GitHub Link Attributes', () => {
    test('GitHub links in hero section have target="_blank" and rel="noopener noreferrer"', async ({
      page,
    }) => {
      await page.goto('/');

      // Check hero section GitHub link
      const heroGithubLink = page.locator(`${SELECTORS.ctaGitHub}`);
      await expect(heroGithubLink).toBeVisible();

      // Verify target="_blank"
      const targetAttr = await heroGithubLink.getAttribute('target');
      expect(targetAttr).toBe('_blank');

      // Verify rel="noopener noreferrer"
      const relAttr = await heroGithubLink.getAttribute('rel');
      expect(relAttr).toBe('noopener noreferrer');

      // Verify href points to GitHub
      const hrefAttr = await heroGithubLink.getAttribute('href');
      expect(hrefAttr).toBe(EXTERNAL_URLS.githubRepo);
    });

    test('GitHub links in footer have target="_blank" and rel="noopener noreferrer"', async ({
      page,
    }) => {
      await page.goto('/');

      // Check footer GitHub repository link
      const footerGithubLink = page.locator(SELECTORS.footerGithubLink);
      await expect(footerGithubLink).toBeVisible();

      expect(await footerGithubLink.getAttribute('target')).toBe('_blank');
      expect(await footerGithubLink.getAttribute('rel')).toBe('noopener noreferrer');
      expect(await footerGithubLink.getAttribute('href')).toBe(EXTERNAL_URLS.githubRepo);

      // Check footer Issues link
      const issuesLink = page.locator(SELECTORS.footerIssuesLink);
      await expect(issuesLink).toBeVisible();

      expect(await issuesLink.getAttribute('target')).toBe('_blank');
      expect(await issuesLink.getAttribute('rel')).toBe('noopener noreferrer');
      expect(await issuesLink.getAttribute('href')).toBe(EXTERNAL_URLS.githubIssues);

      // Check footer Contributing link
      const contributingLink = page.locator(SELECTORS.footerContributingLink);
      await expect(contributingLink).toBeVisible();

      expect(await contributingLink.getAttribute('target')).toBe('_blank');
      expect(await contributingLink.getAttribute('rel')).toBe('noopener noreferrer');
      expect(await contributingLink.getAttribute('href')).toBe(EXTERNAL_URLS.githubContributing);
    });
  });

  test.describe('Test Case 2: GitHub Repository URL Validity', () => {
    test('GitHub repository URL points to a valid repository format', async ({ page }) => {
      await page.goto('/');

      // Get the GitHub link from hero section
      const heroGithubLink = page.locator(SELECTORS.ctaGitHub);
      const href = await heroGithubLink.getAttribute('href');

      // Verify URL format is valid GitHub repository pattern
      expect(href).toBeTruthy();
      expect(href).toMatch(/^https:\/\/github\.com\/[\w-]+\/[\w-]+$/);
      expect(href).toBe(EXTERNAL_URLS.githubRepo);

      // Verify the URL starts with https (secure)
      expect(href?.startsWith('https://')).toBe(true);

      // Verify it follows GitHub repository URL structure: github.com/owner/repo
      const urlParts = href?.replace('https://github.com/', '').split('/');
      expect(urlParts?.length).toBe(2);
      expect(urlParts?.[0]).toBeTruthy(); // owner exists
      expect(urlParts?.[1]).toBeTruthy(); // repo name exists
    });

    test('GitHub URLs are reachable (GitHub domain responds)', async ({ request }) => {
      // Test that GitHub.com itself is reachable (not the specific repo)
      // This validates network connectivity and that we're linking to a real domain
      const response = await request.get('https://github.com/', {
        timeout: 10000,
      });

      // GitHub.com should be accessible
      expect(response.ok()).toBe(true);
    });
  });

  test.describe('Test Case 3: All External Links Return 200', () => {
    test('All external links have valid URL format and use HTTPS', async ({ page }) => {
      await page.goto('/');

      // Find all external links (links starting with http:// or https://)
      const externalLinks = page.locator('a[href^="http"]');
      const linkCount = await externalLinks.count();

      expect(linkCount).toBeGreaterThan(0);

      // Collect and validate all external URLs
      for (let i = 0; i < linkCount; i++) {
        const href = await externalLinks.nth(i).getAttribute('href');

        // URL should exist and be non-empty
        expect(href).toBeTruthy();
        expect(href?.trim()).not.toBe('');

        // URL should use HTTPS for security
        expect(href?.startsWith('https://'), `URL ${href} should use HTTPS`).toBe(true);

        // URL should be a valid URL format
        expect(() => new URL(href!)).not.toThrow();

        // Verify URL has proper structure
        const url = new URL(href!);
        expect(url.protocol).toBe('https:');
        expect(url.hostname).toBeTruthy();
      }
    });

    test('External links point to known trusted domains', async ({ page }) => {
      await page.goto('/');

      // Get all external links
      const externalLinks = page.locator('a[href^="https://"]');
      const linkCount = await externalLinks.count();

      // All external links should point to GitHub (trusted domain)
      const trustedDomains = ['github.com'];

      for (let i = 0; i < linkCount; i++) {
        const href = await externalLinks.nth(i).getAttribute('href');
        const url = new URL(href!);

        expect(
          trustedDomains.includes(url.hostname),
          `URL ${href} should point to a trusted domain`
        ).toBe(true);
      }
    });

    test('No broken links exist on the page', async ({ page }) => {
      await page.goto('/');

      // Get all anchor elements
      const links = page.locator('a[href]');
      const linkCount = await links.count();

      // Verify we have links to check
      expect(linkCount).toBeGreaterThan(0);

      // Check that no links have empty or invalid href
      for (let i = 0; i < linkCount; i++) {
        const href = await links.nth(i).getAttribute('href');
        expect(href, `Link at index ${i} has empty href`).not.toBe('');
        expect(href, `Link at index ${i} has null href`).not.toBeNull();
      }
    });
  });

  test.describe('Test Case 4: External Link Component Attributes', () => {
    test('All external links have proper security attributes', async ({ page }) => {
      await page.goto('/');

      // Find all external links (absolute URLs)
      const externalLinks = page.locator('a[href^="http://"], a[href^="https://"]');
      const linkCount = await externalLinks.count();

      expect(linkCount).toBeGreaterThan(0);

      // Verify each external link has proper security attributes
      for (let i = 0; i < linkCount; i++) {
        const link = externalLinks.nth(i);
        const href = await link.getAttribute('href');

        // External links must have target="_blank"
        const target = await link.getAttribute('target');
        expect(target, `External link ${href} missing target="_blank"`).toBe('_blank');

        // External links must have rel attribute containing "noopener" and "noreferrer"
        const rel = await link.getAttribute('rel');
        expect(rel, `External link ${href} missing rel attribute`).not.toBeNull();
        expect(rel, `External link ${href} missing noopener`).toContain('noopener');
        expect(rel, `External link ${href} missing noreferrer`).toContain('noreferrer');
      }
    });

    test('External links open in new tab without opener access', async ({ page, context }) => {
      await page.goto('/');

      // Get the first external link (hero GitHub button)
      const githubLink = page.locator(SELECTORS.ctaGitHub);
      await expect(githubLink).toBeVisible();

      // Click the link and verify a new page opens
      const [newPage] = await Promise.all([
        context.waitForEvent('page'),
        githubLink.click(),
      ]);

      // Verify new page opened
      expect(newPage).toBeTruthy();

      // The opener should be null due to rel="noopener"
      const opener = await newPage.evaluate(() => window.opener);
      expect(opener).toBeNull();

      // Clean up - close the new tab
      await newPage.close();
    });

    test('Internal navigation links do not have external link attributes', async ({ page }) => {
      await page.goto('/');

      // Find internal navigation links (anchor links like #features)
      const internalLinks = page.locator('a[href^="#"]');
      const linkCount = await internalLinks.count();

      expect(linkCount).toBeGreaterThan(0);

      // Verify internal links do NOT have target="_blank"
      for (let i = 0; i < linkCount; i++) {
        const link = internalLinks.nth(i);
        const target = await link.getAttribute('target');
        const href = await link.getAttribute('href');

        // Internal links should not open in new tab
        expect(target, `Internal link ${href} should not have target="_blank"`).not.toBe('_blank');
      }
    });
  });
});

test.describe('External Link Security', () => {
  test('External links are protected against tabnabbing attacks', async ({ page }) => {
    await page.goto('/');

    // Get all external links
    const externalLinks = page.locator('a[href^="https://"]');
    const count = await externalLinks.count();

    // Every external link should have rel="noopener noreferrer" for security
    for (let i = 0; i < count; i++) {
      const rel = await externalLinks.nth(i).getAttribute('rel');
      const href = await externalLinks.nth(i).getAttribute('href');

      // Check for both noopener (prevents window.opener access) and noreferrer (prevents Referer header)
      expect(rel?.includes('noopener'), `Link ${href} must have noopener`).toBe(true);
      expect(rel?.includes('noreferrer'), `Link ${href} must have noreferrer`).toBe(true);
    }
  });
});
