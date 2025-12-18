import { test, expect } from '@playwright/test';

test.describe('External Links Validation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('TC1: GitHub Repository Link', () => {
    test('GitHub link in navigation should point to valid repository URL', async ({ page }) => {
      // Find the GitHub link in navigation
      const navGithubLink = page.locator('.nav-links a[href*="github.com"]');
      await expect(navGithubLink).toBeVisible();

      // Verify it points to the expected repository
      const href = await navGithubLink.getAttribute('href');
      expect(href).toBe('https://github.com/yetone/mirdb');
    });

    test('GitHub link in hero section should point to valid repository URL', async ({ page }) => {
      // Find the GitHub link in hero section
      const heroGithubLink = page.locator('.hero a[href*="github.com"]');
      await expect(heroGithubLink).toBeVisible();

      // Verify it points to the expected repository
      const href = await heroGithubLink.getAttribute('href');
      expect(href).toBe('https://github.com/yetone/mirdb');
    });

    test('GitHub repository URL should return HTTP 200', async ({ request }) => {
      // Make HTTP HEAD request to verify the URL is valid
      const response = await request.head('https://github.com/yetone/mirdb');

      // GitHub should return 200 for valid repositories
      expect(response.status()).toBe(200);
    });

    test('all GitHub links on page should point to same valid repository', async ({ page }) => {
      // Find all GitHub links
      const githubLinks = page.locator('a[href*="github.com"]');
      const count = await githubLinks.count();

      expect(count).toBeGreaterThan(0);

      // Check each link points to the same URL
      for (let i = 0; i < count; i++) {
        const href = await githubLinks.nth(i).getAttribute('href');
        expect(href).toBe('https://github.com/yetone/mirdb');
      }
    });
  });

  test.describe('TC2: Documentation Link', () => {
    test('documentation link should be present in footer', async ({ page }) => {
      // Scroll to footer
      const footer = page.locator('[data-testid="footer"]');
      await footer.scrollIntoViewIfNeeded();

      // Find the documentation link
      const docsLink = page.locator('[data-testid="footer-docs-link"]');
      await expect(docsLink).toBeVisible();
    });

    test('documentation link should have valid href', async ({ page }) => {
      // Find the documentation link
      const docsLink = page.locator('[data-testid="footer-docs-link"]');
      const href = await docsLink.getAttribute('href');

      // Verify href is present and valid (either internal anchor or external URL)
      expect(href).toBeTruthy();

      // If internal anchor, it should start with #
      // If external, should be valid URL
      if (href?.startsWith('#')) {
        // Internal anchor - verify the target section exists
        const targetSection = page.locator(href);
        await expect(targetSection).toBeVisible();
      } else if (href?.startsWith('http')) {
        // External URL - verify it's properly formatted
        expect(href).toMatch(/^https?:\/\/.+/);
      }
    });

    test('documentation link points to getting-started section or valid URL', async ({ page }) => {
      const docsLink = page.locator('[data-testid="footer-docs-link"]');
      const href = await docsLink.getAttribute('href');

      // Based on the HTML, the docs link points to #getting-started
      // This is a valid placeholder until external docs exist
      if (href === '#getting-started') {
        // Clicking should scroll to getting-started section
        await docsLink.click();
        await page.waitForTimeout(500);

        const gettingStartedSection = page.locator('#getting-started');
        await expect(gettingStartedSection).toBeInViewport();
      }
    });
  });

  test.describe('TC3: External Link Security Attributes', () => {
    test('navigation GitHub link should have rel="noopener noreferrer"', async ({ page }) => {
      const navGithubLink = page.locator('.nav-links a[href*="github.com"]');
      const rel = await navGithubLink.getAttribute('rel');

      expect(rel).toContain('noopener');
      expect(rel).toContain('noreferrer');
    });

    test('hero GitHub link should have rel="noopener noreferrer"', async ({ page }) => {
      const heroGithubLink = page.locator('.hero a[href*="github.com"]');
      const rel = await heroGithubLink.getAttribute('rel');

      expect(rel).toContain('noopener');
      expect(rel).toContain('noreferrer');
    });

    test('all external links should have security attributes', async ({ page }) => {
      // Find all external links
      const externalLinks = page.locator('a[href^="http://"], a[href^="https://"]');
      const count = await externalLinks.count();

      for (let i = 0; i < count; i++) {
        const link = externalLinks.nth(i);
        const href = await link.getAttribute('href');
        const target = await link.getAttribute('target');

        // If opening in new tab, must have noopener
        if (target === '_blank') {
          const rel = await link.getAttribute('rel');
          expect(rel, `External link ${href} with target="_blank" should have noopener`).toContain('noopener');
        }
      }
    });
  });

  test.describe('TC4: External Links Open in New Tab', () => {
    test('navigation GitHub link should have target="_blank"', async ({ page }) => {
      const navGithubLink = page.locator('.nav-links a[href*="github.com"]');
      const target = await navGithubLink.getAttribute('target');

      expect(target).toBe('_blank');
    });

    test('hero GitHub link should have target="_blank"', async ({ page }) => {
      const heroGithubLink = page.locator('.hero a[href*="github.com"]');
      const target = await heroGithubLink.getAttribute('target');

      expect(target).toBe('_blank');
    });

    test('all external links should open in new tab', async ({ page }) => {
      // Find all external links
      const externalLinks = page.locator('a[href^="http://"], a[href^="https://"]');
      const count = await externalLinks.count();

      const linksWithoutNewTab: string[] = [];

      for (let i = 0; i < count; i++) {
        const link = externalLinks.nth(i);
        const href = await link.getAttribute('href');
        const target = await link.getAttribute('target');

        if (target !== '_blank') {
          linksWithoutNewTab.push(href || 'unknown');
        }
      }

      expect(linksWithoutNewTab).toEqual([]);
    });

    test('internal anchor links should NOT open in new tab', async ({ page }) => {
      // Find all internal anchor links
      const internalLinks = page.locator('a[href^="#"]');
      const count = await internalLinks.count();

      const internalLinksWithNewTab: string[] = [];

      for (let i = 0; i < count; i++) {
        const link = internalLinks.nth(i);
        const href = await link.getAttribute('href');
        const target = await link.getAttribute('target');

        if (target === '_blank') {
          internalLinksWithNewTab.push(href || 'unknown');
        }
      }

      expect(internalLinksWithNewTab).toEqual([]);
    });
  });

  test.describe('Link Accessibility', () => {
    test('external links should have descriptive text', async ({ page }) => {
      const externalLinks = page.locator('a[href^="http://"], a[href^="https://"]');
      const count = await externalLinks.count();

      for (let i = 0; i < count; i++) {
        const link = externalLinks.nth(i);
        const text = await link.textContent();

        // Links should have meaningful text (not just "click here" or empty)
        expect(text?.trim().length).toBeGreaterThan(0);
        expect(text?.toLowerCase()).not.toBe('click here');
      }
    });
  });
});
