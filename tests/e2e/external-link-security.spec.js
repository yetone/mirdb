// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E tests for External Link Security
 * Scenario: Verify external links have proper security attributes to prevent tabnapping
 */
test.describe('External Link Security', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case 1: Check external links for rel attribute
  test.describe('TC1: External links have proper rel attributes', () => {
    test('all links with target="_blank" have rel="noopener" or rel="noreferrer"', async ({ page }) => {
      const linksWithBlank = await page.locator('a[target="_blank"]').all();
      const violations = [];

      for (const link of linksWithBlank) {
        const rel = await link.getAttribute('rel');
        const href = await link.getAttribute('href');

        const hasNoopener = rel && rel.includes('noopener');
        const hasNoreferrer = rel && rel.includes('noreferrer');

        if (!hasNoopener && !hasNoreferrer) {
          violations.push({
            href: href,
            rel: rel || 'missing',
          });
        }
      }

      expect(violations).toEqual([]);
    });

    test('hero section GitHub link has security attributes', async ({ page }) => {
      const heroGithubLink = page.locator('.hero a[href*="github.com"]');
      await expect(heroGithubLink).toBeVisible();

      const rel = await heroGithubLink.getAttribute('rel');
      expect(rel).toContain('noopener');

      const target = await heroGithubLink.getAttribute('target');
      expect(target).toBe('_blank');
    });

    test('documentation links have security attributes', async ({ page }) => {
      const docsLinks = page.locator('.docs-link');
      const count = await docsLinks.count();

      for (let i = 0; i < count; i++) {
        const link = docsLinks.nth(i);
        const rel = await link.getAttribute('rel');
        const target = await link.getAttribute('target');

        expect(rel).toContain('noopener');
        expect(target).toBe('_blank');
      }
    });

    test('footer links have security attributes', async ({ page }) => {
      const footerLinks = page.locator('footer a[href^="https://"]');
      const count = await footerLinks.count();

      for (let i = 0; i < count; i++) {
        const link = footerLinks.nth(i);
        const rel = await link.getAttribute('rel');
        const target = await link.getAttribute('target');

        expect(rel).toContain('noopener');
        expect(target).toBe('_blank');
      }
    });
  });

  // Test Case 2: Check for mixed content
  test.describe('TC2: No mixed content issues', () => {
    test('no HTTP resources loaded on page', async ({ page }) => {
      const httpResources = [];

      // Monitor all network requests
      page.on('request', (request) => {
        const url = request.url();
        if (url.startsWith('http://') && !url.includes('localhost')) {
          httpResources.push(url);
        }
      });

      // Reload to capture all requests
      await page.reload();
      await page.waitForLoadState('networkidle');

      expect(httpResources).toEqual([]);
    });

    test('no mixed content warnings in console', async ({ page }) => {
      const consoleMessages = [];

      page.on('console', (msg) => {
        const text = msg.text().toLowerCase();
        if (text.includes('mixed content') || text.includes('blocked loading')) {
          consoleMessages.push(msg.text());
        }
      });

      await page.reload();
      await page.waitForLoadState('networkidle');

      expect(consoleMessages).toEqual([]);
    });

    test('all external script sources use HTTPS', async ({ page }) => {
      const scripts = await page.locator('script[src^="http"]').all();

      for (const script of scripts) {
        const src = await script.getAttribute('src');
        expect(src).toMatch(/^https:\/\//);
      }
    });

    test('all external stylesheet sources use HTTPS', async ({ page }) => {
      const stylesheets = await page.locator('link[rel="stylesheet"][href^="http"]').all();

      for (const stylesheet of stylesheets) {
        const href = await stylesheet.getAttribute('href');
        expect(href).toMatch(/^https:\/\//);
      }
    });

    test('all external links use HTTPS protocol', async ({ page }) => {
      const httpLinks = await page.locator('a[href^="http://"]').all();
      const insecureLinks = [];

      for (const link of httpLinks) {
        const href = await link.getAttribute('href');
        // Exclude localhost for development
        if (href && !href.includes('localhost')) {
          insecureLinks.push(href);
        }
      }

      expect(insecureLinks).toEqual([]);
    });
  });

  // Test Case 3: Check for inline event handlers
  test.describe('TC3: No inline event handlers', () => {
    test('no inline onclick handlers in HTML', async ({ page }) => {
      const count = await page.locator('[onclick]').count();
      expect(count).toBe(0);
    });

    test('no inline onmouseover handlers in HTML', async ({ page }) => {
      const count = await page.locator('[onmouseover]').count();
      expect(count).toBe(0);
    });

    test('no inline onfocus handlers in HTML', async ({ page }) => {
      const count = await page.locator('[onfocus]').count();
      expect(count).toBe(0);
    });

    test('no inline onload handlers in HTML', async ({ page }) => {
      const count = await page.locator('[onload]').count();
      expect(count).toBe(0);
    });

    test('no inline onerror handlers in HTML', async ({ page }) => {
      const count = await page.locator('[onerror]').count();
      expect(count).toBe(0);
    });

    test('no javascript: URLs in links', async ({ page }) => {
      const count = await page.locator('a[href^="javascript:"]').count();
      expect(count).toBe(0);
    });

    test('comprehensive check for all inline event handlers', async ({ page }) => {
      const inlineHandlers = [
        'onclick', 'ondblclick', 'onmousedown', 'onmouseup', 'onmouseover',
        'onmouseout', 'onmousemove', 'onfocus', 'onblur', 'onchange',
        'onsubmit', 'onkeydown', 'onkeyup', 'onkeypress', 'onload', 'onerror'
      ];

      const violations = [];

      for (const handler of inlineHandlers) {
        const count = await page.locator(`[${handler}]`).count();
        if (count > 0) {
          violations.push({ handler, count });
        }
      }

      expect(violations).toEqual([]);
    });
  });

  // Additional security validations
  test.describe('Additional security checks', () => {
    test('clicking external link does not give access to opener', async ({ page, context }) => {
      // Get the first external link
      const externalLink = page.locator('a[href^="https://"][target="_blank"]').first();
      await expect(externalLink).toBeVisible();

      const rel = await externalLink.getAttribute('rel');

      // Verify the rel attribute prevents opener access
      expect(rel).toContain('noopener');
    });

    test('CDN resources are loaded securely', async ({ page }) => {
      const cdnScripts = await page.locator('script[src*="cdnjs.cloudflare.com"]').all();

      for (const script of cdnScripts) {
        const src = await script.getAttribute('src');
        expect(src).toMatch(/^https:\/\//);
      }
    });

    test('page meta tags use HTTPS URLs', async ({ page }) => {
      const ogImage = page.locator('meta[property="og:image"]');
      const ogUrl = page.locator('meta[property="og:url"]');

      if (await ogImage.count() > 0) {
        const content = await ogImage.getAttribute('content');
        expect(content).toMatch(/^https:\/\//);
      }

      if (await ogUrl.count() > 0) {
        const content = await ogUrl.getAttribute('content');
        expect(content).toMatch(/^https:\/\//);
      }
    });
  });
});
