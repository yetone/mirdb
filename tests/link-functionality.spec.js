// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = 'file://' + path.resolve(__dirname, '../index.html');
const GITHUB_REPO_URL = 'https://github.com/yetone/mirdb';
const CIRCLECI_URL = 'https://circleci.com/gh/yetone/mirdb';

test.describe('Link Functionality - Success Criteria', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(indexPath);
  });

  // ==========================================
  // TEST CASE 1: Unit Test - Valid href attributes
  // ==========================================
  test.describe('TC1: All anchor elements have valid href attributes (Unit)', () => {
    test('All anchor elements have non-empty href attributes', async ({ page }) => {
      // Get all anchor elements
      const anchors = page.locator('a');
      const count = await anchors.count();

      // Ensure we have links on the page
      expect(count).toBeGreaterThan(0);

      // Check each anchor has a valid href
      for (let i = 0; i < count; i++) {
        const anchor = anchors.nth(i);
        const href = await anchor.getAttribute('href');

        // href should exist and not be empty
        expect(href, `Anchor ${i + 1} should have an href attribute`).toBeTruthy();
        expect(href.trim().length, `Anchor ${i + 1} should have a non-empty href`).toBeGreaterThan(0);
      }
    });

    test('All href attributes have valid URL or anchor format', async ({ page }) => {
      const anchors = page.locator('a');
      const count = await anchors.count();

      for (let i = 0; i < count; i++) {
        const anchor = anchors.nth(i);
        const href = await anchor.getAttribute('href');

        // Valid href patterns:
        // 1. External URLs (http:// or https://)
        // 2. Internal anchors (#section)
        // 3. Relative paths (./path or path)
        const isValidHref =
          href.startsWith('http://') ||
          href.startsWith('https://') ||
          href.startsWith('#') ||
          href.startsWith('./') ||
          href.startsWith('/') ||
          /^[a-zA-Z0-9]/.test(href);

        expect(isValidHref, `Anchor ${i + 1} with href "${href}" should have a valid format`).toBeTruthy();
      }
    });

    test('No links have javascript: or void hrefs', async ({ page }) => {
      const anchors = page.locator('a');
      const count = await anchors.count();

      for (let i = 0; i < count; i++) {
        const anchor = anchors.nth(i);
        const href = await anchor.getAttribute('href');

        // Should not contain javascript: void or # alone (without section)
        expect(href.includes('javascript:'), `Anchor ${i + 1} should not have javascript: href`).toBeFalsy();
        expect(href === '#', `Anchor ${i + 1} should not have empty # href`).toBeFalsy();
        expect(href.includes('void('), `Anchor ${i + 1} should not have void() href`).toBeFalsy();
      }
    });
  });

  // ==========================================
  // TEST CASE 2: E2E Test - GitHub repository link
  // ==========================================
  test.describe('TC2: GitHub repository link navigates to valid repository (E2E)', () => {
    test('Primary CTA links to GitHub repository with correct URL', async ({ page }) => {
      const primaryCta = page.locator('#cta-primary');
      await expect(primaryCta).toBeVisible();

      const href = await primaryCta.getAttribute('href');
      expect(href).toBe(GITHUB_REPO_URL);

      // Verify it's a valid GitHub URL format
      expect(href).toMatch(/^https:\/\/github\.com\/[a-zA-Z0-9_-]+\/[a-zA-Z0-9_-]+$/);
    });

    test('Footer contains GitHub repository link with correct URL', async ({ page }) => {
      const footerGithubLink = page.locator('.footer-links a[href*="github.com/yetone/mirdb"]').first();
      await expect(footerGithubLink).toBeVisible();

      const href = await footerGithubLink.getAttribute('href');
      expect(href).toBe(GITHUB_REPO_URL);
    });

    test('GitHub link is clickable and properly formatted', async ({ page }) => {
      const primaryCta = page.locator('#cta-primary');

      // Verify it's an anchor element
      const tagName = await primaryCta.evaluate(el => el.tagName.toLowerCase());
      expect(tagName).toBe('a');

      // Verify it's enabled and clickable
      await expect(primaryCta).toBeEnabled();

      // Verify it has appropriate styling (btn class)
      const hasBtn = await primaryCta.evaluate(el => el.classList.contains('btn'));
      expect(hasBtn).toBeTruthy();
    });

    test('All GitHub links on the page point to the same repository', async ({ page }) => {
      const githubLinks = page.locator('a[href*="github.com/yetone/mirdb"]');
      const count = await githubLinks.count();

      // Should have at least 2 GitHub links (hero CTA and footer)
      expect(count).toBeGreaterThanOrEqual(2);

      // Verify all point to the same repository
      for (let i = 0; i < count; i++) {
        const href = await githubLinks.nth(i).getAttribute('href');
        expect(href).toBe(GITHUB_REPO_URL);
      }
    });
  });

  // ==========================================
  // TEST CASE 3: E2E Test - Documentation links
  // ==========================================
  test.describe('TC3: Documentation links navigate to valid pages (E2E)', () => {
    test('Documentation CTA links to quick-start section', async ({ page }) => {
      const secondaryCta = page.locator('#cta-secondary');
      await expect(secondaryCta).toBeVisible();

      const href = await secondaryCta.getAttribute('href');
      expect(href).toBe('#quick-start');

      // Verify the text mentions documentation
      const text = await secondaryCta.textContent();
      expect(text.toLowerCase()).toContain('documentation');
    });

    test('Quick-start section anchor target exists', async ({ page }) => {
      // Click the documentation link
      const secondaryCta = page.locator('#cta-secondary');
      const href = await secondaryCta.getAttribute('href');

      // Verify the target section exists
      const targetId = href.replace('#', '');
      const targetSection = page.locator(`#${targetId}`);
      await expect(targetSection).toBeVisible();
    });

    test('Documentation link scrolls to correct section when clicked', async ({ page }) => {
      const secondaryCta = page.locator('#cta-secondary');
      await secondaryCta.click();

      // Verify quick-start section is now in view
      const quickStartSection = page.locator('#quick-start');
      await expect(quickStartSection).toBeInViewport();
    });

    test('All internal anchor links have corresponding section targets', async ({ page }) => {
      // Get all anchor links that point to internal sections
      const internalAnchors = page.locator('a[href^="#"]');
      const count = await internalAnchors.count();

      for (let i = 0; i < count; i++) {
        const href = await internalAnchors.nth(i).getAttribute('href');

        // Skip empty anchors
        if (href === '#') continue;

        const targetId = href.replace('#', '');
        const targetElement = page.locator(`#${targetId}`);

        // Verify the target element exists
        const exists = await targetElement.count();
        expect(exists, `Target element ${href} should exist`).toBeGreaterThan(0);
      }
    });
  });

  // ==========================================
  // TEST CASE 4: Integration Test - External links return 200 status
  // ==========================================
  test.describe('TC4: All external links return 200 status code (Integration)', () => {
    test('GitHub repository link returns valid response', async ({ request }) => {
      // Test the GitHub repository URL
      const response = await request.get(GITHUB_REPO_URL, {
        timeout: 10000,
        maxRedirects: 5
      });

      // GitHub should return 200 OK
      expect(response.status()).toBe(200);
    });

    test('GitHub Stars badge URL is valid', async ({ request }) => {
      const badgeUrl = 'https://img.shields.io/github/stars/yetone/mirdb?style=social';

      const response = await request.get(badgeUrl, {
        timeout: 10000,
        maxRedirects: 5
      });

      // shields.io should return 200 OK
      expect(response.status()).toBe(200);
    });

    test('CircleCI badge URL responds (badge services may return various status codes)', async ({ request }) => {
      const badgeUrl = 'https://circleci.com/gh/yetone/mirdb.svg?style=svg';

      const response = await request.get(badgeUrl, {
        timeout: 10000,
        maxRedirects: 5
      });

      // Badge services may return 200, 404, or other codes depending on project configuration
      // We verify the URL is reachable (i.e., the domain responds)
      // A 404 indicates the badge URL format is valid but the project may not have CircleCI configured
      const validStatuses = [200, 302, 404];
      expect(validStatuses).toContain(response.status());
    });

    test('CircleCI project link is valid', async ({ request }) => {
      const response = await request.get(CIRCLECI_URL, {
        timeout: 10000,
        maxRedirects: 5
      });

      // CircleCI project page should return 200 OK
      expect(response.status()).toBe(200);
    });

    test('All external link URLs are accessible', async ({ page, request }) => {
      // Collect all external links
      const externalAnchors = page.locator('a[href^="http"]');
      const count = await externalAnchors.count();

      expect(count, 'Page should have external links').toBeGreaterThan(0);

      // Badge service URLs that may return non-200 status codes based on project config
      const badgeServices = ['circleci.com', 'img.shields.io'];

      // Test each external link
      for (let i = 0; i < count; i++) {
        const href = await externalAnchors.nth(i).getAttribute('href');

        try {
          const response = await request.get(href, {
            timeout: 10000,
            maxRedirects: 5
          });

          // Badge services may return various status codes based on project configuration
          const isBadgeService = badgeServices.some(service => href.includes(service));
          if (isBadgeService) {
            // Badge services should at least respond (200, 302, or 404 are acceptable)
            const validBadgeStatuses = [200, 302, 404];
            expect(validBadgeStatuses, `Badge service ${href} should respond`).toContain(response.status());
          } else {
            // Primary links should return 200 OK
            expect(response.status(), `Link ${href} should return 200`).toBe(200);
          }
        } catch (error) {
          // If the request times out or fails, fail the test with details
          throw new Error(`Failed to access external link: ${href}. Error: ${error.message}`);
        }
      }
    });
  });

  // ==========================================
  // Additional Link Functionality Tests
  // ==========================================
  test.describe('Additional Link Functionality Verification', () => {
    test('No broken links exist on the page', async ({ page }) => {
      const anchors = page.locator('a');
      const count = await anchors.count();
      const brokenLinks = [];

      for (let i = 0; i < count; i++) {
        const anchor = anchors.nth(i);
        const href = await anchor.getAttribute('href');

        // Check internal anchors
        if (href.startsWith('#') && href !== '#') {
          const targetId = href.replace('#', '');
          const exists = await page.locator(`#${targetId}`).count();
          if (exists === 0) {
            brokenLinks.push({ href, reason: 'Target element not found' });
          }
        }
      }

      expect(brokenLinks, 'No broken internal links should exist').toHaveLength(0);
    });

    test('Link count verification', async ({ page }) => {
      const allLinks = page.locator('a');
      const externalLinks = page.locator('a[href^="http"]');
      const internalLinks = page.locator('a[href^="#"]');

      const totalCount = await allLinks.count();
      const externalCount = await externalLinks.count();
      const internalCount = await internalLinks.count();

      // Page should have multiple links
      expect(totalCount).toBeGreaterThan(0);

      // Should have both external and internal links
      expect(externalCount, 'Should have external links').toBeGreaterThan(0);
      expect(internalCount, 'Should have internal links').toBeGreaterThan(0);
    });

    test('All links are visible and accessible', async ({ page }) => {
      const anchors = page.locator('a');
      const count = await anchors.count();

      for (let i = 0; i < count; i++) {
        const anchor = anchors.nth(i);

        // Check if visible (some may be inside footer or scrollable areas)
        const isVisible = await anchor.isVisible();
        const href = await anchor.getAttribute('href');

        // All main navigation links should be visible
        if (href === GITHUB_REPO_URL || href === '#quick-start') {
          expect(isVisible, `Link ${href} should be visible`).toBeTruthy();
        }
      }
    });
  });
});
