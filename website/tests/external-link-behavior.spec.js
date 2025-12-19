// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

// Base URL for the static HTML file
const BASE_URL = 'file://' + path.resolve(__dirname, '../dist/index.html');

/**
 * External Link Behavior Tests
 *
 * Scenario: Verify external links open in new tabs and preserve user's place
 *
 * Test Cases:
 * 1. GitHub repository link opens in new tab (target='_blank'), original page preserved
 * 2. Documentation links open in new tabs
 * 3. External links have rel='noopener noreferrer' for security
 */
test.describe('External Link Behavior', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(BASE_URL);
  });

  test.describe('Test Case 1: GitHub Repository Link', () => {
    test('Hero section GitHub link opens in new tab', async ({ page }) => {
      // Find the GitHub button in the hero section
      const githubButton = page.locator('[data-testid="cta-github"]');
      await expect(githubButton).toBeVisible();

      // Verify it contains GitHub text
      await expect(githubButton).toContainText('GitHub');

      // Verify it links to the MirDB repository
      await expect(githubButton).toHaveAttribute('href', 'https://github.com/pjzhong/mirdb');

      // Verify it opens in a new tab (target='_blank')
      await expect(githubButton).toHaveAttribute('target', '_blank');
    });

    test('Footer GitHub link opens in new tab', async ({ page }) => {
      // Find the GitHub link in the footer
      const footerGithubLink = page.locator('[data-testid="footer-github-link"]');
      await expect(footerGithubLink).toBeVisible();

      // Verify it links to GitHub
      await expect(footerGithubLink).toHaveAttribute('href', /github\.com.*mirdb/);

      // Verify it opens in a new tab (target='_blank')
      await expect(footerGithubLink).toHaveAttribute('target', '_blank');
    });

    test('Original page is preserved after clicking external link', async ({ page, context }) => {
      // Listen for new pages (tabs) being opened
      const pagePromise = context.waitForEvent('page');

      // Click the GitHub button in hero section
      const githubButton = page.locator('[data-testid="cta-github"]');
      await githubButton.click();

      // Wait for the new page to open
      const newPage = await pagePromise;

      // Verify the original page still exists and is accessible
      await expect(page.locator('[data-testid="hero-section"]')).toBeVisible();

      // Verify the new page opened (it may fail to load due to network, but it should have opened)
      expect(newPage).toBeTruthy();

      // Close the new page
      await newPage.close();
    });
  });

  test.describe('Test Case 2: Documentation Links', () => {
    test('Footer documentation link opens in new tab', async ({ page }) => {
      // Find the documentation link in the footer
      const docsLink = page.locator('[data-testid="footer-docs-link"]');
      await expect(docsLink).toBeVisible();

      // Verify it contains Documentation text
      await expect(docsLink).toContainText('Documentation');

      // Verify it has an href attribute
      const href = await docsLink.getAttribute('href');
      expect(href).toBeTruthy();
      expect(href).toContain('github.com');

      // Verify it opens in a new tab (target='_blank')
      await expect(docsLink).toHaveAttribute('target', '_blank');
    });

    test('Architecture documentation link opens in new tab', async ({ page }) => {
      // Find the architecture docs link
      const architectureDocsLink = page.locator('[data-testid="architecture-docs-link"]');
      await expect(architectureDocsLink).toBeVisible();

      // Verify it links to architecture documentation
      await expect(architectureDocsLink).toHaveAttribute('href', /github\.com.*mirdb.*architecture/);

      // Verify it opens in a new tab (target='_blank')
      await expect(architectureDocsLink).toHaveAttribute('target', '_blank');
    });

    test('Configuration documentation link opens in new tab', async ({ page }) => {
      // Find the configuration docs link
      const configDocsLink = page.locator('[data-testid="config-docs-link"]');
      await expect(configDocsLink).toBeVisible();

      // Verify it links to configuration documentation
      await expect(configDocsLink).toHaveAttribute('href', /github\.com.*mirdb.*configuration/);

      // Verify it opens in a new tab (target='_blank')
      await expect(configDocsLink).toHaveAttribute('target', '_blank');
    });

    test('All documentation links preserve original page', async ({ page, context }) => {
      // Get all documentation links
      const docLinks = [
        page.locator('[data-testid="footer-docs-link"]'),
        page.locator('[data-testid="architecture-docs-link"]'),
        page.locator('[data-testid="config-docs-link"]')
      ];

      for (const link of docLinks) {
        // Verify each link has target="_blank"
        await expect(link).toHaveAttribute('target', '_blank');
      }

      // Test clicking one to verify page preservation
      const pagePromise = context.waitForEvent('page');
      await docLinks[0].click();
      const newPage = await pagePromise;

      // Original page should still show the hero section
      await expect(page.locator('[data-testid="hero-section"]')).toBeVisible();

      await newPage.close();
    });
  });

  test.describe('Test Case 3: Security Attributes (rel="noopener noreferrer")', () => {
    test('Hero GitHub button has rel="noopener noreferrer"', async ({ page }) => {
      const githubButton = page.locator('[data-testid="cta-github"]');
      await expect(githubButton).toHaveAttribute('rel', 'noopener noreferrer');
    });

    test('Footer GitHub link has rel="noopener noreferrer"', async ({ page }) => {
      const footerGithubLink = page.locator('[data-testid="footer-github-link"]');
      const rel = await footerGithubLink.getAttribute('rel');
      expect(rel).toContain('noopener');
      expect(rel).toContain('noreferrer');
    });

    test('Footer documentation link has rel="noopener noreferrer"', async ({ page }) => {
      const docsLink = page.locator('[data-testid="footer-docs-link"]');
      const rel = await docsLink.getAttribute('rel');
      expect(rel).toContain('noopener');
      expect(rel).toContain('noreferrer');
    });

    test('Architecture documentation link has rel="noopener noreferrer"', async ({ page }) => {
      const architectureDocsLink = page.locator('[data-testid="architecture-docs-link"]');
      await expect(architectureDocsLink).toHaveAttribute('rel', 'noopener noreferrer');
    });

    test('Configuration documentation link has rel="noopener noreferrer"', async ({ page }) => {
      const configDocsLink = page.locator('[data-testid="config-docs-link"]');
      await expect(configDocsLink).toHaveAttribute('rel', 'noopener noreferrer');
    });

    test('All external links have proper security attributes', async ({ page }) => {
      // Get all external links (those with target="_blank")
      const externalLinks = await page.evaluate(() => {
        const links = document.querySelectorAll('a[target="_blank"]');
        return Array.from(links).map(link => ({
          href: link.getAttribute('href'),
          rel: link.getAttribute('rel'),
          testId: link.getAttribute('data-testid')
        }));
      });

      // Verify we have external links
      expect(externalLinks.length).toBeGreaterThan(0);

      // Verify each external link has noopener and noreferrer
      for (const link of externalLinks) {
        expect(link.rel, `Link to ${link.href} should have rel attribute`).toBeTruthy();
        expect(link.rel, `Link to ${link.href} should contain 'noopener'`).toContain('noopener');
        expect(link.rel, `Link to ${link.href} should contain 'noreferrer'`).toContain('noreferrer');
      }
    });
  });
});
