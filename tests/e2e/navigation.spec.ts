/**
 * Navigation and Links E2E Tests
 * Owner: Scenario 2 - Navigation and External Links
 *
 * Tests:
 * - GitHub repository link works and opens in new tab
 * - Documentation link works and opens in new tab
 * - Footer links are functional
 * - All external links have target="_blank" and rel="noopener"
 *
 * Traceability: REQ-5, REQ-6, NFR-5
 */
import { test, expect } from '@playwright/test';
import { waitForPageLoad, getByTestId, GITHUB_URL, DOCS_URL } from './test-utils';

test.describe('Navigation and External Links', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await waitForPageLoad(page);
  });

  test.describe('Test Case 1: GitHub repository link opens in new tab', () => {
    test('main GitHub link has target="_blank" attribute', async ({ page }) => {
      const githubLink = getByTestId(page, 'github-link');
      await expect(githubLink).toBeVisible();
      await expect(githubLink).toHaveAttribute('target', '_blank');
      await expect(githubLink).toHaveAttribute('href', GITHUB_URL);
    });

    test('View Source Code button has target="_blank" attribute', async ({ page }) => {
      const viewSourceBtn = getByTestId(page, 'view-source-btn');
      await expect(viewSourceBtn).toBeVisible();
      await expect(viewSourceBtn).toHaveAttribute('target', '_blank');
      await expect(viewSourceBtn).toHaveAttribute('href', GITHUB_URL);
    });
  });

  test.describe('Test Case 2: Documentation/README link opens in new tab', () => {
    test('documentation link has target="_blank" and points to README', async ({ page }) => {
      const docsLink = getByTestId(page, 'docs-link');
      await expect(docsLink).toBeVisible();
      await expect(docsLink).toHaveAttribute('target', '_blank');
      await expect(docsLink).toHaveAttribute('href', DOCS_URL);
    });

    test('Get Started button links to documentation', async ({ page }) => {
      const getStartedBtn = getByTestId(page, 'get-started-btn');
      await expect(getStartedBtn).toBeVisible();
      await expect(getStartedBtn).toHaveAttribute('target', '_blank');
      await expect(getStartedBtn).toHaveAttribute('href', DOCS_URL);
    });
  });

  test.describe('Test Case 3: External links have rel="noopener noreferrer" for security', () => {
    test('all external links have rel="noopener noreferrer"', async ({ page }) => {
      // Get all external links (links with target="_blank")
      const externalLinks = page.locator('a[target="_blank"]');
      const count = await externalLinks.count();

      expect(count).toBeGreaterThan(0);

      for (let i = 0; i < count; i++) {
        const link = externalLinks.nth(i);
        const rel = await link.getAttribute('rel');
        expect(rel).toContain('noopener');
        expect(rel).toContain('noreferrer');
      }
    });

    test('header GitHub link has rel="noopener noreferrer"', async ({ page }) => {
      const githubLink = getByTestId(page, 'github-link');
      await expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer');
    });

    test('header docs link has rel="noopener noreferrer"', async ({ page }) => {
      const docsLink = getByTestId(page, 'docs-link');
      await expect(docsLink).toHaveAttribute('rel', 'noopener noreferrer');
    });
  });

  test.describe('Test Case 4: Footer contains GitHub link', () => {
    test('footer section includes a link to the GitHub repository', async ({ page }) => {
      const footerGithubLink = getByTestId(page, 'footer-github-link');
      await expect(footerGithubLink).toBeVisible();
      await expect(footerGithubLink).toHaveAttribute('href', GITHUB_URL);
      await expect(footerGithubLink).toHaveAttribute('target', '_blank');
      await expect(footerGithubLink).toHaveAttribute('rel', 'noopener noreferrer');
    });

    test('footer documentation link is functional', async ({ page }) => {
      const footerDocsLink = getByTestId(page, 'footer-docs-link');
      await expect(footerDocsLink).toBeVisible();
      await expect(footerDocsLink).toHaveAttribute('href', DOCS_URL);
      await expect(footerDocsLink).toHaveAttribute('target', '_blank');
    });
  });

  test.describe('Test Case 5: Footer contains license information', () => {
    test('footer displays license type', async ({ page }) => {
      const footerLicense = getByTestId(page, 'footer-license');
      await expect(footerLicense).toBeVisible();

      // Check that license text contains MIT License
      const licenseText = await footerLicense.textContent();
      expect(licenseText).toContain('MIT License');
    });

    test('license link points to LICENSE file', async ({ page }) => {
      const licenseLink = page.locator('[data-testid="footer-license"] a');
      await expect(licenseLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb/blob/master/LICENSE');
      await expect(licenseLink).toHaveAttribute('target', '_blank');
      await expect(licenseLink).toHaveAttribute('rel', 'noopener noreferrer');
    });
  });

  test.describe('Test Case 6: Footer contains copyright notice', () => {
    test('footer includes copyright notice with current year', async ({ page }) => {
      const footerCopyright = getByTestId(page, 'footer-copyright');
      await expect(footerCopyright).toBeVisible();

      const copyrightText = await footerCopyright.textContent();
      // Check for copyright symbol and year
      expect(copyrightText).toContain('©');
      expect(copyrightText).toContain('2026');
      expect(copyrightText).toContain('MirDB');
    });
  });

  test.describe('Link Accessibility', () => {
    test('navigation links are keyboard accessible', async ({ page }) => {
      // Tab to documentation link
      await page.keyboard.press('Tab');
      await page.keyboard.press('Tab');
      await page.keyboard.press('Tab');

      // Check that a navigation link receives focus
      const focusedElement = page.locator(':focus');
      const tagName = await focusedElement.evaluate(el => el.tagName.toLowerCase());
      expect(tagName).toBe('a');
    });

    test('all links have visible focus indicators', async ({ page }) => {
      const githubLink = getByTestId(page, 'github-link');
      await githubLink.focus();

      // Check that focus produces a visible outline
      const outline = await githubLink.evaluate(el => {
        const style = window.getComputedStyle(el);
        return style.outline || style.outlineWidth;
      });

      // Should have some form of outline (not 'none' or '0px')
      expect(outline).not.toBe('none');
    });
  });
});
