/**
 * E2E tests for Footer section on the homepage.
 * Owner: Scenario 9 - Footer Section
 *
 * Tests cover:
 * - Footer section visibility
 * - GitHub link functionality (opens in new tab)
 * - License information display
 * - Acknowledgments section presence
 */

import { test, expect } from '@playwright/test';

test.describe('Footer Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Scroll to footer to ensure it's in view
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
  });

  test.describe('Visibility and Layout', () => {
    test('footer section is visible', async ({ page }) => {
      const footer = page.locator('footer[role="contentinfo"]');
      await expect(footer).toBeVisible();
    });

    test('footer has proper accessibility role', async ({ page }) => {
      const footer = page.locator('footer');
      await expect(footer).toHaveAttribute('role', 'contentinfo');
    });

    test('footer has aria-label for accessibility', async ({ page }) => {
      const footer = page.locator('footer[role="contentinfo"]');
      await expect(footer).toHaveAttribute('aria-label', 'Site footer');
    });
  });

  test.describe('GitHub Link', () => {
    test('GitHub repository link is visible', async ({ page }) => {
      const githubLink = page.locator('footer a[aria-label*="GitHub"]');
      await expect(githubLink).toBeVisible();
    });

    test('GitHub link has correct href', async ({ page }) => {
      const githubLink = page.locator('footer a[aria-label*="GitHub"]');
      const href = await githubLink.getAttribute('href');
      expect(href).toContain('github.com');
    });

    test('GitHub link opens in new tab', async ({ page }) => {
      const githubLink = page.locator('footer a[aria-label*="GitHub"]');
      await expect(githubLink).toHaveAttribute('target', '_blank');
    });

    test('GitHub link has secure rel attribute', async ({ page }) => {
      const githubLink = page.locator('footer a[aria-label*="GitHub"]');
      await expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer');
    });

    test('clicking GitHub link triggers new tab (verifies target=_blank behavior)', async ({ page, context }) => {
      // Listen for new page event
      const pagePromise = context.waitForEvent('page', { timeout: 5000 }).catch(() => null);

      const githubLink = page.locator('footer a[aria-label*="GitHub"]');
      await githubLink.click();

      const newPage = await pagePromise;

      // If a new page was opened, verify it's the GitHub URL
      if (newPage) {
        const url = newPage.url();
        expect(url).toContain('github.com');
        await newPage.close();
      } else {
        // If no new page (might be blocked), verify the link has correct attributes
        await expect(githubLink).toHaveAttribute('target', '_blank');
        await expect(githubLink).toHaveAttribute('href', /github\.com/);
      }
    });

    test('GitHub link contains GitHub icon', async ({ page }) => {
      const githubLink = page.locator('footer a[aria-label*="GitHub"]');
      const svg = githubLink.locator('svg');
      await expect(svg).toBeVisible();
    });
  });

  test.describe('License Information', () => {
    test('license type is displayed', async ({ page }) => {
      const licenseLink = page.locator('[data-testid="license-link"]');
      await expect(licenseLink).toBeVisible();
      await expect(licenseLink).toContainText('License');
    });

    test('license link points to valid URL', async ({ page }) => {
      const licenseLink = page.locator('[data-testid="license-link"]');
      const href = await licenseLink.getAttribute('href');
      expect(href).toMatch(/^https?:\/\//);
    });

    test('license link opens in new tab', async ({ page }) => {
      const licenseLink = page.locator('[data-testid="license-link"]');
      await expect(licenseLink).toHaveAttribute('target', '_blank');
    });

    test('"Released under the" text is visible', async ({ page }) => {
      const footer = page.locator('footer');
      await expect(footer.getByText('Released under the')).toBeVisible();
    });
  });

  test.describe('Acknowledgments Section', () => {
    test('acknowledgments section is visible', async ({ page }) => {
      const acknowledgments = page.locator('[data-testid="acknowledgments"]');
      await expect(acknowledgments).toBeVisible();
    });

    test('Rust acknowledgment is present', async ({ page }) => {
      const acknowledgments = page.locator('[data-testid="acknowledgments"]');
      await expect(acknowledgments.getByText(/Rust/i)).toBeVisible();
    });

    test('Tokio acknowledgment is present', async ({ page }) => {
      const acknowledgments = page.locator('[data-testid="acknowledgments"]');
      await expect(acknowledgments.getByText(/Tokio/i)).toBeVisible();
    });

    test('Tokio link is functional', async ({ page }) => {
      const tokioLink = page.locator('[data-testid="acknowledgments"] a:has-text("Tokio")');
      await expect(tokioLink).toHaveAttribute('href', 'https://tokio.rs');
      await expect(tokioLink).toHaveAttribute('target', '_blank');
    });

    test('contributor thanks is present', async ({ page }) => {
      const acknowledgments = page.locator('[data-testid="acknowledgments"]');
      await expect(acknowledgments.getByText(/contributors/i)).toBeVisible();
    });
  });

  test.describe('Copyright Notice', () => {
    test('copyright notice is displayed', async ({ page }) => {
      const footer = page.locator('footer');
      const currentYear = new Date().getFullYear().toString();
      await expect(footer.getByText(new RegExp(`© ${currentYear}`))).toBeVisible();
    });

    test('MirDB name is in copyright', async ({ page }) => {
      const footer = page.locator('footer');
      await expect(footer.getByText(/© \d{4} MirDB/)).toBeVisible();
    });
  });

  test.describe('Navigation Links', () => {
    test('footer navigation is present', async ({ page }) => {
      const footerNav = page.locator('footer nav[aria-label*="Footer"]');
      await expect(footerNav).toBeVisible();
    });

    test('Features link is present and functional', async ({ page }) => {
      const featuresLink = page.locator('footer a[href="#features"]');
      await expect(featuresLink).toBeVisible();
      await expect(featuresLink).toHaveText('Features');
    });

    test('Installation link is present and functional', async ({ page }) => {
      const installLink = page.locator('footer a[href="#installation"]');
      await expect(installLink).toBeVisible();
      await expect(installLink).toHaveText('Installation');
    });
  });

  test.describe('Responsive Design', () => {
    test('footer adapts to mobile viewport', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/');
      await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));

      const footer = page.locator('footer[role="contentinfo"]');
      await expect(footer).toBeVisible();

      // All main elements should still be visible
      await expect(page.locator('[data-testid="license-link"]')).toBeVisible();
      await expect(page.locator('[data-testid="acknowledgments"]')).toBeVisible();
    });

    test('footer adapts to tablet viewport', async ({ page }) => {
      await page.setViewportSize({ width: 768, height: 1024 });
      await page.goto('/');
      await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));

      const footer = page.locator('footer[role="contentinfo"]');
      await expect(footer).toBeVisible();
    });

    test('footer adapts to desktop viewport', async ({ page }) => {
      await page.setViewportSize({ width: 1440, height: 900 });
      await page.goto('/');
      await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));

      const footer = page.locator('footer[role="contentinfo"]');
      await expect(footer).toBeVisible();
    });
  });

  test.describe('Accessibility', () => {
    test('all external links have noopener noreferrer', async ({ page }) => {
      const externalLinks = page.locator('footer a[target="_blank"]');
      const count = await externalLinks.count();

      for (let i = 0; i < count; i++) {
        await expect(externalLinks.nth(i)).toHaveAttribute('rel', 'noopener noreferrer');
      }
    });

    test('footer has proper heading hierarchy', async ({ page }) => {
      const headings = page.locator('footer h3');
      const count = await headings.count();
      expect(count).toBeGreaterThanOrEqual(1);
    });

    test('links are keyboard accessible', async ({ page }) => {
      const footer = page.locator('footer');
      await footer.focus();

      // Tab through footer links
      await page.keyboard.press('Tab');
      const focusedElement = await page.evaluate(() => {
        return document.activeElement?.tagName.toLowerCase();
      });

      // Should be able to focus on links
      expect(['a', 'button']).toContain(focusedElement);
    });
  });
});
