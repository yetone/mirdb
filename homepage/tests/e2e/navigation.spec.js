/**
 * Navigation E2E Tests
 * Owner: Scenario 8 - Navigation and Footer
 *
 * Test cases:
 * - Navigation links present
 * - Smooth scroll to sections works
 * - Footer content displays correctly
 */

const { test, expect } = require('@playwright/test');

test.describe('Navigation and Footer', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for page to fully load
    await page.waitForLoadState('domcontentloaded');
  });

  test.describe('Navigation Bar', () => {
    test('should display navigation with logo, Features link, and Docs link', async ({ page }) => {
      // Check navigation bar exists
      const nav = page.locator('nav[role="navigation"]');
      await expect(nav).toBeVisible();

      // Check logo is present
      const logo = page.locator('.nav-logo');
      await expect(logo).toBeVisible();
      await expect(logo).toHaveText('MirDB');

      // Check Features link is present
      const featuresLink = page.locator('.nav-link[href="#features"]');
      await expect(featuresLink).toBeVisible();
      await expect(featuresLink).toHaveText('Features');

      // Check Docs link is present
      const docsLink = page.locator('.nav-link[href="https://docs.rs/mirdb"]');
      await expect(docsLink).toBeVisible();
      await expect(docsLink).toHaveText('Docs');
    });

    test('should have GitHub link in navigation', async ({ page }) => {
      const githubLink = page.locator('.nav-link[href="https://github.com/penberg/mirdb"]');
      await expect(githubLink).toBeVisible();
      await expect(githubLink).toHaveText('GitHub');
      await expect(githubLink).toHaveAttribute('target', '_blank');
      await expect(githubLink).toHaveAttribute('rel', 'noopener');
    });

    test('should have Quick Start link in navigation', async ({ page }) => {
      const quickStartLink = page.locator('.nav-link[href="#quickstart"]');
      await expect(quickStartLink).toBeVisible();
      await expect(quickStartLink).toHaveText('Quick Start');
    });

    test('should have accessible navigation structure', async ({ page }) => {
      const nav = page.locator('nav');
      await expect(nav).toHaveAttribute('role', 'navigation');
      await expect(nav).toHaveAttribute('aria-label', 'Main navigation');

      const header = page.locator('header.site-header');
      await expect(header).toHaveAttribute('role', 'banner');
    });
  });

  test.describe('Smooth Scroll Navigation', () => {
    test('should smoothly scroll to features section when Features link is clicked', async ({ page }) => {
      // Get initial scroll position
      const initialScrollY = await page.evaluate(() => window.scrollY);

      // Click Features link
      await page.locator('.nav-link[href="#features"]').click();

      // Wait for smooth scroll to complete
      await page.waitForTimeout(1000);

      // Check that page has scrolled
      const newScrollY = await page.evaluate(() => window.scrollY);
      expect(newScrollY).toBeGreaterThan(initialScrollY);

      // Check that features section is now in viewport
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeInViewport();
    });

    test('should smoothly scroll to quickstart section when Quick Start link is clicked', async ({ page }) => {
      // Get initial scroll position
      const initialScrollY = await page.evaluate(() => window.scrollY);

      // Click Quick Start link
      await page.locator('.nav-link[href="#quickstart"]').click();

      // Wait for smooth scroll to complete
      await page.waitForTimeout(1000);

      // Check that page has scrolled
      const newScrollY = await page.evaluate(() => window.scrollY);
      expect(newScrollY).toBeGreaterThan(initialScrollY);

      // Check that quickstart section is now in viewport
      const quickstartSection = page.locator('#quickstart');
      await expect(quickstartSection).toBeInViewport();
    });
  });

  test.describe('Footer Section', () => {
    test('should display footer with copyright notice containing current year', async ({ page }) => {
      const footer = page.locator('footer.site-footer');
      await expect(footer).toBeVisible();

      const copyright = page.locator('.footer-copyright');
      await expect(copyright).toBeVisible();

      // Check that copyright contains the current year
      const currentYear = new Date().getFullYear().toString();
      await expect(copyright).toContainText(currentYear);
      await expect(copyright).toContainText('MirDB');
    });

    test('should display license information in footer', async ({ page }) => {
      const footer = page.locator('footer.site-footer');
      await expect(footer).toBeVisible();

      // Check for MIT License text
      const copyright = page.locator('.footer-copyright');
      await expect(copyright).toContainText('MIT License');
    });

    test('should contain links to GitHub and documentation in footer', async ({ page }) => {
      const footer = page.locator('footer.site-footer');
      await expect(footer).toBeVisible();

      // Check for GitHub Repository link
      const githubLink = footer.locator('a[href="https://github.com/penberg/mirdb"]');
      await expect(githubLink).toBeVisible();

      // Check for Documentation link
      const docsLink = footer.locator('a[href="https://docs.rs/mirdb"]');
      await expect(docsLink).toBeVisible();
    });

    test('should have proper footer sections with titles', async ({ page }) => {
      // Check for Resources section
      const resourcesTitle = page.locator('.footer-section__title:has-text("Resources")');
      await expect(resourcesTitle).toBeVisible();

      // Check for Project section
      const projectTitle = page.locator('.footer-section__title:has-text("Project")');
      await expect(projectTitle).toBeVisible();
    });

    test('should have accessible footer structure', async ({ page }) => {
      const footer = page.locator('footer.site-footer');
      await expect(footer).toHaveAttribute('role', 'contentinfo');
    });

    test('should have working internal links in footer', async ({ page }) => {
      // Click Features link in footer
      await page.locator('.footer-link[href="#features"]').click();

      // Wait for scroll
      await page.waitForTimeout(1000);

      // Check that features section is in viewport
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeInViewport();
    });

    test('should have external links that open in new tabs', async ({ page }) => {
      const footer = page.locator('footer.site-footer');

      // Check GitHub link opens in new tab
      const githubLink = footer.locator('a[href="https://github.com/penberg/mirdb"]');
      await expect(githubLink).toHaveAttribute('target', '_blank');
      await expect(githubLink).toHaveAttribute('rel', 'noopener');

      // Check Docs link opens in new tab
      const docsLink = footer.locator('a[href="https://docs.rs/mirdb"]');
      await expect(docsLink).toHaveAttribute('target', '_blank');
      await expect(docsLink).toHaveAttribute('rel', 'noopener');
    });
  });

  test.describe('Footer Brand', () => {
    test('should display footer logo and tagline', async ({ page }) => {
      const footerLogo = page.locator('.footer-logo');
      await expect(footerLogo).toBeVisible();
      await expect(footerLogo).toHaveText('MirDB');

      const footerTagline = page.locator('.footer-tagline');
      await expect(footerTagline).toBeVisible();
      await expect(footerTagline).toContainText('persistent key-value store');
    });
  });
});
