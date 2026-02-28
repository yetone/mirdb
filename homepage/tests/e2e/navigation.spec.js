/**
 * E2E tests for navigation
 * Owner: Scenario 4 - Navigation and External Links
 *
 * Tests: Header navigation, footer links, external link attributes,
 * mobile menu functionality
 */

const { test, expect } = require('@playwright/test');

test.describe('Navigation and External Links', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('Header Navigation', () => {
    test('TC1: header element exists with navigation content', async ({ page }) => {
      const header = page.locator('header');
      await expect(header).toBeVisible();

      const nav = header.locator('nav');
      await expect(nav).toBeVisible();
    });

    test('TC2: GitHub link exists in header', async ({ page }) => {
      const header = page.locator('header');
      const githubLink = header.locator('a[href*="github.com"]');
      await expect(githubLink).toBeVisible();
    });

    test('TC3: GitHub link URL is correct', async ({ page }) => {
      const header = page.locator('header');
      const githubLink = header.locator('a[href*="github.com"]');
      const href = await githubLink.getAttribute('href');
      expect(href).toBe('https://github.com/yetone/mirdb');
    });

    test('TC4: GitHub link opens in new tab', async ({ page }) => {
      const header = page.locator('header');
      const githubLink = header.locator('a[href*="github.com"]');
      const target = await githubLink.getAttribute('target');
      expect(target).toBe('_blank');
    });

    test('TC5: GitHub link has security attributes', async ({ page }) => {
      const header = page.locator('header');
      const githubLink = header.locator('a[href*="github.com"]');
      const rel = await githubLink.getAttribute('rel');
      expect(rel).toContain('noopener');
    });

    test('header contains navigation links to sections', async ({ page }) => {
      const nav = page.locator('header nav');

      const featuresLink = nav.locator('a[href="#features"]');
      await expect(featuresLink).toBeVisible();

      const statusLink = nav.locator('a[href="#status"]');
      await expect(statusLink).toBeVisible();

      const usageLink = nav.locator('a[href="#usage"]');
      await expect(usageLink).toBeVisible();

      const quickstartLink = nav.locator('a[href="#quickstart"]');
      await expect(quickstartLink).toBeVisible();
    });
  });

  test.describe('Footer Navigation', () => {
    test('TC6: footer element exists', async ({ page }) => {
      const footer = page.locator('footer');
      await expect(footer).toBeVisible();
    });

    test('TC7: documentation link exists', async ({ page }) => {
      const footer = page.locator('footer');
      const docsLink = footer.locator('a[href*="github.com"][href*="readme" i], a[href*="github.com/yetone/mirdb#"], a:has-text("Documentation"), a:has-text("README")');
      await expect(docsLink.first()).toBeVisible();
    });

    test('TC8: issues/discussions link exists', async ({ page }) => {
      const footer = page.locator('footer');
      const issuesLink = footer.locator('a[href*="issues"], a[href*="discussions"]');
      await expect(issuesLink.first()).toBeVisible();
    });

    test('TC9: license information is present in footer', async ({ page }) => {
      const footer = page.locator('footer');
      // Check for text containing "License" or link to license
      const licenseText = footer.locator('text=/[Ll]icense/');
      const licenseLink = footer.locator('a[href*="LICENSE"]');
      const hasLicenseText = await licenseText.count() > 0;
      const hasLicenseLink = await licenseLink.count() > 0;
      expect(hasLicenseText || hasLicenseLink).toBe(true);
    });

    test('footer contains GitHub link', async ({ page }) => {
      const footer = page.locator('footer');
      const githubLink = footer.locator('a[href*="github.com"]');
      await expect(githubLink.first()).toBeVisible();
    });
  });

  test.describe('External Link Security', () => {
    test('TC10: all external links have proper security attributes', async ({ page }) => {
      // Get all external links (links that start with http and are not to the same domain)
      const externalLinks = page.locator('a[href^="http"]');
      const count = await externalLinks.count();

      expect(count).toBeGreaterThan(0);

      for (let i = 0; i < count; i++) {
        const link = externalLinks.nth(i);
        const href = await link.getAttribute('href');

        // Skip if it's a same-domain link
        if (href && !href.includes('localhost') && !href.includes('127.0.0.1')) {
          const target = await link.getAttribute('target');
          const rel = await link.getAttribute('rel');

          expect(target, `Link ${href} should have target="_blank"`).toBe('_blank');
          expect(rel, `Link ${href} should contain "noopener"`).toContain('noopener');
        }
      }
    });

    test('header GitHub link has all security attributes', async ({ page }) => {
      const githubLink = page.locator('header a[href="https://github.com/yetone/mirdb"]');

      const target = await githubLink.getAttribute('target');
      const rel = await githubLink.getAttribute('rel');

      expect(target).toBe('_blank');
      expect(rel).toContain('noopener');
      expect(rel).toContain('noreferrer');
    });
  });

  test.describe('Mobile Menu', () => {
    test('mobile menu toggle button exists on mobile viewport', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });

      const menuToggle = page.locator('.mobile-menu-toggle, .menu-toggle, button[aria-label*="menu" i]');
      // The toggle should exist
      await expect(menuToggle).toHaveCount(1);
    });

    test('mobile menu toggles visibility when clicked', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });

      const menuToggle = page.locator('.mobile-menu-toggle, .menu-toggle, button[aria-label*="menu" i]');
      const nav = page.locator('header nav');

      // Toggle should show menu
      await menuToggle.click();
      await expect(nav).toHaveClass(/open|active|visible|show/);

      // Toggle again should hide menu
      await menuToggle.click();
      await expect(nav).not.toHaveClass(/open|active|visible|show/);
    });
  });

  test.describe('Smooth Scrolling', () => {
    test('clicking navigation link scrolls to section', async ({ page }) => {
      const featuresLink = page.locator('header nav a[href="#features"]');

      await featuresLink.click();

      // Wait for smooth scroll to complete
      await page.waitForTimeout(500);

      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeInViewport();
    });
  });
});
