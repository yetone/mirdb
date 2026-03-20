/**
 * Navigation E2E Tests
 * Owner: Scenario 7 - Navigation and Repository Links
 *
 * Test cases:
 * - Navigation links scroll to sections smoothly
 * - GitHub link points to correct repository
 * - Footer contains repository and license links
 */

const { test, expect } = require('@playwright/test');

test.describe('Navigation and Repository Links', () => {

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('Section Navigation', () => {

    test('clicking navigation link to features section scrolls smoothly', async ({ page }) => {
      // Get initial scroll position
      const initialScrollY = await page.evaluate(() => window.scrollY);

      // Click the Features navigation link
      await page.click('[data-testid="nav-features"]');

      // Wait for scroll to complete
      await page.waitForTimeout(1000);

      // Verify page has scrolled
      const newScrollY = await page.evaluate(() => window.scrollY);
      expect(newScrollY).toBeGreaterThan(initialScrollY);

      // Verify the features section is now in view
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeInViewport();
    });

    test('clicking Demo navigation link scrolls to demo section', async ({ page }) => {
      await page.click('[data-testid="nav-demo"]');
      await page.waitForTimeout(1000);

      const demoSection = page.locator('#demo');
      await expect(demoSection).toBeInViewport();
    });

    test('clicking Quick Start navigation link scrolls to quickstart section', async ({ page }) => {
      await page.click('[data-testid="nav-quickstart"]');
      await page.waitForTimeout(1000);

      const quickstartSection = page.locator('#quickstart');
      await expect(quickstartSection).toBeInViewport();
    });

    test('navigation updates URL hash on click', async ({ page }) => {
      await page.click('[data-testid="nav-features"]');
      await page.waitForTimeout(500);

      const url = page.url();
      expect(url).toContain('#features');
    });

  });

  test.describe('GitHub Repository Link', () => {

    test('clicking GitHub link in header navigates to MirDB repository', async ({ page }) => {
      const githubLink = page.locator('[data-testid="nav-github"]');

      // Verify link exists
      await expect(githubLink).toBeVisible();

      // Verify link has correct href
      const href = await githubLink.getAttribute('href');
      expect(href).toContain('github.com');
      expect(href).toContain('mirdb');
    });

    test('GitHub link opens in new tab', async ({ page }) => {
      const githubLink = page.locator('[data-testid="nav-github"]');

      const target = await githubLink.getAttribute('target');
      expect(target).toBe('_blank');

      const rel = await githubLink.getAttribute('rel');
      expect(rel).toContain('noopener');
    });

    test('GitHub link URL points to correct MirDB repository', async ({ page }) => {
      const githubLink = page.locator('[data-testid="nav-github"]');
      const href = await githubLink.getAttribute('href');

      // Should contain the repository path
      expect(href).toMatch(/github\.com\/.*\/mirdb/i);
    });

  });

  test.describe('Footer Links', () => {

    test('footer contains link to source repository', async ({ page }) => {
      // Scroll to footer to ensure it's loaded
      await page.evaluate(() => {
        document.getElementById('footer').scrollIntoView();
      });

      const footerGithub = page.locator('[data-testid="footer-github"]');
      await expect(footerGithub).toBeVisible();

      const href = await footerGithub.getAttribute('href');
      expect(href).toContain('github.com');
      expect(href).toContain('mirdb');
    });

    test('footer displays or links to license information', async ({ page }) => {
      await page.evaluate(() => {
        document.getElementById('footer').scrollIntoView();
      });

      // Check for license link in footer
      const licenseLink = page.locator('[data-testid="footer-license"]');
      await expect(licenseLink).toBeVisible();

      const href = await licenseLink.getAttribute('href');
      expect(href).toContain('LICENSE');
    });

    test('footer copyright includes license link', async ({ page }) => {
      await page.evaluate(() => {
        document.getElementById('footer').scrollIntoView();
      });

      const licenseLinkInCopyright = page.locator('[data-testid="footer-license-link"]');
      await expect(licenseLinkInCopyright).toBeVisible();

      const text = await licenseLinkInCopyright.textContent();
      expect(text.toLowerCase()).toContain('mit');
    });

    test('footer contains issues link', async ({ page }) => {
      await page.evaluate(() => {
        document.getElementById('footer').scrollIntoView();
      });

      const issuesLink = page.locator('[data-testid="footer-issues"]');
      await expect(issuesLink).toBeVisible();

      const href = await issuesLink.getAttribute('href');
      expect(href).toContain('issues');
    });

    test('footer GitHub icon links to repository', async ({ page }) => {
      await page.evaluate(() => {
        document.getElementById('footer').scrollIntoView();
      });

      const githubIcon = page.locator('[data-testid="footer-github-icon"]');
      await expect(githubIcon).toBeVisible();

      const href = await githubIcon.getAttribute('href');
      expect(href).toContain('github.com');
      expect(href).toContain('mirdb');
    });

  });

  test.describe('Navigation Accessibility', () => {

    test('navigation has proper ARIA labels', async ({ page }) => {
      const nav = page.locator('#main-nav');
      const ariaLabel = await nav.getAttribute('aria-label');
      expect(ariaLabel).toBeTruthy();
    });

    test('mobile menu toggle has proper ARIA attributes', async ({ page }) => {
      const menuToggle = page.locator('#menu-toggle');

      const ariaLabel = await menuToggle.getAttribute('aria-label');
      expect(ariaLabel).toBeTruthy();

      const ariaExpanded = await menuToggle.getAttribute('aria-expanded');
      expect(ariaExpanded).toBe('false');

      const ariaControls = await menuToggle.getAttribute('aria-controls');
      expect(ariaControls).toBe('main-nav');
    });

    test('navigation links are keyboard accessible', async ({ page }) => {
      // Focus on the first nav link
      const firstLink = page.locator('[data-testid="nav-features"]');
      await firstLink.focus();

      // Verify it can receive focus
      const isFocused = await firstLink.evaluate(el => el === document.activeElement);
      expect(isFocused).toBe(true);

      // Tab to next link
      await page.keyboard.press('Tab');

      const secondLink = page.locator('[data-testid="nav-demo"]');
      const isSecondFocused = await secondLink.evaluate(el => el === document.activeElement);
      expect(isSecondFocused).toBe(true);
    });

  });

  test.describe('Mobile Navigation', () => {

    test.use({ viewport: { width: 375, height: 667 } });

    test('mobile menu toggle is visible on small screens', async ({ page }) => {
      const menuToggle = page.locator('#menu-toggle');
      await expect(menuToggle).toBeVisible();
    });

    test('clicking mobile menu toggle opens navigation', async ({ page }) => {
      const menuToggle = page.locator('#menu-toggle');
      const nav = page.locator('#main-nav');

      // Initially nav should not have open class
      await expect(nav).not.toHaveClass(/header__nav--open/);

      // Click toggle
      await menuToggle.click();
      await page.waitForTimeout(300);

      // Nav should now have open class
      await expect(nav).toHaveClass(/header__nav--open/);

      // ARIA expanded should be true
      const ariaExpanded = await menuToggle.getAttribute('aria-expanded');
      expect(ariaExpanded).toBe('true');
    });

    test('clicking mobile menu toggle closes navigation when open', async ({ page }) => {
      const menuToggle = page.locator('#menu-toggle');
      const nav = page.locator('#main-nav');

      // Open menu
      await menuToggle.click();
      await page.waitForTimeout(300);
      await expect(nav).toHaveClass(/header__nav--open/);

      // Close menu
      await menuToggle.click();
      await page.waitForTimeout(300);
      await expect(nav).not.toHaveClass(/header__nav--open/);
    });

    test('navigation closes when clicking a link on mobile', async ({ page }) => {
      const menuToggle = page.locator('#menu-toggle');
      const nav = page.locator('#main-nav');

      // Open menu
      await menuToggle.click();
      await page.waitForTimeout(300);

      // Click a nav link
      await page.click('[data-testid="nav-features"]');
      await page.waitForTimeout(500);

      // Menu should close
      await expect(nav).not.toHaveClass(/header__nav--open/);
    });

  });

  test.describe('Header Scroll Behavior', () => {

    test('header gets shadow class when scrolled', async ({ page }) => {
      const header = page.locator('#header');

      // Initially no scrolled class
      await expect(header).not.toHaveClass(/header--scrolled/);

      // Scroll down
      await page.evaluate(() => window.scrollBy(0, 100));
      await page.waitForTimeout(200);

      // Should have scrolled class
      await expect(header).toHaveClass(/header--scrolled/);
    });

    test('header is fixed at top', async ({ page }) => {
      const header = page.locator('#header');

      // Check position is fixed
      const position = await header.evaluate(el => window.getComputedStyle(el).position);
      expect(position).toBe('fixed');

      // Check top is 0
      const top = await header.evaluate(el => window.getComputedStyle(el).top);
      expect(top).toBe('0px');
    });

  });

});
