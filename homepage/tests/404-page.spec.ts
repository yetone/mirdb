import { test, expect } from '@playwright/test';

test.describe('Error Handling - 404 Page', () => {
  test.describe('TC1: Custom 404 page display', () => {
    test('navigating to non-existent URL shows custom 404 page', async ({ page }) => {
      // Navigate to a non-existent page
      await page.goto('/nonexistent-page-xyz');

      // Verify custom 404 page is displayed (not browser default)
      const notFoundSection = page.locator('[data-testid="not-found-section"]');
      await expect(notFoundSection).toBeVisible();

      // Verify 404 heading is present
      const heading = page.locator('h1');
      await expect(heading).toContainText('404');
    });

    test('404 page displays user-friendly message', async ({ page }) => {
      await page.goto('/nonexistent-page-xyz');

      // Verify user-friendly message is displayed
      const message = page.locator('[data-testid="not-found-message"]');
      await expect(message).toBeVisible();
      await expect(message).toContainText(/page.*not found|couldn.*t find/i);
    });
  });

  test.describe('TC2: Homepage link on 404 page', () => {
    test('404 page contains a link to return to homepage', async ({ page }) => {
      await page.goto('/nonexistent-page-xyz');

      // Verify homepage link is present
      const homepageLink = page.locator('[data-testid="back-to-home"]');
      await expect(homepageLink).toBeVisible();
      await expect(homepageLink).toHaveAttribute('href', '/');
    });

    test('clicking homepage link navigates to homepage', async ({ page }) => {
      await page.goto('/nonexistent-page-xyz');

      // Click on homepage link
      const homepageLink = page.locator('[data-testid="back-to-home"]');
      await homepageLink.click();

      // Verify navigation to homepage
      await expect(page).toHaveURL('/');

      // Verify hero section is visible (confirming we're on homepage)
      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();
    });
  });

  test.describe('TC3: 404 page maintains site styling', () => {
    test('404 page uses the same design system as main site', async ({ page }) => {
      await page.goto('/nonexistent-page-xyz');

      // Verify navigation bar is present
      const navbar = page.locator('[data-testid="navbar"]');
      await expect(navbar).toBeVisible();

      // Verify MirDB logo/brand is present in nav
      const navLogo = page.locator('.nav-logo');
      await expect(navLogo).toBeVisible();
      await expect(navLogo).toContainText('MirDB');
    });

    test('404 page has consistent fonts and colors', async ({ page }) => {
      // First visit homepage to capture styles
      await page.goto('/');
      const homepageBody = page.locator('body');
      const homepageFontFamily = await homepageBody.evaluate(
        (el) => getComputedStyle(el).fontFamily
      );

      // Visit 404 page
      await page.goto('/nonexistent-page-xyz');
      const notFoundBody = page.locator('body');
      const notFoundFontFamily = await notFoundBody.evaluate(
        (el) => getComputedStyle(el).fontFamily
      );

      // Verify consistent font family
      expect(notFoundFontFamily).toBe(homepageFontFamily);
    });

    test('404 page includes footer section', async ({ page }) => {
      await page.goto('/nonexistent-page-xyz');

      // Verify footer is present
      const footer = page.locator('[data-testid="footer-section"]');
      await expect(footer).toBeVisible();
    });
  });

  test.describe('Responsive 404 Page', () => {
    test('404 page is responsive on mobile viewport', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/nonexistent-page-xyz');

      // Verify 404 content is visible on mobile
      const notFoundSection = page.locator('[data-testid="not-found-section"]');
      await expect(notFoundSection).toBeVisible();

      const homepageLink = page.locator('[data-testid="back-to-home"]');
      await expect(homepageLink).toBeVisible();
    });

    test('404 page is responsive on tablet viewport', async ({ page }) => {
      await page.setViewportSize({ width: 768, height: 1024 });
      await page.goto('/nonexistent-page-xyz');

      // Verify 404 content is visible on tablet
      const notFoundSection = page.locator('[data-testid="not-found-section"]');
      await expect(notFoundSection).toBeVisible();

      const homepageLink = page.locator('[data-testid="back-to-home"]');
      await expect(homepageLink).toBeVisible();
    });
  });
});
