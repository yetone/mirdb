import { test, expect } from '@playwright/test';

/**
 * Navigation to Registration - E2E Tests
 *
 * This test file covers the scenario: "Verify users can navigate from homepage
 * to registration page via CTA buttons as specified in US-4"
 *
 * Test Cases:
 * 1. E2E: Click 'Get Started Free' button in hero section → navigates to /register
 * 2. Integration: Verify button uses React Router Link → navigation without page reload
 * 3. E2E: Check Get Started button in footer CTA → navigates to /register
 */

test.describe('Navigation to Registration - E2E Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: E2E Test
   * Input: Click 'Get Started Free' button in hero section
   * Expected: User is navigated to /register route
   */
  test.describe('Test Case 1: Hero Section CTA Navigation', () => {
    test('Get Started Free button is visible in hero section', async ({ page }) => {
      const getStartedButton = page.getByTestId('cta-get-started');

      await expect(getStartedButton).toBeVisible();
      await expect(getStartedButton).toHaveText('Get Started Free');
    });

    test('clicking Get Started Free button navigates to /register route', async ({ page }) => {
      const getStartedButton = page.getByTestId('cta-get-started');

      await getStartedButton.click();

      await expect(page).toHaveURL(/\/register$/);
    });

    test('Get Started Free button has correct href attribute', async ({ page }) => {
      const getStartedButton = page.getByTestId('cta-get-started');

      await expect(getStartedButton).toHaveAttribute('href', '/register');
    });

    test('Get Started Free button is prominently displayed', async ({ page }) => {
      const getStartedButton = page.getByTestId('cta-get-started');

      await expect(getStartedButton).toBeVisible();
      await expect(getStartedButton).toBeEnabled();
      await getStartedButton.focus();
      await expect(getStartedButton).toBeFocused();
    });

    test('Get Started Free button has primary styling', async ({ page }) => {
      const getStartedButton = page.getByTestId('cta-get-started');

      await expect(getStartedButton).toBeVisible();

      // Verify it has primary background class
      const classList = await getStartedButton.evaluate(el => el.className);
      expect(classList).toContain('bg-primary');
    });
  });

  /**
   * Test Case 2: Integration Test
   * Input: Verify button uses React Router Link
   * Expected: Navigation occurs without full page reload
   */
  test.describe('Test Case 2: Client-Side Navigation (No Page Reload)', () => {
    test('navigation to /register is client-side without full page reload', async ({ page }) => {
      let fullPageLoadOccurred = false;

      page.on('load', () => {
        fullPageLoadOccurred = true;
      });

      await page.waitForLoadState('load');
      fullPageLoadOccurred = false;

      const getStartedButton = page.getByTestId('cta-get-started');
      await getStartedButton.click();

      await page.waitForURL(/\/register$/);

      expect(fullPageLoadOccurred).toBe(false);
    });

    test('Get Started link has no target attribute (opens in same tab)', async ({ page }) => {
      const getStartedButton = page.getByTestId('cta-get-started');

      const hasTarget = await getStartedButton.evaluate(el => el.hasAttribute('target'));
      expect(hasTarget).toBe(false);
    });

    test('Get Started link is not an external link', async ({ page }) => {
      const getStartedButton = page.getByTestId('cta-get-started');

      const href = await getStartedButton.getAttribute('href');

      expect(href).toBe('/register');
      expect(href).not.toContain('http');
      expect(href).not.toContain('//');
    });

    test('back button returns to homepage after registration navigation', async ({ page }) => {
      const getStartedButton = page.getByTestId('cta-get-started');
      await getStartedButton.click();
      await expect(page).toHaveURL(/\/register$/);

      await page.goBack();

      await expect(page).toHaveURL('/');
    });

    test('navigation updates browser history correctly', async ({ page }) => {
      const initialUrl = page.url();

      const getStartedButton = page.getByTestId('cta-get-started');
      await getStartedButton.click();

      const newUrl = page.url();
      expect(newUrl).not.toBe(initialUrl);
      expect(newUrl).toContain('/register');
    });
  });

  /**
   * Test Case 3: E2E Test
   * Input: Check Get Started button in footer CTA
   * Expected: Footer CTA button also navigates to /register
   */
  test.describe('Test Case 3: Footer CTA Navigation', () => {
    test('Get Started Free button is visible in footer CTA section', async ({ page }) => {
      const footerCtaButton = page.getByTestId('footer-cta-get-started');

      // Scroll to footer first
      await footerCtaButton.scrollIntoViewIfNeeded();

      await expect(footerCtaButton).toBeVisible();
      await expect(footerCtaButton).toHaveText('Get Started Free');
    });

    test('clicking footer CTA navigates to /register route', async ({ page }) => {
      const footerCtaButton = page.getByTestId('footer-cta-get-started');

      await footerCtaButton.scrollIntoViewIfNeeded();
      await footerCtaButton.click();

      await expect(page).toHaveURL(/\/register$/);
    });

    test('footer CTA button has correct href attribute', async ({ page }) => {
      const footerCtaButton = page.getByTestId('footer-cta-get-started');

      await expect(footerCtaButton).toHaveAttribute('href', '/register');
    });

    test('footer CTA navigation is client-side without page reload', async ({ page }) => {
      let fullPageLoadOccurred = false;

      page.on('load', () => {
        fullPageLoadOccurred = true;
      });

      await page.waitForLoadState('load');
      fullPageLoadOccurred = false;

      const footerCtaButton = page.getByTestId('footer-cta-get-started');
      await footerCtaButton.scrollIntoViewIfNeeded();
      await footerCtaButton.click();

      await page.waitForURL(/\/register$/);

      expect(fullPageLoadOccurred).toBe(false);
    });

    test('footer CTA has consistent styling with hero CTA', async ({ page }) => {
      const heroCtaButton = page.getByTestId('cta-get-started');
      const footerCtaButton = page.getByTestId('footer-cta-get-started');

      // Both should have primary background class
      const heroClasses = await heroCtaButton.evaluate(el => el.className);
      const footerClasses = await footerCtaButton.evaluate(el => el.className);

      expect(heroClasses).toContain('bg-primary');
      expect(footerClasses).toContain('bg-primary');
    });
  });

  test.describe('Both Hero and Footer CTAs Navigate to Same Route', () => {
    test('both hero and footer CTAs navigate to the same /register route', async ({ page }) => {
      const heroCta = page.getByTestId('cta-get-started');
      const footerCta = page.getByTestId('footer-cta-get-started');

      // Verify both point to /register
      await expect(heroCta).toHaveAttribute('href', '/register');
      await expect(footerCta).toHaveAttribute('href', '/register');
    });
  });

  test.describe('Navigation to Registration - Mobile Viewport', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });
    });

    test('Get Started button is visible on mobile', async ({ page }) => {
      await page.goto('/');
      const getStartedButton = page.getByTestId('cta-get-started');

      await expect(getStartedButton).toBeVisible();
    });

    test('Get Started navigation works on mobile', async ({ page }) => {
      await page.goto('/');
      const getStartedButton = page.getByTestId('cta-get-started');

      await getStartedButton.click();
      await expect(page).toHaveURL(/\/register$/);
    });

    test('Get Started button is tappable on mobile (sufficient touch target)', async ({ page }) => {
      await page.goto('/');
      const getStartedButton = page.getByTestId('cta-get-started');

      const box = await getStartedButton.boundingBox();
      expect(box).not.toBeNull();

      // WCAG 2.1 recommends minimum 44x44px touch targets
      expect(box!.width).toBeGreaterThanOrEqual(44);
      expect(box!.height).toBeGreaterThanOrEqual(44);
    });

    test('footer CTA is accessible on mobile', async ({ page }) => {
      await page.goto('/');
      const footerCtaButton = page.getByTestId('footer-cta-get-started');

      await footerCtaButton.scrollIntoViewIfNeeded();
      await expect(footerCtaButton).toBeVisible();

      await footerCtaButton.click();
      await expect(page).toHaveURL(/\/register$/);
    });
  });
});
