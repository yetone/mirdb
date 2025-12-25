import { test, expect } from '@playwright/test';

test.describe('Error Handling - 404 Page', () => {
  test.describe('Test Case 1: Navigate to non-existent page displays custom 404', () => {
    test('TC1: Custom 404 page is displayed with clear message when navigating to invalid URL', async ({ page }) => {
      // Navigate to a non-existent page
      const response = await page.goto('/nonexistent-page');

      // Verify we get a 404 status code
      expect(response?.status()).toBe(404);

      // Verify the 404 section is present
      const errorSection = page.locator('[data-testid="error-404-section"]');
      await expect(errorSection).toBeVisible();

      // Verify the error code "404" is displayed prominently
      const errorCode = page.locator('[data-testid="error-code"]');
      await expect(errorCode).toBeVisible();
      await expect(errorCode).toHaveText('404');

      // Verify the error title is displayed
      const errorTitle = page.locator('[data-testid="error-title"]');
      await expect(errorTitle).toBeVisible();
      await expect(errorTitle).toContainText('Page Not Found');

      // Verify an error message is displayed explaining what happened
      const errorMessage = page.locator('[data-testid="error-message"]');
      await expect(errorMessage).toBeVisible();
      const messageText = await errorMessage.textContent();
      expect(messageText).toContain("doesn't exist");
    });

    test('TC1b: 404 page includes link to homepage', async ({ page }) => {
      // Navigate to a non-existent page
      await page.goto('/nonexistent-page');

      // Verify the back to homepage link is present
      const homepageLink = page.locator('[data-testid="back-to-homepage"]');
      await expect(homepageLink).toBeVisible();

      // Verify the link text is clear
      await expect(homepageLink).toContainText('Homepage');

      // Verify the link points to the homepage
      await expect(homepageLink).toHaveAttribute('href', '/');
    });

    test('TC1c: 404 page has navigation bar with brand logo', async ({ page }) => {
      // Navigate to a non-existent page
      await page.goto('/some-random-invalid-url');

      // Verify the navigation bar is present
      const navbar = page.locator('nav.navbar');
      await expect(navbar).toBeVisible();

      // Verify the brand logo links to homepage
      const brandLink = page.locator('[data-testid="nav-brand"]');
      await expect(brandLink).toBeVisible();
      await expect(brandLink).toHaveText('MirDB');
      await expect(brandLink).toHaveAttribute('href', '/');
    });
  });

  test.describe('Test Case 2: 404 page navigation functionality', () => {
    test('TC2: Clicking back to homepage link navigates to homepage', async ({ page }) => {
      // Start on a non-existent page
      await page.goto('/this-page-does-not-exist');

      // Click the back to homepage button
      const homepageLink = page.locator('[data-testid="back-to-homepage"]');
      await homepageLink.click();

      // Wait for navigation to complete
      await page.waitForURL('/');

      // Verify we're on the homepage
      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();

      // Verify the product name is displayed
      const productName = page.locator('[data-testid="product-name"]');
      await expect(productName).toHaveText('MirDB');
    });

    test('TC2b: Clicking brand logo on 404 page navigates to homepage', async ({ page }) => {
      // Start on a non-existent page
      await page.goto('/another-invalid-page');

      // Click the brand logo in the navigation
      const brandLink = page.locator('[data-testid="nav-brand"]');
      await brandLink.click();

      // Wait for navigation to complete
      await page.waitForURL('/');

      // Verify we're on the homepage
      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();
    });

    test('TC2c: Navigation links on 404 page work correctly', async ({ page }) => {
      // Start on a non-existent page
      await page.goto('/invalid-url-test');

      // Verify the navigation links point to homepage sections
      const navLinks = page.locator('.nav-links');

      // Check Features link
      const featuresLink = navLinks.locator('a[href="/#features"]');
      await expect(featuresLink).toBeVisible();

      // Check Getting Started link
      const gettingStartedLink = navLinks.locator('a[href="/#getting-started"]');
      await expect(gettingStartedLink).toBeVisible();

      // Check Configuration link
      const configurationLink = navLinks.locator('a[href="/#configuration"]');
      await expect(configurationLink).toBeVisible();
    });

    test('TC2d: Clicking navigation link from 404 page goes to correct section', async ({ page }) => {
      // Start on a non-existent page
      await page.goto('/404-test-page');

      // Click the Features link
      const featuresLink = page.locator('.nav-links a[href="/#features"]');
      await featuresLink.click();

      // Wait for navigation to complete
      await page.waitForURL('/#features');

      // Verify Features section is visible
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeVisible();
    });
  });

  test.describe('Additional 404 page tests', () => {
    test('404 page footer is present', async ({ page }) => {
      await page.goto('/random-invalid-url');

      // Verify footer is present
      const footer = page.locator('[data-testid="footer-section"]');
      await expect(footer).toBeVisible();

      // Verify GitHub link is in footer
      const footerText = await footer.textContent();
      expect(footerText).toContain('GitHub');
    });

    test('404 page uses proper layout and styling', async ({ page }) => {
      await page.goto('/test-404-styling');

      // Verify the 404 error code has appropriate styling (large text)
      const errorCode = page.locator('[data-testid="error-code"]');
      const fontSize = await errorCode.evaluate(el => getComputedStyle(el).fontSize);
      // Should be at least 96px (text-8xl) on desktop
      expect(parseInt(fontSize)).toBeGreaterThanOrEqual(96);

      // Verify the main section has centered content
      const errorSection = page.locator('[data-testid="error-404-section"]');
      const display = await errorSection.evaluate(el => getComputedStyle(el).display);
      expect(display).toBe('flex');
    });

    test('404 page is accessible with different invalid URLs', async ({ page }) => {
      // Test various invalid URL patterns
      const invalidUrls = [
        '/nonexistent',
        '/some/nested/path',
        '/test.html',
        '/api/not-found',
        '/page-with-numbers-123'
      ];

      for (const url of invalidUrls) {
        const response = await page.goto(url);
        expect(response?.status()).toBe(404);

        const errorCode = page.locator('[data-testid="error-code"]');
        await expect(errorCode).toBeVisible();
        await expect(errorCode).toHaveText('404');
      }
    });
  });
});
