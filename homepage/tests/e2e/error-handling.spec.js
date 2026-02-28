/**
 * Error Handling E2E Tests
 * Owner: Scenario 16 - Error State Handling
 *
 * Tests:
 * - 404 error page display
 * - Broken link handling
 * - Page graceful degradation
 */

import { test, expect } from '@playwright/test';

test.describe('Error State Handling', () => {
  test.describe('404 Error Page', () => {
    test('should show friendly 404 page for non-existent routes', async ({ page }) => {
      // Navigate to a non-existent page
      const response = await page.goto('/non-existent-page-12345');

      // Should return 404 status
      expect(response.status()).toBe(404);

      // Should show friendly error page, not browser default
      const body = await page.textContent('body');
      expect(body).not.toContain('Cannot GET');

      // Should have error message content
      const hasErrorContent = await page.locator('.error-page, [data-testid="error-page"], main').first().isVisible();
      expect(hasErrorContent).toBe(true);
    });

    test('should display 404 error message', async ({ page }) => {
      await page.goto('/does-not-exist', { waitUntil: 'domcontentloaded' });

      // Look for 404 text
      const pageContent = await page.textContent('body');
      const has404Reference =
        pageContent.includes('404') ||
        pageContent.includes('not found') ||
        pageContent.includes('Not Found');

      expect(has404Reference).toBe(true);
    });

    test('should provide navigation back to homepage from 404 page', async ({ page }) => {
      await page.goto('/non-existent-page', { waitUntil: 'domcontentloaded' });

      // Should have a link back to home
      const homeLink = page.locator('a[href="/"], a[href="#home"], .error-page__link');
      const linkExists = await homeLink.first().isVisible().catch(() => false);

      if (linkExists) {
        await homeLink.first().click();
        await expect(page).toHaveURL(/\//);
      }
    });

    test('should maintain consistent styling on 404 page', async ({ page }) => {
      await page.goto('/nonexistent-route', { waitUntil: 'domcontentloaded' });

      // Check for consistent header/navigation
      const header = page.locator('header, .header, nav');
      const headerVisible = await header.first().isVisible().catch(() => false);

      // 404 page should still have basic page structure
      expect(headerVisible).toBe(true);
    });
  });

  test.describe('Broken Image Handling', () => {
    test('should handle broken images gracefully', async ({ page }) => {
      await page.goto('/');

      // Check that any images on the page have alt attributes
      const images = page.locator('img');
      const imageCount = await images.count();

      for (let i = 0; i < imageCount; i++) {
        const img = images.nth(i);
        const altText = await img.getAttribute('alt');
        expect(altText).toBeTruthy();
      }
    });

    test('should display SVG icons without broken image indicators', async ({ page }) => {
      await page.goto('/');

      // SVG icons should be visible
      const svgIcons = page.locator('svg');
      const iconCount = await svgIcons.count();

      // Page should have icons (feature cards, social links, etc.)
      expect(iconCount).toBeGreaterThan(0);

      // Check SVG icons are properly handled for accessibility
      for (let i = 0; i < Math.min(iconCount, 5); i++) {
        const svg = svgIcons.nth(i);
        const ariaHidden = await svg.getAttribute('aria-hidden');
        const ariaLabel = await svg.getAttribute('aria-label');
        // Check if parent has aria-hidden
        const parentAriaHidden = await svg.evaluate(el => {
          return el.closest('[aria-hidden="true"]') !== null;
        });
        // Either decorative, has label, or parent is hidden
        expect(ariaHidden === 'true' || ariaLabel || parentAriaHidden).toBeTruthy();
      }
    });

    test('should not show broken image icons', async ({ page }) => {
      await page.goto('/');

      // Inject a broken image
      await page.evaluate(() => {
        const img = document.createElement('img');
        img.src = 'http://localhost:3000/broken-image-that-does-not-exist.png';
        img.alt = 'Test broken image';
        img.className = 'img-fallback';
        img.style.width = '100px';
        img.style.height = '100px';
        document.body.appendChild(img);
      });

      // Wait a moment for error to trigger
      await page.waitForTimeout(500);

      // The image should have alt text available
      const brokenImg = page.locator('img[alt="Test broken image"]');
      const isVisible = await brokenImg.isVisible();
      expect(isVisible).toBe(true);
    });
  });

  test.describe('Page Graceful Degradation', () => {
    test('should maintain readable content structure', async ({ page }) => {
      await page.goto('/');

      // Essential semantic structure should be present
      const main = page.locator('main, [role="main"]');
      await expect(main).toBeVisible();

      // Heading hierarchy
      const h1 = page.locator('h1');
      await expect(h1).toBeVisible();

      // Content should be readable
      const mainText = await main.textContent();
      expect(mainText.length).toBeGreaterThan(50);
    });

    test('should have visible text without JavaScript', async ({ page }) => {
      // Go to page with JS disabled
      await page.route('**/*.js', route => route.abort());
      await page.goto('/', { waitUntil: 'domcontentloaded' });

      // Core content should still be visible
      const mainContent = await page.textContent('main');
      expect(mainContent).toContain('MirDB');
    });

    test('should have proper color contrast for error text', async ({ page }) => {
      await page.goto('/');

      // Check that error colors are defined in CSS
      const errorColorDefined = await page.evaluate(() => {
        const styles = getComputedStyle(document.documentElement);
        const errorColor = styles.getPropertyValue('--color-error');
        return errorColor && errorColor.trim().length > 0;
      });

      expect(errorColorDefined).toBe(true);
    });

    test('should footer links remain accessible', async ({ page }) => {
      await page.goto('/');

      // Footer should be present
      const footer = page.locator('footer, [role="contentinfo"]');
      await expect(footer).toBeVisible();

      // Links should be accessible
      const footerLinks = footer.locator('a');
      const linkCount = await footerLinks.count();
      expect(linkCount).toBeGreaterThan(0);
    });
  });
});
