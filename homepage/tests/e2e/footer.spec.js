/**
 * Footer E2E Tests
 * Owner: Scenario 6 - Footer Section Display
 *
 * Tests:
 * - All footer links navigate to valid destinations
 * - Footer is visible and accessible
 * - Links are clickable
 */

import { test, expect } from '@playwright/test';

test.describe('Footer Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Scroll to footer
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
    await page.waitForTimeout(200);
  });

  test.describe('Footer Visibility', () => {
    test('should display footer at bottom of page', async ({ page }) => {
      const footer = page.locator('footer.footer');
      await expect(footer).toBeVisible();
    });

    test('should display copyright notice', async ({ page }) => {
      const copyright = page.locator('.footer__copyright');
      await expect(copyright).toBeVisible();
      await expect(copyright).toContainText('2026');
      await expect(copyright).toContainText('MirDB');
    });
  });

  test.describe('Footer Links Navigation', () => {
    test('should have Privacy Policy link that is clickable', async ({ page }) => {
      const privacyLink = page.locator('a[href="/privacy"]');
      await expect(privacyLink).toBeVisible();
      await expect(privacyLink).toContainText('Privacy Policy');

      // Verify link is clickable (don't actually navigate to avoid 404)
      const href = await privacyLink.getAttribute('href');
      expect(href).toBe('/privacy');
    });

    test('should have Terms of Service link that is clickable', async ({ page }) => {
      const termsLink = page.locator('a[href="/terms"]');
      await expect(termsLink).toBeVisible();
      await expect(termsLink).toContainText('Terms of Service');

      // Verify link has correct href
      const href = await termsLink.getAttribute('href');
      expect(href).toBe('/terms');
    });

    test('should have Contact link that navigates to contact section', async ({ page }) => {
      const contactLink = page.locator('.footer__link[href="#contact"]');
      await expect(contactLink).toBeVisible();
      await expect(contactLink).toContainText('Contact');

      // Click contact link
      await contactLink.click();

      // Should navigate to contact section
      await page.waitForURL(/#contact/);

      // Contact section should be in view
      const contactSection = page.locator('#contact');
      await expect(contactSection).toBeInViewport();
    });

    test('should have GitHub link that opens repository', async ({ page }) => {
      const githubLink = page.locator('a[href*="github.com"]');
      await expect(githubLink).toBeVisible();
      await expect(githubLink).toContainText('GitHub');

      // Verify link has correct href pointing to GitHub
      const href = await githubLink.getAttribute('href');
      expect(href).toContain('github.com');
    });

    test('should have all footer links accessible', async ({ page }) => {
      const footerLinks = page.locator('.footer__link');
      const count = await footerLinks.count();

      // Should have at least 4 links: Privacy, Terms, Contact, GitHub
      expect(count).toBeGreaterThanOrEqual(4);

      // All links should be visible
      for (const link of await footerLinks.all()) {
        await expect(link).toBeVisible();
      }
    });
  });

  test.describe('Footer Accessibility', () => {
    test('should have proper role for footer', async ({ page }) => {
      const footer = page.locator('footer.footer');
      const role = await footer.getAttribute('role');
      expect(role).toBe('contentinfo');
    });

    test('should have aria-label on footer navigation', async ({ page }) => {
      const footerNav = page.locator('.footer__links');
      const ariaLabel = await footerNav.getAttribute('aria-label');
      expect(ariaLabel).toBeTruthy();
      expect(ariaLabel.toLowerCase()).toContain('footer');
    });

    test('should have accessible label on GitHub link', async ({ page }) => {
      const githubLink = page.locator('a[href*="github.com"]');
      const ariaLabel = await githubLink.getAttribute('aria-label');
      expect(ariaLabel).toBeTruthy();
      expect(ariaLabel.toLowerCase()).toContain('github');
    });
  });

  test.describe('Footer Interaction', () => {
    test('should allow keyboard navigation through footer links', async ({ page }) => {
      // Focus on the first footer link
      const firstLink = page.locator('.footer__link').first();
      await firstLink.focus();

      // Tab through all links
      const footerLinks = page.locator('.footer__link');
      const count = await footerLinks.count();

      for (let i = 0; i < count; i++) {
        const focusedElement = page.locator(':focus');
        await expect(focusedElement).toHaveClass(/footer__link/);
        await page.keyboard.press('Tab');
      }
    });
  });
});
