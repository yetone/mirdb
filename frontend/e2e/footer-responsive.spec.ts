import { test, expect } from '@playwright/test';

test.describe('Footer Section Display - Responsive (Test Case 4)', () => {
  test.describe('Mobile Viewport (375px)', () => {
    test('footer displays correctly on mobile viewport', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/');

      // Scroll to footer
      const footer = page.getByTestId('footer');
      await footer.scrollIntoViewIfNeeded();
      await expect(footer).toBeVisible();

      // Verify navigation section is visible
      const navigation = page.getByTestId('footer-navigation');
      await expect(navigation).toBeVisible();

      // Verify all navigation links are visible
      await expect(page.getByTestId('footer-nav-home')).toBeVisible();
      await expect(page.getByTestId('footer-nav-login')).toBeVisible();
      await expect(page.getByTestId('footer-nav-register')).toBeVisible();
      await expect(page.getByTestId('footer-nav-dashboard')).toBeVisible();

      // Verify legal section is visible
      const legalSection = page.getByTestId('footer-legal-links');
      await expect(legalSection).toBeVisible();

      // Verify legal links are visible
      await expect(page.getByTestId('footer-legal-terms-of-service')).toBeVisible();
      await expect(page.getByTestId('footer-legal-privacy-policy')).toBeVisible();

      // Verify copyright is visible (use the one in the Footer component)
      const copyright = page.getByTestId('footer').getByTestId('footer-copyright');
      await expect(copyright).toBeVisible();
      await expect(copyright).toContainText('URL Shortener');
    });

    test('footer content does not overflow on mobile', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/');

      // Scroll to footer
      const footer = page.getByTestId('footer');
      await footer.scrollIntoViewIfNeeded();

      // Check that footer doesn't cause horizontal scroll
      const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
      const viewportWidth = await page.evaluate(() => window.innerWidth);
      expect(bodyScrollWidth).toBeLessThanOrEqual(viewportWidth);

      // Check that navigation is within viewport bounds
      const navigation = page.getByTestId('footer-navigation');
      const navBounds = await navigation.boundingBox();
      expect(navBounds).not.toBeNull();
      expect(navBounds!.x).toBeGreaterThanOrEqual(0);
      expect(navBounds!.x + navBounds!.width).toBeLessThanOrEqual(viewportWidth);
    });

    test('footer links stack vertically on mobile', async ({ page }) => {
      await page.setViewportSize({ width: 320, height: 568 });
      await page.goto('/');

      // Scroll to footer
      const footer = page.getByTestId('footer');
      await footer.scrollIntoViewIfNeeded();

      // Get brand and navigation sections
      const brandTitle = page.getByTestId('footer-brand-title');
      const navTitle = page.getByTestId('footer-nav-title');

      // Verify they both exist
      await expect(brandTitle).toBeVisible();
      await expect(navTitle).toBeVisible();

      // On mobile, sections should stack (brand above nav)
      const brandBounds = await brandTitle.boundingBox();
      const navBounds = await navTitle.boundingBox();

      expect(brandBounds).not.toBeNull();
      expect(navBounds).not.toBeNull();

      // Navigation section should be below brand on mobile (grid single column)
      expect(navBounds!.y).toBeGreaterThan(brandBounds!.y);
    });
  });

  test.describe('Tablet Viewport (768px)', () => {
    test('footer displays correctly on tablet viewport', async ({ page }) => {
      await page.setViewportSize({ width: 768, height: 1024 });
      await page.goto('/');

      // Scroll to footer
      const footer = page.getByTestId('footer');
      await footer.scrollIntoViewIfNeeded();
      await expect(footer).toBeVisible();

      // Verify all footer elements are visible
      await expect(page.getByTestId('footer-brand-title')).toBeVisible();
      await expect(page.getByTestId('footer-navigation')).toBeVisible();
      await expect(page.getByTestId('footer-legal-links')).toBeVisible();
      await expect(page.getByTestId('footer').getByTestId('footer-copyright')).toBeVisible();

      // Verify navigation links
      await expect(page.getByTestId('footer-nav-home')).toBeVisible();
      await expect(page.getByTestId('footer-nav-login')).toBeVisible();
      await expect(page.getByTestId('footer-nav-register')).toBeVisible();
      await expect(page.getByTestId('footer-nav-dashboard')).toBeVisible();

      // Verify legal links
      await expect(page.getByTestId('footer-legal-terms-of-service')).toBeVisible();
      await expect(page.getByTestId('footer-legal-privacy-policy')).toBeVisible();
    });

    test('footer navigation links are functional', async ({ page }) => {
      await page.setViewportSize({ width: 768, height: 1024 });
      await page.goto('/');

      // Scroll to footer
      const footer = page.getByTestId('footer');
      await footer.scrollIntoViewIfNeeded();

      // Verify navigation links have correct href attributes
      const homeLink = page.getByTestId('footer-nav-home');
      await expect(homeLink).toHaveAttribute('href', '/');

      const loginLink = page.getByTestId('footer-nav-login');
      await expect(loginLink).toHaveAttribute('href', '/login');

      const registerLink = page.getByTestId('footer-nav-register');
      await expect(registerLink).toHaveAttribute('href', '/register');

      const dashboardLink = page.getByTestId('footer-nav-dashboard');
      await expect(dashboardLink).toHaveAttribute('href', '/dashboard');

      // Verify legal links have correct href attributes
      const termsLink = page.getByTestId('footer-legal-terms-of-service');
      await expect(termsLink).toHaveAttribute('href', '/terms');

      const privacyLink = page.getByTestId('footer-legal-privacy-policy');
      await expect(privacyLink).toHaveAttribute('href', '/privacy');
    });
  });

  test.describe('Desktop Viewport (1280px)', () => {
    test('footer displays correctly on desktop viewport', async ({ page }) => {
      await page.setViewportSize({ width: 1280, height: 800 });
      await page.goto('/');

      // Scroll to footer
      const footer = page.getByTestId('footer');
      await footer.scrollIntoViewIfNeeded();
      await expect(footer).toBeVisible();

      // Verify all footer elements are visible
      await expect(page.getByTestId('footer-brand-title')).toBeVisible();
      await expect(page.getByTestId('footer-navigation')).toBeVisible();
      await expect(page.getByTestId('footer-legal-links')).toBeVisible();
      await expect(page.getByTestId('footer').getByTestId('footer-copyright')).toBeVisible();

      // Verify navigation links
      await expect(page.getByTestId('footer-nav-home')).toBeVisible();
      await expect(page.getByTestId('footer-nav-login')).toBeVisible();
      await expect(page.getByTestId('footer-nav-register')).toBeVisible();
      await expect(page.getByTestId('footer-nav-dashboard')).toBeVisible();

      // Verify legal links
      await expect(page.getByTestId('footer-legal-terms-of-service')).toBeVisible();
      await expect(page.getByTestId('footer-legal-privacy-policy')).toBeVisible();
    });

    test('footer sections are displayed horizontally on desktop', async ({ page }) => {
      await page.setViewportSize({ width: 1280, height: 800 });
      await page.goto('/');

      // Scroll to footer
      const footer = page.getByTestId('footer');
      await footer.scrollIntoViewIfNeeded();

      // Get brand, navigation, and legal section titles
      const brandTitle = page.getByTestId('footer-brand-title');
      const navTitle = page.getByTestId('footer-nav-title');
      const legalTitle = page.getByTestId('footer-legal-title');

      await expect(brandTitle).toBeVisible();
      await expect(navTitle).toBeVisible();
      await expect(legalTitle).toBeVisible();

      // On desktop, sections should be on the same row (md:grid-cols-3)
      const brandBounds = await brandTitle.boundingBox();
      const navBounds = await navTitle.boundingBox();
      const legalBounds = await legalTitle.boundingBox();

      expect(brandBounds).not.toBeNull();
      expect(navBounds).not.toBeNull();
      expect(legalBounds).not.toBeNull();

      // All sections should be roughly on the same vertical level
      const yTolerance = 50;
      expect(Math.abs(brandBounds!.y - navBounds!.y)).toBeLessThan(yTolerance);
      expect(Math.abs(navBounds!.y - legalBounds!.y)).toBeLessThan(yTolerance);
    });

    test('footer CTA button navigates to register page', async ({ page }) => {
      await page.setViewportSize({ width: 1280, height: 800 });
      await page.goto('/');

      // Scroll to footer CTA
      const footerCta = page.getByTestId('footer-cta');
      await footerCta.scrollIntoViewIfNeeded();

      // Click the CTA button
      const ctaButton = page.getByTestId('footer-cta-get-started');
      await expect(ctaButton).toBeVisible();
      await expect(ctaButton).toHaveAttribute('href', '/register');
    });
  });

  test.describe('Wide Desktop Viewport (1920px)', () => {
    test('footer displays correctly on wide desktop viewport', async ({ page }) => {
      await page.setViewportSize({ width: 1920, height: 1080 });
      await page.goto('/');

      // Scroll to footer
      const footer = page.getByTestId('footer');
      await footer.scrollIntoViewIfNeeded();
      await expect(footer).toBeVisible();

      // Verify all critical elements are visible
      const brandTitle = page.getByTestId('footer-brand-title');
      await expect(brandTitle).toBeVisible();

      const navigation = page.getByTestId('footer-navigation');
      await expect(navigation).toBeVisible();

      const copyright = page.getByTestId('footer').getByTestId('footer-copyright');
      await expect(copyright).toBeVisible();

      // Footer should be full width
      const footerBounds = await footer.boundingBox();
      expect(footerBounds).not.toBeNull();
      expect(footerBounds!.width).toBe(1920);
    });
  });
});
