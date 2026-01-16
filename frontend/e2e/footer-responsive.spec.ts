import { test, expect } from '@playwright/test';

test.describe('Footer Section Display - Viewport Responsiveness', () => {
  test.describe('Test Case: Footer displays correctly on mobile viewport', () => {
    test('footer renders with all required elements on mobile (375px)', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/');

      // Scroll to footer
      const footer = page.getByTestId('footer');
      await footer.scrollIntoViewIfNeeded();
      await expect(footer).toBeVisible();

      // Verify footer navigation section exists
      const footerNav = page.getByTestId('footer-navigation');
      await expect(footerNav).toBeVisible();

      // Verify all navigation links are present and visible
      await expect(page.getByTestId('footer-nav-home')).toBeVisible();
      await expect(page.getByTestId('footer-nav-login')).toBeVisible();
      await expect(page.getByTestId('footer-nav-register')).toBeVisible();
      await expect(page.getByTestId('footer-nav-dashboard')).toBeVisible();

      // Verify legal links section exists
      const legalLinks = page.getByTestId('footer-legal-links');
      await expect(legalLinks).toBeVisible();

      // Verify legal links are present
      await expect(page.getByTestId('footer-legal-terms-of-service')).toBeVisible();
      await expect(page.getByTestId('footer-legal-privacy-policy')).toBeVisible();

      // Verify copyright notice is visible (use the one in our Footer component)
      const copyright = page.getByTestId('footer').getByTestId('footer-copyright');
      await expect(copyright).toBeVisible();
      await expect(copyright).toContainText('URL Shortener');
      await expect(copyright).toContainText('All rights reserved');
    });

    test('footer has no horizontal overflow on mobile', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/');

      // Scroll to footer
      const footer = page.getByTestId('footer');
      await footer.scrollIntoViewIfNeeded();

      // Verify no horizontal scroll
      const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
      const viewportWidth = await page.evaluate(() => window.innerWidth);
      expect(bodyScrollWidth).toBeLessThanOrEqual(viewportWidth);

      // Verify footer content is within viewport bounds
      const footerBounds = await footer.boundingBox();
      expect(footerBounds).not.toBeNull();
      expect(footerBounds!.x).toBeGreaterThanOrEqual(0);
      expect(footerBounds!.x + footerBounds!.width).toBeLessThanOrEqual(viewportWidth + 1); // +1 for rounding
    });

    test('footer elements stack vertically on mobile', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/');

      // Scroll to footer
      const footer = page.getByTestId('footer');
      await footer.scrollIntoViewIfNeeded();

      // The grid should stack elements vertically on mobile
      // Brand section, nav section, and legal section should be vertically stacked
      const brandTitle = page.getByTestId('footer-brand-title');
      const navTitle = page.getByTestId('footer-nav-title');
      const legalTitle = page.getByTestId('footer-legal-title');

      await expect(brandTitle).toBeVisible();
      await expect(navTitle).toBeVisible();
      await expect(legalTitle).toBeVisible();

      // Get bounding boxes to verify vertical stacking
      const brandBounds = await brandTitle.boundingBox();
      const navBounds = await navTitle.boundingBox();
      const legalBounds = await legalTitle.boundingBox();

      expect(brandBounds).not.toBeNull();
      expect(navBounds).not.toBeNull();
      expect(legalBounds).not.toBeNull();

      // On mobile (single column), sections should be stacked vertically
      // Brand should be above Nav, Nav should be above Legal
      expect(brandBounds!.y).toBeLessThan(navBounds!.y);
      expect(navBounds!.y).toBeLessThan(legalBounds!.y);
    });
  });

  test.describe('Test Case: Footer displays correctly on tablet viewport', () => {
    test('footer renders with all required elements on tablet (768px)', async ({ page }) => {
      await page.setViewportSize({ width: 768, height: 1024 });
      await page.goto('/');

      // Scroll to footer
      const footer = page.getByTestId('footer');
      await footer.scrollIntoViewIfNeeded();
      await expect(footer).toBeVisible();

      // Verify footer navigation section exists
      await expect(page.getByTestId('footer-navigation')).toBeVisible();

      // Verify all navigation links are present
      await expect(page.getByTestId('footer-nav-home')).toBeVisible();
      await expect(page.getByTestId('footer-nav-login')).toBeVisible();
      await expect(page.getByTestId('footer-nav-register')).toBeVisible();
      await expect(page.getByTestId('footer-nav-dashboard')).toBeVisible();

      // Verify legal links are present
      await expect(page.getByTestId('footer-legal-terms-of-service')).toBeVisible();
      await expect(page.getByTestId('footer-legal-privacy-policy')).toBeVisible();

      // Verify copyright notice is visible (use the one in our Footer component)
      const copyright = page.getByTestId('footer').getByTestId('footer-copyright');
      await expect(copyright).toBeVisible();
    });

    test('footer uses multi-column layout on tablet', async ({ page }) => {
      await page.setViewportSize({ width: 768, height: 1024 });
      await page.goto('/');

      // Scroll to footer
      const footer = page.getByTestId('footer');
      await footer.scrollIntoViewIfNeeded();

      // Get section titles to check horizontal arrangement
      const brandTitle = page.getByTestId('footer-brand-title');
      const navTitle = page.getByTestId('footer-nav-title');
      const legalTitle = page.getByTestId('footer-legal-title');

      const brandBounds = await brandTitle.boundingBox();
      const navBounds = await navTitle.boundingBox();
      const legalBounds = await legalTitle.boundingBox();

      expect(brandBounds).not.toBeNull();
      expect(navBounds).not.toBeNull();
      expect(legalBounds).not.toBeNull();

      // On tablet with md breakpoint (768px+), should use multi-column layout
      // Brand, Nav, and Legal should be arranged horizontally (same row or close)
      // They should have different x positions
      expect(brandBounds!.x).not.toBe(navBounds!.x);
      expect(navBounds!.x).not.toBe(legalBounds!.x);
    });
  });

  test.describe('Test Case: Footer displays correctly on desktop viewport', () => {
    test('footer renders with all required elements on desktop (1280px)', async ({ page }) => {
      await page.setViewportSize({ width: 1280, height: 800 });
      await page.goto('/');

      // Scroll to footer
      const footer = page.getByTestId('footer');
      await footer.scrollIntoViewIfNeeded();
      await expect(footer).toBeVisible();

      // Verify footer navigation section exists
      await expect(page.getByTestId('footer-navigation')).toBeVisible();

      // Verify all navigation links are present and have correct hrefs
      const homeLink = page.getByTestId('footer-nav-home');
      const loginLink = page.getByTestId('footer-nav-login');
      const registerLink = page.getByTestId('footer-nav-register');
      const dashboardLink = page.getByTestId('footer-nav-dashboard');

      await expect(homeLink).toBeVisible();
      await expect(homeLink).toHaveAttribute('href', '/');

      await expect(loginLink).toBeVisible();
      await expect(loginLink).toHaveAttribute('href', '/login');

      await expect(registerLink).toBeVisible();
      await expect(registerLink).toHaveAttribute('href', '/register');

      await expect(dashboardLink).toBeVisible();
      await expect(dashboardLink).toHaveAttribute('href', '/dashboard');

      // Verify legal links with correct hrefs
      const termsLink = page.getByTestId('footer-legal-terms-of-service');
      const privacyLink = page.getByTestId('footer-legal-privacy-policy');

      await expect(termsLink).toBeVisible();
      await expect(termsLink).toHaveAttribute('href', '/terms');

      await expect(privacyLink).toBeVisible();
      await expect(privacyLink).toHaveAttribute('href', '/privacy');

      // Verify copyright notice (use the one in our Footer component)
      const copyright = page.getByTestId('footer').getByTestId('footer-copyright');
      await expect(copyright).toBeVisible();
      const currentYear = new Date().getFullYear().toString();
      await expect(copyright).toContainText(currentYear);
    });

    test('footer sections are laid out horizontally on desktop', async ({ page }) => {
      await page.setViewportSize({ width: 1280, height: 800 });
      await page.goto('/');

      // Scroll to footer
      const footer = page.getByTestId('footer');
      await footer.scrollIntoViewIfNeeded();

      // Get section titles
      const brandTitle = page.getByTestId('footer-brand-title');
      const navTitle = page.getByTestId('footer-nav-title');
      const legalTitle = page.getByTestId('footer-legal-title');

      const brandBounds = await brandTitle.boundingBox();
      const navBounds = await navTitle.boundingBox();
      const legalBounds = await legalTitle.boundingBox();

      expect(brandBounds).not.toBeNull();
      expect(navBounds).not.toBeNull();
      expect(legalBounds).not.toBeNull();

      // On desktop, sections should be in a horizontal row
      // Check that they're roughly on the same vertical level (accounting for slight differences)
      const yTolerance = 50;
      expect(Math.abs(brandBounds!.y - navBounds!.y)).toBeLessThan(yTolerance);
      expect(Math.abs(navBounds!.y - legalBounds!.y)).toBeLessThan(yTolerance);

      // They should have increasing x positions (left to right)
      expect(brandBounds!.x).toBeLessThan(navBounds!.x);
      expect(navBounds!.x).toBeLessThan(legalBounds!.x);
    });
  });

  test.describe('Test Case: Footer link functionality', () => {
    test('navigation links in footer are clickable and navigate correctly', async ({ page }) => {
      await page.setViewportSize({ width: 1280, height: 800 });
      await page.goto('/');

      // Scroll to footer
      const footer = page.getByTestId('footer');
      await footer.scrollIntoViewIfNeeded();

      // Test Login link navigation
      const loginLink = page.getByTestId('footer-nav-login');
      await loginLink.click();
      await expect(page).toHaveURL(/\/login/);

      // Go back to homepage
      await page.goto('/');
      await footer.scrollIntoViewIfNeeded();

      // Test Home link navigation
      const homeLink = page.getByTestId('footer-nav-home');
      await homeLink.click();
      await expect(page).toHaveURL(/\/$/);
    });
  });

  test.describe('Test Case: Footer accessibility', () => {
    test('footer links have proper semantic structure', async ({ page }) => {
      await page.setViewportSize({ width: 1280, height: 800 });
      await page.goto('/');

      // Scroll to footer
      const footer = page.getByTestId('footer');
      await footer.scrollIntoViewIfNeeded();

      // Verify footer uses semantic footer element
      const footerElement = await footer.evaluate(el => el.tagName.toLowerCase());
      expect(footerElement).toBe('footer');

      // Verify navigation sections use nav elements
      const footerNav = page.getByTestId('footer-navigation');
      const navElement = await footerNav.evaluate(el => el.tagName.toLowerCase());
      expect(navElement).toBe('nav');

      const legalNav = page.getByTestId('footer-legal-links');
      const legalNavElement = await legalNav.evaluate(el => el.tagName.toLowerCase());
      expect(legalNavElement).toBe('nav');
    });

    test('footer links are keyboard accessible', async ({ page }) => {
      await page.setViewportSize({ width: 1280, height: 800 });
      await page.goto('/');

      // Scroll to footer
      const footer = page.getByTestId('footer');
      await footer.scrollIntoViewIfNeeded();

      // Focus on the first navigation link
      const homeLink = page.getByTestId('footer-nav-home');
      await homeLink.focus();

      // Verify link is focused
      await expect(homeLink).toBeFocused();

      // Tab to the next link
      await page.keyboard.press('Tab');

      // Verify next link is focused
      const loginLink = page.getByTestId('footer-nav-login');
      await expect(loginLink).toBeFocused();
    });
  });
});
