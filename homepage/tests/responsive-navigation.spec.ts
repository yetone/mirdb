import { test, expect } from '@playwright/test';

test.describe('Responsive Navigation', () => {
  test.describe('Desktop Navigation', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize({ width: 1280, height: 800 });
      await page.goto('/');
    });

    test('TC1: Navigation bar shows Features, Quick Start, Documentation, GitHub links at desktop viewport', async ({ page }) => {
      const nav = page.locator('[data-testid="desktop-nav"]');
      await expect(nav).toBeVisible();

      // Verify all navigation links are visible
      const featuresLink = page.locator('[data-testid="nav-features"]');
      await expect(featuresLink).toBeVisible();
      await expect(featuresLink).toContainText('Features');

      const quickStartLink = page.locator('[data-testid="nav-quick-start"]');
      await expect(quickStartLink).toBeVisible();
      await expect(quickStartLink).toContainText('Quick Start');

      const documentationLink = page.locator('[data-testid="nav-documentation"]');
      await expect(documentationLink).toBeVisible();
      await expect(documentationLink).toContainText('Documentation');

      const githubLink = page.locator('[data-testid="nav-github"]');
      await expect(githubLink).toBeVisible();
      await expect(githubLink).toContainText('GitHub');
    });

    test('TC2: Click Features nav link scrolls to features section', async ({ page }) => {
      const featuresLink = page.locator('[data-testid="nav-features"]');
      await featuresLink.click();

      // Wait for scroll animation to complete
      await page.waitForTimeout(500);

      // Verify features section is in view
      const featuresSection = page.locator('[data-testid="features-section"]');
      await expect(featuresSection).toBeInViewport();
    });

    test('TC3: Click Quick Start nav link scrolls to quick-start section', async ({ page }) => {
      const quickStartLink = page.locator('[data-testid="nav-quick-start"]');
      await quickStartLink.click();

      // Wait for scroll animation to complete
      await page.waitForTimeout(500);

      // Verify quick-start section is in view
      const quickStartSection = page.locator('[data-testid="quick-start-section"]');
      await expect(quickStartSection).toBeInViewport();
    });

    test('TC4: Click Documentation nav link scrolls to documentation section', async ({ page }) => {
      const documentationLink = page.locator('[data-testid="nav-documentation"]');
      await documentationLink.click();

      // Wait for scroll animation to complete
      await page.waitForTimeout(500);

      // Verify documentation section is in view
      const documentationSection = page.locator('[data-testid="documentation-section"]');
      await expect(documentationSection).toBeInViewport();
    });

    test('TC5: Click GitHub nav link opens GitHub repository in new tab', async ({ page }) => {
      const githubLink = page.locator('[data-testid="nav-github"]');

      // Verify GitHub link has correct href and opens in new tab
      await expect(githubLink).toHaveAttribute('href', 'https://github.com/pjtatlow/mirdb');
      await expect(githubLink).toHaveAttribute('target', '_blank');
      await expect(githubLink).toHaveAttribute('rel', /noopener/);
    });
  });

  test.describe('Mobile Navigation', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/');
    });

    test('TC6: Mobile navigation hamburger icon is present at mobile viewport', async ({ page }) => {
      // Desktop nav should be hidden
      const desktopNav = page.locator('[data-testid="desktop-nav"]');
      await expect(desktopNav).toBeHidden();

      // Mobile hamburger button should be visible
      const mobileMenuButton = page.locator('[data-testid="mobile-menu-button"]');
      await expect(mobileMenuButton).toBeVisible();
    });

    test('TC7: Open mobile menu and click nav item - menu closes and navigates to section', async ({ page }) => {
      // Open mobile menu
      const mobileMenuButton = page.locator('[data-testid="mobile-menu-button"]');
      await mobileMenuButton.click();

      // Wait for menu to open
      await page.waitForTimeout(300);

      // Verify mobile menu is visible
      const mobileMenu = page.locator('[data-testid="mobile-menu"]');
      await expect(mobileMenu).toBeVisible();

      // Verify all nav items are visible in mobile menu
      const featuresLink = page.locator('[data-testid="mobile-nav-features"]');
      await expect(featuresLink).toBeVisible();

      // Click features link
      await featuresLink.click();

      // Wait for animation/navigation
      await page.waitForTimeout(500);

      // Verify menu closes
      await expect(mobileMenu).toBeHidden();

      // Verify features section is in view
      const featuresSection = page.locator('[data-testid="features-section"]');
      await expect(featuresSection).toBeInViewport();
    });

    test('TC7b: Mobile menu contains all navigation items', async ({ page }) => {
      // Open mobile menu
      const mobileMenuButton = page.locator('[data-testid="mobile-menu-button"]');
      await mobileMenuButton.click();

      // Wait for menu to open
      await page.waitForTimeout(300);

      // Verify all mobile nav items
      await expect(page.locator('[data-testid="mobile-nav-features"]')).toBeVisible();
      await expect(page.locator('[data-testid="mobile-nav-quick-start"]')).toBeVisible();
      await expect(page.locator('[data-testid="mobile-nav-documentation"]')).toBeVisible();
      await expect(page.locator('[data-testid="mobile-nav-github"]')).toBeVisible();
    });
  });
});
