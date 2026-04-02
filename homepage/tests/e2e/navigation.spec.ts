import { test, expect } from '@playwright/test';

test.describe('Navigation Bar Functionality', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('navigation bar is visible with logo and links', async ({ page }) => {
    // Check navigation bar exists
    const navbar = page.locator('header[role="banner"]');
    await expect(navbar).toBeVisible();

    // Check logo is present
    const logo = page.getByLabel('MirDB Home');
    await expect(logo).toBeVisible();

    // Check all navigation links are visible (desktop view)
    await expect(page.getByTestId('nav-link-home')).toBeVisible();
    await expect(page.getByTestId('nav-link-features')).toBeVisible();
    await expect(page.getByTestId('nav-link-docs')).toBeVisible();
    await expect(page.getByTestId('nav-link-github')).toBeVisible();
  });

  test('Home link scrolls to top of page', async ({ page }) => {
    // First scroll down
    await page.evaluate(() => window.scrollTo(0, 500));
    await page.waitForTimeout(100);

    // Click Home link
    const homeLink = page.getByTestId('nav-link-home');
    await homeLink.click();

    // Wait for scroll animation
    await page.waitForTimeout(500);

    // Check that page scrolled to top
    const scrollY = await page.evaluate(() => window.scrollY);
    expect(scrollY).toBeLessThan(100);
  });

  test('Features link smooth scrolls to Features section', async ({ page }) => {
    // Click Features link
    const featuresLink = page.getByTestId('nav-link-features');
    await featuresLink.click();

    // Wait for scroll animation
    await page.waitForTimeout(500);

    // Check that Features section is in view
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();
  });

  test('GitHub link opens in new tab with correct URL', async ({ page, context }) => {
    // Get the GitHub link
    const githubLink = page.getByTestId('nav-link-github');

    // Verify link attributes
    await expect(githubLink).toHaveAttribute('target', '_blank');
    await expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer');
    await expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
  });

  test('navigation links have correct href attributes', async ({ page }) => {
    // Home link
    const homeLink = page.getByTestId('nav-link-home');
    await expect(homeLink).toHaveAttribute('href', '#');

    // Features link
    const featuresLink = page.getByTestId('nav-link-features');
    await expect(featuresLink).toHaveAttribute('href', '#features');

    // Docs link
    const docsLink = page.getByTestId('nav-link-docs');
    await expect(docsLink).toHaveAttribute('href', '#quickstart');

    // GitHub link
    const githubLink = page.getByTestId('nav-link-github');
    await expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
  });

  test('navigation has proper accessibility attributes', async ({ page }) => {
    // Check header role
    const header = page.locator('header[role="banner"]');
    await expect(header).toBeVisible();

    // Check main navigation aria-label
    const nav = page.getByRole('navigation', { name: 'Main navigation' });
    await expect(nav).toBeVisible();

    // Check menubar role
    const menubar = page.getByRole('menubar');
    await expect(menubar).toBeVisible();
  });

  test.describe('Mobile Navigation', () => {
    test.beforeEach(async ({ page }) => {
      // Set mobile viewport
      await page.setViewportSize({ width: 375, height: 667 });
    });

    test('mobile menu button is visible on mobile', async ({ page }) => {
      const menuButton = page.getByTestId('mobile-menu-button');
      await expect(menuButton).toBeVisible();
    });

    test('mobile menu opens and closes', async ({ page }) => {
      const menuButton = page.getByTestId('mobile-menu-button');

      // Initially the menu should be closed
      await expect(menuButton).toHaveAttribute('aria-expanded', 'false');

      // Click to open
      await menuButton.click();
      await expect(menuButton).toHaveAttribute('aria-expanded', 'true');

      // Mobile nav links should be visible
      await expect(page.getByTestId('mobile-nav-link-home')).toBeVisible();
      await expect(page.getByTestId('mobile-nav-link-features')).toBeVisible();
      await expect(page.getByTestId('mobile-nav-link-github')).toBeVisible();

      // Click to close
      await menuButton.click();
      await expect(menuButton).toHaveAttribute('aria-expanded', 'false');
    });

    test('mobile GitHub link has target="_blank"', async ({ page }) => {
      const menuButton = page.getByTestId('mobile-menu-button');
      await menuButton.click();

      const mobileGithubLink = page.getByTestId('mobile-nav-link-github');
      await expect(mobileGithubLink).toHaveAttribute('target', '_blank');
      await expect(mobileGithubLink).toHaveAttribute('rel', 'noopener noreferrer');
    });
  });
});
