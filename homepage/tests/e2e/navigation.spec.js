/**
 * Navigation E2E Tests
 * Owner: Scenario 2 - Navigation Menu Desktop
 *
 * Tests:
 * - Navigation links work
 * - Sticky header behavior
 * - Logo links to homepage
 * - Desktop viewport displays horizontal nav
 */

import { test, expect } from '@playwright/test';

test.describe('Navigation Menu Desktop', () => {
  test.beforeEach(async ({ page }) => {
    // Set desktop viewport (>= 1024px)
    await page.setViewportSize({ width: 1200, height: 800 });
    await page.goto('/');
  });

  test.describe('Navigation Display at Desktop Width', () => {
    test('should display navigation horizontally at 1200px viewport', async ({ page }) => {
      const navMenu = page.locator('.nav__menu');
      await expect(navMenu).toBeVisible();

      // Menu should be displayed as flex (horizontal)
      const display = await navMenu.evaluate(el => window.getComputedStyle(el).display);
      expect(display).toBe('flex');
    });

    test('should not show hamburger menu on desktop', async ({ page }) => {
      const toggle = page.locator('.nav__toggle');
      await expect(toggle).toBeHidden();
    });

    test('should display all four navigation links', async ({ page }) => {
      const navLinks = page.locator('.nav__link');
      await expect(navLinks).toHaveCount(4);

      // Check each link is visible
      for (const link of await navLinks.all()) {
        await expect(link).toBeVisible();
      }
    });
  });

  test.describe('Logo/Product Name', () => {
    test('should display logo with product name', async ({ page }) => {
      const logo = page.locator('.nav__logo');
      await expect(logo).toBeVisible();
      await expect(logo).toContainText('MirDB');
    });

    test('should link logo back to homepage', async ({ page }) => {
      const logo = page.locator('.nav__logo');
      const href = await logo.getAttribute('href');
      expect(href).toBe('/');
    });

    test('should navigate to homepage when logo is clicked', async ({ page }) => {
      // First navigate to a section
      await page.goto('/#features');

      // Click logo
      await page.locator('.nav__logo').click();

      // Should be at homepage
      await expect(page).toHaveURL('/');
    });
  });

  test.describe('Navigation Links', () => {
    test('should have Home link pointing to #home', async ({ page }) => {
      const homeLink = page.locator('.nav__link[href="#home"]');
      await expect(homeLink).toBeVisible();
      await expect(homeLink).toContainText('Home');
    });

    test('should have Features link pointing to #features', async ({ page }) => {
      const featuresLink = page.locator('.nav__link[href="#features"]');
      await expect(featuresLink).toBeVisible();
      await expect(featuresLink).toContainText('Features');
    });

    test('should have About link pointing to #about', async ({ page }) => {
      const aboutLink = page.locator('.nav__link[href="#about"]');
      await expect(aboutLink).toBeVisible();
      await expect(aboutLink).toContainText('About');
    });

    test('should have Contact link pointing to #contact', async ({ page }) => {
      const contactLink = page.locator('.nav__link[href="#contact"]');
      await expect(contactLink).toBeVisible();
      await expect(contactLink).toContainText('Contact');
    });

    test('should navigate to correct section when Features link is clicked', async ({ page }) => {
      await page.locator('.nav__link[href="#features"]').click();

      // Wait for navigation
      await page.waitForURL(/#features/);

      // Features section should be in view
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeInViewport();
    });
  });

  test.describe('Sticky/Fixed Header', () => {
    test('should have fixed position header', async ({ page }) => {
      const header = page.locator('.header');
      const position = await header.evaluate(el => window.getComputedStyle(el).position);
      expect(position).toBe('fixed');
    });

    test('should remain visible after scrolling 500px', async ({ page }) => {
      // Scroll down
      await page.evaluate(() => window.scrollTo(0, 500));

      // Wait for scroll
      await page.waitForTimeout(100);

      // Header should still be visible
      const header = page.locator('.header');
      await expect(header).toBeVisible();
      await expect(header).toBeInViewport();
    });

    test('should remain accessible after scrolling to bottom', async ({ page }) => {
      // Scroll to bottom of page
      await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));

      // Wait for scroll
      await page.waitForTimeout(100);

      // Header should still be visible and in viewport
      const header = page.locator('.header');
      await expect(header).toBeVisible();

      // Verify header is at top of viewport
      const headerBox = await header.boundingBox();
      expect(headerBox.y).toBe(0);
    });

    test('should have navigation links accessible after scrolling', async ({ page }) => {
      // Scroll down significantly
      await page.evaluate(() => window.scrollTo(0, 1000));
      await page.waitForTimeout(100);

      // All navigation links should still be clickable
      const navLinks = page.locator('.nav__link');
      for (const link of await navLinks.all()) {
        await expect(link).toBeVisible();
      }
    });
  });

  test.describe('Accessibility', () => {
    test('should have proper navigation aria-label', async ({ page }) => {
      const nav = page.locator('header nav.nav');
      const ariaLabel = await nav.getAttribute('aria-label');
      expect(ariaLabel).toBeTruthy();
      expect(ariaLabel.toLowerCase()).toContain('navigation');
    });

    test('should have proper logo aria-label', async ({ page }) => {
      const logo = page.locator('.nav__logo');
      const ariaLabel = await logo.getAttribute('aria-label');
      expect(ariaLabel).toBeTruthy();
    });
  });
});
