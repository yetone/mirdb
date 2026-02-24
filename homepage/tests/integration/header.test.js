/**
 * Header Integration Tests
 * Owner: Scenario 2 - Navigation Header
 *
 * Test cases:
 * - Header renders with logo
 * - GitHub link is present and correct
 * - Documentation link is present
 * - Theme toggle button is present
 * - Mobile menu appears at small viewport
 */

import { test, expect } from '@playwright/test';

test.describe('Navigation Header', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('Header Elements', () => {
    test('Header contains MirDB logo/text', async ({ page }) => {
      const header = page.locator('header.header');
      await expect(header).toBeVisible();

      // Check for logo link
      const logo = header.locator('.logo');
      await expect(logo).toBeVisible();

      // Check for MirDB text
      const logoText = header.locator('.logo-text');
      await expect(logoText).toHaveText('MirDB');
    });

    test('Header contains GitHub link with correct URL', async ({ page }) => {
      const githubLink = page.locator('#github-link');
      await expect(githubLink).toBeVisible();
      await expect(githubLink).toHaveAttribute('href', 'https://github.com/example/mirdb');
      await expect(githubLink).toHaveAttribute('target', '_blank');
      await expect(githubLink).toHaveAttribute('rel', /noopener/);
    });

    test('Header contains Documentation link', async ({ page }) => {
      const docsLink = page.locator('#docs-link');
      await expect(docsLink).toBeVisible();
      await expect(docsLink).toHaveAttribute('href', 'https://mirdb.io/docs');
      await expect(docsLink).toContainText('Documentation');
    });

    test('Header contains theme toggle button', async ({ page }) => {
      const themeToggle = page.locator('#theme-toggle');
      await expect(themeToggle).toBeVisible();
      await expect(themeToggle).toHaveAttribute('aria-label', 'Toggle dark mode');
    });

    test('Header is fixed/sticky at top of page', async ({ page }) => {
      const header = page.locator('header.header');
      await expect(header).toHaveCSS('position', 'fixed');
      await expect(header).toHaveCSS('top', '0px');
    });

    test('Navigation has proper ARIA label', async ({ page }) => {
      const nav = page.locator('header nav[aria-label="Main navigation"]');
      await expect(nav).toBeVisible();
    });
  });

  test.describe('Desktop Navigation', () => {
    test.beforeEach(async ({ page }) => {
      // Ensure desktop viewport
      await page.setViewportSize({ width: 1280, height: 720 });
    });

    test('Desktop nav links are visible', async ({ page }) => {
      const navLinks = page.locator('.nav-links');
      await expect(navLinks).toBeVisible();

      // Check Quick Start link
      const quickStartLink = navLinks.locator('a[href="#quickstart"]');
      await expect(quickStartLink).toBeVisible();

      // Check Features link
      const featuresLink = navLinks.locator('a[href="#features"]');
      await expect(featuresLink).toBeVisible();
    });

    test('Hamburger button is hidden on desktop', async ({ page }) => {
      const hamburgerBtn = page.locator('#hamburger-btn');
      await expect(hamburgerBtn).toBeHidden();
    });
  });

  test.describe('Mobile Navigation at 375px viewport', () => {
    test.beforeEach(async ({ page }) => {
      // Set mobile viewport
      await page.setViewportSize({ width: 375, height: 667 });
    });

    test('Hamburger menu icon is visible, nav links are hidden', async ({ page }) => {
      // Desktop nav links should be hidden
      const navLinks = page.locator('.nav-links');
      await expect(navLinks).toBeHidden();

      // Hamburger button should be visible
      const hamburgerBtn = page.locator('#hamburger-btn');
      await expect(hamburgerBtn).toBeVisible();
    });

    test('Hamburger button has proper accessibility attributes', async ({ page }) => {
      const hamburgerBtn = page.locator('#hamburger-btn');
      await expect(hamburgerBtn).toHaveAttribute('aria-expanded', 'false');
      await expect(hamburgerBtn).toHaveAttribute('aria-controls', 'mobile-nav');
      await expect(hamburgerBtn).toHaveAttribute('aria-label', 'Open navigation menu');
    });

    test('Mobile navigation menu slides in with all links visible on hamburger click', async ({ page }) => {
      const hamburgerBtn = page.locator('#hamburger-btn');
      const mobileNav = page.locator('#mobile-nav');

      // Initially mobile nav should be hidden (transformed off-screen)
      await expect(mobileNav).toHaveAttribute('aria-hidden', 'true');

      // Click hamburger to open
      await hamburgerBtn.click();

      // Mobile nav should be visible with is-open class
      await expect(mobileNav).toHaveClass(/is-open/);
      await expect(mobileNav).toHaveAttribute('aria-hidden', 'false');

      // Check hamburger button state
      await expect(hamburgerBtn).toHaveAttribute('aria-expanded', 'true');
      await expect(hamburgerBtn).toHaveClass(/is-active/);

      // All mobile nav links should be visible
      const quickStartLink = mobileNav.locator('a[href="#quickstart"]');
      await expect(quickStartLink).toBeVisible();

      const featuresLink = mobileNav.locator('a[href="#features"]');
      await expect(featuresLink).toBeVisible();

      const docsLink = mobileNav.locator('#mobile-docs-link');
      await expect(docsLink).toBeVisible();

      const githubLink = mobileNav.locator('#mobile-github-link');
      await expect(githubLink).toBeVisible();

      const themeToggle = mobileNav.locator('#mobile-theme-toggle');
      await expect(themeToggle).toBeVisible();
    });

    test('Mobile navigation closes on hamburger click again', async ({ page }) => {
      const hamburgerBtn = page.locator('#hamburger-btn');
      const mobileNav = page.locator('#mobile-nav');

      // Open
      await hamburgerBtn.click();
      await expect(mobileNav).toHaveClass(/is-open/);

      // Close
      await hamburgerBtn.click();
      await expect(mobileNav).not.toHaveClass(/is-open/);
      await expect(mobileNav).toHaveAttribute('aria-hidden', 'true');
      await expect(hamburgerBtn).toHaveAttribute('aria-expanded', 'false');
    });

    test('Mobile navigation closes on Escape key', async ({ page }) => {
      const hamburgerBtn = page.locator('#hamburger-btn');
      const mobileNav = page.locator('#mobile-nav');

      // Open
      await hamburgerBtn.click();
      await expect(mobileNav).toHaveClass(/is-open/);

      // Press Escape
      await page.keyboard.press('Escape');

      await expect(mobileNav).not.toHaveClass(/is-open/);
      await expect(hamburgerBtn).toHaveAttribute('aria-expanded', 'false');
    });

    test('Mobile navigation closes when anchor link is clicked', async ({ page }) => {
      const hamburgerBtn = page.locator('#hamburger-btn');
      const mobileNav = page.locator('#mobile-nav');

      // Open
      await hamburgerBtn.click();
      await expect(mobileNav).toHaveClass(/is-open/);

      // Click an anchor link
      const quickStartLink = mobileNav.locator('a[href="#quickstart"]');
      await quickStartLink.click();

      await expect(mobileNav).not.toHaveClass(/is-open/);
    });

    test('Mobile GitHub link is correct', async ({ page }) => {
      const hamburgerBtn = page.locator('#hamburger-btn');
      await hamburgerBtn.click();

      const githubLink = page.locator('#mobile-github-link');
      await expect(githubLink).toHaveAttribute('href', 'https://github.com/example/mirdb');
    });

    test('Mobile Documentation link is correct', async ({ page }) => {
      const hamburgerBtn = page.locator('#hamburger-btn');
      await hamburgerBtn.click();

      const docsLink = page.locator('#mobile-docs-link');
      await expect(docsLink).toHaveAttribute('href', 'https://mirdb.io/docs');
    });
  });
});
