/**
 * Navigation Header E2E Tests
 * Owner: Scenario 1 - Navigation Header
 *
 * Tests:
 * - Header presence and visibility
 * - Navigation links (GitHub, Documentation, About)
 * - Responsive hamburger menu
 * - Theme toggle functionality
 * - Sticky header behavior
 */

import { test, expect } from '@playwright/test';

test.describe('Navigation Header', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Header element exists with navigation links visible', async ({ page }) => {
    // Verify header is present
    const header = page.locator('header.header');
    await expect(header).toBeVisible();

    // Verify logo is present
    const logo = page.locator('.header__logo');
    await expect(logo).toBeVisible();
    await expect(logo).toContainText('MirDB');

    // Get viewport size to determine desktop vs mobile
    const viewportSize = page.viewportSize();
    const isMobile = viewportSize && viewportSize.width < 769;

    const nav = page.locator('.header__nav');
    const hamburgerBtn = page.locator('#hamburger-btn');

    if (isMobile) {
      // On mobile, nav should be hidden and hamburger visible
      await expect(nav).not.toBeVisible();
      await expect(hamburgerBtn).toBeVisible();

      // Open hamburger menu to verify links exist
      await hamburgerBtn.click();
      await expect(nav).toBeVisible();
    } else {
      // On desktop, nav should be visible
      await expect(nav).toBeVisible();
    }

    // Check all navigation links exist (visible after opening menu on mobile)
    const githubLink = page.locator('.header__nav-link').filter({ hasText: 'GitHub' });
    const docsLink = page.locator('.header__nav-link').filter({ hasText: 'Documentation' });
    const aboutLink = page.locator('.header__nav-link').filter({ hasText: 'About' });

    await expect(githubLink).toBeVisible();
    await expect(docsLink).toBeVisible();
    await expect(aboutLink).toBeVisible();
  });

  test('TC2: GitHub link points to correct repository', async ({ page }) => {
    // Get viewport size to determine desktop vs mobile
    const viewportSize = page.viewportSize();
    const isMobile = viewportSize && viewportSize.width < 769;

    // On mobile, open hamburger menu first
    if (isMobile) {
      const hamburgerBtn = page.locator('#hamburger-btn');
      await hamburgerBtn.click();
    }

    const githubLink = page.locator('.header__nav-link[href="https://github.com/yetone/mirdb"]');
    await expect(githubLink).toBeVisible();
    await expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
    await expect(githubLink).toHaveAttribute('target', '_blank');
    await expect(githubLink).toHaveAttribute('rel', 'noopener noreferrer');
  });

  test('TC3: Navigation collapses to hamburger menu on mobile', async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 320, height: 568 });

    // Verify hamburger button is visible
    const hamburgerBtn = page.locator('#hamburger-btn');
    await expect(hamburgerBtn).toBeVisible();

    // Verify nav list is hidden initially
    const nav = page.locator('.header__nav');
    await expect(nav).not.toBeVisible();

    // Click hamburger to open menu
    await hamburgerBtn.click();

    // Verify nav is now visible
    await expect(nav).toBeVisible();

    // Verify hamburger button shows expanded state
    await expect(hamburgerBtn).toHaveAttribute('aria-expanded', 'true');

    // Verify navigation links are visible in mobile menu
    const githubLink = page.locator('.header__nav-link').filter({ hasText: 'GitHub' });
    await expect(githubLink).toBeVisible();

    // Click hamburger again to close
    await hamburgerBtn.click();

    // Verify nav is hidden again
    await expect(nav).not.toBeVisible();
    await expect(hamburgerBtn).toHaveAttribute('aria-expanded', 'false');
  });

  test('TC4: Theme toggle switches between dark and light themes', async ({ page }) => {
    const themeToggle = page.locator('#theme-toggle');
    await expect(themeToggle).toBeVisible();

    // Check initial state (light theme - no data-theme attribute or light)
    const html = page.locator('html');
    const initialTheme = await html.getAttribute('data-theme');

    // Click to toggle theme
    await themeToggle.click();

    // Verify theme changed
    const newTheme = await html.getAttribute('data-theme');

    if (initialTheme === 'dark' || initialTheme === null) {
      // If was dark or unset, should now be different
      if (initialTheme === 'dark') {
        expect(newTheme).not.toBe('dark');
      } else {
        // If unset, clicking should set to dark
        expect(newTheme).toBe('dark');
      }
    }

    // Click again to toggle back
    await themeToggle.click();
    const finalTheme = await html.getAttribute('data-theme');

    // Should toggle back
    if (newTheme === 'dark') {
      expect(finalTheme).not.toBe('dark');
    }
  });

  test('TC5: Navigation header remains sticky when scrolling', async ({ page }) => {
    // Get initial header position
    const header = page.locator('header.header');
    const initialBoundingBox = await header.boundingBox();

    // Scroll down the page
    await page.evaluate(() => window.scrollBy(0, 500));

    // Wait for scroll to complete
    await page.waitForTimeout(100);

    // Get header position after scroll
    const afterScrollBoundingBox = await header.boundingBox();

    // Verify header is still at the top (sticky)
    expect(afterScrollBoundingBox).not.toBeNull();
    expect(afterScrollBoundingBox!.y).toBe(0);

    // Verify header is still visible
    await expect(header).toBeVisible();
    await expect(header).toBeInViewport();

    // Verify header has position: sticky
    const headerStyles = await header.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        position: styles.position,
        top: styles.top,
      };
    });

    expect(headerStyles.position).toBe('sticky');
    expect(headerStyles.top).toBe('0px');
  });

  test('Accessibility: Navigation has proper ARIA attributes', async ({ page }) => {
    // Check header role
    const header = page.locator('header.header');
    await expect(header).toHaveAttribute('role', 'banner');

    // Check nav role
    const nav = page.locator('.header__nav');
    await expect(nav).toHaveAttribute('role', 'navigation');
    await expect(nav).toHaveAttribute('aria-label', 'Main navigation');

    // Check theme toggle has aria-label
    const themeToggle = page.locator('#theme-toggle');
    await expect(themeToggle).toHaveAttribute('aria-label', 'Toggle dark/light theme');

    // Check hamburger has proper ARIA attributes
    const hamburger = page.locator('#hamburger-btn');
    await expect(hamburger).toHaveAttribute('aria-label', 'Toggle navigation menu');
    await expect(hamburger).toHaveAttribute('aria-expanded', 'false');
    await expect(hamburger).toHaveAttribute('aria-controls', 'nav-list');
  });

  test('Skip link is present for accessibility', async ({ page }) => {
    const skipLink = page.locator('.skip-link');
    await expect(skipLink).toHaveText('Skip to main content');
    await expect(skipLink).toHaveAttribute('href', '#main');
  });
});
