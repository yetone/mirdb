/**
 * Navigation E2E Tests
 * Owner: Scenario 7 - Navigation and Smooth Scrolling
 *
 * Tests for:
 * - Fixed header presence and visibility
 * - Navigation links presence
 * - Smooth scrolling behavior
 * - Header remains fixed while scrolling
 */
import { test, expect } from '@playwright/test';

test.describe('Navigation and Smooth Scrolling', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for page to be fully loaded
    await page.waitForLoadState('networkidle');
  });

  test('should display fixed header with navigation menu at top of page', async ({ page }) => {
    const header = page.locator('header.header');

    // Check header is visible
    await expect(header).toBeVisible();

    // Check header is fixed
    const headerStyle = await header.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        position: styles.position,
        top: styles.top,
        left: styles.left
      };
    });

    expect(headerStyle.position).toBe('fixed');
    expect(headerStyle.top).toBe('0px');
    expect(headerStyle.left).toBe('0px');

    // Check navigation is present within header
    const nav = header.locator('nav.nav');
    await expect(nav).toBeVisible();

    // Check nav has links
    const navLinks = header.locator('.nav-link');
    await expect(navLinks).toHaveCount(5);
  });

  test('should smoothly scroll to Features section when clicking nav link', async ({ page }) => {
    const featuresSection = page.locator('#features');
    const featuresLink = page.locator('.nav-link[href="#features"]');

    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Click Features link
    await featuresLink.click();

    // Wait for smooth scroll animation to complete
    await page.waitForTimeout(1000);

    // Check that the page has scrolled
    const finalScrollY = await page.evaluate(() => window.scrollY);
    expect(finalScrollY).toBeGreaterThan(initialScrollY);

    // Check that Features section is now visible in viewport
    await expect(featuresSection).toBeInViewport();
  });

  test('should smoothly scroll to Quick Start section when clicking nav link', async ({ page }) => {
    const quickStartSection = page.locator('#quick-start');
    const quickStartLink = page.locator('.nav-link[href="#quick-start"]');

    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Click Quick Start link
    await quickStartLink.click();

    // Wait for smooth scroll animation to complete
    await page.waitForTimeout(1000);

    // Check that the page has scrolled
    const finalScrollY = await page.evaluate(() => window.scrollY);
    expect(finalScrollY).toBeGreaterThan(initialScrollY);

    // Check that Quick Start section is now visible in viewport
    await expect(quickStartSection).toBeInViewport();
  });

  test('should keep header fixed at top while scrolling to bottom', async ({ page }) => {
    const header = page.locator('header.header');

    // Scroll to the bottom of the page
    await page.evaluate(() => {
      window.scrollTo(0, document.body.scrollHeight);
    });

    // Wait for scroll to complete
    await page.waitForTimeout(500);

    // Verify we actually scrolled
    const scrollY = await page.evaluate(() => window.scrollY);
    expect(scrollY).toBeGreaterThan(100);

    // Check header is still visible
    await expect(header).toBeVisible();

    // Check header is at top of viewport
    const headerBox = await header.boundingBox();
    expect(headerBox).not.toBeNull();
    expect(headerBox!.y).toBe(0);

    // Check header position is still fixed
    const position = await header.evaluate((el) =>
      window.getComputedStyle(el).position
    );
    expect(position).toBe('fixed');
  });

  test('should have navigation links to all sections: Features, Quick Start, Architecture, Protocol, Status', async ({ page }) => {
    const navLinks = page.locator('.nav-link');

    // Check total count
    await expect(navLinks).toHaveCount(5);

    // Check each section link exists
    const expectedSections = [
      { href: '#features', text: 'Features' },
      { href: '#quick-start', text: 'Quick Start' },
      { href: '#architecture', text: 'Architecture' },
      { href: '#protocol', text: 'Protocol' },
      { href: '#status', text: 'Status' }
    ];

    for (const section of expectedSections) {
      const link = page.locator(`.nav-link[href="${section.href}"]`);
      await expect(link).toBeVisible();
      await expect(link).toHaveText(section.text);
    }
  });

  test('should scroll to each section when clicking corresponding nav link', async ({ page }) => {
    const sections = ['#features', '#quick-start', '#architecture', '#protocol', '#status'];

    for (const sectionId of sections) {
      // Scroll back to top first
      await page.evaluate(() => window.scrollTo(0, 0));
      await page.waitForTimeout(300);

      const section = page.locator(sectionId);
      const link = page.locator(`.nav-link[href="${sectionId}"]`);

      // Click the nav link
      await link.click();

      // Wait for smooth scroll
      await page.waitForTimeout(800);

      // Check section is in viewport
      await expect(section).toBeInViewport();
    }
  });

  test('should show logo in header', async ({ page }) => {
    const logo = page.locator('.header-logo');
    const logoText = page.locator('.logo-text');

    await expect(logo).toBeVisible();
    await expect(logoText).toHaveText('MirDB');
  });

  test('should add scrolled class to header when scrolled down', async ({ page }) => {
    const header = page.locator('header.header');

    // Check header does not have scrolled class initially
    await expect(header).not.toHaveClass(/scrolled/);

    // Scroll down a bit
    await page.evaluate(() => window.scrollTo(0, 100));
    await page.waitForTimeout(300);

    // Check header now has scrolled class
    await expect(header).toHaveClass(/scrolled/);

    // Scroll back to top
    await page.evaluate(() => window.scrollTo(0, 0));
    await page.waitForTimeout(300);

    // Check header no longer has scrolled class
    await expect(header).not.toHaveClass(/scrolled/);
  });
});

test.describe('Navigation - Mobile Menu', () => {
  test.beforeEach(async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');
    await page.waitForLoadState('networkidle');
  });

  test('should show mobile menu toggle on mobile viewport', async ({ page }) => {
    const toggleButton = page.locator('.mobile-menu-toggle');
    await expect(toggleButton).toBeVisible();
  });

  test('should toggle mobile menu when clicking hamburger', async ({ page }) => {
    const toggleButton = page.locator('.mobile-menu-toggle');
    const nav = page.locator('nav.nav');

    // Nav should be hidden initially on mobile
    await expect(nav).not.toBeVisible();

    // Click toggle to open menu
    await toggleButton.click();
    await page.waitForTimeout(100);

    // Nav should now be visible
    await expect(nav).toBeVisible();

    // Check aria-expanded is true
    await expect(toggleButton).toHaveAttribute('aria-expanded', 'true');

    // Click toggle again to close menu
    await toggleButton.click();
    await page.waitForTimeout(100);

    // Nav should be hidden again
    await expect(nav).not.toBeVisible();

    // Check aria-expanded is false
    await expect(toggleButton).toHaveAttribute('aria-expanded', 'false');
  });

  test('should close mobile menu after clicking nav link', async ({ page }) => {
    const toggleButton = page.locator('.mobile-menu-toggle');
    const nav = page.locator('nav.nav');
    const featuresLink = page.locator('.nav-link[href="#features"]');

    // Open menu
    await toggleButton.click();
    await page.waitForTimeout(100);
    await expect(nav).toBeVisible();

    // Click a nav link
    await featuresLink.click();
    await page.waitForTimeout(800);

    // Menu should be closed
    await expect(nav).not.toBeVisible();
  });
});
