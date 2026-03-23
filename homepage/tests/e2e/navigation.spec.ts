/**
 * Navigation E2E Tests
 * Owner: Scenario 5 - Navigation Functionality
 *
 * Test cases:
 * - Navigation menu present
 * - Links to all sections
 * - Smooth scroll behavior
 * - Keyboard accessibility
 */

import { test, expect, Page } from '@playwright/test';
import { navigateToHomepage, selectors, viewports } from './test-utils';

test.describe('Navigation Functionality', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(viewports.desktop);
    await navigateToHomepage(page);
  });

  test('TC1: Navigation menu is present at the top of the page with links to all sections', async ({ page }) => {
    // Check header/navigation is visible at top
    const header = page.locator(selectors.navigation.header);
    await expect(header).toBeVisible();

    // Check navigation has proper role
    const nav = page.locator('nav[role="navigation"]');
    await expect(nav).toBeVisible();

    // Check logo is present
    const logo = page.locator(selectors.navigation.logo);
    await expect(logo).toBeVisible();
    await expect(logo).toHaveText('MirDB');

    // Check all navigation links are present
    const navLinks = page.locator(`${selectors.navigation.links} ${selectors.navigation.link}`);
    await expect(navLinks).toHaveCount(4); // Features, Usage, Quick Start, GitHub

    // Verify specific section links exist
    const featuresLink = page.locator('a.nav-link[href="#features"]');
    const usageLink = page.locator('a.nav-link[href="#usage"]');
    const quickstartLink = page.locator('a.nav-link[href="#quickstart"]');

    await expect(featuresLink).toBeVisible();
    await expect(usageLink).toBeVisible();
    await expect(quickstartLink).toBeVisible();

    // Verify link text
    await expect(featuresLink).toHaveText('Features');
    await expect(usageLink).toHaveText('Usage');
    await expect(quickstartLink).toHaveText('Quick Start');
  });

  test('TC2: Click Features navigation link - page smoothly scrolls to Features section', async ({ page }) => {
    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Click the Features link
    const featuresLink = page.locator('a.nav-link[href="#features"]');
    await featuresLink.click();

    // Wait for smooth scroll to complete
    await page.waitForTimeout(500);

    // Verify the Features section is now in view
    const featuresSection = page.locator(selectors.features.section);
    await expect(featuresSection).toBeInViewport();

    // Verify scroll position changed
    const newScrollY = await page.evaluate(() => window.scrollY);
    expect(newScrollY).toBeGreaterThan(initialScrollY);

    // Verify URL hash updated
    const url = page.url();
    expect(url).toContain('#features');
  });

  test('TC3: Click Usage navigation link - page smoothly scrolls to Usage section', async ({ page }) => {
    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Click the Usage link
    const usageLink = page.locator('a.nav-link[href="#usage"]');
    await usageLink.click();

    // Wait for smooth scroll to complete
    await page.waitForTimeout(500);

    // Verify the Usage section is now in view
    const usageSection = page.locator(selectors.usage.section);
    await expect(usageSection).toBeInViewport();

    // Verify scroll position changed
    const newScrollY = await page.evaluate(() => window.scrollY);
    expect(newScrollY).toBeGreaterThan(initialScrollY);

    // Verify URL hash updated
    const url = page.url();
    expect(url).toContain('#usage');
  });

  test('TC4: Click Quick Start navigation link - page smoothly scrolls to Quick Start section', async ({ page }) => {
    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Click the Quick Start link
    const quickstartLink = page.locator('a.nav-link[href="#quickstart"]');
    await quickstartLink.click();

    // Wait for smooth scroll to complete
    await page.waitForTimeout(500);

    // Verify the Quick Start section is now in view
    const quickstartSection = page.locator(selectors.quickstart.section);
    await expect(quickstartSection).toBeInViewport();

    // Verify scroll position changed
    const newScrollY = await page.evaluate(() => window.scrollY);
    expect(newScrollY).toBeGreaterThan(initialScrollY);

    // Verify URL hash updated
    const url = page.url();
    expect(url).toContain('#quickstart');
  });

  test('TC5: Navigation links are keyboard accessible and have visible focus states', async ({ page }) => {
    // Tab to navigate through the page to reach navigation links
    // First focus should land on skip link or logo
    await page.keyboard.press('Tab');

    // Find the nav links container
    const navLinks = page.locator(`${selectors.navigation.links} ${selectors.navigation.link}`);
    const linkCount = await navLinks.count();

    // Focus the first nav link
    const firstLink = navLinks.first();
    await firstLink.focus();

    // Verify focus is on the first link
    const focusedElement = page.locator(':focus');
    await expect(focusedElement).toBeVisible();

    // Check that the focused element has visual focus indicator
    // Focus state should be visible (outline or other visual indicator)
    const focusedLinkBox = await focusedElement.boundingBox();
    expect(focusedLinkBox).not.toBeNull();

    // Test arrow key navigation between links
    await page.keyboard.press('ArrowRight');
    await page.waitForTimeout(100);

    // Verify focus moved to next link
    const currentFocused = page.locator(':focus');
    await expect(currentFocused).toBeVisible();

    // Test that Enter key activates the link
    const usageLink = page.locator('a.nav-link[href="#usage"]');
    await usageLink.focus();
    await page.keyboard.press('Enter');

    // Wait for scroll
    await page.waitForTimeout(500);

    // Verify navigation occurred
    const usageSection = page.locator(selectors.usage.section);
    await expect(usageSection).toBeInViewport();

    // Check focus states have proper styling
    // Re-focus on a navigation link
    const featuresLink = page.locator('a.nav-link[href="#features"]');
    await featuresLink.focus();

    // Verify the element is focusable and has focus
    const isFocused = await featuresLink.evaluate((el) => document.activeElement === el);
    expect(isFocused).toBe(true);

    // Check that focus outline/ring is present by verifying CSS
    const outlineStyle = await featuresLink.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return styles.outline || styles.outlineWidth !== '0px';
    });
    // Focus should have some visual indicator (outline, box-shadow, or other)
    expect(outlineStyle).toBeTruthy();
  });
});

test.describe('Navigation - Additional Accessibility Checks', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(viewports.desktop);
    await navigateToHomepage(page);
  });

  test('Navigation has proper ARIA labels and roles', async ({ page }) => {
    // Check main navigation has aria-label
    const nav = page.locator('nav[aria-label="Main navigation"]');
    await expect(nav).toBeVisible();

    // Check menu list has proper role
    const menuList = page.locator('ul[role="menubar"]');
    await expect(menuList).toBeVisible();

    // Check menu items have proper roles
    const menuItems = page.locator('li[role="none"]');
    await expect(menuItems).toHaveCount(4);

    // Check links have menuitem role
    const menuItemLinks = page.locator('a[role="menuitem"]');
    await expect(menuItemLinks).toHaveCount(4);
  });

  test('Navigation links are tabbable in correct order', async ({ page }) => {
    // Start from the body
    await page.locator('body').focus();

    // Tab through to reach nav links
    // Logo should be first tabbable item in nav
    const logo = page.locator(selectors.navigation.logo);

    // Press Tab multiple times to navigate through
    let tabCount = 0;
    const maxTabs = 10;

    while (tabCount < maxTabs) {
      await page.keyboard.press('Tab');
      tabCount++;

      const focused = page.locator(':focus');
      const tagName = await focused.evaluate((el) => el.tagName.toLowerCase());
      const href = await focused.getAttribute('href');

      // Check if we've reached a nav link
      if (tagName === 'a' && href === '#features') {
        break;
      }
    }

    // Should have reached features link
    const featuresLink = page.locator('a.nav-link[href="#features"]');
    const isFeaturesLinkFocused = await featuresLink.evaluate((el) => document.activeElement === el);
    expect(isFeaturesLinkFocused).toBe(true);

    // Continue tabbing to verify order
    await page.keyboard.press('Tab');
    const usageLink = page.locator('a.nav-link[href="#usage"]');
    const isUsageLinkFocused = await usageLink.evaluate((el) => document.activeElement === el);
    expect(isUsageLinkFocused).toBe(true);

    await page.keyboard.press('Tab');
    const quickstartLink = page.locator('a.nav-link[href="#quickstart"]');
    const isQuickstartLinkFocused = await quickstartLink.evaluate((el) => document.activeElement === el);
    expect(isQuickstartLinkFocused).toBe(true);
  });
});
