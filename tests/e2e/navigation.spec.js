/**
 * Navigation Tests
 * Owner: Scenario 4 - Navigation Menu
 *        Scenario 11 - Smooth Scroll Navigation
 *
 * Tests:
 * - Nav menu presence
 * - Features link
 * - Quick Start link
 * - Smooth scroll behavior
 * - Hover states
 */

const { test, expect } = require('@playwright/test');
const { SELECTORS, gotoHomepage } = require('./test-utils');

test.describe('Navigation Menu Functionality', () => {
    test.beforeEach(async ({ page }) => {
        await gotoHomepage(page);
    });

    // Test Case 1: Navigation element exists with menu items
    test('navigation element (nav) exists with menu items', async ({ page }) => {
        // Check nav element exists
        const nav = page.locator(SELECTORS.nav);
        await expect(nav).toBeVisible();

        // Check nav has the proper role
        await expect(nav).toHaveAttribute('role', 'navigation');
        await expect(nav).toHaveAttribute('aria-label', 'Main navigation');

        // Check nav links exist
        const navLinks = page.locator(SELECTORS.navLinks);
        await expect(navLinks).toBeVisible();

        // Check there are at least 2 navigation links (Features and Quick Start)
        const navLinkItems = page.locator(`${SELECTORS.navLinks} li`);
        await expect(navLinkItems).toHaveCount(3); // Features, Quick Start, GitHub
    });

    // Test Case 2: Navigation contains link to Features section with anchor #features
    test('navigation contains link to Features section with anchor #features', async ({ page }) => {
        const featuresLink = page.locator('nav a[href="#features"]');
        await expect(featuresLink).toBeVisible();
        await expect(featuresLink).toHaveText('Features');
    });

    // Test Case 3: Navigation contains link to Quick Start section with anchor #quickstart
    test('navigation contains link to Quick Start section with anchor #quickstart', async ({ page }) => {
        const quickstartLink = page.locator('nav a[href="#quickstart"]');
        await expect(quickstartLink).toBeVisible();
        await expect(quickstartLink).toHaveText('Quick Start');
    });

    // Test Case 4: Page scrolls to Features section smoothly when clicking Features nav link
    test('clicking Features nav link scrolls to Features section smoothly', async ({ page }) => {
        // Get initial scroll position
        const initialScrollY = await page.evaluate(() => window.scrollY);

        // Click the Features link
        await page.click('nav a[href="#features"]');

        // Wait for scroll animation to complete
        await page.waitForTimeout(500);

        // Check that the page has scrolled
        const newScrollY = await page.evaluate(() => window.scrollY);
        expect(newScrollY).toBeGreaterThan(initialScrollY);

        // Verify the Features section is in viewport
        const featuresSection = page.locator(SELECTORS.features);
        await expect(featuresSection).toBeInViewport({ ratio: 0.3 });

        // Verify smooth scroll is enabled via CSS
        const scrollBehavior = await page.evaluate(() => {
            return getComputedStyle(document.documentElement).scrollBehavior;
        });
        expect(scrollBehavior).toBe('smooth');
    });

    // Test Case 5: Page scrolls to Quick Start section smoothly when clicking Quick Start nav link
    test('clicking Quick Start nav link scrolls to Quick Start section smoothly', async ({ page }) => {
        // Get initial scroll position
        const initialScrollY = await page.evaluate(() => window.scrollY);

        // Click the Quick Start link
        await page.click('nav a[href="#quickstart"]');

        // Wait for scroll animation to complete
        await page.waitForTimeout(500);

        // Check that the page has scrolled
        const newScrollY = await page.evaluate(() => window.scrollY);
        expect(newScrollY).toBeGreaterThan(initialScrollY);

        // Verify the Quick Start section is in viewport
        const quickstartSection = page.locator(SELECTORS.quickstart);
        await expect(quickstartSection).toBeInViewport({ ratio: 0.3 });
    });

    // Additional supporting tests for navigation behavior
    test('navigation logo links to home', async ({ page }) => {
        const navLogo = page.locator(SELECTORS.navLogo);
        await expect(navLogo).toBeVisible();
        await expect(navLogo).toHaveAttribute('href', '#');
        await expect(navLogo).toHaveText('MirDB');
    });

    test('navigation has proper accessibility attributes', async ({ page }) => {
        // Check the header has banner role
        const header = page.locator(SELECTORS.header);
        await expect(header).toHaveAttribute('role', 'banner');

        // Check navigation has proper labeling
        const nav = page.locator(SELECTORS.nav);
        await expect(nav).toHaveAttribute('aria-label', 'Main navigation');
    });
});
