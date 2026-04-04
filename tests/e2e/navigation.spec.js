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

/**
 * Scenario 11: Smooth Scroll and Interactive Elements
 * Tests for smooth scroll behavior and hover states
 */
test.describe('Smooth Scroll and Interactive Elements', () => {
    test.beforeEach(async ({ page }) => {
        await gotoHomepage(page);
    });

    // Test Case 1: Check CSS scroll-behavior property
    test('HTML or body has scroll-behavior: smooth applied', async ({ page }) => {
        // Check scroll-behavior on html element
        const htmlScrollBehavior = await page.evaluate(() => {
            return getComputedStyle(document.documentElement).scrollBehavior;
        });

        // Check scroll-behavior on body element as fallback
        const bodyScrollBehavior = await page.evaluate(() => {
            return getComputedStyle(document.body).scrollBehavior;
        });

        // Either html or body should have smooth scroll behavior
        const hasSmooth = htmlScrollBehavior === 'smooth' || bodyScrollBehavior === 'smooth';
        expect(hasSmooth).toBe(true);
    });

    // Test Case 2: Check button hover states
    test('buttons change appearance on hover (color, shadow, or transform)', async ({ page }) => {
        // Test primary button (Get Started)
        const primaryBtn = page.locator(SELECTORS.heroCtaPrimary);
        await expect(primaryBtn).toBeVisible();

        // Get initial button styles
        const initialPrimaryStyles = await primaryBtn.evaluate((el) => {
            const styles = getComputedStyle(el);
            return {
                backgroundColor: styles.backgroundColor,
                boxShadow: styles.boxShadow,
                transform: styles.transform
            };
        });

        // Hover over the button and wait for transition
        await primaryBtn.hover();
        await page.waitForTimeout(200);

        // Get styles after hover
        const hoverPrimaryStyles = await primaryBtn.evaluate((el) => {
            const styles = getComputedStyle(el);
            return {
                backgroundColor: styles.backgroundColor,
                boxShadow: styles.boxShadow,
                transform: styles.transform
            };
        });

        // Check that at least one style property changed on hover
        const primaryStyleChanged =
            initialPrimaryStyles.backgroundColor !== hoverPrimaryStyles.backgroundColor ||
            initialPrimaryStyles.boxShadow !== hoverPrimaryStyles.boxShadow ||
            initialPrimaryStyles.transform !== hoverPrimaryStyles.transform;

        expect(primaryStyleChanged).toBe(true);

        // Test secondary button (View on GitHub)
        const secondaryBtn = page.locator(SELECTORS.heroCtaSecondary);
        await expect(secondaryBtn).toBeVisible();

        // Get initial button styles
        const initialSecondaryStyles = await secondaryBtn.evaluate((el) => {
            const styles = getComputedStyle(el);
            return {
                backgroundColor: styles.backgroundColor,
                boxShadow: styles.boxShadow,
                transform: styles.transform,
                color: styles.color
            };
        });

        // Hover over the button and wait for transition
        await secondaryBtn.hover();
        await page.waitForTimeout(200);

        // Get styles after hover
        const hoverSecondaryStyles = await secondaryBtn.evaluate((el) => {
            const styles = getComputedStyle(el);
            return {
                backgroundColor: styles.backgroundColor,
                boxShadow: styles.boxShadow,
                transform: styles.transform,
                color: styles.color
            };
        });

        // Check that at least one style property changed on hover
        const secondaryStyleChanged =
            initialSecondaryStyles.backgroundColor !== hoverSecondaryStyles.backgroundColor ||
            initialSecondaryStyles.boxShadow !== hoverSecondaryStyles.boxShadow ||
            initialSecondaryStyles.transform !== hoverSecondaryStyles.transform ||
            initialSecondaryStyles.color !== hoverSecondaryStyles.color;

        expect(secondaryStyleChanged).toBe(true);
    });

    // Test Case 3: Check link hover states
    test('links have visual hover indication (underline, color change)', async ({ page }) => {
        // Test navigation links
        const navLinks = page.locator(`${SELECTORS.navLinks} a`).first();
        await expect(navLinks).toBeVisible();

        // Get initial link styles
        const initialNavStyles = await navLinks.evaluate((el) => {
            const styles = getComputedStyle(el);
            return {
                color: styles.color,
                textDecoration: styles.textDecoration,
                textDecorationLine: styles.textDecorationLine
            };
        });

        // Hover over the link and wait for transition
        await navLinks.hover();
        await page.waitForTimeout(200);

        // Get styles after hover
        const hoverNavStyles = await navLinks.evaluate((el) => {
            const styles = getComputedStyle(el);
            return {
                color: styles.color,
                textDecoration: styles.textDecoration,
                textDecorationLine: styles.textDecorationLine
            };
        });

        // Check that color or text-decoration changed
        const navStyleChanged =
            initialNavStyles.color !== hoverNavStyles.color ||
            initialNavStyles.textDecoration !== hoverNavStyles.textDecoration ||
            initialNavStyles.textDecorationLine !== hoverNavStyles.textDecorationLine;

        expect(navStyleChanged).toBe(true);

        // Test footer link
        const footerLink = page.locator(SELECTORS.footerLink);

        // Scroll to footer to ensure it's visible
        await footerLink.scrollIntoViewIfNeeded();
        await expect(footerLink).toBeVisible();

        // Get initial footer link styles
        const initialFooterStyles = await footerLink.evaluate((el) => {
            const styles = getComputedStyle(el);
            return {
                color: styles.color,
                textDecoration: styles.textDecoration,
                textDecorationLine: styles.textDecorationLine
            };
        });

        // Hover over the footer link and wait for transition
        await footerLink.hover();
        await page.waitForTimeout(200);

        // Get styles after hover
        const hoverFooterStyles = await footerLink.evaluate((el) => {
            const styles = getComputedStyle(el);
            return {
                color: styles.color,
                textDecoration: styles.textDecoration,
                textDecorationLine: styles.textDecorationLine
            };
        });

        // Check that color or text-decoration changed
        const footerStyleChanged =
            initialFooterStyles.color !== hoverFooterStyles.color ||
            initialFooterStyles.textDecoration !== hoverFooterStyles.textDecoration ||
            initialFooterStyles.textDecorationLine !== hoverFooterStyles.textDecorationLine;

        expect(footerStyleChanged).toBe(true);
    });
});
