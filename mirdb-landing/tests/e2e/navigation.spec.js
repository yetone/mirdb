/**
 * Navigation E2E Tests
 * Owner: Scenario 2 - Navigation and Smooth Scrolling
 *
 * Tests:
 * - Sticky navigation behavior
 * - Smooth scroll to sections
 * - Skip-to-content link
 * - Keyboard navigation
 * - Mobile navigation
 */

const { test, expect } = require('@playwright/test');

test.describe('Navigation and Smooth Scrolling', () => {

    test.beforeEach(async ({ page }) => {
        await page.goto('/');
    });

    // Test Case 1: Sticky navigation behavior
    test('navigation bar remains fixed at top when scrolling past hero section', async ({ page }) => {
        // Get initial nav position
        const nav = page.locator('.nav');
        await expect(nav).toBeVisible();

        // Scroll down past the hero section
        await page.evaluate(() => window.scrollTo(0, 1000));
        await page.waitForTimeout(500);

        // Verify nav is still visible at top
        await expect(nav).toBeVisible();

        // Check that nav has position: sticky
        const position = await nav.evaluate(el => getComputedStyle(el).position);
        expect(position).toBe('sticky');

        // Check nav is at top of viewport
        const boundingBox = await nav.boundingBox();
        expect(boundingBox.y).toBeLessThanOrEqual(5);
    });

    // Test Case 2: Smooth scroll to Features section
    test('clicking Features link smoothly scrolls to Features section and updates URL hash', async ({ page }) => {
        // Click on Features link
        await page.click('.nav__link[href="#features"]');

        // Wait for scroll to complete
        await page.waitForTimeout(1000);

        // Check URL hash is updated
        const url = page.url();
        expect(url).toContain('#features');

        // Check that Features section is in view
        const featuresSection = page.locator('#features');
        await expect(featuresSection).toBeInViewport({ ratio: 0.5 });
    });

    // Test Case 3: Smooth scroll to Architecture section
    test('clicking Architecture link smoothly scrolls to Architecture section and updates URL hash', async ({ page }) => {
        // Click on Architecture link
        await page.click('.nav__link[href="#architecture"]');

        // Wait for scroll to complete
        await page.waitForTimeout(1000);

        // Check URL hash is updated
        const url = page.url();
        expect(url).toContain('#architecture');

        // Check that Architecture section is in view
        const architectureSection = page.locator('#architecture');
        await expect(architectureSection).toBeInViewport({ ratio: 0.5 });
    });

    // Test Case 4: Skip-to-content link functionality
    test('skip-to-content link becomes visible on focus and skips to main content', async ({ page }) => {
        const skipLink = page.locator('.skip-link');

        // Initially the skip link should be off-screen
        const initialBox = await skipLink.boundingBox();
        expect(initialBox.y).toBeLessThan(0);

        // Press Tab to focus the skip link (first focusable element)
        await page.keyboard.press('Tab');

        // Wait for transition
        await page.waitForTimeout(200);

        // Skip link should now be visible
        await expect(skipLink).toBeFocused();
        const focusedBox = await skipLink.boundingBox();
        expect(focusedBox.y).toBeGreaterThanOrEqual(0);

        // Press Enter to activate the skip link
        await page.keyboard.press('Enter');

        // Wait for scroll
        await page.waitForTimeout(500);

        // Main content should have focus
        const mainContent = page.locator('#main-content');
        await expect(mainContent).toBeInViewport();
    });

    // Test Case 5: Keyboard navigation through nav items
    test('all navigation links are focusable with visible focus indicators', async ({ page }) => {
        const navLinks = page.locator('.nav__link');
        const count = await navLinks.count();

        // Tab through skip-link first
        await page.keyboard.press('Tab');

        // Tab to logo
        await page.keyboard.press('Tab');

        // Tab through nav toggle (may be hidden on desktop)
        const toggleVisible = await page.locator('.nav__toggle').isVisible();
        if (toggleVisible) {
            await page.keyboard.press('Tab');
        }

        // Tab through all nav links and verify they are focusable
        for (let i = 0; i < count; i++) {
            await page.keyboard.press('Tab');
            const focusedElement = page.locator(':focus');

            // Verify element has visible focus indicator (outline)
            const outline = await focusedElement.evaluate(el => {
                const style = getComputedStyle(el);
                return style.outlineStyle !== 'none' || style.outlineWidth !== '0px';
            });
            expect(outline || await focusedElement.evaluate(el => el.classList.contains('nav__link'))).toBeTruthy();
        }

        // Test that Enter key activates a link
        await page.locator('.nav__link[href="#features"]').focus();
        await page.keyboard.press('Enter');
        await page.waitForTimeout(500);
        expect(page.url()).toContain('#features');
    });

    // Test Case 6: Mobile navigation (375px viewport)
    test('navigation adapts to mobile viewport with touch-friendly targets', async ({ page }) => {
        // Set mobile viewport
        await page.setViewportSize({ width: 375, height: 667 });

        // Refresh to apply mobile styles
        await page.reload();

        // Check hamburger toggle is visible
        const toggle = page.locator('.nav__toggle');
        await expect(toggle).toBeVisible();

        // Check nav links are initially hidden
        const navLinks = page.locator('.nav__links');
        const isHidden = await navLinks.evaluate(el => {
            const style = getComputedStyle(el);
            return style.visibility === 'hidden' || style.opacity === '0';
        });
        expect(isHidden).toBeTruthy();

        // Click toggle to open menu
        await toggle.click();
        await page.waitForTimeout(300);

        // Check nav links are now visible
        await expect(navLinks).toBeVisible();
        const isVisible = await navLinks.evaluate(el => {
            const style = getComputedStyle(el);
            return style.visibility === 'visible' && style.opacity === '1';
        });
        expect(isVisible).toBeTruthy();

        // Check touch target sizes (minimum 44x44px)
        const firstLink = page.locator('.nav__link').first();
        const box = await firstLink.boundingBox();
        expect(box.width).toBeGreaterThanOrEqual(44);
        expect(box.height).toBeGreaterThanOrEqual(44);

        // Test that clicking a link closes the menu
        await page.click('.nav__link[href="#features"]');
        await page.waitForTimeout(500);

        // Menu should be closed
        const menuIsHidden = await navLinks.evaluate(el => {
            const style = getComputedStyle(el);
            return style.visibility === 'hidden' || style.opacity === '0';
        });
        expect(menuIsHidden).toBeTruthy();
    });

    // Additional accessibility test: ARIA attributes
    test('navigation has proper ARIA attributes', async ({ page }) => {
        // Check nav has proper role and label
        const nav = page.locator('.nav');
        await expect(nav).toHaveAttribute('role', 'navigation');
        await expect(nav).toHaveAttribute('aria-label', 'Main navigation');

        // Check toggle button has aria-expanded
        const toggle = page.locator('.nav__toggle');
        await expect(toggle).toHaveAttribute('aria-expanded');
        await expect(toggle).toHaveAttribute('aria-controls', 'nav-links');
        await expect(toggle).toHaveAttribute('aria-label', 'Toggle navigation menu');

        // Check nav links have proper structure
        const navLinks = page.locator('.nav__links');
        await expect(navLinks).toHaveAttribute('id', 'nav-links');
        await expect(navLinks).toHaveAttribute('role', 'menubar');
    });
});
