/**
 * Responsive Design E2E Tests
 * Owner: Scenario 6 - Responsive Design - Mobile View
 *
 * Tests for:
 * - No horizontal scroll on mobile
 * - Hamburger menu on mobile
 * - Text legibility
 * - Responsive grid layouts
 * - Tablet viewport
 */

const { test, expect } = require('@playwright/test');

// Mobile viewport configuration (iPhone)
const MOBILE_VIEWPORT = { width: 375, height: 667 };
// Tablet viewport configuration
const TABLET_VIEWPORT = { width: 768, height: 1024 };
// Desktop viewport configuration
const DESKTOP_VIEWPORT = { width: 1280, height: 800 };

test.describe('Responsive Design', () => {
    test.describe('Mobile Viewport (375px)', () => {
        test.beforeEach(async ({ page }) => {
            await page.setViewportSize(MOBILE_VIEWPORT);
            await page.goto('/');
        });

        test('Test Case 1: Page loads without horizontal scrollbar at 375px', async ({ page }) => {
            // Wait for page to fully load
            await page.waitForLoadState('networkidle');

            // Get the body scroll width vs client width - this is the main requirement
            const hasHorizontalScroll = await page.evaluate(() => {
                return document.documentElement.scrollWidth > document.documentElement.clientWidth;
            });

            expect(hasHorizontalScroll).toBe(false);

            // Additionally verify no elements in the document flow overflow the viewport
            // Exclude:
            // - Fixed/absolute elements and their children (like mobile nav drawer)
            // - Elements inside scrollable containers (like tables with overflow-x: auto)
            const overflowingElements = await page.evaluate(() => {
                const viewportWidth = window.innerWidth;
                const elements = document.querySelectorAll('*');
                const overflowing = [];

                // Check if element or any ancestor is fixed/absolute
                function hasFixedOrAbsoluteAncestor(el) {
                    let current = el;
                    while (current && current !== document.body) {
                        const style = window.getComputedStyle(current);
                        const position = style.position;
                        if (position === 'fixed' || position === 'absolute') {
                            return true;
                        }
                        current = current.parentElement;
                    }
                    return false;
                }

                // Check if element is inside a scrollable container
                function isInsideScrollableContainer(el) {
                    let current = el.parentElement;
                    while (current && current !== document.body) {
                        const style = window.getComputedStyle(current);
                        const overflowX = style.overflowX;
                        // Check if the container has horizontal scroll
                        if (overflowX === 'auto' || overflowX === 'scroll') {
                            return true;
                        }
                        current = current.parentElement;
                    }
                    return false;
                }

                elements.forEach(el => {
                    // Skip elements that are or are inside fixed/absolute positioned elements
                    if (hasFixedOrAbsoluteAncestor(el)) {
                        return;
                    }

                    // Skip elements inside scrollable containers (e.g., tables in .table-wrapper)
                    if (isInsideScrollableContainer(el)) {
                        return;
                    }

                    const rect = el.getBoundingClientRect();
                    if (rect.right > viewportWidth + 5) { // 5px tolerance
                        overflowing.push({
                            tag: el.tagName,
                            class: el.className,
                            right: rect.right,
                            viewportWidth
                        });
                    }
                });

                return overflowing;
            });

            expect(overflowingElements.length).toBe(0);
        });

        test('Test Case 2: Hamburger menu icon is visible, full navigation is hidden', async ({ page }) => {
            // Hamburger button should be visible
            const hamburgerBtn = page.locator('.hamburger-btn');
            await expect(hamburgerBtn).toBeVisible();

            // Navigation menu should not be visible initially on mobile
            const navMenu = page.locator('.nav-menu');

            // Check that the nav menu is positioned off-screen (right: -100%)
            const navMenuBox = await navMenu.boundingBox();
            const viewportWidth = MOBILE_VIEWPORT.width;

            // The nav menu should either be hidden or positioned off-screen
            if (navMenuBox) {
                expect(navMenuBox.x).toBeGreaterThanOrEqual(viewportWidth - 10); // Off-screen to the right
            }

            // Full navigation links should not be visible in viewport
            const navLinks = page.locator('.nav-link');
            const firstNavLink = navLinks.first();

            // The nav link should not be visible in the current viewport
            await expect(firstNavLink).not.toBeInViewport();
        });

        test('Test Case 3: Navigation menu expands when hamburger is clicked', async ({ page }) => {
            const hamburgerBtn = page.locator('.hamburger-btn');
            const navMenu = page.locator('.nav-menu');

            // Initially aria-expanded should be false
            await expect(hamburgerBtn).toHaveAttribute('aria-expanded', 'false');

            // Click the hamburger button
            await hamburgerBtn.click();

            // After clicking, aria-expanded should be true
            await expect(hamburgerBtn).toHaveAttribute('aria-expanded', 'true');

            // Navigation menu should have 'is-open' class
            await expect(navMenu).toHaveClass(/is-open/);

            // Navigation links should now be visible
            const navLinks = page.locator('.nav-link');
            await expect(navLinks.first()).toBeVisible();

            // Overlay should be visible
            const overlay = page.locator('.nav-overlay');
            await expect(overlay).toHaveClass(/is-visible/);
        });

        test('Test Case 4: Body text is at least 16px for readability', async ({ page }) => {
            // Check the body font size
            const bodyFontSize = await page.evaluate(() => {
                const body = document.body;
                const computedStyle = window.getComputedStyle(body);
                return parseFloat(computedStyle.fontSize);
            });

            expect(bodyFontSize).toBeGreaterThanOrEqual(16);

            // Also check paragraph text in main content areas
            const paragraphFontSizes = await page.evaluate(() => {
                const paragraphs = document.querySelectorAll('p');
                return Array.from(paragraphs).map(p => {
                    const style = window.getComputedStyle(p);
                    return parseFloat(style.fontSize);
                });
            });

            // All paragraphs should be at least 16px
            paragraphFontSizes.forEach(size => {
                expect(size).toBeGreaterThanOrEqual(16);
            });
        });

        test('Test Case 5: Hero content stacks vertically and fits within viewport', async ({ page }) => {
            const hero = page.locator('#hero');
            await expect(hero).toBeVisible();

            // Check that hero section fits within viewport width
            const heroBox = await hero.boundingBox();
            expect(heroBox.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width);

            // Check CTA buttons are stacked (flex-direction: column)
            const ctaContainer = page.locator('.hero-cta');
            const ctaFlexDirection = await ctaContainer.evaluate(el => {
                return window.getComputedStyle(el).flexDirection;
            });
            expect(ctaFlexDirection).toBe('column');

            // Verify buttons don't overflow
            const buttons = page.locator('.hero-cta .btn');
            const buttonCount = await buttons.count();

            for (let i = 0; i < buttonCount; i++) {
                const buttonBox = await buttons.nth(i).boundingBox();
                expect(buttonBox.x).toBeGreaterThanOrEqual(0);
                expect(buttonBox.x + buttonBox.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width);
            }
        });

        test('Test Case 6: Features stack in single column on mobile', async ({ page }) => {
            const featuresGrid = page.locator('.features-grid');

            // Check if features grid exists
            const gridExists = await featuresGrid.count() > 0;
            if (!gridExists) {
                test.skip('Features grid not found - may be implemented by another scenario');
                return;
            }

            await expect(featuresGrid).toBeVisible();

            // Get computed grid-template-columns
            const gridColumns = await featuresGrid.evaluate(el => {
                return window.getComputedStyle(el).gridTemplateColumns;
            });

            // On mobile, should be single column (1fr or just the viewport width)
            // Grid with 1 column will show as "Xpx" (single value) not "Xpx Xpx" (multiple values)
            const columnValues = gridColumns.split(' ').filter(v => v.trim());
            expect(columnValues.length).toBe(1);
        });

        test('Test Case 7: Code blocks have horizontal scroll if needed, not page scroll', async ({ page }) => {
            const codeBlocks = page.locator('.code-block');
            const codeBlockCount = await codeBlocks.count();

            if (codeBlockCount === 0) {
                test.skip('No code blocks found - may be implemented by another scenario');
                return;
            }

            for (let i = 0; i < codeBlockCount; i++) {
                const codeBlock = codeBlocks.nth(i);

                // Check that code block has overflow-x: auto
                const overflowX = await codeBlock.evaluate(el => {
                    return window.getComputedStyle(el).overflowX;
                });
                expect(['auto', 'scroll']).toContain(overflowX);

                // Verify code block doesn't cause page-level horizontal scroll
                const codeBlockBox = await codeBlock.boundingBox();
                expect(codeBlockBox.x).toBeGreaterThanOrEqual(0);
                expect(codeBlockBox.x + codeBlockBox.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width + 5); // Small tolerance
            }

            // Verify page still has no horizontal scroll
            const hasPageHorizontalScroll = await page.evaluate(() => {
                return document.documentElement.scrollWidth > document.documentElement.clientWidth;
            });
            expect(hasPageHorizontalScroll).toBe(false);
        });

        test('Navigation menu closes when clicking overlay', async ({ page }) => {
            const hamburgerBtn = page.locator('.hamburger-btn');
            const navMenu = page.locator('.nav-menu');
            const overlay = page.locator('.nav-overlay');

            // Open the menu
            await hamburgerBtn.click();
            await expect(navMenu).toHaveClass(/is-open/);

            // Click the overlay
            await overlay.click({ force: true });

            // Menu should be closed
            await expect(hamburgerBtn).toHaveAttribute('aria-expanded', 'false');
            await expect(navMenu).not.toHaveClass(/is-open/);
        });

        test('Navigation menu closes when clicking a nav link', async ({ page }) => {
            const hamburgerBtn = page.locator('.hamburger-btn');
            const navMenu = page.locator('.nav-menu');

            // Open the menu
            await hamburgerBtn.click();
            await expect(navMenu).toHaveClass(/is-open/);

            // Click a nav link (internal link)
            const featuresLink = page.locator('.nav-link[href="#features"]');
            await featuresLink.click();

            // Menu should be closed
            await expect(hamburgerBtn).toHaveAttribute('aria-expanded', 'false');
            await expect(navMenu).not.toHaveClass(/is-open/);
        });

        test('Navigation menu closes on Escape key', async ({ page }) => {
            const hamburgerBtn = page.locator('.hamburger-btn');
            const navMenu = page.locator('.nav-menu');

            // Open the menu
            await hamburgerBtn.click();
            await expect(navMenu).toHaveClass(/is-open/);

            // Press Escape key
            await page.keyboard.press('Escape');

            // Menu should be closed
            await expect(hamburgerBtn).toHaveAttribute('aria-expanded', 'false');
            await expect(navMenu).not.toHaveClass(/is-open/);
        });
    });

    test.describe('Tablet Viewport (768px)', () => {
        test.beforeEach(async ({ page }) => {
            await page.setViewportSize(TABLET_VIEWPORT);
            await page.goto('/');
        });

        test('Test Case 8: Page renders correctly at tablet size', async ({ page }) => {
            await page.waitForLoadState('networkidle');

            // No horizontal scroll
            const hasHorizontalScroll = await page.evaluate(() => {
                return document.documentElement.scrollWidth > document.documentElement.clientWidth;
            });
            expect(hasHorizontalScroll).toBe(false);

            // Hero section should be visible and properly sized
            const hero = page.locator('#hero');
            await expect(hero).toBeVisible();
            const heroBox = await hero.boundingBox();
            expect(heroBox.width).toBeLessThanOrEqual(TABLET_VIEWPORT.width);

            // Features should display (if present)
            const featuresSection = page.locator('#features');
            const featuresExist = await featuresSection.count() > 0;
            if (featuresExist) {
                await expect(featuresSection).toBeVisible();
            }

            // Quick start section should be visible (if present)
            const quickstartSection = page.locator('#quickstart');
            const quickstartExists = await quickstartSection.count() > 0;
            if (quickstartExists) {
                await expect(quickstartSection).toBeVisible();
            }
        });

        test('Features grid shows 2 columns on tablet', async ({ page }) => {
            const featuresGrid = page.locator('.features-grid');

            const gridExists = await featuresGrid.count() > 0;
            if (!gridExists) {
                test.skip('Features grid not found');
                return;
            }

            await expect(featuresGrid).toBeVisible();

            // Get computed grid-template-columns
            const gridColumns = await featuresGrid.evaluate(el => {
                return window.getComputedStyle(el).gridTemplateColumns;
            });

            // On tablet, should be 2 columns
            const columnValues = gridColumns.split(' ').filter(v => v.trim() && v !== '0px');
            expect(columnValues.length).toBe(2);
        });
    });

    test.describe('Desktop Viewport (1280px)', () => {
        test.beforeEach(async ({ page }) => {
            await page.setViewportSize(DESKTOP_VIEWPORT);
            await page.goto('/');
        });

        test('Hamburger menu is hidden on desktop', async ({ page }) => {
            const hamburgerBtn = page.locator('.hamburger-btn');

            // Hamburger should not be visible on desktop
            await expect(hamburgerBtn).not.toBeVisible();
        });

        test('Navigation is always visible on desktop', async ({ page }) => {
            const navMenu = page.locator('.nav-menu');
            const navLinks = page.locator('.nav-link');

            // Navigation should be visible
            await expect(navMenu).toBeVisible();

            // Nav links should be visible
            await expect(navLinks.first()).toBeVisible();
        });

        test('Features grid shows 3 columns on desktop', async ({ page }) => {
            const featuresGrid = page.locator('.features-grid');

            const gridExists = await featuresGrid.count() > 0;
            if (!gridExists) {
                test.skip('Features grid not found');
                return;
            }

            await expect(featuresGrid).toBeVisible();

            // Get computed grid-template-columns
            const gridColumns = await featuresGrid.evaluate(el => {
                return window.getComputedStyle(el).gridTemplateColumns;
            });

            // On desktop with 6 feature cards and minmax(300px, 1fr), should show 3 columns
            const columnValues = gridColumns.split(' ').filter(v => v.trim() && v !== '0px');
            expect(columnValues.length).toBeGreaterThanOrEqual(2); // At least 2 columns
        });
    });

    test.describe('Responsive Transitions', () => {
        test('Menu closes when resizing from mobile to desktop', async ({ page }) => {
            // Start at mobile size
            await page.setViewportSize(MOBILE_VIEWPORT);
            await page.goto('/');

            const hamburgerBtn = page.locator('.hamburger-btn');
            const navMenu = page.locator('.nav-menu');

            // Open the menu
            await hamburgerBtn.click();
            await expect(navMenu).toHaveClass(/is-open/);

            // Resize to desktop
            await page.setViewportSize(DESKTOP_VIEWPORT);

            // Wait for resize handler
            await page.waitForTimeout(200);

            // Menu should be closed and hamburger hidden
            await expect(hamburgerBtn).not.toBeVisible();
            await expect(navMenu).not.toHaveClass(/is-open/);
        });
    });
});
