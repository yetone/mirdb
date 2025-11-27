/**
 * Navigation Menu Functionality Tests
 * Test cases for verifying navigation between sections works correctly as per REQ-8
 */

const { test, expect } = require('@playwright/test');

test.describe('Navigation Menu Functionality', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
    });

    test.describe('Test Case 1: Navigation Menu Existence', () => {
        test('should have a navigation menu element', async ({ page }) => {
            const navMenu = page.locator('nav[data-testid="navigation-menu"]');

            await expect(navMenu).toBeVisible();
            const tagName = await navMenu.evaluate(el => el.tagName);
            expect(tagName).toBe('NAV');
        });

        test('navigation menu should contain links to Features, Usage, and Getting Started', async ({ page }) => {
            const navMenu = page.locator('nav[data-testid="navigation-menu"]');
            const links = navMenu.locator('a[href^="#"]');

            // Check that we have internal navigation links
            await expect(links).toHaveCount(3);

            // Verify link texts
            const linkTexts = await links.allTextContents();
            const linkHrefs = await links.evaluateAll(els => els.map(el => el.getAttribute('href')));

            // Check for Features link
            expect(linkTexts.some(text => text.toLowerCase().includes('feature'))).toBe(true);
            expect(linkHrefs.some(href => href === '#features')).toBe(true);

            // Check for Usage link
            expect(linkTexts.some(text => text.toLowerCase().includes('usage'))).toBe(true);
            expect(linkHrefs.some(href => href === '#usage')).toBe(true);

            // Check for Getting Started link
            expect(linkTexts.some(text => text.toLowerCase().includes('getting started') || text.toLowerCase().includes('started'))).toBe(true);
            expect(linkHrefs.some(href => href === '#getting-started')).toBe(true);
        });

        test('links should have proper href attributes pointing to corresponding sections', async ({ page }) => {
            const navMenu = page.locator('nav[data-testid="navigation-menu"]');
            const links = navMenu.locator('a');

            const linkCount = await links.count();
            for (let i = 0; i < linkCount; i++) {
                const href = await links.nth(i).getAttribute('href');

                // All internal links should start with #
                if (href && href.startsWith('#')) {
                    const targetSection = page.locator(href);
                    await expect(targetSection).toBeVisible();
                }
            }
        });
    });

    test.describe('Test Case 2: Smooth Scroll Behavior', () => {
        test('clicking navigation links should scroll to target section', async ({ page }) => {
            const navMenu = page.locator('nav[data-testid="navigation-menu"]');
            await expect(navMenu).toBeVisible();

            const links = navMenu.locator('a[href^="#"]');
            const linkCount = await links.count();
            expect(linkCount).toBeGreaterThan(0);

            // Test clicking each nav link
            for (let i = 0; i < linkCount; i++) {
                const link = links.nth(i);
                const href = await link.getAttribute('href');

                if (href) {
                    await link.click();

                    // Give time for scroll to complete
                    await page.waitForTimeout(500);

                    // Verify we're at the target section
                    const targetSection = page.locator(href);
                    await expect(targetSection).toBeInViewport();
                }
            }
        });

        test('links should have smooth scrolling classes and attributes', async ({ page }) => {
            const navMenu = page.locator('nav[data-testid="navigation-menu"]');
            const links = navMenu.locator('a[href^="#"]');

            const linkCount = await links.count();
            for (let i = 0; i < linkCount; i++) {
                const link = links.nth(i);

                // Verify smooth scroll behavior classes
                await expect(link).toHaveClass(/smooth-scroll/);
                await expect(link).toHaveAttribute('data-scroll', 'smooth');
            }
        });
    });

    test.describe('Navigation Menu Accessibility', () => {
        test('navigation menu should be keyboard accessible', async ({ page }) => {
            const navMenu = page.locator('nav[data-testid="navigation-menu"]');
            const links = navMenu.locator('a');

            // Tab to the first link
            await page.keyboard.press('Tab');

            // Verify we can tab through navigation links
            await expect(links.first()).toBeFocused();
        });

        test('links should have descriptive text for screen readers', async ({ page }) => {
            const navMenu = page.locator('nav[data-testid="navigation-menu"]');
            const links = navMenu.locator('a');

            const linkCount = await links.count();
            for (let i = 0; i < linkCount; i++) {
                const textContent = await links.nth(i).textContent();
                expect(textContent.trim().length).toBeGreaterThan(0);
            }
        });
    });

    test.describe('Navigation Menu Styling', () => {
        test('navigation menu should have appropriate styling classes', async ({ page }) => {
            const navMenu = page.locator('nav[data-testid="navigation-menu"]');

            await expect(navMenu).toHaveClass(/navigation-menu/);
            await expect(navMenu).toHaveClass(/sticky/);
        });

        test('navigation menu should be fixed at the top of the page', async ({ page }) => {
            const navMenu = page.locator('nav[data-testid="navigation-menu"]');

            // Verify position is sticky or fixed
            const position = await navMenu.evaluate(el => {
                const style = window.getComputedStyle(el);
                return style.position;
            });
            expect(['fixed', 'sticky']).toContain(position);
        });
    });

    test.describe('Navigation from All Sections', () => {
        test('navigation menu should be accessible from Features section', async ({ page }) => {
            const navMenu = page.locator('nav[data-testid="navigation-menu"]');
            await expect(navMenu).toBeVisible();

            // Navigate to features section
            const featuresLink = navMenu.locator('a[href="#features"]');
            await featuresLink.click();

            // Give time for scroll to complete
            await page.waitForTimeout(500);

            // Verify we're at the features section
            const featuresSection = page.locator('#features');
            await expect(featuresSection).toBeInViewport();

            // Navigation should still be visible
            await expect(navMenu).toBeVisible();
        });

        test('navigation menu should be accessible from Getting Started section', async ({ page }) => {
            const navMenu = page.locator('nav[data-testid="navigation-menu"]');

            // Navigate to getting started section
            const gettingStartedLink = navMenu.locator('a[href="#getting-started"]');
            await gettingStartedLink.click();

            // Give time for scroll to complete
            await page.waitForTimeout(500);

            // Verify we're at the getting started section
            const gettingStartedSection = page.locator('#getting-started');
            await expect(gettingStartedSection).toBeInViewport();

            // Navigation should still be visible
            await expect(navMenu).toBeVisible();
        });

        test('nav links should navigate to corresponding sections', async ({ page }) => {
            const navMenu = page.locator('nav[data-testid="navigation-menu"]');
            const links = navMenu.locator('a[href^="#"]');

            const linkCount = await links.count();
            for (let i = 0; i < linkCount; i++) {
                const link = links.nth(i);
                const href = await link.getAttribute('href');

                if (href && href.startsWith('#')) {
                    await link.click();
                    await page.waitForTimeout(500);

                    const targetSection = page.locator(href);
                    await expect(targetSection).toBeInViewport();

                    // Verify the section has the correct id
                    const sectionId = await targetSection.getAttribute('id');
                    expect(sectionId).toBe(href.substring(1));
                }
            }
        });
    });
});
