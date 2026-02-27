// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Responsive Design E2E Tests for MirDB Homepage
 *
 * Tests validate responsive layout across mobile (320px), tablet (768px),
 * desktop (1920px), and ultra-wide (2560px) viewports.
 *
 * Scenario 8: Responsive Design
 * REQ-7: Responsive design functional 320px to 2560px
 */

// Viewport configurations
const VIEWPORTS = {
    mobile: { width: 320, height: 568 },
    tablet: { width: 768, height: 1024 },
    desktop: { width: 1920, height: 1080 },
    ultraWide: { width: 2560, height: 1440 }
};

test.describe('Responsive Design - Viewport Tests', () => {

    // Test Case 1: Mobile viewport (320px)
    test('TC1: Page renders without horizontal scroll at 320px width', async ({ page }) => {
        await page.setViewportSize(VIEWPORTS.mobile);
        await page.goto('/');

        // Wait for content to load
        await page.waitForSelector('.hero');

        // Check for horizontal overflow
        const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
        const viewportWidth = await page.evaluate(() => window.innerWidth);

        // Body scroll width should not exceed viewport width (no horizontal scroll)
        expect(bodyWidth).toBeLessThanOrEqual(viewportWidth + 1); // +1 for rounding tolerance

        // Verify all main sections are visible
        await expect(page.locator('.hero')).toBeVisible();
        await expect(page.locator('.features')).toBeVisible();
        await expect(page.locator('.quick-start')).toBeVisible();
        await expect(page.locator('.architecture')).toBeVisible();
        await expect(page.locator('.benchmarks')).toBeVisible();
        await expect(page.locator('.main-footer')).toBeVisible();
    });

    // Test Case 2: Tablet viewport (768px)
    test('TC2: Page renders with tablet-appropriate layout at 768px width', async ({ page }) => {
        await page.setViewportSize(VIEWPORTS.tablet);
        await page.goto('/');

        await page.waitForSelector('.hero');

        // Check no horizontal overflow
        const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
        const viewportWidth = await page.evaluate(() => window.innerWidth);
        expect(bodyWidth).toBeLessThanOrEqual(viewportWidth + 1);

        // Verify content is visible and properly laid out
        await expect(page.locator('.hero')).toBeVisible();
        await expect(page.locator('.features-grid')).toBeVisible();

        // Hero should have appropriate size for tablet
        const heroH1 = page.locator('.hero h1');
        await expect(heroH1).toBeVisible();
    });

    // Test Case 3: Desktop viewport (1920px)
    test('TC3: Page renders with full desktop layout, content centered at 1920px', async ({ page }) => {
        await page.setViewportSize(VIEWPORTS.desktop);
        await page.goto('/');

        await page.waitForSelector('.hero');

        // Verify hero content is visible and centered
        const heroContent = page.locator('.hero-content');
        await expect(heroContent).toBeVisible();

        // Check container is centered (has max-width and auto margins)
        const container = page.locator('.features .container');
        const containerBox = await container.boundingBox();

        // Container should be centered - left margin should be roughly equal to right margin
        if (containerBox) {
            const leftMargin = containerBox.x;
            const rightMargin = VIEWPORTS.desktop.width - (containerBox.x + containerBox.width);
            // Allow some tolerance for margin difference
            expect(Math.abs(leftMargin - rightMargin)).toBeLessThan(100);
        }

        // Verify navigation links are visible on desktop
        const navLinks = page.locator('.nav-links');
        await expect(navLinks).toBeVisible();
    });

    // Test Case 4: Ultra-wide viewport (2560px)
    test('TC4: Page renders correctly at ultra-wide resolution (2560px)', async ({ page }) => {
        await page.setViewportSize(VIEWPORTS.ultraWide);
        await page.goto('/');

        await page.waitForSelector('.hero');

        // Verify page renders without issues
        await expect(page.locator('.hero')).toBeVisible();
        await expect(page.locator('.features')).toBeVisible();

        // Content should still be contained and centered
        const container = page.locator('.features .container');
        const containerBox = await container.boundingBox();

        if (containerBox) {
            // Container should have max-width and not stretch to full viewport
            expect(containerBox.width).toBeLessThanOrEqual(1200 + 64); // max-width + padding
        }

        // No horizontal overflow
        const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
        const viewportWidth = await page.evaluate(() => window.innerWidth);
        expect(bodyWidth).toBeLessThanOrEqual(viewportWidth + 1);
    });
});

test.describe('Responsive Design - Navigation Adaptation', () => {

    // Test Case 5: Navigation at mobile viewport
    test('TC5: Navigation collapses to hamburger menu or stacked layout at 320px', async ({ page }) => {
        await page.setViewportSize(VIEWPORTS.mobile);
        await page.goto('/');

        await page.waitForSelector('.site-header');

        // Check if nav-links are hidden (collapsed navigation)
        const navLinks = page.locator('.nav-links');

        // Navigation should either be hidden or have mobile-specific styling
        // In our implementation, nav-links are hidden on mobile via CSS
        const isNavHidden = await navLinks.evaluate(el => {
            const style = window.getComputedStyle(el);
            return style.display === 'none' || style.visibility === 'hidden';
        });

        expect(isNavHidden).toBe(true);

        // Logo should still be visible
        const navLogo = page.locator('.nav-logo');
        await expect(navLogo).toBeVisible();
    });
});

test.describe('Responsive Design - Features Grid Adaptation', () => {

    // Test Case 6: Features grid at mobile viewport
    test('TC6: Features display in single column layout at 320px', async ({ page }) => {
        await page.setViewportSize(VIEWPORTS.mobile);
        await page.goto('/');

        await page.waitForSelector('.features-grid');

        // Get all feature cards
        const featureCards = page.locator('.feature-card');
        const count = await featureCards.count();
        expect(count).toBeGreaterThan(0);

        // Get bounding boxes of first two cards
        const firstCard = await featureCards.nth(0).boundingBox();
        const secondCard = await featureCards.nth(1).boundingBox();

        if (firstCard && secondCard) {
            // In single column, cards should be stacked vertically
            // Second card should be below first card (higher Y value)
            expect(secondCard.y).toBeGreaterThan(firstCard.y);

            // Cards should have similar X position (same column)
            expect(Math.abs(firstCard.x - secondCard.x)).toBeLessThan(10);
        }
    });

    // Test Case 7: Features grid at desktop viewport
    test('TC7: Features display in 2-3 column grid layout at 1920px', async ({ page }) => {
        await page.setViewportSize(VIEWPORTS.desktop);
        await page.goto('/');

        await page.waitForSelector('.features-grid');

        // Get first three feature cards
        const featureCards = page.locator('.feature-card');
        const count = await featureCards.count();
        expect(count).toBeGreaterThanOrEqual(3);

        const firstCard = await featureCards.nth(0).boundingBox();
        const secondCard = await featureCards.nth(1).boundingBox();

        if (firstCard && secondCard) {
            // In multi-column layout, first two cards should be on same row
            // Their Y positions should be approximately equal
            expect(Math.abs(firstCard.y - secondCard.y)).toBeLessThan(10);

            // They should have different X positions (different columns)
            expect(Math.abs(firstCard.x - secondCard.x)).toBeGreaterThan(100);
        }
    });
});

test.describe('Responsive Design - Code Blocks', () => {

    // Test Case 8: Code blocks at mobile viewport
    test('TC8: Code blocks have horizontal scroll or wrap appropriately at mobile', async ({ page }) => {
        await page.setViewportSize(VIEWPORTS.mobile);
        await page.goto('/');

        await page.waitForSelector('.code-block');

        // Get code block and its pre element
        const codeBlock = page.locator('.code-block').first();
        const preElement = codeBlock.locator('pre');

        await expect(codeBlock).toBeVisible();

        // Check that the code block has overflow-x: auto or scroll for horizontal scrolling
        const overflowX = await preElement.evaluate(el => {
            const style = window.getComputedStyle(el);
            return style.overflowX;
        });

        // Overflow should allow scrolling
        expect(['auto', 'scroll']).toContain(overflowX);

        // Code block should not overflow its container (visually)
        const codeBlockBox = await codeBlock.boundingBox();
        if (codeBlockBox) {
            // Code block should fit within the viewport
            expect(codeBlockBox.x).toBeGreaterThanOrEqual(0);
            expect(codeBlockBox.x + codeBlockBox.width).toBeLessThanOrEqual(VIEWPORTS.mobile.width + 1);
        }
    });
});

test.describe('Responsive Design - Benchmarks Table', () => {

    // Test Case 9: Benchmarks table at mobile viewport
    test('TC9: Table is scrollable or adapts to narrow viewport at mobile', async ({ page }) => {
        await page.setViewportSize(VIEWPORTS.mobile);
        await page.goto('/');

        await page.waitForSelector('.benchmarks-table-wrapper');

        const tableWrapper = page.locator('.benchmarks-table-wrapper');
        await expect(tableWrapper).toBeVisible();

        // Check that the table wrapper has overflow-x for scrolling
        const overflowX = await tableWrapper.evaluate(el => {
            const style = window.getComputedStyle(el);
            return style.overflowX;
        });

        // Table wrapper should allow horizontal scrolling
        expect(['auto', 'scroll']).toContain(overflowX);

        // Table wrapper should not exceed viewport width
        const wrapperBox = await tableWrapper.boundingBox();
        if (wrapperBox) {
            expect(wrapperBox.x).toBeGreaterThanOrEqual(0);
            expect(wrapperBox.x + wrapperBox.width).toBeLessThanOrEqual(VIEWPORTS.mobile.width + 1);
        }
    });
});

test.describe('Responsive Design - Touch Targets', () => {

    // Test Case 10: Touch targets at mobile viewport
    test('TC10: All interactive elements have minimum 44x44px touch target at mobile', async ({ page }) => {
        await page.setViewportSize(VIEWPORTS.mobile);
        await page.goto('/');

        await page.waitForSelector('.hero');

        // Test CTA buttons
        const primaryBtn = page.locator('.hero .btn-primary');
        await expect(primaryBtn).toBeVisible();

        const primaryBtnBox = await primaryBtn.boundingBox();
        if (primaryBtnBox) {
            expect(primaryBtnBox.height).toBeGreaterThanOrEqual(44);
            expect(primaryBtnBox.width).toBeGreaterThanOrEqual(44);
        }

        const secondaryBtn = page.locator('.hero .btn-secondary');
        await expect(secondaryBtn).toBeVisible();

        const secondaryBtnBox = await secondaryBtn.boundingBox();
        if (secondaryBtnBox) {
            expect(secondaryBtnBox.height).toBeGreaterThanOrEqual(44);
            expect(secondaryBtnBox.width).toBeGreaterThanOrEqual(44);
        }

        // Test copy buttons (should have adequate touch target)
        const copyBtn = page.locator('.copy-btn').first();
        await expect(copyBtn).toBeVisible();

        const copyBtnBox = await copyBtn.boundingBox();
        if (copyBtnBox) {
            expect(copyBtnBox.height).toBeGreaterThanOrEqual(44);
            expect(copyBtnBox.width).toBeGreaterThanOrEqual(44);
        }

        // Test skip link (when focused, it should be accessible)
        const skipLink = page.locator('.skip-link');
        // Skip link is visually hidden until focused, so we check it exists
        await expect(skipLink).toBeAttached();
    });
});

test.describe('Responsive Design - Additional Breakpoint Tests', () => {

    // Test at 480px (common mobile landscape)
    test('Page renders correctly at 480px (mobile landscape)', async ({ page }) => {
        await page.setViewportSize({ width: 480, height: 320 });
        await page.goto('/');

        await page.waitForSelector('.hero');

        // Verify no horizontal scroll
        const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
        const viewportWidth = await page.evaluate(() => window.innerWidth);
        expect(bodyWidth).toBeLessThanOrEqual(viewportWidth + 1);

        // All sections visible
        await expect(page.locator('.hero')).toBeVisible();
        await expect(page.locator('.features')).toBeVisible();
    });

    // Test at 1024px (tablet landscape / small desktop)
    test('Page renders correctly at 1024px (tablet landscape)', async ({ page }) => {
        await page.setViewportSize({ width: 1024, height: 768 });
        await page.goto('/');

        await page.waitForSelector('.hero');

        // Navigation should be visible at this size
        const navLinks = page.locator('.nav-links');
        await expect(navLinks).toBeVisible();

        // Features should be in multi-column layout
        const featureCards = page.locator('.feature-card');
        const firstCard = await featureCards.nth(0).boundingBox();
        const secondCard = await featureCards.nth(1).boundingBox();

        if (firstCard && secondCard) {
            // Cards should be on same row
            expect(Math.abs(firstCard.y - secondCard.y)).toBeLessThan(10);
        }
    });

    // Test at 1280px (common laptop)
    test('Page renders correctly at 1280px (laptop)', async ({ page }) => {
        await page.setViewportSize({ width: 1280, height: 800 });
        await page.goto('/');

        await page.waitForSelector('.hero');

        await expect(page.locator('.hero')).toBeVisible();
        await expect(page.locator('.nav-links')).toBeVisible();

        // Content should be centered
        const container = page.locator('.features .container');
        const containerBox = await container.boundingBox();

        if (containerBox) {
            const leftMargin = containerBox.x;
            const rightMargin = 1280 - (containerBox.x + containerBox.width);
            expect(Math.abs(leftMargin - rightMargin)).toBeLessThan(50);
        }
    });
});

test.describe('Responsive Design - Typography Scaling', () => {

    test('Hero heading scales appropriately between mobile and desktop', async ({ page }) => {
        // Mobile
        await page.setViewportSize(VIEWPORTS.mobile);
        await page.goto('/');
        await page.waitForSelector('.hero h1');

        const mobileFontSize = await page.locator('.hero h1').evaluate(el => {
            return parseFloat(window.getComputedStyle(el).fontSize);
        });

        // Desktop
        await page.setViewportSize(VIEWPORTS.desktop);
        await page.waitForTimeout(100); // Allow styles to update

        const desktopFontSize = await page.locator('.hero h1').evaluate(el => {
            return parseFloat(window.getComputedStyle(el).fontSize);
        });

        // Desktop font should be larger than mobile
        expect(desktopFontSize).toBeGreaterThan(mobileFontSize);

        // Mobile font should still be readable (at least 24px for h1)
        expect(mobileFontSize).toBeGreaterThanOrEqual(24);
    });
});
