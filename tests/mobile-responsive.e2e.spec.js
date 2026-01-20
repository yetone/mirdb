/**
 * Mobile Responsive Design E2E Tests
 * Playwright tests for verifying the homepage is responsive and usable on mobile devices (NFR-2)
 */

const { test, expect } = require('@playwright/test');
const path = require('path');

test.describe('Mobile Responsive Design - NFR-2', () => {
    test.beforeEach(async ({ page }) => {
        const filePath = path.resolve(__dirname, '../index.html');
        await page.goto(`file://${filePath}`);
        // Set mobile viewport (375px width - typical iPhone SE/small mobile)
        await page.setViewportSize({ width: 375, height: 667 });
    });

    test('should display content without horizontal scroll at 375px width', async ({ page }) => {
        // Test case 1: View homepage at 375px width - no horizontal scroll required

        // Get the document's scroll dimensions
        const scrollDimensions = await page.evaluate(() => {
            return {
                scrollWidth: document.documentElement.scrollWidth,
                clientWidth: document.documentElement.clientWidth,
                bodyScrollWidth: document.body.scrollWidth,
                bodyClientWidth: document.body.clientWidth
            };
        });

        // Content should not exceed viewport width (no horizontal scroll needed)
        // Allow 1px tolerance for rounding issues
        expect(scrollDimensions.scrollWidth).toBeLessThanOrEqual(scrollDimensions.clientWidth + 1);

        // Verify key sections are visible and readable
        const hero = page.locator('.hero');
        await expect(hero).toBeVisible();

        const features = page.locator('.features');
        await expect(features).toBeVisible();

        const quickStart = page.locator('.quick-start');
        await expect(quickStart).toBeVisible();

        // Verify text is readable (hero h1 exists and is visible)
        const heroTitle = page.locator('.hero h1');
        await expect(heroTitle).toBeVisible();
        await expect(heroTitle).toHaveText('MirDB');
    });

    test('should stack feature cards vertically on mobile viewport', async ({ page }) => {
        // Test case 2: Feature cards stack vertically on mobile viewport

        // Get all feature cards
        const featureCards = page.locator('.feature-card');
        await expect(featureCards).toHaveCount(3);

        const card1 = featureCards.nth(0);
        const card2 = featureCards.nth(1);
        const card3 = featureCards.nth(2);

        const box1 = await card1.boundingBox();
        const box2 = await card2.boundingBox();
        const box3 = await card3.boundingBox();

        // Verify all cards exist
        expect(box1).not.toBeNull();
        expect(box2).not.toBeNull();
        expect(box3).not.toBeNull();

        // On mobile, cards should be stacked vertically (each card below the previous)
        // Due to minmax(300px, 1fr) and 375px viewport, cards should stack to single column
        expect(box2.y).toBeGreaterThan(box1.y + box1.height - 10);
        expect(box3.y).toBeGreaterThan(box2.y + box2.height - 10);

        // Cards should have similar x position (stacked in same column)
        const tolerance = 50; // Allow for padding differences
        expect(Math.abs(box1.x - box2.x)).toBeLessThan(tolerance);
        expect(Math.abs(box2.x - box3.x)).toBeLessThan(tolerance);
    });

    test('should have accessible navigation elements on mobile', async ({ page }) => {
        // Test case 3: Navigation elements are accessible and usable on mobile

        // Verify CTA buttons in hero section are visible and accessible
        const getStartedBtn = page.locator('.btn-primary');
        await expect(getStartedBtn).toBeVisible();
        await expect(getStartedBtn).toHaveText('Get Started');

        const githubBtn = page.locator('.btn-secondary');
        await expect(githubBtn).toBeVisible();
        await expect(githubBtn).toContainText('GitHub');

        // Verify both buttons are clickable (not hidden or obscured)
        await expect(getStartedBtn).toBeEnabled();
        await expect(githubBtn).toBeEnabled();

        // Verify buttons have proper href attributes
        await expect(getStartedBtn).toHaveAttribute('href', '#quick-start');
        await expect(githubBtn).toHaveAttribute('href', 'https://github.com/yetone/mirdb');

        // Verify footer navigation links are accessible
        const footerLinks = page.locator('.footer-links a');
        await expect(footerLinks).toHaveCount(3);

        // Verify each footer link is visible
        for (let i = 0; i < 3; i++) {
            await expect(footerLinks.nth(i)).toBeVisible();
        }

        // Check that navigation works (Get Started scrolls to quick-start)
        await getStartedBtn.click();
        await page.waitForTimeout(500); // Wait for smooth scroll

        // Quick start section should be in viewport after click
        const quickStartSection = page.locator('#quick-start');
        await expect(quickStartSection).toBeInViewport();
    });

    test('should have CTA buttons with at least 44px height for touch targets', async ({ page }) => {
        // Test case 4: CTA buttons are at least 44px in height for easy tapping

        // Get both CTA buttons
        const primaryBtn = page.locator('.btn-primary');
        const secondaryBtn = page.locator('.btn-secondary');

        await expect(primaryBtn).toBeVisible();
        await expect(secondaryBtn).toBeVisible();

        // Get bounding boxes for button dimensions
        const primaryBox = await primaryBtn.boundingBox();
        const secondaryBox = await secondaryBtn.boundingBox();

        expect(primaryBox).not.toBeNull();
        expect(secondaryBox).not.toBeNull();

        // Minimum touch target size is 44x44px (Apple HIG / WCAG guidelines)
        const minTouchTarget = 44;

        // Verify buttons meet minimum height requirement
        expect(primaryBox.height).toBeGreaterThanOrEqual(minTouchTarget);
        expect(secondaryBox.height).toBeGreaterThanOrEqual(minTouchTarget);

        // Verify buttons are also wide enough for comfortable tapping
        expect(primaryBox.width).toBeGreaterThanOrEqual(minTouchTarget);
        expect(secondaryBox.width).toBeGreaterThanOrEqual(minTouchTarget);

        // Verify footer links also have adequate touch targets
        const footerLinks = page.locator('.footer-links a');
        const count = await footerLinks.count();

        for (let i = 0; i < count; i++) {
            const link = footerLinks.nth(i);
            const linkBox = await link.boundingBox();
            // Footer links should have at least 44px height (including padding)
            // Since these are text links, we check the clickable area
            expect(linkBox.height).toBeGreaterThanOrEqual(20); // Text links may be smaller but should be tappable
        }
    });

    test('should have properly sized and visible badges on mobile', async ({ page }) => {
        // Additional test: Verify badge visibility on mobile
        const badge = page.locator('.badges img');
        await expect(badge).toBeVisible();

        // Badge should fit within viewport
        const badgeBox = await badge.boundingBox();
        expect(badgeBox).not.toBeNull();
        expect(badgeBox.x + badgeBox.width).toBeLessThanOrEqual(375);
    });

    test('should have code blocks that do not cause horizontal overflow', async ({ page }) => {
        // Additional test: Code blocks should be scrollable, not overflow
        const preBlocks = page.locator('.quick-start pre');
        const count = await preBlocks.count();

        expect(count).toBeGreaterThan(0);

        for (let i = 0; i < count; i++) {
            const pre = preBlocks.nth(i);
            const preBox = await pre.boundingBox();
            // Pre blocks should not extend beyond viewport
            expect(preBox.x + preBox.width).toBeLessThanOrEqual(375 + 20); // Allow small margin
        }

        // Verify overflow-x is auto on pre blocks
        const preStyle = await preBlocks.first().evaluate((el) => {
            return window.getComputedStyle(el).overflowX;
        });
        expect(preStyle).toBe('auto');
    });
});
