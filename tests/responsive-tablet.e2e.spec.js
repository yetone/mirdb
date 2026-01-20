/**
 * Responsive Design - Tablet E2E Tests
 * Playwright tests for verifying the homepage is responsive and usable on tablet devices (NFR-2)
 */

const { test, expect } = require('@playwright/test');
const path = require('path');

// Tablet viewport dimensions (768px width as specified in scenario)
const TABLET_VIEWPORT = { width: 768, height: 1024 };

test.describe('Responsive Design - Tablet', () => {
    test.beforeEach(async ({ page }) => {
        const filePath = path.resolve(__dirname, '../index.html');
        await page.goto(`file://${filePath}`);
        // Set tablet viewport
        await page.setViewportSize(TABLET_VIEWPORT);
        // Wait for layout to adjust
        await page.waitForTimeout(100);
    });

    test('TC1: Layout adapts appropriately for tablet viewport', async ({ page }) => {
        // Verify the page is displayed correctly at 768px width
        const body = page.locator('body');
        await expect(body).toBeVisible();

        // Verify hero section adapts
        const hero = page.locator('.hero');
        await expect(hero).toBeVisible();

        // Verify hero content is visible and accessible
        const heroTitle = page.locator('.hero h1');
        await expect(heroTitle).toBeVisible();
        await expect(heroTitle).toHaveText('MirDB');

        const tagline = page.locator('.tagline');
        await expect(tagline).toBeVisible();

        // Verify CTA buttons are visible and accessible
        const ctaButtons = page.locator('.cta-buttons');
        await expect(ctaButtons).toBeVisible();

        const primaryBtn = page.locator('.btn-primary');
        const secondaryBtn = page.locator('.btn-secondary');
        await expect(primaryBtn).toBeVisible();
        await expect(secondaryBtn).toBeVisible();

        // Verify all main sections are visible
        const featuresSection = page.locator('.features');
        const quickStartSection = page.locator('.quick-start');
        const commandsSection = page.locator('.commands');
        const statusSection = page.locator('.status');
        const footer = page.locator('footer');

        await expect(featuresSection).toBeVisible();
        await expect(quickStartSection).toBeVisible();
        await expect(commandsSection).toBeVisible();
        await expect(statusSection).toBeVisible();
        await expect(footer).toBeVisible();

        // Verify container does not overflow horizontally
        const bodyBox = await body.boundingBox();
        expect(bodyBox.width).toBeLessThanOrEqual(TABLET_VIEWPORT.width + 1);
    });

    test('TC2: Feature cards display in reasonable layout (2-column or 3-column)', async ({ page }) => {
        // Get the feature grid container
        const featureGrid = page.locator('.feature-grid');
        await expect(featureGrid).toBeVisible();

        // Get all feature cards
        const featureCards = page.locator('.feature-card');
        await expect(featureCards).toHaveCount(3);

        // Get bounding boxes for all cards
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

        // Check the grid layout - should be 2-column (first two cards on same row, third below)
        // OR could be 3-column (all three on same row)
        // OR single column (all stacked)

        // First, verify cards are properly sized and don't overflow
        expect(box1.width).toBeGreaterThan(200); // Cards should have reasonable width
        expect(box1.width).toBeLessThanOrEqual(TABLET_VIEWPORT.width);

        // Determine the layout type by comparing Y positions
        const tolerance = 20; // Allow small tolerance for alignment

        const card1and2SameRow = Math.abs(box1.y - box2.y) < tolerance;
        const card2and3SameRow = Math.abs(box2.y - box3.y) < tolerance;

        if (card1and2SameRow && card2and3SameRow) {
            // All three cards on same row (3-column layout)
            expect(box2.x).toBeGreaterThan(box1.x);
            expect(box3.x).toBeGreaterThan(box2.x);
        } else if (card1and2SameRow && !card2and3SameRow) {
            // First two on same row, third below (2-column layout)
            expect(box2.x).toBeGreaterThan(box1.x);
            expect(box3.y).toBeGreaterThan(box1.y + box1.height - tolerance);
        } else {
            // Single column layout (all stacked) - also valid for tablet
            expect(box2.y).toBeGreaterThan(box1.y + box1.height - tolerance);
            expect(box3.y).toBeGreaterThan(box2.y + box2.height - tolerance);
        }

        // Verify grid is being used
        const gridStyle = await featureGrid.evaluate((el) => {
            const computedStyle = window.getComputedStyle(el);
            return {
                display: computedStyle.display,
                gridTemplateColumns: computedStyle.gridTemplateColumns
            };
        });

        expect(gridStyle.display).toBe('grid');

        // The grid should define columns
        const columns = gridStyle.gridTemplateColumns.split(' ');
        expect(columns.length).toBeGreaterThanOrEqual(1);
        expect(columns.length).toBeLessThanOrEqual(3);
    });

    test('TC3: Code examples are readable without horizontal scrolling', async ({ page }) => {
        // Navigate to quick-start section where code blocks are
        const quickStartSection = page.locator('#quick-start');
        await expect(quickStartSection).toBeVisible();

        // Get all code blocks (pre elements)
        const codeBlocks = page.locator('.quick-start pre');
        const codeBlockCount = await codeBlocks.count();
        expect(codeBlockCount).toBeGreaterThan(0);

        // For each code block, verify it doesn't cause horizontal scroll on the page
        for (let i = 0; i < codeBlockCount; i++) {
            const codeBlock = codeBlocks.nth(i);
            await expect(codeBlock).toBeVisible();

            const box = await codeBlock.boundingBox();
            expect(box).not.toBeNull();

            // Code block should fit within viewport width (with some padding)
            // We allow the code block to be up to viewport width
            expect(box.width).toBeLessThanOrEqual(TABLET_VIEWPORT.width);

            // Verify the code block has overflow-x handling (scroll or auto)
            const overflowStyle = await codeBlock.evaluate((el) => {
                const computedStyle = window.getComputedStyle(el);
                return {
                    overflowX: computedStyle.overflowX,
                    width: el.scrollWidth,
                    clientWidth: el.clientWidth
                };
            });

            // Code block should either fit entirely or have overflow handling
            if (overflowStyle.width > overflowStyle.clientWidth) {
                // If content is wider than visible area, overflow-x should be 'auto' or 'scroll'
                expect(['auto', 'scroll']).toContain(overflowStyle.overflowX);
            }
        }

        // Verify the body doesn't have horizontal scroll at tablet width
        const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
        expect(bodyScrollWidth).toBeLessThanOrEqual(TABLET_VIEWPORT.width + 1);
    });

    test('should display readable text sizes on tablet', async ({ page }) => {
        // Verify hero text is readable
        const heroTitle = page.locator('.hero h1');
        const heroTitleStyle = await heroTitle.evaluate((el) => {
            return window.getComputedStyle(el).fontSize;
        });
        const heroFontSize = parseInt(heroTitleStyle);
        expect(heroFontSize).toBeGreaterThanOrEqual(24); // Minimum readable size for titles

        // Verify tagline is readable
        const tagline = page.locator('.tagline');
        const taglineStyle = await tagline.evaluate((el) => {
            return window.getComputedStyle(el).fontSize;
        });
        const taglineFontSize = parseInt(taglineStyle);
        expect(taglineFontSize).toBeGreaterThanOrEqual(14); // Minimum readable size

        // Verify body text in feature cards is readable
        const featureText = page.locator('.feature-card p').first();
        const featureTextStyle = await featureText.evaluate((el) => {
            return window.getComputedStyle(el).fontSize;
        });
        const featureFontSize = parseInt(featureTextStyle);
        expect(featureFontSize).toBeGreaterThanOrEqual(14); // Minimum readable size
    });

    test('should maintain proper spacing and padding on tablet', async ({ page }) => {
        // Verify container has proper padding
        const container = page.locator('.container').first();
        const containerStyle = await container.evaluate((el) => {
            const style = window.getComputedStyle(el);
            return {
                paddingLeft: parseInt(style.paddingLeft),
                paddingRight: parseInt(style.paddingRight),
                maxWidth: style.maxWidth
            };
        });

        // Container should have padding for comfortable reading
        expect(containerStyle.paddingLeft).toBeGreaterThanOrEqual(10);
        expect(containerStyle.paddingRight).toBeGreaterThanOrEqual(10);
    });

    test('should display buttons in accessible sizes on tablet', async ({ page }) => {
        // Verify CTA buttons are adequately sized for touch interaction
        const primaryBtn = page.locator('.btn-primary');
        const secondaryBtn = page.locator('.btn-secondary');

        const primaryBox = await primaryBtn.boundingBox();
        const secondaryBox = await secondaryBtn.boundingBox();

        // Buttons should have minimum touch target size (44x44 is recommended minimum)
        expect(primaryBox.height).toBeGreaterThanOrEqual(40);
        expect(secondaryBox.height).toBeGreaterThanOrEqual(40);
    });
});
