/**
 * Feature Highlights Section E2E Tests
 * Playwright tests for verifying the three-column layout on desktop viewport (REQ-2)
 */

const { test, expect } = require('@playwright/test');
const path = require('path');

test.describe('Feature Highlights Section - Layout', () => {
    test.beforeEach(async ({ page }) => {
        const filePath = path.resolve(__dirname, '../index.html');
        await page.goto(`file://${filePath}`);
    });

    test('should display feature cards in three-column layout on desktop viewport', async ({ page }) => {
        // Set desktop viewport (1280x720)
        await page.setViewportSize({ width: 1280, height: 720 });

        // Get the features section
        const featuresSection = page.locator('.features');
        await expect(featuresSection).toBeVisible();

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

        // All cards should have roughly the same top position (within 10px tolerance)
        // This confirms they are on the same row (horizontal layout)
        const tolerance = 10;
        expect(Math.abs(box1.y - box2.y)).toBeLessThan(tolerance);
        expect(Math.abs(box2.y - box3.y)).toBeLessThan(tolerance);

        // Cards should be arranged horizontally (each card's left is to the right of the previous)
        expect(box2.x).toBeGreaterThan(box1.x);
        expect(box3.x).toBeGreaterThan(box2.x);

        // Verify the CSS grid is being used
        const gridStyle = await featureGrid.evaluate((el) => {
            const computedStyle = window.getComputedStyle(el);
            return {
                display: computedStyle.display,
                gridTemplateColumns: computedStyle.gridTemplateColumns
            };
        });

        expect(gridStyle.display).toBe('grid');

        // Grid template columns should show 3 columns (computed pixel values)
        const columns = gridStyle.gridTemplateColumns.split(' ');
        expect(columns.length).toBe(3);
    });

    test('should stack feature cards vertically on mobile viewport', async ({ page }) => {
        // Set mobile viewport (375x667)
        await page.setViewportSize({ width: 375, height: 667 });

        // Wait a bit for layout to adjust
        await page.waitForTimeout(100);

        // Get all feature cards
        const featureCards = page.locator('.feature-card');
        await expect(featureCards).toHaveCount(3);

        const card1 = featureCards.nth(0);
        const card2 = featureCards.nth(1);
        const card3 = featureCards.nth(2);

        const box1 = await card1.boundingBox();
        const box2 = await card2.boundingBox();
        const box3 = await card3.boundingBox();

        // On mobile, cards should be stacked vertically (each card below the previous)
        // Due to minmax(300px, 1fr) and 375px viewport, cards should stack to single column
        expect(box2.y).toBeGreaterThan(box1.y + box1.height - 10);
        expect(box3.y).toBeGreaterThan(box2.y + box2.height - 10);
    });
});
