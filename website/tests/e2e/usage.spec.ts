/**
 * Usage Examples Section E2E Tests
 * Owner: Scenario 4 - Usage Examples Section
 *
 * Tests for usage examples section including usage.gif display, alt text,
 * caption, and proper image loading.
 */

import { test, expect } from '@playwright/test';

test.describe('Usage Examples Section', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
    });

    test('TC1: Section with heading containing Usage or Example exists', async ({ page }) => {
        // Navigate to usage section and check for heading
        const usageSection = page.locator('#usage');
        await expect(usageSection).toBeVisible();

        // Check for h2 heading containing 'Usage' or 'Example'
        const heading = usageSection.locator('h2');
        await expect(heading).toBeVisible();

        const headingText = await heading.textContent();
        expect(headingText).toBeTruthy();
        const lowerHeading = headingText!.toLowerCase();
        const hasUsageOrExample = lowerHeading.includes('usage') || lowerHeading.includes('example');
        expect(hasUsageOrExample).toBe(true);
    });

    test('TC2: Image element with src containing usage.gif is present and visible', async ({ page }) => {
        // Query for usage.gif image
        const usageImage = page.locator('#usage img[src*="usage.gif"]');

        // Check image is visible
        await expect(usageImage).toBeVisible();

        // Verify src attribute contains usage.gif
        const src = await usageImage.getAttribute('src');
        expect(src).toBeTruthy();
        expect(src).toContain('usage.gif');
    });

    test('TC3: Alt text is non-empty and describes the usage demonstration', async ({ page }) => {
        // Check usage.gif alt attribute
        const usageImage = page.locator('#usage img[src*="usage.gif"]');
        await expect(usageImage).toBeVisible();

        // Get alt attribute
        const altText = await usageImage.getAttribute('alt');

        // Alt text should be non-empty
        expect(altText).toBeTruthy();
        expect(altText!.length).toBeGreaterThan(0);

        // Alt text should be descriptive (more than just "image" or similar)
        expect(altText!.length).toBeGreaterThan(10);

        // Alt text should reference the usage or demonstration context
        const lowerAlt = altText!.toLowerCase();
        const isDescriptive = lowerAlt.includes('mirdb') ||
            lowerAlt.includes('usage') ||
            lowerAlt.includes('demonstration') ||
            lowerAlt.includes('example') ||
            lowerAlt.includes('key-value') ||
            lowerAlt.includes('memcached');
        expect(isDescriptive).toBe(true);
    });

    test('TC4: Caption or figcaption element exists near usage image', async ({ page }) => {
        // Query for caption or figcaption element
        const usageSection = page.locator('#usage');
        await expect(usageSection).toBeVisible();

        // Look for figcaption within a figure element
        const figcaption = usageSection.locator('figcaption');
        await expect(figcaption).toBeVisible();

        // Get caption text
        const captionText = await figcaption.textContent();

        // Caption should be non-empty
        expect(captionText).toBeTruthy();
        expect(captionText!.trim().length).toBeGreaterThan(0);

        // Caption should explain the demonstration
        const lowerCaption = captionText!.toLowerCase();
        const isExplanatory = lowerCaption.includes('mirdb') ||
            lowerCaption.includes('memcached') ||
            lowerCaption.includes('key') ||
            lowerCaption.includes('interact') ||
            lowerCaption.includes('client');
        expect(isExplanatory).toBe(true);
    });

    test('TC5: usage.gif loads without 404 error and has valid dimensions', async ({ page }) => {
        // Set up request interception to check for 404
        const failedRequests: string[] = [];
        page.on('response', response => {
            if (response.url().includes('usage.gif') && response.status() >= 400) {
                failedRequests.push(response.url());
            }
        });

        // Reload page to trigger request
        await page.goto('/');

        // Check no 404 errors for usage.gif
        expect(failedRequests.length).toBe(0);

        // Get the image element
        const usageImage = page.locator('#usage img[src*="usage.gif"]');
        await expect(usageImage).toBeVisible();

        // Wait for the image to load completely
        await page.waitForFunction(
            (selector) => {
                const img = document.querySelector(selector) as HTMLImageElement;
                return img && img.complete && img.naturalWidth > 0;
            },
            '#usage img[src*="usage.gif"]'
        );

        // Check image has valid dimensions (naturalWidth and naturalHeight > 0)
        const dimensions = await usageImage.evaluate((img: HTMLImageElement) => ({
            naturalWidth: img.naturalWidth,
            naturalHeight: img.naturalHeight,
            displayWidth: img.offsetWidth,
            displayHeight: img.offsetHeight
        }));

        // Natural dimensions should be greater than 0
        expect(dimensions.naturalWidth).toBeGreaterThan(0);
        expect(dimensions.naturalHeight).toBeGreaterThan(0);

        // Display dimensions should also be greater than 0
        expect(dimensions.displayWidth).toBeGreaterThan(0);
        expect(dimensions.displayHeight).toBeGreaterThan(0);
    });
});
