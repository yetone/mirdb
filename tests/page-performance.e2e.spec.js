/**
 * Page Performance E2E Tests
 * Verifies the homepage loads within acceptable performance thresholds (NFR-1)
 *
 * Test Cases:
 * 1. Page fully loads within 3 seconds on standard connection
 * 2. All image elements load successfully without 404 errors
 * 3. No JavaScript console errors on page load
 * 4. Stylesheets load without errors and styles are applied
 */

const { test, expect } = require('@playwright/test');
const path = require('path');

test.describe('Page Performance - NFR-1 Compliance', () => {
    const indexPath = path.resolve(__dirname, '../index.html');
    const pageUrl = `file://${indexPath}`;

    test('should load page fully within 3 seconds on standard connection', async ({ page }) => {
        // Set up performance measurement
        const startTime = Date.now();

        // Navigate to the page and wait for load event
        await page.goto(pageUrl, { waitUntil: 'load' });

        // Measure the time taken
        const loadTime = Date.now() - startTime;

        // Verify load time is under 3 seconds (3000ms)
        expect(loadTime).toBeLessThan(3000);

        // Additional verification: page should be fully interactive
        await expect(page.locator('body')).toBeVisible();
        await expect(page.locator('.hero')).toBeVisible();
        await expect(page.locator('footer')).toBeVisible();

        // Log the actual load time for reference
        console.log(`Page load time: ${loadTime}ms`);
    });

    test('should load all image elements successfully without 404 errors', async ({ page }) => {
        // Track failed resource requests for LOCAL images only
        const failedLocalResources = [];
        const localImageLoadErrors = [];

        // Listen for failed requests (local images only)
        page.on('requestfailed', (request) => {
            const url = request.url();
            // Only track local image failures (file:// or relative paths)
            if (request.resourceType() === 'image' && url.startsWith('file://')) {
                failedLocalResources.push({
                    url: url,
                    error: request.failure()?.errorText || 'Unknown error'
                });
            }
        });

        // Listen for response errors (local images only)
        page.on('response', (response) => {
            const url = response.url();
            if (response.request().resourceType() === 'image' && url.startsWith('file://')) {
                const status = response.status();
                if (status >= 400) {
                    localImageLoadErrors.push({
                        url: url,
                        status: status
                    });
                }
            }
        });

        // Navigate to the page
        await page.goto(pageUrl, { waitUntil: 'load' });

        // Get all image elements
        const images = page.locator('img');
        const imageCount = await images.count();

        // Verify images exist
        expect(imageCount).toBeGreaterThan(0);

        // Check each LOCAL image has loaded (naturalWidth > 0 indicates successful load)
        let localImageCount = 0;
        for (let i = 0; i < imageCount; i++) {
            const img = images.nth(i);
            const src = await img.getAttribute('src');

            // For local images (not external URLs), check they're visible and loaded
            if (src && !src.startsWith('http')) {
                localImageCount++;
                await expect(img).toBeVisible();

                // Verify image has loaded by checking naturalWidth
                const naturalWidth = await img.evaluate((el) => el.naturalWidth);
                expect(naturalWidth, `Image ${src} should have loaded`).toBeGreaterThan(0);
            }
        }

        // Verify we have local images to test
        expect(localImageCount, 'Should have local images to test').toBeGreaterThan(0);

        // Assert no failed local image requests
        expect(failedLocalResources, 'No local images should fail to load').toEqual([]);
        expect(localImageLoadErrors, 'No local images should return 404 or other error status').toEqual([]);
    });

    test('should have no JavaScript console errors on page load', async ({ page }) => {
        // Collect console errors (excluding external resource failures)
        const consoleErrors = [];

        page.on('console', (msg) => {
            if (msg.type() === 'error') {
                const text = msg.text();
                const location = msg.location();

                // Filter out external resource failures (network errors for external URLs)
                // These are not JavaScript errors in our code
                const isExternalResourceError =
                    text.includes('net::ERR_') ||
                    (location?.url && !location.url.startsWith('file://'));

                if (!isExternalResourceError) {
                    consoleErrors.push({
                        text: text,
                        location: location
                    });
                }
            }
        });

        // Listen for page errors (uncaught exceptions in our JavaScript code)
        const pageErrors = [];
        page.on('pageerror', (error) => {
            pageErrors.push({
                message: error.message,
                stack: error.stack
            });
        });

        // Navigate to the page
        await page.goto(pageUrl, { waitUntil: 'load' });

        // Wait a moment for any async scripts to execute
        await page.waitForTimeout(500);

        // Assert no JavaScript console errors (excluding network errors for external resources)
        expect(consoleErrors, 'No JavaScript console errors should be present').toEqual([]);

        // Assert no page errors (uncaught exceptions)
        expect(pageErrors, 'No uncaught JavaScript exceptions should occur').toEqual([]);
    });

    test('should load stylesheets without errors and apply styles correctly', async ({ page }) => {
        // Track stylesheet loading
        const cssLoadErrors = [];
        const cssRequests = [];

        page.on('requestfailed', (request) => {
            if (request.resourceType() === 'stylesheet') {
                cssLoadErrors.push({
                    url: request.url(),
                    error: request.failure()?.errorText || 'Unknown error'
                });
            }
        });

        page.on('response', (response) => {
            if (response.request().resourceType() === 'stylesheet') {
                cssRequests.push({
                    url: response.url(),
                    status: response.status()
                });
                if (response.status() >= 400) {
                    cssLoadErrors.push({
                        url: response.url(),
                        status: response.status()
                    });
                }
            }
        });

        // Navigate to the page
        await page.goto(pageUrl, { waitUntil: 'load' });

        // Assert no CSS load errors
        expect(cssLoadErrors, 'No stylesheets should fail to load').toEqual([]);

        // Verify styles are applied by checking computed styles on key elements
        const heroSection = page.locator('.hero');
        await expect(heroSection).toBeVisible();

        // Check hero section has styling applied (background gradient or color)
        const heroStyles = await heroSection.evaluate((el) => {
            const styles = window.getComputedStyle(el);
            return {
                backgroundImage: styles.backgroundImage,
                backgroundColor: styles.backgroundColor,
                display: styles.display,
                paddingTop: styles.paddingTop,
                color: styles.color
            };
        });

        // Verify hero has a styled background (either gradient or solid color)
        // CSS uses: background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
        const hasGradient = heroStyles.backgroundImage && heroStyles.backgroundImage !== 'none';
        const hasBackgroundColor = heroStyles.backgroundColor && heroStyles.backgroundColor !== 'rgba(0, 0, 0, 0)';
        expect(hasGradient || hasBackgroundColor, 'Hero should have styled background').toBe(true);

        // Verify CSS link element exists and is properly loaded
        const cssLink = page.locator('link[rel="stylesheet"]');
        await expect(cssLink).toHaveCount(1);

        // Verify CSS file href is correct
        const href = await cssLink.getAttribute('href');
        expect(href).toBe('styles.css');

        // Verify feature cards have grid styling
        const featureGrid = page.locator('.feature-grid');
        const gridDisplay = await featureGrid.evaluate((el) => {
            return window.getComputedStyle(el).display;
        });
        expect(gridDisplay).toBe('grid');

        // Verify buttons have styled appearance
        const primaryBtn = page.locator('.btn-primary').first();
        await expect(primaryBtn).toBeVisible();
        const btnStyles = await primaryBtn.evaluate((el) => {
            const styles = window.getComputedStyle(el);
            return {
                backgroundColor: styles.backgroundColor,
                cursor: styles.cursor
            };
        });
        expect(btnStyles.cursor).toBe('pointer');
    });
});
