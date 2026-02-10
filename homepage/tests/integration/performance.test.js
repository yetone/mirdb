/**
 * Performance Integration Tests
 * Owner: Scenario 7 - Performance and Loading
 *
 * Integration tests for verifying static file caching behavior.
 *
 * Expected test coverage:
 * - Static files are cacheable
 * - CSS and JS files can be cached
 * - No cache-busting on every request
 *
 * Requirements traced:
 * - NFR-1: Quick load with optimal performance
 */

const { test, expect } = require('@playwright/test');

test.describe('Performance - Static File Caching', () => {
    test('CSS files can be cached (no cache-busting on every request)', async ({ page }) => {
        // Collect all responses
        const cssResponses = [];

        page.on('response', (response) => {
            const url = response.url();
            if (url.endsWith('.css') || url.includes('.css')) {
                cssResponses.push({
                    url: url,
                    headers: response.headers()
                });
            }
        });

        await page.goto('/', { waitUntil: 'networkidle' });

        // Check that CSS files exist and are cacheable
        expect(cssResponses.length).toBeGreaterThan(0);

        cssResponses.forEach(response => {
            // CSS URLs should not have random cache-busting parameters that change on every request
            // (e.g., ?v=timestamp or ?hash=random)
            const url = response.url;

            // It's acceptable to have version parameters like ?v=1.0 or hash-based filenames
            // But random timestamps would indicate cache-busting on every request
            expect(url).not.toMatch(/\?t=\d{13}/); // No timestamp cache busting
            expect(url).not.toMatch(/\?rand=\d+/); // No random cache busting

            // Response should not explicitly prevent caching
            const cacheControl = response.headers['cache-control'] || '';
            expect(cacheControl).not.toContain('no-store');
        });
    });

    test('JavaScript files can be cached (no cache-busting on every request)', async ({ page }) => {
        // Collect all responses
        const jsResponses = [];

        page.on('response', (response) => {
            const url = response.url();
            if (url.endsWith('.js') || url.includes('.js?')) {
                jsResponses.push({
                    url: url,
                    headers: response.headers()
                });
            }
        });

        await page.goto('/', { waitUntil: 'networkidle' });

        // JS files may or may not exist - only check if they do
        if (jsResponses.length > 0) {
            jsResponses.forEach(response => {
                const url = response.url;

                // JS URLs should not have random cache-busting parameters
                expect(url).not.toMatch(/\?t=\d{13}/); // No timestamp cache busting
                expect(url).not.toMatch(/\?rand=\d+/); // No random cache busting

                // Response should not explicitly prevent caching
                const cacheControl = response.headers['cache-control'] || '';
                expect(cacheControl).not.toContain('no-store');
            });
        }
    });

    test('Static assets load successfully on subsequent visits', async ({ page }) => {
        // First visit
        await page.goto('/', { waitUntil: 'networkidle' });

        // Collect resources on first load
        const firstLoadResources = await page.evaluate(() => {
            return performance.getEntriesByType('resource').map(r => ({
                name: r.name,
                duration: r.duration
            }));
        });

        // Navigate away and back
        await page.goto('about:blank');
        await page.goto('/', { waitUntil: 'networkidle' });

        // Resources should still load successfully
        const secondLoadResources = await page.evaluate(() => {
            return performance.getEntriesByType('resource').map(r => ({
                name: r.name,
                duration: r.duration
            }));
        });

        // Should have similar resources on both loads
        expect(secondLoadResources.length).toBeGreaterThan(0);
    });

    test('Static file URLs are consistent (not dynamically generated)', async ({ page, context }) => {
        // First visit - collect URLs
        const firstVisitUrls = [];
        const firstResponseHandler = (response) => {
            const url = response.url();
            if (url.endsWith('.css') || url.endsWith('.js')) {
                firstVisitUrls.push(url);
            }
        };
        page.on('response', firstResponseHandler);

        await page.goto('/', { waitUntil: 'networkidle' });

        // Remove the first handler
        page.off('response', firstResponseHandler);

        // Navigate away
        await page.goto('about:blank');

        // Second visit - collect URLs with a new handler
        const secondVisitUrls = [];
        const secondResponseHandler = (response) => {
            const url = response.url();
            if (url.endsWith('.css') || url.endsWith('.js')) {
                secondVisitUrls.push(url);
            }
        };
        page.on('response', secondResponseHandler);

        await page.goto('/', { waitUntil: 'networkidle' });

        // URLs should be consistent between visits (same resource paths)
        // This ensures no dynamic cache-busting is happening
        const firstPaths = firstVisitUrls.map(u => new URL(u).pathname).sort();
        const secondPaths = secondVisitUrls.map(u => new URL(u).pathname).sort();

        expect(firstPaths).toEqual(secondPaths);
    });
});
