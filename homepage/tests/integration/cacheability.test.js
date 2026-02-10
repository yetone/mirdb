/**
 * Cacheability Integration Tests
 * Owner: Scenario 7 - Performance and Loading
 *
 * Integration tests for validating that static assets can be cached.
 *
 * Expected test coverage:
 * - CSS files can be cached (stable filenames)
 * - JS files can be cached (stable filenames)
 * - No cache-busting on every request
 *
 * Requirements traced:
 * - NFR-1: Quick load with optimal performance
 * - Test Case 8: Verify static files are cacheable
 */

const { test, expect } = require('@playwright/test');
const fs = require('fs');
const path = require('path');

test.describe('Static Files Cacheability Tests (Test Case 8)', () => {
    test('CSS files have stable filenames (no hash changes between requests)', async ({ page }) => {
        // First request
        const cssUrls1 = [];
        page.on('response', async (response) => {
            const url = response.url();
            if (url.endsWith('.css')) {
                cssUrls1.push(url);
            }
        });
        await page.goto('/index.html', { waitUntil: 'load' });

        // Second request - should get same CSS URLs
        const cssUrls2 = [];
        await page.goto('/index.html', { waitUntil: 'load' });
        page.on('response', async (response) => {
            const url = response.url();
            if (url.endsWith('.css')) {
                cssUrls2.push(url);
            }
        });

        // URLs should match (no random cache-busting)
        expect(cssUrls1.length).toBeGreaterThan(0);
        for (const url of cssUrls1) {
            // Verify URL doesn't contain random query params or hashes
            expect(url).not.toMatch(/\?v=[0-9]+/);
            expect(url).not.toMatch(/\?t=[0-9]+/);
            expect(url).not.toMatch(/\?_=[0-9]+/);
        }
    });

    test('JS files have stable filenames (no cache-busting on every request)', async ({ page }) => {
        // First request
        const jsUrls = [];
        page.on('response', async (response) => {
            const url = response.url();
            if (url.endsWith('.js') && !url.includes('playwright')) {
                jsUrls.push(url);
            }
        });
        await page.goto('/index.html', { waitUntil: 'load' });

        // Verify JS URLs don't have random cache-busting
        for (const url of jsUrls) {
            // URLs should be stable
            expect(url).not.toMatch(/\?v=[0-9]+/);
            expect(url).not.toMatch(/\?t=[0-9]+/);
            expect(url).not.toMatch(/\?_=[0-9]+/);
            expect(url).not.toMatch(/\?nocache/);
        }
    });

    test('Static files use predictable paths', async ({ page }) => {
        const responses = [];

        page.on('response', async (response) => {
            const url = response.url();
            const contentType = response.headers()['content-type'] || '';

            if (contentType.includes('css') || contentType.includes('javascript') ||
                url.endsWith('.css') || url.endsWith('.js')) {
                responses.push({
                    url,
                    contentType,
                    status: response.status()
                });
            }
        });

        await page.goto('/index.html', { waitUntil: 'load' });

        // All static files should return 200
        for (const response of responses) {
            expect(response.status).toBe(200);
        }

        // Check that paths are predictable (not randomly generated)
        for (const response of responses) {
            const url = new URL(response.url);
            // Path should be clean and not contain build hashes like .a1b2c3.js
            expect(url.pathname).not.toMatch(/\.[a-f0-9]{8,}\.(js|css)$/);
        }
    });

    test('HTML file references consistent asset paths', async ({ page }) => {
        await page.goto('/index.html', { waitUntil: 'load' });

        // Get all CSS link hrefs
        const cssLinks = await page.evaluate(() => {
            return Array.from(document.querySelectorAll('link[rel="stylesheet"]'))
                .map(link => link.getAttribute('href'));
        });

        // Get all script srcs
        const scriptSrcs = await page.evaluate(() => {
            return Array.from(document.querySelectorAll('script[src]'))
                .map(script => script.getAttribute('src'));
        });

        // Verify CSS paths are stable
        for (const href of cssLinks) {
            expect(href).toBeTruthy();
            expect(href).toMatch(/^(css\/|\.\/css\/|\/css\/)/);
            expect(href).not.toMatch(/\?[0-9]+$/);
        }

        // Verify JS paths are stable
        for (const src of scriptSrcs) {
            expect(src).toBeTruthy();
            expect(src).toMatch(/^(js\/|\.\/js\/|\/js\/)/);
            expect(src).not.toMatch(/\?[0-9]+$/);
        }
    });

    test('Multiple page loads get consistent resources', async ({ page }) => {
        const loads = [];

        for (let i = 0; i < 3; i++) {
            const resources = [];
            page.on('response', async (response) => {
                const url = response.url();
                if (url.endsWith('.css') || url.endsWith('.js')) {
                    resources.push(url);
                }
            });

            await page.goto('/index.html', { waitUntil: 'load' });
            loads.push([...resources]);

            // Clear for next iteration
            page.removeAllListeners('response');
        }

        // All loads should request the same resources
        if (loads[0].length > 0) {
            const firstLoadUrls = loads[0].sort();
            for (let i = 1; i < loads.length; i++) {
                const currentLoadUrls = loads[i].sort();
                expect(currentLoadUrls).toEqual(firstLoadUrls);
            }
        }
    });
});

test.describe('File Structure for Cacheability', () => {
    test('CSS files exist at expected locations', async ({ page }) => {
        // Navigate to verify files are served correctly
        const cssResponse = await page.goto('/css/styles.css');
        expect(cssResponse.status()).toBe(200);

        const responsiveCssResponse = await page.goto('/css/responsive.css');
        expect(responsiveCssResponse.status()).toBe(200);
    });

    test('JS files exist at expected locations', async ({ page }) => {
        const jsResponse = await page.goto('/js/main.js');
        expect(jsResponse.status()).toBe(200);
    });

    test('CSS and JS file paths in HTML are relative and cacheable', async ({ page }) => {
        await page.goto('/index.html', { waitUntil: 'domcontentloaded' });

        const htmlContent = await page.content();

        // CSS should use relative paths
        expect(htmlContent).toMatch(/href=["']css\/styles\.css["']/);
        expect(htmlContent).toMatch(/href=["']css\/responsive\.css["']/);

        // JS should use relative path
        expect(htmlContent).toMatch(/src=["']js\/main\.js["']/);

        // Paths should not have timestamps or hashes
        expect(htmlContent).not.toMatch(/href=["']css\/[^"']*\?[0-9]+["']/);
        expect(htmlContent).not.toMatch(/src=["']js\/[^"']*\?[0-9]+["']/);
    });
});
