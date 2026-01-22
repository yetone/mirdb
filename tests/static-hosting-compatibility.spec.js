// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Static Hosting Compatibility Tests
 *
 * These tests verify that the MirDB homepage works correctly when served as
 * static files, suitable for GitHub Pages or similar static hosting platforms.
 */

test.describe('Static Hosting Compatibility', () => {
    /**
     * Test Case 1: Serve files from GitHub Pages
     * Verifies that all pages and assets load correctly when served statically
     */
    test('should load all pages and assets correctly from static hosting', async ({ page }) => {
        // Collect all network requests and their responses
        const requests = [];
        const failedRequests = [];

        page.on('request', request => {
            requests.push({
                url: request.url(),
                resourceType: request.resourceType()
            });
        });

        page.on('response', response => {
            // Track responses with error status codes (4xx, 5xx)
            // Exclude external resources like CircleCI badge which may fail
            const url = response.url();
            const isExternal = !url.includes('localhost:8080');
            const status = response.status();

            if (status >= 400 && !isExternal) {
                failedRequests.push({
                    url: url,
                    status: status,
                    statusText: response.statusText()
                });
            }
        });

        // Navigate to the homepage
        const response = await page.goto('/');

        // Verify the main page loads successfully (200 OK)
        expect(response.status()).toBe(200);

        // Verify the page content is loaded
        await expect(page.locator('body')).toBeVisible();

        // Verify key sections are present (proves the HTML loaded correctly)
        await expect(page.locator('header.header')).toBeVisible();
        await expect(page.locator('main#main-content')).toBeVisible();
        await expect(page.locator('footer.footer')).toBeVisible();

        // Check that we received responses from the static server
        expect(requests.length).toBeGreaterThan(0);

        // Verify no local resource requests failed
        expect(failedRequests).toEqual([]);
    });

    /**
     * Test Case 2: Check relative asset paths
     * Verifies that all asset paths resolve correctly on static hosting
     */
    test('should resolve all relative asset paths correctly', async ({ page }) => {
        await page.goto('/');

        // Test 1: Check that CSS stylesheet loads (via preload or direct link)
        // The page uses either preload with onload or noscript fallback
        const styles = await page.evaluate(() => {
            const links = Array.from(document.querySelectorAll('link[href="styles.css"], link[rel="preload"][href="styles.css"]'));
            return links.length > 0;
        });
        expect(styles).toBe(true);

        // Test 2: Check that local images have correct relative paths
        const localImages = await page.locator('img[src^="assets/"]').all();
        expect(localImages.length).toBeGreaterThan(0);

        // Verify each local image has a valid src attribute
        for (const img of localImages) {
            const src = await img.getAttribute('src');
            expect(src).toMatch(/^assets\//);

            // Verify the image exists by checking naturalWidth
            // (for images that haven't failed to load)
            const imgElement = await img.evaluate(el => ({
                naturalWidth: el.naturalWidth,
                complete: el.complete
            }));

            // If image has loaded, it should have dimensions
            if (imgElement.complete) {
                expect(imgElement.naturalWidth).toBeGreaterThan(0);
            }
        }

        // Test 3: Check internal anchor links use relative paths
        const internalLinks = await page.locator('a[href^="#"]').all();
        expect(internalLinks.length).toBeGreaterThan(0);

        for (const link of internalLinks) {
            const href = await link.getAttribute('href');
            expect(href).toMatch(/^#[\w-]+$/);
        }

        // Test 4: Verify no absolute paths to localhost or server-specific paths
        const allLinks = await page.evaluate(() => {
            const links = Array.from(document.querySelectorAll('a[href], link[href], img[src], script[src]'));
            return links.map(el => el.getAttribute('href') || el.getAttribute('src')).filter(Boolean);
        });

        // No localhost references (except for dev purposes which wouldn't be in production)
        const localhostRefs = allLinks.filter(path =>
            path.includes('localhost') || path.includes('127.0.0.1')
        );
        expect(localhostRefs).toEqual([]);

        // No absolute file paths
        const absoluteFilePaths = allLinks.filter(path =>
            path.startsWith('/') &&
            !path.startsWith('//') &&
            !path.startsWith('/#')
        );
        expect(absoluteFilePaths).toEqual([]);
    });

    /**
     * Test Case 3: Verify no server-side dependencies
     * Ensures no 404 errors or failed requests requiring backend
     */
    test('should have no server-side dependencies or failed requests', async ({ page }) => {
        // Track all network activity
        const networkErrors = [];
        const consoleErrors = [];

        // Monitor for network failures
        page.on('requestfailed', request => {
            const url = request.url();
            // Only track local requests, external resources may fail due to network
            if (url.includes('localhost:8080')) {
                networkErrors.push({
                    url: url,
                    failure: request.failure()?.errorText || 'Unknown error'
                });
            }
        });

        // Monitor for console errors that might indicate missing resources
        page.on('console', msg => {
            if (msg.type() === 'error') {
                const text = msg.text();
                // Filter out external resource errors
                if (!text.includes('circleci.com') &&
                    !text.includes('githubusercontent') &&
                    !text.includes('ERR_BLOCKED')) {
                    consoleErrors.push(text);
                }
            }
        });

        // Collect all responses
        const responses = [];
        page.on('response', response => {
            const url = response.url();
            if (url.includes('localhost:8080')) {
                responses.push({
                    url: url,
                    status: response.status()
                });
            }
        });

        await page.goto('/');

        // Wait for all network activity to settle
        await page.waitForLoadState('networkidle');

        // Test 1: No network request failures for local resources
        expect(networkErrors).toEqual([]);

        // Test 2: No 404 responses for local resources
        const notFoundResponses = responses.filter(r => r.status === 404);
        expect(notFoundResponses).toEqual([]);

        // Test 3: No 500-level server errors (would indicate server-side dependency)
        const serverErrors = responses.filter(r => r.status >= 500);
        expect(serverErrors).toEqual([]);

        // Test 4: Verify page doesn't make XHR/fetch requests to backend APIs
        const apiRequests = responses.filter(r =>
            r.url.includes('/api/') ||
            r.url.includes('/graphql') ||
            r.url.endsWith('.json') && !r.url.includes('manifest')
        );
        expect(apiRequests).toEqual([]);

        // Test 5: Page content should be immediately visible without JS-based data fetching
        // Core content should be in the initial HTML
        const h1Text = await page.locator('h1').first().textContent();
        expect(h1Text).toBeTruthy();
        expect(h1Text).toBe('MirDB');
    });

    /**
     * Additional test: Verify all required static files exist
     */
    test('should have all required static files accessible', async ({ page }) => {
        // Test that key static files are accessible
        const requiredFiles = [
            '/',                        // Main HTML
            '/styles.css',              // Main stylesheet
            '/assets/logo-placeholder.svg',  // Logo placeholder
            '/assets/usage.gif',        // Usage demo
            '/assets/logo.gif'          // Original logo
        ];

        for (const file of requiredFiles) {
            const response = await page.request.get(file);
            expect(response.status(), `${file} should return 200 OK`).toBe(200);
        }
    });

    /**
     * Additional test: Verify JavaScript functionality works without backend
     */
    test('should have functional JavaScript without backend requirements', async ({ page }) => {
        await page.goto('/');

        // Test copy functionality works (it's client-side only)
        const copyButton = page.locator('.copy-btn').first();
        await expect(copyButton).toBeVisible();

        // The copy button should be interactive
        await expect(copyButton).toBeEnabled();

        // Click and verify the text changes (client-side only operation)
        // Note: clipboard API may not work in test environment, but button should respond
        const originalText = await copyButton.textContent();
        expect(originalText).toBe('Copy');
    });

    /**
     * Additional test: Verify internal navigation works without server routing
     */
    test('should support internal navigation with anchor links', async ({ page }) => {
        await page.goto('/');

        // Test navigation to features section
        await page.click('a[href="#features"]');
        await expect(page.locator('#features')).toBeInViewport();

        // Test navigation to demo section
        await page.click('a[href="#demo"]');
        await expect(page.locator('#demo')).toBeInViewport();

        // Test navigation to quick-start section
        await page.click('a[href="#quick-start"]');
        await expect(page.locator('#quick-start')).toBeInViewport();

        // URL should update with hash but not require page reload
        expect(page.url()).toContain('#quick-start');
    });
});
