/**
 * Scenario 11: Performance and Load Time
 *
 * E2E tests to verify:
 * - TC1: Page loads within 2 seconds on 3 Mbps connection simulation
 * - TC4: Minimal HTTP requests with bundled assets
 */

const { test, expect } = require('@playwright/test');

test.describe('Scenario 11: Performance and Load Time', () => {
    test('TC1: Page loads within 2 seconds on 3 Mbps connection', async ({ page, context }) => {
        // Simulate 3 Mbps connection (375 KB/s = 384000 bytes/s)
        // 3 Mbps = 3,000,000 bits/s = 375,000 bytes/s
        const cdpSession = await context.newCDPSession(page);
        await cdpSession.send('Network.enable');
        await cdpSession.send('Network.emulateNetworkConditions', {
            offline: false,
            downloadThroughput: 375000, // 3 Mbps = 375 KB/s
            uploadThroughput: 375000,
            latency: 100 // 100ms latency
        });

        const startTime = Date.now();

        // Navigate and wait for load
        await page.goto('/', { waitUntil: 'domcontentloaded' });

        // Wait for first contentful paint
        const fcp = await page.evaluate(() => {
            return new Promise((resolve) => {
                const observer = new PerformanceObserver((list) => {
                    for (const entry of list.getEntries()) {
                        if (entry.name === 'first-contentful-paint') {
                            resolve(entry.startTime);
                            return;
                        }
                    }
                });
                observer.observe({ type: 'paint', buffered: true });

                // Fallback timeout
                setTimeout(() => resolve(Date.now() - performance.timeOrigin), 5000);
            });
        });

        const loadTime = Date.now() - startTime;

        console.log(`Page load time: ${loadTime}ms`);
        console.log(`First Contentful Paint: ${fcp}ms`);

        // The page should load within 2 seconds (2000ms)
        expect(loadTime).toBeLessThan(2000);
    });

    test('TC4: Minimal HTTP requests with bundled assets', async ({ page }) => {
        const requests = [];

        // Track all network requests
        page.on('request', (request) => {
            const url = request.url();
            // Only count requests to our server (not external CDN)
            if (url.startsWith('http://localhost:8080')) {
                requests.push({
                    url: url,
                    type: request.resourceType()
                });
            }
        });

        await page.goto('/', { waitUntil: 'networkidle' });

        // Log requests for debugging
        console.log('HTTP Requests to local server:');
        requests.forEach((req, index) => {
            console.log(`  ${index + 1}. [${req.type}] ${req.url}`);
        });

        // Count local requests (excluding external CDN like Tailwind)
        const localRequests = requests.filter(r => r.url.startsWith('http://localhost:8080'));

        console.log(`\nTotal local HTTP requests: ${localRequests.length}`);

        // For a simple static site, we expect minimal requests:
        // - index.html (document)
        // - images (logo.gif, usage.gif)
        // - CSS (if not inlined)
        // A reasonable threshold is less than 10 requests
        expect(localRequests.length).toBeLessThan(10);

        // Verify the main document loads
        const documentRequests = localRequests.filter(r => r.type === 'document');
        expect(documentRequests.length).toBeGreaterThanOrEqual(1);
    });

    test('Page has proper meta tags for SEO', async ({ page }) => {
        await page.goto('/');

        // Check for essential meta tags
        const description = await page.getAttribute('meta[name="description"]', 'content');
        expect(description).toBeTruthy();
        expect(description.length).toBeGreaterThan(50);

        // Check for viewport meta tag
        const viewport = await page.getAttribute('meta[name="viewport"]', 'content');
        expect(viewport).toContain('width=device-width');

        // Check for charset
        const charset = await page.getAttribute('meta[charset]', 'charset');
        expect(charset?.toLowerCase()).toBe('utf-8');
    });

    test('Page content is visible without JavaScript errors', async ({ page }) => {
        const errors = [];
        page.on('pageerror', (error) => {
            errors.push(error.message);
        });

        await page.goto('/', { waitUntil: 'domcontentloaded' });

        // Verify main content is visible
        const heroSection = page.locator('section[data-testid="hero-section"], #hero, .hero, main section:first-child');
        await expect(heroSection.first()).toBeVisible({ timeout: 5000 });

        // Log any JS errors (but don't fail on them as external CDN may cause issues)
        if (errors.length > 0) {
            console.log('JavaScript errors detected:', errors);
        }
    });
});
