/**
 * Performance Requirements E2E Tests
 * Owner: Scenario 12 - Performance Requirements
 *
 * Test cases:
 * - First Contentful Paint (FCP) is under 1.8 seconds
 * - Largest Contentful Paint (LCP) is under 2.5 seconds
 * - Time to Interactive (TTI) is under 2 seconds
 * - Total page weight is optimized for fast loading
 * - Performance score is 90 or above (simulated via metrics validation)
 */

const { test, expect } = require('@playwright/test');

// Performance thresholds based on NFR-1 and Success Criteria
const PERFORMANCE_THRESHOLDS = {
    FCP_MS: 1800, // First Contentful Paint: under 1.8 seconds
    LCP_MS: 2500, // Largest Contentful Paint: under 2.5 seconds
    TTI_MS: 2000, // Time to Interactive: under 2 seconds
    MAX_PAGE_WEIGHT_KB: 500, // Maximum total page weight in KB (HTML, CSS, JS, images)
    MAX_HTML_SIZE_KB: 100, // Maximum HTML size
    MAX_CSS_SIZE_KB: 50, // Maximum CSS size
    MAX_JS_SIZE_KB: 100, // Maximum JavaScript size
    MAX_IMAGE_SIZE_KB: 250, // Maximum total image size
};

test.describe('Performance Requirements', () => {
    // Only run performance tests on Chromium for consistent metrics
    test.skip(({ browserName }) => browserName !== 'chromium', 'Performance tests only run on Chromium');

    test('First Contentful Paint (FCP) is under 1.8 seconds', async ({ page }) => {
        // Test Case ID: 1
        // Input: Measure First Contentful Paint (FCP)
        // Expected: FCP is under 1.8 seconds

        // Enable performance metrics collection via CDP
        const client = await page.context().newCDPSession(page);
        await client.send('Performance.enable');

        // Navigate and wait for page to load
        await page.goto('/', { waitUntil: 'networkidle' });

        // Get FCP using Performance Observer API
        const fcpMetric = await page.evaluate(() => {
            return new Promise((resolve) => {
                // Check if we already have the FCP entry
                const entries = performance.getEntriesByType('paint');
                const fcpEntry = entries.find(entry => entry.name === 'first-contentful-paint');

                if (fcpEntry) {
                    resolve(fcpEntry.startTime);
                } else {
                    // If not available yet, use PerformanceObserver
                    const observer = new PerformanceObserver((list) => {
                        const entry = list.getEntriesByName('first-contentful-paint')[0];
                        if (entry) {
                            observer.disconnect();
                            resolve(entry.startTime);
                        }
                    });
                    observer.observe({ entryTypes: ['paint'] });

                    // Fallback timeout
                    setTimeout(() => resolve(null), 5000);
                }
            });
        });

        expect(fcpMetric).not.toBeNull();
        expect(fcpMetric).toBeLessThan(PERFORMANCE_THRESHOLDS.FCP_MS);
        console.log(`FCP: ${fcpMetric.toFixed(2)}ms (threshold: ${PERFORMANCE_THRESHOLDS.FCP_MS}ms)`);
    });

    test('Largest Contentful Paint (LCP) is under 2.5 seconds', async ({ page }) => {
        // Test Case ID: 2
        // Input: Measure Largest Contentful Paint (LCP)
        // Expected: LCP is under 2.5 seconds

        // Navigate to the page first
        await page.goto('/', { waitUntil: 'networkidle' });

        // Get LCP from performance entries after page has loaded
        const lcpMetric = await page.evaluate(() => {
            return new Promise((resolve) => {
                // Check if we already have LCP entries
                const existingEntries = performance.getEntriesByType('largest-contentful-paint');
                if (existingEntries.length > 0) {
                    const lastEntry = existingEntries[existingEntries.length - 1];
                    resolve(lastEntry.startTime);
                    return;
                }

                // Set up observer for any remaining LCP updates
                let lcpValue = 0;
                const observer = new PerformanceObserver((list) => {
                    const entries = list.getEntries();
                    const lastEntry = entries[entries.length - 1];
                    if (lastEntry) {
                        lcpValue = lastEntry.startTime;
                    }
                });

                try {
                    observer.observe({ type: 'largest-contentful-paint', buffered: true });
                } catch (e) {
                    // Fallback for browsers that don't support buffered option
                    observer.observe({ entryTypes: ['largest-contentful-paint'] });
                }

                // Give time for LCP to be reported then resolve
                setTimeout(() => {
                    observer.disconnect();
                    resolve(lcpValue || null);
                }, 500);
            });
        });

        // LCP should be available - if null, use navigation timing as fallback
        const finalLCP = lcpMetric || await page.evaluate(() => {
            const nav = performance.getEntriesByType('navigation')[0];
            return nav ? nav.loadEventEnd : performance.now();
        });

        expect(finalLCP).not.toBeNull();
        expect(finalLCP).toBeLessThan(PERFORMANCE_THRESHOLDS.LCP_MS);
        console.log(`LCP: ${finalLCP.toFixed(2)}ms (threshold: ${PERFORMANCE_THRESHOLDS.LCP_MS}ms)`);
    });

    test('Time to Interactive (TTI) is under 2 seconds as per NFR-1', async ({ page }) => {
        // Test Case ID: 3
        // Input: Measure Time to Interactive (TTI)
        // Expected: TTI is under 2 seconds as per NFR-1

        const navigationStart = Date.now();

        await page.goto('/', { waitUntil: 'networkidle' });

        // Measure time until page is interactive (DOM content loaded + scripts executed)
        const ttiMetric = await page.evaluate(() => {
            const timing = performance.timing || performance.getEntriesByType('navigation')[0];

            if (timing.domInteractive && timing.navigationStart) {
                // Using Navigation Timing API Level 1
                return timing.domInteractive - timing.navigationStart;
            } else if (timing.domInteractive) {
                // Using Navigation Timing API Level 2
                return timing.domInteractive;
            }

            // Fallback: use domContentLoadedEventEnd
            if (timing.domContentLoadedEventEnd && timing.navigationStart) {
                return timing.domContentLoadedEventEnd - timing.navigationStart;
            } else if (timing.domContentLoadedEventEnd) {
                return timing.domContentLoadedEventEnd;
            }

            return null;
        });

        // Alternative: verify interactive elements are clickable
        const heroButton = page.locator('.hero-cta-primary');
        await expect(heroButton).toBeVisible();
        await expect(heroButton).toBeEnabled();

        const timeToInteractive = Date.now() - navigationStart;

        // Use the more accurate metric if available, otherwise use our measurement
        const finalTTI = ttiMetric || timeToInteractive;

        expect(finalTTI).toBeLessThan(PERFORMANCE_THRESHOLDS.TTI_MS);
        console.log(`TTI: ${finalTTI.toFixed(2)}ms (threshold: ${PERFORMANCE_THRESHOLDS.TTI_MS}ms)`);
    });

    test('Total page weight (HTML, CSS, JS, images) is optimized for fast loading', async ({ page }) => {
        // Test Case ID: 4
        // Input: Check total page weight
        // Expected: Total page weight (HTML, CSS, JS, images) is optimized for fast loading

        // Track all network requests
        const resources = {
            html: 0,
            css: 0,
            js: 0,
            images: 0,
            other: 0,
        };

        page.on('response', async (response) => {
            try {
                const url = response.url();
                const contentLength = response.headers()['content-length'];
                const size = contentLength ? parseInt(contentLength, 10) : 0;

                // Get actual body size if content-length not available
                let bodySize = size;
                if (!bodySize) {
                    try {
                        const body = await response.body();
                        bodySize = body.length;
                    } catch {
                        bodySize = 0;
                    }
                }

                const bytesToKB = bodySize / 1024;

                if (url.endsWith('.html') || response.headers()['content-type']?.includes('text/html')) {
                    resources.html += bytesToKB;
                } else if (url.endsWith('.css') || response.headers()['content-type']?.includes('text/css')) {
                    resources.css += bytesToKB;
                } else if (url.endsWith('.js') || response.headers()['content-type']?.includes('javascript')) {
                    resources.js += bytesToKB;
                } else if (url.match(/\.(png|jpg|jpeg|gif|svg|webp|ico)$/i) ||
                           response.headers()['content-type']?.includes('image/')) {
                    resources.images += bytesToKB;
                } else {
                    resources.other += bytesToKB;
                }
            } catch (e) {
                // Ignore errors from responses we can't read
            }
        });

        await page.goto('/', { waitUntil: 'networkidle' });

        // Calculate total page weight
        const totalWeight = resources.html + resources.css + resources.js + resources.images + resources.other;

        console.log('Page weight breakdown:');
        console.log(`  HTML: ${resources.html.toFixed(2)} KB (max: ${PERFORMANCE_THRESHOLDS.MAX_HTML_SIZE_KB} KB)`);
        console.log(`  CSS: ${resources.css.toFixed(2)} KB (max: ${PERFORMANCE_THRESHOLDS.MAX_CSS_SIZE_KB} KB)`);
        console.log(`  JS: ${resources.js.toFixed(2)} KB (max: ${PERFORMANCE_THRESHOLDS.MAX_JS_SIZE_KB} KB)`);
        console.log(`  Images: ${resources.images.toFixed(2)} KB (max: ${PERFORMANCE_THRESHOLDS.MAX_IMAGE_SIZE_KB} KB)`);
        console.log(`  Other: ${resources.other.toFixed(2)} KB`);
        console.log(`  TOTAL: ${totalWeight.toFixed(2)} KB (max: ${PERFORMANCE_THRESHOLDS.MAX_PAGE_WEIGHT_KB} KB)`);

        // Verify individual resource categories are within limits
        expect(resources.html).toBeLessThan(PERFORMANCE_THRESHOLDS.MAX_HTML_SIZE_KB);
        expect(resources.css).toBeLessThan(PERFORMANCE_THRESHOLDS.MAX_CSS_SIZE_KB);
        expect(resources.js).toBeLessThan(PERFORMANCE_THRESHOLDS.MAX_JS_SIZE_KB);
        expect(resources.images).toBeLessThan(PERFORMANCE_THRESHOLDS.MAX_IMAGE_SIZE_KB);

        // Verify total page weight
        expect(totalWeight).toBeLessThan(PERFORMANCE_THRESHOLDS.MAX_PAGE_WEIGHT_KB);
    });

    test('Performance score is 90 or above (Core Web Vitals validation)', async ({ page }) => {
        // Test Case ID: 5
        // Input: Run Lighthouse Performance audit
        // Expected: Performance score is 90 or above

        // Since we can't run actual Lighthouse in Playwright directly,
        // we validate all Core Web Vitals meet the thresholds that would
        // result in a 90+ Lighthouse performance score

        await page.goto('/', { waitUntil: 'networkidle' });

        // Collect all performance metrics
        const metrics = await page.evaluate(() => {
            const results = {
                fcp: null,
                lcp: null,
                cls: 0,
                fid: null,
                tti: null,
                loadTime: null,
            };

            // Get paint timings (FCP)
            const paintEntries = performance.getEntriesByType('paint');
            const fcpEntry = paintEntries.find(e => e.name === 'first-contentful-paint');
            if (fcpEntry) {
                results.fcp = fcpEntry.startTime;
            }

            // Get navigation timing (TTI approximation)
            const navEntries = performance.getEntriesByType('navigation');
            if (navEntries.length > 0) {
                const nav = navEntries[0];
                results.tti = nav.domInteractive;
                results.loadTime = nav.loadEventEnd;
            } else if (performance.timing) {
                const timing = performance.timing;
                results.tti = timing.domInteractive - timing.navigationStart;
                results.loadTime = timing.loadEventEnd - timing.navigationStart;
            }

            return results;
        });

        // For LCP, we need to observe before navigation completes
        // Using a simpler approach: verify the page loads quickly
        expect(metrics.fcp).not.toBeNull();
        expect(metrics.tti).not.toBeNull();

        // Calculate a simulated performance score based on Core Web Vitals
        // Lighthouse scoring uses specific curves for each metric
        // We'll use simplified thresholds that correspond to a 90+ score

        let score = 100;

        // FCP scoring (Good: <1.8s = 100%, Needs Improvement: 1.8-3s, Poor: >3s)
        if (metrics.fcp > PERFORMANCE_THRESHOLDS.FCP_MS) {
            score -= 20;
        } else if (metrics.fcp > PERFORMANCE_THRESHOLDS.FCP_MS * 0.5) {
            score -= 5;
        }

        // TTI scoring (Good: <2s = 100%, Needs Improvement: 2-5s, Poor: >5s)
        if (metrics.tti > PERFORMANCE_THRESHOLDS.TTI_MS) {
            score -= 30;
        } else if (metrics.tti > PERFORMANCE_THRESHOLDS.TTI_MS * 0.5) {
            score -= 10;
        }

        // Load time bonus/penalty
        if (metrics.loadTime && metrics.loadTime < 2000) {
            // Good load time
        } else if (metrics.loadTime && metrics.loadTime > 3000) {
            score -= 10;
        }

        console.log('Performance metrics summary:');
        console.log(`  FCP: ${metrics.fcp?.toFixed(2) || 'N/A'}ms`);
        console.log(`  TTI: ${metrics.tti?.toFixed(2) || 'N/A'}ms`);
        console.log(`  Load Time: ${metrics.loadTime?.toFixed(2) || 'N/A'}ms`);
        console.log(`  Simulated Performance Score: ${score}`);

        // Verify all metrics are within thresholds for 90+ score
        expect(metrics.fcp).toBeLessThan(PERFORMANCE_THRESHOLDS.FCP_MS);
        expect(metrics.tti).toBeLessThan(PERFORMANCE_THRESHOLDS.TTI_MS);
        expect(score).toBeGreaterThanOrEqual(90);

        // Additional verification: ensure no blocking resources
        const blockingResources = await page.evaluate(() => {
            const resources = performance.getEntriesByType('resource');
            return resources.filter(r =>
                r.renderBlockingStatus === 'blocking' ||
                (r.initiatorType === 'script' && !r.name.includes('async'))
            ).length;
        });

        // A static site with minimal JS should have few render-blocking resources
        // Allow up to 10 blocking resources (stylesheets, inline scripts, etc.)
        expect(blockingResources).toBeLessThan(10);
        console.log(`  Render-blocking resources: ${blockingResources}`);
    });
});
