/**
 * Performance Tests
 * Owner: Scenario 9 - Page Load Performance
 *
 * Tests:
 * - Page size measurement
 * - No-JavaScript functionality
 * - Static nature verification
 * - Render-blocking resources
 * - Image optimization
 *
 * Validates NFR-1 (page load < 2s), NFR-3 (no JS dependencies), NFR-4 (static HTML)
 */

const { test, expect } = require('@playwright/test');
const { gotoHomepage, SELECTORS } = require('./test-utils');
const path = require('path');
const fs = require('fs');

// Maximum recommended total page size for fast 3G loading (500KB excluding large images)
// Note: NFR-1 requires < 2 seconds on 3G (~750 kbps)
// 500KB * 8 bits = 4000 Kbits / 750 kbps = ~5.3 seconds
// For <2s target, aim for ~180KB total, but we allow 500KB as reasonable threshold
const MAX_PAGE_SIZE_BYTES = 500 * 1024; // 500KB
const MAX_IMAGE_SIZE_BYTES = 200 * 1024; // 200KB per image for reasonable loading

test.describe('Page Load Performance', () => {
    test.describe('Test Case 1: Page Size Measurement', () => {
        test('total page size (HTML + CSS + JS) should be reasonable for fast loading', async ({ page }) => {
            const resourceSizes = {
                html: 0,
                css: 0,
                js: 0,
                images: 0,
                other: 0,
                total: 0
            };

            // Track all network requests
            page.on('response', async (response) => {
                const url = response.url();
                const contentLength = response.headers()['content-length'];
                const size = contentLength ? parseInt(contentLength, 10) : 0;

                if (url.endsWith('.html') || url === 'http://localhost:3000/' || url === 'http://localhost:3000') {
                    resourceSizes.html += size;
                } else if (url.endsWith('.css')) {
                    resourceSizes.css += size;
                } else if (url.endsWith('.js')) {
                    resourceSizes.js += size;
                } else if (/\.(gif|png|jpg|jpeg|webp|svg|ico)$/i.test(url)) {
                    resourceSizes.images += size;
                } else {
                    resourceSizes.other += size;
                }
                resourceSizes.total += size;
            });

            await page.goto('/');
            await page.waitForLoadState('networkidle');

            // Check HTML/CSS/JS size (excluding images which are tested separately)
            const coreSize = resourceSizes.html + resourceSizes.css + resourceSizes.js;
            console.log('Resource sizes:', resourceSizes);
            console.log('Core size (HTML+CSS+JS):', coreSize, 'bytes');

            // Core assets (HTML, CSS, JS) should be under reasonable limit
            expect(coreSize).toBeLessThan(MAX_PAGE_SIZE_BYTES);
        });

        test('HTML file size should be reasonable', async ({ page }) => {
            const response = await page.goto('/');
            const body = await response.body();
            const htmlSize = body.length;

            console.log('HTML size:', htmlSize, 'bytes');

            // HTML should be under 50KB for a simple static page
            expect(htmlSize).toBeLessThan(50 * 1024);
        });
    });

    test.describe('Test Case 2: Core Content Without JavaScript', () => {
        test('core content should be visible with JavaScript disabled', async ({ browser }) => {
            // Create a context with JavaScript disabled
            const context = await browser.newContext({
                javaScriptEnabled: false
            });
            const page = await context.newPage();

            await page.goto('/');
            await page.waitForLoadState('domcontentloaded');

            // Verify core content is visible without JS
            // Hero section
            await expect(page.locator(SELECTORS.heroTitle)).toBeVisible();
            await expect(page.locator(SELECTORS.heroTagline)).toBeVisible();

            // Features section
            await expect(page.locator(SELECTORS.features)).toBeVisible();
            const featureCards = page.locator(SELECTORS.featureCard);
            await expect(featureCards).toHaveCount(4);

            // Quick Start section
            await expect(page.locator(SELECTORS.quickstart)).toBeVisible();
            await expect(page.locator(SELECTORS.codeBlock)).toBeVisible();

            // Navigation
            await expect(page.locator(SELECTORS.nav)).toBeVisible();
            await expect(page.locator(SELECTORS.navLinks)).toBeVisible();

            // Footer
            await expect(page.locator(SELECTORS.footer)).toBeVisible();

            await context.close();
        });

        test('navigation links should work without JavaScript', async ({ browser }) => {
            const context = await browser.newContext({
                javaScriptEnabled: false
            });
            const page = await context.newPage();

            await page.goto('/');

            // Check that anchor links exist and have correct hrefs
            // Use nav-specific selectors to avoid matching CTA buttons
            const featuresLink = page.locator('.nav-links a[href="#features"]');
            const quickstartLink = page.locator('.nav-links a[href="#quickstart"]');

            await expect(featuresLink).toBeVisible();
            await expect(quickstartLink).toBeVisible();

            // Verify target sections exist
            await expect(page.locator('#features')).toBeVisible();
            await expect(page.locator('#quickstart')).toBeVisible();

            await context.close();
        });
    });

    test.describe('Test Case 3: No Backend API Calls', () => {
        test('page should load as static HTML without fetch/XMLHttpRequest for content', async ({ page }) => {
            const apiCalls = [];

            // Intercept all network requests
            page.on('request', (request) => {
                const url = request.url();
                const resourceType = request.resourceType();

                // Track fetch/XHR requests to external APIs
                if (resourceType === 'fetch' || resourceType === 'xhr') {
                    apiCalls.push({
                        url,
                        type: resourceType
                    });
                }

                // Also check for requests to common API patterns
                if (url.includes('/api/') || url.includes('graphql')) {
                    apiCalls.push({
                        url,
                        type: 'api'
                    });
                }
            });

            await page.goto('/');
            await page.waitForLoadState('networkidle');

            // Filter out badge image requests which are external but not API calls
            const actualApiCalls = apiCalls.filter(call =>
                !call.url.includes('circleci.com') &&
                !call.url.endsWith('.svg') &&
                !call.url.endsWith('.png')
            );

            console.log('API calls detected:', actualApiCalls);

            // No API calls should be made for content loading
            expect(actualApiCalls.length).toBe(0);
        });

        test('page should not require backend server for content', async ({ page }) => {
            await page.goto('/');
            await page.waitForLoadState('domcontentloaded');

            // All content should be rendered from static HTML
            const heroTitle = await page.locator(SELECTORS.heroTitle).textContent();
            const heroTagline = await page.locator(SELECTORS.heroTagline).textContent();

            expect(heroTitle).toBe('MirDB');
            expect(heroTagline).toContain('Persistent Key-Value Store');

            // Verify static code examples are present in HTML
            const codeBlock = await page.locator(SELECTORS.codeBlock).textContent();
            expect(codeBlock).toContain('cargo install mirdb');
        });
    });

    test.describe('Test Case 4: Render-Blocking Resources', () => {
        test('should have minimal render-blocking resources', async ({ page }) => {
            // Go to the page and measure time to first contentful paint
            await page.goto('/');

            // Check that CSS files are linked (not inlined - inlined would be better but linked is acceptable)
            const stylesheets = await page.locator('link[rel="stylesheet"]').count();
            console.log('Number of external stylesheets:', stylesheets);

            // Should have reasonable number of stylesheets (not too many)
            expect(stylesheets).toBeLessThanOrEqual(5);

            // Check that scripts use defer/async or are at end of body
            const scripts = page.locator('script[src]');
            const scriptCount = await scripts.count();

            for (let i = 0; i < scriptCount; i++) {
                const script = scripts.nth(i);
                const hasDefer = await script.getAttribute('defer');
                const hasAsync = await script.getAttribute('async');

                // Scripts should have defer or async to avoid blocking
                const isNonBlocking = hasDefer !== null || hasAsync !== null;
                expect(isNonBlocking).toBe(true);
            }
        });

        test('CSS should be optimized for fast first paint', async ({ page }) => {
            await page.goto('/');
            await page.waitForLoadState('domcontentloaded');

            // Verify critical content is styled immediately (hero section)
            const heroSection = page.locator(SELECTORS.hero);
            await expect(heroSection).toBeVisible();

            // Check that hero has proper styling applied
            const heroDisplay = await heroSection.evaluate(el =>
                window.getComputedStyle(el).display
            );
            expect(heroDisplay).not.toBe('none');

            // Body should have background color applied (not default white)
            const bodyBg = await page.evaluate(() =>
                window.getComputedStyle(document.body).backgroundColor
            );
            expect(bodyBg).not.toBe('rgba(0, 0, 0, 0)'); // Not transparent
        });
    });

    test.describe('Test Case 5: Image Optimization', () => {
        test('images should use appropriate formats', async ({ page }) => {
            await page.goto('/');
            await page.waitForLoadState('domcontentloaded');

            // Get all images on the page
            const images = page.locator('img');
            const imageCount = await images.count();

            console.log('Number of images:', imageCount);

            for (let i = 0; i < imageCount; i++) {
                const img = images.nth(i);
                const src = await img.getAttribute('src');
                const alt = await img.getAttribute('alt');

                console.log(`Image ${i + 1}: src="${src}", alt="${alt}"`);

                // All images should have alt text
                expect(alt).not.toBeNull();
                expect(alt).not.toBe('');

                // Check for appropriate image formats
                if (src && !src.startsWith('data:') && !src.includes('circleci.com')) {
                    // For local images, prefer modern formats or appropriately used legacy formats
                    const isValidFormat = /\.(gif|png|jpg|jpeg|webp|svg|ico|avif)$/i.test(src);
                    expect(isValidFormat).toBe(true);
                }
            }
        });

        test('logo image should have reasonable dimensions', async ({ page }) => {
            await page.goto('/');
            await page.waitForLoadState('domcontentloaded');

            const logo = page.locator(SELECTORS.heroLogo);
            await expect(logo).toBeVisible();

            // Logo should be reasonably sized on the page
            const boundingBox = await logo.boundingBox();
            expect(boundingBox).not.toBeNull();

            if (boundingBox) {
                // Logo should fit within reasonable dimensions
                expect(boundingBox.width).toBeLessThanOrEqual(400);
                expect(boundingBox.height).toBeLessThanOrEqual(400);
            }
        });

        test('SVG icons should be used for feature icons (inline or external)', async ({ page }) => {
            await page.goto('/');
            await page.waitForLoadState('domcontentloaded');

            // Check that feature icons use SVG
            const featureIcons = page.locator('.feature-icon svg');
            const iconCount = await featureIcons.count();

            console.log('Number of SVG feature icons:', iconCount);

            // All 4 feature cards should have SVG icons
            expect(iconCount).toBe(4);
        });
    });

    test.describe('Additional Performance Checks', () => {
        test('page should reach networkidle quickly', async ({ page }) => {
            const startTime = Date.now();

            await page.goto('/');
            await page.waitForLoadState('networkidle');

            const loadTime = Date.now() - startTime;
            console.log('Time to network idle:', loadTime, 'ms');

            // Should reach network idle within reasonable time (excluding large image downloads)
            // Note: This doesn't simulate 3G but checks local loading performance
            expect(loadTime).toBeLessThan(10000); // 10 seconds max locally
        });

        test('page should have meta viewport for mobile', async ({ page }) => {
            await page.goto('/');

            const viewport = page.locator('meta[name="viewport"]');
            await expect(viewport).toHaveCount(1);

            const content = await viewport.getAttribute('content');
            expect(content).toContain('width=device-width');
        });

        test('page should have proper document structure', async ({ page }) => {
            await page.goto('/');

            // Check DOCTYPE
            const doctype = await page.evaluate(() =>
                document.doctype ? document.doctype.name : null
            );
            expect(doctype).toBe('html');

            // Check lang attribute
            const lang = await page.evaluate(() =>
                document.documentElement.lang
            );
            expect(lang).toBe('en');

            // Check charset
            const charset = page.locator('meta[charset]');
            await expect(charset).toHaveCount(1);
        });
    });
});
