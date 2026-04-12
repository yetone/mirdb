/**
 * Asset Loading and Error Handling E2E Tests
 * Owner: Scenario 15 - Asset Loading and Error Handling
 *
 * Tests for verifying all assets load correctly and graceful degradation on errors.
 * Includes checks for logo.gif, usage.gif, CircleCI badge, console errors, and internal links.
 */

import { test, expect, type Page, type ConsoleMessage } from '@playwright/test';

test.describe('Asset Loading and Error Handling', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
    });

    test('TC1: logo.gif returns 200 status and valid image content-type', async ({ page, request }) => {
        // Check the logo element exists in the page
        const logo = page.locator('img[src*="logo.gif"]');
        await expect(logo).toBeVisible();

        // Get the full URL for the logo
        const logoSrc = await logo.getAttribute('src');
        expect(logoSrc).toBeTruthy();

        // Make HTTP request to verify the asset loads correctly
        const response = await request.get(`http://localhost:3000/assets/logo.gif`);

        // Verify 200 status code
        expect(response.status()).toBe(200);

        // Verify valid image content-type (GIF)
        const contentType = response.headers()['content-type'];
        expect(contentType).toContain('image/gif');
    });

    test('TC2: usage.gif returns 200 status and valid image content-type', async ({ page, request }) => {
        // Check the usage image exists in the page
        const usageImg = page.locator('img[src*="usage.gif"]');
        await expect(usageImg).toBeVisible();

        // Get the full URL for the usage gif
        const usageSrc = await usageImg.getAttribute('src');
        expect(usageSrc).toBeTruthy();

        // Make HTTP request to verify the asset loads correctly
        const response = await request.get(`http://localhost:3000/assets/usage.gif`);

        // Verify 200 status code
        expect(response.status()).toBe(200);

        // Verify valid image content-type (GIF)
        const contentType = response.headers()['content-type'];
        expect(contentType).toContain('image/gif');
    });

    test('TC3: CircleCI badge image loads (check for presence in DOM)', async ({ page }) => {
        // Find the CircleCI badge image
        const badgeImg = page.locator('img[src*="atompunk.yetone.fun"], img[alt*="CircleCI"], img[alt*="Build Status"]');

        // Check the badge element exists in the DOM
        await expect(badgeImg.first()).toBeVisible();

        // Get the src attribute
        const badgeSrc = await badgeImg.first().getAttribute('src');
        expect(badgeSrc).toBeTruthy();

        // Verify it's pointing to the expected CircleCI badge URL
        expect(badgeSrc).toContain('atompunk.yetone.fun/github/yetone/mirdb');

        // Verify the badge has alt text for accessibility
        const altText = await badgeImg.first().getAttribute('alt');
        expect(altText).toBeTruthy();
        expect(altText!.length).toBeGreaterThan(0);
    });

    test('TC4: No JavaScript errors or 404s logged in browser console', async ({ page }) => {
        const consoleErrors: ConsoleMessage[] = [];
        const networkErrors: string[] = [];

        // Collect console errors
        page.on('console', (msg) => {
            if (msg.type() === 'error') {
                consoleErrors.push(msg);
            }
        });

        // Collect 404 network errors
        page.on('response', (response) => {
            if (response.status() === 404) {
                networkErrors.push(`404: ${response.url()}`);
            }
        });

        // Navigate and wait for full page load
        await page.goto('/', { waitUntil: 'networkidle' });

        // Wait a bit for any lazy-loaded resources
        await page.waitForTimeout(1000);

        // Filter out external resource errors (we can't control external services)
        const internalErrors = consoleErrors.filter(msg => {
            const text = msg.text();
            const location = msg.location();
            const locationUrl = location?.url || '';

            // Ignore errors from external resources (like CircleCI badge which may fail)
            // Check both the error text and the source location URL
            const isExternalError =
                text.includes('atompunk.yetone.fun') ||
                text.includes('circleci.com') ||
                text.includes('github.com') ||
                locationUrl.includes('atompunk.yetone.fun') ||
                locationUrl.includes('circleci.com') ||
                locationUrl.includes('github.com') ||
                // Also ignore generic network errors for external resources
                (text.includes('ERR_NAME_NOT_RESOLVED') && !locationUrl.includes('localhost')) ||
                (text.includes('net::ERR_') && !locationUrl.includes('localhost') && locationUrl.startsWith('http'));

            return !isExternalError;
        });

        const internal404s = networkErrors.filter(url => {
            // Ignore 404s from external resources
            return url.includes('localhost:3000');
        });

        // Assert no internal JavaScript errors
        expect(internalErrors, `Console errors found: ${internalErrors.map(e => e.text()).join(', ')}`).toHaveLength(0);

        // Assert no internal 404 errors
        expect(internal404s, `404 errors found: ${internal404s.join(', ')}`).toHaveLength(0);
    });

    test('TC5: No broken internal links (all href point to valid targets)', async ({ page }) => {
        // Get all internal anchor links (href starting with # or /)
        const internalLinks = page.locator('a[href^="#"], a[href^="/"]');
        const linkCount = await internalLinks.count();

        expect(linkCount).toBeGreaterThan(0);

        // Check each internal link
        for (let i = 0; i < linkCount; i++) {
            const link = internalLinks.nth(i);
            const href = await link.getAttribute('href');

            if (href?.startsWith('#')) {
                // For anchor links, verify the target element exists
                const targetId = href.slice(1); // Remove the # prefix

                if (targetId === '') {
                    // href="#" is a valid top-of-page link
                    continue;
                }

                const targetElement = page.locator(`#${targetId}`);
                const targetExists = await targetElement.count();

                expect(targetExists, `Anchor target not found for href="${href}"`).toBeGreaterThan(0);
            } else if (href?.startsWith('/')) {
                // For internal page links, we would verify the page exists
                // For this static site, we only have the root page
                expect(href).toBe('/');
            }
        }
    });

    test('Images have proper dimensions and are not broken', async ({ page }) => {
        // Check all images on the page
        const images = page.locator('img');
        const imageCount = await images.count();

        expect(imageCount).toBeGreaterThan(0);

        for (let i = 0; i < imageCount; i++) {
            const img = images.nth(i);
            const src = await img.getAttribute('src');

            // Skip external images (CircleCI badge)
            if (src?.startsWith('http') && !src.includes('localhost')) {
                continue;
            }

            // Check image has natural dimensions (not broken)
            const naturalWidth = await img.evaluate((el: HTMLImageElement) => el.naturalWidth);
            const naturalHeight = await img.evaluate((el: HTMLImageElement) => el.naturalHeight);

            // naturalWidth and naturalHeight should be > 0 for properly loaded images
            expect(naturalWidth, `Image ${src} has no natural width (may be broken)`).toBeGreaterThan(0);
            expect(naturalHeight, `Image ${src} has no natural height (may be broken)`).toBeGreaterThan(0);
        }
    });

    test('Page handles external resource failures gracefully', async ({ page }) => {
        // Route to block the external badge image
        await page.route('**/atompunk.yetone.fun/**', (route) => {
            route.abort();
        });

        // Navigate with blocked external resource
        await page.goto('/');

        // Page should still load and be functional
        await expect(page.locator('h1')).toBeVisible();
        await expect(page.locator('#hero')).toBeVisible();
        await expect(page.locator('#features')).toBeVisible();
        await expect(page.locator('#quickstart')).toBeVisible();
        await expect(page.locator('#usage')).toBeVisible();
        await expect(page.locator('footer')).toBeVisible();

        // Internal images should still load
        const logo = page.locator('img[src*="logo.gif"]');
        await expect(logo).toBeVisible();

        const usageGif = page.locator('img[src*="usage.gif"]');
        await expect(usageGif).toBeVisible();

        // Page should be interactive - Get Started button should work
        const ctaButton = page.locator('a[href="#quickstart"]').first();
        await expect(ctaButton).toBeVisible();
    });
});
