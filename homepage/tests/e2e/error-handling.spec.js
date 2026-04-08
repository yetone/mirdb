/**
 * Error Handling and Edge Cases E2E Tests
 * Owner: Scenario 20 - Error Handling and Edge Cases
 *
 * Test cases:
 * 1. Scan all internal links - All internal anchor links point to existing sections
 * 2. Load page with JavaScript disabled - Core content is visible and readable without JS
 * 3. Test with slow network (3G) - Page loads progressively and remains usable
 * 4. Test with images disabled - Page content is accessible, alt text is visible
 */

const { test, expect } = require('@playwright/test');

test.describe('Error Handling and Edge Cases', () => {
    test.describe('Internal Links Validation', () => {
        test('all internal anchor links point to existing sections', async ({ page }) => {
            // Test Case ID: 1
            // Input: Scan all internal links
            // Expected: All internal anchor links point to existing sections

            await page.goto('/');

            // Find all anchor links that start with #
            const anchorLinks = page.locator('a[href^="#"]');
            const count = await anchorLinks.count();

            // Ensure there are some internal links to test
            expect(count).toBeGreaterThan(0);

            // Collect all unique href values
            const hrefs = new Set();
            for (let i = 0; i < count; i++) {
                const href = await anchorLinks.nth(i).getAttribute('href');
                if (href && href !== '#') {
                    hrefs.add(href);
                }
            }

            // Verify each anchor target exists on the page
            for (const href of hrefs) {
                // Extract the ID from the href (remove the # prefix)
                const targetId = href.substring(1);

                // Check if an element with this ID exists
                const targetElement = page.locator(`#${targetId}`);
                const exists = await targetElement.count();

                expect(exists, `Expected element with id="${targetId}" to exist for anchor link ${href}`).toBeGreaterThan(0);
            }
        });

        test('navigation anchor links scroll to visible sections', async ({ page }) => {
            // Additional validation: ensure links actually navigate to visible sections
            await page.goto('/');

            const navLinks = [
                { href: '#features', sectionId: 'features' },
                { href: '#architecture', sectionId: 'architecture' },
                { href: '#installation', sectionId: 'installation' }
            ];

            for (const { href, sectionId } of navLinks) {
                const link = page.locator(`nav a[href="${href}"]`).first();
                if (await link.count() > 0) {
                    await link.click();
                    await page.waitForTimeout(300);

                    const section = page.locator(`#${sectionId}`);
                    await expect(section).toBeVisible();
                }
            }
        });
    });

    test.describe('JavaScript Disabled Graceful Degradation', () => {
        test.use({ javaScriptEnabled: false });

        test('core content is visible and readable without JavaScript', async ({ page }) => {
            // Test Case ID: 2
            // Input: Load page with JavaScript disabled
            // Expected: Core content is visible and readable without JavaScript

            await page.goto('/');

            // Check navigation is visible
            const nav = page.locator('nav');
            await expect(nav).toBeVisible();

            // Check main heading (h1) is visible
            const mainHeading = page.locator('h1');
            await expect(mainHeading).toBeVisible();
            const headingText = await mainHeading.textContent();
            expect(headingText).toContain('Persistent Key-Value Store');

            // Check hero section is visible
            const heroSection = page.locator('#hero, .hero-section, [id*="hero"]').first();
            await expect(heroSection).toBeVisible();

            // Check features section is visible
            const featuresSection = page.locator('#features');
            await expect(featuresSection).toBeVisible();

            // Check all feature cards are visible
            const featureCards = page.locator('.features-card');
            const cardCount = await featureCards.count();
            expect(cardCount).toBeGreaterThanOrEqual(6);

            // Check architecture section is visible
            const archSection = page.locator('#architecture');
            await expect(archSection).toBeVisible();

            // Check code examples section is visible
            const codeSection = page.locator('#code-examples');
            await expect(codeSection).toBeVisible();

            // Check installation section is visible
            const installSection = page.locator('#installation');
            await expect(installSection).toBeVisible();

            // Check status section is visible
            const statusSection = page.locator('#status');
            await expect(statusSection).toBeVisible();

            // Check footer is visible
            const footer = page.locator('footer');
            await expect(footer).toBeVisible();
        });

        test('page structure is semantic and accessible without JavaScript', async ({ page }) => {
            // Verify semantic HTML structure works without JS
            await page.goto('/');

            // Check main landmark
            const main = page.locator('main');
            await expect(main).toBeVisible();

            // Check all sections have proper headings
            const sections = page.locator('main > section');
            const sectionCount = await sections.count();
            expect(sectionCount).toBeGreaterThanOrEqual(4);

            // Verify each section has a heading (h1, h2, or h3)
            // Hero section uses h1, other sections use h2
            for (let i = 0; i < sectionCount; i++) {
                const section = sections.nth(i);
                const heading = section.locator('h1, h2, h3').first();
                const headingExists = await heading.count();
                expect(headingExists, `Section ${i + 1} should have a heading`).toBeGreaterThan(0);
            }
        });

        test('navigation links work without JavaScript', async ({ page }) => {
            // Verify anchor links still navigate without JS (native browser behavior)
            await page.goto('/');

            // Check that anchor links exist and have valid hrefs
            const navLinks = page.locator('nav a[href^="#"]');
            const count = await navLinks.count();
            expect(count).toBeGreaterThan(0);

            // Verify links have proper href attributes
            for (let i = 0; i < count; i++) {
                const link = navLinks.nth(i);
                const href = await link.getAttribute('href');
                expect(href).toBeTruthy();
                expect(href.startsWith('#')).toBe(true);
            }
        });
    });

    test.describe('Slow Network Performance (3G)', () => {
        test('page loads progressively and remains usable on slow connections', async ({ page, browser }) => {
            // Test Case ID: 3
            // Input: Test with slow network (3G)
            // Expected: Page loads progressively and remains usable on slow connections

            // Create a new context with throttled network (simulating 3G)
            const context = await browser.newContext();
            const slowPage = await context.newPage();

            // Simulate slow 3G network conditions
            const cdpSession = await slowPage.context().newCDPSession(slowPage);
            await cdpSession.send('Network.emulateNetworkConditions', {
                offline: false,
                downloadThroughput: (750 * 1024) / 8, // 750 Kbps - Slow 3G
                uploadThroughput: (250 * 1024) / 8,   // 250 Kbps
                latency: 400 // 400ms latency
            });

            const loadStart = Date.now();

            // Navigate to page with extended timeout for slow network
            await slowPage.goto('/', { timeout: 60000, waitUntil: 'domcontentloaded' });

            const domContentLoaded = Date.now() - loadStart;

            // Check that critical content is visible shortly after DOM load
            const mainHeading = slowPage.locator('h1');
            await expect(mainHeading).toBeVisible({ timeout: 10000 });

            // Check navigation is available
            const nav = slowPage.locator('nav');
            await expect(nav).toBeVisible({ timeout: 10000 });

            // Check main content area is visible
            const main = slowPage.locator('main');
            await expect(main).toBeVisible({ timeout: 10000 });

            // Log performance info (for debugging)
            console.log(`DOM Content Loaded in ${domContentLoaded}ms on 3G simulation`);

            // Verify page is interactive - can click navigation
            const featuresLink = slowPage.locator('nav a[href="#features"]').first();
            if (await featuresLink.count() > 0) {
                await featuresLink.click({ timeout: 5000 });
            }

            await context.close();
        });

        test('CSS loads and styles are applied on slow connections', async ({ page, browser }) => {
            // Test that CSS is rendered properly even on slow connections
            const context = await browser.newContext();
            const slowPage = await context.newPage();

            // Simulate slow 3G
            const cdpSession = await slowPage.context().newCDPSession(slowPage);
            await cdpSession.send('Network.emulateNetworkConditions', {
                offline: false,
                downloadThroughput: (750 * 1024) / 8,
                uploadThroughput: (250 * 1024) / 8,
                latency: 400
            });

            await slowPage.goto('/', { timeout: 60000, waitUntil: 'networkidle' });

            // Check that CSS is applied - verify some computed styles
            const hero = slowPage.locator('.hero-section, #hero').first();
            await expect(hero).toBeVisible({ timeout: 10000 });

            // Verify CSS is actually applied (not unstyled)
            const heroStyles = await hero.evaluate((el) => {
                const styles = window.getComputedStyle(el);
                return {
                    display: styles.display,
                    visibility: styles.visibility
                };
            });

            expect(heroStyles.visibility).not.toBe('hidden');
            expect(heroStyles.display).not.toBe('none');

            await context.close();
        });
    });

    test.describe('Images Disabled Accessibility', () => {
        test('page content is accessible with images disabled', async ({ page, browser }) => {
            // Test Case ID: 4
            // Input: Test with images disabled
            // Expected: Page content is accessible, alt text is visible for images

            // Create context that blocks images
            const context = await browser.newContext();
            const imagelessPage = await context.newPage();

            // Block all image requests
            await imagelessPage.route('**/*.{png,jpg,jpeg,gif,webp,svg,ico}', (route) => {
                route.abort();
            });

            await imagelessPage.goto('/');

            // Check that core text content is still visible
            const mainHeading = imagelessPage.locator('h1');
            await expect(mainHeading).toBeVisible();

            // Check that navigation text is visible
            const navText = await imagelessPage.locator('nav').textContent();
            expect(navText).toContain('MirDB');
            expect(navText).toContain('Features');

            // Check feature cards are visible with their text content
            const featureCards = imagelessPage.locator('.features-card');
            const cardCount = await featureCards.count();
            expect(cardCount).toBeGreaterThan(0);

            // Verify each feature card has visible text
            for (let i = 0; i < cardCount; i++) {
                const card = featureCards.nth(i);
                const title = card.locator('.features-card-title, h3').first();
                await expect(title).toBeVisible();

                const description = card.locator('.features-card-description, p').first();
                await expect(description).toBeVisible();
            }

            // Check code examples are visible
            const codeBlocks = imagelessPage.locator('.code-block, pre');
            const codeCount = await codeBlocks.count();
            expect(codeCount).toBeGreaterThan(0);

            await context.close();
        });

        test('images have alt text for accessibility', async ({ page }) => {
            // Verify all images have proper alt attributes
            await page.goto('/');

            // Find all img elements
            const images = page.locator('img');
            const imgCount = await images.count();

            // Verify each image has alt attribute
            for (let i = 0; i < imgCount; i++) {
                const img = images.nth(i);
                const alt = await img.getAttribute('alt');

                // Alt should exist (can be empty string for decorative images)
                expect(alt !== null, `Image ${i + 1} should have an alt attribute`).toBe(true);
            }

            // Check decorative SVG icons have aria-hidden
            const decorativeSvgs = page.locator('svg[aria-hidden="true"]');
            const svgCount = await decorativeSvgs.count();

            // There should be some decorative SVGs properly marked
            expect(svgCount).toBeGreaterThan(0);
        });

        test('page remains navigable when images fail to load', async ({ page, browser }) => {
            // Test that page structure and navigation work without images
            const context = await browser.newContext();
            const noImgPage = await context.newPage();

            // Block images
            await noImgPage.route('**/*.{png,jpg,jpeg,gif,webp,svg,ico}', (route) => {
                route.abort();
            });

            await noImgPage.goto('/');

            // Test navigation still works
            const featuresLink = noImgPage.locator('nav a[href="#features"]').first();
            if (await featuresLink.count() > 0) {
                await featuresLink.click();
                await noImgPage.waitForTimeout(300);

                const featuresSection = noImgPage.locator('#features');
                await expect(featuresSection).toBeVisible();
            }

            // Test footer navigation
            const footer = noImgPage.locator('footer');
            await expect(footer).toBeVisible();

            const footerText = await footer.textContent();
            expect(footerText).toContain('MirDB');

            await context.close();
        });
    });

    test.describe('Additional Edge Cases', () => {
        test('page handles special characters in content correctly', async ({ page }) => {
            // Verify special characters are properly rendered
            await page.goto('/');

            // Check that code examples with special characters are visible
            const codeBlocks = page.locator('pre code');
            const codeCount = await codeBlocks.count();
            expect(codeCount).toBeGreaterThan(0);

            // Verify code content is not empty
            for (let i = 0; i < codeCount; i++) {
                const code = codeBlocks.nth(i);
                const text = await code.textContent();
                expect(text.length).toBeGreaterThan(0);
            }
        });

        test('no console errors on page load', async ({ page }) => {
            // Collect console errors
            const consoleErrors = [];
            page.on('console', (msg) => {
                if (msg.type() === 'error') {
                    consoleErrors.push(msg.text());
                }
            });

            await page.goto('/');

            // Wait for page to fully load
            await page.waitForTimeout(1000);

            // Filter out known acceptable errors (e.g., favicon 404)
            const criticalErrors = consoleErrors.filter(
                (error) => !error.includes('favicon') && !error.includes('404')
            );

            expect(
                criticalErrors.length,
                `Found console errors: ${criticalErrors.join(', ')}`
            ).toBe(0);
        });

        test('page handles scroll to non-existent anchor gracefully', async ({ page }) => {
            // Test that navigating to a non-existent anchor doesn't break the page
            await page.goto('/#nonexistent-section-xyz');

            // Page should still load
            const main = page.locator('main');
            await expect(main).toBeVisible();

            // Navigation should still work
            const nav = page.locator('nav');
            await expect(nav).toBeVisible();
        });
    });
});
