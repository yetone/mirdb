/**
 * Error Handling - Broken Assets E2E Tests
 * Verifies the page handles missing or broken assets gracefully
 *
 * Test Cases:
 * 1. Block logo.gif loading - page remains functional and alt text is displayed
 * 2. Block usage.gif loading - quick start section remains usable with alt text fallback
 * 3. Verify no JavaScript errors occur when images fail to load
 */

const { test, expect } = require('@playwright/test');
const path = require('path');

test.describe('Error Handling - Broken Assets', () => {
    const indexPath = path.resolve(__dirname, '../index.html');
    const pageUrl = `file://${indexPath}`;

    test('TC1: Page remains functional and alt text is displayed when logo.gif fails to load', async ({ page }) => {
        // Block logo.gif from loading
        await page.route('**/logo.gif', (route) => {
            route.abort('failed');
        });

        // Navigate to the page
        await page.goto(pageUrl, { waitUntil: 'load' });

        // Verify page remains functional - key sections are visible
        await expect(page.locator('.hero')).toBeVisible();
        await expect(page.locator('h1')).toContainText('MirDB');
        await expect(page.locator('.tagline')).toBeVisible();

        // Verify CTA buttons are functional
        const getStartedBtn = page.locator('.btn-primary');
        await expect(getStartedBtn).toBeVisible();
        await expect(getStartedBtn).toHaveAttribute('href', '#quick-start');

        const githubBtn = page.locator('.btn-secondary');
        await expect(githubBtn).toBeVisible();
        await expect(githubBtn).toHaveAttribute('href', 'https://github.com/yetone/mirdb');

        // Verify the logo image element exists with proper alt text
        const logoImg = page.locator('img.logo');
        await expect(logoImg).toBeVisible();

        // Get alt text - should be displayed as fallback when image fails
        const altText = await logoImg.getAttribute('alt');
        expect(altText).toBeTruthy();
        expect(altText.length).toBeGreaterThan(0);
        expect(altText.toLowerCase()).toMatch(/mirdb|logo|database/i);

        // Verify other sections are still functional
        await expect(page.locator('.features')).toBeVisible();
        await expect(page.locator('#quick-start')).toBeVisible();
        await expect(page.locator('footer')).toBeVisible();

        // Verify navigation still works - click Get Started should scroll to quick-start
        await getStartedBtn.click();
        await expect(page.locator('#quick-start')).toBeInViewport();
    });

    test('TC2: Quick start section remains usable with alt text fallback when usage.gif fails to load', async ({ page }) => {
        // Block usage.gif from loading
        await page.route('**/usage.gif', (route) => {
            route.abort('failed');
        });

        // Navigate to the page
        await page.goto(pageUrl, { waitUntil: 'load' });

        // Verify quick start section is visible and functional
        const quickStartSection = page.locator('#quick-start');
        await expect(quickStartSection).toBeVisible();

        // Verify section heading
        await expect(quickStartSection.locator('h2')).toContainText('Quick Start');

        // Verify installation instructions are visible
        const installationSection = quickStartSection.locator('.installation');
        await expect(installationSection).toBeVisible();
        await expect(installationSection.locator('h3')).toContainText('Installation');
        await expect(installationSection.locator('pre code')).toBeVisible();

        // Verify usage example section is visible
        const usageSection = quickStartSection.locator('.usage');
        await expect(usageSection).toBeVisible();
        await expect(usageSection.locator('h3')).toContainText('Usage Example');
        await expect(usageSection.locator('pre code')).toBeVisible();

        // Verify the demo section is visible even without the image
        const demoSection = quickStartSection.locator('.demo');
        await expect(demoSection).toBeVisible();
        await expect(demoSection.locator('h3')).toContainText('See It In Action');

        // Verify usage.gif image element exists with proper alt text
        const usageImg = page.locator('img.usage-gif');
        await expect(usageImg).toBeVisible();

        // Get alt text - should provide meaningful fallback description
        const altText = await usageImg.getAttribute('alt');
        expect(altText).toBeTruthy();
        expect(altText.length).toBeGreaterThan(10);
        expect(altText.toLowerCase()).toMatch(/usage|demonstration|demo|terminal|command|mirdb/i);

        // Verify users can still understand the content via code examples
        const codeBlocks = quickStartSection.locator('pre code');
        const codeBlockCount = await codeBlocks.count();
        expect(codeBlockCount).toBeGreaterThanOrEqual(2);

        // Verify key commands are visible in the usage example
        const usageCode = await usageSection.locator('pre code').textContent();
        expect(usageCode).toContain('telnet');
        expect(usageCode).toContain('set');
        expect(usageCode).toContain('get');
    });

    test('TC3: No JavaScript errors occur when images fail to load', async ({ page }) => {
        // Collect JavaScript console errors
        const consoleErrors = [];
        page.on('console', (msg) => {
            if (msg.type() === 'error') {
                const text = msg.text();
                const location = msg.location();

                // Track JavaScript errors - exclude network resource failures
                // which are expected when we block images
                const isNetworkError =
                    text.includes('net::ERR_') ||
                    text.includes('Failed to load resource');

                // Only track actual JS errors, not network failures from blocked images
                if (!isNetworkError) {
                    consoleErrors.push({
                        text: text,
                        location: location
                    });
                }
            }
        });

        // Collect page errors (uncaught exceptions)
        const pageErrors = [];
        page.on('pageerror', (error) => {
            pageErrors.push({
                message: error.message,
                stack: error.stack
            });
        });

        // Block both images from loading
        await page.route('**/logo.gif', (route) => route.abort('failed'));
        await page.route('**/usage.gif', (route) => route.abort('failed'));

        // Navigate to the page
        await page.goto(pageUrl, { waitUntil: 'load' });

        // Wait for any async operations
        await page.waitForTimeout(500);

        // Interact with the page to ensure no errors during interaction
        await page.locator('.btn-primary').click();
        await page.waitForTimeout(200);

        // Scroll through the page
        await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
        await page.waitForTimeout(200);
        await page.evaluate(() => window.scrollTo(0, 0));
        await page.waitForTimeout(200);

        // Assert no JavaScript console errors
        expect(consoleErrors, 'No JavaScript console errors should occur when images fail to load').toEqual([]);

        // Assert no uncaught page errors
        expect(pageErrors, 'No uncaught JavaScript exceptions should occur when images fail to load').toEqual([]);
    });

    test('Page remains fully functional when all images fail to load', async ({ page }) => {
        // Block all images from loading
        await page.route('**/*.gif', (route) => route.abort('failed'));
        await page.route('**/*.png', (route) => route.abort('failed'));
        await page.route('**/*.jpg', (route) => route.abort('failed'));
        await page.route('**/*.jpeg', (route) => route.abort('failed'));

        // Navigate to the page
        await page.goto(pageUrl, { waitUntil: 'load' });

        // Verify all major sections are visible and functional
        await expect(page.locator('.hero')).toBeVisible();
        await expect(page.locator('.features')).toBeVisible();
        await expect(page.locator('#quick-start')).toBeVisible();
        await expect(page.locator('.commands')).toBeVisible();
        await expect(page.locator('.status')).toBeVisible();
        await expect(page.locator('footer')).toBeVisible();

        // Verify all headings are visible
        await expect(page.locator('h1')).toContainText('MirDB');
        await expect(page.locator('.features h2')).toContainText('Key Features');
        await expect(page.locator('#quick-start h2')).toContainText('Quick Start');
        await expect(page.locator('.commands h2')).toContainText('Supported Commands');
        await expect(page.locator('.status h2')).toContainText('Project Status');

        // Verify feature cards are visible
        const featureCards = page.locator('.feature-card');
        await expect(featureCards).toHaveCount(3);

        // Verify links are still functional
        const footerLinks = page.locator('footer a');
        const linkCount = await footerLinks.count();
        expect(linkCount).toBeGreaterThanOrEqual(3);

        // Verify all images have alt text for accessibility
        const allImages = page.locator('img');
        const imageCount = await allImages.count();

        for (let i = 0; i < imageCount; i++) {
            const img = allImages.nth(i);
            const altText = await img.getAttribute('alt');
            const src = await img.getAttribute('src');

            // Each image should have alt text
            expect(altText, `Image ${src} should have alt text`).toBeTruthy();
        }
    });

    test('Alt text provides meaningful description when images fail', async ({ page }) => {
        // Block all local images
        await page.route('**/logo.gif', (route) => route.abort('failed'));
        await page.route('**/usage.gif', (route) => route.abort('failed'));

        await page.goto(pageUrl, { waitUntil: 'load' });

        // Check logo alt text is descriptive
        const logoAlt = await page.locator('img.logo').getAttribute('alt');
        expect(logoAlt.length).toBeGreaterThan(10);
        expect(logoAlt.toLowerCase()).toContain('mirdb');

        // Check usage gif alt text is descriptive
        const usageAlt = await page.locator('img.usage-gif').getAttribute('alt');
        expect(usageAlt.length).toBeGreaterThan(10);
        expect(usageAlt.toLowerCase()).toMatch(/usage|demonstration|terminal|command/i);

        // Verify alt text is not just the filename
        expect(logoAlt.toLowerCase()).not.toBe('logo');
        expect(logoAlt.toLowerCase()).not.toBe('logo.gif');
        expect(usageAlt.toLowerCase()).not.toBe('usage');
        expect(usageAlt.toLowerCase()).not.toBe('usage.gif');
    });
});
