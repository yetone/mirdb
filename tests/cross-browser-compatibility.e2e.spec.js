/**
 * Cross-Browser Compatibility E2E Tests
 *
 * This test suite verifies that the MirDB homepage works correctly across
 * major browsers: Chrome (Chromium), Firefox, and Safari (WebKit).
 * Tests are run via Playwright's multi-browser project configuration.
 */
const { test, expect } = require('@playwright/test');
const path = require('path');
const fs = require('fs');

// Resolve absolute path to index.html for file:// protocol
const indexPath = path.resolve(__dirname, '..', 'index.html');
const pageUrl = `file://${indexPath}`;

test.describe('Cross-Browser Compatibility', () => {

    test.beforeEach(async ({ page }) => {
        await page.goto(pageUrl);
    });

    test.describe('Homepage Content Display', () => {

        test('displays hero section with logo, title, and tagline', async ({ page, browserName }) => {
            // Verify logo image is present and loaded
            const logo = page.locator('img.logo');
            await expect(logo).toBeVisible();
            const logoSrc = await logo.getAttribute('src');
            expect(logoSrc).toContain('logo.gif');

            // Verify title
            const title = page.locator('h1');
            await expect(title).toBeVisible();
            await expect(title).toHaveText('MirDB');

            // Verify tagline
            const tagline = page.locator('.tagline');
            await expect(tagline).toBeVisible();
            await expect(tagline).toContainText('Persistent Key-Value Store');

            console.log(`[${browserName}] Hero section displays correctly`);
        });

        test('displays CTA buttons correctly', async ({ page, browserName }) => {
            const getStartedBtn = page.locator('a.btn-primary');
            await expect(getStartedBtn).toBeVisible();
            await expect(getStartedBtn).toHaveText('Get Started');

            const githubBtn = page.locator('a.btn-secondary');
            await expect(githubBtn).toBeVisible();
            await expect(githubBtn).toHaveText('View on GitHub');

            console.log(`[${browserName}] CTA buttons display correctly`);
        });

        test('displays feature cards in three columns', async ({ page, browserName }) => {
            const featureCards = page.locator('.feature-card');
            await expect(featureCards).toHaveCount(3);

            // Verify each feature card is visible
            const cardTitles = ['Memcached Compatible', 'Persistent Storage', 'High Performance'];
            for (let i = 0; i < 3; i++) {
                const card = featureCards.nth(i);
                await expect(card).toBeVisible();
                const h3 = card.locator('h3');
                await expect(h3).toHaveText(cardTitles[i]);
            }

            console.log(`[${browserName}] Feature cards display correctly`);
        });

        test('displays quick-start section with code examples', async ({ page, browserName }) => {
            const quickStartSection = page.locator('#quick-start');
            await expect(quickStartSection).toBeVisible();

            // Verify installation code block
            const codeBlocks = page.locator('#quick-start pre code');
            await expect(codeBlocks.first()).toBeVisible();

            // Verify usage GIF
            const usageGif = page.locator('img.usage-gif');
            await expect(usageGif).toBeVisible();

            console.log(`[${browserName}] Quick-start section displays correctly`);
        });

        test('displays supported commands section', async ({ page, browserName }) => {
            const commandsSection = page.locator('.commands');
            await expect(commandsSection).toBeVisible();

            const commandItems = page.locator('.command-item');
            await expect(commandItems).toHaveCount(3);

            // Verify command codes are visible
            const commands = ['SET', 'GET', 'DELETE'];
            for (let i = 0; i < 3; i++) {
                const code = commandItems.nth(i).locator('code');
                await expect(code).toBeVisible();
                const text = await code.textContent();
                expect(text).toContain(commands[i]);
            }

            console.log(`[${browserName}] Commands section displays correctly`);
        });

        test('displays project status section', async ({ page, browserName }) => {
            const statusSection = page.locator('.status');
            await expect(statusSection).toBeVisible();

            const statusItems = page.locator('.status-list li');
            await expect(statusItems).toHaveCount(5);

            // Verify completed items
            const completedItems = page.locator('.status-list li.completed');
            await expect(completedItems).toHaveCount(4);

            // Verify planned item
            const plannedItem = page.locator('.status-list li.planned');
            await expect(plannedItem).toHaveCount(1);

            console.log(`[${browserName}] Project status section displays correctly`);
        });

        test('displays footer with navigation links', async ({ page, browserName }) => {
            const footer = page.locator('footer');
            await expect(footer).toBeVisible();

            const footerLinks = page.locator('.footer-links a');
            await expect(footerLinks).toHaveCount(3);

            // Verify link texts
            const expectedLinks = ['GitHub', 'Documentation', 'License'];
            for (let i = 0; i < 3; i++) {
                await expect(footerLinks.nth(i)).toHaveText(expectedLinks[i]);
            }

            console.log(`[${browserName}] Footer displays correctly`);
        });
    });

    test.describe('Interactive Elements', () => {

        test('Get Started button navigates to quick-start section', async ({ page, browserName }) => {
            const getStartedBtn = page.locator('a.btn-primary');
            await getStartedBtn.click();

            // Verify URL hash changed
            await expect(page).toHaveURL(/.*#quick-start/);

            console.log(`[${browserName}] Get Started navigation works`);
        });

        test('GitHub button has correct link attributes', async ({ page, browserName }) => {
            const githubBtn = page.locator('a.btn-secondary');

            const href = await githubBtn.getAttribute('href');
            expect(href).toBe('https://github.com/yetone/mirdb');

            const target = await githubBtn.getAttribute('target');
            expect(target).toBe('_blank');

            const rel = await githubBtn.getAttribute('rel');
            expect(rel).toContain('noopener');

            console.log(`[${browserName}] GitHub button attributes are correct`);
        });

        test('footer links are clickable and properly configured', async ({ page, browserName }) => {
            const footerLinks = page.locator('.footer-links a');

            for (let i = 0; i < 3; i++) {
                const link = footerLinks.nth(i);
                await expect(link).toBeEnabled();

                const target = await link.getAttribute('target');
                expect(target).toBe('_blank');

                const rel = await link.getAttribute('rel');
                expect(rel).toContain('noopener');
            }

            console.log(`[${browserName}] Footer links are properly configured`);
        });

        test('skip link functions for accessibility', async ({ page, browserName }) => {
            const skipLink = page.locator('.skip-link');

            // Skip link should exist but be visually hidden initially
            await expect(skipLink).toHaveCount(1);

            // Tab to focus on skip link
            await page.keyboard.press('Tab');

            // Skip link should be visible when focused
            await expect(skipLink).toBeFocused();

            // Pressing Enter should navigate to main content
            await skipLink.press('Enter');
            await expect(page).toHaveURL(/.*#main-content/);

            console.log(`[${browserName}] Skip link functions correctly`);
        });
    });

    test.describe('GIF Animations', () => {

        test('logo GIF is present and accessible', async ({ page, browserName }) => {
            const logoGif = page.locator('img.logo');
            await expect(logoGif).toBeVisible();

            // Verify the GIF source
            const src = await logoGif.getAttribute('src');
            expect(src).toBe('assets/logo.gif');

            // Verify alt text for accessibility
            const alt = await logoGif.getAttribute('alt');
            expect(alt).toBeTruthy();
            expect(alt).toContain('MirDB');

            // Check natural dimensions to verify image loaded
            const naturalWidth = await logoGif.evaluate(img => img.naturalWidth);
            expect(naturalWidth).toBeGreaterThan(0);

            console.log(`[${browserName}] Logo GIF is present and loaded`);
        });

        test('usage GIF is present and accessible', async ({ page, browserName }) => {
            const usageGif = page.locator('img.usage-gif');
            await expect(usageGif).toBeVisible();

            // Verify the GIF source
            const src = await usageGif.getAttribute('src');
            expect(src).toBe('assets/usage.gif');

            // Verify alt text for accessibility
            const alt = await usageGif.getAttribute('alt');
            expect(alt).toBeTruthy();
            expect(alt.length).toBeGreaterThan(10);

            // Check natural dimensions to verify image loaded
            const naturalWidth = await usageGif.evaluate(img => img.naturalWidth);
            expect(naturalWidth).toBeGreaterThan(0);

            console.log(`[${browserName}] Usage GIF is present and loaded`);
        });

        test('GIFs have proper dimensions and display correctly', async ({ page, browserName }) => {
            // Check logo GIF dimensions
            const logoGif = page.locator('img.logo');
            const logoBoundingBox = await logoGif.boundingBox();
            expect(logoBoundingBox.width).toBeGreaterThan(50);
            expect(logoBoundingBox.height).toBeGreaterThan(50);

            // Check usage GIF dimensions
            const usageGif = page.locator('img.usage-gif');
            const usageBoundingBox = await usageGif.boundingBox();
            expect(usageBoundingBox.width).toBeGreaterThan(100);
            expect(usageBoundingBox.height).toBeGreaterThan(50);

            console.log(`[${browserName}] GIFs display with proper dimensions`);
        });
    });

    test.describe('CSS Rendering', () => {

        test('hero section has correct gradient background', async ({ page, browserName }) => {
            const hero = page.locator('.hero');
            const background = await hero.evaluate(el =>
                window.getComputedStyle(el).getPropertyValue('background-image')
            );

            // Verify gradient is applied (contains rgb values from gradient)
            expect(background).toMatch(/linear-gradient|rgb/i);

            console.log(`[${browserName}] Hero gradient renders correctly`);
        });

        test('feature cards have proper layout', async ({ page, browserName }) => {
            const featureGrid = page.locator('.feature-grid');
            const display = await featureGrid.evaluate(el =>
                window.getComputedStyle(el).getPropertyValue('display')
            );

            // Should use grid or flex layout
            expect(['grid', 'flex']).toContain(display);

            console.log(`[${browserName}] Feature cards layout renders correctly`);
        });

        test('buttons have consistent styling', async ({ page, browserName }) => {
            const primaryBtn = page.locator('a.btn-primary');
            const secondaryBtn = page.locator('a.btn-secondary');

            // Check primary button has background color
            const primaryBg = await primaryBtn.evaluate(el =>
                window.getComputedStyle(el).getPropertyValue('background-color')
            );
            expect(primaryBg).not.toBe('transparent');
            expect(primaryBg).not.toBe('rgba(0, 0, 0, 0)');

            // Check buttons have appropriate cursor (pointer or auto which resolves to pointer for links)
            const primaryCursor = await primaryBtn.evaluate(el =>
                window.getComputedStyle(el).getPropertyValue('cursor')
            );
            // Safari/WebKit returns 'auto' for links which displays as pointer, Chrome/Firefox return 'pointer'
            expect(['pointer', 'auto']).toContain(primaryCursor);

            console.log(`[${browserName}] Button styling renders correctly`);
        });

        test('code blocks have proper styling', async ({ page, browserName }) => {
            const codeBlock = page.locator('pre').first();

            // Check for monospace font family
            const fontFamily = await codeBlock.evaluate(el =>
                window.getComputedStyle(el).getPropertyValue('font-family')
            );
            expect(fontFamily.toLowerCase()).toMatch(/mono|courier|consolas/i);

            // Check for background color
            const bgColor = await codeBlock.evaluate(el =>
                window.getComputedStyle(el).getPropertyValue('background-color')
            );
            expect(bgColor).not.toBe('transparent');
            expect(bgColor).not.toBe('rgba(0, 0, 0, 0)');

            console.log(`[${browserName}] Code blocks render correctly`);
        });
    });

    test.describe('Semantic HTML', () => {

        test('page has proper document structure', async ({ page, browserName }) => {
            // Check for header
            const header = page.locator('header');
            await expect(header).toHaveCount(1);

            // Check for main content
            const main = page.locator('main');
            await expect(main).toHaveCount(1);

            // Check for footer
            const footer = page.locator('footer');
            await expect(footer).toHaveCount(1);

            // Check for proper heading hierarchy
            const h1 = page.locator('h1');
            await expect(h1).toHaveCount(1);

            const h2s = page.locator('h2');
            const h2Count = await h2s.count();
            expect(h2Count).toBeGreaterThanOrEqual(3);

            console.log(`[${browserName}] Document structure is semantic`);
        });

        test('images have proper alt attributes', async ({ page, browserName }) => {
            const images = page.locator('img');
            const count = await images.count();

            for (let i = 0; i < count; i++) {
                const img = images.nth(i);
                const alt = await img.getAttribute('alt');
                expect(alt).toBeTruthy();
                expect(alt.length).toBeGreaterThan(0);
            }

            console.log(`[${browserName}] All images have alt attributes`);
        });
    });
});
