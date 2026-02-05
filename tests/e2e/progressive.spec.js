/**
 * Progressive Enhancement E2E Tests
 * Owner: Scenario 11 - Progressive Enhancement and No-JS Fallback
 *
 * Tests for:
 * - Content visibility without JS
 * - Navigation functionality without JS
 * - Mobile navigation fallback
 */

const { test, expect } = require('@playwright/test');

// Base URL for the tests
const BASE_URL = 'http://localhost:3000';

test.describe('Progressive Enhancement - No JavaScript', () => {
    // Disable JavaScript for all tests in this describe block
    test.use({ javaScriptEnabled: false });

    test('Test Case 1: Page loads and displays content without errors', async ({ page }) => {
        // Load page with JavaScript disabled
        const response = await page.goto(BASE_URL);

        // Verify page loads successfully (HTTP 200)
        expect(response.status()).toBe(200);

        // Verify no console errors by checking the page is functional
        const body = page.locator('body');
        await expect(body).toBeVisible();

        // Verify the page has content
        const mainContent = page.locator('main');
        await expect(mainContent).toBeVisible();
    });

    test('Test Case 2: Hero section with product name and tagline is visible', async ({ page }) => {
        await page.goto(BASE_URL);

        // Verify hero section exists and is visible
        const heroSection = page.locator('#hero');
        await expect(heroSection).toBeVisible();

        // Verify product name "MirDB" is visible
        const productName = page.locator('#hero h1, #hero .hero-title');
        await expect(productName).toBeVisible();
        await expect(productName).toContainText('MirDB');

        // Verify tagline is visible
        const tagline = page.locator('#hero .hero-tagline');
        await expect(tagline).toBeVisible();
        await expect(tagline).toContainText(/Persistent Key-Value/i);

        // Verify description mentions Memcached compatibility
        const description = page.locator('#hero .hero-description');
        await expect(description).toBeVisible();
        await expect(description).toContainText(/Memcached/i);
    });

    test('Test Case 3: All feature cards are visible and readable', async ({ page }) => {
        await page.goto(BASE_URL);

        // Verify features section exists and is visible
        const featuresSection = page.locator('#features');
        await expect(featuresSection).toBeVisible();

        // Verify feature cards are visible
        const featureCards = page.locator('#features .feature-card');
        const cardCount = await featureCards.count();

        // Should have at least 5 features as per PRD
        expect(cardCount).toBeGreaterThanOrEqual(5);

        // Verify each feature card has title and description
        for (let i = 0; i < cardCount; i++) {
            const card = featureCards.nth(i);
            await expect(card).toBeVisible();

            const title = card.locator('.feature-title');
            await expect(title).toBeVisible();

            const description = card.locator('.feature-description');
            await expect(description).toBeVisible();
        }

        // Verify key features are mentioned
        const featuresText = await featuresSection.textContent();
        expect(featuresText).toMatch(/Memcached/i);
        expect(featuresText).toMatch(/Persistent/i);
        expect(featuresText).toMatch(/LSM/i);
    });

    test('Test Case 4: Code examples are visible (copy button may not function)', async ({ page }) => {
        await page.goto(BASE_URL);

        // Verify quickstart section exists and is visible
        const quickstartSection = page.locator('#quickstart');
        await expect(quickstartSection).toBeVisible();

        // Verify code blocks are visible
        const codeBlocks = page.locator('#quickstart .code-block, #quickstart pre');
        const codeCount = await codeBlocks.count();
        expect(codeCount).toBeGreaterThan(0);

        // Verify code blocks have content
        for (let i = 0; i < codeCount; i++) {
            const codeBlock = codeBlocks.nth(i);
            await expect(codeBlock).toBeVisible();
            const text = await codeBlock.textContent();
            expect(text.length).toBeGreaterThan(0);
        }

        // Verify set/get commands are visible
        const quickstartText = await quickstartSection.textContent();
        expect(quickstartText).toMatch(/set/i);
        expect(quickstartText).toMatch(/get/i);

        // Copy buttons should be visible but functionality won't work without JS
        const copyButtons = page.locator('#quickstart .copy-btn');
        const copyCount = await copyButtons.count();
        expect(copyCount).toBeGreaterThan(0);
        // Buttons are visible, but clicking them won't copy without JS
    });

    test('Test Case 5: All navigation links work using standard href navigation', async ({ page }) => {
        await page.goto(BASE_URL);

        // Verify navigation exists
        const nav = page.locator('nav, header nav');
        await expect(nav).toBeVisible();

        // Verify navigation links are present
        const navLinks = page.locator('.nav-links a, .nav-menu a, nav a');
        const linkCount = await navLinks.count();
        expect(linkCount).toBeGreaterThan(0);

        // Test internal anchor links work
        const featuresLink = page.locator('a[href="#features"]');
        if (await featuresLink.count() > 0) {
            await expect(featuresLink.first()).toHaveAttribute('href', '#features');
        }

        const quickstartLink = page.locator('a[href="#quickstart"]');
        if (await quickstartLink.count() > 0) {
            await expect(quickstartLink.first()).toHaveAttribute('href', '#quickstart');
        }

        const configLink = page.locator('a[href="#configuration"]');
        if (await configLink.count() > 0) {
            await expect(configLink.first()).toHaveAttribute('href', '#configuration');
        }

        // Test external links have proper href
        const githubLink = page.locator('a[href*="github.com"]');
        if (await githubLink.count() > 0) {
            await expect(githubLink.first()).toHaveAttribute('target', '_blank');
        }

        // Test clicking an internal anchor link (should work without JS)
        const anyInternalLink = page.locator('a[href^="#"]').first();
        if (await anyInternalLink.count() > 0) {
            const href = await anyInternalLink.getAttribute('href');
            await anyInternalLink.click();
            // After clicking, the URL hash should change
            await expect(page).toHaveURL(new RegExp(href.replace('#', '#')));
        }
    });

    test('Test Case 6: Mobile navigation is accessible without JS (shows all links or CSS-only toggle)', async ({ page }) => {
        // Set mobile viewport
        await page.setViewportSize({ width: 375, height: 667 });
        await page.goto(BASE_URL);

        // Navigation links should be accessible on mobile even without JS
        // Either all links are visible directly, or there's a CSS-only fallback
        const navMenu = page.locator('.nav-menu, .nav-links, #nav-links');
        await expect(navMenu).toBeAttached();

        // In no-JS mode, the mobile menu should be visible without needing JS to open it
        // Check that the noscript fallback styles are applied and nav is accessible

        // Check if navigation links are present in the DOM
        const allNavLinks = page.locator('.nav-link');
        const navLinkCount = await allNavLinks.count();
        expect(navLinkCount).toBeGreaterThan(0);

        // With the noscript CSS fallback, the navigation should be visible
        // The noscript styles ensure nav-menu is position: static, not hidden
        // We verify by checking that the nav menu is either:
        // 1. Visible (can interact with it)
        // 2. In a static position (not off-screen)

        // Get the bounding box of the first nav link to verify it's accessible
        const firstNavLink = allNavLinks.first();
        const boundingBox = await firstNavLink.boundingBox();

        // If the nav link has a bounding box and is within viewport, it's accessible
        // The noscript fallback should position links in a visible area
        expect(boundingBox).not.toBeNull();
        if (boundingBox) {
            // Link should be on screen (x position less than viewport width)
            expect(boundingBox.x).toBeLessThan(375); // viewport width
        }
    });
});

test.describe('Progressive Enhancement - With JavaScript (baseline)', () => {
    // These tests verify normal JS functionality still works

    test('Page functions normally with JavaScript enabled', async ({ page }) => {
        await page.goto(BASE_URL);

        // Verify page loads
        const heroSection = page.locator('#hero');
        await expect(heroSection).toBeVisible();

        // Verify JS-enhanced features work (copy button)
        const copyButton = page.locator('.copy-btn').first();
        if (await copyButton.count() > 0) {
            await expect(copyButton).toBeVisible();
        }
    });
});
