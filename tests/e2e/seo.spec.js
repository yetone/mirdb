/**
 * SEO E2E Tests
 * Owner: Scenario 10 - SEO and Meta Tags
 *
 * Tests for:
 * - Title tag
 * - Meta description
 * - Open Graph tags
 * - Viewport meta
 * - Semantic HTML structure
 */

const { test, expect } = require('@playwright/test');

test.describe('SEO and Meta Tags', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
    });

    test('TC1: Page has title tag containing MirDB', async ({ page }) => {
        const title = await page.title();
        expect(title).toContain('MirDB');
    });

    test('TC2: Meta description exists with meaningful content about MirDB', async ({ page }) => {
        const metaDescription = await page.locator('meta[name="description"]').getAttribute('content');
        expect(metaDescription).toBeTruthy();
        expect(metaDescription.toLowerCase()).toContain('mirdb');
        expect(metaDescription.length).toBeGreaterThan(50);
    });

    test('TC3: Open Graph title meta tag is present', async ({ page }) => {
        const ogTitle = await page.locator('meta[property="og:title"]').getAttribute('content');
        expect(ogTitle).toBeTruthy();
        expect(ogTitle).toContain('MirDB');
    });

    test('TC4: Open Graph description meta tag is present', async ({ page }) => {
        const ogDescription = await page.locator('meta[property="og:description"]').getAttribute('content');
        expect(ogDescription).toBeTruthy();
        expect(ogDescription.length).toBeGreaterThan(20);
    });

    test('TC5: Viewport meta tag is set for responsive design', async ({ page }) => {
        const viewport = await page.locator('meta[name="viewport"]').getAttribute('content');
        expect(viewport).toBeTruthy();
        expect(viewport).toContain('width=device-width');
    });

    test('Page has proper semantic HTML structure', async ({ page }) => {
        // Check for main landmark
        const main = page.locator('main');
        await expect(main).toBeVisible();

        // Check for header landmark
        const header = page.locator('header');
        await expect(header).toBeVisible();

        // Check for footer landmark
        const footer = page.locator('footer');
        await expect(footer).toBeVisible();

        // Check for proper heading hierarchy - h1 should exist
        const h1 = page.locator('h1');
        await expect(h1).toHaveCount(1);

        // Check for nav element
        const nav = page.locator('nav');
        await expect(nav).toBeVisible();
    });

    test('Page has lang attribute on html element', async ({ page }) => {
        const lang = await page.locator('html').getAttribute('lang');
        expect(lang).toBe('en');
    });

    test('Page has charset meta tag', async ({ page }) => {
        const charset = await page.locator('meta[charset]').getAttribute('charset');
        expect(charset.toLowerCase()).toBe('utf-8');
    });

    test('All sections have proper id attributes for anchor links', async ({ page }) => {
        // Check main sections have IDs for SEO-friendly navigation
        await expect(page.locator('#hero')).toBeVisible();
        await expect(page.locator('#features')).toBeVisible();
        await expect(page.locator('#quickstart')).toBeVisible();
        await expect(page.locator('#configuration')).toBeVisible();
    });

    test('Open Graph type meta tag is present', async ({ page }) => {
        const ogType = await page.locator('meta[property="og:type"]').getAttribute('content');
        expect(ogType).toBeTruthy();
    });

    test('External links have rel noopener noreferrer for security', async ({ page }) => {
        const externalLinks = page.locator('a[target="_blank"]');
        const count = await externalLinks.count();

        for (let i = 0; i < count; i++) {
            const rel = await externalLinks.nth(i).getAttribute('rel');
            expect(rel).toContain('noopener');
        }
    });
});
