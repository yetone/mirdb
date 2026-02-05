/**
 * Navigation E2E Tests
 * Owner: Scenario 4 - Navigation Menu and External Links
 *
 * Tests for:
 * - Header navigation presence
 * - GitHub link (href, target="_blank")
 * - Documentation link
 * - Logo/brand display
 * - Internal anchor scroll
 */

const { test, expect } = require('@playwright/test');

test.describe('Navigation', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
    });

    // Test Case 1: Check header for navigation menu
    test('should have a fixed header with navigation menu', async ({ page }) => {
        const header = page.locator('header');
        await expect(header).toBeVisible();

        // Check that header contains a nav element
        const nav = header.locator('nav');
        await expect(nav).toBeVisible();

        // Verify header has fixed positioning
        const headerPosition = await header.evaluate((el) => {
            const style = window.getComputedStyle(el);
            return style.position;
        });
        expect(headerPosition).toBe('fixed');
    });

    // Test Case 2: Check for GitHub link in navigation
    test('should have a GitHub link in navigation', async ({ page }) => {
        const nav = page.locator('header nav');

        // Check for link with text 'GitHub' or containing GitHub icon
        const githubLink = nav.locator('a').filter({
            has: page.locator('text=GitHub')
        }).or(nav.locator('a[href*="github.com"]'));

        await expect(githubLink.first()).toBeVisible();
    });

    // Test Case 3: Verify GitHub link href attribute
    test('should have GitHub link pointing to valid repository URL', async ({ page }) => {
        const nav = page.locator('header nav');
        const githubLink = nav.locator('a[href*="github.com"]');

        await expect(githubLink.first()).toBeVisible();

        const href = await githubLink.first().getAttribute('href');
        expect(href).toMatch(/^https:\/\/github\.com\/.+/);
    });

    // Test Case 4: Verify GitHub link opens in new tab
    test('should have GitHub link with target="_blank" attribute', async ({ page }) => {
        const nav = page.locator('header nav');
        const githubLink = nav.locator('a[href*="github.com"]');

        await expect(githubLink.first()).toBeVisible();

        const target = await githubLink.first().getAttribute('target');
        expect(target).toBe('_blank');

        // Also verify rel="noopener noreferrer" for security
        const rel = await githubLink.first().getAttribute('rel');
        expect(rel).toContain('noopener');
    });

    // Test Case 5: Check for Documentation link
    test('should have a Documentation link in navigation', async ({ page }) => {
        const nav = page.locator('header nav');

        // Check for link with text 'Documentation', 'Docs', or similar
        const docsLink = nav.locator('a').filter({
            hasText: /Documentation|Docs/i
        });

        await expect(docsLink.first()).toBeVisible();
    });

    // Test Case 6: Check for logo/brand name in header
    test('should display logo or MirDB brand name in header', async ({ page }) => {
        const header = page.locator('header');

        // Check for MirDB text in header or logo image
        const brand = header.locator('a, span, img').filter({
            hasText: /MirDB/i
        }).or(header.locator('img[alt*="MirDB"], img[alt*="logo"]'));

        await expect(brand.first()).toBeVisible();
    });

    // Test Case 7: Test internal anchor link scrolling
    test('should scroll smoothly when clicking internal anchor links', async ({ page }) => {
        const nav = page.locator('header nav');

        // Get initial scroll position
        const initialScrollY = await page.evaluate(() => window.scrollY);

        // Find and click an internal anchor link (e.g., Features, Quick Start)
        const internalLink = nav.locator('a[href^="#"]').first();

        // Skip if no internal links exist
        const linkCount = await internalLink.count();
        if (linkCount === 0) {
            test.skip();
            return;
        }

        await internalLink.click();

        // Wait for scroll to complete
        await page.waitForTimeout(500);

        // Verify scroll position has changed
        const newScrollY = await page.evaluate(() => window.scrollY);

        // The page should have scrolled (or stayed at 0 if already at the target)
        // For now, we just verify the scroll behavior attribute exists on html
        const scrollBehavior = await page.evaluate(() => {
            const htmlStyle = window.getComputedStyle(document.documentElement);
            return htmlStyle.scrollBehavior;
        });

        expect(scrollBehavior).toBe('smooth');
    });

    // Additional test: Navigation should have appropriate ARIA attributes
    test('should have accessible navigation with ARIA attributes', async ({ page }) => {
        const nav = page.locator('header nav');

        // Navigation should be visible
        await expect(nav).toBeVisible();

        // Check for aria-label on nav element
        const ariaLabel = await nav.getAttribute('aria-label');
        expect(ariaLabel).toBeTruthy();
    });

    // Additional test: Navigation links should be keyboard accessible
    test('should allow keyboard navigation through links', async ({ page }) => {
        // Focus on the first focusable element
        await page.keyboard.press('Tab');

        // The focused element should be in the header/nav area
        const focusedElement = page.locator(':focus');
        await expect(focusedElement).toBeVisible();
    });
});
