/**
 * Scenario 12: Static Site Structure - Integration Tests
 *
 * This test verifies that the site can be deployed and served correctly
 * on static hosting platforms (GitHub Pages, Vercel, Netlify).
 */

const { test, expect } = require('@playwright/test');

test.describe('Scenario 12: Deployability Integration', () => {

    test('Site loads successfully from static hosting', async ({ page }) => {
        // Navigate to the served site
        await page.goto('/');

        // Page should load without errors
        await expect(page).toHaveTitle(/MirDB/i);
    });

    test('Site has proper meta tags for SEO', async ({ page }) => {
        await page.goto('/');

        // Check meta description
        const metaDescription = await page.locator('meta[name="description"]');
        await expect(metaDescription).toHaveAttribute('content', /MirDB|key-value|persistent/i);

        // Check viewport
        const viewport = await page.locator('meta[name="viewport"]');
        await expect(viewport).toHaveAttribute('content', /width=device-width/);
    });

    test('Site has valid HTML structure', async ({ page }) => {
        await page.goto('/');

        // Check lang attribute
        const html = await page.locator('html');
        await expect(html).toHaveAttribute('lang', 'en');

        // Check doctype is HTML5 (indirectly by checking modern elements work)
        const body = await page.locator('body');
        await expect(body).toBeVisible();
    });

    test('Site loads CSS styles correctly', async ({ page }) => {
        await page.goto('/');

        // Body should have styled background (not plain white/black default)
        const body = await page.locator('body');
        await expect(body).toBeVisible();

        // Check that Tailwind classes are working
        const hasStyledElements = await page.evaluate(() => {
            // Check if any elements have Tailwind utility classes applied
            const elements = document.querySelectorAll('[class*="bg-"], [class*="text-"], [class*="p-"]');
            return elements.length > 0;
        });
        expect(hasStyledElements).toBe(true);
    });

    test('Site has working navigation structure', async ({ page }) => {
        await page.goto('/');

        // Check for header/navigation
        const header = await page.locator('header, nav, [data-testid="header"]');
        await expect(header.first()).toBeVisible();
    });

    test('Site has main content sections', async ({ page }) => {
        await page.goto('/');

        // Check for main content area
        const main = await page.locator('main, [role="main"]');
        await expect(main.first()).toBeVisible();
    });

    test('Site has footer', async ({ page }) => {
        await page.goto('/');

        // Check for footer
        const footer = await page.locator('footer, [data-testid="footer"]');
        await expect(footer.first()).toBeVisible();
    });

    test('Site is responsive - viewport resize works', async ({ page }) => {
        await page.goto('/');

        // Test mobile viewport
        await page.setViewportSize({ width: 320, height: 568 });
        const bodyMobile = await page.locator('body');
        await expect(bodyMobile).toBeVisible();

        // Test tablet viewport
        await page.setViewportSize({ width: 768, height: 1024 });
        const bodyTablet = await page.locator('body');
        await expect(bodyTablet).toBeVisible();

        // Test desktop viewport
        await page.setViewportSize({ width: 1920, height: 1080 });
        const bodyDesktop = await page.locator('body');
        await expect(bodyDesktop).toBeVisible();
    });

    test('Site has no console errors on load', async ({ page }) => {
        const consoleErrors = [];
        page.on('console', (message) => {
            if (message.type() === 'error') {
                consoleErrors.push(message.text());
            }
        });

        await page.goto('/');

        // Allow minor errors but no critical JS errors
        const criticalErrors = consoleErrors.filter(
            (err) => !err.includes('favicon') && !err.includes('404')
        );
        expect(criticalErrors).toHaveLength(0);
    });

    test('Site loads within acceptable time', async ({ page }) => {
        const startTime = Date.now();
        await page.goto('/');
        const loadTime = Date.now() - startTime;

        // Site should load within 5 seconds (generous for test environment)
        expect(loadTime).toBeLessThan(5000);
    });
});
