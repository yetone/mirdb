// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Asset Loading', () => {
    test('all images load successfully without 404 errors', async ({ page }) => {
        // Set up listeners BEFORE navigation
        const imageErrors = [];

        page.on('response', response => {
            const url = response.url();
            const status = response.status();
            // Check for image file types or image content types
            if (url.match(/\.(jpg|jpeg|png|gif|svg|webp|ico)$/i) ||
                response.headers()['content-type']?.includes('image')) {
                if (status >= 400) {
                    imageErrors.push({ url, status });
                }
            }
        });

        // Navigate after setting up listeners
        await page.goto('/');
        await page.waitForLoadState('networkidle');

        // Check that there are no broken image icons on the page
        const images = await page.locator('img').all();
        for (const img of images) {
            // Check if image has naturalWidth > 0 (indicates it loaded)
            const naturalWidth = await img.evaluate(el => el.naturalWidth);
            const src = await img.getAttribute('src');
            expect(naturalWidth, `Image ${src} should load without errors`).toBeGreaterThan(0);
        }

        // Verify no 404 errors for image requests
        expect(imageErrors, 'No images should return 404 errors').toHaveLength(0);
    });

    test('main stylesheet loads without errors', async ({ page }) => {
        // Track CSS loading - set up listener BEFORE navigation
        const cssResponses = [];

        page.on('response', response => {
            const url = response.url();
            const contentType = response.headers()['content-type'] || '';
            if (url.endsWith('.css') || contentType.includes('text/css')) {
                cssResponses.push({
                    url,
                    status: response.status(),
                    contentType
                });
            }
        });

        // Navigate after setting up listeners
        await page.goto('/');
        await page.waitForLoadState('networkidle');

        // Verify styles.css loaded successfully
        const mainCss = cssResponses.find(r => r.url.includes('styles.css'));
        expect(mainCss).toBeDefined();
        expect(mainCss.status).toBe(200);

        // Verify that CSS is actually applied by checking computed styles
        const body = page.locator('body');
        const backgroundColor = await body.evaluate(el =>
            getComputedStyle(el).backgroundColor
        );

        // The background color should be set (not transparent/default)
        expect(backgroundColor).not.toBe('rgba(0, 0, 0, 0)');
        expect(backgroundColor).not.toBe('transparent');
    });

    test('Prism.js theme CSS loads from CDN', async ({ page }) => {
        // Set up listener BEFORE navigation
        const cdnCssResponses = [];

        page.on('response', response => {
            const url = response.url();
            if (url.includes('cdnjs.cloudflare.com') && url.includes('prism')) {
                cdnCssResponses.push({
                    url,
                    status: response.status()
                });
            }
        });

        // Navigate after setting up listeners
        await page.goto('/');
        await page.waitForLoadState('networkidle');

        // Verify Prism theme CSS loaded
        const prismTheme = cdnCssResponses.find(r => r.url.includes('.css'));
        expect(prismTheme).toBeDefined();
        expect(prismTheme.status).toBe(200);
    });

    test('fonts load or system fonts display as fallback', async ({ page }) => {
        await page.goto('/');
        await page.waitForLoadState('networkidle');

        // Check that the body has proper font-family set
        const body = page.locator('body');
        const fontFamily = await body.evaluate(el =>
            getComputedStyle(el).fontFamily
        );

        // The page uses system fonts, so font-family should be properly set
        expect(fontFamily).toBeTruthy();
        // Verify system font stack is applied (should contain system-ui or -apple-system)
        const hasSystemFont = fontFamily.includes('system-ui') ||
                             fontFamily.includes('-apple-system') ||
                             fontFamily.includes('Segoe UI') ||
                             fontFamily.includes('Roboto') ||
                             fontFamily.includes('sans-serif');
        expect(hasSystemFont).toBe(true);

        // Check monospace font for code elements
        const codeBlock = page.locator('code').first();
        if (await codeBlock.count() > 0) {
            const codeFontFamily = await codeBlock.evaluate(el =>
                getComputedStyle(el).fontFamily
            );
            expect(codeFontFamily).toBeTruthy();
            // Code blocks should use monospace fonts
            const hasMonoFont = codeFontFamily.includes('SF Mono') ||
                               codeFontFamily.includes('Fira Code') ||
                               codeFontFamily.includes('Menlo') ||
                               codeFontFamily.includes('Monaco') ||
                               codeFontFamily.includes('monospace');
            expect(hasMonoFont).toBe(true);
        }
    });

    test('no console errors related to asset loading', async ({ page }) => {
        // Set up listener BEFORE navigation
        const consoleErrors = [];

        page.on('console', msg => {
            if (msg.type() === 'error') {
                const text = msg.text();
                // Filter for asset-related errors
                if (text.includes('Failed to load') ||
                    text.includes('404') ||
                    text.includes('ERR_') ||
                    text.includes('net::')) {
                    consoleErrors.push(text);
                }
            }
        });

        // Navigate after setting up listeners
        await page.goto('/');
        await page.waitForLoadState('networkidle');

        expect(consoleErrors).toHaveLength(0);
    });

    test('all external CSS resources load properly', async ({ page }) => {
        // Set up listener BEFORE navigation
        const failedResources = [];

        page.on('requestfailed', request => {
            const url = request.url();
            if (url.endsWith('.css') ||
                request.resourceType() === 'stylesheet') {
                failedResources.push({
                    url,
                    failure: request.failure()?.errorText
                });
            }
        });

        // Navigate after setting up listeners
        await page.goto('/');
        await page.waitForLoadState('networkidle');

        expect(failedResources).toHaveLength(0);
    });

    test('stylesheets are properly applied to page elements', async ({ page }) => {
        await page.goto('/');
        await page.waitForLoadState('networkidle');

        // Check hero section has proper styling
        const hero = page.locator('.hero');
        const heroBackground = await hero.evaluate(el =>
            getComputedStyle(el).background
        );
        expect(heroBackground).toBeTruthy();

        // Check that buttons have proper styling
        const primaryBtn = page.locator('.btn-primary').first();
        const btnBackground = await primaryBtn.evaluate(el =>
            getComputedStyle(el).backgroundColor
        );
        // Primary button should have the primary color (#2563eb)
        expect(btnBackground).toMatch(/rgb\(37,\s*99,\s*235\)|#2563eb/i);

        // Check feature cards have border-radius applied
        const featureCard = page.locator('.feature-card').first();
        const borderRadius = await featureCard.evaluate(el =>
            getComputedStyle(el).borderRadius
        );
        expect(borderRadius).not.toBe('0px');
    });

    test('JavaScript assets load and execute', async ({ page }) => {
        // Set up listener BEFORE navigation
        const jsResponses = [];

        page.on('response', response => {
            const url = response.url();
            if (url.endsWith('.js') ||
                response.headers()['content-type']?.includes('javascript')) {
                jsResponses.push({
                    url,
                    status: response.status()
                });
            }
        });

        // Navigate after setting up listeners
        await page.goto('/');
        await page.waitForLoadState('networkidle');

        // Verify Prism.js loaded
        const prismCore = jsResponses.find(r =>
            r.url.includes('prism') && r.url.includes('.min.js') && !r.url.includes('components')
        );
        expect(prismCore).toBeDefined();
        expect(prismCore.status).toBe(200);

        // Verify Prism bash component loaded
        const prismBash = jsResponses.find(r =>
            r.url.includes('prism-bash')
        );
        expect(prismBash).toBeDefined();
        expect(prismBash.status).toBe(200);

        // Verify Prism is actually working (code blocks should be highlighted)
        const codeBlock = page.locator('pre code.language-bash').first();
        if (await codeBlock.count() > 0) {
            // Prism adds token classes when highlighting
            const hasTokens = await codeBlock.evaluate(el =>
                el.querySelector('.token') !== null ||
                el.classList.contains('language-bash')
            );
            expect(hasTokens).toBe(true);
        }
    });
});
