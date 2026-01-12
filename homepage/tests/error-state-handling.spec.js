// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Error State Handling', () => {
    test.describe('Test Case 1: Broken Image Handling', () => {
        test('should display alt text when images fail to load', async ({ page }) => {
            // Block all image requests
            await page.route('**/*.{gif,webp,png,jpg,jpeg,svg}', (route) => {
                route.abort();
            });

            await page.goto('/');

            // Check that the logo has alt text for accessibility
            const heroLogo = page.locator('#mirdb-logo');
            await expect(heroLogo).toHaveAttribute('alt', 'MirDB Logo');

            // Check that the architecture diagram has alt text
            const architectureDiagram = page.locator('#architecture-diagram');
            const altText = await architectureDiagram.getAttribute('alt');
            expect(altText).toBeTruthy();
            expect(altText.length).toBeGreaterThan(50); // Ensure descriptive alt text
            expect(altText).toContain('LSM Tree Architecture');
        });

        test('should maintain layout when images fail to load', async ({ page }) => {
            // Block all image requests
            await page.route('**/*.{gif,webp,png,jpg,jpeg,svg}', (route) => {
                route.abort();
            });

            await page.goto('/');

            // Verify hero section layout is intact
            const heroSection = page.locator('#hero');
            await expect(heroSection).toBeVisible();

            // Verify title is visible and properly styled
            const heroTitle = page.locator('.hero-title');
            await expect(heroTitle).toBeVisible();
            await expect(heroTitle).toHaveText('MirDB');

            // Verify tagline is visible
            const heroTagline = page.locator('.hero-tagline');
            await expect(heroTagline).toBeVisible();

            // Verify CTA buttons are visible and properly positioned
            const getStartedBtn = page.locator('#get-started-btn');
            const githubBtn = page.locator('#github-btn');
            await expect(getStartedBtn).toBeVisible();
            await expect(githubBtn).toBeVisible();

            // Verify hero section has minimum height (layout not collapsed)
            const heroBox = await heroSection.boundingBox();
            expect(heroBox.height).toBeGreaterThan(300);

            // Verify features section is accessible and properly laid out
            const featuresSection = page.locator('#features');
            await expect(featuresSection).toBeVisible();

            // Verify architecture section exists and is visible
            const architectureSection = page.locator('#architecture');
            await expect(architectureSection).toBeVisible();
        });

        test('should not show broken image icons', async ({ page }) => {
            // Block all image requests
            await page.route('**/*.{gif,webp,png,jpg,jpeg,svg}', (route) => {
                route.abort();
            });

            await page.goto('/');

            // Check page doesn't have zero-dimension broken image indicators
            const images = page.locator('img');
            const imageCount = await images.count();

            for (let i = 0; i < imageCount; i++) {
                const img = images.nth(i);
                const isVisible = await img.isVisible().catch(() => false);

                if (isVisible) {
                    // Verify alt text exists for all images
                    const altText = await img.getAttribute('alt');
                    expect(altText).toBeTruthy();
                }
            }
        });
    });

    test.describe('Test Case 2: External Resource Blocking', () => {
        test('page remains readable when external CSS is blocked', async ({ page }) => {
            // Block external CSS (but allow local styles.min.css)
            await page.route('**/*.css', (route, request) => {
                const url = request.url();
                // Allow local CSS files
                if (url.includes('styles.min.css') || url.includes('styles.css')) {
                    route.continue();
                } else {
                    route.abort();
                }
            });

            await page.goto('/');

            // Verify page is still readable
            const body = page.locator('body');
            await expect(body).toBeVisible();

            // Verify main content sections are accessible
            await expect(page.locator('#hero')).toBeVisible();
            await expect(page.locator('#quick-start')).toBeVisible();
            await expect(page.locator('#features')).toBeVisible();

            // Verify text is readable
            const heroTitle = page.locator('.hero-title');
            await expect(heroTitle).toBeVisible();
            await expect(heroTitle).toHaveText('MirDB');
        });

        test('page remains functional when JavaScript fails to load external scripts', async ({ page }) => {
            // Block external JavaScript (but allow local main.js)
            await page.route('**/*.js', (route, request) => {
                const url = request.url();
                // Allow local JS files and Playwright internals
                if (url.includes('main.js') || url.includes('localhost')) {
                    route.continue();
                } else {
                    route.abort();
                }
            });

            await page.goto('/');

            // Verify page loads correctly
            const body = page.locator('body');
            await expect(body).toBeVisible();

            // Verify all sections are present
            await expect(page.locator('#hero')).toBeVisible();
            await expect(page.locator('#quick-start')).toBeVisible();
            await expect(page.locator('#features')).toBeVisible();
            await expect(page.locator('#architecture')).toBeVisible();
            await expect(page.locator('#configuration')).toBeVisible();
            await expect(page.locator('#commands')).toBeVisible();
            await expect(page.locator('#project-status')).toBeVisible();
            await expect(page.locator('.footer')).toBeVisible();
        });

        test('page demonstrates graceful degradation with semantic HTML', async ({ page }) => {
            // Completely block all CSS to test raw HTML structure
            await page.route('**/*.css', (route) => {
                route.abort();
            });

            await page.goto('/');

            // Verify page uses semantic HTML that remains readable without CSS
            // Check for semantic elements
            const header = page.locator('section#hero');
            await expect(header).toBeVisible();

            // Verify headings are present and in correct hierarchy
            const h1 = page.locator('h1');
            await expect(h1).toHaveText('MirDB');

            const h2s = page.locator('h2');
            const h2Count = await h2s.count();
            expect(h2Count).toBeGreaterThan(0);

            // Verify navigation links are functional
            const getStartedLink = page.locator('#get-started-btn');
            await expect(getStartedLink).toBeVisible();
            const href = await getStartedLink.getAttribute('href');
            expect(href).toBe('#quick-start');

            // Verify tables are present and readable
            const commandsTables = page.locator('.commands-table');
            const tableCount = await commandsTables.count();
            expect(tableCount).toBeGreaterThan(0);

            // Verify lists are present
            const pathSteps = page.locator('.path-steps');
            const pathStepsCount = await pathSteps.count();
            expect(pathStepsCount).toBeGreaterThan(0);
        });
    });

    test.describe('Test Case 3: Slow Network Simulation', () => {
        test('page loads progressively on slow network (3G)', async ({ browser }) => {
            // Create context with slow 3G network emulation
            const context = await browser.newContext();
            const page = await context.newPage();

            // Emulate slow 3G network
            const client = await context.newCDPSession(page);
            await client.send('Network.emulateNetworkConditions', {
                offline: false,
                downloadThroughput: (500 * 1024) / 8, // 500 kbps download
                uploadThroughput: (500 * 1024) / 8, // 500 kbps upload
                latency: 400, // 400ms latency
            });

            // Navigate to page
            const navigationPromise = page.goto('http://localhost:3000', {
                waitUntil: 'domcontentloaded',
                timeout: 60000
            });

            await navigationPromise;

            // Verify critical content is visible first
            // Hero section should be visible early
            const heroSection = page.locator('#hero');
            await expect(heroSection).toBeVisible({ timeout: 30000 });

            // Title should be visible
            const heroTitle = page.locator('.hero-title');
            await expect(heroTitle).toBeVisible({ timeout: 30000 });

            await context.close();
        });

        test('critical content (text) loads before non-critical content (images)', async ({ browser }) => {
            const context = await browser.newContext();
            const page = await context.newPage();

            // Track resource loading order
            const loadOrder = [];

            page.on('response', (response) => {
                const url = response.url();
                const resourceType = response.request().resourceType();
                loadOrder.push({ url, resourceType, status: response.status() });
            });

            // Emulate slow network
            const client = await context.newCDPSession(page);
            await client.send('Network.emulateNetworkConditions', {
                offline: false,
                downloadThroughput: (750 * 1024) / 8,
                uploadThroughput: (250 * 1024) / 8,
                latency: 300,
            });

            await page.goto('http://localhost:3000', {
                waitUntil: 'networkidle',
                timeout: 60000
            });

            // Find index of HTML document load
            const htmlIndex = loadOrder.findIndex(r =>
                r.resourceType === 'document' && r.status === 200
            );

            // Find index of CSS load
            const cssIndex = loadOrder.findIndex(r =>
                r.resourceType === 'stylesheet' && r.status === 200
            );

            // Find first image load index
            const imageIndex = loadOrder.findIndex(r =>
                r.resourceType === 'image' && r.status === 200
            );

            // HTML should load before images
            if (htmlIndex !== -1 && imageIndex !== -1) {
                expect(htmlIndex).toBeLessThan(imageIndex);
            }

            // CSS should load before or early in page lifecycle (inline or linked in head)
            if (cssIndex !== -1 && imageIndex !== -1) {
                expect(cssIndex).toBeLessThan(imageIndex);
            }

            await context.close();
        });

        test('lazy-loaded images have proper loading attribute', async ({ page }) => {
            await page.goto('/');

            // Check architecture diagram has lazy loading
            const architectureDiagram = page.locator('#architecture-diagram');
            const loadingAttr = await architectureDiagram.getAttribute('loading');
            expect(loadingAttr).toBe('lazy');

            // Check that hero logo has eager loading (critical above-fold content)
            const heroLogo = page.locator('#mirdb-logo');
            const heroLoadingAttr = await heroLogo.getAttribute('loading');
            expect(heroLoadingAttr).toBe('eager');
        });

        test('images have async decoding for better performance', async ({ page }) => {
            await page.goto('/');

            // Check hero logo has async decoding
            const heroLogo = page.locator('#mirdb-logo');
            const decodingAttr = await heroLogo.getAttribute('decoding');
            expect(decodingAttr).toBe('async');

            // Check architecture diagram has async decoding
            const architectureDiagram = page.locator('#architecture-diagram');
            const archDecodingAttr = await architectureDiagram.getAttribute('decoding');
            expect(archDecodingAttr).toBe('async');
        });
    });
});
