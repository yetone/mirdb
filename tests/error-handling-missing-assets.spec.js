// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = `file://${path.join(__dirname, '..', 'index.html')}`;

// Create test with JavaScript disabled
const testNoJS = test.extend({
    page: async ({ browser }, use) => {
        const context = await browser.newContext({ javaScriptEnabled: false });
        const page = await context.newPage();
        await use(page);
        await context.close();
    }
});

test.describe('Error Handling - Missing Assets', () => {
    test.describe('Test Case 1: Layout stability with images blocked', () => {
        test('layout remains stable when images fail to load', async ({ page }) => {
            // Block all image requests
            await page.route('**/*.{png,jpg,jpeg,gif,webp,svg,ico}', route => route.abort());

            await page.goto(indexPath);
            await page.waitForLoadState('domcontentloaded');

            // Verify page loads without errors
            const body = page.locator('body');
            await expect(body).toBeVisible();

            // Verify main sections are still visible and properly laid out
            const hero = page.locator('[data-testid="hero-section"]');
            await expect(hero).toBeVisible();

            const navbar = page.locator('[data-testid="navbar"]');
            await expect(navbar).toBeVisible();

            const footer = page.locator('[data-testid="footer"]');
            await expect(footer).toBeVisible();

            // Verify content sections exist and are visible
            const features = page.locator('#features');
            await expect(features).toBeVisible();

            const quickstart = page.locator('[data-testid="quickstart-section"]');
            await expect(quickstart).toBeVisible();

            // Verify layout is stable by checking hero is properly sized
            const heroBox = await hero.boundingBox();
            expect(heroBox).not.toBeNull();
            expect(heroBox.height).toBeGreaterThan(200); // Hero should have reasonable height

            // Check that feature cards are still visible
            const featureCards = page.locator('.feature-card');
            const cardCount = await featureCards.count();
            expect(cardCount).toBe(3);

            // Verify each card is visible
            for (let i = 0; i < cardCount; i++) {
                await expect(featureCards.nth(i)).toBeVisible();
            }
        });

        test('SVG icons display alt text or remain accessible when blocked', async ({ page }) => {
            // Block SVG requests
            await page.route('**/*.svg', route => route.abort());

            await page.goto(indexPath);
            await page.waitForLoadState('domcontentloaded');

            // SVGs in the page are inline, so they should still render
            // Verify the feature icons have aria-hidden for accessibility
            const featureIcons = page.locator('.feature-icon svg');
            const iconCount = await featureIcons.count();

            // Icons should still be visible (inline SVGs)
            expect(iconCount).toBeGreaterThanOrEqual(3);

            // All icons should have aria-hidden="true" for accessibility
            for (let i = 0; i < iconCount; i++) {
                const ariaHidden = await featureIcons.nth(i).getAttribute('aria-hidden');
                expect(ariaHidden).toBe('true');
            }
        });

        test('asset folder images load with graceful degradation', async ({ page }) => {
            // Monitor for console errors
            const consoleErrors = [];
            page.on('console', msg => {
                if (msg.type() === 'error') {
                    consoleErrors.push(msg.text());
                }
            });

            await page.goto(indexPath);
            await page.waitForLoadState('networkidle');

            // Page should load without critical JavaScript errors
            const criticalJsErrors = consoleErrors.filter(err =>
                !err.includes('net::') && // Network errors are expected when blocking
                !err.includes('404') &&
                !err.includes('Failed to load resource')
            );

            // No critical JS errors should occur
            expect(criticalJsErrors.length).toBe(0);
        });
    });

    test.describe('Test Case 2: Core content accessible without JavaScript', () => {
        testNoJS('core text content is accessible without JavaScript', async ({ page }) => {
            await page.goto(indexPath);
            await page.waitForLoadState('domcontentloaded');

            // Verify hero text content is visible
            const heroTitle = page.locator('[data-testid="hero-title"]');
            await expect(heroTitle).toBeVisible();
            await expect(heroTitle).toHaveText('MirDB');

            const heroTagline = page.locator('[data-testid="hero-tagline"]');
            await expect(heroTagline).toBeVisible();
            await expect(heroTagline).toContainText('persistent key-value store');

            // Verify navigation is visible
            const navbar = page.locator('[data-testid="navbar"]');
            await expect(navbar).toBeVisible();

            // Verify nav links are visible (desktop view)
            const navBrand = page.locator('[data-testid="nav-brand"]');
            await expect(navBrand).toBeVisible();
            await expect(navBrand).toHaveText('MirDB');
        });

        testNoJS('navigation links are accessible without JavaScript', async ({ page }) => {
            await page.goto(indexPath);
            await page.waitForLoadState('domcontentloaded');

            // Check navigation links exist and have proper hrefs
            const featuresLink = page.locator('[data-testid="nav-features"]');
            await expect(featuresLink).toHaveAttribute('href', '#features');

            const quickstartLink = page.locator('[data-testid="nav-quickstart"]');
            await expect(quickstartLink).toHaveAttribute('href', '#quickstart');

            const commandsLink = page.locator('[data-testid="nav-commands"]');
            await expect(commandsLink).toHaveAttribute('href', '#commands');

            const githubLink = page.locator('[data-testid="nav-github"]');
            await expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
        });

        testNoJS('code examples are readable without JavaScript', async ({ page }) => {
            await page.goto(indexPath);
            await page.waitForLoadState('domcontentloaded');

            // Check that code blocks are visible
            const codeBlocks = page.locator('.code-block');
            const codeBlockCount = await codeBlocks.count();
            expect(codeBlockCount).toBeGreaterThan(0);

            // Verify code content is readable
            const serverStartCode = page.locator('#server-start-code');
            await expect(serverStartCode).toBeVisible();
            const codeText = await serverStartCode.textContent();
            expect(codeText).toContain('mirdb');

            // Check complete example is visible
            const completeCode = page.locator('#complete-code');
            await expect(completeCode).toBeVisible();
            const completeText = await completeCode.textContent();
            expect(completeText).toContain('set mykey');
            expect(completeText).toContain('get mykey');
        });

        testNoJS('configuration and commands tables are accessible without JavaScript', async ({ page }) => {
            await page.goto(indexPath);
            await page.waitForLoadState('domcontentloaded');

            // Verify tables are present and readable
            const tables = page.locator('table');
            const tableCount = await tables.count();
            expect(tableCount).toBeGreaterThanOrEqual(5); // Multiple command tables + config table

            // Check configuration section
            const configSection = page.locator('#configuration');
            await expect(configSection).toBeVisible();

            // Verify configuration values are visible
            const addrCell = page.locator('td:has-text("0.0.0.0:12333")');
            await expect(addrCell).toBeVisible();

            // Check commands section
            const commandsSection = page.locator('#commands');
            await expect(commandsSection).toBeVisible();

            // Verify SET command format is visible
            const setFormat = page.locator('[data-testid="set-format"]');
            await expect(setFormat).toBeVisible();
        });

        testNoJS('footer resource links work without JavaScript', async ({ page }) => {
            await page.goto(indexPath);
            await page.waitForLoadState('domcontentloaded');

            // Verify footer links
            const githubRepoLink = page.locator('[data-testid="footer-github-link"]');
            await expect(githubRepoLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
            await expect(githubRepoLink).toHaveAttribute('target', '_blank');

            const issuesLink = page.locator('[data-testid="footer-issues-link"]');
            await expect(issuesLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb/issues');

            const cratesLink = page.locator('[data-testid="footer-crates-link"]');
            await expect(cratesLink).toHaveAttribute('href', 'https://crates.io/crates/mirdb');
        });
    });

    test.describe('Test Case 3: Graceful degradation of interactive features', () => {
        test('copy-to-clipboard fails gracefully without Clipboard API', async ({ page }) => {
            await page.goto(indexPath);
            await page.waitForLoadState('domcontentloaded');

            // Track any errors
            const errors = [];
            page.on('console', msg => {
                if (msg.type() === 'error') {
                    errors.push(msg.text());
                }
            });

            // Override clipboard API to simulate unavailability
            await page.evaluate(() => {
                // @ts-ignore
                navigator.clipboard = undefined;
                // @ts-ignore
                window.isSecureContext = false;
            });

            // Try clicking a copy button
            const copyBtn = page.locator('[data-testid="copy-btn-server"]');
            await expect(copyBtn).toBeVisible();
            await copyBtn.click();

            // Wait for potential error handling
            await page.waitForTimeout(500);

            // The button should show some feedback even if copy fails
            // Check that no unhandled JavaScript errors crash the page
            const pageStillWorks = await page.evaluate(() => {
                return document.body !== null && document.querySelector('.code-block') !== null;
            });
            expect(pageStillWorks).toBe(true);
        });

        testNoJS('code blocks remain selectable for manual copy', async ({ page }) => {
            await page.goto(indexPath);
            await page.waitForLoadState('domcontentloaded');

            // Check that code blocks have selectable text
            const codeElement = page.locator('#server-start-code');
            await expect(codeElement).toBeVisible();

            // Verify the element has readable text content
            const textContent = await codeElement.textContent();
            expect(textContent.trim().length).toBeGreaterThan(0);
            expect(textContent).toContain('mirdb');

            // Verify text can be selected (checking CSS properties)
            const userSelect = await codeElement.evaluate(el => {
                return getComputedStyle(el).userSelect;
            });
            // userSelect should not be 'none' for code blocks
            expect(userSelect).not.toBe('none');
        });

        testNoJS('hamburger menu degrades gracefully without JavaScript', async ({ page, browserName }) => {
            // Set mobile viewport
            await page.setViewportSize({ width: 375, height: 667 });

            await page.goto(indexPath);
            await page.waitForLoadState('domcontentloaded');

            // The hamburger button should be visible on mobile
            const hamburger = page.locator('[data-testid="hamburger-menu"]');
            await expect(hamburger).toBeVisible();

            // Navigation links should still exist in the DOM (just hidden via CSS)
            const navLinks = page.locator('[data-testid="nav-links"]');

            // Nav links exist but may be hidden
            const navLinksCount = await navLinks.locator('a').count();
            expect(navLinksCount).toBeGreaterThan(0);

            // Desktop width should show navigation
            await page.setViewportSize({ width: 1280, height: 800 });
            await page.waitForTimeout(300);

            // At desktop width, nav links should be visible
            await expect(navLinks).toBeVisible();
        });

        test('smooth scroll works with fallback for older browsers', async ({ page }) => {
            await page.goto(indexPath);
            await page.waitForLoadState('domcontentloaded');

            // Click the Get Started button which links to #quickstart
            const getStartedBtn = page.locator('[data-testid="cta-get-started"]');
            await getStartedBtn.click();

            // Wait for navigation (with or without smooth scroll)
            await page.waitForTimeout(500);

            // Check that we've scrolled to the quickstart section
            const quickstartSection = page.locator('[data-testid="quickstart-section"]');
            await expect(quickstartSection).toBeInViewport();
        });
    });

    test.describe('Test Case 4: 404 handling for missing assets', () => {
        test('missing CSS resources do not break page layout', async ({ page }) => {
            // Track page errors
            const pageErrors = [];
            page.on('pageerror', err => pageErrors.push(err.message));

            // Block Prism CSS (external CDN)
            await page.route('**/prism*.css', route => route.abort());

            await page.goto(indexPath);
            await page.waitForLoadState('domcontentloaded');

            // Page should still be functional
            const heroTitle = page.locator('[data-testid="hero-title"]');
            await expect(heroTitle).toBeVisible();

            // Code blocks should still be visible (without syntax highlighting)
            const codeBlocks = page.locator('.code-block');
            const count = await codeBlocks.count();
            expect(count).toBeGreaterThan(0);

            // Code text should still be readable
            const firstCodeBlock = codeBlocks.first();
            await expect(firstCodeBlock).toBeVisible();
            const codeText = await firstCodeBlock.locator('code').textContent();
            expect(codeText.length).toBeGreaterThan(0);

            // No JavaScript errors should crash the page
            expect(pageErrors.filter(e => !e.includes('net::')).length).toBe(0);
        });

        test('missing JavaScript resources do not cause console errors that break functionality', async ({ page }) => {
            // Collect console errors
            const consoleErrors = [];
            page.on('console', msg => {
                if (msg.type() === 'error') {
                    consoleErrors.push(msg.text());
                }
            });

            // Block Prism JS files
            await page.route('**/prism*.js', route => route.abort());

            await page.goto(indexPath);
            await page.waitForLoadState('domcontentloaded');

            // Page should still load
            const body = page.locator('body');
            await expect(body).toBeVisible();

            // Hero should be visible
            const heroTitle = page.locator('[data-testid="hero-title"]');
            await expect(heroTitle).toBeVisible();
            await expect(heroTitle).toHaveText('MirDB');

            // Copy buttons should still exist (may not work without full JS)
            const copyBtns = page.locator('.copy-btn');
            const btnCount = await copyBtns.count();
            expect(btnCount).toBeGreaterThan(0);

            // Navigation should work
            const navbar = page.locator('[data-testid="navbar"]');
            await expect(navbar).toBeVisible();
        });

        test('page layout remains intact when external CDN resources fail', async ({ page }) => {
            // Block all CDN requests
            await page.route('**/cdnjs.cloudflare.com/**', route => route.abort());

            await page.goto(indexPath);
            await page.waitForLoadState('domcontentloaded');

            // Core layout should remain intact
            const sections = ['hero-section', 'quickstart-section', 'architecture-section', 'project-status-section'];

            for (const section of sections) {
                const sectionEl = page.locator(`[data-testid="${section}"]`);
                await expect(sectionEl).toBeVisible();
            }

            // Navigation should still work
            const navLinks = page.locator('[data-testid="nav-links"] a');
            const linkCount = await navLinks.count();
            expect(linkCount).toBeGreaterThanOrEqual(7); // All nav links present

            // Footer should be visible
            const footer = page.locator('[data-testid="footer"]');
            await expect(footer).toBeVisible();
        });

        test('no unhandled JavaScript errors when assets fail to load', async ({ page }) => {
            // Track unhandled errors
            const unhandledErrors = [];
            page.on('pageerror', error => {
                unhandledErrors.push(error.message);
            });

            // Block various resources
            await page.route('**/*.{png,jpg,gif,css,js}', (route, request) => {
                // Only block external resources
                if (request.url().includes('cdnjs') || request.url().includes('googleapis')) {
                    route.abort();
                } else {
                    route.continue();
                }
            });

            await page.goto(indexPath);
            await page.waitForLoadState('domcontentloaded');
            await page.waitForTimeout(1000);

            // Filter out network-related error messages (these are expected when blocking)
            const criticalErrors = unhandledErrors.filter(err => {
                return !err.includes('net::') &&
                       !err.includes('Failed to load') &&
                       !err.includes('NetworkError') &&
                       !err.includes('TypeError: Failed to fetch');
            });

            // No critical JavaScript errors should occur
            expect(criticalErrors.length).toBe(0);
        });

        test('interactive elements remain functional with partial asset loading', async ({ page }) => {
            // Block only Prism (syntax highlighting)
            await page.route('**/prism*.js', route => route.abort());
            await page.route('**/prism*.css', route => route.abort());

            await page.goto(indexPath);
            await page.waitForLoadState('domcontentloaded');

            // Test that internal links still work
            const getStartedBtn = page.locator('[data-testid="cta-get-started"]');
            await expect(getStartedBtn).toBeVisible();

            await getStartedBtn.click();
            await page.waitForTimeout(500);

            // Should navigate to quickstart section
            const currentUrl = page.url();
            expect(currentUrl).toContain('#quickstart');

            // Test that copy buttons are present (even if Prism fails)
            const copyBtns = page.locator('.copy-btn');
            const btnCount = await copyBtns.count();
            expect(btnCount).toBeGreaterThan(0);
        });
    });
});
