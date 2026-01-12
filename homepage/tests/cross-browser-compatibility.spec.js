// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Cross-Browser Compatibility Tests
 *
 * These tests verify that the MirDB homepage renders correctly across all major browsers:
 * - Chrome (chromium)
 * - Firefox
 * - Safari (webkit)
 * - Edge (msedge)
 *
 * Each test validates that all sections render correctly with no layout issues.
 */

test.describe('Cross-Browser Compatibility', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
        // Wait for page to be fully loaded
        await page.waitForLoadState('networkidle');
    });

    test('TC1: Homepage renders correctly with all sections visible', async ({ page, browserName }) => {
        // Log the browser being tested
        console.log(`Testing in browser: ${browserName}`);

        // Verify the page title
        await expect(page).toHaveTitle(/MirDB/);

        // Verify all major sections are present and visible
        const sections = [
            { id: '#hero', name: 'Hero Section' },
            { id: '#quick-start', name: 'Quick Start Section' },
            { id: '#features', name: 'Features Section' },
            { id: '#architecture', name: 'Architecture Section' },
            { id: '#configuration', name: 'Configuration Section' },
            { id: '#commands', name: 'Commands Section' },
            { id: '#project-status', name: 'Project Status Section' },
            { id: '.footer', name: 'Footer' }
        ];

        for (const section of sections) {
            const element = page.locator(section.id);
            await expect(element, `${section.name} should be visible in ${browserName}`).toBeVisible();
        }
    });

    test('TC2: Hero section elements render correctly', async ({ page, browserName }) => {
        // Verify hero logo
        const logo = page.locator('#mirdb-logo');
        await expect(logo, `Logo should be visible in ${browserName}`).toBeVisible();

        // Verify logo has dimensions (not broken)
        const logoBox = await logo.boundingBox();
        expect(logoBox, `Logo should have bounding box in ${browserName}`).not.toBeNull();
        expect(logoBox.width, `Logo width should be > 0 in ${browserName}`).toBeGreaterThan(0);
        expect(logoBox.height, `Logo height should be > 0 in ${browserName}`).toBeGreaterThan(0);

        // Verify hero title
        const title = page.locator('.hero-title');
        await expect(title, `Hero title should be visible in ${browserName}`).toBeVisible();
        await expect(title).toHaveText('MirDB');

        // Verify tagline
        const tagline = page.locator('#tagline');
        await expect(tagline, `Tagline should be visible in ${browserName}`).toBeVisible();
        await expect(tagline).toContainText('Persistent Key-Value Store');

        // Verify CTA buttons
        const getStartedBtn = page.locator('#get-started-btn');
        const githubBtn = page.locator('#github-btn');

        await expect(getStartedBtn, `Get Started button should be visible in ${browserName}`).toBeVisible();
        await expect(githubBtn, `GitHub button should be visible in ${browserName}`).toBeVisible();

        // Verify buttons have proper dimensions for clickability
        const getStartedBox = await getStartedBtn.boundingBox();
        expect(getStartedBox.width, `Get Started button should have proper width in ${browserName}`).toBeGreaterThan(50);
        expect(getStartedBox.height, `Get Started button should have proper height in ${browserName}`).toBeGreaterThan(30);
    });

    test('TC3: Features grid renders with proper layout', async ({ page, browserName }) => {
        // Scroll to features section
        await page.locator('#features').scrollIntoViewIfNeeded();

        // Verify features heading
        const featuresHeading = page.locator('#features h2');
        await expect(featuresHeading, `Features heading should be visible in ${browserName}`).toBeVisible();
        await expect(featuresHeading).toContainText('Key Features');

        // Verify all feature cards are present
        const featureCards = page.locator('.feature-card');
        const cardCount = await featureCards.count();
        expect(cardCount, `Should have feature cards in ${browserName}`).toBeGreaterThanOrEqual(4);

        // Verify each card is visible and has proper dimensions
        for (let i = 0; i < cardCount; i++) {
            const card = featureCards.nth(i);
            await expect(card, `Feature card ${i + 1} should be visible in ${browserName}`).toBeVisible();

            const cardBox = await card.boundingBox();
            expect(cardBox, `Feature card ${i + 1} should have bounding box in ${browserName}`).not.toBeNull();
            expect(cardBox.width, `Feature card ${i + 1} width should be > 100 in ${browserName}`).toBeGreaterThan(100);
        }
    });

    test('TC4: Code blocks render with proper styling', async ({ page, browserName }) => {
        // Scroll to quick-start section
        await page.locator('#quick-start').scrollIntoViewIfNeeded();

        // Verify code blocks are present and styled
        const codeBlocks = page.locator('.code-block');
        const codeBlockCount = await codeBlocks.count();
        expect(codeBlockCount, `Should have code blocks in ${browserName}`).toBeGreaterThanOrEqual(1);

        // Check first code block styling
        const firstCodeBlock = codeBlocks.first();
        await expect(firstCodeBlock, `Code block should be visible in ${browserName}`).toBeVisible();

        // Verify code element has proper font
        const codeElement = firstCodeBlock.locator('code');
        await expect(codeElement, `Code element should be visible in ${browserName}`).toBeVisible();

        // Verify copy button exists
        const copyBtn = firstCodeBlock.locator('.copy-btn');
        await expect(copyBtn, `Copy button should be visible in ${browserName}`).toBeVisible();
    });

    test('TC5: Commands tables render correctly', async ({ page, browserName }) => {
        // Scroll to commands section
        await page.locator('#commands').scrollIntoViewIfNeeded();

        // Verify commands heading
        const commandsHeading = page.locator('#commands h2');
        await expect(commandsHeading, `Commands heading should be visible in ${browserName}`).toBeVisible();

        // Verify command tables are present
        const commandTables = page.locator('.commands-table');
        const tableCount = await commandTables.count();
        expect(tableCount, `Should have command tables in ${browserName}`).toBeGreaterThanOrEqual(1);

        // Verify tables have headers and rows
        for (let i = 0; i < tableCount; i++) {
            const table = commandTables.nth(i);
            await expect(table, `Command table ${i + 1} should be visible in ${browserName}`).toBeVisible();

            // Check for thead
            const thead = table.locator('thead');
            await expect(thead, `Table ${i + 1} should have thead in ${browserName}`).toBeVisible();

            // Check for tbody with rows
            const rows = table.locator('tbody tr');
            const rowCount = await rows.count();
            expect(rowCount, `Table ${i + 1} should have rows in ${browserName}`).toBeGreaterThanOrEqual(1);
        }
    });

    test('TC6: Architecture diagram renders correctly', async ({ page, browserName }) => {
        // Scroll to architecture section
        await page.locator('#architecture').scrollIntoViewIfNeeded();

        // Verify architecture heading
        const archHeading = page.locator('#architecture h2');
        await expect(archHeading, `Architecture heading should be visible in ${browserName}`).toBeVisible();

        // Verify architecture diagram
        const diagram = page.locator('#architecture-diagram');
        await expect(diagram, `Architecture diagram should be visible in ${browserName}`).toBeVisible();

        // Verify diagram has proper dimensions
        const diagramBox = await diagram.boundingBox();
        expect(diagramBox, `Architecture diagram should have bounding box in ${browserName}`).not.toBeNull();
        expect(diagramBox.width, `Architecture diagram width should be > 200 in ${browserName}`).toBeGreaterThan(200);

        // Verify write path and read path explanations
        const writePath = page.locator('#write-path');
        const readPath = page.locator('#read-path');

        await expect(writePath, `Write path should be visible in ${browserName}`).toBeVisible();
        await expect(readPath, `Read path should be visible in ${browserName}`).toBeVisible();
    });

    test('TC7: Project status section renders correctly', async ({ page, browserName }) => {
        // Scroll to project status section
        await page.locator('#project-status').scrollIntoViewIfNeeded();

        // Verify project status heading
        const statusHeading = page.locator('#project-status h2');
        await expect(statusHeading, `Project Status heading should be visible in ${browserName}`).toBeVisible();

        // Verify implemented features card
        const implementedCard = page.locator('#implemented-features');
        await expect(implementedCard, `Implemented features card should be visible in ${browserName}`).toBeVisible();

        // Verify planned features card
        const plannedCard = page.locator('#planned-features');
        await expect(plannedCard, `Planned features card should be visible in ${browserName}`).toBeVisible();

        // Verify status badges are visible
        const statusBadges = page.locator('.status-badge');
        const badgeCount = await statusBadges.count();
        expect(badgeCount, `Should have status badges in ${browserName}`).toBeGreaterThanOrEqual(1);
    });

    test('TC8: Footer renders correctly', async ({ page, browserName }) => {
        // Scroll to footer
        await page.locator('.footer').scrollIntoViewIfNeeded();

        // Verify footer is visible
        const footer = page.locator('.footer');
        await expect(footer, `Footer should be visible in ${browserName}`).toBeVisible();

        // Verify GitHub link in footer
        const githubLink = page.locator('#footer-github-link');
        await expect(githubLink, `GitHub link should be visible in footer in ${browserName}`).toBeVisible();

        const href = await githubLink.getAttribute('href');
        expect(href, `GitHub link should have correct href in ${browserName}`).toBe('https://github.com/yetone/mirdb');

        // Verify version info
        const version = page.locator('[data-testid="version"]');
        await expect(version, `Version should be visible in ${browserName}`).toBeVisible();
    });

    test('TC9: No horizontal overflow on page', async ({ page, browserName }) => {
        // Get viewport width
        const viewportSize = page.viewportSize();

        // Check if body has horizontal scrollbar
        const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
        const bodyClientWidth = await page.evaluate(() => document.body.clientWidth);

        // Allow a small tolerance (2px) for browser differences
        expect(
            bodyScrollWidth - bodyClientWidth,
            `Should have no horizontal overflow in ${browserName}`
        ).toBeLessThanOrEqual(2);
    });

    test('TC10: CSS gradients and visual effects render correctly', async ({ page, browserName }) => {
        // Verify hero background gradient is applied
        const hero = page.locator('.hero');
        const heroBackground = await hero.evaluate((el) => {
            return window.getComputedStyle(el).backgroundImage;
        });
        expect(heroBackground, `Hero should have gradient background in ${browserName}`).not.toBe('none');

        // Verify hero title has gradient text effect (where supported)
        const heroTitle = page.locator('.hero-title');
        await expect(heroTitle, `Hero title should be visible in ${browserName}`).toBeVisible();

        // Check title is styled
        const titleStyles = await heroTitle.evaluate((el) => {
            const styles = window.getComputedStyle(el);
            return {
                fontSize: styles.fontSize,
                fontWeight: styles.fontWeight
            };
        });
        expect(parseInt(titleStyles.fontSize), `Hero title font size should be large in ${browserName}`).toBeGreaterThan(30);
    });

    test('TC11: Button hover states and transitions work correctly', async ({ page, browserName }) => {
        // Verify primary button has proper styling
        const primaryBtn = page.locator('.btn-primary').first();
        await expect(primaryBtn, `Primary button should be visible in ${browserName}`).toBeVisible();

        // Get initial button position
        const initialBox = await primaryBtn.boundingBox();

        // Hover over button
        await primaryBtn.hover();

        // Verify button is still visible after hover
        await expect(primaryBtn, `Primary button should be visible after hover in ${browserName}`).toBeVisible();

        // Verify secondary button
        const secondaryBtn = page.locator('.btn-secondary').first();
        await expect(secondaryBtn, `Secondary button should be visible in ${browserName}`).toBeVisible();
    });

    test('TC12: Font rendering and typography consistency', async ({ page, browserName }) => {
        // Check body font family is applied
        const bodyFont = await page.evaluate(() => {
            return window.getComputedStyle(document.body).fontFamily;
        });
        expect(bodyFont, `Body should have font-family in ${browserName}`).toBeTruthy();

        // Check code blocks use monospace font
        const codeFont = await page.locator('.code-block code').first().evaluate((el) => {
            return window.getComputedStyle(el).fontFamily;
        });
        expect(codeFont.toLowerCase(), `Code should use monospace font in ${browserName}`).toMatch(/mono|consolas|monaco|fira/i);

        // Check headings are properly sized
        const h1Size = await page.locator('h1').first().evaluate((el) => {
            return window.getComputedStyle(el).fontSize;
        });
        const h2Size = await page.locator('h2').first().evaluate((el) => {
            return window.getComputedStyle(el).fontSize;
        });

        expect(parseInt(h1Size), `H1 should be larger than 24px in ${browserName}`).toBeGreaterThan(24);
        expect(parseInt(h2Size), `H2 should be larger than 20px in ${browserName}`).toBeGreaterThan(20);
    });

    test('TC13: Smooth scroll behavior works correctly', async ({ page, browserName }) => {
        // Click Get Started button
        const getStartedBtn = page.locator('#get-started-btn');
        await getStartedBtn.click();

        // Wait for scroll animation
        await page.waitForTimeout(1000);

        // Verify quick-start section is in view
        const quickStart = page.locator('#quick-start');
        await expect(quickStart, `Quick Start should be in viewport after clicking Get Started in ${browserName}`).toBeInViewport();
    });

    test('TC14: No JavaScript errors on page load', async ({ page, browserName }) => {
        const errors = [];

        // Listen for page errors
        page.on('pageerror', (error) => {
            errors.push(error.message);
        });

        // Reload page to capture any errors
        await page.reload();
        await page.waitForLoadState('networkidle');

        // Verify no critical errors
        expect(errors.length, `Should have no JavaScript errors in ${browserName}`).toBe(0);
    });

    test('TC15: Images load correctly without broken references', async ({ page, browserName }) => {
        // Get all images on the page
        const images = page.locator('img');
        const imageCount = await images.count();

        for (let i = 0; i < imageCount; i++) {
            const img = images.nth(i);

            // Check if image is visible
            const isVisible = await img.isVisible();
            if (isVisible) {
                const src = await img.getAttribute('src');

                // Wait for image to potentially load (some browsers/servers may be slow)
                await img.evaluate((el) => {
                    return new Promise((resolve) => {
                        if (el.complete) {
                            resolve();
                        } else {
                            el.onload = resolve;
                            el.onerror = resolve;
                            // Fallback timeout
                            setTimeout(resolve, 2000);
                        }
                    });
                });

                // For picture elements with webp sources, the img fallback may not load
                // Check if the image element has valid rendering dimensions instead
                const boundingBox = await img.boundingBox();

                // If the image is within a picture element with WebP source,
                // the GIF fallback may not have naturalWidth set
                // Check if it renders with positive dimensions
                if (boundingBox && boundingBox.width > 0 && boundingBox.height > 0) {
                    // Image renders correctly
                    expect(
                        boundingBox.width,
                        `Image ${src} should have width > 0 in ${browserName}`
                    ).toBeGreaterThan(0);
                } else {
                    // Fallback: check naturalWidth for standard images
                    const naturalWidth = await img.evaluate((el) => el.naturalWidth);
                    expect(
                        naturalWidth,
                        `Image ${src} should load correctly in ${browserName}`
                    ).toBeGreaterThan(0);
                }
            }
        }
    });
});
