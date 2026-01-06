// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for Cross-Browser Compatibility
 * Scenario: Verify homepage functions correctly across Chrome, Firefox, Safari, and Edge (NFR-4)
 *
 * Tests run against multiple browsers configured in playwright.config.js:
 * - chromium (Chrome)
 * - firefox (Firefox)
 * - webkit (Safari)
 * - msedge (Edge)
 */

test.describe('Cross-Browser Compatibility - Homepage Rendering', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
    });

    /**
     * Test Case 1: Page renders correctly with all features functional in Chrome
     * Test Case 2: Page renders correctly with all features functional in Firefox
     * Test Case 3: Page renders correctly with all features functional in Safari (webkit)
     * Test Case 4: Page renders correctly with all features functional in Edge
     *
     * These tests automatically run in all configured browsers via Playwright projects
     */

    test('Homepage loads successfully and displays core elements', async ({ page, browserName }) => {
        // Verify page title
        await expect(page).toHaveTitle(/MirDB/);

        // Verify hero section is visible
        const heroSection = page.locator('[data-testid="hero-section"]');
        await expect(heroSection).toBeVisible();

        // Verify hero title
        const heroTitle = page.locator('[data-testid="hero-title"]');
        await expect(heroTitle).toHaveText('MirDB');

        // Verify tagline
        const tagline = page.locator('[data-testid="hero-tagline"]');
        await expect(tagline).toContainText('Persistent Key-Value Store');
    });

    test('Navigation renders and functions correctly', async ({ page, browserName }) => {
        // Verify navigation exists
        const navigation = page.locator('[data-testid="navigation"]');
        await expect(navigation).toBeVisible();

        // Verify navigation links
        const navLinks = page.locator('[data-testid="nav-links"]');
        await expect(navLinks).toBeVisible();

        // Verify Getting Started link
        const gettingStartedLink = page.locator('[data-testid="nav-getting-started"]');
        await expect(gettingStartedLink).toBeVisible();
        await expect(gettingStartedLink).toHaveAttribute('href', '#get-started');

        // Verify Documentation link
        const docsLink = page.locator('[data-testid="nav-documentation"]');
        await expect(docsLink).toBeVisible();
        await expect(docsLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb#readme');

        // Verify GitHub link
        const githubLink = page.locator('[data-testid="nav-github"]');
        await expect(githubLink).toBeVisible();
        await expect(githubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
    });

    test('Status badges are visible and properly rendered', async ({ page, browserName }) => {
        // Verify status badges container
        const badgesContainer = page.locator('[data-testid="status-badges"]');
        await expect(badgesContainer).toBeVisible();

        // Verify build status badge
        const buildBadge = page.locator('[data-testid="badge-build-status"]');
        await expect(buildBadge).toBeVisible();

        // Verify version badge
        const versionBadge = page.locator('[data-testid="badge-version"]');
        await expect(versionBadge).toBeVisible();

        // Verify license badge
        const licenseBadge = page.locator('[data-testid="badge-license"]');
        await expect(licenseBadge).toBeVisible();
    });

    test('CTA buttons are visible and have correct attributes', async ({ page, browserName }) => {
        // Verify Get Started CTA
        const getStartedCTA = page.locator('[data-testid="cta-get-started"]');
        await expect(getStartedCTA).toBeVisible();
        await expect(getStartedCTA).toHaveAttribute('href', '#get-started');
        await expect(getStartedCTA).toContainText('Get Started');

        // Verify GitHub CTA
        const githubCTA = page.locator('[data-testid="cta-github"]');
        await expect(githubCTA).toBeVisible();
        await expect(githubCTA).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
        await expect(githubCTA).toContainText('View on GitHub');
    });

    test('Quick Start section renders correctly', async ({ page, browserName }) => {
        // Verify Quick Start section exists
        const quickStartSection = page.locator('[data-testid="quick-start-section"]');
        await expect(quickStartSection).toBeVisible();

        // Verify installation code block
        const installCodeBlock = page.locator('[data-testid="install-code-block"]');
        await expect(installCodeBlock).toBeVisible();

        // Verify install code content
        const installCode = page.locator('[data-testid="install-code"]');
        await expect(installCode).toContainText('cargo install mirdb-server');

        // Verify usage code block
        const usageCodeBlock = page.locator('[data-testid="usage-code-block"]');
        await expect(usageCodeBlock).toBeVisible();

        // Verify copy buttons are present
        const installCopyBtn = page.locator('[data-testid="copy-install-btn"]');
        await expect(installCopyBtn).toBeVisible();

        const usageCopyBtn = page.locator('[data-testid="copy-usage-btn"]');
        await expect(usageCopyBtn).toBeVisible();
    });

    test('Comparison table renders correctly', async ({ page, browserName }) => {
        // Verify comparison table exists
        const comparisonTable = page.locator('[data-testid="comparison-table"]');
        await expect(comparisonTable).toBeVisible();

        // Verify table has proper structure
        const table = comparisonTable.locator('table');
        await expect(table).toBeVisible();

        // Verify table headers
        const headers = table.locator('thead th');
        await expect(headers).toHaveCount(4); // Feature, MirDB, Memcached, Redis
    });

    test('Architecture diagram renders correctly', async ({ page, browserName }) => {
        // Verify architecture section exists
        const architectureSection = page.locator('[data-testid="architecture-section"]');
        await expect(architectureSection).toBeVisible();

        // Verify SVG diagram exists
        const diagram = page.locator('[data-testid="architecture-diagram"]');
        await expect(diagram).toBeVisible();

        // Verify SVG has accessibility attributes
        await expect(diagram).toHaveAttribute('role', 'img');
    });

    test('Usage demo section renders correctly', async ({ page, browserName }) => {
        // Verify usage demo section exists (there are two, use first)
        const usageDemoSection = page.locator('[data-testid="usage-demo-section"]').first();
        await expect(usageDemoSection).toBeVisible();

        // Scroll to the demo section to trigger lazy loading
        await usageDemoSection.scrollIntoViewIfNeeded();

        // Verify demo media exists (with lazy loading, wait for it to load)
        const demoMedia = page.locator('[data-testid="usage-demo-media"]').first();
        await expect(demoMedia).toBeVisible({ timeout: 10000 });
    });
});

test.describe('Cross-Browser Compatibility - Copy to Clipboard', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
    });

    /**
     * Test Case 5: Copy functionality works in all supported browsers
     */

    test('Copy button shows visual feedback on click', async ({ page, context, browserName }) => {
        // Grant clipboard permissions where supported
        try {
            await context.grantPermissions(['clipboard-read', 'clipboard-write']);
        } catch (e) {
            // Some browsers may not support clipboard permissions
        }

        // Click the copy button
        const copyBtn = page.locator('[data-testid="copy-install-btn"]');
        await expect(copyBtn).toBeVisible();
        await copyBtn.click();

        // Verify visual feedback - button should show "Copied"
        const copyText = copyBtn.locator('.copy-text');
        await expect(copyText).toHaveText('Copied');

        // Verify button has "copied" class
        await expect(copyBtn).toHaveClass(/copied/);

        // Verify icon changed to checkmark
        const copyIcon = copyBtn.locator('.copy-icon');
        await expect(copyIcon).toHaveText('✓');
    });

    test('Copy button is keyboard accessible', async ({ page, browserName }) => {
        // Focus on the copy button
        const copyBtn = page.locator('[data-testid="copy-install-btn"]');
        await copyBtn.focus();

        // Verify button is focused
        await expect(copyBtn).toBeFocused();

        // Verify button has accessible label
        await expect(copyBtn).toHaveAttribute('aria-label', 'Copy installation command');
    });

    test('Copy functionality works with clipboard API', async ({ page, context, browserName }) => {
        // Skip Edge for clipboard verification (uses chromium engine, tested via chromium)
        if (browserName === 'msedge') {
            test.skip();
        }

        // Grant clipboard permissions
        try {
            await context.grantPermissions(['clipboard-read', 'clipboard-write']);
        } catch (e) {
            // Skip test if clipboard permissions not supported
            test.skip();
        }

        // Click the copy button
        const copyBtn = page.locator('[data-testid="copy-install-btn"]');
        await copyBtn.click();

        // Wait for visual feedback
        await expect(copyBtn.locator('.copy-text')).toHaveText('Copied');

        // Verify clipboard content
        const clipboardContent = await page.evaluate(async () => {
            return await navigator.clipboard.readText();
        });
        expect(clipboardContent).toBe('cargo install mirdb-server');
    });

    test('Usage code copy button works', async ({ page, context, browserName }) => {
        // Grant clipboard permissions where supported
        try {
            await context.grantPermissions(['clipboard-read', 'clipboard-write']);
        } catch (e) {
            // Some browsers may not support clipboard permissions
        }

        // Click the usage copy button
        const usageCopyBtn = page.locator('[data-testid="copy-usage-btn"]');
        await expect(usageCopyBtn).toBeVisible();
        await usageCopyBtn.click();

        // Verify visual feedback
        const copyText = usageCopyBtn.locator('.copy-text');
        await expect(copyText).toHaveText('Copied');

        // Verify icon changed
        const copyIcon = usageCopyBtn.locator('.copy-icon');
        await expect(copyIcon).toHaveText('✓');
    });

    test('Copy button resets after feedback timeout', async ({ page, context, browserName }) => {
        // Grant clipboard permissions where supported
        try {
            await context.grantPermissions(['clipboard-read', 'clipboard-write']);
        } catch (e) {
            // Some browsers may not support clipboard permissions
        }

        // Click the copy button
        const copyBtn = page.locator('[data-testid="copy-install-btn"]');
        await copyBtn.click();

        // Verify feedback shows
        await expect(copyBtn.locator('.copy-text')).toHaveText('Copied');

        // Wait for reset (timeout is 2 seconds, add buffer)
        await expect(copyBtn.locator('.copy-text')).toHaveText('Copy', { timeout: 3000 });
        await expect(copyBtn.locator('.copy-icon')).toHaveText('📋', { timeout: 3000 });
    });
});

test.describe('Cross-Browser Compatibility - Interactive Elements', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
    });

    test('Internal anchor links work correctly', async ({ page, browserName }) => {
        // Click on "Get Started" CTA which links to #get-started
        const getStartedCTA = page.locator('[data-testid="cta-get-started"]');
        await getStartedCTA.click();

        // Verify page scrolled to quick-start section (URL should have hash)
        await expect(page).toHaveURL(/#get-started/);

        // Verify quick-start section is visible
        const quickStartSection = page.locator('[data-testid="quick-start-section"]');
        await expect(quickStartSection).toBeInViewport();
    });

    test('Navigation Getting Started link scrolls to section', async ({ page, browserName }) => {
        // Click on Getting Started nav link
        const navLink = page.locator('[data-testid="nav-getting-started"]');
        await navLink.click();

        // Verify URL has hash
        await expect(page).toHaveURL(/#get-started/);
    });

    test('External links have proper security attributes', async ({ page, browserName }) => {
        // Check GitHub link
        const githubLink = page.locator('[data-testid="nav-github"]');
        await expect(githubLink).toHaveAttribute('target', '_blank');
        await expect(githubLink).toHaveAttribute('rel', /noopener/);

        // Check documentation link
        const docsLink = page.locator('[data-testid="nav-documentation"]');
        await expect(docsLink).toHaveAttribute('target', '_blank');
        await expect(docsLink).toHaveAttribute('rel', /noopener/);
    });

    test('Page renders without JavaScript errors', async ({ page, browserName }) => {
        const errors = [];

        // Listen for page errors
        page.on('pageerror', (error) => {
            errors.push(error.message);
        });

        // Reload page to catch any initialization errors
        await page.reload();
        await page.waitForLoadState('networkidle');

        // Verify no JavaScript errors
        expect(errors).toHaveLength(0);
    });
});

test.describe('Cross-Browser Compatibility - CSS and Layout', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
    });

    test('Hero section has proper CSS styling', async ({ page, browserName }) => {
        const heroSection = page.locator('[data-testid="hero-section"]');

        // Verify hero section is visible
        await expect(heroSection).toBeVisible();

        // Verify proper layout (centered content)
        const heroContent = heroSection.locator('.hero-content');
        const box = await heroContent.boundingBox();
        expect(box).not.toBeNull();
        expect(box.width).toBeGreaterThan(0);
        expect(box.height).toBeGreaterThan(0);
    });

    test('Buttons have proper styling', async ({ page, browserName }) => {
        const primaryBtn = page.locator('[data-testid="cta-get-started"]');
        const secondaryBtn = page.locator('[data-testid="cta-github"]');

        // Verify buttons are visible and have proper dimensions
        await expect(primaryBtn).toBeVisible();
        await expect(secondaryBtn).toBeVisible();

        // Get button bounding boxes
        const primaryBox = await primaryBtn.boundingBox();
        const secondaryBox = await secondaryBtn.boundingBox();

        expect(primaryBox).not.toBeNull();
        expect(secondaryBox).not.toBeNull();
        expect(primaryBox.width).toBeGreaterThan(0);
        expect(secondaryBox.width).toBeGreaterThan(0);
    });

    test('Code blocks are properly styled', async ({ page, browserName }) => {
        const codeBlock = page.locator('[data-testid="install-code-block"]');
        await expect(codeBlock).toBeVisible();

        // Verify code block has proper dimensions
        const box = await codeBlock.boundingBox();
        expect(box).not.toBeNull();
        expect(box.width).toBeGreaterThan(100);
        expect(box.height).toBeGreaterThan(30);
    });

    test('SVG diagram renders properly', async ({ page, browserName }) => {
        const diagram = page.locator('[data-testid="architecture-diagram"]');
        await expect(diagram).toBeVisible();

        // Verify SVG has proper viewBox
        await expect(diagram).toHaveAttribute('viewBox', '0 0 600 500');

        // Verify SVG has content
        const rects = diagram.locator('rect');
        const texts = diagram.locator('text');
        expect(await rects.count()).toBeGreaterThan(0);
        expect(await texts.count()).toBeGreaterThan(0);
    });
});
