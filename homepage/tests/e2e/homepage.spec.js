/**
 * Homepage E2E Tests
 * Owner: Multiple scenarios contribute to this file
 *
 * Test Sections:
 * - Hero section tests (Scenario 1)
 * - Features section tests (Scenario 2)
 * - Quick Start section tests (Scenario 3)
 * - Status section tests (Scenario 4)
 * - Theme toggle tests (Scenario 8)
 *
 * Framework: Playwright
 */

const { test, expect } = require('@playwright/test');
const path = require('path');

const homepageUrl = 'file://' + path.resolve(__dirname, '../../index.html');

/* ========================================
   Hero Section Tests (Scenario 1)
   ======================================== */
test.describe('Hero Section', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto(homepageUrl);
    });

    test('hero section is visible with h1 containing MirDB', async ({ page }) => {
        // Check hero section exists and is visible
        const hero = page.locator('.hero');
        await expect(hero).toBeVisible();

        // Check h1 contains 'MirDB'
        const h1 = page.locator('.hero h1');
        await expect(h1).toBeVisible();
        await expect(h1).toContainText('MirDB');
    });

    test('tagline contains Persistent Key-Value Store and Memcached', async ({ page }) => {
        const tagline = page.locator('.hero-tagline');
        await expect(tagline).toBeVisible();

        const taglineText = await tagline.textContent();
        expect(taglineText).toContain('Persistent Key-Value Store');
        expect(taglineText).toContain('Memcached');
    });

    test('View Repository button links to GitHub and opens in new tab', async ({ page }) => {
        const viewRepoBtn = page.locator('a.btn-primary:has-text("View Repository")');
        await expect(viewRepoBtn).toBeVisible();

        // Check href is correct
        const href = await viewRepoBtn.getAttribute('href');
        expect(href).toBe('https://github.com/yetone/mirdb');

        // Check opens in new tab
        const target = await viewRepoBtn.getAttribute('target');
        expect(target).toBe('_blank');

        // Check has noopener noreferrer for security
        const rel = await viewRepoBtn.getAttribute('rel');
        expect(rel).toContain('noopener');
        expect(rel).toContain('noreferrer');
    });

    test('Get Started button links to Quick Start section', async ({ page }) => {
        const getStartedBtn = page.locator('a.btn-secondary:has-text("Get Started")');
        await expect(getStartedBtn).toBeVisible();

        // Check href points to quickstart section
        const href = await getStartedBtn.getAttribute('href');
        expect(href).toBe('#quickstart');
    });

    test('hero section has semantic header element with role', async ({ page }) => {
        // Check hero is a header element with banner role
        const hero = page.locator('header.hero');
        await expect(hero).toBeVisible();

        const role = await hero.getAttribute('role');
        expect(role).toBe('banner');
    });
});

/* ========================================
   Features Section Tests (Scenario 2)
   ======================================== */
test.describe('Features Section', () => {
    // Tests will be added by Scenario 2
});

/* ========================================
   Quick Start Section Tests (Scenario 3)
   ======================================== */
test.describe('Quick Start Section', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto(homepageUrl);
    });

    test('should have Quick Start section with correct id or heading', async ({ page }) => {
        // Test Case 1: Section with id='quickstart' or heading 'Quick Start' is present
        const quickstartSection = page.locator('#quickstart');
        await expect(quickstartSection).toBeVisible();

        const heading = quickstartSection.locator('h2');
        await expect(heading).toHaveText('Quick Start');
    });

    test('should have at least one code block element', async ({ page }) => {
        // Test Case 2: At least one <pre><code> or similar code block element exists
        const quickstartSection = page.locator('#quickstart');
        const codeBlocks = quickstartSection.locator('pre code');

        const count = await codeBlocks.count();
        expect(count).toBeGreaterThanOrEqual(1);
    });

    test('should contain memcached command examples', async ({ page }) => {
        // Test Case 3: Code block contains memcached commands like 'set', 'get', or 'telnet'
        const quickstartSection = page.locator('#quickstart');
        const codeBlocks = quickstartSection.locator('pre code');

        // Get all code block text content
        const codeTexts = await codeBlocks.allTextContents();
        const combinedText = codeTexts.join(' ').toLowerCase();

        // Check for memcached commands
        const hasSet = combinedText.includes('set');
        const hasGet = combinedText.includes('get');
        const hasTelnet = combinedText.includes('telnet');

        expect(hasSet || hasGet || hasTelnet).toBe(true);
    });

    test('should have code block with monospace font and distinguishable background', async ({ page }) => {
        // Test Case 4: Code block has monospace font and distinguishable background
        const quickstartSection = page.locator('#quickstart');
        const codeBlock = quickstartSection.locator('pre').first();

        await expect(codeBlock).toBeVisible();

        // Check background color - should be dark (not white)
        const bgColor = await codeBlock.evaluate((el) => {
            return window.getComputedStyle(el).backgroundColor;
        });

        // Background should not be white (rgb(255, 255, 255))
        expect(bgColor).not.toBe('rgb(255, 255, 255)');

        // Check that code has monospace font
        const codeElement = quickstartSection.locator('pre code').first();
        const fontFamily = await codeElement.evaluate((el) => {
            return window.getComputedStyle(el).fontFamily;
        });

        // Font family should include a monospace font
        const hasMonospace = fontFamily.toLowerCase().includes('mono') ||
                            fontFamily.toLowerCase().includes('courier') ||
                            fontFamily.toLowerCase().includes('consolas');
        expect(hasMonospace).toBe(true);
    });

    test('should have readable code with sufficient contrast', async ({ page }) => {
        // Test Case 5: Code text has sufficient contrast and is legible
        const quickstartSection = page.locator('#quickstart');
        const codeElement = quickstartSection.locator('pre code').first();

        await expect(codeElement).toBeVisible();

        // Get text color and font size
        const styles = await codeElement.evaluate((el) => {
            const computed = window.getComputedStyle(el);
            return {
                color: computed.color,
                fontSize: computed.fontSize
            };
        });

        // Check that text is not too small (at least 12px)
        const fontSize = parseFloat(styles.fontSize);
        expect(fontSize).toBeGreaterThanOrEqual(12);

        // Check that color is not transparent or zero opacity
        expect(styles.color).not.toBe('transparent');
        expect(styles.color).not.toBe('rgba(0, 0, 0, 0)');
    });

    test('should navigate to Quick Start section when clicking nav link', async ({ page }) => {
        // Verify navigation works
        await page.click('a[href="#quickstart"]');

        // Wait for scroll to complete
        await page.waitForTimeout(500);

        // Check that quickstart section is in view
        const quickstartSection = page.locator('#quickstart');
        await expect(quickstartSection).toBeInViewport();
    });
});

/* ========================================
   Status Section Tests (Scenario 4)
   ======================================== */
test.describe('Status Section', () => {
    // Tests will be added by Scenario 4
});

/* ========================================
   Theme Toggle Tests (Scenario 8)
   ======================================== */
test.describe('Theme Toggle', () => {
    // Tests will be added by Scenario 8
});
