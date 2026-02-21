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
    test.beforeEach(async ({ page }) => {
        await page.goto(homepageUrl);
    });

    test('TC1: Status section is present and visible', async ({ page }) => {
        // Test Case 1: Section displaying project status/roadmap is present
        const statusSection = page.locator('#status');
        await expect(statusSection).toBeVisible();

        const heading = statusSection.locator('h2');
        await expect(heading).toBeVisible();
        await expect(heading).toContainText('Project Status');
    });

    test('TC2: Tokio with memcached protocol is marked as completed', async ({ page }) => {
        // Test Case 2: Item 'tokio with memcached protocol' is marked as completed
        const statusSection = page.locator('#status');
        const tokioItem = statusSection.locator('.status-item:has-text("Tokio with memcached protocol")');

        await expect(tokioItem).toBeVisible();
        await expect(tokioItem).toHaveClass(/status-completed/);

        // Verify checkmark icon is present
        const icon = tokioItem.locator('.status-icon');
        await expect(icon).toContainText('✓');

        // Verify completed badge
        const badge = tokioItem.locator('.status-badge.completed');
        await expect(badge).toBeVisible();
    });

    test('TC3: Memtable with skiplist is marked as completed', async ({ page }) => {
        // Test Case 3: Item 'memtable with skiplist' is marked as completed
        const statusSection = page.locator('#status');
        const skiplistItem = statusSection.locator('.status-item:has-text("Memtable with skiplist")');

        await expect(skiplistItem).toBeVisible();
        await expect(skiplistItem).toHaveClass(/status-completed/);

        const icon = skiplistItem.locator('.status-icon');
        await expect(icon).toContainText('✓');
    });

    test('TC4: Minor compaction is marked as completed', async ({ page }) => {
        // Test Case 4: Item 'minor compaction' is marked as completed
        const statusSection = page.locator('#status');
        const minorCompactionItem = statusSection.locator('.status-item:has-text("Minor compaction")');

        await expect(minorCompactionItem).toBeVisible();
        await expect(minorCompactionItem).toHaveClass(/status-completed/);

        const icon = minorCompactionItem.locator('.status-icon');
        await expect(icon).toContainText('✓');
    });

    test('TC5: Major compaction is marked as completed', async ({ page }) => {
        // Test Case 5: Item 'major compaction' is marked as completed
        const statusSection = page.locator('#status');
        const majorCompactionItem = statusSection.locator('.status-item:has-text("Major compaction")');

        await expect(majorCompactionItem).toBeVisible();
        await expect(majorCompactionItem).toHaveClass(/status-completed/);

        const icon = majorCompactionItem.locator('.status-icon');
        await expect(icon).toContainText('✓');
    });

    test('TC6: Raft consensus is marked as planned/pending', async ({ page }) => {
        // Test Case 6: Item 'raft' is marked as planned/pending
        const statusSection = page.locator('#status');
        const raftItem = statusSection.locator('.status-item:has-text("Raft consensus")');

        await expect(raftItem).toBeVisible();
        await expect(raftItem).toHaveClass(/status-pending/);

        // Verify pending icon (different from checkmark)
        const icon = raftItem.locator('.status-icon');
        await expect(icon).toContainText('○');

        // Verify planned badge
        const badge = raftItem.locator('.status-badge.pending');
        await expect(badge).toBeVisible();
        await expect(badge).toContainText('Planned');
    });

    test('TC7: Visual distinction between completed and pending items', async ({ page }) => {
        // Test Case 7: Completed items have different styling than pending items
        const statusSection = page.locator('#status');

        // Get a completed item
        const completedItem = statusSection.locator('.status-completed').first();
        await expect(completedItem).toBeVisible();

        // Get the pending item
        const pendingItem = statusSection.locator('.status-pending').first();
        await expect(pendingItem).toBeVisible();

        // Verify completed item has checkmark
        const completedIcon = completedItem.locator('.status-icon');
        await expect(completedIcon).toContainText('✓');

        // Verify pending item has different icon
        const pendingIcon = pendingItem.locator('.status-icon');
        await expect(pendingIcon).toContainText('○');

        // Verify badges have different classes/styles
        const completedBadge = completedItem.locator('.status-badge.completed');
        const pendingBadge = pendingItem.locator('.status-badge.pending');

        await expect(completedBadge).toBeVisible();
        await expect(pendingBadge).toBeVisible();

        // Verify different background colors for badges
        const completedBgColor = await completedBadge.evaluate((el) => {
            return window.getComputedStyle(el).backgroundColor;
        });
        const pendingBgColor = await pendingBadge.evaluate((el) => {
            return window.getComputedStyle(el).backgroundColor;
        });

        expect(completedBgColor).not.toBe(pendingBgColor);

        // Verify icon colors are different
        const completedIconColor = await completedIcon.evaluate((el) => {
            return window.getComputedStyle(el).color;
        });
        const pendingIconColor = await pendingIcon.evaluate((el) => {
            return window.getComputedStyle(el).color;
        });

        expect(completedIconColor).not.toBe(pendingIconColor);
    });
});

/* ========================================
   Theme Toggle Tests (Scenario 8)
   ======================================== */
test.describe('Theme Toggle', () => {
    // Tests will be added by Scenario 8
});
