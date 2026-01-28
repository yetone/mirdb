/**
 * Features Section E2E Tests
 * Owner: Scenario 2 - Features Section Display
 *
 * Tests for:
 * - Feature card count (5+ cards)
 * - Memcached Protocol feature
 * - Skip-list Memtable feature
 * - Compaction feature
 * - Async/Tokio feature
 * - Persistent Storage feature
 * - Grid layout on desktop
 */

const { test, expect } = require('@playwright/test');

test.describe('Features Section Display', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
        // Scroll to features section
        await page.locator('#features').scrollIntoViewIfNeeded();
    });

    test('TC1: At least 5 feature cards are present', async ({ page }) => {
        const featureCards = page.locator('.feature-card');
        const count = await featureCards.count();
        expect(count).toBeGreaterThanOrEqual(5);
    });

    test('TC2: Memcached Protocol feature exists with correct content', async ({ page }) => {
        const memcachedCard = page.locator('.feature-card[data-feature="memcached"]');
        await expect(memcachedCard).toBeVisible();

        const title = memcachedCard.locator('h3');
        await expect(title).toContainText('Memcached Protocol');

        const description = memcachedCard.locator('p');
        const descText = await description.textContent();
        expect(descText.toLowerCase()).toContain('compatibility');
    });

    test('TC3: Skip-list Memtable feature exists with correct content', async ({ page }) => {
        const skiplistCard = page.locator('.feature-card[data-feature="skiplist"]');
        await expect(skiplistCard).toBeVisible();

        const title = skiplistCard.locator('h3');
        const titleText = await title.textContent();
        expect(titleText.toLowerCase()).toMatch(/skip-list|memtable/);

        const description = skiplistCard.locator('p');
        const descText = await description.textContent();
        expect(descText.toLowerCase()).toMatch(/in-memory|efficient|memory/);
    });

    test('TC4: Compaction feature exists with correct content', async ({ page }) => {
        const compactionCard = page.locator('.feature-card[data-feature="compaction"]');
        await expect(compactionCard).toBeVisible();

        const title = compactionCard.locator('h3');
        await expect(title).toContainText('Compaction');

        const description = compactionCard.locator('p');
        const descText = await description.textContent();
        expect(descText.toLowerCase()).toMatch(/storage|optimiz/);
    });

    test('TC5: Tokio/Async feature exists with correct content', async ({ page }) => {
        const tokioCard = page.locator('.feature-card[data-feature="tokio"]');
        await expect(tokioCard).toBeVisible();

        const title = tokioCard.locator('h3');
        const titleText = await title.textContent();
        expect(titleText.toLowerCase()).toMatch(/tokio|async/);

        const description = tokioCard.locator('p');
        const descText = await description.textContent();
        expect(descText.toLowerCase()).toMatch(/performance|high-performance|concurrent/);
    });

    test('TC6: Persistent Storage feature exists with correct content', async ({ page }) => {
        const persistentCard = page.locator('.feature-card[data-feature="persistent"]');
        await expect(persistentCard).toBeVisible();

        const title = persistentCard.locator('h3');
        const titleText = await title.textContent();
        expect(titleText.toLowerCase()).toMatch(/persistent|storage/);

        const description = persistentCard.locator('p');
        const descText = await description.textContent();
        expect(descText.toLowerCase()).toMatch(/lsm-tree|durability|durable/);
    });

    test('TC7: Features are displayed in a grid layout on desktop', async ({ page }) => {
        // Set desktop viewport
        await page.setViewportSize({ width: 1280, height: 800 });

        const featuresGrid = page.locator('.features-grid');
        await expect(featuresGrid).toBeVisible();

        // Get computed style of the grid
        const gridStyle = await featuresGrid.evaluate((el) => {
            const computed = window.getComputedStyle(el);
            return {
                display: computed.display,
                gridTemplateColumns: computed.gridTemplateColumns
            };
        });

        // Check that display is grid
        expect(gridStyle.display).toBe('grid');

        // Check that grid has multiple columns (not single column = stacked)
        // gridTemplateColumns should not be 'none' and should have multiple values
        const columns = gridStyle.gridTemplateColumns.split(' ').filter(col => col !== '0px');
        expect(columns.length).toBeGreaterThan(1);
    });

    test('All feature cards have icons', async ({ page }) => {
        const featureCards = page.locator('.feature-card');
        const count = await featureCards.count();

        for (let i = 0; i < count; i++) {
            const card = featureCards.nth(i);
            const icon = card.locator('.feature-icon');
            await expect(icon).toBeVisible();
        }
    });

    test('All feature cards have titles and descriptions', async ({ page }) => {
        const featureCards = page.locator('.feature-card');
        const count = await featureCards.count();

        for (let i = 0; i < count; i++) {
            const card = featureCards.nth(i);
            const title = card.locator('h3');
            const description = card.locator('p');

            await expect(title).toBeVisible();
            await expect(description).toBeVisible();

            const titleText = await title.textContent();
            const descText = await description.textContent();

            expect(titleText.trim().length).toBeGreaterThan(0);
            expect(descText.trim().length).toBeGreaterThan(0);
        }
    });

    test('Features section has proper heading', async ({ page }) => {
        const featuresSection = page.locator('#features');
        const heading = featuresSection.locator('h2');
        await expect(heading).toBeVisible();
        await expect(heading).toContainText('Features');
    });
});
