/**
 * Features Section E2E Tests
 * Owner: Scenario 2 - Features Section Display
 *
 * Tests for:
 * - At least 5 features displayed
 * - Key features: Memcached, persistence, LSM tree, Skip list, WAL
 * - Feature cards have title and description
 */

const { test, expect } = require('@playwright/test');

test.describe('Features Section', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('http://localhost:3000/');
    });

    test('should display at least 5 feature items', async ({ page }) => {
        const featuresSection = page.locator('#features');
        await expect(featuresSection).toBeVisible();

        const featureCards = featuresSection.locator('.feature-card');
        const count = await featureCards.count();
        expect(count).toBeGreaterThanOrEqual(5);
    });

    test('should have Memcached compatibility feature', async ({ page }) => {
        const featuresSection = page.locator('#features');

        // Check for feature mentioning Memcached protocol compatibility
        const memcachedFeature = featuresSection.locator('.feature-card', {
            hasText: /memcached/i
        });
        await expect(memcachedFeature.first()).toBeVisible();

        // Verify it mentions protocol compatibility
        const featureText = await memcachedFeature.first().textContent();
        expect(featureText.toLowerCase()).toMatch(/protocol|compatible|compatibility/i);
    });

    test('should have persistent storage feature', async ({ page }) => {
        const featuresSection = page.locator('#features');

        // Check for feature mentioning persistent or disk-based storage
        const persistentFeature = featuresSection.locator('.feature-card', {
            hasText: /persistent|disk/i
        });
        await expect(persistentFeature.first()).toBeVisible();
    });

    test('should have LSM tree architecture feature', async ({ page }) => {
        const featuresSection = page.locator('#features');

        // Check for feature mentioning LSM tree
        const lsmFeature = featuresSection.locator('.feature-card', {
            hasText: /lsm tree|lsm-tree|log-structured merge/i
        });
        await expect(lsmFeature.first()).toBeVisible();
    });

    test('should have Skip list memtable feature', async ({ page }) => {
        const featuresSection = page.locator('#features');

        // Check for feature mentioning Skip list or memtable
        const skipListFeature = featuresSection.locator('.feature-card', {
            hasText: /skip list|skiplist|memtable/i
        });
        await expect(skipListFeature.first()).toBeVisible();
    });

    test('should have WAL/durability feature', async ({ page }) => {
        const featuresSection = page.locator('#features');

        // Check for feature mentioning WAL, Write-Ahead Log, or ACID durability
        const walFeature = featuresSection.locator('.feature-card', {
            hasText: /wal|write-ahead log|write ahead log|acid/i
        });
        await expect(walFeature.first()).toBeVisible();
    });

    test('each feature card should have title and description', async ({ page }) => {
        const featuresSection = page.locator('#features');
        const featureCards = featuresSection.locator('.feature-card');
        const count = await featureCards.count();

        expect(count).toBeGreaterThan(0);

        for (let i = 0; i < count; i++) {
            const card = featureCards.nth(i);

            // Each card should have a title element
            const title = card.locator('.feature-title');
            await expect(title).toBeVisible();
            const titleText = await title.textContent();
            expect(titleText.trim().length).toBeGreaterThan(0);

            // Each card should have a description element
            const description = card.locator('.feature-description');
            await expect(description).toBeVisible();
            const descText = await description.textContent();
            expect(descText.trim().length).toBeGreaterThan(0);
        }
    });
});
