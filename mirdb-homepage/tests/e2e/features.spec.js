/**
 * Features Section E2E Tests
 * Owner: Scenario 2 - Features Section Display
 *
 * Tests:
 * - Features section presence
 * - All 6 feature cards display correctly
 * - Feature card hover effects
 * - Correct content for each feature
 */

const { test, expect } = require('@playwright/test');

test.describe('Features Section Display', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
        await page.waitForLoadState('domcontentloaded');
    });

    test('TC1: Features section exists with id="features" and contains 6 feature cards', async ({ page }) => {
        // Check features section exists with correct id
        const featuresSection = page.locator('#features');
        await expect(featuresSection).toBeVisible();

        // Check it contains exactly 6 feature cards
        const featureCards = page.locator('#features .feature-card');
        await expect(featureCards).toHaveCount(6);
    });

    test('TC2: Memcached Protocol feature card displays correctly', async ({ page }) => {
        const card = page.locator('.feature-card').filter({ hasText: 'Memcached Protocol' });
        await expect(card).toBeVisible();

        // Check title
        const title = card.locator('h3');
        await expect(title).toHaveText('Memcached Protocol');

        // Check description mentions familiar usage
        const description = card.locator('p');
        await expect(description).toContainText('familiar');
    });

    test('TC3: Persistence feature card displays correctly', async ({ page }) => {
        const card = page.locator('.feature-card').filter({ hasText: 'Persistence' }).first();
        await expect(card).toBeVisible();

        // Check title
        const title = card.locator('h3');
        await expect(title).toHaveText('Persistence');

        // Check description mentions data surviving restarts
        const description = card.locator('p');
        await expect(description).toContainText('survives restarts');
    });

    test('TC4: LSM Tree feature card displays correctly', async ({ page }) => {
        const card = page.locator('.feature-card').filter({ hasText: 'LSM Tree' });
        await expect(card).toBeVisible();

        // Check title
        const title = card.locator('h3');
        await expect(title).toHaveText('LSM Tree');

        // Check description mentions efficient storage
        const description = card.locator('p');
        await expect(description).toContainText('efficient');
    });

    test('TC5: Skip List Memtable feature card displays correctly', async ({ page }) => {
        const card = page.locator('.feature-card').filter({ hasText: 'Skip List Memtable' });
        await expect(card).toBeVisible();

        // Check title
        const title = card.locator('h3');
        await expect(title).toHaveText('Skip List Memtable');

        // Check description mentions fast in-memory operations
        const description = card.locator('p');
        await expect(description).toContainText('in-memory');
    });

    test('TC6: Compaction feature card displays correctly', async ({ page }) => {
        const card = page.locator('.feature-card').filter({ hasText: 'Compaction' }).first();
        await expect(card).toBeVisible();

        // Check title
        const title = card.locator('h3');
        await expect(title).toHaveText('Compaction');

        // Check description mentions automatic optimization
        const description = card.locator('p');
        await expect(description).toContainText('optimization');
    });

    test('TC7: Raft (Future) feature card displays correctly', async ({ page }) => {
        const card = page.locator('.feature-card').filter({ hasText: 'Raft (Future)' });
        await expect(card).toBeVisible();

        // Check title indicates planned feature
        const title = card.locator('h3');
        await expect(title).toHaveText('Raft (Future)');

        // Check description mentions distributed consensus
        const description = card.locator('p');
        await expect(description).toContainText('distributed consensus');
    });

    test('TC8: Feature cards have CSS hover effects', async ({ page }) => {
        const featureCard = page.locator('.feature-card').first();

        // Get initial transform state
        const initialTransform = await featureCard.evaluate((el) => {
            return window.getComputedStyle(el).transform;
        });

        // Get initial box-shadow
        const initialShadow = await featureCard.evaluate((el) => {
            return window.getComputedStyle(el).boxShadow;
        });

        // Hover over the card
        await featureCard.hover();

        // Wait for transition to complete
        await page.waitForTimeout(300);

        // Get hover state styles
        const hoverTransform = await featureCard.evaluate((el) => {
            return window.getComputedStyle(el).transform;
        });

        const hoverShadow = await featureCard.evaluate((el) => {
            return window.getComputedStyle(el).boxShadow;
        });

        // Verify that hover state is different (transform or shadow changed)
        const transformChanged = initialTransform !== hoverTransform;
        const shadowChanged = initialShadow !== hoverShadow;

        expect(transformChanged || shadowChanged).toBeTruthy();
    });
});
