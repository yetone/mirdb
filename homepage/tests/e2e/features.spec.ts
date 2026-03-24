/**
 * Features Section E2E Tests
 * Owner: Scenario 3 - Features Section
 *
 * Tests for:
 * - Feature cards presence and count
 * - Individual feature validation (Memcached, Persistence, LSM-Tree)
 * - Responsive grid layout (desktop vs mobile)
 */

import { test, expect } from '@playwright/test';

test.describe('Features Section', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
    });

    // Test Case 1: Count feature cards in section
    test('should display at least 4 feature cards', async ({ page }) => {
        const featuresSection = page.locator('#features');
        await expect(featuresSection).toBeVisible();

        const featureCards = featuresSection.locator('.feature-card');
        const count = await featureCards.count();

        expect(count).toBeGreaterThanOrEqual(4);
    });

    // Test Case 2: Check Memcached Protocol feature
    test('should display Memcached Protocol feature card', async ({ page }) => {
        const featuresSection = page.locator('#features');

        // Find feature card with Memcached Protocol title
        const memcachedCard = featuresSection.locator('.feature-card', {
            has: page.locator('.feature-title', { hasText: /memcached/i })
        });

        await expect(memcachedCard).toBeVisible();

        // Verify it has a title
        const title = memcachedCard.locator('.feature-title');
        await expect(title).toContainText(/memcached/i);

        // Verify it has a description about memcached compatibility
        const description = memcachedCard.locator('.feature-description');
        await expect(description).toBeVisible();
        const descText = await description.textContent();
        expect(descText?.toLowerCase()).toMatch(/memcached|protocol|compatibility|replacement/i);
    });

    // Test Case 3: Check Disk Persistence feature
    test('should display Disk Persistence feature card', async ({ page }) => {
        const featuresSection = page.locator('#features');

        // Find feature card with Disk Persistence title
        const persistenceCard = featuresSection.locator('.feature-card', {
            has: page.locator('.feature-title', { hasText: /persistence/i })
        });

        await expect(persistenceCard).toBeVisible();

        // Verify it has a title about persistence
        const title = persistenceCard.locator('.feature-title');
        await expect(title).toContainText(/persistence/i);

        // Verify it has a description about data persistence
        const description = persistenceCard.locator('.feature-description');
        await expect(description).toBeVisible();
        const descText = await description.textContent();
        expect(descText?.toLowerCase()).toMatch(/persist|disk|data|storage|survives/i);
    });

    // Test Case 4: Check LSM-Tree feature
    test('should display LSM-Tree Architecture feature card', async ({ page }) => {
        const featuresSection = page.locator('#features');

        // Find feature card with LSM-Tree title
        const lsmCard = featuresSection.locator('.feature-card', {
            has: page.locator('.feature-title', { hasText: /lsm/i })
        });

        await expect(lsmCard).toBeVisible();

        // Verify it has a title about LSM-Tree
        const title = lsmCard.locator('.feature-title');
        await expect(title).toContainText(/lsm/i);

        // Verify it has a description about LSM-tree architecture
        const description = lsmCard.locator('.feature-description');
        await expect(description).toBeVisible();
        const descText = await description.textContent();
        expect(descText?.toLowerCase()).toMatch(/lsm|tree|log-structured|merge|memtable|sstable|compaction/i);
    });

    // Test Case 5: Features displayed in multi-column grid on desktop
    test('should display features in multi-column grid on desktop (1024px+)', async ({ page }) => {
        // Set desktop viewport
        await page.setViewportSize({ width: 1200, height: 800 });

        const featuresGrid = page.locator('.features-grid');
        await expect(featuresGrid).toBeVisible();

        // Check grid is using multi-column layout
        const gridStyle = await featuresGrid.evaluate((el) => {
            const computed = window.getComputedStyle(el);
            return {
                display: computed.display,
                gridTemplateColumns: computed.gridTemplateColumns,
            };
        });

        expect(gridStyle.display).toBe('grid');
        // Should have multiple columns (3 columns on desktop)
        // gridTemplateColumns returns pixel values like "350px 350px 350px"
        const columnCount = gridStyle.gridTemplateColumns.split(' ').length;
        expect(columnCount).toBeGreaterThan(1);
    });

    // Test Case 6: Features stack vertically on mobile (320px)
    test('should stack features vertically on mobile (320px)', async ({ page }) => {
        // Set mobile viewport
        await page.setViewportSize({ width: 320, height: 568 });

        const featuresGrid = page.locator('.features-grid');
        await expect(featuresGrid).toBeVisible();

        // Check grid is using single column layout on mobile
        const gridStyle = await featuresGrid.evaluate((el) => {
            const computed = window.getComputedStyle(el);
            return {
                display: computed.display,
                gridTemplateColumns: computed.gridTemplateColumns,
            };
        });

        expect(gridStyle.display).toBe('grid');
        // Should have single column on mobile
        const columnCount = gridStyle.gridTemplateColumns.split(' ').length;
        expect(columnCount).toBe(1);
    });

    // Additional validation tests
    test('each feature card should have an icon', async ({ page }) => {
        const featureCards = page.locator('#features .feature-card');
        const count = await featureCards.count();

        for (let i = 0; i < count; i++) {
            const icon = featureCards.nth(i).locator('.feature-icon');
            await expect(icon).toBeVisible();
        }
    });

    test('features section should have a section title', async ({ page }) => {
        const sectionTitle = page.locator('#features .section-title');
        await expect(sectionTitle).toBeVisible();
        await expect(sectionTitle).toContainText(/features/i);
    });
});
