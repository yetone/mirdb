/**
 * Project Status Section E2E Tests
 * Owner: Scenario 7 - Project Status Section
 *
 * Tests for:
 * - Status section presence with implemented/planned columns
 * - Implemented features (Tokio, skip list, minor/major compaction)
 * - Planned features (Raft consensus)
 * - Visual distinction between implemented and planned items
 */

import { test, expect } from '@playwright/test';

test.describe('Project Status Section', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
    });

    // Test Case 1: Status section is present with implemented/planned columns
    test('should display project status section with two columns', async ({ page }) => {
        const statusSection = page.locator('#status');
        await expect(statusSection).toBeVisible();

        // Check for section title
        const sectionTitle = statusSection.locator('.section-title, h2');
        await expect(sectionTitle).toBeVisible();
        await expect(sectionTitle).toContainText(/status/i);

        // Check for two columns (implemented and planned)
        const columns = statusSection.locator('.status-column');
        await expect(columns).toHaveCount(2);

        // Check for implemented features column
        const implementedColumn = statusSection.locator('.status-column--implemented');
        await expect(implementedColumn).toBeVisible();

        // Check for planned features column
        const plannedColumn = statusSection.locator('.status-column--planned');
        await expect(plannedColumn).toBeVisible();
    });

    // Test Case 2: Tokio async networking is listed as implemented
    test('should display Tokio async networking as implemented', async ({ page }) => {
        const statusSection = page.locator('#status');
        await expect(statusSection).toBeVisible();

        // Find Tokio in the implemented features
        const implementedColumn = statusSection.locator('.status-column--implemented');
        const tokioItem = implementedColumn.locator('.status-item').filter({
            hasText: /tokio/i
        });
        await expect(tokioItem).toBeVisible();
        await expect(tokioItem).toHaveClass(/status-item--implemented/);
    });

    // Test Case 3: Memtable with skip list is listed as implemented
    test('should display Memtable with skip list as implemented', async ({ page }) => {
        const statusSection = page.locator('#status');
        await expect(statusSection).toBeVisible();

        // Find skip list memtable in the implemented features
        const implementedColumn = statusSection.locator('.status-column--implemented');
        const skipListItem = implementedColumn.locator('.status-item').filter({
            hasText: /skip\s*list|memtable/i
        });
        await expect(skipListItem).toBeVisible();
        await expect(skipListItem).toHaveClass(/status-item--implemented/);
    });

    // Test Case 4: Minor compaction is listed as implemented
    test('should display minor compaction as implemented', async ({ page }) => {
        const statusSection = page.locator('#status');
        await expect(statusSection).toBeVisible();

        // Find minor compaction in the implemented features
        const implementedColumn = statusSection.locator('.status-column--implemented');
        const minorCompactionItem = implementedColumn.locator('.status-item').filter({
            hasText: /minor\s*compaction/i
        });
        await expect(minorCompactionItem).toBeVisible();
        await expect(minorCompactionItem).toHaveClass(/status-item--implemented/);
    });

    // Test Case 5: Major compaction is listed as implemented
    test('should display major compaction as implemented', async ({ page }) => {
        const statusSection = page.locator('#status');
        await expect(statusSection).toBeVisible();

        // Find major compaction in the implemented features
        const implementedColumn = statusSection.locator('.status-column--implemented');
        const majorCompactionItem = implementedColumn.locator('.status-item').filter({
            hasText: /major\s*compaction/i
        });
        await expect(majorCompactionItem).toBeVisible();
        await expect(majorCompactionItem).toHaveClass(/status-item--implemented/);
    });

    // Test Case 6: Raft consensus is listed as planned/roadmap
    test('should display Raft consensus as planned feature', async ({ page }) => {
        const statusSection = page.locator('#status');
        await expect(statusSection).toBeVisible();

        // Find Raft in the planned features
        const plannedColumn = statusSection.locator('.status-column--planned');
        const raftItem = plannedColumn.locator('.status-item').filter({
            hasText: /raft/i
        });
        await expect(raftItem).toBeVisible();
        await expect(raftItem).toHaveClass(/status-item--planned/);
    });

    // Test Case 7: Visual distinction - implemented features have checkmarks, planned have roadmap icons
    test('should have visual distinction between implemented and planned features', async ({ page }) => {
        const statusSection = page.locator('#status');
        await expect(statusSection).toBeVisible();

        // Check implemented features have checkmarks
        const implementedColumn = statusSection.locator('.status-column--implemented');
        const implementedItems = implementedColumn.locator('.status-item--implemented');
        const implementedCount = await implementedItems.count();
        expect(implementedCount).toBeGreaterThan(0);

        // Each implemented item should have a checkmark icon
        for (let i = 0; i < implementedCount; i++) {
            const item = implementedItems.nth(i);
            const icon = item.locator('.status-item-icon');
            await expect(icon).toBeVisible();
            // Check the icon contains checkmark character
            const iconText = await icon.textContent();
            expect(iconText).toContain('✓');
        }

        // Check planned features have different icons
        const plannedColumn = statusSection.locator('.status-column--planned');
        const plannedItems = plannedColumn.locator('.status-item--planned');
        const plannedCount = await plannedItems.count();
        expect(plannedCount).toBeGreaterThan(0);

        // Each planned item should have a roadmap icon
        for (let i = 0; i < plannedCount; i++) {
            const item = plannedItems.nth(i);
            const icon = item.locator('.status-item-icon');
            await expect(icon).toBeVisible();
            // Check the icon does NOT contain checkmark (different visual treatment)
            const iconText = await icon.textContent();
            expect(iconText).not.toContain('✓');
        }
    });

    // Additional tests for accessibility and structure
    test('status section should have proper aria-labelledby', async ({ page }) => {
        const statusSection = page.locator('#status');
        await expect(statusSection).toHaveAttribute('aria-labelledby', 'status-title');

        const titleId = statusSection.locator('#status-title');
        await expect(titleId).toBeVisible();
    });

    test('status lists should have aria-label for accessibility', async ({ page }) => {
        const implementedList = page.locator('.status-column--implemented .status-list');
        await expect(implementedList).toHaveAttribute('aria-label', /implemented/i);

        const plannedList = page.locator('.status-column--planned .status-list');
        await expect(plannedList).toHaveAttribute('aria-label', /planned/i);
    });

    test('status column titles should include appropriate icons', async ({ page }) => {
        const implementedTitle = page.locator('.status-column--implemented .status-column-title');
        await expect(implementedTitle).toBeVisible();
        await expect(implementedTitle).toContainText(/implemented/i);

        // Check for checkmark icon in title
        const checkIcon = implementedTitle.locator('.status-icon--check');
        await expect(checkIcon).toBeVisible();

        const plannedTitle = page.locator('.status-column--planned .status-column-title');
        await expect(plannedTitle).toBeVisible();
        await expect(plannedTitle).toContainText(/roadmap/i);

        // Check for roadmap icon in title
        const roadmapIcon = plannedTitle.locator('.status-icon--roadmap');
        await expect(roadmapIcon).toBeVisible();
    });
});
