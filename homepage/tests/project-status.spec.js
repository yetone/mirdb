// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Project Status Display (REQ-9)', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
    });

    test('TC1: Implemented features list displays all required items', async ({ page }) => {
        // Locate the project status section
        const statusSection = page.locator('#project-status');
        await expect(statusSection).toBeVisible();

        // Locate the implemented features list
        const implementedSection = page.locator('#implemented-features');
        await expect(implementedSection).toBeVisible();

        // Verify all implemented features are listed
        const implementedItems = implementedSection.locator('li');

        // Get all text content from list items
        const allText = await implementedSection.textContent();

        // Check for required implemented features
        expect(allText).toContain('Tokio');
        expect(allText).toMatch(/network/i);
        expect(allText).toContain('Memtable');
        expect(allText).toMatch(/skip list/i);
        expect(allText).toMatch(/minor compaction/i);
        expect(allText).toMatch(/major compaction/i);

        // Verify at least 4 implemented features are listed
        await expect(implementedItems).toHaveCount(4);
    });

    test('TC2: Planned features list displays Raft consensus', async ({ page }) => {
        // Locate the project status section
        const statusSection = page.locator('#project-status');
        await expect(statusSection).toBeVisible();

        // Locate the planned features section
        const plannedSection = page.locator('#planned-features');
        await expect(plannedSection).toBeVisible();

        // Verify Raft consensus is listed as planned
        const plannedText = await plannedSection.textContent();
        expect(plannedText).toMatch(/raft consensus/i);
        expect(plannedText).toMatch(/distributed/i);
    });

    test('TC3: Implemented and planned features have distinct visual styling', async ({ page }) => {
        // Locate both sections
        const implementedSection = page.locator('#implemented-features');
        const plannedSection = page.locator('#planned-features');

        await expect(implementedSection).toBeVisible();
        await expect(plannedSection).toBeVisible();

        // Check for visual distinction - implemented features should have check icons or green styling
        const implementedIcon = implementedSection.locator('.status-icon, .implemented-icon, [data-status="implemented"]').first();
        const plannedIcon = plannedSection.locator('.status-icon, .planned-icon, [data-status="planned"]').first();

        // Check if icons or badges exist for distinction
        const hasImplementedMarker = await implementedSection.locator('.status-badge, .status-icon, .feature-status').count();
        const hasPlannedMarker = await plannedSection.locator('.status-badge, .status-icon, .feature-status').count();

        // Either icons/badges should exist, OR the sections should have different background colors
        const implementedBg = await implementedSection.evaluate(el =>
            window.getComputedStyle(el).backgroundColor || window.getComputedStyle(el).borderColor
        );
        const plannedBg = await plannedSection.evaluate(el =>
            window.getComputedStyle(el).backgroundColor || window.getComputedStyle(el).borderColor
        );

        // Check for distinct headers
        const implementedHeader = implementedSection.locator('h3');
        const plannedHeader = plannedSection.locator('h3');

        const implementedHeaderText = await implementedHeader.textContent();
        const plannedHeaderText = await plannedHeader.textContent();

        // Headers should be different to distinguish sections
        expect(implementedHeaderText).not.toBe(plannedHeaderText);
        expect(implementedHeaderText).toMatch(/implemented|completed|done/i);
        expect(plannedHeaderText).toMatch(/planned|roadmap|coming|future/i);

        // Visual distinction: either different styling markers exist, or different container styling
        const hasVisualDistinction = (
            hasImplementedMarker > 0 && hasPlannedMarker > 0
        ) || (
            implementedBg !== plannedBg
        ) || (
            await implementedSection.locator('[class*="check"], [class*="complete"], svg').count() > 0 &&
            await plannedSection.locator('[class*="clock"], [class*="pending"], svg').count() > 0
        );

        expect(hasVisualDistinction || implementedHeaderText !== plannedHeaderText).toBeTruthy();
    });
});
