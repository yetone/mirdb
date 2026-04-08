/**
 * Project Status Section E2E Tests
 * Owner: Scenario 7 - Project Status Section
 *
 * Test cases:
 * - Implemented features (memcached protocol, LSM tree, persistence) are marked as complete
 * - Planned features (Raft) are marked as upcoming/planned
 * - Visual distinction between implemented and planned features (icons, colors, badges)
 */

const { test, expect } = require('@playwright/test');

test.describe('Project Status Section', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
    });

    test('status section displays implemented features marked as complete', async ({ page }) => {
        // Test Case ID: 1
        // Input: Query status section for implemented features
        // Expected: Implemented features (memcached protocol, LSM tree, persistence) are marked as complete

        const statusSection = page.locator('#status');
        await expect(statusSection).toBeVisible();

        // Check that the section has implemented features
        const implementedFeatures = statusSection.locator('.status-feature[data-status="implemented"], .status-feature.status-implemented');
        const featureCount = await implementedFeatures.count();
        expect(featureCount).toBeGreaterThanOrEqual(3);

        // Verify specific implemented features are present
        const statusContent = await statusSection.textContent();
        expect(statusContent.toLowerCase()).toContain('memcached');
        expect(statusContent.toLowerCase()).toContain('lsm');
        expect(statusContent.toLowerCase()).toContain('persist');

        // Check that implemented features have completed status indicator
        const completedBadges = statusSection.locator('.status-badge-implemented, .status-complete, [data-status="implemented"]');
        await expect(completedBadges.first()).toBeVisible();
    });

    test('status section displays planned features marked as upcoming', async ({ page }) => {
        // Test Case ID: 2
        // Input: Query status section for planned features
        // Expected: Planned features like Raft are marked as upcoming/planned

        const statusSection = page.locator('#status');
        await expect(statusSection).toBeVisible();

        // Check that planned features exist
        const plannedFeatures = statusSection.locator('.status-feature[data-status="planned"], .status-feature.status-planned');
        const featureCount = await plannedFeatures.count();
        expect(featureCount).toBeGreaterThanOrEqual(1);

        // Verify Raft is mentioned as a planned feature
        const statusContent = await statusSection.textContent();
        expect(statusContent.toLowerCase()).toContain('raft');

        // Check that planned features have upcoming/planned status indicator
        const plannedBadges = statusSection.locator('.status-badge-planned, .status-upcoming, [data-status="planned"]');
        await expect(plannedBadges.first()).toBeVisible();
    });

    test('implemented and planned features have clear visual differentiation', async ({ page }) => {
        // Test Case ID: 3
        // Input: Verify visual distinction between implemented and planned
        // Expected: Implemented and planned features have clear visual differentiation (icons, colors, badges)

        const statusSection = page.locator('#status');
        await expect(statusSection).toBeVisible();

        // Get an implemented feature
        const implementedFeature = statusSection.locator('.status-feature[data-status="implemented"], .status-feature.status-implemented').first();
        await expect(implementedFeature).toBeVisible();

        // Get a planned feature
        const plannedFeature = statusSection.locator('.status-feature[data-status="planned"], .status-feature.status-planned').first();
        await expect(plannedFeature).toBeVisible();

        // Check for visual indicators (badges, icons, or distinct elements)
        const implementedBadge = implementedFeature.locator('.status-badge, .status-icon, svg');
        const plannedBadge = plannedFeature.locator('.status-badge, .status-icon, svg');

        await expect(implementedBadge.first()).toBeVisible();
        await expect(plannedBadge.first()).toBeVisible();

        // Verify different styling by checking CSS classes or computed styles
        const implementedClass = await implementedFeature.getAttribute('class');
        const plannedClass = await plannedFeature.getAttribute('class');

        // They should have different styling classes or data attributes
        const implementedStatus = await implementedFeature.getAttribute('data-status') || implementedClass;
        const plannedStatus = await plannedFeature.getAttribute('data-status') || plannedClass;

        // Ensure the two statuses are different
        expect(implementedStatus).not.toEqual(plannedStatus);

        // Verify color differentiation - implemented should use success/green, planned should use different color
        const implementedBadgeElement = statusSection.locator('.status-badge-implemented, [data-status="implemented"] .status-badge').first();
        const plannedBadgeElement = statusSection.locator('.status-badge-planned, [data-status="planned"] .status-badge').first();

        // Both badges should be visible and have text content
        await expect(implementedBadgeElement).toBeVisible();
        await expect(plannedBadgeElement).toBeVisible();

        const implementedBadgeText = await implementedBadgeElement.textContent();
        const plannedBadgeText = await plannedBadgeElement.textContent();

        // Badge text should be different (e.g., "Complete" vs "Planned")
        expect(implementedBadgeText).not.toEqual(plannedBadgeText);
    });

    test('status section has proper heading and structure', async ({ page }) => {
        // Additional test for proper semantic structure
        const statusSection = page.locator('#status');
        await expect(statusSection).toBeVisible();

        // Check for proper heading
        const heading = statusSection.locator('h2');
        await expect(heading).toBeVisible();
        await expect(heading).toContainText('Status');

        // Check that the main grid container exists
        const gridContainer = statusSection.locator('.status-grid');
        await expect(gridContainer).toBeVisible();

        // Check that features containers exist (there are multiple)
        const featuresContainers = statusSection.locator('.status-features');
        const count = await featuresContainers.count();
        expect(count).toBeGreaterThanOrEqual(2);
    });

    test('implemented features show checkmark or completion icon', async ({ page }) => {
        // Verify visual icon differentiation
        const statusSection = page.locator('#status');

        const implementedFeature = statusSection.locator('[data-status="implemented"]').first();
        await expect(implementedFeature).toBeVisible();

        // Check for SVG icon or visual indicator
        const icon = implementedFeature.locator('svg, .status-icon');
        await expect(icon.first()).toBeVisible();
    });
});
