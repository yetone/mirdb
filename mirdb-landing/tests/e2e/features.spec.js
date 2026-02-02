/**
 * Features Section E2E Tests
 * Owner: Scenario 2 - Feature Showcase Section
 *
 * Tests:
 * - All 6 features displayed
 * - Feature content validation
 * - Visual layout verification
 */

import { test, expect } from '@playwright/test';

test.describe('Feature Showcase Section', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
    });

    test('features section is visible', async ({ page }) => {
        const featuresSection = page.locator('#features');
        await expect(featuresSection).toBeVisible();
    });

    test('test case 1: Memcached Protocol feature exists', async ({ page }) => {
        // Query feature section for 'Memcached Protocol' content
        const featureCard = page.locator('[data-feature="memcached-protocol"]');
        await expect(featureCard).toBeVisible();

        // Verify it contains the expected content
        const title = featureCard.locator('.feature-title');
        await expect(title).toContainText('Memcached Protocol');

        const description = featureCard.locator('.feature-description');
        await expect(description).toContainText('memcached');
    });

    test('test case 2: Persistent Storage feature exists', async ({ page }) => {
        // Query feature section for 'Persistent Storage' content
        const featureCard = page.locator('[data-feature="persistent-storage"]');
        await expect(featureCard).toBeVisible();

        // Verify it mentions SSTables
        const title = featureCard.locator('.feature-title');
        await expect(title).toContainText('Persistent Storage');
        await expect(title).toContainText('SSTables');

        const description = featureCard.locator('.feature-description');
        await expect(description).toContainText('SSTable');
    });

    test('test case 3: LSM-Tree Architecture feature exists', async ({ page }) => {
        // Query feature section for 'LSM-Tree' content
        const featureCard = page.locator('[data-feature="lsm-tree"]');
        await expect(featureCard).toBeVisible();

        const title = featureCard.locator('.feature-title');
        await expect(title).toContainText('LSM-Tree');

        const description = featureCard.locator('.feature-description');
        await expect(description).toContainText('Log-Structured Merge-tree');
    });

    test('test case 4: Async Tokio Networking feature exists', async ({ page }) => {
        // Query feature section for 'Async' or 'Tokio' content
        const featureCard = page.locator('[data-feature="async-networking"]');
        await expect(featureCard).toBeVisible();

        const title = featureCard.locator('.feature-title');
        await expect(title).toContainText('Tokio');
        await expect(title).toContainText('Async');

        const description = featureCard.locator('.feature-description');
        await expect(description).toContainText('async');
    });

    test('test case 5: Skip List Memtable feature exists', async ({ page }) => {
        // Query feature section for 'Skip List' or 'Memtable' content
        const featureCard = page.locator('[data-feature="skip-list"]');
        await expect(featureCard).toBeVisible();

        const title = featureCard.locator('.feature-title');
        await expect(title).toContainText('Skip List');
        await expect(title).toContainText('Memtable');

        const description = featureCard.locator('.feature-description');
        await expect(description).toContainText('skip list');
    });

    test('test case 6: Compaction Support feature exists', async ({ page }) => {
        // Query feature section for 'Compaction' content
        const featureCard = page.locator('[data-feature="compaction"]');
        await expect(featureCard).toBeVisible();

        const title = featureCard.locator('.feature-title');
        await expect(title).toContainText('Compaction');

        const description = featureCard.locator('.feature-description');
        await expect(description).toContainText(/minor/i);
        await expect(description).toContainText(/major/i);
    });

    test('test case 7: at least 6 feature items are displayed', async ({ page }) => {
        // Count total feature items displayed
        const featureCards = page.locator('#features .feature-card');
        const count = await featureCards.count();

        // Verify at least 6 feature items
        expect(count).toBeGreaterThanOrEqual(6);
    });

    test('features grid layout is displayed', async ({ page }) => {
        // Verify the grid layout exists
        const featuresGrid = page.locator('.features-grid');
        await expect(featuresGrid).toBeVisible();
    });

    test('feature cards have icons', async ({ page }) => {
        // Verify all feature cards have icons
        const featureIcons = page.locator('.feature-card .feature-icon');
        const count = await featureIcons.count();

        expect(count).toBeGreaterThanOrEqual(6);
    });

    test('features section has proper heading', async ({ page }) => {
        // Verify the section has a heading
        const heading = page.locator('#features-heading');
        await expect(heading).toBeVisible();
        await expect(heading).toContainText('Key Features');
    });

    test('features section has subtitle', async ({ page }) => {
        // Verify the section has a subtitle
        const subtitle = page.locator('.features-subtitle');
        await expect(subtitle).toBeVisible();
    });

    test('features section is accessible with proper aria-labelledby', async ({ page }) => {
        // Verify the section has proper accessibility attributes
        const featuresSection = page.locator('#features');
        const ariaLabelledBy = await featuresSection.getAttribute('aria-labelledby');
        expect(ariaLabelledBy).toBe('features-heading');
    });
});
