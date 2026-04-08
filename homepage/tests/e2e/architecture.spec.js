/**
 * Architecture Section E2E Tests
 * Owner: Scenario 5 - Architecture Overview
 *
 * Test cases:
 * - Architecture section contains LSM tree diagram or description
 * - Memtable explanation with skip-list implementation
 * - SSTable persistent storage explanation
 * - Multi-level compaction strategy explanation
 */

const { test, expect } = require('@playwright/test');

test.describe('Architecture Overview', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
    });

    test('architecture section contains visual or textual representation of LSM tree architecture', async ({ page }) => {
        // Test Case ID: 1
        // Input: Query architecture section for LSM tree diagram or description
        // Expected: Section contains visual or textual representation of LSM tree architecture

        const archSection = page.locator('#architecture');

        // Scroll to the architecture section to trigger lazy loading
        await archSection.scrollIntoViewIfNeeded();
        await expect(archSection).toBeVisible();

        // Check for section title
        const archTitle = archSection.locator('#architecture-title');
        await expect(archTitle).toBeVisible();
        await expect(archTitle).toHaveText('Architecture');

        // Check for LSM tree diagram (wait for lazy load)
        const archDiagram = archSection.locator('.arch-diagram');
        await expect(archDiagram).toBeVisible({ timeout: 10000 });

        // Verify the diagram has proper alt text mentioning LSM
        const altText = await archDiagram.getAttribute('alt');
        expect(altText.toLowerCase()).toContain('lsm');

        // Check for subtitle mentioning LSM tree
        const archSubtitle = archSection.locator('.arch-subtitle');
        await expect(archSubtitle).toBeVisible();
        const subtitleText = await archSubtitle.textContent();
        expect(subtitleText.toLowerCase()).toContain('lsm');
    });

    test('architecture section explains memtable component with skip-list implementation', async ({ page }) => {
        // Test Case ID: 2
        // Input: Query for memtable explanation
        // Expected: Architecture section explains memtable component with skip-list implementation

        const archSection = page.locator('#architecture');
        await expect(archSection).toBeVisible();

        // Find the memtable component card
        const memtableComponent = archSection.locator('[data-component="memtable"]');
        await expect(memtableComponent).toBeVisible();

        // Check for Memtable title
        const memtableTitle = memtableComponent.locator('.arch-component-title');
        await expect(memtableTitle).toBeVisible();
        await expect(memtableTitle).toHaveText('Memtable');

        // Check for skip-list mention in description
        const memtableDescription = memtableComponent.locator('.arch-component-description');
        await expect(memtableDescription).toBeVisible();
        const descriptionText = await memtableDescription.textContent();
        expect(descriptionText.toLowerCase()).toContain('skip list');

        // Check for details list
        const memtableDetails = memtableComponent.locator('.arch-component-details li');
        const detailsCount = await memtableDetails.count();
        expect(detailsCount).toBeGreaterThan(0);
    });

    test('architecture section explains SSTable persistent storage', async ({ page }) => {
        // Test Case ID: 3
        // Input: Query for SSTable explanation
        // Expected: Architecture section explains SSTable persistent storage

        const archSection = page.locator('#architecture');
        await expect(archSection).toBeVisible();

        // Find the SSTable component card
        const sstableComponent = archSection.locator('[data-component="sstable"]');
        await expect(sstableComponent).toBeVisible();

        // Check for SSTable title
        const sstableTitle = sstableComponent.locator('.arch-component-title');
        await expect(sstableTitle).toBeVisible();
        await expect(sstableTitle).toHaveText('SSTable');

        // Check for persistent storage mention in description
        const sstableDescription = sstableComponent.locator('.arch-component-description');
        await expect(sstableDescription).toBeVisible();
        const descriptionText = await sstableDescription.textContent();
        expect(descriptionText.toLowerCase()).toContain('persistent');

        // Check that the description mentions storage
        expect(descriptionText.toLowerCase()).toContain('storage');

        // Check for details list
        const sstableDetails = sstableComponent.locator('.arch-component-details li');
        const detailsCount = await sstableDetails.count();
        expect(detailsCount).toBeGreaterThan(0);
    });

    test('architecture section explains multi-level compaction strategy', async ({ page }) => {
        // Test Case ID: 4
        // Input: Query for compaction explanation
        // Expected: Architecture section explains multi-level compaction strategy

        const archSection = page.locator('#architecture');
        await expect(archSection).toBeVisible();

        // Find the compaction component card
        const compactionComponent = archSection.locator('[data-component="compaction"]');
        await expect(compactionComponent).toBeVisible();

        // Check for compaction title (should mention multi-level)
        const compactionTitle = compactionComponent.locator('.arch-component-title');
        await expect(compactionTitle).toBeVisible();
        const titleText = await compactionTitle.textContent();
        expect(titleText.toLowerCase()).toContain('compaction');
        expect(titleText.toLowerCase()).toContain('multi-level');

        // Check for compaction description
        const compactionDescription = compactionComponent.locator('.arch-component-description');
        await expect(compactionDescription).toBeVisible();
        const descriptionText = await compactionDescription.textContent();
        expect(descriptionText.toLowerCase()).toContain('compaction');

        // Check for minor and major compaction mentions
        expect(descriptionText.toLowerCase()).toContain('minor');
        expect(descriptionText.toLowerCase()).toContain('major');

        // Check for details about levels
        const compactionDetails = compactionComponent.locator('.arch-component-details li');
        const detailsCount = await compactionDetails.count();
        expect(detailsCount).toBeGreaterThan(0);
    });

    test('architecture diagram is accessible with proper attributes', async ({ page }) => {
        // Additional test for accessibility
        const archSection = page.locator('#architecture');
        await expect(archSection).toBeVisible();

        // Check that the section has proper aria-labelledby
        await expect(archSection).toHaveAttribute('aria-labelledby', 'architecture-title');

        // Check diagram wrapper exists
        const diagramWrapper = archSection.locator('.arch-diagram-wrapper');
        await expect(diagramWrapper).toBeVisible();

        // Check diagram image has alt text
        const diagram = archSection.locator('.arch-diagram');
        const altText = await diagram.getAttribute('alt');
        expect(altText).toBeTruthy();
        expect(altText.length).toBeGreaterThan(10);
    });

    test('data flow section explains the write-read path', async ({ page }) => {
        // Additional test for data flow explanation
        const archSection = page.locator('#architecture');
        await expect(archSection).toBeVisible();

        // Check data flow section exists
        const dataFlow = archSection.locator('.arch-data-flow');
        await expect(dataFlow).toBeVisible();

        // Check for flow steps
        const flowSteps = dataFlow.locator('.arch-flow-step');
        const stepsCount = await flowSteps.count();
        expect(stepsCount).toBe(4); // Write, Flush, Compact, Read

        // Check that Write and Read steps are present
        const dataFlowText = await dataFlow.textContent();
        expect(dataFlowText).toContain('Write');
        expect(dataFlowText).toContain('Read');
    });
});
