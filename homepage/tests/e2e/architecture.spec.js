/**
 * E2E Tests for Architecture Diagram (Scenario 4)
 *
 * Tests for the LSM tree architecture diagram section.
 * Validates presence, accessibility, and content of the SVG diagram.
 */
const { test, expect } = require('@playwright/test');

test.describe('Architecture Diagram', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
    });

    // Test Case 1: Query Architecture section for SVG element
    test('SVG element exists within #architecture section', async ({ page }) => {
        // Navigate to architecture section
        const architectureSection = page.locator('#architecture');
        await expect(architectureSection).toBeVisible();

        // Check for SVG with class .architecture-diagram or within #architecture
        const svg = architectureSection.locator('svg.architecture-diagram');
        await expect(svg).toBeVisible();
        await expect(svg).toHaveCount(1);
    });

    // Test Case 2: Check SVG has accessible attributes
    test('SVG has role="img" and aria-label describing the diagram', async ({ page }) => {
        const svg = page.locator('#architecture svg.architecture-diagram');

        // Check role attribute
        await expect(svg).toHaveAttribute('role', 'img');

        // Check aria-label attribute exists and describes the diagram
        const ariaLabel = await svg.getAttribute('aria-label');
        expect(ariaLabel).toBeTruthy();
        expect(ariaLabel.toLowerCase()).toContain('lsm tree');
        expect(ariaLabel.toLowerCase()).toContain('architecture');
    });

    // Test Case 3: Verify diagram shows Memtable component
    test('SVG contains text or element representing Memtable', async ({ page }) => {
        const svg = page.locator('#architecture svg.architecture-diagram');

        // Check for text element containing "Memtable"
        const memtableText = svg.locator('text:has-text("Memtable")');
        await expect(memtableText.first()).toBeVisible();
    });

    // Test Case 4: Verify diagram shows SSTable component
    test('SVG contains text or element representing SSTable', async ({ page }) => {
        const svg = page.locator('#architecture svg.architecture-diagram');

        // Check for text element containing "SSTable"
        const sstableText = svg.locator('text:has-text("SSTable")');
        await expect(sstableText.first()).toBeVisible();
    });

    // Test Case 5: Verify diagram shows data flow arrows
    test('SVG contains path or line elements indicating data flow direction', async ({ page }) => {
        const svg = page.locator('#architecture svg.architecture-diagram');

        // Check for arrow paths (data flow indicators)
        const arrowPaths = svg.locator('path[class*="arrow"]');
        const arrowCount = await arrowPaths.count();

        // Should have multiple arrow paths for data flow
        expect(arrowCount).toBeGreaterThan(0);

        // Also check for arrow lines in legend
        const arrowLines = svg.locator('line[class*="arrow"]');
        const lineCount = await arrowLines.count();

        // Total arrows (paths + lines) should indicate data flow
        expect(arrowCount + lineCount).toBeGreaterThan(3);
    });

    // Additional test for title element
    test('SVG has title element for accessibility', async ({ page }) => {
        const svg = page.locator('#architecture svg.architecture-diagram');
        const title = svg.locator('title');

        await expect(title).toHaveCount(1);
        const titleText = await title.textContent();
        expect(titleText).toBeTruthy();
        expect(titleText.toLowerCase()).toContain('mirdb');
    });

    // Additional test for desc element
    test('SVG has desc element for accessibility', async ({ page }) => {
        const svg = page.locator('#architecture svg.architecture-diagram');
        const desc = svg.locator('desc');

        await expect(desc).toHaveCount(1);
        const descText = await desc.textContent();
        expect(descText).toBeTruthy();
        expect(descText.toLowerCase()).toContain('lsm tree');
    });

    // Test for compaction process visibility
    test('Diagram shows compaction process', async ({ page }) => {
        const svg = page.locator('#architecture svg.architecture-diagram');

        // Check for text element containing "Compaction"
        const compactionText = svg.locator('text:has-text("Compaction")');
        await expect(compactionText.first()).toBeVisible();
    });

    // Test for WAL component
    test('Diagram shows WAL component', async ({ page }) => {
        const svg = page.locator('#architecture svg.architecture-diagram');

        // Check for text element containing "WAL"
        const walText = svg.locator('text:has-text("WAL")');
        await expect(walText.first()).toBeVisible();
    });

    // Test for Client component
    test('Diagram shows Client component', async ({ page }) => {
        const svg = page.locator('#architecture svg.architecture-diagram');

        // Check for text element containing "Client"
        const clientText = svg.locator('text:has-text("Client")');
        await expect(clientText.first()).toBeVisible();
    });
});
