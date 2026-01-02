// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for MirDB Landing Page Architecture Overview Section
 * These tests verify the architecture section displays LSM tree data flow diagram
 * and explanation as required by REQ-7 and NFR-3 (accessibility).
 */

test.describe('Architecture Overview Section', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
    });

    test('TC1: Architecture section exists on the page with heading', async ({ page }) => {
        // Test Case 1: Check architecture section presence
        // Expected: Architecture section exists on the page with heading

        const architectureSection = page.locator('#architecture');
        await expect(architectureSection).toBeVisible();

        const heading = architectureSection.locator('h2');
        await expect(heading).toBeVisible();

        const headingText = await heading.textContent();
        expect(headingText?.toLowerCase()).toContain('architecture');
    });

    test('TC2: Section contains an image or SVG diagram element', async ({ page }) => {
        // Test Case 2: Check for diagram/visual element
        // Expected: Section contains an image or SVG diagram element

        const architectureSection = page.locator('#architecture');
        await expect(architectureSection).toBeVisible();

        // Check for either an img element or an SVG element
        const imgDiagram = architectureSection.locator('img.architecture-diagram, img[alt*="LSM"], img[alt*="architecture"]');
        const svgDiagram = architectureSection.locator('svg.architecture-diagram, svg[role="img"]');

        // At least one type of diagram should be present
        const imgCount = await imgDiagram.count();
        const svgCount = await svgDiagram.count();

        expect(imgCount + svgCount).toBeGreaterThan(0);
    });

    test('TC3: Diagram has descriptive alt text for accessibility', async ({ page }) => {
        // Test Case 3: Verify diagram alt text
        // Expected: Diagram has descriptive alt text for accessibility (NFR-3: WCAG 2.1 Level AA)

        const architectureSection = page.locator('#architecture');
        await expect(architectureSection).toBeVisible();

        // Check for img with alt text
        const imgDiagram = architectureSection.locator('img.architecture-diagram, img[alt*="LSM"], img[alt*="architecture"]');
        const svgDiagram = architectureSection.locator('svg.architecture-diagram, svg[role="img"]');

        const imgCount = await imgDiagram.count();
        const svgCount = await svgDiagram.count();

        if (imgCount > 0) {
            // Check that img has non-empty alt text
            const altText = await imgDiagram.first().getAttribute('alt');
            expect(altText).toBeTruthy();
            expect(altText?.length).toBeGreaterThan(10); // Meaningful alt text should be descriptive
            // Alt text should describe the LSM tree or data flow
            expect(altText?.toLowerCase()).toMatch(/lsm|data flow|architecture|write|memtable|sstable/);
        }

        if (svgCount > 0) {
            // Check that SVG has accessible label via aria-label, aria-labelledby, or title
            const svg = svgDiagram.first();
            const ariaLabel = await svg.getAttribute('aria-label');
            const ariaLabelledby = await svg.getAttribute('aria-labelledby');
            const title = await svg.locator('title').textContent().catch(() => null);

            const hasAccessibleLabel = ariaLabel || ariaLabelledby || title;
            expect(hasAccessibleLabel).toBeTruthy();
        }
    });

    test('TC4: Text explanation mentioning WAL, Memtable, and SSTable pipeline', async ({ page }) => {
        // Test Case 4: Check for LSM tree explanation
        // Expected: Text explanation mentioning WAL, Memtable, and SSTable pipeline

        const architectureSection = page.locator('#architecture');
        await expect(architectureSection).toBeVisible();

        // Get all text content from the architecture section
        const sectionText = await architectureSection.textContent();
        const textLower = sectionText?.toLowerCase() || '';

        // Verify key LSM tree components are mentioned
        expect(textLower).toContain('wal');
        expect(textLower).toContain('memtable');
        expect(textLower).toContain('sstable');
    });

    test('Architecture section is within the main content area', async ({ page }) => {
        // Additional test for proper semantic structure
        const main = page.locator('main');
        const architectureSection = main.locator('#architecture');

        await expect(architectureSection).toBeVisible();
    });

    test('Architecture section has proper heading hierarchy', async ({ page }) => {
        // Accessibility test for heading hierarchy
        const architectureSection = page.locator('#architecture');
        const heading = architectureSection.locator('h2');

        await expect(heading).toBeVisible();
        // Ensure h2 is used (not h1 or h3-h6 for section heading)
        await expect(heading).toHaveCount(1);
    });
});
