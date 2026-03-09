/**
 * Architecture Section E2E Tests
 * Owner: Scenario 3 - Architecture Section and Diagram
 *
 * Tests:
 * - Architecture diagram present (SVG or image)
 * - WAL component explained
 * - Memtable component explained
 * - SSTable component explained
 * - Diagram responsive on mobile
 */

const { test, expect } = require('@playwright/test');

test.describe('Architecture Section', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
        await page.waitForLoadState('domcontentloaded');
    });

    test('TC1: Architecture section contains diagram with proper accessibility', async ({ page }) => {
        // Navigate to architecture section
        const architectureSection = page.locator('#architecture');
        await expect(architectureSection).toBeVisible();

        // Check for SVG diagram with role="img" and aria-label
        const diagram = architectureSection.locator('svg.architecture-diagram');
        await expect(diagram).toBeVisible();

        // Verify SVG has role="img" for accessibility
        await expect(diagram).toHaveAttribute('role', 'img');

        // Verify SVG has aria-label describing the diagram
        const ariaLabel = await diagram.getAttribute('aria-label');
        expect(ariaLabel).toBeTruthy();
        expect(ariaLabel.toLowerCase()).toContain('architecture');

        // Check for title and desc elements within SVG for screen readers
        // These are metadata elements so we check for their presence, not visibility
        const svgTitle = diagram.locator('title');
        await expect(svgTitle).toHaveCount(1);
        const titleText = await svgTitle.textContent();
        expect(titleText.length).toBeGreaterThan(0);

        const svgDesc = diagram.locator('desc');
        await expect(svgDesc).toHaveCount(1);

        // Verify the desc contains meaningful content
        const descText = await svgDesc.textContent();
        expect(descText.length).toBeGreaterThan(50);
    });

    test('TC2: Architecture section explains WAL (Write-Ahead Log)', async ({ page }) => {
        const architectureSection = page.locator('#architecture');
        await expect(architectureSection).toBeVisible();

        // Check for WAL explanation in the components section
        const walSection = architectureSection.locator('#wal-description');
        await expect(walSection).toBeVisible();

        // Verify the heading contains WAL or Write-Ahead Log
        const walHeading = walSection.locator('h3');
        const headingText = await walHeading.textContent();
        expect(
            headingText.includes('WAL') || headingText.includes('Write-Ahead Log')
        ).toBeTruthy();

        // Verify there is a description paragraph
        const walDescription = walSection.locator('p');
        const descriptionText = await walDescription.first().textContent();
        expect(descriptionText.length).toBeGreaterThan(50);

        // Verify description mentions durability or persistence
        expect(
            descriptionText.toLowerCase().includes('durability') ||
            descriptionText.toLowerCase().includes('persist') ||
            descriptionText.toLowerCase().includes('crash')
        ).toBeTruthy();
    });

    test('TC3: Architecture section explains memtable (in-memory storage)', async ({ page }) => {
        const architectureSection = page.locator('#architecture');
        await expect(architectureSection).toBeVisible();

        // Check for memtable explanation
        const memtableSection = architectureSection.locator('#memtable-description');
        await expect(memtableSection).toBeVisible();

        // Verify the heading contains memtable
        const memtableHeading = memtableSection.locator('h3');
        const headingText = await memtableHeading.textContent();
        expect(headingText.toLowerCase()).toContain('memtable');

        // Verify description paragraph exists
        const memtableDescription = memtableSection.locator('p');
        const descriptionText = await memtableDescription.first().textContent();
        expect(descriptionText.length).toBeGreaterThan(50);

        // Verify description mentions in-memory or skip list
        expect(
            descriptionText.toLowerCase().includes('in-memory') ||
            descriptionText.toLowerCase().includes('memory') ||
            descriptionText.toLowerCase().includes('skip list')
        ).toBeTruthy();
    });

    test('TC4: Architecture section explains SSTable (Sorted String Table)', async ({ page }) => {
        const architectureSection = page.locator('#architecture');
        await expect(architectureSection).toBeVisible();

        // Check for SSTable explanation
        const sstableSection = architectureSection.locator('#sstable-description');
        await expect(sstableSection).toBeVisible();

        // Verify the heading contains SSTable or Sorted String Table
        const sstableHeading = sstableSection.locator('h3');
        const headingText = await sstableHeading.textContent();
        expect(
            headingText.includes('SSTable') || headingText.includes('Sorted String Table')
        ).toBeTruthy();

        // Verify description paragraph exists
        const sstableDescription = sstableSection.locator('p');
        const descriptionText = await sstableDescription.first().textContent();
        expect(descriptionText.length).toBeGreaterThan(50);

        // Verify description mentions disk storage or sorted keys
        expect(
            descriptionText.toLowerCase().includes('disk') ||
            descriptionText.toLowerCase().includes('sorted') ||
            descriptionText.toLowerCase().includes('immutable')
        ).toBeTruthy();
    });

    test('TC5: Architecture diagram is responsive on mobile viewport', async ({ page }) => {
        // Set mobile viewport
        await page.setViewportSize({ width: 375, height: 667 });

        // Reload page at mobile size
        await page.goto('/');
        await page.waitForLoadState('domcontentloaded');

        const architectureSection = page.locator('#architecture');
        await expect(architectureSection).toBeVisible();

        // Scroll to architecture section
        await architectureSection.scrollIntoViewIfNeeded();

        // Get the diagram wrapper
        const diagramWrapper = page.locator('.architecture-diagram-wrapper');
        await expect(diagramWrapper).toBeVisible();

        // Get the SVG diagram
        const diagram = page.locator('.architecture-diagram');
        await expect(diagram).toBeVisible();

        // Check that the diagram doesn't cause horizontal overflow
        const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
        const viewportWidth = await page.evaluate(() => window.innerWidth);

        // The body scroll width should not exceed viewport width significantly
        // Small tolerance for sub-pixel rendering
        expect(bodyScrollWidth).toBeLessThanOrEqual(viewportWidth + 5);

        // Verify the diagram is visible and has reasonable dimensions
        const diagramBoundingBox = await diagram.boundingBox();
        expect(diagramBoundingBox).toBeTruthy();
        expect(diagramBoundingBox.width).toBeLessThanOrEqual(viewportWidth);
        expect(diagramBoundingBox.width).toBeGreaterThan(200); // Ensure it's still visible

        // Verify the diagram wrapper doesn't overflow
        const wrapperStyles = await diagramWrapper.evaluate(el => {
            const styles = window.getComputedStyle(el);
            return {
                overflow: styles.overflow,
                overflowX: styles.overflowX
            };
        });

        // Overflow should be hidden or not set to visible
        expect(wrapperStyles.overflowX !== 'visible' || wrapperStyles.overflow === 'hidden').toBeTruthy();
    });

    test('Architecture section has proper heading structure', async ({ page }) => {
        const architectureSection = page.locator('#architecture');
        await expect(architectureSection).toBeVisible();

        // Check for h2 section heading
        const sectionHeading = architectureSection.locator('h2');
        await expect(sectionHeading).toBeVisible();
        const headingText = await sectionHeading.textContent();
        expect(headingText).toContain('Architecture');

        // Check for component h3 headings
        const componentHeadings = architectureSection.locator('.arch-component h3');
        const headingCount = await componentHeadings.count();
        expect(headingCount).toBeGreaterThanOrEqual(3); // WAL, Memtable, SSTable at minimum
    });

    test('Architecture diagram contains key components labeled', async ({ page }) => {
        const diagram = page.locator('.architecture-diagram');
        await expect(diagram).toBeVisible();

        // Get the SVG content
        const svgContent = await diagram.innerHTML();

        // Verify WAL is labeled in the diagram
        expect(svgContent).toContain('WAL');

        // Verify Memtable is labeled
        expect(svgContent).toContain('Memtable');

        // Verify SSTable is referenced (Level 0 or Level 1+)
        expect(svgContent.includes('SSTable') || svgContent.includes('SSTs')).toBeTruthy();

        // Verify compaction is mentioned
        expect(svgContent.toLowerCase()).toContain('compaction');
    });

    test('Architecture section intro paragraph provides context', async ({ page }) => {
        const architectureSection = page.locator('#architecture');
        await expect(architectureSection).toBeVisible();

        // Check for intro paragraph
        const introParagraph = architectureSection.locator('.section-intro');
        await expect(introParagraph).toBeVisible();

        const introText = await introParagraph.textContent();

        // Should mention LSM tree
        expect(introText.toLowerCase()).toContain('lsm');

        // Should provide meaningful context (at least 50 characters)
        expect(introText.length).toBeGreaterThan(50);
    });

    test('Compaction description is present', async ({ page }) => {
        const architectureSection = page.locator('#architecture');
        await expect(architectureSection).toBeVisible();

        // Check for compaction explanation
        const compactionSection = architectureSection.locator('#compaction-description');
        await expect(compactionSection).toBeVisible();

        // Verify heading
        const compactionHeading = compactionSection.locator('h3');
        const headingText = await compactionHeading.textContent();
        expect(headingText.toLowerCase()).toContain('compaction');

        // Verify minor and major compaction are explained
        const compactionText = await compactionSection.textContent();
        expect(compactionText.toLowerCase()).toContain('minor');
        expect(compactionText.toLowerCase()).toContain('major');
    });
});
