// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Architecture Section', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
    });

    test('TC1: Architecture diagram is present and visible', async ({ page }) => {
        // Navigate to architecture section
        const architectureSection = page.locator('[data-testid="architecture-section"]');
        await expect(architectureSection).toBeVisible();

        // Check for architecture diagram container
        const diagramContainer = page.locator('[data-testid="architecture-diagram"]');
        await expect(diagramContainer).toBeVisible();

        // Check for SVG diagram element
        const svgDiagram = page.locator('[data-testid="lsm-tree-diagram"]');
        await expect(svgDiagram).toBeVisible();

        // Verify SVG has proper role attribute for accessibility
        await expect(svgDiagram).toHaveAttribute('role', 'img');
    });

    test('TC2: Architecture diagram has descriptive alt text', async ({ page }) => {
        // Check for SVG diagram
        const svgDiagram = page.locator('[data-testid="lsm-tree-diagram"]');
        await expect(svgDiagram).toBeVisible();

        // Verify aria-label contains descriptive text about LSM tree architecture
        const ariaLabel = await svgDiagram.getAttribute('aria-label');
        expect(ariaLabel).toBeTruthy();
        expect(ariaLabel).toContain('LSM');
        expect(ariaLabel).toContain('data flow');
        expect(ariaLabel).toContain('WAL');
        expect(ariaLabel).toContain('Memtable');
        expect(ariaLabel).toContain('SSTable');

        // Verify SVG has a title element for additional accessibility
        // Note: SVG title elements are not "visible" in the DOM but exist for screen readers
        const svgTitle = svgDiagram.locator('title');
        await expect(svgTitle).toHaveCount(1);
        const titleText = await svgTitle.textContent();
        expect(titleText).toContain('LSM Tree');

        // Verify SVG has a desc element for detailed description
        const svgDesc = svgDiagram.locator('desc');
        await expect(svgDesc).toHaveCount(1);
        const descText = await svgDesc.textContent();
        expect(descText).toContain('LSM tree data flow');
        expect(descText).toContain('Write-Ahead Log');
    });

    test('TC3: LSM tree explanation text is provided near the diagram', async ({ page }) => {
        // Check for architecture explanation section
        const explanationSection = page.locator('[data-testid="architecture-explanation"]');
        await expect(explanationSection).toBeVisible();

        // Verify explanation contains LSM tree content
        const explanationText = await explanationSection.textContent();
        expect(explanationText).toContain('LSM');
        expect(explanationText).toContain('Log-Structured Merge');

        // Check for "How LSM Trees Work" heading
        const headings = explanationSection.locator('h3');
        const headingsText = await headings.allTextContents();
        expect(headingsText.some(h => h.includes('LSM'))).toBeTruthy();

        // Check for key benefits list
        const benefitsList = page.locator('[data-testid="architecture-benefits"]');
        await expect(benefitsList).toBeVisible();

        // Verify benefits include key LSM tree advantages
        const benefitsText = await benefitsList.textContent();
        expect(benefitsText).toContain('write throughput');
        expect(benefitsText).toContain('Write-Ahead Log');
        expect(benefitsText).toContain('compaction');
    });

    test('TC4: Architecture diagram loads correctly without errors', async ({ page }) => {
        // Check that the SVG diagram is present and has content
        const svgDiagram = page.locator('[data-testid="lsm-tree-diagram"]');
        await expect(svgDiagram).toBeVisible();

        // Verify SVG has viewBox attribute (proper SVG structure)
        await expect(svgDiagram).toHaveAttribute('viewBox');

        // Verify SVG contains expected elements (boxes for each component)
        const rects = svgDiagram.locator('rect');
        const rectCount = await rects.count();
        expect(rectCount).toBeGreaterThanOrEqual(5); // WAL, Memtable, Imm Memtables, Level 0, Level 1+

        // Verify SVG contains text labels
        const texts = svgDiagram.locator('text');
        const textCount = await texts.count();
        expect(textCount).toBeGreaterThanOrEqual(5);

        // Verify key component labels are present
        const allText = await svgDiagram.textContent();
        expect(allText).toContain('WAL');
        expect(allText).toContain('Memtable');
        expect(allText).toContain('Level 0');
        expect(allText).toContain('Level 1+');

        // Verify arrows/paths exist for data flow
        const paths = svgDiagram.locator('path');
        const pathCount = await paths.count();
        expect(pathCount).toBeGreaterThanOrEqual(4); // At least 4 arrows connecting components
    });

    test('Architecture section has proper heading hierarchy', async ({ page }) => {
        // Check section title
        const sectionTitle = page.locator('#architecture .section-title');
        await expect(sectionTitle).toBeVisible();
        await expect(sectionTitle).toHaveText('Architecture Overview');

        // Check section subtitle
        const sectionSubtitle = page.locator('#architecture .section-subtitle');
        await expect(sectionSubtitle).toBeVisible();
        const subtitleText = await sectionSubtitle.textContent();
        expect(subtitleText).toContain('LSM tree');
    });

    test('Navigation link to architecture section works', async ({ page }) => {
        // Find and click the architecture nav link
        const architectureLink = page.locator('[data-testid="nav-link-architecture"]');
        await expect(architectureLink).toBeVisible();
        await architectureLink.click();

        // Verify the architecture section is in view
        const architectureSection = page.locator('[data-testid="architecture-section"]');
        await expect(architectureSection).toBeInViewport();
    });
});
