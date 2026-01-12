// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Architecture Overview - Scenario 6 (REQ-7)', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
    });

    // Test Case 1: Check for architecture diagram image with proper alt text
    test('TC1: should display architecture diagram with proper alt text describing LSM tree structure', async ({ page }) => {
        // Navigate to architecture section
        const archSection = page.locator('#architecture');
        await expect(archSection).toBeVisible();

        // Check the section title
        const title = archSection.locator('h2');
        await expect(title).toHaveText('Architecture Overview');

        // Check for architecture diagram image
        const diagram = archSection.locator('#architecture-diagram');
        await expect(diagram).toBeVisible();

        // Verify the image has proper alt text describing LSM tree structure
        const altText = await diagram.getAttribute('alt');
        expect(altText).toBeTruthy();
        expect(altText).toContain('LSM');
        expect(altText).toContain('WAL');
        expect(altText).toContain('Memtable');
        expect(altText).toContain('SSTable');

        // Verify the image source exists
        const src = await diagram.getAttribute('src');
        expect(src).toBeTruthy();
        expect(src).toContain('lsm-architecture');
    });

    // Test Case 2: Check write path explanation
    test('TC2: should explain write path: WAL -> Memtable -> Immutable Memtables -> SSTable levels', async ({ page }) => {
        // Navigate to architecture section
        const archSection = page.locator('#architecture');
        await expect(archSection).toBeVisible();

        // Check for write path explanation section
        const writePath = archSection.locator('#write-path');
        await expect(writePath).toBeVisible();

        // Check the heading
        const writePathHeading = writePath.locator('h3');
        await expect(writePathHeading).toHaveText('Write Path');

        // Get the write path content
        const writePathContent = await writePath.textContent();

        // Verify WAL is explained
        expect(writePathContent).toContain('WAL');
        expect(writePathContent).toMatch(/Write-Ahead Log|write-ahead log/i);

        // Verify Memtable is explained
        expect(writePathContent).toContain('Memtable');

        // Verify Immutable Memtables are explained
        expect(writePathContent).toMatch(/Immutable Memtable/i);

        // Verify SSTables are explained
        expect(writePathContent).toContain('SSTable');

        // Verify the flow order is explained (checking for Level 0 and higher levels)
        expect(writePathContent).toContain('Level 0');
        expect(writePathContent).toMatch(/Level 1|higher level/i);

        // Verify compaction is mentioned
        expect(writePathContent).toMatch(/compaction/i);
    });

    // Test Case 3: Check read path explanation
    test('TC3: should explain read path: memtable first, then SSTables', async ({ page }) => {
        // Navigate to architecture section
        const archSection = page.locator('#architecture');
        await expect(archSection).toBeVisible();

        // Check for read path explanation section
        const readPath = archSection.locator('#read-path');
        await expect(readPath).toBeVisible();

        // Check the heading
        const readPathHeading = readPath.locator('h3');
        await expect(readPathHeading).toHaveText('Read Path');

        // Get the read path content
        const readPathContent = await readPath.textContent();

        // Verify memtable is checked first
        expect(readPathContent).toContain('Memtable');
        expect(readPathContent).toMatch(/first|active/i);

        // Verify immutable memtables are searched
        expect(readPathContent).toMatch(/Immutable Memtable/i);

        // Verify SSTables are searched
        expect(readPathContent).toContain('SSTable');

        // Verify the search order logic (memtable -> immutable -> SSTables)
        const steps = readPath.locator('.path-steps li');
        await expect(steps).toHaveCount(3);

        // First step should mention memtable
        const firstStep = await steps.nth(0).textContent();
        expect(firstStep).toContain('Memtable');

        // Second step should mention immutable memtables
        const secondStep = await steps.nth(1).textContent();
        expect(secondStep).toMatch(/Immutable/i);

        // Third step should mention SSTables
        const thirdStep = await steps.nth(2).textContent();
        expect(thirdStep).toContain('SSTable');
    });

    // Additional test: Verify architecture section has both diagram and explanations
    test('should have complete architecture section with diagram and data flow explanations', async ({ page }) => {
        const archSection = page.locator('#architecture');
        await expect(archSection).toBeVisible();

        // Check for diagram container
        const diagramContainer = archSection.locator('.architecture-diagram');
        await expect(diagramContainer).toBeVisible();

        // Check for diagram image
        const diagram = diagramContainer.locator('img');
        await expect(diagram).toBeVisible();

        // Check for data flow explanation container
        const dataFlowExplanation = archSection.locator('.data-flow-explanation');
        await expect(dataFlowExplanation).toBeVisible();

        // Check for both path explanations
        const pathExplanations = archSection.locator('.path-explanation');
        await expect(pathExplanations).toHaveCount(2);
    });

    // Additional test: Verify write path explains the complete data flow
    test('should explain minor and major compaction in write path', async ({ page }) => {
        const writePath = page.locator('#write-path');
        await expect(writePath).toBeVisible();

        const writePathContent = await writePath.textContent();

        // Verify minor compaction is explained (memtable to Level 0)
        expect(writePathContent).toMatch(/minor compaction/i);

        // Verify major compaction is explained (Level to Level merging)
        expect(writePathContent).toMatch(/major compaction/i);
    });

    // Additional test: Verify diagram has proper accessibility attributes
    test('should have accessible architecture diagram with descriptive alt text', async ({ page }) => {
        const diagram = page.locator('#architecture-diagram');
        await expect(diagram).toBeVisible();

        // Check alt text is comprehensive
        const altText = await diagram.getAttribute('alt');
        expect(altText).toBeTruthy();
        expect(altText.length).toBeGreaterThan(50); // Should be descriptive

        // Verify it describes the data flow
        expect(altText).toMatch(/data flow|Data Flow/i);
        expect(altText).toContain('Write');
        expect(altText).toContain('Read');
    });
});
