// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E Tests for Technical Overview Section
 * Scenario: Verify technical overview section displays supported commands
 * and configuration defaults (PRD Interface Requirements)
 */

test.describe('Technical Overview Section', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
    });

    /**
     * Test Case 1: Technical overview section exists on the page
     */
    test('TC1: Technical overview section exists on the page', async ({ page }) => {
        // Navigate to technical overview section
        const technicalOverviewSection = page.locator('[data-testid="technical-overview-section"]');

        // Verify section exists
        await expect(technicalOverviewSection).toBeVisible();

        // Verify section has expected heading
        const heading = technicalOverviewSection.locator('h2');
        await expect(heading).toHaveText('Technical Overview');

        // Verify section description
        const description = technicalOverviewSection.locator('.section-description');
        await expect(description).toBeVisible();
        await expect(description).toContainText('supported commands');
        await expect(description).toContainText('configuration');
    });

    /**
     * Test Case 2: List of supported Memcached commands is displayed
     */
    test('TC2: List of supported Memcached commands is displayed', async ({ page }) => {
        // Check for supported commands card
        const commandsCard = page.locator('[data-testid="supported-commands"]');
        await expect(commandsCard).toBeVisible();

        // Verify commands card heading
        const commandsHeading = commandsCard.locator('h3');
        await expect(commandsHeading).toHaveText('Supported Commands');

        // Verify storage commands list
        const storageCommandsList = page.locator('[data-testid="storage-commands-list"]');
        await expect(storageCommandsList).toBeVisible();
        await expect(storageCommandsList).toContainText('set');
        await expect(storageCommandsList).toContainText('add');
        await expect(storageCommandsList).toContainText('replace');
        await expect(storageCommandsList).toContainText('append');
        await expect(storageCommandsList).toContainText('prepend');

        // Verify retrieval commands list
        const retrievalCommandsList = page.locator('[data-testid="retrieval-commands-list"]');
        await expect(retrievalCommandsList).toBeVisible();
        await expect(retrievalCommandsList).toContainText('get');
        await expect(retrievalCommandsList).toContainText('gets');

        // Verify deletion commands list
        const deletionCommandsList = page.locator('[data-testid="deletion-commands-list"]');
        await expect(deletionCommandsList).toBeVisible();
        await expect(deletionCommandsList).toContainText('delete');

        // Verify MirDB-specific commands list
        const mirdbCommandsList = page.locator('[data-testid="mirdb-commands-list"]');
        await expect(mirdbCommandsList).toBeVisible();
        await expect(mirdbCommandsList).toContainText('info');
        await expect(mirdbCommandsList).toContainText('major_compaction');
    });

    /**
     * Test Case 3: Default configuration values are shown (port 12333, etc.)
     */
    test('TC3: Default configuration values are displayed', async ({ page }) => {
        // Check for configuration defaults card
        const configCard = page.locator('[data-testid="configuration-defaults"]');
        await expect(configCard).toBeVisible();

        // Verify configuration card heading
        const configHeading = configCard.locator('h3');
        await expect(configHeading).toHaveText('Default Configuration');

        // Verify configuration table exists
        const configTable = page.locator('[data-testid="config-table"]');
        await expect(configTable).toBeVisible();

        // Verify default port (12333)
        const portValue = page.locator('[data-testid="config-port"]');
        await expect(portValue).toBeVisible();
        await expect(portValue).toContainText('12333');

        // Verify default work directory
        const workDirValue = page.locator('[data-testid="config-work-dir"]');
        await expect(workDirValue).toBeVisible();
        await expect(workDirValue).toContainText('/tmp/mirdb');

        // Verify max LSM levels
        const maxLevelValue = page.locator('[data-testid="config-max-level"]');
        await expect(maxLevelValue).toBeVisible();
        await expect(maxLevelValue).toHaveText('7');

        // Verify SSTable max size
        const sstSizeValue = page.locator('[data-testid="config-sst-size"]');
        await expect(sstSizeValue).toBeVisible();
        await expect(sstSizeValue).toContainText('100MB');

        // Verify memtable max size
        const memtableSizeValue = page.locator('[data-testid="config-memtable-size"]');
        await expect(memtableSizeValue).toBeVisible();
        await expect(memtableSizeValue).toContainText('4MB');

        // Verify block size
        const blockSizeValue = page.locator('[data-testid="config-block-size"]');
        await expect(blockSizeValue).toBeVisible();
        await expect(blockSizeValue).toContainText('4KB');
    });

    /**
     * Test Case 4: Section is accessible via scrolling
     */
    test('TC4: Technical overview section is scrollable', async ({ page }) => {
        // Scroll to technical overview section
        const technicalOverviewSection = page.locator('[data-testid="technical-overview-section"]');
        await technicalOverviewSection.scrollIntoViewIfNeeded();

        // Verify section is visible after scroll
        await expect(technicalOverviewSection).toBeInViewport();
    });
});

test.describe('Technical Overview Commands Details', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
    });

    /**
     * Test: Commands are displayed with descriptions
     */
    test('Commands have descriptions', async ({ page }) => {
        const storageCommandsList = page.locator('[data-testid="storage-commands-list"]');

        // Verify commands have descriptive text
        await expect(storageCommandsList).toContainText('Store a key-value pair');
        await expect(storageCommandsList).toContainText("Store only if key doesn't exist");
        await expect(storageCommandsList).toContainText('Store only if key exists');
    });

    /**
     * Test: Commands are styled with code formatting
     */
    test('Commands are styled with code formatting', async ({ page }) => {
        const commandsCard = page.locator('[data-testid="supported-commands"]');
        const codeElements = commandsCard.locator('code');

        // Verify multiple code elements exist
        const count = await codeElements.count();
        expect(count).toBeGreaterThan(0);

        // Verify first code element has proper styling (monospace font)
        const firstCode = codeElements.first();
        const fontFamily = await firstCode.evaluate(el =>
            window.getComputedStyle(el).fontFamily
        );
        expect(fontFamily.toLowerCase()).toMatch(/mono|courier|sf mono|monaco|inconsolata|roboto mono/i);
    });

    /**
     * Test: Command categories are organized
     */
    test('Commands are organized by category', async ({ page }) => {
        const commandsCard = page.locator('[data-testid="supported-commands"]');

        // Verify category headings exist
        const categoryHeadings = commandsCard.locator('h4');
        const headingsCount = await categoryHeadings.count();
        expect(headingsCount).toBe(4);

        // Verify category names
        await expect(categoryHeadings.nth(0)).toContainText('Storage');
        await expect(categoryHeadings.nth(1)).toContainText('Retrieval');
        await expect(categoryHeadings.nth(2)).toContainText('Deletion');
        await expect(categoryHeadings.nth(3)).toContainText('MirDB');
    });
});

test.describe('Technical Overview Configuration Table', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
    });

    /**
     * Test: Configuration table has proper structure
     */
    test('Configuration table has proper structure', async ({ page }) => {
        const configTable = page.locator('[data-testid="config-table"]');

        // Verify table has header
        const thead = configTable.locator('thead');
        await expect(thead).toBeVisible();

        // Verify table has body
        const tbody = configTable.locator('tbody');
        await expect(tbody).toBeVisible();

        // Verify table headers
        const headerCells = thead.locator('th');
        await expect(headerCells.nth(0)).toHaveText('Parameter');
        await expect(headerCells.nth(1)).toHaveText('Default Value');
        await expect(headerCells.nth(2)).toHaveText('Description');
    });

    /**
     * Test: Configuration table has correct number of rows
     */
    test('Configuration table has expected rows', async ({ page }) => {
        const configTable = page.locator('[data-testid="config-table"]');
        const bodyRows = configTable.locator('tbody tr');

        // Verify 6 configuration rows
        const count = await bodyRows.count();
        expect(count).toBe(6);
    });

    /**
     * Test: Configuration table is accessible
     */
    test('Configuration table has accessibility attributes', async ({ page }) => {
        const configTable = page.locator('[data-testid="config-table"]');

        // Verify table has aria-label
        await expect(configTable).toHaveAttribute('aria-label', 'MirDB default configuration values');

        // Verify table headers have scope
        const headerCells = configTable.locator('thead th');
        const firstHeader = headerCells.first();
        await expect(firstHeader).toHaveAttribute('scope', 'col');
    });
});

test.describe('Technical Overview Responsive Design', () => {
    /**
     * Test: Section is responsive on mobile viewport
     */
    test('Section is responsive on mobile', async ({ page }) => {
        await page.setViewportSize({ width: 375, height: 667 });
        await page.goto('/');

        const technicalOverviewSection = page.locator('[data-testid="technical-overview-section"]');
        await technicalOverviewSection.scrollIntoViewIfNeeded();
        await expect(technicalOverviewSection).toBeVisible();

        // Verify content cards are visible
        const commandsCard = page.locator('[data-testid="supported-commands"]');
        const configCard = page.locator('[data-testid="configuration-defaults"]');
        await expect(commandsCard).toBeVisible();
        await expect(configCard).toBeVisible();
    });

    /**
     * Test: Section is responsive on tablet viewport
     */
    test('Section is responsive on tablet', async ({ page }) => {
        await page.setViewportSize({ width: 768, height: 1024 });
        await page.goto('/');

        const technicalOverviewSection = page.locator('[data-testid="technical-overview-section"]');
        await technicalOverviewSection.scrollIntoViewIfNeeded();
        await expect(technicalOverviewSection).toBeVisible();

        // Verify cards are still visible
        const commandsCard = page.locator('[data-testid="supported-commands"]');
        const configCard = page.locator('[data-testid="configuration-defaults"]');
        await expect(commandsCard).toBeVisible();
        await expect(configCard).toBeVisible();
    });
});
