// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Commands Reference Section', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
    });

    test('TC1: Storage commands section displays SET, ADD, REPLACE, APPEND, PREPEND with syntax', async ({ page }) => {
        // Navigate to commands section
        const commandsSection = page.locator('#commands');
        await commandsSection.scrollIntoViewIfNeeded();

        // Verify commands section is visible
        await expect(commandsSection).toBeVisible();

        // Locate storage commands category
        const storageCommands = page.locator('#storage-commands');
        await expect(storageCommands).toBeVisible();

        // Verify storage commands header
        const storageHeader = storageCommands.locator('h3');
        await expect(storageHeader).toHaveText('Storage Commands');

        // Verify storage commands table exists
        const storageTable = storageCommands.locator('.commands-table');
        await expect(storageTable).toBeVisible();

        // Verify SET command is present with syntax
        const setRow = storageTable.locator('tr[data-command="set"]');
        await expect(setRow).toBeVisible();
        await expect(setRow.locator('td').first()).toContainText('SET');
        await expect(setRow.locator('td').last()).toContainText('set');

        // Verify ADD command is present with syntax
        const addRow = storageTable.locator('tr[data-command="add"]');
        await expect(addRow).toBeVisible();
        await expect(addRow.locator('td').first()).toContainText('ADD');
        await expect(addRow.locator('td').last()).toContainText('add');

        // Verify REPLACE command is present with syntax
        const replaceRow = storageTable.locator('tr[data-command="replace"]');
        await expect(replaceRow).toBeVisible();
        await expect(replaceRow.locator('td').first()).toContainText('REPLACE');
        await expect(replaceRow.locator('td').last()).toContainText('replace');

        // Verify APPEND command is present with syntax
        const appendRow = storageTable.locator('tr[data-command="append"]');
        await expect(appendRow).toBeVisible();
        await expect(appendRow.locator('td').first()).toContainText('APPEND');
        await expect(appendRow.locator('td').last()).toContainText('append');

        // Verify PREPEND command is present with syntax
        const prependRow = storageTable.locator('tr[data-command="prepend"]');
        await expect(prependRow).toBeVisible();
        await expect(prependRow.locator('td').first()).toContainText('PREPEND');
        await expect(prependRow.locator('td').last()).toContainText('prepend');

        // Verify all 5 storage commands are in the table
        const storageRows = storageTable.locator('tbody tr');
        await expect(storageRows).toHaveCount(5);
    });

    test('TC2: Retrieval commands section displays GET, GETS with syntax', async ({ page }) => {
        // Navigate to commands section
        const commandsSection = page.locator('#commands');
        await commandsSection.scrollIntoViewIfNeeded();

        // Locate retrieval commands category
        const retrievalCommands = page.locator('#retrieval-commands');
        await expect(retrievalCommands).toBeVisible();

        // Verify retrieval commands header
        const retrievalHeader = retrievalCommands.locator('h3');
        await expect(retrievalHeader).toHaveText('Retrieval Commands');

        // Verify retrieval commands table exists
        const retrievalTable = retrievalCommands.locator('.commands-table');
        await expect(retrievalTable).toBeVisible();

        // Verify GET command is present with syntax
        const getRow = retrievalTable.locator('tr[data-command="get"]');
        await expect(getRow).toBeVisible();
        await expect(getRow.locator('td').first()).toContainText('GET');
        await expect(getRow.locator('td').last()).toContainText('get');

        // Verify GETS command is present with syntax
        const getsRow = retrievalTable.locator('tr[data-command="gets"]');
        await expect(getsRow).toBeVisible();
        await expect(getsRow.locator('td').first()).toContainText('GETS');
        await expect(getsRow.locator('td').last()).toContainText('gets');

        // Verify all 2 retrieval commands are in the table
        const retrievalRows = retrievalTable.locator('tbody tr');
        await expect(retrievalRows).toHaveCount(2);
    });

    test('TC3: Deletion commands section displays DELETE with syntax', async ({ page }) => {
        // Navigate to commands section
        const commandsSection = page.locator('#commands');
        await commandsSection.scrollIntoViewIfNeeded();

        // Locate deletion commands category
        const deletionCommands = page.locator('#deletion-commands');
        await expect(deletionCommands).toBeVisible();

        // Verify deletion commands header
        const deletionHeader = deletionCommands.locator('h3');
        await expect(deletionHeader).toHaveText('Deletion Commands');

        // Verify deletion commands table exists
        const deletionTable = deletionCommands.locator('.commands-table');
        await expect(deletionTable).toBeVisible();

        // Verify DELETE command is present with syntax
        const deleteRow = deletionTable.locator('tr[data-command="delete"]');
        await expect(deleteRow).toBeVisible();
        await expect(deleteRow.locator('td').first()).toContainText('DELETE');
        await expect(deleteRow.locator('td').last()).toContainText('delete');

        // Verify DELETE command has syntax with noreply option
        await expect(deleteRow.locator('td').last()).toContainText('noreply');

        // Verify 1 deletion command in the table
        const deletionRows = deletionTable.locator('tbody tr');
        await expect(deletionRows).toHaveCount(1);
    });

    test('TC4: Administrative commands section displays INFO and MAJOR_COMPACTION MirDB-specific commands', async ({ page }) => {
        // Navigate to commands section
        const commandsSection = page.locator('#commands');
        await commandsSection.scrollIntoViewIfNeeded();

        // Locate administrative commands category
        const adminCommands = page.locator('#administrative-commands');
        await expect(adminCommands).toBeVisible();

        // Verify administrative commands header indicates MirDB-specific
        const adminHeader = adminCommands.locator('h3');
        await expect(adminHeader).toContainText('Administrative Commands');
        await expect(adminHeader).toContainText('MirDB-Specific');

        // Verify administrative commands table exists
        const adminTable = adminCommands.locator('.commands-table');
        await expect(adminTable).toBeVisible();

        // Verify INFO command is present with syntax
        const infoRow = adminTable.locator('tr[data-command="info"]');
        await expect(infoRow).toBeVisible();
        await expect(infoRow.locator('td').first()).toContainText('INFO');
        await expect(infoRow.locator('td').last()).toContainText('info');

        // Verify MAJOR_COMPACTION command is present with syntax
        const compactionRow = adminTable.locator('tr[data-command="major_compaction"]');
        await expect(compactionRow).toBeVisible();
        await expect(compactionRow.locator('td').first()).toContainText('MAJOR_COMPACTION');
        await expect(compactionRow.locator('td').last()).toContainText('major_compaction');

        // Verify all 2 administrative commands are in the table
        const adminRows = adminTable.locator('tbody tr');
        await expect(adminRows).toHaveCount(2);
    });

    test('Commands table structure is organized by category', async ({ page }) => {
        // Navigate to commands section
        const commandsSection = page.locator('#commands');
        await commandsSection.scrollIntoViewIfNeeded();

        // Verify section title
        const sectionTitle = commandsSection.locator('h2');
        await expect(sectionTitle).toHaveText('Commands Reference');

        // Verify all command categories exist
        const categories = commandsSection.locator('.command-category');
        await expect(categories).toHaveCount(4);

        // Verify each category has its own table
        const tables = commandsSection.locator('.commands-table');
        await expect(tables).toHaveCount(4);

        // Verify table headers contain required columns
        const firstTable = tables.first();
        const headers = firstTable.locator('thead th');
        await expect(headers).toHaveCount(3);
        await expect(headers.nth(0)).toHaveText('Command');
        await expect(headers.nth(1)).toHaveText('Description');
        await expect(headers.nth(2)).toHaveText('Syntax');
    });
});
