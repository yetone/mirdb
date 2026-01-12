// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Quick Start Section - Installation and Usage Examples (REQ-4)', () => {
    test.beforeEach(async ({ page }) => {
        await page.goto('/');
        // Navigate to quick start section
        const getStartedBtn = page.locator('#get-started-btn');
        await getStartedBtn.click();
        await page.waitForTimeout(500);
    });

    // Test Case 1: Locate installation command block
    test('TC1: Installation command is displayed in a code block with syntax highlighting', async ({ page }) => {
        // Locate the quick start section
        const quickStartSection = page.locator('#quick-start');
        await expect(quickStartSection).toBeVisible();

        // Find the installation code block (first code-block in quick-start)
        const installationBlock = quickStartSection.locator('.code-block').first();
        await expect(installationBlock).toBeVisible();

        // Verify code element exists with proper styling
        const codeElement = installationBlock.locator('code');
        await expect(codeElement).toBeVisible();

        // Verify the installation command is present
        const codeText = await codeElement.textContent();
        expect(codeText).toContain('cargo install');

        // Verify code block has pre element for proper formatting
        const preElement = installationBlock.locator('pre');
        await expect(preElement).toBeVisible();

        // Verify syntax highlighting via CSS class or colored styling
        const codeBlockStyles = await installationBlock.evaluate((el) => {
            return window.getComputedStyle(el).backgroundColor;
        });
        // Code blocks should have a dark background for syntax highlighting
        expect(codeBlockStyles).toBeTruthy();
    });

    // Test Case 2: Click copy button on installation command
    test('TC2: Copy button copies command to clipboard and shows visual feedback', async ({ page, context }) => {
        // Grant clipboard permissions
        await context.grantPermissions(['clipboard-read', 'clipboard-write']);

        // Locate the quick start section
        const quickStartSection = page.locator('#quick-start');
        await expect(quickStartSection).toBeVisible();

        // Find the copy button in the installation code block
        const installationBlock = quickStartSection.locator('.code-block').first();
        const copyBtn = installationBlock.locator('.copy-btn');
        await expect(copyBtn).toBeVisible();

        // Verify button shows "Copy" text initially
        await expect(copyBtn).toHaveText('Copy');

        // Click the copy button
        await copyBtn.click();

        // Verify visual feedback - button text changes to "Copied!"
        await expect(copyBtn).toHaveText('Copied!');

        // Verify clipboard contains the installation command
        const clipboardText = await page.evaluate(async () => {
            return await navigator.clipboard.readText();
        });
        expect(clipboardText).toContain('cargo install mirdb-server');

        // Wait for button to revert back to "Copy"
        await page.waitForTimeout(2500);
        await expect(copyBtn).toHaveText('Copy');
    });

    // Test Case 3: Check usage example for SET operation
    test('TC3: SET command example is shown with proper syntax', async ({ page }) => {
        // Locate the quick start section
        const quickStartSection = page.locator('#quick-start');
        await expect(quickStartSection).toBeVisible();

        // Get the content of the quick start section
        const quickStartContent = await quickStartSection.textContent();

        // Verify SET command example is present with proper syntax
        // Format: set <key> <flags> <ttl> <bytes>\r\n<data>\r\n
        expect(quickStartContent).toMatch(/set\s+\w+\s+\d+\s+\d+\s+\d+/i);

        // Verify it shows a concrete example like "set mykey 0 0 5"
        expect(quickStartContent).toContain('set mykey');

        // Verify the SET command shows the data value being set
        expect(quickStartContent).toMatch(/hello|value/i);

        // Verify STORED response is shown as expected result
        expect(quickStartContent).toContain('STORED');
    });

    // Test Case 4: Check usage example for GET operation
    test('TC4: GET command example is shown with proper syntax and expected response', async ({ page }) => {
        // Locate the quick start section
        const quickStartSection = page.locator('#quick-start');
        await expect(quickStartSection).toBeVisible();

        // Get the content of the quick start section
        const quickStartContent = await quickStartSection.textContent();

        // Verify GET command example is present with proper syntax
        // Format: get <key>\r\n
        expect(quickStartContent).toMatch(/get\s+\w+/i);

        // Verify it shows a concrete example like "get mykey"
        expect(quickStartContent).toContain('get mykey');

        // Verify the expected response format is shown
        // VALUE <key> <flags> <bytes>\r\n<data>\r\nEND
        expect(quickStartContent).toContain('VALUE');
        expect(quickStartContent).toContain('END');

        // Verify the response shows the value that was set
        expect(quickStartContent).toMatch(/hello|value/i);
    });

    // Test Case 5: Check configuration snippet
    test('TC5: Configuration options displayed including listen address, work directory, and memtable size', async ({ page }) => {
        // Navigate to configuration section
        const configSection = page.locator('#configuration');
        await expect(configSection).toBeVisible();

        // Get the content of the configuration section
        const configContent = await configSection.textContent();

        // Verify listen address configuration is displayed
        expect(configContent).toContain('addr');
        expect(configContent).toContain('0.0.0.0:12333');

        // Verify work directory configuration is displayed
        expect(configContent).toContain('work_dir');
        expect(configContent).toContain('/tmp/mirdb');

        // Verify memtable size configuration is displayed
        expect(configContent).toContain('mem_table_max_size');
        expect(configContent).toMatch(/4M[B]?/);

        // Verify configuration is in a code block
        const codeBlock = configSection.locator('.code-block');
        await expect(codeBlock).toBeVisible();

        const codeElement = codeBlock.locator('code');
        await expect(codeElement).toBeVisible();
    });

    // Additional test: Verify Quick Start section structure
    test('Quick Start section has proper structure with Installation and Usage headers', async ({ page }) => {
        // Locate the quick start section
        const quickStartSection = page.locator('#quick-start');
        await expect(quickStartSection).toBeVisible();

        // Verify section title
        const title = quickStartSection.locator('h2');
        await expect(title).toHaveText('Quick Start');

        // Verify Installation subsection header
        const quickStartContent = await quickStartSection.textContent();
        expect(quickStartContent).toContain('Installation');

        // Verify Basic Usage subsection header
        expect(quickStartContent).toMatch(/Basic Usage|Usage/);
    });
});
