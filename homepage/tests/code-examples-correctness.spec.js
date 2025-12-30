// @ts-check
const { test, expect } = require('@playwright/test');

test.describe('Code Examples Correctness', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: All code blocks contain valid shell or programming syntax', async ({ page }) => {
    // Test Case 1: Extract all code blocks and validate syntax
    // Expected: All code blocks contain valid shell or programming language syntax
    const codeBlocks = page.locator('pre code');
    const count = await codeBlocks.count();

    expect(count).toBeGreaterThan(0);

    for (let i = 0; i < count; i++) {
      const codeBlock = codeBlocks.nth(i);
      const content = await codeBlock.textContent();

      // Validate content exists and is not empty
      expect(content).toBeTruthy();
      expect(content.trim().length).toBeGreaterThan(0);

      // Validate shell commands syntax patterns
      const lines = content.split('\n').filter(line => line.trim().length > 0);

      for (const line of lines) {
        const trimmedLine = line.trim();

        // Skip empty lines and comments
        if (trimmedLine === '' || trimmedLine.startsWith('#')) continue;

        // Skip response lines (STORED, END, VALUE, etc.)
        if (['STORED', 'END', 'DELETED', 'NOT_FOUND', 'NOT_STORED', 'EXISTS', 'ERROR'].includes(trimmedLine)) continue;
        if (trimmedLine.startsWith('VALUE ')) continue;

        // Check for valid shell command patterns (starts with known commands)
        const validShellCommands = ['git', 'cargo', 'cd', 'telnet', './target', 'npm', 'yarn', 'pnpm'];
        const validMemcachedCommands = ['set', 'get', 'gets', 'delete', 'add', 'replace', 'append', 'prepend', 'info', 'major_compaction'];

        const isValidShell = validShellCommands.some(cmd => trimmedLine.toLowerCase().startsWith(cmd));
        const isValidMemcached = validMemcachedCommands.some(cmd => trimmedLine.toLowerCase().startsWith(cmd));
        const isDataLine = /^[a-zA-Z0-9_-]+$/.test(trimmedLine) && trimmedLine.length < 50; // Simple data values like 'hello'

        // At least one should match for valid code
        expect(isValidShell || isValidMemcached || isDataLine,
          `Line "${trimmedLine}" should be a valid command or data`).toBeTruthy();
      }
    }
  });

  test('TC2: SET command example follows memcached protocol format', async ({ page }) => {
    // Test Case 2: Verify SET command example follows memcached protocol format
    // Expected: SET example uses correct syntax: SET <key> <flags> <exptime> <bytes>
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    const codeBlock = quickStartSection.locator('pre code');
    const content = await codeBlock.textContent();

    // Find SET command line
    const lines = content.split('\n');
    const setLine = lines.find(line => line.trim().toLowerCase().startsWith('set '));

    expect(setLine, 'SET command should exist in code block').toBeTruthy();

    // Validate SET command format: set <key> <flags> <exptime> <bytes>
    // Example: set mykey 0 0 5
    const setPattern = /^set\s+\S+\s+\d+\s+\d+\s+\d+$/i;
    expect(setLine.trim()).toMatch(setPattern);

    // Verify the SET command has proper structure
    const parts = setLine.trim().split(/\s+/);
    expect(parts.length).toBe(5); // set, key, flags, exptime, bytes
    expect(parts[0].toLowerCase()).toBe('set');
    expect(parts[2]).toMatch(/^\d+$/); // flags should be numeric
    expect(parts[3]).toMatch(/^\d+$/); // exptime should be numeric
    expect(parts[4]).toMatch(/^\d+$/); // bytes should be numeric

    // Verify data follows SET command
    const setIndex = lines.findIndex(line => line.trim().toLowerCase().startsWith('set '));
    const dataLine = lines[setIndex + 1];
    expect(dataLine, 'Data should follow SET command').toBeTruthy();

    // Verify byte count matches data length
    const byteCount = parseInt(parts[4], 10);
    expect(dataLine.trim().length).toBe(byteCount);
  });

  test('TC3: GET command example follows memcached protocol format', async ({ page }) => {
    // Test Case 3: Verify GET command example follows memcached protocol format
    // Expected: GET example uses correct syntax: GET <key>
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    const codeBlock = quickStartSection.locator('pre code');
    const content = await codeBlock.textContent();

    // Find GET command line
    const lines = content.split('\n');
    const getLine = lines.find(line => line.trim().toLowerCase().startsWith('get '));

    expect(getLine, 'GET command should exist in code block').toBeTruthy();

    // Validate GET command format: get <key> [<key2> ...]
    // Example: get mykey
    const getPattern = /^get\s+\S+(\s+\S+)*$/i;
    expect(getLine.trim()).toMatch(getPattern);

    // Verify the GET command has proper structure
    const parts = getLine.trim().split(/\s+/);
    expect(parts.length).toBeGreaterThanOrEqual(2); // get, key (at minimum)
    expect(parts[0].toLowerCase()).toBe('get');
    expect(parts[1]).toBeTruthy(); // key should exist

    // Find the corresponding VALUE response
    const getIndex = lines.findIndex(line => line.trim().toLowerCase().startsWith('get '));
    const valueLine = lines.slice(getIndex + 1).find(line => line.trim().startsWith('VALUE '));
    expect(valueLine, 'VALUE response should follow GET command').toBeTruthy();

    // Validate VALUE response format: VALUE <key> <flags> <bytes>
    const valuePattern = /^VALUE\s+\S+\s+\d+\s+\d+$/;
    expect(valueLine.trim()).toMatch(valuePattern);
  });

  test('TC4: Code blocks preserve whitespace correctly', async ({ page }) => {
    // Test Case 4: Verify code blocks preserve whitespace correctly
    // Expected: Indentation and formatting is preserved in code blocks
    const codeBlocks = page.locator('pre code');
    const count = await codeBlocks.count();

    expect(count).toBeGreaterThan(0);

    for (let i = 0; i < count; i++) {
      const codeBlock = codeBlocks.nth(i);

      // Check that whitespace is preserved (not normalized/collapsed)
      const content = await codeBlock.textContent();
      const innerHTML = await codeBlock.innerHTML();

      // Verify newlines are preserved
      expect(content).toContain('\n');

      // Verify the pre element has white-space: pre or pre-wrap
      const preElement = page.locator('pre').nth(i);
      const whiteSpace = await preElement.evaluate((el) => {
        return window.getComputedStyle(el).whiteSpace;
      });

      expect(['pre', 'pre-wrap', 'pre-line']).toContain(whiteSpace);

      // Verify no hidden characters that would break copy-paste
      // Check that content doesn't contain zero-width characters
      const hasHiddenChars = /[\u200B\u200C\u200D\uFEFF]/.test(content);
      expect(hasHiddenChars).toBe(false);

      // Verify code block is selectable (user-select not none)
      const userSelect = await preElement.evaluate((el) => {
        return window.getComputedStyle(el).userSelect;
      });
      expect(userSelect).not.toBe('none');
    }
  });
});
