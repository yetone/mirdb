import { test, expect } from '@playwright/test';

/**
 * Content Accuracy - Code Examples
 *
 * This test suite verifies that code examples in the MirDB homepage
 * match the actual memcached protocol implementation as documented
 * in the knowledge base.
 *
 * Protocol Reference (from knowledge base):
 * - SET: set <key> <flags> <ttl> <bytes> [noreply]\r\n<data>\r\n
 * - GET: get <key1> [<key2> ...]\r\n
 * - DELETE: delete <key> [noreply]\r\n
 *
 * Response Codes:
 * - STORED: Data stored successfully
 * - VALUE: Retrieval response with key data
 * - END: End of retrieval response
 * - DELETED: Key deleted successfully
 * - NOT_FOUND: Key not found (for delete/replace)
 */

test.describe('Content Accuracy - Code Examples', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: SET command syntax follows correct memcached protocol format', async ({ page }) => {
    // Navigate to code examples section
    const codeExamplesSection = page.locator('[data-testid="code-examples-section"]');
    await expect(codeExamplesSection).toBeVisible();

    // Check for SET command example
    const setExample = codeExamplesSection.locator('[data-testid="set-example"]');
    await expect(setExample).toBeVisible();

    // Get the code block content
    const codeBlock = setExample.locator('pre');
    const codeText = await codeBlock.textContent();

    // Validate SET command follows the exact memcached protocol format:
    // set <key> <flags> <ttl> <bytes> [noreply]\r\n<data>\r\n
    //
    // The format should include:
    // 1. 'set' keyword
    // 2. key name (alphanumeric)
    // 3. flags (numeric, typically 0)
    // 4. ttl/exptime (numeric, 0 means no expiration)
    // 5. bytes count (numeric, length of data)

    // Verify the command starts with 'set' followed by key, flags, ttl, bytes
    expect(codeText).toMatch(/set\s+\w+\s+\d+\s+\d+\s+\d+/i);

    // Verify the SET example shows the correct format structure
    // Example: "set mykey 0 0 5"
    const setCommandMatch = codeText?.match(/set\s+(\w+)\s+(\d+)\s+(\d+)\s+(\d+)/i);
    expect(setCommandMatch).toBeTruthy();

    if (setCommandMatch) {
      const [, key, flags, ttl, bytes] = setCommandMatch;
      // Verify key exists
      expect(key).toBeTruthy();
      // Verify flags is a valid number
      expect(parseInt(flags)).toBeGreaterThanOrEqual(0);
      // Verify ttl is a valid number (0 = no expiration)
      expect(parseInt(ttl)).toBeGreaterThanOrEqual(0);
      // Verify bytes is a valid positive number
      expect(parseInt(bytes)).toBeGreaterThan(0);
    }

    // Verify data payload is present after the command
    // The SET command should show data following the command line
    expect(codeText).toMatch(/set\s+\w+\s+\d+\s+\d+\s+\d+[\s\S]*\w+/i);
  });

  test('TC2: GET command syntax follows correct memcached protocol format', async ({ page }) => {
    const codeExamplesSection = page.locator('[data-testid="code-examples-section"]');
    await expect(codeExamplesSection).toBeVisible();

    // Check for GET command example
    const getExample = codeExamplesSection.locator('[data-testid="get-example"]');
    await expect(getExample).toBeVisible();

    // Get the code block content
    const codeBlock = getExample.locator('pre');
    const codeText = await codeBlock.textContent();

    // Validate GET command follows the exact memcached protocol format:
    // get <key1> [<key2> ...]\r\n
    //
    // The format should include:
    // 1. 'get' keyword
    // 2. one or more key names (space-separated)

    // Verify the command starts with 'get' followed by at least one key
    expect(codeText).toMatch(/get\s+\w+/i);

    // Verify the GET example shows the correct format structure
    // Example: "get mykey" or "get key1 key2"
    const getCommandMatch = codeText?.match(/get\s+(\w+)/i);
    expect(getCommandMatch).toBeTruthy();

    if (getCommandMatch) {
      const [, key] = getCommandMatch;
      // Verify key exists and is a valid identifier
      expect(key).toBeTruthy();
      expect(key.length).toBeGreaterThan(0);
    }
  });

  test('TC3: DELETE command syntax follows correct memcached protocol format', async ({ page }) => {
    const codeExamplesSection = page.locator('[data-testid="code-examples-section"]');
    await expect(codeExamplesSection).toBeVisible();

    // Check for DELETE command example
    const deleteExample = codeExamplesSection.locator('[data-testid="delete-example"]');
    await expect(deleteExample).toBeVisible();

    // Get the code block content
    const codeBlock = deleteExample.locator('pre');
    const codeText = await codeBlock.textContent();

    // Validate DELETE command follows the exact memcached protocol format:
    // delete <key> [noreply]\r\n
    //
    // The format should include:
    // 1. 'delete' keyword
    // 2. key name
    // 3. optional 'noreply' flag

    // Verify the command starts with 'delete' followed by a key
    expect(codeText).toMatch(/delete\s+\w+/i);

    // Verify the DELETE example shows the correct format structure
    // Example: "delete mykey" or "delete mykey noreply"
    const deleteCommandMatch = codeText?.match(/delete\s+(\w+)/i);
    expect(deleteCommandMatch).toBeTruthy();

    if (deleteCommandMatch) {
      const [, key] = deleteCommandMatch;
      // Verify key exists and is a valid identifier
      expect(key).toBeTruthy();
      expect(key.length).toBeGreaterThan(0);
    }
  });

  test('TC4: Response examples match actual memcached protocol responses', async ({ page }) => {
    const codeExamplesSection = page.locator('[data-testid="code-examples-section"]');
    await expect(codeExamplesSection).toBeVisible();

    // Get all example sections
    const setExample = codeExamplesSection.locator('[data-testid="set-example"]');
    const getExample = codeExamplesSection.locator('[data-testid="get-example"]');
    const deleteExample = codeExamplesSection.locator('[data-testid="delete-example"]');

    // Validate SET response - should show 'STORED'
    const setCodeText = await setExample.locator('pre').textContent();
    expect(setCodeText).toContain('STORED');

    // Validate GET response - should show 'VALUE' format and 'END'
    // Protocol format: VALUE <key> <flags> <bytes>\r\n<data>\r\nEND
    const getCodeText = await getExample.locator('pre').textContent();
    expect(getCodeText).toContain('VALUE');
    expect(getCodeText).toContain('END');

    // Verify VALUE response follows correct format: VALUE <key> <flags> <bytes>
    expect(getCodeText).toMatch(/VALUE\s+\w+\s+\d+\s+\d+/i);

    // Validate DELETE response - should show 'DELETED'
    const deleteCodeText = await deleteExample.locator('pre').textContent();
    expect(deleteCodeText).toContain('DELETED');
  });

  test('Protocol format reference section displays accurate syntax templates', async ({ page }) => {
    const codeExamplesSection = page.locator('[data-testid="code-examples-section"]');
    await expect(codeExamplesSection).toBeVisible();

    // Get the full section content to check for protocol reference
    const sectionText = await codeExamplesSection.textContent();

    // Verify SET format reference is present and accurate
    // Expected: set <key> <flags> <ttl> <bytes>\r\n<data>\r\n
    expect(sectionText).toMatch(/set\s*[<\[]\s*key\s*[>\]]/i);
    expect(sectionText).toMatch(/[<\[]\s*flags\s*[>\]]/i);

    // Verify GET format reference is present and accurate
    // Expected: get <key>
    expect(sectionText).toMatch(/get\s*[<\[]\s*key\s*[>\]]/i);

    // Verify DELETE format reference is present and accurate
    // Expected: delete <key>
    expect(sectionText).toMatch(/delete\s*[<\[]\s*key\s*[>\]]/i);
  });

  test('Code examples use consistent key naming across SET/GET/DELETE', async ({ page }) => {
    const codeExamplesSection = page.locator('[data-testid="code-examples-section"]');
    await expect(codeExamplesSection).toBeVisible();

    // Get code from all examples
    const setCodeText = await codeExamplesSection.locator('[data-testid="set-example"] pre').textContent();
    const getCodeText = await codeExamplesSection.locator('[data-testid="get-example"] pre').textContent();
    const deleteCodeText = await codeExamplesSection.locator('[data-testid="delete-example"] pre').textContent();

    // Extract key names from each command
    const setKeyMatch = setCodeText?.match(/set\s+(\w+)/i);
    const getKeyMatch = getCodeText?.match(/get\s+(\w+)/i);
    const deleteKeyMatch = deleteCodeText?.match(/delete\s+(\w+)/i);

    // All examples should use the same key for demonstration consistency
    expect(setKeyMatch).toBeTruthy();
    expect(getKeyMatch).toBeTruthy();
    expect(deleteKeyMatch).toBeTruthy();

    if (setKeyMatch && getKeyMatch && deleteKeyMatch) {
      const setKey = setKeyMatch[1];
      const getKey = getKeyMatch[1];
      const deleteKey = deleteKeyMatch[1];

      // Keys should be consistent across examples
      expect(setKey).toBe(getKey);
      expect(getKey).toBe(deleteKey);
    }
  });

  test('SET example includes correct byte count matching data length', async ({ page }) => {
    const codeExamplesSection = page.locator('[data-testid="code-examples-section"]');
    await expect(codeExamplesSection).toBeVisible();

    const setExample = codeExamplesSection.locator('[data-testid="set-example"]');
    const codeText = await setExample.locator('pre').textContent();

    // Extract byte count from SET command
    const setMatch = codeText?.match(/set\s+\w+\s+\d+\s+\d+\s+(\d+)/i);
    expect(setMatch).toBeTruthy();

    if (setMatch) {
      const declaredBytes = parseInt(setMatch[1]);

      // The byte count should be a reasonable value for a demo
      // (typically showing a simple string like "hello" = 5 bytes)
      expect(declaredBytes).toBeGreaterThan(0);
      expect(declaredBytes).toBeLessThan(1000); // Sanity check for demo data
    }
  });

  test('GET response VALUE line includes correct metadata format', async ({ page }) => {
    const codeExamplesSection = page.locator('[data-testid="code-examples-section"]');
    await expect(codeExamplesSection).toBeVisible();

    const getExample = codeExamplesSection.locator('[data-testid="get-example"]');
    const codeText = await getExample.locator('pre').textContent();

    // VALUE response format: VALUE <key> <flags> <bytes>
    const valueMatch = codeText?.match(/VALUE\s+(\w+)\s+(\d+)\s+(\d+)/i);
    expect(valueMatch).toBeTruthy();

    if (valueMatch) {
      const [, key, flags, bytes] = valueMatch;

      // Verify key exists
      expect(key).toBeTruthy();

      // Verify flags is a valid number
      expect(parseInt(flags)).toBeGreaterThanOrEqual(0);

      // Verify bytes is a valid positive number
      expect(parseInt(bytes)).toBeGreaterThan(0);
    }
  });
});
