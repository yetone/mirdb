/**
 * Usage and Code Examples Tests
 * Owner: Scenario 5 - Usage and Code Examples
 *
 * Tests:
 * - Usage section presence with id='usage'
 * - Code block presence with syntax highlighting
 * - SET/GET command examples
 * - ADD/REPLACE/APPEND/PREPEND/DELETE command examples
 * - Copy-to-clipboard functionality
 */

const { test, expect } = require('@playwright/test');
const { BASE_URL, SELECTORS, waitForPageLoad } = require('./test-utils');

test.describe('Usage and Code Examples Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await waitForPageLoad(page);
  });

  test('TC1: Usage section exists with id="usage"', async ({ page }) => {
    // Test case 1: Check usage section element
    const usageSection = page.locator('#usage');
    await expect(usageSection).toBeVisible();
    await expect(usageSection).toHaveClass(/usage/);
  });

  test('TC2: Code blocks with syntax highlighting exist', async ({ page }) => {
    // Test case 2: Check for code block elements with syntax highlighting
    const codeBlocks = page.locator('#usage pre.usage__code');
    const count = await codeBlocks.count();
    expect(count).toBeGreaterThan(0);

    // Check that code blocks have the language class
    const firstCodeBlock = codeBlocks.first();
    await expect(firstCodeBlock).toHaveClass(/language-bash/);
  });

  test('TC3: SET command example is present', async ({ page }) => {
    // Test case 3: Search for 'set' command example
    const usageSection = page.locator('#usage');

    // Look for SET command in code examples
    const setExample = usageSection.locator('pre:has-text("set ")').first();
    await expect(setExample).toBeVisible();

    // Verify SET command syntax is shown
    const setCode = await setExample.textContent();
    expect(setCode.toLowerCase()).toContain('set');
  });

  test('TC4: GET command example is present', async ({ page }) => {
    // Test case 4: Search for 'get' command example
    const usageSection = page.locator('#usage');

    // Look for GET command in code examples
    const getExample = usageSection.locator('pre:has-text("get ")').first();
    await expect(getExample).toBeVisible();

    // Verify GET command syntax is shown
    const getCode = await getExample.textContent();
    expect(getCode.toLowerCase()).toContain('get');
  });

  test('TC5: Copy buttons exist on code blocks', async ({ page }) => {
    // Test case 5: Check for copy button on code blocks
    const usageSection = page.locator('#usage');
    const copyButtons = usageSection.locator('.usage__copy-btn');

    const count = await copyButtons.count();
    expect(count).toBeGreaterThan(0);

    // Check that copy buttons have the necessary attributes
    const firstCopyBtn = copyButtons.first();
    await expect(firstCopyBtn).toBeVisible();
    await expect(firstCopyBtn).toHaveAttribute('data-copy-target');

    // Verify button has aria-label for accessibility
    await expect(firstCopyBtn).toHaveAttribute('aria-label', 'Copy to clipboard');
  });

  test('TC6: Syntax highlighting is applied to code blocks', async ({ page }) => {
    // Test case 6: Verify syntax highlighting applied
    const usageSection = page.locator('#usage');

    // Check for code blocks with language class
    const highlightedBlocks = usageSection.locator('pre[class*="language-"]');
    const count = await highlightedBlocks.count();
    expect(count).toBeGreaterThan(0);

    // Wait for JavaScript to apply highlighting
    await page.waitForTimeout(500);

    // Check for highlighted class (added by prism.js after processing)
    const firstBlock = highlightedBlocks.first();
    await expect(firstBlock).toHaveClass(/highlighted/);
  });

  test('ADD command example is present', async ({ page }) => {
    // Additional test: Check for ADD command
    const usageSection = page.locator('#usage');
    const addExample = usageSection.locator('pre:has-text("add ")').first();
    await expect(addExample).toBeVisible();
  });

  test('REPLACE command example is present', async ({ page }) => {
    // Additional test: Check for REPLACE command
    const usageSection = page.locator('#usage');
    const replaceExample = usageSection.locator('pre:has-text("replace ")').first();
    await expect(replaceExample).toBeVisible();
  });

  test('APPEND command example is present', async ({ page }) => {
    // Additional test: Check for APPEND command
    const usageSection = page.locator('#usage');
    const appendExample = usageSection.locator('pre:has-text("append ")').first();
    await expect(appendExample).toBeVisible();
  });

  test('PREPEND command example is present', async ({ page }) => {
    // Additional test: Check for PREPEND command
    const usageSection = page.locator('#usage');
    const prependExample = usageSection.locator('pre:has-text("prepend ")').first();
    await expect(prependExample).toBeVisible();
  });

  test('DELETE command example is present', async ({ page }) => {
    // Additional test: Check for DELETE command
    const usageSection = page.locator('#usage');
    const deleteExample = usageSection.locator('pre:has-text("delete ")').first();
    await expect(deleteExample).toBeVisible();
  });

  test('Usage section has proper structure', async ({ page }) => {
    // Verify section structure
    const usageSection = page.locator('#usage');

    // Check for title
    const title = usageSection.locator('.usage__title');
    await expect(title).toBeVisible();
    await expect(title).toContainText('Usage');

    // Check for subtitle
    const subtitle = usageSection.locator('.usage__subtitle');
    await expect(subtitle).toBeVisible();

    // Check for section containers
    const sections = usageSection.locator('.usage__section');
    const sectionCount = await sections.count();
    expect(sectionCount).toBeGreaterThanOrEqual(2);
  });

  test('Copy button changes state on click', async ({ page, context }) => {
    // Grant clipboard permissions for the test
    await context.grantPermissions(['clipboard-write', 'clipboard-read']);

    // Test copy button interaction
    const usageSection = page.locator('#usage');
    const firstCopyBtn = usageSection.locator('.usage__copy-btn').first();

    // Click the copy button
    await firstCopyBtn.click();

    // Wait for state change
    await page.waitForTimeout(300);

    // Check that button text changed to indicate success or error state
    // (clipboard API may not work in all test environments)
    const buttonText = await firstCopyBtn.textContent();
    // Button should change state - either to "Copied!" or "Error" depending on clipboard permissions
    expect(buttonText).toMatch(/(Copied|Error)/);
  });

  test('Code blocks contain properly formatted examples', async ({ page }) => {
    // Verify code formatting
    const usageSection = page.locator('#usage');
    const codeBlocks = usageSection.locator('pre.usage__code code');

    const count = await codeBlocks.count();
    expect(count).toBeGreaterThan(0);

    // Each code block should have actual content
    for (let i = 0; i < count; i++) {
      const codeContent = await codeBlocks.nth(i).textContent();
      expect(codeContent.trim().length).toBeGreaterThan(0);
    }
  });

  test('Code header shows language label', async ({ page }) => {
    // Verify language labels
    const usageSection = page.locator('#usage');
    const langLabels = usageSection.locator('.usage__code-lang');

    const count = await langLabels.count();
    expect(count).toBeGreaterThan(0);

    // Check first label contains 'bash'
    const firstLabel = langLabels.first();
    await expect(firstLabel).toContainText('bash');
  });
});

/**
 * Getting Started Section Tests
 * Owner: Scenario 13 - Getting Started Section
 *
 * Tests:
 * - Getting started section presence with id='getting-started'
 * - Installation instructions with cargo build commands
 * - Default port 12333 is mentioned
 * - Connection examples (telnet or memcached client)
 */
test.describe('Getting Started Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await waitForPageLoad(page);
  });

  test('TC1: Getting started section exists with id="getting-started"', async ({ page }) => {
    // Test case 1: Check getting started section exists
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();
    await expect(gettingStartedSection).toHaveClass(/getting-started/);
  });

  test('TC2: Cargo build instructions are present', async ({ page }) => {
    // Test case 2: Search for 'cargo' or build instructions
    const gettingStartedSection = page.locator('#getting-started');

    // Look for cargo build command in code examples
    const cargoExample = gettingStartedSection.locator('pre:has-text("cargo build")').first();
    await expect(cargoExample).toBeVisible();

    // Verify cargo build command is shown
    const cargoCode = await cargoExample.textContent();
    expect(cargoCode.toLowerCase()).toContain('cargo build');
  });

  test('TC3: Default port 12333 is mentioned', async ({ page }) => {
    // Test case 3: Search for connection port
    const gettingStartedSection = page.locator('#getting-started');

    // Look for port 12333 in the content
    const sectionText = await gettingStartedSection.textContent();
    expect(sectionText).toContain('12333');
  });

  test('TC4: Telnet connection example is present', async ({ page }) => {
    // Test case 4: Check for client connection example (telnet)
    const gettingStartedSection = page.locator('#getting-started');

    // Look for telnet connection example
    const telnetExample = gettingStartedSection.locator('pre:has-text("telnet")').first();
    await expect(telnetExample).toBeVisible();

    // Verify telnet localhost command
    const telnetCode = await telnetExample.textContent();
    expect(telnetCode.toLowerCase()).toContain('telnet');
    expect(telnetCode).toContain('localhost');
  });

  test('Getting started section has proper structure', async ({ page }) => {
    // Verify section structure
    const gettingStartedSection = page.locator('#getting-started');

    // Check for title
    const title = gettingStartedSection.locator('.getting-started__title');
    await expect(title).toBeVisible();
    await expect(title).toContainText('Getting Started');

    // Check for subtitle
    const subtitle = gettingStartedSection.locator('.getting-started__subtitle');
    await expect(subtitle).toBeVisible();

    // Check for section containers (Installation and Connecting)
    const sections = gettingStartedSection.locator('.getting-started__section');
    const sectionCount = await sections.count();
    expect(sectionCount).toBeGreaterThanOrEqual(2);
  });

  test('Installation section has step-by-step instructions', async ({ page }) => {
    // Verify installation steps
    const gettingStartedSection = page.locator('#getting-started');
    const steps = gettingStartedSection.locator('.getting-started__step');

    const stepCount = await steps.count();
    expect(stepCount).toBeGreaterThanOrEqual(3);

    // Check that steps have titles
    const stepTitles = gettingStartedSection.locator('.getting-started__step-title');
    const titleCount = await stepTitles.count();
    expect(titleCount).toBeGreaterThanOrEqual(3);
  });

  test('Code blocks have copy buttons', async ({ page }) => {
    // Verify copy buttons exist
    const gettingStartedSection = page.locator('#getting-started');
    const copyButtons = gettingStartedSection.locator('.getting-started__copy-btn');

    const count = await copyButtons.count();
    expect(count).toBeGreaterThan(0);

    // Check first copy button has proper attributes
    const firstCopyBtn = copyButtons.first();
    await expect(firstCopyBtn).toBeVisible();
    await expect(firstCopyBtn).toHaveAttribute('data-copy-target');
    await expect(firstCopyBtn).toHaveAttribute('aria-label', 'Copy to clipboard');
  });

  test('Git clone command is present', async ({ page }) => {
    // Verify git clone command
    const gettingStartedSection = page.locator('#getting-started');
    const cloneExample = gettingStartedSection.locator('pre:has-text("git clone")').first();
    await expect(cloneExample).toBeVisible();

    const cloneCode = await cloneExample.textContent();
    expect(cloneCode).toContain('https://github.com/yetone/mirdb');
  });

  test('Python memcached client example is present', async ({ page }) => {
    // Verify memcached client example
    const gettingStartedSection = page.locator('#getting-started');
    const pythonExample = gettingStartedSection.locator('pre:has-text("pymemcache")').first();
    await expect(pythonExample).toBeVisible();
  });

  test('Default configuration note is visible', async ({ page }) => {
    // Verify configuration note
    const gettingStartedSection = page.locator('#getting-started');
    const note = gettingStartedSection.locator('.getting-started__note');
    await expect(note).toBeVisible();

    // Check note mentions default port and data directory
    const noteText = await note.textContent();
    expect(noteText).toContain('12333');
    expect(noteText).toContain('/tmp/mirdb');
  });

  test('Code header shows language labels', async ({ page }) => {
    // Verify language labels in getting started section
    const gettingStartedSection = page.locator('#getting-started');
    const langLabels = gettingStartedSection.locator('.getting-started__code-lang');

    const count = await langLabels.count();
    expect(count).toBeGreaterThan(0);

    // Check for bash label
    const bashLabel = gettingStartedSection.locator('.getting-started__code-lang:has-text("bash")');
    await expect(bashLabel.first()).toBeVisible();

    // Check for python label
    const pythonLabel = gettingStartedSection.locator('.getting-started__code-lang:has-text("python")');
    await expect(pythonLabel.first()).toBeVisible();
  });
});
