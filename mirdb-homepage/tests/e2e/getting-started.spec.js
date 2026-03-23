/**
 * Getting Started Section Tests
 * Owner: Scenario 3 - Getting Started Guide Section
 *
 * Test cases:
 * - Git clone command present
 * - Cargo build command present
 * - Run command present
 * - SET operation example
 * - GET operation example
 * - Syntax highlighting applied
 * - Copy-pasteable code blocks
 */
const { test, expect } = require('@playwright/test');

test.describe('Getting Started Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('http://localhost:3000/index.html');
    // Navigate to getting-started section
    await page.locator('#getting-started').scrollIntoViewIfNeeded();
  });

  test('TC1: should contain git clone command with MirDB repository URL', async ({ page }) => {
    const gettingStartedSection = page.locator('#getting-started');

    // Check for git clone command
    const codeBlocks = gettingStartedSection.locator('pre code');
    const allCodeContent = await codeBlocks.allTextContents();
    const combinedCode = allCodeContent.join('\n');

    expect(combinedCode).toContain('git clone');
    expect(combinedCode).toContain('github.com/mirdb/mirdb');
  });

  test('TC2: should contain cargo build --release command', async ({ page }) => {
    const gettingStartedSection = page.locator('#getting-started');

    const codeBlocks = gettingStartedSection.locator('pre code');
    const allCodeContent = await codeBlocks.allTextContents();
    const combinedCode = allCodeContent.join('\n');

    expect(combinedCode).toContain('cargo build --release');
  });

  test('TC3: should show how to start the MirDB server', async ({ page }) => {
    const gettingStartedSection = page.locator('#getting-started');

    const codeBlocks = gettingStartedSection.locator('pre code');
    const allCodeContent = await codeBlocks.allTextContents();
    const combinedCode = allCodeContent.join('\n');

    // Should contain either cargo run or binary execution
    const hasCargoRun = combinedCode.includes('cargo run');
    const hasBinaryExecution = combinedCode.includes('./target/release/mirdb');

    expect(hasCargoRun || hasBinaryExecution).toBeTruthy();
  });

  test('TC4: should contain SET operation example', async ({ page }) => {
    const gettingStartedSection = page.locator('#getting-started');

    const codeBlocks = gettingStartedSection.locator('pre code');
    const allCodeContent = await codeBlocks.allTextContents();
    const combinedCode = allCodeContent.join('\n');

    // Check for SET command with key-value storage using netcat
    expect(combinedCode.toLowerCase()).toContain('set');
    expect(combinedCode).toContain('nc localhost');
    // Check for expected response or storage confirmation
    expect(combinedCode).toMatch(/STORED|set.*\d+.*\d+.*\d+/i);
  });

  test('TC5: should contain GET operation example', async ({ page }) => {
    const gettingStartedSection = page.locator('#getting-started');

    const codeBlocks = gettingStartedSection.locator('pre code');
    const allCodeContent = await codeBlocks.allTextContents();
    const combinedCode = allCodeContent.join('\n');

    // Check for GET command to retrieve value using netcat
    expect(combinedCode.toLowerCase()).toContain('get');
    expect(combinedCode).toContain('nc localhost');
    // Check for expected response format (VALUE or END)
    expect(combinedCode).toMatch(/VALUE|END/);
  });

  test('TC6: should have syntax highlighting classes on code blocks', async ({ page }) => {
    const gettingStartedSection = page.locator('#getting-started');

    // Check for code blocks with language-specific class
    const codeWithLanguageClass = gettingStartedSection.locator('pre code[class*="language-"]');
    const count = await codeWithLanguageClass.count();

    expect(count).toBeGreaterThan(0);

    // Verify at least one code block has syntax highlighting class
    const firstCode = codeWithLanguageClass.first();
    const className = await firstCode.getAttribute('class');
    expect(className).toMatch(/language-\w+/);
  });

  test('TC7: should have copy-pasteable code blocks with pre/code tags', async ({ page }) => {
    const gettingStartedSection = page.locator('#getting-started');

    // Check that code blocks use proper pre/code structure
    const preCodeBlocks = gettingStartedSection.locator('pre code');
    const count = await preCodeBlocks.count();

    expect(count).toBeGreaterThan(0);

    // Verify code blocks are styled for easy selection
    const firstPre = gettingStartedSection.locator('pre').first();
    const firstPreStyles = await firstPre.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        fontFamily: styles.fontFamily,
        whiteSpace: styles.whiteSpace,
        overflow: styles.overflow
      };
    });

    // Code blocks should use monospace font and preserve whitespace
    expect(firstPreStyles.fontFamily.toLowerCase()).toMatch(/mono|consolas|menlo|courier/);
  });

  test('should have proper section structure with heading', async ({ page }) => {
    const section = page.locator('#getting-started');

    // Check section exists and has proper heading
    await expect(section).toBeVisible();

    const heading = section.locator('h2');
    await expect(heading).toHaveText('Getting Started');
  });

  test('should have step-by-step instructions in order', async ({ page }) => {
    const gettingStartedSection = page.locator('#getting-started');

    // Check for ordered steps (h3 headings)
    const steps = gettingStartedSection.locator('h3');
    const stepCount = await steps.count();

    expect(stepCount).toBeGreaterThanOrEqual(3);

    // Verify steps are numbered
    const stepTexts = await steps.allTextContents();
    expect(stepTexts[0]).toMatch(/1\./);
    expect(stepTexts[1]).toMatch(/2\./);
    expect(stepTexts[2]).toMatch(/3\./);
  });
});
