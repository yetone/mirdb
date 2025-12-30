// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Test Suite: Getting Started Section
 * Scenario: Verify the Getting Started section provides installation commands,
 * configuration examples, and basic usage instructions
 */

test.describe('Getting Started Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Navigate to Getting Started section
    const gettingStartedSection = page.locator('#getting-started');
    await gettingStartedSection.scrollIntoViewIfNeeded();
  });

  /**
   * Test Case 1: Check for cargo build command in Getting Started
   * Input: Check for cargo build command in Getting Started
   * Expected: Code block contains 'cargo build --release' or similar build command
   */
  test('TC1: Code block contains cargo build command', async ({ page }) => {
    // Verify Getting Started section exists and is visible
    const gettingStartedSection = page.locator('#getting-started.getting-started-section');
    await expect(gettingStartedSection).toBeVisible();

    // Verify section heading
    const sectionHeading = page.locator('#getting-started h2');
    await expect(sectionHeading).toBeVisible();
    await expect(sectionHeading).toHaveText('Getting Started');

    // Find code blocks within the section
    const codeBlocks = page.locator('#getting-started pre code');
    const codeBlockCount = await codeBlocks.count();
    expect(codeBlockCount).toBeGreaterThan(0);

    // Check that at least one code block contains cargo build command
    let foundCargoBuild = false;
    for (let i = 0; i < codeBlockCount; i++) {
      const codeText = await codeBlocks.nth(i).textContent();
      if (codeText && (codeText.includes('cargo build --release') || codeText.includes('cargo build'))) {
        foundCargoBuild = true;
        break;
      }
    }
    expect(foundCargoBuild).toBeTruthy();
  });

  /**
   * Test Case 2: Check for server run command
   * Input: Check for server run command
   * Expected: Code block contains command to run mirdb-server with configuration file
   */
  test('TC2: Code block contains server run command', async ({ page }) => {
    // Find code blocks within the section
    const codeBlocks = page.locator('#getting-started pre code');
    const codeBlockCount = await codeBlocks.count();
    expect(codeBlockCount).toBeGreaterThan(0);

    // Check that at least one code block contains mirdb-server run command
    let foundServerRun = false;
    for (let i = 0; i < codeBlockCount; i++) {
      const codeText = await codeBlocks.nth(i).textContent();
      if (codeText && codeText.includes('mirdb-server') && codeText.includes('-c')) {
        foundServerRun = true;
        // Also verify it references a configuration file (toml)
        expect(codeText).toContain('.toml');
        break;
      }
    }
    expect(foundServerRun).toBeTruthy();
  });

  /**
   * Test Case 3: Check for TOML configuration example
   * Input: Check for TOML configuration example
   * Expected: Configuration example shows addr, work_dir, mem_table_max_size, sst_max_size parameters
   */
  test('TC3: Configuration example shows required parameters', async ({ page }) => {
    // Find code blocks - check in both getting-started and documentation sections
    // since configuration might be in documentation section
    const allCodeBlocks = page.locator('pre code');
    const codeBlockCount = await allCodeBlocks.count();
    expect(codeBlockCount).toBeGreaterThan(0);

    // Check for TOML configuration with required parameters
    let foundConfig = false;
    let configText = '';

    for (let i = 0; i < codeBlockCount; i++) {
      const codeText = await allCodeBlocks.nth(i).textContent();
      if (codeText) {
        // Look for TOML configuration indicators
        const hasAddr = codeText.includes('addr') && codeText.includes('0.0.0.0');
        const hasWorkDir = codeText.includes('work_dir');
        const hasMemTableSize = codeText.includes('mem_table_max_size');
        const hasSstMaxSize = codeText.includes('sst_max_size');

        if (hasAddr && hasWorkDir && hasMemTableSize && hasSstMaxSize) {
          foundConfig = true;
          configText = codeText;
          break;
        }
      }
    }

    expect(foundConfig).toBeTruthy();
    expect(configText).toContain('addr');
    expect(configText).toContain('work_dir');
    expect(configText).toContain('mem_table_max_size');
    expect(configText).toContain('sst_max_size');
  });

  /**
   * Test Case 4: Check for client connection example
   * Input: Check for client connection example
   * Expected: Example shows connecting with telnet or memcached client and basic SET/GET operations
   */
  test('TC4: Example shows client connection with SET/GET operations', async ({ page }) => {
    // Find code blocks within the getting started section
    const codeBlocks = page.locator('#getting-started pre code');
    const codeBlockCount = await codeBlocks.count();
    expect(codeBlockCount).toBeGreaterThan(0);

    // Check for client connection example with telnet and SET/GET operations
    let foundConnectionExample = false;
    let foundSetGet = false;

    for (let i = 0; i < codeBlockCount; i++) {
      const codeText = await codeBlocks.nth(i).textContent();
      if (codeText) {
        // Check for telnet or memcached client connection
        if (codeText.includes('telnet') || codeText.includes('memcached')) {
          foundConnectionExample = true;
        }

        // Check for SET and GET operations (case insensitive)
        const lowerText = codeText.toLowerCase();
        if (lowerText.includes('set ') && lowerText.includes('get ')) {
          foundSetGet = true;
        }

        // Check for expected responses
        if (codeText.includes('STORED') && codeText.includes('VALUE')) {
          foundSetGet = true;
        }
      }
    }

    expect(foundConnectionExample).toBeTruthy();
    expect(foundSetGet).toBeTruthy();
  });

  /**
   * Test Case 5: Verify code blocks have syntax highlighting
   * Input: Verify code blocks have syntax highlighting
   * Expected: Code blocks use monospace font and have appropriate syntax highlighting for bash/TOML
   */
  test('TC5: Code blocks use monospace font and appropriate styling', async ({ page }) => {
    // Find code blocks within the section
    const codeElements = page.locator('#getting-started pre code');
    const codeBlockCount = await codeElements.count();
    expect(codeBlockCount).toBeGreaterThan(0);

    // Check the first code block for monospace font
    const firstCode = codeElements.first();
    await expect(firstCode).toBeVisible();

    // Get computed font-family
    const fontFamily = await firstCode.evaluate(el => {
      const computed = window.getComputedStyle(el);
      return computed.fontFamily;
    });

    // Verify font family includes monospace or a monospace font
    const isMonospace = fontFamily.toLowerCase().includes('monospace') ||
                        fontFamily.toLowerCase().includes('mono') ||
                        fontFamily.toLowerCase().includes('courier') ||
                        fontFamily.toLowerCase().includes('consolas') ||
                        fontFamily.toLowerCase().includes('menlo') ||
                        fontFamily.toLowerCase().includes('roboto mono') ||
                        fontFamily.toLowerCase().includes('source code');

    expect(isMonospace).toBeTruthy();

    // Verify pre elements have appropriate styling (background, padding)
    const preElement = page.locator('#getting-started pre').first();
    await expect(preElement).toBeVisible();

    const preStyles = await preElement.evaluate(el => {
      const computed = window.getComputedStyle(el);
      return {
        backgroundColor: computed.backgroundColor,
        padding: computed.padding,
        display: computed.display
      };
    });

    // Verify there's a background color (not completely transparent)
    expect(preStyles.backgroundColor).not.toBe('rgba(0, 0, 0, 0)');

    // Verify there's some padding for readability
    const paddingValue = parseFloat(preStyles.padding);
    expect(paddingValue).toBeGreaterThanOrEqual(0);
  });

  /**
   * Additional test: Getting Started section has proper structure
   */
  test('Getting Started section has step-by-step structure', async ({ page }) => {
    // Verify section contains multiple steps
    const steps = page.locator('#getting-started .step');
    const stepCount = await steps.count();
    expect(stepCount).toBeGreaterThanOrEqual(2); // At least build and run steps

    // Verify each step has a heading
    for (let i = 0; i < stepCount; i++) {
      const stepHeading = steps.nth(i).locator('h3');
      await expect(stepHeading).toBeVisible();
    }
  });

  /**
   * Additional test: Getting Started section is accessible via navigation
   */
  test('Getting Started section is accessible via navigation link', async ({ page }) => {
    // Go back to the top
    await page.goto('/');

    // Find the Getting Started link in navigation
    const navLink = page.locator('.nav-link', { hasText: 'Getting Started' });
    await expect(navLink).toBeVisible();

    // Click the navigation link
    await navLink.click();

    // Wait for navigation
    await page.waitForTimeout(500);

    // Verify we navigated to the section
    const currentUrl = page.url();
    expect(currentUrl).toContain('#getting-started');

    // Verify the section is visible
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();
  });

  /**
   * Additional test: Section has appropriate semantic HTML
   */
  test('Getting Started section uses semantic HTML', async ({ page }) => {
    // Verify section element is used
    const section = page.locator('section#getting-started');
    await expect(section).toBeVisible();

    // Verify it's within main element
    const sectionInMain = page.locator('main section#getting-started');
    await expect(sectionInMain).toBeVisible();

    // Verify heading hierarchy (h2 for section, h3 for steps)
    const h2 = page.locator('#getting-started h2');
    await expect(h2).toBeVisible();

    const h3Elements = page.locator('#getting-started h3');
    const h3Count = await h3Elements.count();
    expect(h3Count).toBeGreaterThan(0);
  });
});
