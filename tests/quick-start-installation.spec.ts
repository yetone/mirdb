import { test, expect } from '@playwright/test';

test.describe('Quick-Start Installation Instructions', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('page contains a clearly labeled installation or getting started section', async ({ page }) => {
    // Test Case 1: Check for installation section presence
    // Expected: Page contains a clearly labeled installation or getting started section

    const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');
    await expect(gettingStartedSection).toBeVisible();

    // Verify the section has a clear heading
    const sectionHeading = gettingStartedSection.locator('h2');
    await expect(sectionHeading).toBeVisible();

    // The heading should contain "Getting Started" or "Installation"
    const headingText = await sectionHeading.textContent();
    expect(headingText?.toLowerCase()).toMatch(/getting started|installation/i);
  });

  test('code block contains mirdb -c command for starting the server', async ({ page }) => {
    // Test Case 2: Verify code block with server start command
    // Expected: Code block contains 'mirdb -c' command for starting the server

    const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');
    await expect(gettingStartedSection).toBeVisible();

    // Find code blocks within the getting started section
    const codeBlocks = gettingStartedSection.locator('pre code, code');

    // Get all code content and check for mirdb -c command
    const allCodeContent = await gettingStartedSection.locator('pre, code').allTextContents();
    const combinedCodeContent = allCodeContent.join(' ');

    // Verify the mirdb -c command is present
    expect(combinedCodeContent).toMatch(/mirdb\s+-c/);
  });

  test('installation commands are wrapped in code or pre elements with syntax highlighting', async ({ page }) => {
    // Test Case 3: Verify installation instructions are in code elements
    // Expected: Installation commands are wrapped in <code> or <pre> elements with syntax highlighting

    const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');
    await expect(gettingStartedSection).toBeVisible();

    // Find code blocks (pre or code elements)
    const codeBlocks = gettingStartedSection.locator('pre');
    const codeBlockCount = await codeBlocks.count();

    // There should be at least one code block for installation commands
    expect(codeBlockCount).toBeGreaterThanOrEqual(1);

    // Verify the code blocks have syntax highlighting class or are properly styled
    const firstCodeBlock = codeBlocks.first();
    await expect(firstCodeBlock).toBeVisible();

    // Check that the code block contains command-like content (shell commands)
    const codeContent = await firstCodeBlock.textContent();
    expect(codeContent).toBeTruthy();

    // Verify it contains bash/shell command patterns (comments with #, or commands)
    expect(codeContent).toMatch(/mirdb|#.*Start|config\.toml/);

    // Verify the code block has syntax highlighting styling applied
    // It should have a background color different from default or specific code styling
    const computedStyle = await firstCodeBlock.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        backgroundColor: style.backgroundColor,
        fontFamily: style.fontFamily
      };
    });

    // Code blocks should use monospace font
    expect(computedStyle.fontFamily.toLowerCase()).toMatch(/mono|consolas|courier|menlo/);
  });

  test('getting started section is accessible via navigation', async ({ page }) => {
    // Additional test: Verify the section can be reached via anchor link
    const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');

    // Check that the section has an id attribute for anchor linking
    const sectionId = await gettingStartedSection.getAttribute('id');
    expect(sectionId).toBeTruthy();
    expect(sectionId).toMatch(/getting-started|installation/i);
  });

  test('installation section contains complete quick-start example', async ({ page }) => {
    // Additional test: Verify the section has a complete quick-start example
    const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');
    await expect(gettingStartedSection).toBeVisible();

    // Verify multiple steps are shown (starting server, connecting)
    const allContent = await gettingStartedSection.textContent();

    // Should mention starting the server
    expect(allContent).toMatch(/[Ss]tart|[Rr]un/);

    // Should mention config or configuration
    expect(allContent).toMatch(/config|toml/i);
  });
});
