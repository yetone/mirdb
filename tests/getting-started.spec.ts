import { test, expect } from '@playwright/test';

/**
 * E2E Tests for Getting Started Section
 *
 * This test suite verifies that the Getting Started section includes
 * installation and basic usage instructions as required by REQ-3.
 *
 * Requirements: REQ-3 - Include a "Get Started" section with installation and basic usage instructions
 */

test.describe('Getting Started Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 1: Check for installation commands - Clear installation instructions including cargo build command are present', async ({ page }) => {
    // Navigate to Getting Started section
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Verify section has a heading
    const sectionHeading = gettingStartedSection.locator('h2');
    await expect(sectionHeading).toContainText('Getting Started');

    // Verify installation step with cargo build command is present
    const codeBlocks = gettingStartedSection.locator('pre code');

    // Get all code block contents
    const allCodeContent = await codeBlocks.allTextContents();
    const combinedContent = allCodeContent.join(' ');

    // Verify cargo build command is present
    expect(combinedContent).toContain('cargo build');

    // Verify git clone instructions are present
    expect(combinedContent).toContain('git clone');

    // Verify cd command to change directory is present
    expect(combinedContent).toContain('cd mirdb');
  });

  test('Test Case 2: Check for basic usage commands - Basic usage commands to run MirDB are displayed', async ({ page }) => {
    // Navigate to Getting Started section
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Get all code blocks in the section
    const codeBlocks = gettingStartedSection.locator('pre code');
    const allCodeContent = await codeBlocks.allTextContents();
    const combinedContent = allCodeContent.join(' ');

    // Verify the run command for mirdb-server is present
    expect(combinedContent).toContain('mirdb-server');

    // Verify telnet connection example is present (for testing the server)
    expect(combinedContent).toContain('telnet localhost');

    // Verify basic memcached commands are shown
    expect(combinedContent).toContain('set');
    expect(combinedContent).toContain('get');
  });

  test('Test Case 3: Check for configuration overview - Default configuration details (port 12333, etc.) are mentioned', async ({ page }) => {
    // Navigate to the page and look for port 12333 mention
    const pageContent = await page.textContent('body');

    // Verify default port 12333 is mentioned somewhere on the page
    expect(pageContent).toContain('12333');

    // Check Getting Started section specifically
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Get all code blocks to verify port is in examples
    const codeBlocks = gettingStartedSection.locator('pre code');
    const allCodeContent = await codeBlocks.allTextContents();
    const combinedContent = allCodeContent.join(' ');

    // Verify port 12333 is mentioned in connection example
    expect(combinedContent).toContain('12333');

    // Check if configuration section exists and displays default values
    const configSection = page.locator('#configuration');
    const hasConfigSection = await configSection.isVisible().catch(() => false);

    if (hasConfigSection) {
      // If configuration section exists, verify it shows key defaults
      const configContent = await configSection.textContent();
      expect(configContent).toContain('12333');
    }
  });

  test('Getting Started section should have clear step-by-step instructions', async ({ page }) => {
    // Navigate to Getting Started section
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Verify there are numbered steps
    const steps = gettingStartedSection.locator('.step');
    const stepCount = await steps.count();

    // Should have at least 2 steps (build and run)
    expect(stepCount).toBeGreaterThanOrEqual(2);

    // Verify each step has a heading
    for (let i = 0; i < stepCount; i++) {
      const step = steps.nth(i);
      const stepHeading = step.locator('h3');
      await expect(stepHeading).toBeVisible();
    }
  });

  test('Getting Started section should be accessible from hero CTA button', async ({ page }) => {
    // Find the Get Started button in the hero section
    const getStartedBtn = page.locator('a[href="#getting-started"]').first();
    await expect(getStartedBtn).toBeVisible();

    // Verify the button text
    await expect(getStartedBtn).toContainText('Get Started');

    // Click the button and verify navigation
    await getStartedBtn.click();

    // Verify the Getting Started section is now in view
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeInViewport();
  });

  test('Code blocks in Getting Started should be properly formatted', async ({ page }) => {
    // Navigate to Getting Started section
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Verify code blocks exist
    const codeBlocks = gettingStartedSection.locator('pre code');
    const codeBlockCount = await codeBlocks.count();

    // Should have multiple code blocks for different steps
    expect(codeBlockCount).toBeGreaterThanOrEqual(2);

    // Verify each code block has content
    for (let i = 0; i < codeBlockCount; i++) {
      const codeBlock = codeBlocks.nth(i);
      const content = await codeBlock.textContent();
      expect(content?.trim().length).toBeGreaterThan(0);
    }
  });
});
