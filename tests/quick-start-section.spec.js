const { test, expect } = require('@playwright/test');

test.describe('Quick Start Section (REQ-4)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Page contains a clearly labeled Quick Start or Getting Started section', async ({ page }) => {
    // Check for a section with id "quickstart" or similar
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeVisible();

    // Verify the section has a title containing "Quick Start" or "Getting Started"
    const sectionTitle = quickstartSection.locator('.section-title, h2').first();
    await expect(sectionTitle).toBeVisible();
    const titleText = await sectionTitle.textContent();
    const hasQuickStart = /quick\s*start/i.test(titleText);
    const hasGettingStarted = /getting\s*started/i.test(titleText);
    expect(hasQuickStart || hasGettingStarted).toBe(true);
  });

  test('TC2: Section contains at least one code snippet showing basic usage', async ({ page }) => {
    // Navigate to quickstart section
    const quickstartSection = page.locator('#quickstart');
    await quickstartSection.scrollIntoViewIfNeeded();

    // Check for code block element
    const codeBlock = quickstartSection.locator('.code-block, pre, code');
    await expect(codeBlock.first()).toBeVisible();

    // Verify the code block contains actual code content (not empty)
    const codeContent = await quickstartSection.locator('pre').textContent();
    expect(codeContent.trim().length).toBeGreaterThan(0);

    // Verify it shows basic usage patterns (commands)
    const hasCommands = codeContent.includes('set') || codeContent.includes('get') || codeContent.includes('telnet');
    expect(hasCommands).toBe(true);
  });

  test('TC3: Code example demonstrates SET command usage', async ({ page }) => {
    // Navigate to quickstart section
    const quickstartSection = page.locator('#quickstart');
    await quickstartSection.scrollIntoViewIfNeeded();

    // Get the code block content
    const codeContent = await quickstartSection.locator('pre').textContent();

    // Verify SET command is demonstrated
    // The SET command should be in the format: set <key> <flags> <ttl> <bytes>
    const hasSetCommand = /set\s+\w+/i.test(codeContent);
    expect(hasSetCommand).toBe(true);

    // Verify a SET response is shown (STORED)
    const hasStoredResponse = codeContent.includes('STORED');
    expect(hasStoredResponse).toBe(true);
  });

  test('TC4: Code example demonstrates GET command usage', async ({ page }) => {
    // Navigate to quickstart section
    const quickstartSection = page.locator('#quickstart');
    await quickstartSection.scrollIntoViewIfNeeded();

    // Get the code block content
    const codeContent = await quickstartSection.locator('pre').textContent();

    // Verify GET command is demonstrated
    const hasGetCommand = /get\s+\w+/i.test(codeContent);
    expect(hasGetCommand).toBe(true);

    // Verify a GET response is shown (VALUE or END)
    const hasValueResponse = codeContent.includes('VALUE') || codeContent.includes('END');
    expect(hasValueResponse).toBe(true);
  });

  test('TC5: Default port 12333 is displayed in connection examples', async ({ page }) => {
    // Navigate to quickstart section
    const quickstartSection = page.locator('#quickstart');
    await quickstartSection.scrollIntoViewIfNeeded();

    // Check for port 12333 in the quick start section
    const sectionContent = await quickstartSection.textContent();
    const hasDefaultPort = sectionContent.includes('12333');
    expect(hasDefaultPort).toBe(true);

    // Verify it appears in a connection context (telnet, localhost, or comment)
    const codeContent = await quickstartSection.locator('pre').textContent();
    const hasPortInCode = codeContent.includes('12333');
    expect(hasPortInCode).toBe(true);
  });

  test('Quick start section is accessible via navigation', async ({ page }) => {
    // Verify there's a navigation link to quick start
    const quickstartLink = page.locator('a[href="#quickstart"]');
    await expect(quickstartLink.first()).toBeVisible();

    // Click the link and verify the section scrolls into view
    await quickstartLink.first().click();
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeInViewport();
  });
});
