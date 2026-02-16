// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Getting Started Section E2E Tests
 * Owner: Scenario 3 - Getting Started Section Implementation
 */

test.describe('Getting Started Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  // Test Case 1: Navigate to #getting-started section
  test('should display Getting Started section with clear heading', async ({ page }) => {
    const section = page.locator('#getting-started');
    await expect(section).toBeVisible();

    const heading = page.locator('#getting-started-title');
    await expect(heading).toBeVisible();
    await expect(heading).toHaveText('Getting Started');

    const subtitle = page.locator('.getting-started__subtitle');
    await expect(subtitle).toBeVisible();
    await expect(subtitle).toContainText('5 minutes');
  });

  // Test Case 2: Verify installation command presence
  test('should display installation command with cargo install mirdb-server', async ({ page }) => {
    const installCommand = page.locator('#install-command');
    await expect(installCommand).toBeVisible();

    const commandText = await installCommand.textContent();
    expect(commandText).toContain('cargo install mirdb-server');
  });

  // Test Case 3: Click copy button on installation command
  test('should copy installation command to clipboard and show feedback', async ({ page, context }) => {
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    const copyButton = page.locator('[data-copy-target="#install-command"]');
    await expect(copyButton).toBeVisible();

    await copyButton.click();

    await expect(copyButton).toHaveClass(/getting-started__copy-btn--copied/);

    const checkIcon = copyButton.locator('.check-icon');
    await expect(checkIcon).toBeVisible();

    await page.waitForTimeout(2500);
    await expect(copyButton).not.toHaveClass(/getting-started__copy-btn--copied/);
  });

  // Test Case 4: Verify server startup example
  test('should display server startup example with mirdb-server command', async ({ page }) => {
    const startCommand = page.locator('#start-command');
    await expect(startCommand).toBeVisible();

    const commandText = await startCommand.textContent();
    expect(commandText).toContain('mirdb-server');
    expect(commandText).toContain('Listening on');
    expect(commandText).toContain('12333');
  });

  // Test Case 5: Verify basic usage commands
  test('should display examples for set, get, and delete operations', async ({ page }) => {
    const connectCommand = page.locator('#connect-command');
    await expect(connectCommand).toBeVisible();
    const connectText = await connectCommand.textContent();
    expect(connectText).toContain('telnet localhost 12333');
    expect(connectText).toContain('nc localhost 12333');

    const setCommand = page.locator('#set-command');
    await expect(setCommand).toBeVisible();

    const getCommand = page.locator('#get-command');
    await expect(getCommand).toBeVisible();

    const deleteCommand = page.locator('#delete-command');
    await expect(deleteCommand).toBeVisible();
  });

  // Test Case 6: Verify set command example format
  test('should show set command with proper format', async ({ page }) => {
    const setCommand = page.locator('#set-command');
    const setText = await setCommand.textContent();

    expect(setText).toContain('set');
    expect(setText).toMatch(/set\s+\w+\s+\d+\s+\d+\s+\d+/);
    expect(setText).toContain('STORED');
  });

  // Test Case 7: Verify get command example and response format
  test('should show get command and expected response format', async ({ page }) => {
    const getCommand = page.locator('#get-command');
    const getText = await getCommand.textContent();

    expect(getText).toContain('get mykey');
    expect(getText).toContain('VALUE');
    expect(getText).toContain('END');
  });

  // Test Case 8: Verify default configuration display
  test('should display default configuration values', async ({ page }) => {
    const configTable = page.locator('.getting-started__config-table');
    await expect(configTable).toBeVisible();

    const tableText = await configTable.textContent();

    expect(tableText).toContain('12333');
    expect(tableText).toContain('/tmp/mirdb');
    expect(tableText).toContain('4MB');
    expect(tableText).toContain('100MB');
  });

  // Test Case 9: Test code block syntax highlighting
  test('should have syntax highlighting applied to code blocks', async ({ page }) => {
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    const codeBlocks = gettingStartedSection.locator('code');
    const codeBlockCount = await codeBlocks.count();
    expect(codeBlockCount).toBeGreaterThan(0);

    const tokenSelectors = [
      '.token',
      '[class*="token-"]',
      'span[class]'
    ];

    let hasHighlighting = false;
    for (const selector of tokenSelectors) {
      const codeWithHighlighting = gettingStartedSection.locator(`code ${selector}`);
      const count = await codeWithHighlighting.count();
      if (count > 0) {
        hasHighlighting = true;
        break;
      }
    }

    expect(hasHighlighting).toBe(true);

    const spansInCode = gettingStartedSection.locator('code span').first();
    if (await spansInCode.count() > 0) {
      const color = await spansInCode.evaluate((el) => {
        return window.getComputedStyle(el).color;
      });
      expect(color).toBeTruthy();
    }
  });

  // Test Case 10: Verify all code blocks have copy buttons
  test('should have copy button for each code block', async ({ page }) => {
    const codeBlocks = page.locator('#getting-started .getting-started__code');
    const copyButtons = page.locator('#getting-started .getting-started__copy-btn');

    const codeBlockCount = await codeBlocks.count();
    const copyButtonCount = await copyButtons.count();

    expect(codeBlockCount).toBeGreaterThan(0);
    expect(copyButtonCount).toBe(codeBlockCount);

    for (let i = 0; i < copyButtonCount; i++) {
      const button = copyButtons.nth(i);
      const target = await button.getAttribute('data-copy-target');
      expect(target).toBeTruthy();
    }
  });

  // Test Case 11: Test getting-started on mobile viewport
  test('should be responsive on mobile viewport', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 667 });

    const section = page.locator('#getting-started');
    await expect(section).toBeVisible();

    const codeBlocks = page.locator('#getting-started .getting-started__code pre');
    const firstCodeBlock = codeBlocks.first();
    await expect(firstCodeBlock).toBeVisible();

    const overflowX = await firstCodeBlock.evaluate((el) => {
      return window.getComputedStyle(el).overflowX;
    });
    expect(overflowX).toBe('auto');

    const copyButtons = page.locator('#getting-started .getting-started__copy-btn');
    const firstCopyButton = copyButtons.first();
    await expect(firstCopyButton).toBeVisible();

    const buttonBox = await firstCopyButton.boundingBox();
    expect(buttonBox).toBeTruthy();
    expect(buttonBox.width).toBeGreaterThanOrEqual(32);
    expect(buttonBox.height).toBeGreaterThanOrEqual(32);
  });

  test('should have accessible copy buttons with aria-label', async ({ page }) => {
    const copyButtons = page.locator('#getting-started .getting-started__copy-btn');
    const count = await copyButtons.count();

    for (let i = 0; i < count; i++) {
      const button = copyButtons.nth(i);
      const ariaLabel = await button.getAttribute('aria-label');
      expect(ariaLabel).toBeTruthy();
      expect(ariaLabel).toContain('clipboard');
    }
  });

  test('should have step numbers for each step', async ({ page }) => {
    const stepNumbers = page.locator('#getting-started .getting-started__step-number');
    const count = await stepNumbers.count();

    expect(count).toBe(4);

    for (let i = 0; i < count; i++) {
      const stepNumber = stepNumbers.nth(i);
      const text = await stepNumber.textContent();
      expect(text).toBe(String(i + 1));
    }
  });

  test('should navigate to section when clicking anchor link', async ({ page }) => {
    await page.evaluate(() => window.scrollTo(0, 0));
    await page.goto('/#getting-started');

    const section = page.locator('#getting-started');
    await expect(section).toBeInViewport();
  });
});
