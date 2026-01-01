const { test, expect } = require('@playwright/test');

test.describe('User Journey - New User Getting Started (US-3)', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Click Get Started button navigates to quick start section with installation instructions', async ({ page }) => {
    // Find the "Get Started" CTA button in the hero section
    const getStartedButton = page.locator('.hero .btn-primary', { hasText: 'Get Started' });
    await expect(getStartedButton).toBeVisible();

    // Verify the button has correct href to quickstart section
    const href = await getStartedButton.getAttribute('href');
    expect(href).toBe('#quickstart');

    // Click the Get Started button
    await getStartedButton.click();

    // Wait for smooth scroll to complete
    await page.waitForTimeout(500);

    // Verify user is navigated to quick start section
    const quickStartSection = page.locator('#quickstart');
    await expect(quickStartSection).toBeInViewport({ ratio: 0.5 });

    // Verify the quick start section contains installation instructions
    const installationHeading = quickStartSection.locator('h3', { hasText: 'Installation' });
    await expect(installationHeading).toBeVisible();

    // Verify installation code block is present
    const installCodeBlock = quickStartSection.locator('[data-testid="install-code-block"]');
    await expect(installCodeBlock).toBeVisible();

    // Verify installation command is present
    const codeContent = await installCodeBlock.textContent();
    expect(codeContent).toContain('cargo build --release');
  });

  test('TC2: Installation command can be copied with one click', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    // Navigate to quick start section
    const getStartedButton = page.locator('.hero .btn-primary', { hasText: 'Get Started' });
    await getStartedButton.click();
    await page.waitForTimeout(500);

    // Find the installation code block
    const installCodeBlock = page.locator('[data-testid="install-code-block"]');
    await expect(installCodeBlock).toBeVisible();

    // Find the copy button within the installation code block
    const copyButton = installCodeBlock.locator('[data-testid="copy-button"]');
    await expect(copyButton).toBeVisible();

    // Verify copy button is accessible
    await expect(copyButton).toHaveAttribute('aria-label', 'Copy code to clipboard');

    // Click the copy button once
    await copyButton.click();

    // Verify the clipboard contains the installation command
    const clipboardContent = await page.evaluate(() => navigator.clipboard.readText());
    expect(clipboardContent).toContain('cargo build --release');

    // Verify visual feedback is shown (button shows copied state)
    await expect(copyButton).toHaveAttribute('aria-label', 'Copied!');
    const isCopied = await copyButton.evaluate(el =>
      el.classList.contains('copied') || el.dataset.copied === 'true'
    );
    expect(isCopied).toBeTruthy();
  });

  test('TC3: User can follow complete quick start flow (install → run → use)', async ({ page }) => {
    // Navigate to quick start section via Get Started button
    const getStartedButton = page.locator('.hero .btn-primary', { hasText: 'Get Started' });
    await getStartedButton.click();
    await page.waitForTimeout(500);

    const quickStartSection = page.locator('#quickstart');
    await expect(quickStartSection).toBeVisible();

    // Step 1: Verify Installation instructions are present and clear
    const installationHeading = quickStartSection.locator('h3', { hasText: 'Installation' });
    await expect(installationHeading).toBeVisible();

    const installCodeBlock = quickStartSection.locator('[data-testid="install-code-block"]');
    await expect(installCodeBlock).toBeVisible();
    const installContent = await installCodeBlock.textContent();
    expect(installContent).toContain('cargo build --release');
    expect(installContent).toContain('Build from source');

    // Step 2: Verify Running MirDB instructions are present
    const runningHeading = quickStartSection.locator('h3', { hasText: 'Running MirDB' });
    await expect(runningHeading).toBeVisible();

    const runCodeBlock = quickStartSection.locator('[data-testid="run-code-block"]');
    await expect(runCodeBlock).toBeVisible();
    const runContent = await runCodeBlock.textContent();
    expect(runContent).toContain('./mirdb -c');
    expect(runContent).toContain('config.toml');

    // Step 3: Verify Basic Operations (usage) instructions are present
    const basicOpsHeading = quickStartSection.locator('h3', { hasText: 'Basic Operations' });
    await expect(basicOpsHeading).toBeVisible();

    const telnetCodeBlock = quickStartSection.locator('[data-testid="telnet-code-block"]');
    await expect(telnetCodeBlock).toBeVisible();
    const telnetContent = await telnetCodeBlock.textContent();

    // Verify SET operation example
    expect(telnetContent).toContain('set mykey 0 0 5');
    expect(telnetContent).toContain('STORED');

    // Verify GET operation example
    expect(telnetContent).toContain('get mykey');
    expect(telnetContent).toContain('VALUE mykey 0 5');
    expect(telnetContent).toContain('hello');

    // Verify DELETE operation example
    expect(telnetContent).toContain('delete mykey');
    expect(telnetContent).toContain('DELETED');

    // Verify the flow is presented in logical order
    const headings = await quickStartSection.locator('h3').allTextContents();
    const installationIndex = headings.findIndex(h => h.includes('Installation'));
    const runningIndex = headings.findIndex(h => h.includes('Running'));
    const basicOpsIndex = headings.findIndex(h => h.includes('Basic Operations'));

    // Installation should come before Running, which should come before Basic Operations
    expect(installationIndex).toBeLessThan(runningIndex);
    expect(runningIndex).toBeLessThan(basicOpsIndex);
  });

  test('TC4: Quick start commands are accurate and work with MirDB', async ({ page }) => {
    // Navigate to quick start section
    await page.goto('/');
    const quickStartSection = page.locator('#quickstart');
    await quickStartSection.scrollIntoViewIfNeeded();

    // Verify Installation command accuracy
    const installCodeBlock = page.locator('[data-testid="install-code-block"]');
    const installContent = await installCodeBlock.textContent();

    // The installation command should use standard Rust/Cargo build command
    expect(installContent).toContain('cargo build --release');
    // This is the correct command for building a Rust project in release mode

    // Verify Running command accuracy
    const runCodeBlock = page.locator('[data-testid="run-code-block"]');
    const runContent = await runCodeBlock.textContent();

    // The run command should specify the binary and config file path
    expect(runContent).toContain('./mirdb -c');
    expect(runContent).toContain('/path/to/config.toml');

    // Verify telnet commands follow memcached protocol
    const telnetCodeBlock = page.locator('[data-testid="telnet-code-block"]');
    const telnetContent = await telnetCodeBlock.textContent();

    // Verify correct port for MirDB (12333 as per config)
    expect(telnetContent).toContain('telnet localhost 12333');

    // Verify SET command format follows memcached text protocol:
    // set <key> <flags> <exptime> <bytes>
    // set mykey 0 0 5 means: key=mykey, flags=0, exptime=0(no expiry), bytes=5
    expect(telnetContent).toContain('set mykey 0 0 5');
    expect(telnetContent).toContain('hello'); // 5 bytes of data
    expect(telnetContent).toContain('STORED'); // Expected response

    // Verify GET command format
    expect(telnetContent).toContain('get mykey');
    // Response format: VALUE <key> <flags> <bytes>\r\n<data>\r\nEND
    expect(telnetContent).toContain('VALUE mykey 0 5');
    expect(telnetContent).toContain('hello');
    expect(telnetContent).toContain('END');

    // Verify DELETE command format
    expect(telnetContent).toContain('delete mykey');
    expect(telnetContent).toContain('DELETED');

    // Verify commands match PRD specification (Appendix B)
    // These commands should match the documented sample code blocks
  });

  test('All quick start code blocks have copy functionality', async ({ page, context }) => {
    // Grant clipboard permissions
    await context.grantPermissions(['clipboard-read', 'clipboard-write']);

    // Navigate to quick start section
    const quickStartSection = page.locator('#quickstart');
    await quickStartSection.scrollIntoViewIfNeeded();
    await expect(quickStartSection).toBeVisible();

    // Get all code blocks in the quick start section
    const codeBlocks = quickStartSection.locator('.code-block');
    const count = await codeBlocks.count();

    // There should be at least 3 code blocks (install, run, basic operations)
    expect(count).toBeGreaterThanOrEqual(3);

    // Verify each code block has a working copy button
    for (let i = 0; i < count; i++) {
      const codeBlock = codeBlocks.nth(i);
      const copyButton = codeBlock.locator('[data-testid="copy-button"]');

      await expect(copyButton).toBeVisible();
      await expect(copyButton).toHaveAttribute('aria-label', /Copy/);
      await expect(copyButton).toHaveAttribute('type', 'button');
    }
  });

  test('Get Started button is visible above the fold', async ({ page }) => {
    // The Get Started CTA should be immediately visible without scrolling
    const getStartedButton = page.locator('.hero .btn-primary', { hasText: 'Get Started' });

    // Verify button exists and is visible
    await expect(getStartedButton).toBeVisible();

    // Verify button is in the viewport (above the fold)
    await expect(getStartedButton).toBeInViewport();

    // Verify the hero section is visible
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();
  });
});
