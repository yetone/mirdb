// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Test Suite: User Journey - Migrating User Quick Start
 * Scenario: Verify US-3: Migrating user can try MirDB within 5 minutes
 *
 * This test suite validates the complete user journey for a developer
 * with existing memcached infrastructure who wants to migrate to MirDB.
 * The focus is on the quick-start guide accessibility and completeness.
 */

test.describe('User Journey - Migrating User Quick Start', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to the homepage before each test
    await page.goto('/');
  });

  /**
   * Test Case 1: Click 'Get Started' CTA from hero
   * Input: Click 'Get Started' CTA from hero
   * Expected: User is directed to quick-start guide section
   *
   * Step 1: Find quick-start guide from hero CTA or navigation
   * Context: User persona is a developer with existing memcached infrastructure
   */
  test('TC1: Click Get Started CTA directs user to quick-start guide section', async ({ page }) => {
    // Verify the hero section is visible
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Find the "Get Started" CTA button in the hero section
    const getStartedBtn = page.locator('#get-started-btn');
    await expect(getStartedBtn).toBeVisible();
    await expect(getStartedBtn).toHaveText('Get Started');

    // Verify the CTA button links to the quick-start section
    const href = await getStartedBtn.getAttribute('href');
    expect(href).toBe('#quick-start');

    // Click the "Get Started" CTA
    await getStartedBtn.click();

    // Wait for scroll animation to complete
    await page.waitForTimeout(500);

    // Verify the quick-start section is now visible and in the viewport
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    // Verify the quick-start section is in the viewport (user can see it)
    const isInViewport = await quickStartSection.evaluate((el) => {
      const rect = el.getBoundingClientRect();
      return rect.top >= -100 && rect.top <= window.innerHeight;
    });
    expect(isInViewport).toBe(true);

    // Verify the quick-start section has a clear heading
    const quickStartHeading = quickStartSection.locator('h2');
    await expect(quickStartHeading).toBeVisible();
    await expect(quickStartHeading).toHaveText('Quick Start');
  });

  /**
   * Test Case 2: Read quick-start guide completeness
   * Input: Read quick-start guide completeness
   * Expected: Guide includes all steps from clone/install to running first command
   *
   * Step 2: Follow installation steps
   * Context: Acceptance criteria is MirDB running within 5 minutes
   */
  test('TC2: Quick-start guide includes all steps from clone/install to running first command', async ({ page }) => {
    // Navigate to the quick-start section
    const quickStartSection = page.locator('#quick-start');
    await quickStartSection.scrollIntoViewIfNeeded();
    await expect(quickStartSection).toBeVisible();

    // STEP 1: Verify installation instructions (cargo install)
    const installSection = page.locator('#install-section');
    await expect(installSection).toBeVisible();

    const installCommand = page.locator('#install-command');
    await expect(installCommand).toBeVisible();
    const installText = await installCommand.textContent();
    expect(installText).toContain('cargo install mirdb');

    // STEP 2: Verify build from source instructions (git clone + cargo build)
    const buildSection = page.locator('#build-from-source-section');
    await expect(buildSection).toBeVisible();

    const buildCommands = page.locator('#build-commands');
    await expect(buildCommands).toBeVisible();
    const buildText = await buildCommands.textContent();
    expect(buildText).toContain('git clone');
    expect(buildText).toContain('github.com');
    expect(buildText).toContain('mirdb');
    expect(buildText).toContain('cargo build --release');

    // STEP 3: Verify run instructions
    const runSection = page.locator('#run-section');
    await expect(runSection).toBeVisible();

    const runCommand = page.locator('#run-command');
    await expect(runCommand).toBeVisible();
    const runText = await runCommand.textContent();
    expect(runText).toContain('./target/release/mirdb');

    // STEP 4: Verify connect instructions
    const connectSection = page.locator('#connect-section');
    await expect(connectSection).toBeVisible();

    const connectExample = page.locator('#connect-example');
    await expect(connectExample).toBeVisible();
    const connectText = await connectExample.textContent();
    expect(connectText).toContain('telnet');
    expect(connectText).toContain('localhost');
    expect(connectText).toContain('12333');

    // STEP 5: Verify basic usage instructions (SET/GET)
    const usageSection = page.locator('#usage-section');
    await expect(usageSection).toBeVisible();

    const usageExample = page.locator('#usage-example');
    await expect(usageExample).toBeVisible();

    // Verify the logical order of steps (install -> build -> run -> connect -> use)
    const allSteps = page.locator('.quick-start-step');
    const stepCount = await allSteps.count();
    expect(stepCount).toBeGreaterThanOrEqual(5); // At least 5 steps for completeness
  });

  /**
   * Test Case 3: Verify telnet/memcached client example
   * Input: Verify telnet/memcached client example
   * Expected: Example shows connecting to localhost:12333 and running SET/GET
   *
   * Step 3: Verify memcached compatibility example
   * Context: User needs to validate compatibility with existing clients
   */
  test('TC3: Example shows connecting to localhost:12333 and running SET/GET', async ({ page }) => {
    // Navigate to the quick-start section
    const quickStartSection = page.locator('#quick-start');
    await quickStartSection.scrollIntoViewIfNeeded();
    await expect(quickStartSection).toBeVisible();

    // Verify the connect section shows telnet to localhost:12333
    const connectSection = page.locator('#connect-section');
    await expect(connectSection).toBeVisible();

    const connectExample = page.locator('#connect-example');
    await expect(connectExample).toBeVisible();
    const connectText = await connectExample.textContent();

    // Verify telnet connection example
    expect(connectText).toContain('telnet');
    expect(connectText).toContain('localhost');
    expect(connectText).toContain('12333');

    // Verify the usage section shows SET and GET commands
    const usageSection = page.locator('#usage-section');
    await expect(usageSection).toBeVisible();

    const usageExample = page.locator('#usage-example');
    await expect(usageExample).toBeVisible();
    const usageText = await usageExample.textContent();

    // Verify SET command is demonstrated
    expect(usageText.toLowerCase()).toContain('set');
    expect(usageText).toContain('STORED'); // Expected response from SET

    // Verify GET command is demonstrated
    expect(usageText.toLowerCase()).toContain('get');
    expect(usageText).toContain('VALUE'); // Expected response from GET
    expect(usageText).toContain('END'); // End marker for GET response

    // Verify the example shows a complete memcached session
    // This should include: SET key, receive STORED, GET key, receive VALUE + data + END
    expect(usageText).toContain('hello'); // Key name in the example
    expect(usageText).toContain('world'); // Value in the example
  });

  /**
   * Integration Test: Complete Migrating User Journey Flow
   * Validates the entire migrating user journey from hero to running commands
   */
  test('Complete user journey: Hero CTA -> Quick Start -> All Steps -> Memcached Example', async ({ page }) => {
    // JOURNEY START: User lands on homepage
    await expect(page).toHaveTitle(/MirDB/);

    // Verify the value proposition mentions memcached compatibility
    // (important for migrating users)
    const valueProposition = page.locator('#value-proposition');
    await expect(valueProposition).toBeVisible();
    const valueText = await valueProposition.textContent();
    expect(valueText.toLowerCase()).toContain('memcached');

    // STEP 1: User clicks "Get Started" from hero section
    const getStartedBtn = page.locator('#get-started-btn');
    await expect(getStartedBtn).toBeVisible();
    await getStartedBtn.click();

    // Wait for navigation to quick-start section
    await page.waitForTimeout(500);

    // STEP 2: Verify user is at quick-start section
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    const isInViewport = await quickStartSection.evaluate((el) => {
      const rect = el.getBoundingClientRect();
      return rect.top >= -100 && rect.top <= window.innerHeight;
    });
    expect(isInViewport).toBe(true);

    // STEP 3: Verify all installation methods are documented
    // Option A: cargo install
    const installCommand = page.locator('#install-command');
    const installText = await installCommand.textContent();
    expect(installText).toContain('cargo install mirdb');

    // Option B: Build from source
    const buildCommands = page.locator('#build-commands');
    const buildText = await buildCommands.textContent();
    expect(buildText).toContain('git clone');
    expect(buildText).toContain('cargo build --release');

    // STEP 4: Verify run command is documented
    const runCommand = page.locator('#run-command');
    const runText = await runCommand.textContent();
    expect(runText).toContain('./target/release/mirdb');

    // STEP 5: Verify memcached client connection example
    const connectExample = page.locator('#connect-example');
    const connectText = await connectExample.textContent();
    expect(connectText).toContain('telnet localhost 12333');

    // STEP 6: Verify SET/GET example for memcached compatibility validation
    const usageExample = page.locator('#usage-example');
    const usageText = await usageExample.textContent();

    // Migrating users need to see familiar memcached commands working
    expect(usageText.toLowerCase()).toContain('set');
    expect(usageText).toContain('STORED');
    expect(usageText.toLowerCase()).toContain('get');
    expect(usageText).toContain('VALUE');
    expect(usageText).toContain('END');

    // JOURNEY COMPLETE: User can validate MirDB works with their memcached clients
    // The 5-minute target is achievable because:
    // 1. Installation is documented (cargo install or build from source)
    // 2. Run command is simple (single binary)
    // 3. Connection method is standard (telnet or any memcached client)
    // 4. Example commands are provided (SET/GET)
  });

  /**
   * Additional Test: Verify quick-start is accessible via navigation
   * Ensures migrating users can find the quick-start from multiple entry points
   */
  test('Quick-start section is accessible via Docs navigation link', async ({ page }) => {
    // Find the "Docs" link in the navigation
    const docsLink = page.locator('nav a[href="#quick-start"]');
    await expect(docsLink).toBeVisible();

    // Click the Docs link
    await docsLink.click();

    // Wait for scroll animation
    await page.waitForTimeout(500);

    // Verify the quick-start section is in view
    const quickStartSection = page.locator('#quick-start');
    const isInViewport = await quickStartSection.evaluate((el) => {
      const rect = el.getBoundingClientRect();
      return rect.top >= -100 && rect.top <= window.innerHeight;
    });
    expect(isInViewport).toBe(true);
  });

  /**
   * Additional Test: Verify default port 12333 is consistent
   * Important for migrating users configuring their clients
   */
  test('Default port 12333 is consistently documented', async ({ page }) => {
    // Check the connect example
    const connectExample = page.locator('#connect-example');
    await expect(connectExample).toBeVisible();
    const connectText = await connectExample.textContent();
    expect(connectText).toContain('12333');

    // Scroll to configuration section
    const configSection = page.locator('#configuration');
    await configSection.scrollIntoViewIfNeeded();
    await expect(configSection).toBeVisible();

    // Verify the default addr configuration shows the same port
    const addrConfig = page.locator('[data-param="addr"]');
    await expect(addrConfig).toBeVisible();
    const addrText = await addrConfig.textContent();
    expect(addrText).toContain('12333');
  });
});
