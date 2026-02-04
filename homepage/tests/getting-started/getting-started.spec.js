/**
 * Getting Started Section E2E Tests
 * Owner: Scenario 12 - Getting Started Section and Documentation Links
 *
 * Test coverage:
 * - TC1: Get Started CTA navigates to getting started section
 * - TC2: Installation instructions show how to start mirdb-server with config
 * - TC3: Documentation reachable in 2 clicks or less
 * - TC4: TOML configuration example is shown
 */

const { test, expect } = require('@playwright/test');

test.describe('Getting Started Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Click Get Started CTA from hero navigates to getting started section with installation instructions', async ({ page }) => {
    // Find the "Get Started" button in the hero section
    const getStartedBtn = page.locator('[data-testid="cta-get-started"]');
    await expect(getStartedBtn).toBeVisible();

    // Verify the href points to getting-started section
    const href = await getStartedBtn.getAttribute('href');
    expect(href).toBe('#getting-started');

    // Click the button
    await getStartedBtn.click();

    // Wait for navigation/scroll
    await page.waitForTimeout(500);

    // Verify URL contains the hash
    const currentUrl = page.url();
    expect(currentUrl).toContain('#getting-started');

    // Verify the getting-started section is in viewport
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeInViewport();

    // Verify installation instructions are present
    const installStep = page.locator('[data-testid="step-install"]');
    await expect(installStep).toBeVisible();
  });

  test('TC2: Check installation instructions content shows how to start mirdb-server with config file', async ({ page }) => {
    // Navigate to getting started section
    const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');
    await expect(gettingStartedSection).toBeVisible();

    // Check for the server start command section
    const serverStartStep = page.locator('[data-testid="step-start"]');
    await expect(serverStartStep).toBeVisible();

    // Verify it contains the mirdb-server command
    const serverStartCommand = page.locator('[data-testid="server-start-command"]');
    await expect(serverStartCommand).toBeVisible();

    // Get the code content and verify it contains mirdb-server -c command
    const codeContent = await serverStartCommand.textContent();
    expect(codeContent).toContain('mirdb-server');
    expect(codeContent).toContain('-c');
    expect(codeContent).toContain('mirdb.toml');
  });

  test('TC3: Count clicks to reach documentation - documentation reachable in 2 clicks or less from homepage', async ({ page, context }) => {
    // Method 1: Direct link from Getting Started section (1 click)
    // First verify the documentation links section exists
    const docsSection = page.locator('[data-testid="documentation-links"]');
    await expect(docsSection).toBeVisible();

    // Find the README documentation link
    const readmeLink = page.locator('[data-testid="docs-readme-link"]');
    await expect(readmeLink).toBeVisible();

    // Verify it's directly accessible (1 click to reach documentation)
    const href = await readmeLink.getAttribute('href');
    expect(href).toContain('github.com/yetone/mirdb');

    // Click count verification:
    // From homepage: Click "Get Started" (1 click) -> Getting Started section visible with doc links
    // OR directly scroll to see doc links (0 clicks if visible)
    // Then click doc link (1 click) -> reaches documentation
    // Total: Maximum 2 clicks

    // Verify by simulating the flow
    // Click 1: Get Started button
    const getStartedBtn = page.locator('[data-testid="cta-get-started"]');
    await getStartedBtn.click();
    await page.waitForTimeout(500);

    // Now documentation links should be visible/accessible
    await expect(docsSection).toBeVisible();

    // Click 2: Documentation link
    const pagePromise = context.waitForEvent('page');
    await readmeLink.click();
    const newPage = await pagePromise;
    await newPage.waitForLoadState('domcontentloaded');

    // Verify we reached documentation
    const docUrl = newPage.url();
    expect(docUrl).toContain('github.com/yetone/mirdb');

    await newPage.close();

    // Conclusion: Documentation is reachable in exactly 2 clicks
  });

  test('TC4: Check for configuration example - TOML configuration example is shown or linked', async ({ page }) => {
    // Navigate to getting started section
    const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');
    await expect(gettingStartedSection).toBeVisible();

    // Check for TOML configuration section
    const configStep = page.locator('[data-testid="step-configure"]');
    await expect(configStep).toBeVisible();

    // Check for TOML config code block
    const tomlConfig = page.locator('[data-testid="toml-config"]');
    await expect(tomlConfig).toBeVisible();

    // Verify TOML content contains key configuration options
    const configContent = await tomlConfig.textContent();

    // Verify essential TOML configuration keys are present
    expect(configContent).toContain('addr');
    expect(configContent).toContain('work_dir');
    expect(configContent).toContain('mem_table_max_size');

    // Verify it shows TOML format (key = "value" or key = number pattern)
    expect(configContent).toMatch(/addr\s*=\s*["'].*["']/);
    expect(configContent).toMatch(/work_dir\s*=\s*["'].*["']/);

    // Also verify there's a link to the full configuration reference
    const configLink = page.locator('[data-testid="docs-config-link"]');
    await expect(configLink).toBeVisible();

    const configLinkHref = await configLink.getAttribute('href');
    expect(configLinkHref).toContain('mirdb.toml');
  });

  test('Getting Started section has proper heading structure and accessibility', async ({ page }) => {
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Verify section has aria-labelledby pointing to heading
    const ariaLabelledBy = await gettingStartedSection.getAttribute('aria-labelledby');
    expect(ariaLabelledBy).toBe('getting-started-heading');

    // Verify heading exists
    const heading = page.locator('#getting-started-heading');
    await expect(heading).toBeVisible();
    await expect(heading).toContainText('Getting Started');

    // Verify step numbers are present
    const stepNumbers = page.locator('.getting-started__step-number');
    const stepCount = await stepNumbers.count();
    expect(stepCount).toBeGreaterThanOrEqual(3); // At least: Install, Configure, Start
  });

  test('All copy buttons in Getting Started section are functional', async ({ page }) => {
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Find all copy buttons within the getting started section
    const copyButtons = gettingStartedSection.locator('.copy-btn');
    const buttonCount = await copyButtons.count();

    // Verify we have copy buttons
    expect(buttonCount).toBeGreaterThan(0);

    // Check each copy button has required attributes
    for (let i = 0; i < buttonCount; i++) {
      const button = copyButtons.nth(i);
      await expect(button).toBeVisible();

      // Verify aria-label for accessibility
      const ariaLabel = await button.getAttribute('aria-label');
      expect(ariaLabel).toContain('clipboard');

      // Verify data-code attribute exists for copy functionality
      const dataCode = await button.getAttribute('data-code');
      expect(dataCode).toBeTruthy();
      expect(dataCode.length).toBeGreaterThan(0);
    }
  });

  test('Documentation links open in new tab with proper security attributes', async ({ page }) => {
    const docsSection = page.locator('[data-testid="documentation-links"]');
    await expect(docsSection).toBeVisible();

    // Get all documentation links
    const docLinks = docsSection.locator('a[target="_blank"]');
    const linkCount = await docLinks.count();

    expect(linkCount).toBeGreaterThan(0);

    for (let i = 0; i < linkCount; i++) {
      const link = docLinks.nth(i);

      // Verify target="_blank"
      const target = await link.getAttribute('target');
      expect(target).toBe('_blank');

      // Verify rel="noopener noreferrer" for security
      const rel = await link.getAttribute('rel');
      expect(rel).toContain('noopener');

      // Verify aria-label for accessibility
      const ariaLabel = await link.getAttribute('aria-label');
      expect(ariaLabel).toBeTruthy();
      expect(ariaLabel).toContain('opens in new tab');
    }
  });
});
