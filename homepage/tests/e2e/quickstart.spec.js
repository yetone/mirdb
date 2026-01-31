/**
 * Quick Start Section E2E Tests
 * Owner: Scenario 4 - Quick Start Guide Section
 *
 * Tests:
 * - Installation instructions presence
 * - Configuration example
 * - Sample code (get, set, delete)
 * - Documentation link
 *
 * Requirements: REQ-4
 */

const { test, expect } = require('@playwright/test');

test.describe('Quick Start Guide Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Navigate to Quick Start section
    const quickstartSection = page.locator('#quickstart');
    await quickstartSection.scrollIntoViewIfNeeded();
  });

  test('Test Case 1: Installation code block contains cargo build command', async ({ page }) => {
    const quickstartSection = page.locator('#quickstart');

    // Verify Quick Start section exists and is visible
    await expect(quickstartSection).toBeVisible();

    // Look for code block with installation command
    const installationBlock = quickstartSection.locator('[data-testid="installation-commands"]');
    await expect(installationBlock).toBeVisible();

    // Verify it contains cargo build --release command
    const codeContent = await installationBlock.textContent();
    expect(codeContent).toContain('cargo build --release');
  });

  test('Test Case 2: Server startup command is documented', async ({ page }) => {
    const quickstartSection = page.locator('#quickstart');

    // Look for code block showing server startup
    const startupBlock = quickstartSection.locator('[data-testid="server-startup"]');
    await expect(startupBlock).toBeVisible();

    // Verify it shows how to start mirdb-server with configuration
    const codeContent = await startupBlock.textContent();
    expect(codeContent).toMatch(/mirdb-server|mirdb/i);
    expect(codeContent).toMatch(/-c|--config|\.toml|\.conf/);
  });

  test('Test Case 3: Basic operations example demonstrates set, get, and delete', async ({ page }) => {
    const quickstartSection = page.locator('#quickstart');

    // Look for code block with basic operations
    const operationsBlock = quickstartSection.locator('[data-testid="basic-operations"]');
    await expect(operationsBlock).toBeVisible();

    // Verify set, get, and delete operations are shown
    const codeContent = await operationsBlock.textContent();
    expect(codeContent.toLowerCase()).toContain('set');
    expect(codeContent.toLowerCase()).toContain('get');
    expect(codeContent.toLowerCase()).toContain('delete');
  });

  test('Test Case 4: Configuration example shows addr, work_dir, and size limit options', async ({ page }) => {
    const quickstartSection = page.locator('#quickstart');

    // Look for configuration code block
    const configBlock = quickstartSection.locator('[data-testid="config-example"]');
    await expect(configBlock).toBeVisible();

    // Verify required configuration options are present
    const codeContent = await configBlock.textContent();
    expect(codeContent).toContain('addr');
    expect(codeContent).toContain('work_dir');
    // Check for size limit options (mem_table_max_size or sst_max_size)
    expect(codeContent).toMatch(/mem_table_max_size|sst_max_size|max_size/);
  });

  test('Test Case 6: Section contains link to full documentation', async ({ page }) => {
    const quickstartSection = page.locator('#quickstart');

    // Look for documentation link
    const docLink = quickstartSection.locator('a[data-testid="docs-link"], a:has-text("documentation"), a:has-text("docs")');
    await expect(docLink.first()).toBeVisible();

    // Verify the link has an href
    const href = await docLink.first().getAttribute('href');
    expect(href).toBeTruthy();
    expect(href.length).toBeGreaterThan(0);
  });

  test('Quick Start section is reachable from Get Started CTA', async ({ page }) => {
    // Go back to the top of the page
    await page.goto('/');

    // Click the Get Started button
    const getStartedBtn = page.locator('[data-testid="cta-get-started"]');
    await getStartedBtn.click();

    // Wait for scroll animation
    await page.waitForTimeout(500);

    // Verify Quick Start section is in view
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeInViewport();
  });

  test('Quick Start section has proper semantic structure', async ({ page }) => {
    const quickstartSection = page.locator('#quickstart');

    // Verify it's a section element
    const tagName = await quickstartSection.evaluate(el => el.tagName.toLowerCase());
    expect(tagName).toBe('section');

    // Verify it has aria-labelledby
    await expect(quickstartSection).toHaveAttribute('aria-labelledby', 'quickstart-title');

    // Verify the h2 has the correct id
    const h2 = quickstartSection.locator('h2');
    await expect(h2).toHaveAttribute('id', 'quickstart-title');
  });

  test('Code blocks use appropriate language classes for syntax highlighting', async ({ page }) => {
    const quickstartSection = page.locator('#quickstart');

    // Check for code elements with language classes
    const codeElements = quickstartSection.locator('code[class*="language-"], pre[class*="language-"]');
    const count = await codeElements.count();

    // Should have at least one code block with language class
    expect(count).toBeGreaterThan(0);

    // Verify language classes include bash, toml, or other expected languages
    const classes = [];
    for (let i = 0; i < count; i++) {
      const classAttr = await codeElements.nth(i).getAttribute('class');
      classes.push(classAttr);
    }

    // At least one should have a recognizable language class
    const hasLanguageClass = classes.some(cls =>
      cls && (cls.includes('language-bash') || cls.includes('language-toml') ||
              cls.includes('language-shell') || cls.includes('language-text'))
    );
    expect(hasLanguageClass).toBeTruthy();
  });

  test('Quick Start content includes step-by-step instructions', async ({ page }) => {
    const quickstartSection = page.locator('#quickstart');

    // Look for numbered steps or instruction headings
    const steps = quickstartSection.locator('.quickstart-step, h3, .step');
    const stepCount = await steps.count();

    // Should have multiple steps
    expect(stepCount).toBeGreaterThanOrEqual(2);
  });
});
