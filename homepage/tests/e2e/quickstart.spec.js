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

/**
 * Scenario 17: Getting Started Accessibility Tests
 * Verifies users can access getting started instructions within 2 clicks (User Story 2)
 *
 * Requirements: Success metric - users can find getting started instructions within 2 clicks
 */
test.describe('Getting Started Accessibility - 2-Click Access (Scenario 17)', () => {
  test('Test Case 1: Click Get Started CTA shows installation instructions in 1 click', async ({ page }) => {
    // Start from homepage
    await page.goto('/');

    // Count: Starting point (0 clicks)
    // Verify user is at the top of the page (hero section visible)
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Click the Get Started CTA button from hero section
    const getStartedCTA = page.locator('[data-testid="cta-get-started"]');
    await expect(getStartedCTA).toBeVisible();
    await getStartedCTA.click();

    // Count: 1 click
    // Wait for smooth scroll animation to complete
    await page.waitForTimeout(600);

    // Verify Quick Start section is in viewport (installation instructions visible)
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeInViewport();

    // Verify installation instructions are visible (cargo build command)
    const installationBlock = page.locator('[data-testid="installation-commands"]');
    await expect(installationBlock).toBeVisible();

    // Verify the installation instructions contain the actual build command
    const codeContent = await installationBlock.textContent();
    expect(codeContent).toContain('cargo build');

    // SUCCESS: User can see installation instructions in 1 click
  });

  test('Test Case 2: Navigation path reaches installation instructions in 2 clicks maximum', async ({ page }) => {
    // Start from homepage
    await page.goto('/');

    // Count: Starting point (0 clicks)
    // Verify homepage loaded
    const header = page.locator('header');
    await expect(header).toBeVisible();

    // Click 1: Click Quick Start link in navigation
    const quickstartNavLink = page.locator('nav a[href="#quickstart"]').first();
    await expect(quickstartNavLink).toBeVisible();
    await quickstartNavLink.click();

    // Count: 1 click
    // Wait for smooth scroll animation
    await page.waitForTimeout(600);

    // Verify Quick Start section is in viewport
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeInViewport();

    // Verify installation instructions are visible
    const installationBlock = page.locator('[data-testid="installation-commands"]');
    await expect(installationBlock).toBeVisible();

    // SUCCESS: User can reach installation instructions in 1 click via navigation
    // (Even better than the 2-click requirement)
  });

  test('Test Case 3: Quick start section is on the main homepage (no additional page loads)', async ({ page }) => {
    // Navigate to homepage
    await page.goto('/');

    // Track the initial URL
    const initialUrl = page.url();

    // Verify Quick Start section exists on the same page (no page navigation required)
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeAttached();

    // Scroll to Quick Start section
    await quickstartSection.scrollIntoViewIfNeeded();

    // Verify URL has not changed (no page navigation)
    const currentUrl = page.url();
    // Allow for hash changes but not full page navigation
    expect(currentUrl.split('#')[0]).toBe(initialUrl.split('#')[0]);

    // Verify installation instructions are present on this page
    const installationBlock = quickstartSection.locator('[data-testid="installation-commands"]');
    await expect(installationBlock).toBeVisible();

    // Verify the content contains actual installation instructions
    const codeContent = await installationBlock.textContent();
    expect(codeContent).toContain('cargo build --release');
    expect(codeContent).toContain('git clone');

    // SUCCESS: Quick start section is on the main homepage with no additional page loads required
  });

  test('Hero Get Started CTA button is prominently displayed and accessible', async ({ page }) => {
    // Navigate to homepage
    await page.goto('/');

    // Verify Get Started button exists in hero section
    const getStartedCTA = page.locator('#hero [data-testid="cta-get-started"]');
    await expect(getStartedCTA).toBeVisible();

    // Verify it's a link with proper href
    const href = await getStartedCTA.getAttribute('href');
    expect(href).toBe('#quickstart');

    // Verify it has appropriate styling (primary button class)
    const hasButtonClass = await getStartedCTA.evaluate(el =>
      el.classList.contains('btn') && el.classList.contains('btn-primary')
    );
    expect(hasButtonClass).toBeTruthy();

    // Verify the button text is clear
    const buttonText = await getStartedCTA.textContent();
    expect(buttonText.toLowerCase()).toContain('get started');
  });

  test('Quick Start navigation link is visible in header navigation', async ({ page }) => {
    // Navigate to homepage
    await page.goto('/');

    // Verify navigation contains Quick Start link
    const navLinks = page.locator('nav .nav-links a[href="#quickstart"]');
    await expect(navLinks.first()).toBeVisible();

    // Verify the link text indicates getting started/quick start
    const linkText = await navLinks.first().textContent();
    expect(linkText.toLowerCase()).toMatch(/quick\s*start|getting\s*started/);
  });

  test('Both primary and secondary paths lead to same Quick Start section', async ({ page }) => {
    // Test primary path: Get Started CTA
    await page.goto('/');
    const getStartedCTA = page.locator('[data-testid="cta-get-started"]');
    const ctaHref = await getStartedCTA.getAttribute('href');

    // Test secondary path: Navigation link
    const navLink = page.locator('nav a[href="#quickstart"]').first();
    const navHref = await navLink.getAttribute('href');

    // Both should point to the same section
    expect(ctaHref).toBe('#quickstart');
    expect(navHref).toBe('#quickstart');

    // Verify the Quick Start section actually exists
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeAttached();
  });
});
