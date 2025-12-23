// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Progressive Enhancement E2E Tests
 *
 * Scenario: Verify core content works without JavaScript as per technical constraints
 * Related Requirements: Key Constraint in PRD - "Must work without JavaScript for core content"
 *
 * These tests verify that the MirDB homepage follows progressive enhancement principles:
 * - All core content is accessible without JavaScript
 * - Navigation links work without JS
 * - Code examples are visible without JS
 * - Page displays in a readable default theme without JS
 */

test.describe('Progressive Enhancement - No JavaScript', () => {
  // Use a browser context with JavaScript disabled
  test.use({ javaScriptEnabled: false });

  /**
   * Test Case 1: Page loads and all core content is visible and readable
   * Input: Load page with JavaScript disabled
   * Expected: Page loads and all core content is visible and readable
   */
  test('TC1: Page loads with JavaScript disabled and all core content is visible', async ({ page }) => {
    // Navigate to the page with JS disabled
    await page.goto('/');

    // Verify the page title is correct
    await expect(page).toHaveTitle(/MirDB/);

    // Verify hero section is visible
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Verify product name is visible
    const productName = page.locator('[data-testid="product-name"]');
    await expect(productName).toBeVisible();
    await expect(productName).toContainText('MirDB');

    // Verify tagline is visible
    const tagline = page.locator('[data-testid="tagline"]');
    await expect(tagline).toBeVisible();
    await expect(tagline).toContainText('persistent key-value store');

    // Verify value proposition is visible
    const valueProp = page.locator('[data-testid="value-proposition"]');
    await expect(valueProp).toBeVisible();

    // Verify features section is visible
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Verify feature cards are visible
    const featureCards = page.locator('.feature-card');
    await expect(featureCards.first()).toBeVisible();
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(4);

    // Verify getting started section is visible
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Verify project status section is visible
    const projectStatusSection = page.locator('#project-status');
    await expect(projectStatusSection).toBeVisible();

    // Verify code examples section is visible
    const codeExamplesSection = page.locator('#code-examples');
    await expect(codeExamplesSection).toBeVisible();

    // Verify configuration section is visible
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Verify footer is visible
    const footer = page.locator('footer.footer');
    await expect(footer).toBeVisible();

    // Verify main content is readable (has text content)
    const mainContent = page.locator('main#main-content');
    await expect(mainContent).toBeVisible();
    const textContent = await mainContent.textContent();
    expect(textContent.length).toBeGreaterThan(500); // Page should have substantial content
  });

  /**
   * Test Case 2: Navigation links work without JavaScript
   * Input: Check navigation without JavaScript
   * Expected: Navigation links work and navigate to correct sections/pages
   */
  test('TC2: Navigation links work without JavaScript', async ({ page }) => {
    await page.goto('/');

    // Verify header navigation is visible
    const headerNav = page.locator('header .nav');
    await expect(headerNav).toBeVisible();

    // Verify navigation links are present and have correct hrefs
    const navLinks = page.locator('.nav-links a');
    const linkCount = await navLinks.count();
    expect(linkCount).toBeGreaterThanOrEqual(4);

    // Check Features link
    const featuresLink = page.locator('.nav-links a[href="#features"]');
    await expect(featuresLink).toBeVisible();
    await expect(featuresLink).toHaveAttribute('href', '#features');

    // Check Getting Started link
    const gettingStartedLink = page.locator('.nav-links a[href="#getting-started"]');
    await expect(gettingStartedLink).toBeVisible();
    await expect(gettingStartedLink).toHaveAttribute('href', '#getting-started');

    // Check Documentation link (external)
    const docsLink = page.locator('.nav-links a:has-text("Documentation")');
    await expect(docsLink).toBeVisible();
    const docsHref = await docsLink.getAttribute('href');
    expect(docsHref).toBeTruthy();

    // Check GitHub link (external)
    const githubLink = page.locator('.nav-links a:has-text("GitHub")');
    await expect(githubLink).toBeVisible();
    const githubHref = await githubLink.getAttribute('href');
    expect(githubHref).toContain('github.com');

    // Test anchor navigation - click Features link
    await featuresLink.click();
    // Wait a moment for native browser navigation
    await page.waitForTimeout(100);

    // Verify URL hash changed (native HTML anchor behavior works without JS)
    expect(page.url()).toContain('#features');

    // Verify features section exists and is visible
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Test anchor navigation - click Getting Started link
    await gettingStartedLink.click();
    await page.waitForTimeout(100);
    expect(page.url()).toContain('#getting-started');

    // Verify Getting Started section exists
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Verify footer links exist and have correct attributes
    // Note: We check the footer links exist without scrolling since scrollIntoView uses JS
    const footer = page.locator('footer.footer');

    // Just verify the footer element exists in the DOM
    await expect(footer).toBeAttached();

    const footerGithubLink = footer.locator('a:has-text("GitHub")');
    await expect(footerGithubLink).toBeAttached();
    const footerGithubHref = await footerGithubLink.getAttribute('href');
    expect(footerGithubHref).toContain('github.com');

    const footerLicenseLink = footer.locator('a:has-text("License")');
    await expect(footerLicenseLink).toBeAttached();
  });

  /**
   * Test Case 3: Code examples are visible without JavaScript
   * Input: Check code examples without JavaScript
   * Expected: Code examples are visible (syntax highlighting optional without JS)
   */
  test('TC3: Code examples are visible without JavaScript', async ({ page }) => {
    await page.goto('/');

    // Navigate to code examples section
    const codeExamplesSection = page.locator('#code-examples');
    await codeExamplesSection.scrollIntoViewIfNeeded();
    await expect(codeExamplesSection).toBeVisible();

    // Verify SET operation example is visible
    const setExample = page.locator('[data-testid="set-example"]');
    await expect(setExample).toBeVisible();
    const setCodeBlock = page.locator('[data-testid="set-code-block"]');
    await expect(setCodeBlock).toBeVisible();
    const setCode = await setCodeBlock.textContent();
    expect(setCode).toContain('set');
    expect(setCode).toContain('STORED');

    // Verify GET operation example is visible
    const getExample = page.locator('[data-testid="get-example"]');
    await expect(getExample).toBeVisible();
    const getCodeBlock = page.locator('[data-testid="get-code-block"]');
    await expect(getCodeBlock).toBeVisible();
    const getCode = await getCodeBlock.textContent();
    expect(getCode).toContain('get');
    expect(getCode).toContain('VALUE');

    // Verify DELETE operation example is visible
    const deleteExample = page.locator('[data-testid="delete-example"]');
    await expect(deleteExample).toBeVisible();
    const deleteCodeBlock = page.locator('[data-testid="delete-code-block"]');
    await expect(deleteCodeBlock).toBeVisible();
    const deleteCode = await deleteCodeBlock.textContent();
    expect(deleteCode).toContain('delete');
    expect(deleteCode).toContain('DELETED');

    // Verify getting started code blocks are visible
    const gettingStartedSection = page.locator('#getting-started');
    await gettingStartedSection.scrollIntoViewIfNeeded();

    const cargoInstall = page.locator('[data-testid="cargo-install"]');
    await expect(cargoInstall).toBeVisible();
    const cargoCode = await cargoInstall.textContent();
    expect(cargoCode).toContain('cargo install mirdb');

    const buildFromSource = page.locator('[data-testid="build-from-source"]');
    await expect(buildFromSource).toBeVisible();
    const buildCode = await buildFromSource.textContent();
    expect(buildCode).toContain('git clone');
    expect(buildCode).toContain('cargo build');

    const serverStartup = page.locator('[data-testid="server-startup"]');
    await expect(serverStartup).toBeVisible();
    const serverCode = await serverStartup.textContent();
    expect(serverCode).toContain('mirdb-server');

    // Verify configuration example is visible
    const configSection = page.locator('#configuration');
    await configSection.scrollIntoViewIfNeeded();

    const tomlConfig = page.locator('[data-testid="toml-config-example"]');
    await expect(tomlConfig).toBeVisible();
    const tomlCode = await tomlConfig.textContent();
    expect(tomlCode).toContain('addr');
    expect(tomlCode).toContain('max_level');
    expect(tomlCode).toContain('work_dir');

    // Verify configuration table is visible
    const configTable = page.locator('[data-testid="config-params-table"]');
    await expect(configTable).toBeVisible();
  });

  /**
   * Test Case 4: Page displays in a readable default theme without JavaScript
   * Input: Check theme default without JavaScript
   * Expected: Page displays in a readable default theme without theme toggle
   */
  test('TC4: Page displays in readable default theme without JavaScript', async ({ page }) => {
    await page.goto('/');

    // Verify theme toggle button exists but doesn't rely on JS for basic styling
    const themeToggle = page.locator('.theme-toggle');
    await expect(themeToggle).toBeVisible();

    // Verify the page has proper styling applied via CSS (not dependent on JS)
    // Check that body has proper background color (light theme is default)
    const bodyBgColor = await page.evaluate(() => {
      return window.getComputedStyle(document.body).backgroundColor;
    });
    // Light theme default should have a light background (white or near-white)
    // rgb(255, 255, 255) for white or similar light color
    expect(bodyBgColor).toMatch(/rgb\(255,\s*255,\s*255\)|rgb\(248,\s*249,\s*250\)|white/);

    // Verify text is readable (has dark color on light background)
    const bodyTextColor = await page.evaluate(() => {
      return window.getComputedStyle(document.body).color;
    });
    // Text should be dark for readability
    // The color should not be white or very light
    expect(bodyTextColor).not.toMatch(/rgb\(255,\s*255,\s*255\)|white/);

    // Verify hero section has proper styling
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Verify hero title is visible and styled
    const heroTitle = page.locator('.hero-title');
    await expect(heroTitle).toBeVisible();
    const titleColor = await heroTitle.evaluate((el) => {
      return window.getComputedStyle(el).color;
    });
    // Title should have color applied (primary color)
    expect(titleColor).toBeTruthy();

    // Verify feature cards have proper styling
    const featureCard = page.locator('.feature-card').first();
    await featureCard.scrollIntoViewIfNeeded();
    await expect(featureCard).toBeVisible();

    const cardBgColor = await featureCard.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });
    // Card should have a background color
    expect(cardBgColor).toBeTruthy();
    expect(cardBgColor).not.toBe('transparent');

    // Verify code blocks have proper styling (dark background for code)
    const codeBlock = page.locator('pre').first();
    await codeBlock.scrollIntoViewIfNeeded();
    await expect(codeBlock).toBeVisible();

    const codeBlockBg = await codeBlock.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });
    // Code blocks should have a darker background
    expect(codeBlockBg).toBeTruthy();
    expect(codeBlockBg).not.toMatch(/rgb\(255,\s*255,\s*255\)|white/);

    // Verify buttons are visible and styled
    const primaryButton = page.locator('.btn-primary').first();
    await expect(primaryButton).toBeVisible();

    const buttonBg = await primaryButton.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });
    // Button should have primary color background
    expect(buttonBg).toBeTruthy();
    expect(buttonBg).not.toBe('transparent');
    expect(buttonBg).not.toMatch(/rgb\(255,\s*255,\s*255\)|white/);

    // Verify links are styled and distinguishable
    const link = page.locator('.nav-links a').first();
    await expect(link).toBeVisible();

    // Verify the page has proper CSS custom properties (CSS variables) applied
    // This ensures the theming system works without JS
    const rootStyles = await page.evaluate(() => {
      const root = document.documentElement;
      const style = window.getComputedStyle(root);
      return {
        colorBg: style.getPropertyValue('--color-bg').trim(),
        colorText: style.getPropertyValue('--color-text').trim(),
        colorPrimary: style.getPropertyValue('--color-primary').trim(),
      };
    });

    // CSS custom properties should be defined
    expect(rootStyles.colorBg).toBeTruthy();
    expect(rootStyles.colorText).toBeTruthy();
    expect(rootStyles.colorPrimary).toBeTruthy();
  });

  /**
   * Additional Test: Verify skip link works without JavaScript
   * Important for accessibility
   */
  test('Additional: Skip link is functional without JavaScript', async ({ page }) => {
    await page.goto('/');

    // The skip link should exist
    const skipLink = page.locator('.skip-link');
    await expect(skipLink).toBeAttached();

    // Verify it has correct href
    const href = await skipLink.getAttribute('href');
    expect(href).toBe('#main-content');

    // Verify main content target exists
    const mainContent = page.locator('#main-content');
    await expect(mainContent).toBeVisible();
  });

  /**
   * Additional Test: Verify all sections have proper headings for accessibility
   */
  test('Additional: All sections have proper heading structure', async ({ page }) => {
    await page.goto('/');

    // Check for h1 (should be exactly one)
    const h1Elements = page.locator('h1');
    const h1Count = await h1Elements.count();
    expect(h1Count).toBe(1);

    // Check for h2 section headings
    const h2Elements = page.locator('h2');
    const h2Count = await h2Elements.count();
    expect(h2Count).toBeGreaterThanOrEqual(4); // Features, Getting Started, Project Status, Code Examples, Configuration

    // Verify specific section headings are present
    await expect(page.locator('h2:has-text("Key Features")')).toBeVisible();
    await expect(page.locator('h2:has-text("Getting Started")')).toBeVisible();
    await expect(page.locator('h2:has-text("Project Status")')).toBeVisible();
    await expect(page.locator('h2:has-text("Code Examples")')).toBeVisible();
    await expect(page.locator('h2:has-text("Configuration")')).toBeVisible();
  });

  /**
   * Additional Test: Verify tables are readable without JavaScript
   */
  test('Additional: Configuration table is accessible without JavaScript', async ({ page }) => {
    await page.goto('/');

    // Navigate to configuration section
    const configSection = page.locator('#configuration');
    await configSection.scrollIntoViewIfNeeded();

    // Verify the table has proper structure
    const table = page.locator('[data-testid="config-params-table"]');
    await expect(table).toBeVisible();

    // Verify table header
    const thead = table.locator('thead');
    await expect(thead).toBeVisible();

    // Verify table body has rows
    const tbody = table.locator('tbody');
    await expect(tbody).toBeVisible();
    const rows = tbody.locator('tr');
    const rowCount = await rows.count();
    expect(rowCount).toBeGreaterThanOrEqual(5); // At least 5 configuration parameters

    // Verify content is readable
    const firstCell = rows.first().locator('td').first();
    const cellText = await firstCell.textContent();
    expect(cellText).toBeTruthy();
    expect(cellText.trim().length).toBeGreaterThan(0);
  });

  /**
   * Additional Test: Verify lists are properly rendered without JavaScript
   */
  test('Additional: Feature lists are visible without JavaScript', async ({ page }) => {
    await page.goto('/');

    // Navigate to project status section
    const projectStatusSection = page.locator('#project-status');
    await projectStatusSection.scrollIntoViewIfNeeded();

    // Verify implemented features list
    const implementedList = page.locator('[data-testid="implemented-features-list"]');
    await expect(implementedList).toBeVisible();
    const implementedItems = implementedList.locator('li');
    const implementedCount = await implementedItems.count();
    expect(implementedCount).toBeGreaterThanOrEqual(5);

    // Verify planned features list
    const plannedList = page.locator('[data-testid="planned-features-list"]');
    await expect(plannedList).toBeVisible();
    const plannedItems = plannedList.locator('li');
    const plannedCount = await plannedItems.count();
    expect(plannedCount).toBeGreaterThanOrEqual(2);

    // Verify supported commands lists
    const storageCommands = page.locator('[data-testid="storage-commands-list"]');
    await expect(storageCommands).toBeVisible();
    const storageCount = await storageCommands.locator('li').count();
    expect(storageCount).toBeGreaterThanOrEqual(4);
  });
});
