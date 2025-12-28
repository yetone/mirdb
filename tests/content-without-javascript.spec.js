// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Scenario: Content Without JavaScript (Progressive Enhancement)
 *
 * These tests verify that core content is accessible without JavaScript.
 * The page should follow progressive enhancement principles where JS
 * enhances but isn't required for basic functionality.
 */

test.describe('Content Without JavaScript - Progressive Enhancement', () => {
  // Configure this test suite to run with JavaScript disabled
  test.use({ javaScriptEnabled: false });

  test.describe('Test Case 1: All text content is visible and readable', () => {
    test('hero section content is visible without JavaScript', async ({ page }) => {
      await page.goto('/');

      // Product name is visible
      const productName = page.locator('.product-name');
      await expect(productName).toBeVisible();
      await expect(productName).toHaveText('MirDB');

      // Tagline is visible
      const tagline = page.locator('.tagline');
      await expect(tagline).toBeVisible();
      await expect(tagline).toContainText('persistent key-value store');

      // Value proposition is visible
      const valueProp = page.locator('.value-proposition');
      await expect(valueProp).toBeVisible();
      await expect(valueProp).toContainText('Rust');
    });

    test('value propositions section content is visible', async ({ page }) => {
      await page.goto('/');

      // Value propositions section is visible
      const section = page.locator('[data-testid="value-propositions-section"]');
      await expect(section).toBeVisible();

      // All three value prop cards are visible
      const persistentStorage = page.locator('[data-testid="value-prop-persistent-storage"]');
      await expect(persistentStorage).toBeVisible();
      await expect(persistentStorage.locator('.value-prop-title')).toHaveText('Persistent Storage');

      const memcachedCompatible = page.locator('[data-testid="value-prop-memcached-compatible"]');
      await expect(memcachedCompatible).toBeVisible();
      await expect(memcachedCompatible.locator('.value-prop-title')).toHaveText('Memcached Compatible');

      const rust = page.locator('[data-testid="value-prop-rust"]');
      await expect(rust).toBeVisible();
      await expect(rust.locator('.value-prop-title')).toHaveText('Written in Rust');
    });

    test('features section content is visible', async ({ page }) => {
      await page.goto('/');

      // Features section is visible
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeVisible();

      // Section heading is visible
      const heading = featuresSection.locator('h2');
      await expect(heading).toBeVisible();
      await expect(heading).toHaveText('Key Features');

      // All feature cards are visible
      const featureCards = featuresSection.locator('.feature-card');
      await expect(featureCards).toHaveCount(4);

      // Verify specific feature content
      const lsmTree = featuresSection.locator('[data-feature="lsm-tree"]');
      await expect(lsmTree).toBeVisible();
      await expect(lsmTree.locator('h3')).toHaveText('LSM Tree Architecture');

      const asyncIo = featuresSection.locator('[data-feature="async-io"]');
      await expect(asyncIo).toBeVisible();

      const configurable = featuresSection.locator('[data-feature="configurable"]');
      await expect(configurable).toBeVisible();

      const compaction = featuresSection.locator('[data-feature="compaction"]');
      await expect(compaction).toBeVisible();
    });

    test('quick start section content is visible', async ({ page }) => {
      await page.goto('/');

      // Quick start section is visible
      const quickStartSection = page.locator('#quick-start');
      await expect(quickStartSection).toBeVisible();

      // Section heading is visible
      const heading = quickStartSection.locator('h2');
      await expect(heading).toBeVisible();
      await expect(heading).toHaveText('Quick Start');

      // Installation step is visible
      const installSection = page.locator('#install-section');
      await expect(installSection).toBeVisible();

      // All quick start steps are visible
      const steps = quickStartSection.locator('.quick-start-step');
      await expect(steps).toHaveCount(5);
    });

    test('commands section content is visible', async ({ page }) => {
      await page.goto('/');

      // Commands section is visible
      const commandsSection = page.locator('#commands');
      await expect(commandsSection).toBeVisible();

      // Section heading is visible
      const heading = commandsSection.locator('h2');
      await expect(heading).toBeVisible();
      await expect(heading).toHaveText('Supported Commands');

      // Command categories are visible
      const storageCommands = page.locator('[data-testid="commands-storage"]');
      await expect(storageCommands).toBeVisible();

      const retrievalCommands = page.locator('[data-testid="commands-retrieval"]');
      await expect(retrievalCommands).toBeVisible();

      const deletionCommands = page.locator('[data-testid="commands-deletion"]');
      await expect(deletionCommands).toBeVisible();

      const mirdbCommands = page.locator('[data-testid="commands-mirdb"]');
      await expect(mirdbCommands).toBeVisible();
    });

    test('configuration section content is visible', async ({ page }) => {
      await page.goto('/');

      // Configuration section is visible
      const configSection = page.locator('#configuration');
      await expect(configSection).toBeVisible();

      // Section heading is visible
      const heading = configSection.locator('h2');
      await expect(heading).toBeVisible();
      await expect(heading).toHaveText('Configuration');

      // Config parameters table is visible
      const paramsTable = configSection.locator('.config-params-table');
      await expect(paramsTable).toBeVisible();
    });

    test('footer content is visible', async ({ page }) => {
      await page.goto('/');

      // Footer is visible
      const footer = page.locator('[data-testid="footer"]');
      await expect(footer).toBeVisible();

      // Version info is visible
      const version = page.locator('[data-testid="footer-version"]');
      await expect(version).toBeVisible();

      // License info is visible
      const license = page.locator('[data-testid="footer-license"]');
      await expect(license).toBeVisible();
      await expect(license).toContainText('MIT');
    });
  });

  test.describe('Test Case 2: Navigation links are functional without JavaScript', () => {
    test('desktop navigation links are visible and functional', async ({ page }) => {
      // Set desktop viewport
      await page.setViewportSize({ width: 1200, height: 800 });
      await page.goto('/');

      // Navigation container is visible
      const navContainer = page.locator('.nav-container');
      await expect(navContainer).toBeVisible();

      // Logo link is visible and functional
      const logoLink = page.locator('.logo-link');
      await expect(logoLink).toBeVisible();
      await expect(logoLink).toHaveAttribute('href', '#');

      // Navigation links are visible
      const navLinks = page.locator('.nav-links');
      await expect(navLinks).toBeVisible();

      // Features link is visible and has correct href
      const featuresLink = navLinks.locator('a[href="#features"]');
      await expect(featuresLink).toBeVisible();
      await expect(featuresLink).toHaveText('Features');

      // Docs link is visible and has correct href
      const docsLink = navLinks.locator('a[href="#quick-start"]');
      await expect(docsLink).toBeVisible();
      await expect(docsLink).toHaveText('Docs');

      // GitHub link is visible and has correct href
      const githubLink = navLinks.locator('a[href="https://github.com/yetone/mirdb"]');
      await expect(githubLink).toBeVisible();
      await expect(githubLink).toHaveText('GitHub');
    });

    test('internal navigation links work without JavaScript', async ({ page }) => {
      await page.setViewportSize({ width: 1200, height: 800 });
      await page.goto('/');

      // Click Features link and verify scroll target exists
      const featuresLink = page.locator('.nav-links a[href="#features"]');
      await featuresLink.click();

      // URL should contain the anchor
      await expect(page).toHaveURL(/#features$/);

      // Features section should exist
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeAttached();
    });

    test('CTA buttons are functional without JavaScript', async ({ page }) => {
      await page.goto('/');

      // Get Started button is visible and has correct href
      const getStartedBtn = page.locator('#get-started-btn');
      await expect(getStartedBtn).toBeVisible();
      await expect(getStartedBtn).toHaveAttribute('href', '#quick-start');

      // GitHub button is visible and has correct href
      const githubBtn = page.locator('#github-btn');
      await expect(githubBtn).toBeVisible();
      await expect(githubBtn).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
    });

    test('footer links are functional without JavaScript', async ({ page }) => {
      await page.goto('/');

      // GitHub Repository link in footer
      const footerGithub = page.locator('[data-testid="footer-github-link"]');
      await expect(footerGithub).toBeVisible();
      await expect(footerGithub).toHaveAttribute('href', 'https://github.com/yetone/mirdb');

      // Report Issues link
      const issuesLink = page.locator('footer a[href="https://github.com/yetone/mirdb/issues"]');
      await expect(issuesLink).toBeVisible();
    });

    test('mobile navigation is accessible without JavaScript via noscript styles', async ({ page }) => {
      // Set mobile viewport
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/');

      // Navigation links should be accessible on mobile without JS
      // With our CSS fix using :has() pseudo-class, nav should be visible
      // when JS is disabled and no menu-open class exists
      const navLinks = page.locator('.nav-links');

      // The nav links should be visible on mobile without JS
      // (we'll add CSS to ensure this works)
      await expect(navLinks).toBeVisible();

      // All navigation links should be functional
      const featuresLink = navLinks.locator('a[href="#features"]');
      await expect(featuresLink).toBeVisible();

      const docsLink = navLinks.locator('a[href="#quick-start"]');
      await expect(docsLink).toBeVisible();

      const githubLink = navLinks.locator('a[href="https://github.com/yetone/mirdb"]');
      await expect(githubLink).toBeVisible();
    });
  });

  test.describe('Test Case 3: Code blocks display content without JS syntax highlighting', () => {
    test('installation code block content is visible', async ({ page }) => {
      await page.goto('/');

      // Install command code block
      const installCode = page.locator('#install-command');
      await expect(installCode).toBeVisible();

      const codeText = await installCode.textContent();
      expect(codeText).toContain('cargo install mirdb');
    });

    test('build from source code block content is visible', async ({ page }) => {
      await page.goto('/');

      // Build commands code block
      const buildCode = page.locator('#build-commands');
      await expect(buildCode).toBeVisible();

      const codeText = await buildCode.textContent();
      expect(codeText).toContain('git clone');
      expect(codeText).toContain('cargo build --release');
    });

    test('run command code block content is visible', async ({ page }) => {
      await page.goto('/');

      // Run command code block
      const runCode = page.locator('#run-command');
      await expect(runCode).toBeVisible();

      const codeText = await runCode.textContent();
      expect(codeText).toContain('./target/release/mirdb');
    });

    test('connect example code block content is visible', async ({ page }) => {
      await page.goto('/');

      // Connect example code block
      const connectCode = page.locator('#connect-example');
      await expect(connectCode).toBeVisible();

      const codeText = await connectCode.textContent();
      expect(codeText).toContain('telnet localhost 12333');
    });

    test('usage example code block content is visible', async ({ page }) => {
      await page.goto('/');

      // Usage example code block
      const usageCode = page.locator('#usage-example');
      await expect(usageCode).toBeVisible();

      const codeText = await usageCode.textContent();
      expect(codeText).toContain('set hello');
      expect(codeText).toContain('get hello');
      expect(codeText).toContain('STORED');
    });

    test('command reference code blocks are visible', async ({ page }) => {
      await page.goto('/');

      // SET command example
      const setCommand = page.locator('[data-testid="command-set"] .command-example code');
      await expect(setCommand).toBeVisible();
      const setCode = await setCommand.textContent();
      expect(setCode).toContain('set');

      // GET command example
      const getCommand = page.locator('[data-testid="command-get"] .command-example code');
      await expect(getCommand).toBeVisible();
      const getCode = await getCommand.textContent();
      expect(getCode).toContain('get');

      // DELETE command example
      const deleteCommand = page.locator('[data-testid="command-delete"] .command-example code');
      await expect(deleteCommand).toBeVisible();
      const deleteCode = await deleteCommand.textContent();
      expect(deleteCode).toContain('delete');
    });

    test('configuration code block is visible', async ({ page }) => {
      await page.goto('/');

      // Config example code block (TOML)
      const configCode = page.locator('#configuration .code-block code.language-toml');
      await expect(configCode).toBeVisible();

      const codeText = await configCode.textContent();
      expect(codeText).toContain('addr = "0.0.0.0:12333"');
      expect(codeText).toContain('max_level = 7');
      expect(codeText).toContain('work_dir = "/tmp/mirdb"');
    });

    test('code blocks have proper styling without JS', async ({ page }) => {
      await page.goto('/');

      // Check that code blocks have styling applied
      const codeBlock = page.locator('.code-block').first();
      await expect(codeBlock).toBeVisible();

      // Background color should be set
      const bgColor = await codeBlock.evaluate((el) =>
        window.getComputedStyle(el).backgroundColor
      );
      expect(bgColor).not.toBe('rgba(0, 0, 0, 0)');

      // Code should use monospace font
      const codeElement = codeBlock.locator('code');
      const fontFamily = await codeElement.evaluate((el) =>
        window.getComputedStyle(el).fontFamily
      );
      expect(fontFamily.toLowerCase()).toMatch(/mono|consolas|monaco|courier/);
    });

    test('code block content is readable (has sufficient contrast)', async ({ page }) => {
      await page.goto('/');

      const codeBlock = page.locator('.code-block').first();
      const codeElement = codeBlock.locator('code');

      // Get the text color
      const textColor = await codeElement.evaluate((el) =>
        window.getComputedStyle(el).color
      );

      // Text should not be transparent or invisible
      expect(textColor).not.toBe('rgba(0, 0, 0, 0)');
      expect(textColor).not.toBe('transparent');
    });
  });

  test.describe('Additional progressive enhancement checks', () => {
    test('images have alt text and are visible', async ({ page }) => {
      await page.goto('/');

      // Hero logo
      const heroLogo = page.locator('.hero-logo');
      await expect(heroLogo).toBeVisible();
      await expect(heroLogo).toHaveAttribute('alt');

      // Nav logo
      const navLogo = page.locator('.nav-logo');
      await expect(navLogo).toBeVisible();
      await expect(navLogo).toHaveAttribute('alt');
    });

    test('skip link is present for accessibility', async ({ page }) => {
      await page.goto('/');

      // Skip link should exist
      const skipLink = page.locator('.skip-link');
      await expect(skipLink).toBeAttached();
      await expect(skipLink).toHaveAttribute('href', '#main');
    });

    test('page has proper semantic structure', async ({ page }) => {
      await page.goto('/');

      // Main landmark
      const main = page.locator('main');
      await expect(main).toBeVisible();

      // Header
      const header = page.locator('header');
      await expect(header).toBeVisible();

      // Footer
      const footer = page.locator('footer');
      await expect(footer).toBeVisible();

      // Nav
      const nav = page.locator('nav');
      await expect(nav).toBeVisible();

      // Sections
      const sections = page.locator('main > section');
      const count = await sections.count();
      expect(count).toBeGreaterThan(0);
    });

    test('page title is set', async ({ page }) => {
      await page.goto('/');

      const title = await page.title();
      expect(title).toContain('MirDB');
    });
  });
});
