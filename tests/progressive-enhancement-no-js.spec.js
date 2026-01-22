// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Progressive Enhancement - No JavaScript Tests
 *
 * This test suite verifies that core content is accessible without JavaScript.
 * The page should work following progressive enhancement principles:
 * - All text content, images, and links are visible
 * - Navigation works without JavaScript
 * - Installation commands are readable
 */

test.describe('Progressive Enhancement - No JavaScript', () => {
  /**
   * Test Case 1: Load page with JavaScript disabled
   * Expected: All text content, images, and links are visible
   */
  test.describe('Test Case 1: Content visibility without JavaScript', () => {
    test('should display all essential text content without JS', async ({ browser }) => {
      // Create a context with JavaScript disabled
      const context = await browser.newContext({ javaScriptEnabled: false });
      const page = await context.newPage();

      await page.goto('/');

      // Hero section content should be visible
      const heroTitle = page.locator('#hero-title');
      await expect(heroTitle).toBeVisible();
      await expect(heroTitle).toHaveText('MirDB');

      // Tagline and subtitle should be visible
      const tagline = page.locator('.hero-tagline');
      await expect(tagline).toBeVisible();
      await expect(tagline).toContainText('Memcached Protocol');

      const subtitle = page.locator('.hero-subtitle');
      await expect(subtitle).toBeVisible();
      await expect(subtitle).toContainText('Rust');

      await context.close();
    });

    test('should display all images without JS', async ({ browser }) => {
      const context = await browser.newContext({ javaScriptEnabled: false });
      const page = await context.newPage();

      await page.goto('/');

      // Hero logo should be visible
      const heroLogo = page.locator('.hero-logo');
      await expect(heroLogo).toBeVisible();
      await expect(heroLogo).toHaveAttribute('alt', /MirDB Logo/);

      // Navigation logo should be visible
      const navLogo = page.locator('.nav-logo');
      await expect(navLogo).toBeVisible();

      await context.close();
    });

    test('should display all links without JS', async ({ browser }) => {
      const context = await browser.newContext({ javaScriptEnabled: false });
      const page = await context.newPage();

      await page.goto('/');

      // Navigation links should be visible and clickable
      const navLinks = page.locator('.nav-links a');
      const linkCount = await navLinks.count();
      expect(linkCount).toBeGreaterThan(0);

      // Verify all nav links are visible
      for (let i = 0; i < linkCount; i++) {
        await expect(navLinks.nth(i)).toBeVisible();
      }

      // CTA buttons should be visible
      const getStartedBtn = page.locator('.hero-ctas .btn-primary');
      await expect(getStartedBtn).toBeVisible();

      const githubBtn = page.locator('.hero-ctas .btn-secondary');
      await expect(githubBtn).toBeVisible();

      await context.close();
    });

    test('should display feature cards without JS', async ({ browser }) => {
      const context = await browser.newContext({ javaScriptEnabled: false });
      const page = await context.newPage();

      await page.goto('/');

      // Features section should be visible
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeVisible();

      // Feature cards should be visible
      const featureCards = page.locator('.feature-card');
      const cardCount = await featureCards.count();
      expect(cardCount).toBeGreaterThan(0);

      // Each feature card should have visible title and description
      for (let i = 0; i < cardCount; i++) {
        const card = featureCards.nth(i);
        const title = card.locator('.feature-title');
        const description = card.locator('.feature-description');
        await expect(title).toBeVisible();
        await expect(description).toBeVisible();
      }

      await context.close();
    });

    test('should display footer content without JS', async ({ browser }) => {
      const context = await browser.newContext({ javaScriptEnabled: false });
      const page = await context.newPage();

      await page.goto('/');

      // Footer should be visible
      const footer = page.locator('footer');
      await expect(footer).toBeVisible();

      // Footer links should be visible
      const footerLinks = footer.locator('a');
      const linkCount = await footerLinks.count();
      expect(linkCount).toBeGreaterThan(0);

      await context.close();
    });
  });

  /**
   * Test Case 2: Check navigation without JS
   * Expected: GitHub links and section navigation work without JavaScript
   */
  test.describe('Test Case 2: Navigation without JavaScript', () => {
    test('should have working GitHub links without JS', async ({ browser }) => {
      const context = await browser.newContext({ javaScriptEnabled: false });
      const page = await context.newPage();

      await page.goto('/');

      // Find GitHub link in navigation
      const githubNavLink = page.locator('.nav-links a[href*="github.com"]');
      await expect(githubNavLink).toBeVisible();
      await expect(githubNavLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
      await expect(githubNavLink).toHaveAttribute('target', '_blank');
      await expect(githubNavLink).toHaveAttribute('rel', /noopener/);

      // Find GitHub link in hero CTAs
      const githubCTALink = page.locator('.hero-ctas a[href*="github.com"]');
      await expect(githubCTALink).toBeVisible();
      await expect(githubCTALink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');

      // Find GitHub link in footer
      const githubFooterLink = page.locator('footer a[href*="github.com/yetone/mirdb"]').first();
      await expect(githubFooterLink).toBeVisible();

      await context.close();
    });

    test('should have working section navigation without JS', async ({ browser }) => {
      const context = await browser.newContext({ javaScriptEnabled: false });
      const page = await context.newPage();

      await page.goto('/');

      // Features link should point to features section
      const featuresLink = page.locator('.nav-links a[href="#features"]');
      await expect(featuresLink).toBeVisible();
      await expect(featuresLink).toHaveAttribute('href', '#features');

      // Quick Start link should point to quick-start section
      const quickStartLink = page.locator('.nav-links a[href="#quick-start"]');
      await expect(quickStartLink).toBeVisible();
      await expect(quickStartLink).toHaveAttribute('href', '#quick-start');

      // Demo link should point to demo section
      const demoLink = page.locator('.nav-links a[href="#demo"]');
      await expect(demoLink).toBeVisible();
      await expect(demoLink).toHaveAttribute('href', '#demo');

      await context.close();
    });

    test('should navigate to sections using anchor links without JS', async ({ browser }) => {
      const context = await browser.newContext({ javaScriptEnabled: false });
      const page = await context.newPage();

      // Navigate directly to quick-start section
      await page.goto('/#quick-start');

      // The quick-start section should exist
      const quickStartSection = page.locator('#quick-start');
      await expect(quickStartSection).toBeVisible();

      // Navigate to features section
      await page.goto('/#features');
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeVisible();

      await context.close();
    });

    test('should have working Get Started button linking to quick-start section without JS', async ({ browser }) => {
      const context = await browser.newContext({ javaScriptEnabled: false });
      const page = await context.newPage();

      await page.goto('/');

      // Get Started button should link to quick-start section
      const getStartedBtn = page.locator('.hero-ctas a[href="#quick-start"]');
      await expect(getStartedBtn).toBeVisible();
      await expect(getStartedBtn).toHaveAttribute('href', '#quick-start');

      await context.close();
    });

    test('should have skip link for accessibility without JS', async ({ browser }) => {
      const context = await browser.newContext({ javaScriptEnabled: false });
      const page = await context.newPage();

      await page.goto('/');

      // Skip link should exist
      const skipLink = page.locator('.skip-link');
      await expect(skipLink).toHaveAttribute('href', '#main-content');

      // Main content should have the target id
      const mainContent = page.locator('#main-content');
      await expect(mainContent).toBeVisible();

      await context.close();
    });
  });

  /**
   * Test Case 3: Check code display without JS
   * Expected: Installation commands are readable even if copy button doesn't work
   */
  test.describe('Test Case 3: Code display without JavaScript', () => {
    test('should display installation commands without JS', async ({ browser }) => {
      const context = await browser.newContext({ javaScriptEnabled: false });
      const page = await context.newPage();

      await page.goto('/');

      // Quick start section should be visible
      const quickStartSection = page.locator('#quick-start');
      await expect(quickStartSection).toBeVisible();

      // Installation code should be visible and readable
      const installCode = page.locator('#install-code');
      await expect(installCode).toBeVisible();

      // Verify the installation commands are readable
      const codeText = await installCode.textContent();
      expect(codeText).toContain('git clone https://github.com/yetone/mirdb');
      expect(codeText).toContain('cargo build --release');
      expect(codeText).toContain('mirdb-server');

      await context.close();
    });

    test('should display operation examples without JS', async ({ browser }) => {
      const context = await browser.newContext({ javaScriptEnabled: false });
      const page = await context.newPage();

      await page.goto('/');

      // Operations code should be visible and readable
      const operationsCode = page.locator('#operations-code');
      await expect(operationsCode).toBeVisible();

      // Verify the operation examples are readable
      const codeText = await operationsCode.textContent();
      expect(codeText).toContain('set mykey');
      expect(codeText).toContain('get mykey');
      expect(codeText).toContain('delete mykey');

      await context.close();
    });

    test('should display code blocks with proper formatting without JS', async ({ browser }) => {
      const context = await browser.newContext({ javaScriptEnabled: false });
      const page = await context.newPage();

      await page.goto('/');

      // Code blocks should have proper structure
      const codeBlocks = page.locator('.code-block');
      const blockCount = await codeBlocks.count();
      expect(blockCount).toBeGreaterThan(0);

      // Each code block should have a header and content
      for (let i = 0; i < blockCount; i++) {
        const block = codeBlocks.nth(i);
        const header = block.locator('.code-header');
        const title = block.locator('.code-title');
        const code = block.locator('pre code');

        await expect(header).toBeVisible();
        await expect(title).toBeVisible();
        await expect(code).toBeVisible();
      }

      await context.close();
    });

    test('should have copy buttons present (even if non-functional without JS)', async ({ browser }) => {
      const context = await browser.newContext({ javaScriptEnabled: false });
      const page = await context.newPage();

      await page.goto('/');

      // Copy buttons should be present and visible
      const copyButtons = page.locator('.copy-btn');
      const buttonCount = await copyButtons.count();
      expect(buttonCount).toBeGreaterThan(0);

      // Buttons should be visible
      for (let i = 0; i < buttonCount; i++) {
        await expect(copyButtons.nth(i)).toBeVisible();
      }

      await context.close();
    });

    test('should display code with readable text without JS', async ({ browser }) => {
      const context = await browser.newContext({ javaScriptEnabled: false });
      const page = await context.newPage();

      await page.goto('/');

      // Get the install code element
      const installCode = page.locator('#install-code');

      // Check that the code is not hidden or collapsed
      await expect(installCode).toBeVisible();

      // Verify the code block is not empty
      const textContent = await installCode.textContent();
      expect(textContent.trim().length).toBeGreaterThan(0);

      // Verify the pre element is properly styled (not display:none or visibility:hidden)
      const pre = installCode.locator('..');
      await expect(pre).toBeVisible();

      await context.close();
    });

    test('should display demo GIF section without JS', async ({ browser }) => {
      const context = await browser.newContext({ javaScriptEnabled: false });
      const page = await context.newPage();

      await page.goto('/');

      // Demo section should be visible
      const demoSection = page.locator('#demo');
      await expect(demoSection).toBeVisible();

      // Demo GIF should be visible
      const demoGif = page.locator('.demo-gif');
      await expect(demoGif).toBeVisible();
      await expect(demoGif).toHaveAttribute('alt', /MirDB/i);

      await context.close();
    });
  });
});
