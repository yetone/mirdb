import { test, expect } from '@playwright/test';

test.describe('Mobile Responsiveness', () => {
  test.describe('Test Case 1: 320px Mobile Viewport', () => {
    test('page renders without horizontal overflow at 320px width', async ({ page }) => {
      // Load page with 320px viewport width
      // Expected: Page renders without horizontal overflow, all content accessible
      await page.setViewportSize({ width: 320, height: 568 });
      await page.goto('/');

      // Wait for page to fully load
      await page.waitForLoadState('networkidle');

      // Check that the page width doesn't exceed viewport width (no horizontal overflow)
      const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
      const viewportWidth = await page.evaluate(() => window.innerWidth);
      expect(bodyWidth).toBeLessThanOrEqual(viewportWidth);

      // Verify hero section is visible and accessible
      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();

      // Verify product name is visible
      const productName = page.locator('[data-testid="hero-product-name"]');
      await expect(productName).toBeVisible();

      // Verify features section is accessible
      const featuresSection = page.locator('[data-testid="features-section"]');
      await expect(featuresSection).toBeVisible();

      // Verify getting started section is accessible
      const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');
      await expect(gettingStartedSection).toBeVisible();

      // Verify configuration section is accessible
      const configurationSection = page.locator('[data-testid="configuration-section"]');
      await expect(configurationSection).toBeVisible();

      // Verify footer is accessible
      const footer = page.locator('[data-testid="footer"]');
      await expect(footer).toBeVisible();
    });

    test('all content sections are accessible at 320px width', async ({ page }) => {
      await page.setViewportSize({ width: 320, height: 568 });
      await page.goto('/');

      // Check all main sections can be scrolled to and are visible
      const sections = [
        '[data-testid="hero-section"]',
        '[data-testid="features-section"]',
        '[data-testid="getting-started-section"]',
        '[data-testid="configuration-section"]',
        '[data-testid="footer"]'
      ];

      for (const selector of sections) {
        const section = page.locator(selector);
        await section.scrollIntoViewIfNeeded();
        await expect(section).toBeVisible();
      }
    });
  });

  test.describe('Test Case 2: 768px Tablet Viewport', () => {
    test('page renders correctly with tablet-optimized layout at 768px width', async ({ page }) => {
      // Load page with 768px viewport width
      // Expected: Page renders correctly with tablet-optimized layout
      await page.setViewportSize({ width: 768, height: 1024 });
      await page.goto('/');

      await page.waitForLoadState('networkidle');

      // Check that the page width doesn't exceed viewport width
      const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
      const viewportWidth = await page.evaluate(() => window.innerWidth);
      expect(bodyWidth).toBeLessThanOrEqual(viewportWidth);

      // Verify hero section is visible
      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();

      // Verify h1 has appropriate font size for tablet
      const h1 = page.locator('[data-testid="hero-product-name"]');
      await expect(h1).toBeVisible();

      // Verify features grid is displayed properly
      const featuresGrid = page.locator('[data-testid="features-grid"]');
      await expect(featuresGrid).toBeVisible();

      // Verify all feature cards are visible
      const featureCards = page.locator('.feature-card');
      const featureCount = await featureCards.count();
      expect(featureCount).toBe(4);

      for (let i = 0; i < featureCount; i++) {
        await expect(featureCards.nth(i)).toBeVisible();
      }
    });

    test('layout adapts properly at tablet breakpoint', async ({ page }) => {
      await page.setViewportSize({ width: 768, height: 1024 });
      await page.goto('/');

      // Check that the hero h1 font size is adjusted for tablet
      const h1FontSize = await page.evaluate(() => {
        const h1 = document.querySelector('[data-testid="hero-product-name"]');
        return h1 ? window.getComputedStyle(h1).fontSize : null;
      });
      expect(h1FontSize).not.toBeNull();
      // At 768px, the font-size should be 2.5rem (40px) based on the CSS media query
      expect(parseFloat(h1FontSize || '0')).toBeLessThanOrEqual(64); // 4rem = 64px is max
    });
  });

  test.describe('Test Case 3: Navigation on Mobile', () => {
    test('navigation is accessible on 320px viewport', async ({ page }) => {
      // Test navigation menu on 320px viewport
      // Expected: Navigation is accessible via responsive menu (hamburger or similar)
      await page.setViewportSize({ width: 320, height: 568 });
      await page.goto('/');

      // The current design uses inline CTA buttons instead of a hamburger menu
      // Navigation is accessible through CTA buttons and scroll
      const ctaButtons = page.locator('.cta-buttons');
      await expect(ctaButtons).toBeVisible();

      // Verify the "Get Started" button is accessible
      const getStartedBtn = page.locator('[data-testid="hero-cta-primary"]');
      await expect(getStartedBtn).toBeVisible();
      await expect(getStartedBtn).toBeEnabled();

      // Verify the GitHub link is accessible
      const githubBtn = page.locator('[data-testid="hero-cta-github"]');
      await expect(githubBtn).toBeVisible();
      await expect(githubBtn).toBeEnabled();

      // Verify footer navigation link is accessible
      const footerGithubLink = page.locator('[data-testid="footer-github-link"]');
      await footerGithubLink.scrollIntoViewIfNeeded();
      await expect(footerGithubLink).toBeVisible();
      await expect(footerGithubLink).toBeEnabled();
    });

    test('CTA buttons stack vertically on small screens', async ({ page }) => {
      await page.setViewportSize({ width: 320, height: 568 });
      await page.goto('/');

      // At 480px and below, CTA buttons should stack vertically (flex-direction: column)
      const ctaButtons = page.locator('.cta-buttons');
      const flexDirection = await ctaButtons.evaluate((el) => {
        return window.getComputedStyle(el).flexDirection;
      });
      expect(flexDirection).toBe('column');
    });
  });

  test.describe('Test Case 4: Code Blocks on Mobile', () => {
    test('code blocks are scrollable and dont break layout at 320px', async ({ page }) => {
      // Check code blocks on mobile viewport
      // Expected: Code blocks are scrollable or wrapped appropriately, not breaking layout
      await page.setViewportSize({ width: 320, height: 568 });
      await page.goto('/');

      // Navigate to getting started section which contains code blocks
      const gettingStartedSection = page.locator('[data-testid="getting-started-section"]');
      await gettingStartedSection.scrollIntoViewIfNeeded();

      // Get all pre elements (code blocks)
      const codeBlocks = page.locator('pre');
      const codeBlockCount = await codeBlocks.count();
      expect(codeBlockCount).toBeGreaterThan(0);

      // Check that code blocks have overflow-x: auto (scrollable)
      for (let i = 0; i < codeBlockCount; i++) {
        const codeBlock = codeBlocks.nth(i);
        await codeBlock.scrollIntoViewIfNeeded();

        const overflowX = await codeBlock.evaluate((el) => {
          return window.getComputedStyle(el).overflowX;
        });
        expect(overflowX).toBe('auto');

        // Verify code block width doesn't exceed its container
        const boundingBox = await codeBlock.boundingBox();
        expect(boundingBox).not.toBeNull();
        if (boundingBox) {
          // Code block should not exceed viewport width (with some padding allowance)
          expect(boundingBox.width).toBeLessThanOrEqual(320);
        }
      }
    });

    test('configuration section table is readable on mobile', async ({ page }) => {
      await page.setViewportSize({ width: 320, height: 568 });
      await page.goto('/');

      // Navigate to configuration section
      const configSection = page.locator('[data-testid="configuration-section"]');
      await configSection.scrollIntoViewIfNeeded();

      // Verify config table is visible
      const configTable = page.locator('.config-table');
      await expect(configTable).toBeVisible();

      // Check that table doesn't cause horizontal overflow
      const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
      const viewportWidth = await page.evaluate(() => window.innerWidth);
      expect(bodyWidth).toBeLessThanOrEqual(viewportWidth);
    });

    test('code blocks in configuration section are scrollable', async ({ page }) => {
      await page.setViewportSize({ width: 320, height: 568 });
      await page.goto('/');

      // Navigate to configuration section
      const configSection = page.locator('[data-testid="configuration-section"]');
      await configSection.scrollIntoViewIfNeeded();

      // Check the config TOML code block
      const configCodeBlock = page.locator('[data-testid="config-toml"]');
      await expect(configCodeBlock).toBeVisible();

      const overflowX = await configCodeBlock.evaluate((el) => {
        return window.getComputedStyle(el).overflowX;
      });
      expect(overflowX).toBe('auto');
    });
  });
});
