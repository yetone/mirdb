// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Firefox Browser Compatibility Tests
 *
 * These tests verify that the MirDB homepage renders correctly and functions
 * properly in Mozilla Firefox browser (NFR-3: last 2 versions support).
 *
 * Test coverage:
 * - Page rendering without console errors
 * - Visual element rendering
 * - Interactive element functionality
 * - Navigation and links
 * - CSS layout and styling
 */

test.describe('Firefox Browser Compatibility', () => {
  test.describe('Page Rendering', () => {
    test('should load page without console errors', async ({ page }) => {
      const consoleErrors = [];

      // Collect console errors
      page.on('console', (msg) => {
        if (msg.type() === 'error') {
          consoleErrors.push(msg.text());
        }
      });

      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Filter out known non-critical errors (like favicon 404)
      const criticalErrors = consoleErrors.filter(error =>
        !error.includes('favicon') &&
        !error.includes('404') &&
        !error.includes('net::ERR')
      );

      expect(criticalErrors).toHaveLength(0);
    });

    test('should render page with correct title', async ({ page }) => {
      await page.goto('/');
      await expect(page).toHaveTitle(/MirDB/);
    });

    test('should render all major sections', async ({ page }) => {
      await page.goto('/');

      // Hero section
      const heroSection = page.locator('#hero');
      await expect(heroSection).toBeVisible();

      // Value propositions
      const valuePropsSection = page.locator('#value-propositions');
      await expect(valuePropsSection).toBeVisible();

      // Features section
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeVisible();

      // Quick start section
      const quickStartSection = page.locator('#quick-start');
      await expect(quickStartSection).toBeVisible();

      // Commands section
      const commandsSection = page.locator('#commands');
      await expect(commandsSection).toBeVisible();

      // Configuration section
      const configSection = page.locator('#configuration');
      await expect(configSection).toBeVisible();

      // Footer
      const footer = page.locator('footer');
      await expect(footer).toBeVisible();
    });

    test('should render hero content correctly', async ({ page }) => {
      await page.goto('/');

      // Logo should be visible
      const heroLogo = page.locator('#hero-logo');
      await expect(heroLogo).toBeVisible();

      // Product name
      const productName = page.locator('#product-name');
      await expect(productName).toBeVisible();
      await expect(productName).toHaveText('MirDB');

      // Tagline
      const tagline = page.locator('#tagline');
      await expect(tagline).toBeVisible();
      await expect(tagline).toContainText('persistent key-value store');

      // CTA buttons
      const getStartedBtn = page.locator('#get-started-btn');
      await expect(getStartedBtn).toBeVisible();

      const githubBtn = page.locator('#github-btn');
      await expect(githubBtn).toBeVisible();
    });

    test('should render value proposition cards', async ({ page }) => {
      await page.goto('/');

      // Check all three value props are visible
      const persistentStorage = page.locator('[data-testid="value-prop-persistent-storage"]');
      await expect(persistentStorage).toBeVisible();

      const memcachedCompatible = page.locator('[data-testid="value-prop-memcached-compatible"]');
      await expect(memcachedCompatible).toBeVisible();

      const rustCard = page.locator('[data-testid="value-prop-rust"]');
      await expect(rustCard).toBeVisible();
    });

    test('should render feature cards correctly', async ({ page }) => {
      await page.goto('/');

      const featureCards = page.locator('.feature-card');
      await expect(featureCards).toHaveCount(4);

      // Check specific features
      const lsmTree = page.locator('[data-feature="lsm-tree"]');
      await expect(lsmTree).toBeVisible();

      const asyncIo = page.locator('[data-feature="async-io"]');
      await expect(asyncIo).toBeVisible();

      const configurable = page.locator('[data-feature="configurable"]');
      await expect(configurable).toBeVisible();

      const compaction = page.locator('[data-feature="compaction"]');
      await expect(compaction).toBeVisible();
    });

    test('should render code blocks with proper formatting', async ({ page }) => {
      await page.goto('/');

      // Quick start code blocks
      const installCommand = page.locator('#install-command');
      await expect(installCommand).toBeVisible();

      const buildCommands = page.locator('#build-commands');
      await expect(buildCommands).toBeVisible();

      // Verify code blocks have pre/code structure
      const codeBlocks = page.locator('.code-block pre code');
      const count = await codeBlocks.count();
      expect(count).toBeGreaterThan(0);
    });
  });

  test.describe('Interactive Elements', () => {
    test('should have clickable navigation links', async ({ page }) => {
      await page.goto('/');

      // Features link
      const featuresLink = page.locator('.nav-links a[href="#features"]');
      await expect(featuresLink).toBeVisible();
      await expect(featuresLink).toBeEnabled();

      // Docs link
      const docsLink = page.locator('.nav-links a[href="#quick-start"]');
      await expect(docsLink).toBeVisible();
      await expect(docsLink).toBeEnabled();

      // GitHub link
      const githubLink = page.locator('.nav-links a[href*="github.com"]');
      await expect(githubLink).toBeVisible();
      await expect(githubLink).toBeEnabled();
    });

    test('should navigate to sections on click', async ({ page }) => {
      await page.goto('/');

      // Click features link
      const featuresLink = page.locator('.nav-links a[href="#features"]');
      await featuresLink.click();

      // Wait for navigation
      await page.waitForTimeout(500);

      // Features section should be in viewport or URL should have hash
      const url = page.url();
      expect(url).toContain('#features');
    });

    test('should have working CTA buttons', async ({ page }) => {
      await page.goto('/');

      // Get Started button
      const getStartedBtn = page.locator('#get-started-btn');
      await expect(getStartedBtn).toBeVisible();
      await expect(getStartedBtn).toHaveAttribute('href', '#quick-start');

      // GitHub button
      const githubBtn = page.locator('#github-btn');
      await expect(githubBtn).toBeVisible();
      await expect(githubBtn).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
      await expect(githubBtn).toHaveAttribute('target', '_blank');
    });

    test('should have working footer links', async ({ page }) => {
      await page.goto('/');

      // GitHub repository link in footer
      const footerGithub = page.locator('[data-testid="footer-github-link"]');
      await expect(footerGithub).toBeVisible();
      await expect(footerGithub).toHaveAttribute('href', 'https://github.com/yetone/mirdb');

      // Issues link
      const issuesLink = page.locator('footer a[href*="issues"]');
      await expect(issuesLink).toBeVisible();
    });

    test('should have accessible skip link', async ({ page }) => {
      await page.goto('/');

      // Skip link should exist
      const skipLink = page.locator('.skip-link');
      await expect(skipLink).toHaveAttribute('href', '#main');
    });

    test('mobile menu toggle should be functional', async ({ page }) => {
      // Set mobile viewport
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/');

      const menuToggle = page.locator('[data-testid="mobile-menu-toggle"]');

      // Menu toggle should be visible on mobile
      await expect(menuToggle).toBeVisible();

      // Initially aria-expanded should be false
      await expect(menuToggle).toHaveAttribute('aria-expanded', 'false');

      // Click to open menu
      await menuToggle.click();

      // After click, aria-expanded should be true
      await expect(menuToggle).toHaveAttribute('aria-expanded', 'true');

      // Click again to close
      await menuToggle.click();
      await expect(menuToggle).toHaveAttribute('aria-expanded', 'false');
    });
  });

  test.describe('CSS Layout and Styling', () => {
    test('should have proper grid layout for value props', async ({ page }) => {
      await page.goto('/');

      const valuePropsGrid = page.locator('[data-testid="value-props-grid"]');
      await expect(valuePropsGrid).toBeVisible();

      // Check that grid has proper display
      const display = await valuePropsGrid.evaluate(el =>
        window.getComputedStyle(el).display
      );
      expect(['grid', 'flex']).toContain(display);
    });

    test('should have proper grid layout for features', async ({ page }) => {
      await page.goto('/');

      const featuresGrid = page.locator('.features-grid');
      await expect(featuresGrid).toBeVisible();

      const display = await featuresGrid.evaluate(el =>
        window.getComputedStyle(el).display
      );
      expect(['grid', 'flex']).toContain(display);
    });

    test('should have visible navigation', async ({ page }) => {
      await page.goto('/');

      const nav = page.locator('header nav');
      await expect(nav).toBeVisible();

      // Logo should be visible
      const logo = page.locator('.nav-logo');
      await expect(logo).toBeVisible();
    });

    test('should render SVG icons correctly', async ({ page }) => {
      await page.goto('/');

      // Value prop icons (SVG)
      const svgIcons = page.locator('.value-prop-icon svg');
      const count = await svgIcons.count();
      expect(count).toBe(3);

      // All SVG icons should be visible
      for (let i = 0; i < count; i++) {
        await expect(svgIcons.nth(i)).toBeVisible();
      }
    });

    test('should have proper typography rendering', async ({ page }) => {
      await page.goto('/');

      // Main heading
      const h1 = page.locator('h1');
      await expect(h1).toBeVisible();

      const fontSize = await h1.evaluate(el =>
        window.getComputedStyle(el).fontSize
      );
      // Font size should be substantial (at least 24px)
      expect(parseFloat(fontSize)).toBeGreaterThanOrEqual(24);

      // Section headings
      const h2Elements = page.locator('h2');
      const h2Count = await h2Elements.count();
      expect(h2Count).toBeGreaterThan(0);
    });

    test('should have proper button styling', async ({ page }) => {
      await page.goto('/');

      const primaryBtn = page.locator('.btn-primary').first();
      await expect(primaryBtn).toBeVisible();

      // Check button has background color (not transparent)
      const bgColor = await primaryBtn.evaluate(el =>
        window.getComputedStyle(el).backgroundColor
      );
      expect(bgColor).not.toBe('rgba(0, 0, 0, 0)');
      expect(bgColor).not.toBe('transparent');
    });
  });

  test.describe('Responsive Behavior', () => {
    test('should be responsive on tablet viewport', async ({ page }) => {
      await page.setViewportSize({ width: 768, height: 1024 });
      await page.goto('/');

      // All major sections should still be visible
      await expect(page.locator('#hero')).toBeVisible();
      await expect(page.locator('#features')).toBeVisible();
      await expect(page.locator('#quick-start')).toBeVisible();
      await expect(page.locator('footer')).toBeVisible();
    });

    test('should be responsive on mobile viewport', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/');

      // All major sections should still be visible
      await expect(page.locator('#hero')).toBeVisible();
      await expect(page.locator('#features')).toBeVisible();
      await expect(page.locator('#quick-start')).toBeVisible();
      await expect(page.locator('footer')).toBeVisible();

      // Mobile menu toggle should be visible
      const menuToggle = page.locator('[data-testid="mobile-menu-toggle"]');
      await expect(menuToggle).toBeVisible();
    });

    test('should handle viewport resize', async ({ page }) => {
      await page.goto('/');

      // Start with desktop
      await page.setViewportSize({ width: 1280, height: 800 });
      await expect(page.locator('#hero')).toBeVisible();

      // Resize to tablet
      await page.setViewportSize({ width: 768, height: 1024 });
      await expect(page.locator('#hero')).toBeVisible();

      // Resize to mobile
      await page.setViewportSize({ width: 375, height: 667 });
      await expect(page.locator('#hero')).toBeVisible();
    });
  });

  test.describe('Content Integrity', () => {
    test('should display command categories', async ({ page }) => {
      await page.goto('/');

      // Storage commands
      const storageCommands = page.locator('[data-testid="commands-storage"]');
      await expect(storageCommands).toBeVisible();

      // Retrieval commands
      const retrievalCommands = page.locator('[data-testid="commands-retrieval"]');
      await expect(retrievalCommands).toBeVisible();

      // Deletion commands
      const deletionCommands = page.locator('[data-testid="commands-deletion"]');
      await expect(deletionCommands).toBeVisible();

      // MirDB-specific commands
      const mirdbCommands = page.locator('[data-testid="commands-mirdb"]');
      await expect(mirdbCommands).toBeVisible();
    });

    test('should display configuration parameters', async ({ page }) => {
      await page.goto('/');

      // Config params should be visible
      const addrParam = page.locator('[data-param="addr"]');
      await expect(addrParam).toBeVisible();

      const maxLevelParam = page.locator('[data-param="max_level"]');
      await expect(maxLevelParam).toBeVisible();

      const workDirParam = page.locator('[data-param="work_dir"]');
      await expect(workDirParam).toBeVisible();
    });

    test('should display footer information', async ({ page }) => {
      await page.goto('/');

      // Version info
      const version = page.locator('[data-testid="footer-version"]');
      await expect(version).toBeVisible();
      await expect(version).toContainText('Version');

      // License info
      const license = page.locator('[data-testid="footer-license"]');
      await expect(license).toBeVisible();
      await expect(license).toContainText('MIT');
    });
  });
});
