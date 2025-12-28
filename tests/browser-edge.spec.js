// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Edge Browser Compatibility Tests
 *
 * These tests verify that the MirDB homepage renders correctly and functions
 * properly in Microsoft Edge browser (NFR-3: last 2 versions support).
 *
 * Test coverage:
 * - Page rendering without console errors
 * - Visual element rendering
 * - Interactive element functionality
 * - Navigation and links
 * - CSS layout and styling
 */

test.describe('Browser Compatibility - Edge', () => {
  /**
   * Test Case 1: Page renders correctly without console errors
   * Input: Load page in Edge latest version
   * Expected: Page renders correctly without console errors
   */
  test.describe('TC1: Page Rendering', () => {
    test('should load page without console errors', async ({ page }) => {
      const consoleErrors = [];
      const pageErrors = [];

      // Collect console errors
      page.on('console', (msg) => {
        if (msg.type() === 'error') {
          consoleErrors.push(msg.text());
        }
      });

      // Collect page errors (uncaught exceptions)
      page.on('pageerror', (error) => {
        pageErrors.push(error.message);
      });

      await page.goto('/', { waitUntil: 'networkidle' });
      await page.waitForLoadState('domcontentloaded');

      // Filter out known non-critical errors (like favicon 404)
      const criticalErrors = consoleErrors.filter(
        (error) =>
          !error.includes('favicon') &&
          !error.includes('404') &&
          !error.includes('net::ERR')
      );

      expect(criticalErrors, 'Should have no critical console errors').toHaveLength(0);
      expect(pageErrors, 'Should have no page errors').toHaveLength(0);
    });

    test('should render page with correct title', async ({ page }) => {
      await page.goto('/');
      await expect(page).toHaveTitle(/MirDB/);
    });

    test('should render all major sections', async ({ page }) => {
      await page.goto('/', { waitUntil: 'networkidle' });

      // Header
      const header = page.locator('header');
      await expect(header).toBeVisible();

      // Hero section
      const heroSection = page.locator('#hero');
      await expect(heroSection).toBeVisible();

      // Product name
      const productName = page.locator('#product-name');
      await expect(productName).toBeVisible();
      await expect(productName).toHaveText('MirDB');

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
      await page.goto('/', { waitUntil: 'networkidle' });

      // Logo should be visible
      const heroLogo = page.locator('#hero-logo');
      await expect(heroLogo).toBeVisible();

      // Logo should have natural dimensions (not broken)
      const logoNaturalWidth = await heroLogo.evaluate((img) => img.naturalWidth);
      expect(logoNaturalWidth).toBeGreaterThan(0);

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
      await page.goto('/', { waitUntil: 'networkidle' });

      // Check all three value props are visible
      const persistentStorage = page.locator('[data-testid="value-prop-persistent-storage"]');
      await expect(persistentStorage).toBeVisible();

      const persistentTitle = page.locator('[data-testid="value-prop-persistent-storage-title"]');
      await expect(persistentTitle).toHaveText('Persistent Storage');

      const memcachedCompatible = page.locator('[data-testid="value-prop-memcached-compatible"]');
      await expect(memcachedCompatible).toBeVisible();

      const memcachedTitle = page.locator('[data-testid="value-prop-memcached-compatible-title"]');
      await expect(memcachedTitle).toHaveText('Memcached Compatible');

      const rustCard = page.locator('[data-testid="value-prop-rust"]');
      await expect(rustCard).toBeVisible();

      const rustTitle = page.locator('[data-testid="value-prop-rust-title"]');
      await expect(rustTitle).toHaveText('Written in Rust');
    });

    test('should render feature cards correctly', async ({ page }) => {
      await page.goto('/', { waitUntil: 'networkidle' });

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
      await page.goto('/', { waitUntil: 'networkidle' });

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

    test('should render CSS correctly', async ({ page }) => {
      await page.goto('/', { waitUntil: 'networkidle' });

      // Verify CSS is loaded
      const stylesheets = await page.evaluate(() => {
        return Array.from(document.styleSheets).map((sheet) => ({
          href: sheet.href,
          rules: sheet.cssRules ? sheet.cssRules.length : 0,
        }));
      });

      // Should have at least one stylesheet loaded (styles.css)
      expect(stylesheets.length).toBeGreaterThan(0);

      // Check hero section has center text alignment
      const heroSection = page.locator('.hero-section');
      const heroComputedStyles = await heroSection.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          textAlign: styles.textAlign,
          visibility: styles.visibility,
        };
      });
      expect(heroComputedStyles.textAlign).toBe('center');
      expect(heroComputedStyles.visibility).toBe('visible');

      // Verify value prop cards have correct layout
      const valuePropsGrid = page.locator('[data-testid="value-props-grid"]');
      const gridDisplay = await valuePropsGrid.evaluate((el) => {
        return window.getComputedStyle(el).display;
      });
      expect(['grid', 'flex']).toContain(gridDisplay);
    });

    test('should render SVG icons correctly', async ({ page }) => {
      await page.goto('/', { waitUntil: 'networkidle' });

      // Value prop icons (SVG)
      const svgIcons = page.locator('.value-prop-icon svg');
      const count = await svgIcons.count();
      expect(count).toBe(3);

      // All SVG icons should be visible and have dimensions
      for (let i = 0; i < count; i++) {
        const svg = svgIcons.nth(i);
        await expect(svg).toBeVisible();

        const svgBox = await svg.boundingBox();
        expect(svgBox).toBeTruthy();
        expect(svgBox.width).toBeGreaterThan(0);
        expect(svgBox.height).toBeGreaterThan(0);
      }
    });
  });

  /**
   * Test Case 2: All interactive elements work correctly
   * Input: Test all interactive elements in Edge
   * Expected: All buttons, links, and navigation work correctly
   */
  test.describe('TC2: Interactive Elements', () => {
    test('should have clickable navigation links', async ({ page }) => {
      await page.goto('/', { waitUntil: 'networkidle' });

      // Navigation links should exist and be enabled
      const navLinks = page.locator('header nav .nav-links a');
      const navCount = await navLinks.count();
      expect(navCount).toBeGreaterThanOrEqual(3);

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
      await page.goto('/', { waitUntil: 'networkidle' });

      // Test Features link navigation
      const featuresLink = page.locator('header nav a[href="#features"]');
      await expect(featuresLink).toBeVisible();
      await featuresLink.click();
      await page.waitForTimeout(500);

      const featuresSection = page.locator('#features');
      const featuresInView = await featuresSection.evaluate((el) => {
        const rect = el.getBoundingClientRect();
        return rect.top >= -100 && rect.top <= window.innerHeight;
      });
      expect(featuresInView).toBe(true);

      // Reset scroll position
      await page.evaluate(() => window.scrollTo(0, 0));
      await page.waitForTimeout(300);

      // Test Docs link navigation
      const docsLink = page.locator('header nav a:has-text("Docs")');
      await expect(docsLink).toBeVisible();
      await docsLink.click();
      await page.waitForTimeout(500);
      const docsHref = await docsLink.getAttribute('href');
      const docsTarget = page.locator(docsHref);
      const docsInView = await docsTarget.evaluate((el) => {
        const rect = el.getBoundingClientRect();
        return rect.top >= -100 && rect.top <= window.innerHeight;
      });
      expect(docsInView).toBe(true);
    });

    test('should have working CTA buttons', async ({ page }) => {
      await page.goto('/', { waitUntil: 'networkidle' });

      // Get Started button
      const getStartedBtn = page.locator('#get-started-btn');
      await expect(getStartedBtn).toBeVisible();
      await expect(getStartedBtn).toBeEnabled();
      await expect(getStartedBtn).toHaveAttribute('href', '#quick-start');

      // Click Get Started and verify scroll
      await getStartedBtn.click();
      await page.waitForTimeout(500);
      const quickStartSection = page.locator('#quick-start');
      const quickStartInView = await quickStartSection.evaluate((el) => {
        const rect = el.getBoundingClientRect();
        return rect.top >= -100 && rect.top <= window.innerHeight;
      });
      expect(quickStartInView).toBe(true);
    });

    test('should have working GitHub button with correct attributes', async ({ page }) => {
      await page.goto('/', { waitUntil: 'networkidle' });

      // GitHub button should have correct attributes for new tab
      const githubBtn = page.locator('#github-btn');
      await expect(githubBtn).toBeVisible();
      await expect(githubBtn).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
      await expect(githubBtn).toHaveAttribute('target', '_blank');

      const githubRel = await githubBtn.getAttribute('rel');
      expect(githubRel).toContain('noopener');
    });

    test('should have working footer links', async ({ page }) => {
      await page.goto('/', { waitUntil: 'networkidle' });

      // GitHub repository link in footer
      const footerGithub = page.locator('[data-testid="footer-github-link"]');
      await expect(footerGithub).toBeVisible();
      await expect(footerGithub).toHaveAttribute('href', 'https://github.com/yetone/mirdb');

      // Issues link
      const issuesLink = page.locator('footer a[href*="issues"]');
      await expect(issuesLink).toBeVisible();
    });

    test('should have accessible skip link', async ({ page }) => {
      await page.goto('/', { waitUntil: 'networkidle' });

      // Skip link should exist
      const skipLink = page.locator('.skip-link');
      await expect(skipLink).toHaveAttribute('href', '#main');
    });

    test('should have all anchor links pointing to valid targets', async ({ page }) => {
      await page.goto('/', { waitUntil: 'networkidle' });

      // All anchor links should have valid targets
      const allLinks = await page.locator('a[href^="#"]').all();
      for (const link of allLinks) {
        const href = await link.getAttribute('href');
        if (href && href.length > 1) {
          const targetId = href.substring(1);
          const target = page.locator(`#${targetId}`);
          const targetExists = (await target.count()) > 0;
          expect(targetExists, `Target ${href} should exist`).toBe(true);
        }
      }
    });

    test('mobile menu toggle should be functional', async ({ page }) => {
      // Set mobile viewport
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/', { waitUntil: 'networkidle' });

      const menuToggle = page.locator('[data-testid="mobile-menu-toggle"]');
      const navLinks = page.locator('.nav-links');

      // Menu toggle should be visible on mobile
      await expect(menuToggle).toBeVisible();

      // Initially aria-expanded should be false
      await expect(menuToggle).toHaveAttribute('aria-expanded', 'false');

      // Click to open menu
      await menuToggle.click();

      // After click, aria-expanded should be true
      await expect(menuToggle).toHaveAttribute('aria-expanded', 'true');

      // Verify nav-open class is added
      const hasNavOpen = await navLinks.evaluate((el) =>
        el.classList.contains('nav-open')
      );
      expect(hasNavOpen).toBe(true);

      // Click again to close
      await menuToggle.click();
      await expect(menuToggle).toHaveAttribute('aria-expanded', 'false');
    });
  });

  test.describe('CSS Layout and Styling', () => {
    test('should have proper grid layout for value props', async ({ page }) => {
      await page.goto('/', { waitUntil: 'networkidle' });

      const valuePropsGrid = page.locator('[data-testid="value-props-grid"]');
      await expect(valuePropsGrid).toBeVisible();

      // Check that grid has proper display
      const display = await valuePropsGrid.evaluate((el) =>
        window.getComputedStyle(el).display
      );
      expect(['grid', 'flex']).toContain(display);
    });

    test('should have proper grid layout for features', async ({ page }) => {
      await page.goto('/', { waitUntil: 'networkidle' });

      const featuresGrid = page.locator('.features-grid');
      await expect(featuresGrid).toBeVisible();

      const display = await featuresGrid.evaluate((el) =>
        window.getComputedStyle(el).display
      );
      expect(['grid', 'flex']).toContain(display);
    });

    test('should have visible navigation', async ({ page }) => {
      await page.goto('/', { waitUntil: 'networkidle' });

      const nav = page.locator('header nav');
      await expect(nav).toBeVisible();

      // Logo should be visible
      const logo = page.locator('.nav-logo');
      await expect(logo).toBeVisible();
    });

    test('should have proper typography rendering', async ({ page }) => {
      await page.goto('/', { waitUntil: 'networkidle' });

      // Main heading
      const h1 = page.locator('h1');
      await expect(h1).toBeVisible();

      const fontSize = await h1.evaluate((el) =>
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
      await page.goto('/', { waitUntil: 'networkidle' });

      const primaryBtn = page.locator('.btn-primary').first();
      await expect(primaryBtn).toBeVisible();

      // Check button has background color (not transparent)
      const bgColor = await primaryBtn.evaluate((el) =>
        window.getComputedStyle(el).backgroundColor
      );
      expect(bgColor).not.toBe('rgba(0, 0, 0, 0)');
      expect(bgColor).not.toBe('transparent');
    });
  });

  test.describe('Responsive Behavior', () => {
    test('should be responsive on tablet viewport', async ({ page }) => {
      await page.setViewportSize({ width: 768, height: 1024 });
      await page.goto('/', { waitUntil: 'networkidle' });

      // All major sections should still be visible
      await expect(page.locator('#hero')).toBeVisible();
      await expect(page.locator('#features')).toBeVisible();
      await expect(page.locator('#quick-start')).toBeVisible();
      await expect(page.locator('footer')).toBeVisible();
    });

    test('should be responsive on mobile viewport', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/', { waitUntil: 'networkidle' });

      // All major sections should still be visible
      await expect(page.locator('#hero')).toBeVisible();
      await expect(page.locator('#features')).toBeVisible();
      await expect(page.locator('#quick-start')).toBeVisible();
      await expect(page.locator('footer')).toBeVisible();

      // Mobile menu toggle should be visible
      const menuToggle = page.locator('[data-testid="mobile-menu-toggle"]');
      await expect(menuToggle).toBeVisible();
    });
  });

  test.describe('Content Integrity', () => {
    test('should display all text content correctly', async ({ page }) => {
      await page.goto('/', { waitUntil: 'networkidle' });

      // Verify product name
      const productName = page.locator('#product-name');
      await expect(productName).toHaveText('MirDB');

      // Verify tagline contains key terms
      const tagline = page.locator('#tagline');
      const taglineText = await tagline.textContent();
      expect(taglineText).toContain('persistent');
      expect(taglineText).toContain('memcached');

      // Verify features heading
      const featuresHeading = page.locator('#features h2');
      await expect(featuresHeading).toHaveText('Key Features');

      // Verify quick start heading
      const quickStartHeading = page.locator('#quick-start h2');
      await expect(quickStartHeading).toHaveText('Quick Start');

      // Verify footer version
      const footerVersion = page.locator('[data-testid="footer-version"]');
      await expect(footerVersion).toContainText('Version');

      // Verify footer license
      const footerLicense = page.locator('[data-testid="footer-license"]');
      await expect(footerLicense).toContainText('MIT');
    });

    test('should display command categories', async ({ page }) => {
      await page.goto('/', { waitUntil: 'networkidle' });

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
      await page.goto('/', { waitUntil: 'networkidle' });

      // Config params should be visible
      const addrParam = page.locator('[data-param="addr"]');
      await expect(addrParam).toBeVisible();

      const maxLevelParam = page.locator('[data-param="max_level"]');
      await expect(maxLevelParam).toBeVisible();

      const workDirParam = page.locator('[data-param="work_dir"]');
      await expect(workDirParam).toBeVisible();
    });
  });
});
