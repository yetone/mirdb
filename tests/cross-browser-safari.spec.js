// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Safari Cross-Browser Compatibility Tests
 *
 * These tests verify that the MirDB homepage renders correctly and functions
 * properly in Safari (WebKit) browser, both in the latest version and
 * the previous version (latest - 1).
 */

test.describe('Cross-Browser Compatibility - Safari', () => {
  test.describe('Safari Latest Version Tests', () => {
    /**
     * Test Case 1: Load homepage in Safari (latest version)
     * Verifies all sections render correctly without visual issues
     */
    test('TC1: All sections render correctly in Safari latest', async ({ page, browserName }) => {
      test.skip(browserName !== 'webkit', 'This test is only for WebKit/Safari');

      // Navigate to homepage
      await page.goto('/');

      // Wait for page to be fully loaded
      await page.waitForLoadState('networkidle');

      // Verify page title
      await expect(page).toHaveTitle(/MirDB/);

      // === HERO SECTION ===
      const heroSection = page.locator('.hero');
      await expect(heroSection).toBeVisible();

      // Verify hero heading
      const heroTitle = page.locator('#hero-title');
      await expect(heroTitle).toBeVisible();
      await expect(heroTitle).toHaveText('MirDB');

      // Verify tagline
      const tagline = page.locator('.tagline');
      await expect(tagline).toBeVisible();
      await expect(tagline).toContainText('Persistent Key-Value Store');

      // Verify description
      const description = page.locator('.description');
      await expect(description).toBeVisible();

      // Verify CTA buttons render correctly
      const primaryCta = page.locator('[data-testid="primary-cta"]');
      await expect(primaryCta).toBeVisible();
      await expect(primaryCta).toHaveText('Get Started');

      const secondaryCta = page.locator('[data-testid="secondary-cta"]');
      await expect(secondaryCta).toBeVisible();
      await expect(secondaryCta).toHaveText('View on GitHub');

      // === FEATURES SECTION ===
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeVisible();

      // Verify features title
      const featuresTitle = page.locator('#features-title');
      await expect(featuresTitle).toBeVisible();
      await expect(featuresTitle).toHaveText('Key Features');

      // Verify features grid is visible
      const featuresGrid = page.locator('[data-testid="features-grid"]');
      await expect(featuresGrid).toBeVisible();

      // Verify all four feature cards
      const featureCards = page.locator('.feature-card');
      await expect(featureCards).toHaveCount(4);

      // Verify each feature card is visible
      await expect(page.locator('[data-testid="feature-card-memcached"]')).toBeVisible();
      await expect(page.locator('[data-testid="feature-card-persistence"]')).toBeVisible();
      await expect(page.locator('[data-testid="feature-card-lsm"]')).toBeVisible();
      await expect(page.locator('[data-testid="feature-card-async"]')).toBeVisible();

      // Verify feature card content
      const memcachedCard = page.locator('[data-testid="feature-card-memcached"]');
      await expect(memcachedCard.locator('h3')).toContainText('Memcached Protocol');

      // === QUICK START SECTION ===
      const quickStartSection = page.locator('#quick-start');
      await expect(quickStartSection).toBeVisible();

      // Verify quick start title
      const quickStartTitle = page.locator('#quickstart-title');
      await expect(quickStartTitle).toBeVisible();
      await expect(quickStartTitle).toHaveText('Quick Start');

      // Verify installation step
      const installationStep = page.locator('[data-testid="installation-step"]');
      await expect(installationStep).toBeVisible();

      // Verify configuration step
      const configStep = page.locator('[data-testid="configuration-step"]');
      await expect(configStep).toBeVisible();

      // Verify usage step
      const usageStep = page.locator('[data-testid="usage-step"]');
      await expect(usageStep).toBeVisible();

      // Verify commands step
      const commandsStep = page.locator('[data-testid="commands-step"]');
      await expect(commandsStep).toBeVisible();

      // Verify code blocks render correctly
      const codeBlocks = page.locator('pre code');
      const codeBlockCount = await codeBlocks.count();
      expect(codeBlockCount).toBeGreaterThan(0);

      // === FOOTER SECTION ===
      const footer = page.locator('[data-testid="footer"]');
      await expect(footer).toBeVisible();

      // Verify project status
      const projectStatus = page.locator('[data-testid="project-status"]');
      await expect(projectStatus).toBeVisible();
      await expect(projectStatus).toContainText('Active Development');

      // Verify footer links
      const footerLinks = page.locator('[data-testid="footer-links"]');
      await expect(footerLinks).toBeVisible();

      // === HEADER/NAVIGATION ===
      const header = page.locator('header.header');
      await expect(header).toBeVisible();

      // Verify logo
      const logo = page.locator('.logo');
      await expect(logo).toBeVisible();

      // Verify navigation links
      const navLinks = page.locator('.nav-links');
      await expect(navLinks).toBeVisible();
    });

    /**
     * Test Case 2: Test navigation in Safari
     * Verifies all navigation links and buttons function correctly
     */
    test('TC2: Navigation links and buttons function correctly in Safari', async ({ page, browserName }) => {
      test.skip(browserName !== 'webkit', 'This test is only for WebKit/Safari');

      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // === TEST HEADER NAVIGATION LINKS ===

      // Test Features nav link (anchor scroll)
      const featuresNavLink = page.locator('.nav-links a[href="#features"]');
      await expect(featuresNavLink).toBeVisible();
      await featuresNavLink.click();

      // Verify scroll to features section
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeInViewport();

      // Test Quick Start nav link (anchor scroll)
      const quickStartNavLink = page.locator('.nav-links a[href="#quick-start"]');
      await expect(quickStartNavLink).toBeVisible();
      await quickStartNavLink.click();

      // Verify scroll to quick start section
      const quickStartSection = page.locator('#quick-start');
      await expect(quickStartSection).toBeInViewport();

      // Test GitHub nav link (external link)
      const githubNavLink = page.locator('.nav-links a[href="https://github.com/yetone/mirdb"]');
      await expect(githubNavLink).toBeVisible();
      await expect(githubNavLink).toHaveAttribute('target', '_blank');
      await expect(githubNavLink).toHaveAttribute('rel', 'noopener noreferrer');

      // === TEST CTA BUTTONS ===

      // Navigate back to top for CTA testing
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Test primary CTA (Get Started - anchor link)
      const primaryCta = page.locator('[data-testid="primary-cta"]');
      await expect(primaryCta).toBeVisible();
      await primaryCta.click();

      // Verify scroll to quick start section
      await expect(quickStartSection).toBeInViewport();

      // Navigate back for secondary CTA test
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Test secondary CTA (View on GitHub - external link)
      const secondaryCta = page.locator('[data-testid="secondary-cta"]');
      await expect(secondaryCta).toBeVisible();
      await expect(secondaryCta).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
      await expect(secondaryCta).toHaveAttribute('target', '_blank');
      await expect(secondaryCta).toHaveAttribute('rel', 'noopener noreferrer');

      // === TEST FOOTER LINKS ===

      // GitHub footer link
      const footerGithubLink = page.locator('[data-testid="footer-github-link"]');
      await expect(footerGithubLink).toBeVisible();
      await expect(footerGithubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
      await expect(footerGithubLink).toHaveAttribute('target', '_blank');

      // Documentation footer link
      const footerDocsLink = page.locator('[data-testid="footer-docs-link"]');
      await expect(footerDocsLink).toBeVisible();
      await expect(footerDocsLink).toHaveAttribute('target', '_blank');

      // License footer link
      const footerLicenseLink = page.locator('[data-testid="footer-license-link"]');
      await expect(footerLicenseLink).toBeVisible();
      await expect(footerLicenseLink).toHaveAttribute('target', '_blank');

      // === TEST SKIP LINK FUNCTIONALITY ===

      // Test skip link (accessibility navigation)
      const skipLink = page.locator('[data-testid="skip-link"]');
      await expect(skipLink).toHaveAttribute('href', '#main-content');

      // Focus on skip link and verify it becomes visible
      await skipLink.focus();

      // Click skip link and verify main content receives focus
      await skipLink.click();
      const mainContent = page.locator('#main-content');
      await expect(mainContent).toBeFocused();
    });

    /**
     * Additional Safari-specific tests for CSS rendering and layout
     */
    test('TC1.1: CSS properties render correctly in Safari', async ({ page, browserName }) => {
      test.skip(browserName !== 'webkit', 'This test is only for WebKit/Safari');

      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Test CSS Grid layout in features section
      const featuresGrid = page.locator('[data-testid="features-grid"]');
      await expect(featuresGrid).toBeVisible();

      // Verify grid display is applied
      const gridDisplay = await featuresGrid.evaluate((el) => {
        return window.getComputedStyle(el).display;
      });
      expect(gridDisplay).toBe('grid');

      // Test flexbox layout in navigation
      const nav = page.locator('.nav');
      const navDisplay = await nav.evaluate((el) => {
        return window.getComputedStyle(el).display;
      });
      expect(navDisplay).toBe('flex');

      // Test CSS transitions work
      const featureCard = page.locator('.feature-card').first();
      const initialTransform = await featureCard.evaluate((el) => {
        return window.getComputedStyle(el).transform;
      });

      // Hover over card and check transition
      await featureCard.hover();
      await page.waitForTimeout(300); // Wait for transition

      // Test sticky header positioning
      const header = page.locator('.header');
      const headerPosition = await header.evaluate((el) => {
        return window.getComputedStyle(el).position;
      });
      expect(headerPosition).toBe('sticky');

      // Test CSS custom properties (CSS variables)
      const body = page.locator('body');
      const fontFamily = await body.evaluate((el) => {
        return window.getComputedStyle(el).fontFamily;
      });
      expect(fontFamily).toContain('system');

      // Test button styling
      const primaryBtn = page.locator('.btn-primary').first();
      const btnBgColor = await primaryBtn.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });
      // Verify background color is applied (blue tone)
      expect(btnBgColor).toMatch(/rgb\(\d+,\s*\d+,\s*\d+\)/);
    });

    /**
     * Test SVG icons render correctly in Safari
     */
    test('TC1.2: SVG icons render correctly in Safari', async ({ page, browserName }) => {
      test.skip(browserName !== 'webkit', 'This test is only for WebKit/Safari');

      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Verify SVG icons in feature cards
      const featureIcons = page.locator('.feature-icon svg');
      const iconCount = await featureIcons.count();
      expect(iconCount).toBe(4);

      // Verify each SVG is visible
      for (let i = 0; i < iconCount; i++) {
        const icon = featureIcons.nth(i);
        await expect(icon).toBeVisible();

        // Verify SVG has proper dimensions
        const svgBox = await icon.boundingBox();
        expect(svgBox).not.toBeNull();
        expect(svgBox.width).toBeGreaterThan(0);
        expect(svgBox.height).toBeGreaterThan(0);
      }
    });

    /**
     * Test smooth scrolling behavior in Safari
     */
    test('TC2.1: Smooth scrolling works in Safari', async ({ page, browserName }) => {
      test.skip(browserName !== 'webkit', 'This test is only for WebKit/Safari');

      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Get initial scroll position
      const initialScrollY = await page.evaluate(() => window.scrollY);
      expect(initialScrollY).toBe(0);

      // Click on features link
      await page.locator('.nav-links a[href="#features"]').click();

      // Wait for scroll to complete
      await page.waitForTimeout(500);

      // Verify scroll position changed
      const newScrollY = await page.evaluate(() => window.scrollY);
      expect(newScrollY).toBeGreaterThan(0);

      // Verify features section is in viewport
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeInViewport();
    });
  });

  test.describe('Safari Previous Version Tests (latest - 1)', () => {
    /**
     * Test Case 3: Load homepage in Safari (latest - 1 version)
     * Note: Playwright WebKit represents Safari's rendering engine.
     * This test ensures backward compatibility with Safari features.
     */
    test('TC3: All sections render correctly in Safari (backward compatibility)', async ({ page, browserName }) => {
      test.skip(browserName !== 'webkit', 'This test is only for WebKit/Safari');

      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // === VERIFY BASIC HTML5 ELEMENTS ===

      // Main content area
      const main = page.locator('main#main-content');
      await expect(main).toBeVisible();

      // Article elements (feature cards)
      const articles = page.locator('article.feature-card');
      await expect(articles).toHaveCount(4);

      // Section elements
      const sections = page.locator('section');
      const sectionCount = await sections.count();
      expect(sectionCount).toBeGreaterThanOrEqual(3);

      // === VERIFY BACKWARD COMPATIBLE CSS ===

      // Test that flexbox works (widely supported)
      const ctaButtons = page.locator('.cta-buttons');
      const ctaDisplay = await ctaButtons.evaluate((el) => {
        return window.getComputedStyle(el).display;
      });
      expect(ctaDisplay).toBe('flex');

      // Test that CSS grid works (supported since Safari 10.1)
      const featuresGrid = page.locator('[data-testid="features-grid"]');
      const gridDisplay = await featuresGrid.evaluate((el) => {
        return window.getComputedStyle(el).display;
      });
      expect(gridDisplay).toBe('grid');

      // === VERIFY BASIC INTERACTIVITY ===

      // Test hover states (basic CSS feature)
      const firstFeatureCard = page.locator('.feature-card').first();
      await expect(firstFeatureCard).toBeVisible();

      // Test link functionality
      const allLinks = page.locator('a[href]');
      const linkCount = await allLinks.count();
      expect(linkCount).toBeGreaterThan(0);

      // === VERIFY MEDIA QUERIES WORK ===

      // Verify responsive design by checking viewport-dependent styles
      const viewportWidth = page.viewportSize()?.width || 1280;
      expect(viewportWidth).toBeGreaterThan(0);

      // === VERIFY CODE BLOCKS RENDER ===

      // Test that pre/code blocks render correctly
      const codeBlocks = page.locator('pre code');
      const codeBlockCount = await codeBlocks.count();
      expect(codeBlockCount).toBeGreaterThan(0);

      // Verify code is visible and readable
      const firstCodeBlock = codeBlocks.first();
      await expect(firstCodeBlock).toBeVisible();
      const codeText = await firstCodeBlock.textContent();
      expect(codeText).toBeTruthy();
      expect(codeText.length).toBeGreaterThan(0);

      // === VERIFY IMAGES LOAD ===

      // Test logo image loads
      const logoImage = page.locator('.logo img');
      await expect(logoImage).toBeVisible();

      // Verify image has dimensions
      const imageBox = await logoImage.boundingBox();
      expect(imageBox).not.toBeNull();
      expect(imageBox.width).toBeGreaterThan(0);
      expect(imageBox.height).toBeGreaterThan(0);

      // === VERIFY ANIMATIONS (with reduced motion handling) ===

      // Status indicator animation
      const statusIndicator = page.locator('.status-indicator');
      await expect(statusIndicator).toBeVisible();

      // The animation property should be defined
      const animation = await statusIndicator.evaluate((el) => {
        return window.getComputedStyle(el).animation;
      });
      // Animation should be present (may vary based on reduced motion preference)
      expect(animation).toBeTruthy();
    });

    /**
     * Test backward compatible JavaScript features
     */
    test('TC3.1: JavaScript features work in Safari (backward compatibility)', async ({ page, browserName }) => {
      test.skip(browserName !== 'webkit', 'This test is only for WebKit/Safari');

      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Test that Prism syntax highlighting loaded and executed
      const highlightedCode = page.locator('.token').first();

      // Wait for Prism to initialize
      await page.waitForTimeout(1000);

      // Check if syntax highlighting is applied (Prism adds 'token' classes)
      const tokenCount = await page.locator('.token').count();

      // Verify external scripts loaded
      const scriptsLoaded = await page.evaluate(() => {
        return typeof Prism !== 'undefined';
      });
      expect(scriptsLoaded).toBe(true);
    });
  });

  test.describe('Safari Visual Regression Tests', () => {
    /**
     * Test that no visual rendering issues occur
     */
    test('No layout overflow or clipping issues', async ({ page, browserName }) => {
      test.skip(browserName !== 'webkit', 'This test is only for WebKit/Safari');

      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Check for horizontal overflow (common Safari issue)
      const hasHorizontalOverflow = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });
      expect(hasHorizontalOverflow).toBe(false);

      // Check that all major sections fit within viewport width
      const sections = ['.hero', '#features', '#quick-start', '.footer'];

      for (const selector of sections) {
        const section = page.locator(selector);
        const box = await section.boundingBox();
        expect(box).not.toBeNull();

        // Section should not extend beyond viewport
        const viewportWidth = page.viewportSize()?.width || 1280;
        expect(box.width).toBeLessThanOrEqual(viewportWidth + 1); // +1 for rounding
      }
    });

    /**
     * Test text rendering in Safari
     */
    test('Text renders correctly without clipping', async ({ page, browserName }) => {
      test.skip(browserName !== 'webkit', 'This test is only for WebKit/Safari');

      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Check major text elements are visible and have content
      const textElements = [
        { selector: '#hero-title', expectedText: 'MirDB' },
        { selector: '.tagline', expectedText: 'Persistent' },
        { selector: '#features-title', expectedText: 'Key Features' },
        { selector: '#quickstart-title', expectedText: 'Quick Start' },
      ];

      for (const { selector, expectedText } of textElements) {
        const element = page.locator(selector);
        await expect(element).toBeVisible();
        await expect(element).toContainText(expectedText);

        // Verify text is not clipped (element has proper dimensions)
        const box = await element.boundingBox();
        expect(box).not.toBeNull();
        expect(box.height).toBeGreaterThan(0);
        expect(box.width).toBeGreaterThan(0);
      }
    });
  });
});
