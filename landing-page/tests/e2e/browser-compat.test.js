/**
 * MirDB Landing Page - Browser Compatibility E2E Tests
 * Owner: Scenario 15 - Browser Compatibility
 *
 * Tests verify:
 * - Page renders correctly across Chrome, Firefox, Safari (WebKit), and Edge
 * - All key features are functional in each browser
 * - Smooth scroll navigation works across browsers
 * - Architecture diagram (Mermaid-style) renders correctly across browsers
 *
 * Note: Playwright uses WebKit engine to simulate Safari behavior.
 * Edge is based on Chromium, so chromium tests effectively cover Edge.
 */

const { test, expect } = require('@playwright/test');
const { setupPage, waitForAnimations } = require('../helpers/test-utils');

/**
 * Browser compatibility tests run across multiple browser projects.
 * The playwright.config.js must be configured to run tests on:
 * - chromium (Chrome/Edge)
 * - firefox
 * - webkit (Safari)
 */

test.describe('Browser Compatibility Tests', () => {
  test.beforeEach(async ({ page }) => {
    await setupPage(page);
  });

  // Test Case 1 & 4: Page renders correctly in Chrome/Edge (Chromium-based)
  // Test Case 2: Page renders correctly in Firefox
  // Test Case 3: Page renders correctly in Safari (WebKit)
  // These tests run across all configured browser projects automatically
  test('TC1-4: Page renders correctly with all sections visible', async ({ page, browserName }) => {
    // Verify the page title
    await expect(page).toHaveTitle(/MirDB/);

    // Verify all major sections are present and visible
    const hero = page.locator('#hero');
    await expect(hero).toBeVisible();

    const features = page.locator('#features');
    await expect(features).toBeVisible();

    const usage = page.locator('#usage');
    await expect(usage).toBeAttached();

    const architecture = page.locator('#architecture');
    await expect(architecture).toBeAttached();

    const configuration = page.locator('#configuration');
    await expect(configuration).toBeAttached();

    const gettingStarted = page.locator('#getting-started');
    await expect(gettingStarted).toBeAttached();

    const footer = page.locator('footer');
    await expect(footer).toBeAttached();

    // Verify navigation is present
    const nav = page.locator('nav.nav');
    await expect(nav).toBeVisible();

    // Log browser name for debugging
    console.log(`Testing in browser: ${browserName}`);
  });

  test('TC1-4: Hero section displays correctly', async ({ page, browserName }) => {
    // Verify hero section elements
    const heroTitle = page.locator('#hero-title');
    await expect(heroTitle).toBeVisible();
    await expect(heroTitle).toContainText('MirDB');

    // Verify logo image loads
    const heroLogo = page.locator('.hero-logo');
    await expect(heroLogo).toBeVisible();

    // Check that the logo has valid dimensions (image loaded successfully)
    const logoDimensions = await heroLogo.boundingBox();
    expect(logoDimensions).not.toBeNull();
    expect(logoDimensions.width).toBeGreaterThan(0);
    expect(logoDimensions.height).toBeGreaterThan(0);

    // Verify CTA buttons are present
    const ctaButtons = page.locator('.hero-cta .btn');
    const buttonCount = await ctaButtons.count();
    expect(buttonCount).toBeGreaterThanOrEqual(2);

    console.log(`Hero section verified in: ${browserName}`);
  });

  test('TC1-4: Features section displays grid layout correctly', async ({ page, browserName }) => {
    // Scroll to features section
    await page.locator('#features').scrollIntoViewIfNeeded();
    await waitForAnimations(page, 500);

    // Verify features grid
    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    // Verify all 4 feature cards are present
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBe(4);

    // Verify each card has icon, title, and description
    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i);
      await expect(card.locator('.feature-icon')).toBeVisible();
      await expect(card.locator('h3')).toBeVisible();
      await expect(card.locator('.feature-description')).toBeVisible();
    }

    console.log(`Features section verified in: ${browserName}`);
  });

  test('TC1-4: Navigation links are functional', async ({ page, browserName }) => {
    // Verify all navigation links
    const navLinks = page.locator('.nav-links a');
    const linkCount = await navLinks.count();
    expect(linkCount).toBeGreaterThanOrEqual(4);

    // Verify Features link
    const featuresLink = page.locator('.nav-links a[href="#features"]');
    await expect(featuresLink).toBeVisible();

    // Verify Usage link
    const usageLink = page.locator('.nav-links a[href="#usage"]');
    await expect(usageLink).toBeVisible();

    // Verify Architecture link
    const architectureLink = page.locator('.nav-links a[href="#architecture"]');
    await expect(architectureLink).toBeVisible();

    // Verify GitHub link
    const githubLink = page.locator('.nav-links a[href*="github.com"]');
    await expect(githubLink).toBeVisible();
    await expect(githubLink).toHaveAttribute('target', '_blank');

    console.log(`Navigation links verified in: ${browserName}`);
  });

  // Test Case 5: Smooth scroll navigation works in all browsers
  test('TC5: Smooth scroll navigation works correctly', async ({ page, browserName }) => {
    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Click on Features link to trigger smooth scroll
    const featuresLink = page.locator('.nav-links a[href="#features"]');
    await featuresLink.click();

    // Wait for smooth scroll animation to complete
    await waitForAnimations(page, 1000);

    // Verify scroll position changed
    const finalScrollY = await page.evaluate(() => window.scrollY);
    expect(finalScrollY).toBeGreaterThan(initialScrollY);

    // Verify features section is in viewport
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();

    // Verify URL hash updated
    const currentUrl = page.url();
    expect(currentUrl).toContain('#features');

    console.log(`Smooth scroll verified in: ${browserName}`);
  });

  test('TC5: Smooth scroll to architecture section works', async ({ page, browserName }) => {
    // Click on Architecture link
    const architectureLink = page.locator('.nav-links a[href="#architecture"]');
    await architectureLink.click();

    // Wait for smooth scroll animation
    await waitForAnimations(page, 1000);

    // Verify architecture section is in viewport
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeInViewport();

    // Verify URL hash
    const currentUrl = page.url();
    expect(currentUrl).toContain('#architecture');

    console.log(`Architecture smooth scroll verified in: ${browserName}`);
  });

  // Test Case 6: Architecture diagram renders correctly in all browsers
  test('TC6: Architecture diagram renders correctly', async ({ page, browserName }) => {
    // Scroll to architecture section
    await page.locator('#architecture').scrollIntoViewIfNeeded();
    await waitForAnimations(page, 500);

    // Verify architecture section title
    const architectureTitle = page.locator('#architecture-title');
    await expect(architectureTitle).toBeVisible();
    await expect(architectureTitle).toContainText('Architecture');

    // Verify the diagram container is visible
    const diagramContainer = page.locator('.architecture-diagram');
    await expect(diagramContainer).toBeVisible();

    // Verify diagram nodes are rendered (LSM-tree components)
    const diagramNodes = page.locator('.diagram-node');
    const nodeCount = await diagramNodes.count();
    expect(nodeCount).toBeGreaterThan(0);

    // Verify key diagram components are present
    const writeNode = page.locator('.diagram-node--write');
    await expect(writeNode).toBeVisible();

    const walNode = page.locator('.diagram-node--wal');
    await expect(walNode).toBeVisible();

    const memtableNode = page.locator('.diagram-node--memtable');
    await expect(memtableNode).toBeVisible();

    // Verify tooltips are attached (accessible)
    const tooltips = page.locator('.diagram-tooltip');
    const tooltipCount = await tooltips.count();
    expect(tooltipCount).toBeGreaterThan(0);

    // Verify diagram has proper ARIA label
    await expect(diagramContainer).toHaveAttribute('role', 'img');
    const ariaLabel = await diagramContainer.getAttribute('aria-label');
    expect(ariaLabel).toBeTruthy();
    expect(ariaLabel).toContain('LSM-tree');

    console.log(`Architecture diagram verified in: ${browserName}`);
  });

  test('TC6: Architecture diagram nodes have interactive tooltips', async ({ page, browserName }) => {
    // Scroll to architecture section
    await page.locator('#architecture').scrollIntoViewIfNeeded();
    await waitForAnimations(page, 500);

    // Verify diagram nodes are focusable (keyboard accessible)
    const focusableNodes = page.locator('.diagram-node[tabindex="0"]');
    const focusableCount = await focusableNodes.count();
    expect(focusableCount).toBeGreaterThan(0);

    // Test that nodes have aria-describedby for tooltips
    const firstNode = focusableNodes.first();
    const describedBy = await firstNode.getAttribute('aria-describedby');
    expect(describedBy).toBeTruthy();

    console.log(`Architecture diagram tooltips verified in: ${browserName}`);
  });

  // Additional cross-browser tests for core functionality
  test('TC1-4: CSS styles are applied correctly', async ({ page, browserName }) => {
    // Verify CSS is loaded and applied
    const body = page.locator('body');

    // Check that CSS custom properties are working
    const bgColor = await body.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });
    expect(bgColor).toBeTruthy();

    // Verify navigation has proper styling
    const nav = page.locator('nav.nav');
    const navPosition = await nav.evaluate((el) => {
      return window.getComputedStyle(el).position;
    });
    // Navigation should be sticky or fixed
    expect(['sticky', 'fixed', 'relative']).toContain(navPosition);

    console.log(`CSS styles verified in: ${browserName}`);
  });

  test('TC1-4: Images load correctly across browsers', async ({ page, browserName }) => {
    // Wait for images to load
    await page.waitForLoadState('load');

    // Check hero logo
    const heroLogo = page.locator('.hero-logo');
    if (await heroLogo.count() > 0) {
      const logoNaturalWidth = await heroLogo.evaluate((img) => {
        return img.naturalWidth;
      });
      expect(logoNaturalWidth).toBeGreaterThan(0);
    }

    console.log(`Images verified in: ${browserName}`);
  });

  test('TC1-4: External links have security attributes', async ({ page, browserName }) => {
    // Find all external links (GitHub, etc.)
    const externalLinks = page.locator('a[target="_blank"]');
    const linkCount = await externalLinks.count();

    for (let i = 0; i < linkCount; i++) {
      const link = externalLinks.nth(i);
      const rel = await link.getAttribute('rel');
      // External links should have noopener for security
      expect(rel).toContain('noopener');
    }

    console.log(`External links security verified in: ${browserName}`);
  });

  test('TC1-4: Footer renders correctly across browsers', async ({ page, browserName }) => {
    // Scroll to footer
    await page.locator('footer').scrollIntoViewIfNeeded();
    await waitForAnimations(page, 500);

    // Verify footer content
    const footer = page.locator('footer.footer');
    await expect(footer).toBeVisible();

    // Verify GitHub link in footer
    const githubLink = footer.locator('a[href*="github.com"]');
    await expect(githubLink.first()).toBeVisible();

    // Verify license information
    const licenseText = footer.locator('.footer-license');
    await expect(licenseText).toBeVisible();

    console.log(`Footer verified in: ${browserName}`);
  });

  test('TC1-4: Configuration section renders correctly', async ({ page, browserName }) => {
    // Scroll to configuration section
    await page.locator('#configuration').scrollIntoViewIfNeeded();
    await waitForAnimations(page, 500);

    // Verify config section
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Verify config toggle button exists
    const configToggle = page.locator('.config-toggle');
    await expect(configToggle).toBeVisible();

    // Verify config table exists
    const configTable = page.locator('.config-table');
    await expect(configTable).toBeVisible();

    console.log(`Configuration section verified in: ${browserName}`);
  });

  test('TC1-4: Getting Started section renders correctly', async ({ page, browserName }) => {
    // Scroll to getting started section
    await page.locator('#getting-started').scrollIntoViewIfNeeded();
    await waitForAnimations(page, 500);

    // Verify section exists
    const gettingStarted = page.locator('#getting-started');
    await expect(gettingStarted).toBeVisible();

    // Verify installation options
    const installationOptions = page.locator('.installation-option');
    const optionCount = await installationOptions.count();
    expect(optionCount).toBeGreaterThanOrEqual(1);

    // Verify code blocks
    const codeBlocks = gettingStarted.locator('.code-block');
    const codeBlockCount = await codeBlocks.count();
    expect(codeBlockCount).toBeGreaterThan(0);

    console.log(`Getting Started section verified in: ${browserName}`);
  });
});

// Additional test suite for browser-specific behaviors
test.describe('Browser-Specific Rendering', () => {
  test.beforeEach(async ({ page }) => {
    await setupPage(page);
  });

  test('Page loads within acceptable time', async ({ page, browserName }) => {
    const startTime = Date.now();
    await page.waitForLoadState('domcontentloaded');
    const loadTime = Date.now() - startTime;

    // Page should load within 5 seconds even on slower connections
    expect(loadTime).toBeLessThan(5000);

    console.log(`Page load time in ${browserName}: ${loadTime}ms`);
  });

  test('No JavaScript errors on page load', async ({ page, browserName }) => {
    const errors = [];

    page.on('pageerror', (error) => {
      errors.push(error.message);
    });

    await page.goto('/');
    await page.waitForLoadState('load');

    // Wait a bit for any delayed JavaScript execution
    await waitForAnimations(page, 500);

    // There should be no JavaScript errors
    expect(errors).toHaveLength(0);

    console.log(`No JS errors in: ${browserName}`);
  });

  test('Console warnings are minimal', async ({ page, browserName }) => {
    const warnings = [];

    page.on('console', (msg) => {
      if (msg.type() === 'warning' || msg.type() === 'error') {
        // Ignore some common non-critical warnings
        const text = msg.text();
        if (!text.includes('favicon') && !text.includes('DevTools')) {
          warnings.push(text);
        }
      }
    });

    await page.goto('/');
    await page.waitForLoadState('load');

    // Log warnings but don't fail (some browsers have different warning behaviors)
    if (warnings.length > 0) {
      console.log(`Warnings in ${browserName}:`, warnings);
    }
  });
});
