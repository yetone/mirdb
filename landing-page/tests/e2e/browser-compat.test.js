/**
 * MirDB Landing Page - Browser Compatibility E2E Tests
 * Owner: Scenario 15 - Browser Compatibility
 *
 * Tests verify:
 * - Page renders correctly in Chrome, Firefox, Safari, and Edge
 * - Smooth scroll navigation works across all browsers
 * - Mermaid/Architecture diagram renders correctly in all browsers
 * - All features are functional across supported browsers
 */

const { test, expect } = require('@playwright/test');
const { setupPage, waitForAnimations } = require('../helpers/test-utils');

test.describe('Browser Compatibility', () => {
  test.beforeEach(async ({ page }) => {
    await setupPage(page);
  });

  // Test Case 1: Load page in Chrome/Firefox/Safari/Edge - Page renders correctly
  test('TC1: Page renders correctly with all sections visible', async ({ page, browserName }) => {
    // Verify the page title is correct
    const title = await page.title();
    expect(title).toContain('MirDB');

    // Verify main navigation is visible
    const nav = page.locator('nav.nav');
    await expect(nav).toBeVisible();

    // Verify hero section is visible
    const hero = page.locator('#hero');
    await expect(hero).toBeVisible();

    // Verify hero title
    const heroTitle = page.locator('#hero-title');
    await expect(heroTitle).toBeVisible();
    await expect(heroTitle).toContainText('MirDB');

    // Verify features section is visible
    const features = page.locator('#features');
    await expect(features).toBeVisible();

    // Verify all 4 feature cards are present
    const featureCards = page.locator('.feature-card');
    await expect(featureCards).toHaveCount(4);

    // Verify architecture section is visible
    const architecture = page.locator('#architecture');
    await expect(architecture).toBeVisible();

    // Verify configuration section is visible
    const configuration = page.locator('#configuration');
    await expect(configuration).toBeVisible();

    // Verify getting started section is visible
    const gettingStarted = page.locator('#getting-started');
    await expect(gettingStarted).toBeVisible();

    // Verify footer is visible
    const footer = page.locator('footer.footer');
    await expect(footer).toBeVisible();

    // Log the browser name for debugging
    console.log(`Browser compatibility test passed for: ${browserName}`);
  });

  // Test Case 2: CSS and layout rendering
  test('TC2: CSS layout renders correctly', async ({ page, browserName }) => {
    // Verify CSS is loaded and applied
    const nav = page.locator('nav.nav');
    const position = await nav.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return style.position;
    });
    // Navigation should be sticky or fixed
    expect(['sticky', 'fixed']).toContain(position);

    // Verify navigation has background color (CSS loaded)
    const navBgColor = await nav.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return style.backgroundColor;
    });
    // Should have a non-transparent background (dark theme)
    expect(navBgColor).not.toBe('rgba(0, 0, 0, 0)');
    expect(navBgColor).not.toBe('transparent');

    // Verify feature grid uses CSS grid or flexbox
    const featuresGrid = page.locator('.features-grid');
    const display = await featuresGrid.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return style.display;
    });
    expect(['grid', 'flex']).toContain(display);

    // Verify feature cards have proper styling (border and background)
    const featureCard = page.locator('.feature-card').first();
    const cardBgColor = await featureCard.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return style.backgroundColor;
    });
    // Should have a non-transparent background
    expect(cardBgColor).not.toBe('rgba(0, 0, 0, 0)');
    expect(cardBgColor).not.toBe('transparent');

    console.log(`CSS layout test passed for: ${browserName}`);
  });

  // Test Case 3: JavaScript functionality works
  test('TC3: JavaScript functionality is operational', async ({ page, browserName }) => {
    // Verify navigation links are clickable
    const featuresLink = page.locator('.nav-links a[href="#features"]');
    await expect(featuresLink).toBeVisible();
    await expect(featuresLink).toBeEnabled();

    // Verify interactive elements work
    const configToggle = page.locator('#config-toggle-btn');
    if (await configToggle.count() > 0) {
      await expect(configToggle).toBeVisible();

      // Get initial aria-expanded state
      const initialExpanded = await configToggle.getAttribute('aria-expanded');

      // Click to toggle
      await configToggle.click();
      await waitForAnimations(page, 300);

      // Verify toggle works (state should change)
      const newExpanded = await configToggle.getAttribute('aria-expanded');
      expect(newExpanded).not.toBe(initialExpanded);
    }

    console.log(`JavaScript functionality test passed for: ${browserName}`);
  });

  // Test Case 4: Images load correctly
  test('TC4: Images and assets load correctly', async ({ page, browserName }) => {
    // Verify logo image loads
    const logo = page.locator('.hero-logo');
    await expect(logo).toBeVisible();

    // Check that the image has loaded (natural width/height > 0)
    const isLoaded = await logo.evaluate((img) => {
      return img.complete && img.naturalWidth > 0;
    });
    expect(isLoaded).toBe(true);

    // Verify SVG icons in feature cards render
    const featureIcons = page.locator('.feature-icon svg');
    const iconCount = await featureIcons.count();
    expect(iconCount).toBeGreaterThanOrEqual(4);

    for (let i = 0; i < iconCount; i++) {
      const icon = featureIcons.nth(i);
      await expect(icon).toBeVisible();
    }

    console.log(`Images and assets test passed for: ${browserName}`);
  });

  // Test Case 5: Smooth scroll navigation works in all browsers
  test('TC5: Smooth scroll navigation works', async ({ page, browserName }) => {
    // Start at top of page
    await page.evaluate(() => window.scrollTo(0, 0));
    await waitForAnimations(page, 200);

    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);
    expect(initialScrollY).toBeLessThan(50);

    // Click on Features link
    const featuresLink = page.locator('.nav-links a[href="#features"]');
    await featuresLink.click();

    // Wait for smooth scroll animation
    await waitForAnimations(page, 1000);

    // Verify scroll position changed
    const scrolledY = await page.evaluate(() => window.scrollY);
    expect(scrolledY).toBeGreaterThan(initialScrollY);

    // Verify features section is in viewport
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();

    // Verify URL hash was updated
    const currentUrl = page.url();
    expect(currentUrl).toContain('#features');

    // Test scrolling to architecture section
    const architectureLink = page.locator('.nav-links a[href="#architecture"]');
    await architectureLink.click();
    await waitForAnimations(page, 1000);

    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeInViewport();

    console.log(`Smooth scroll navigation test passed for: ${browserName}`);
  });

  // Test Case 6: Architecture diagram (Mermaid) renders correctly in all browsers
  test('TC6: Architecture diagram renders correctly', async ({ page, browserName }) => {
    // Scroll to architecture section
    await page.locator('#architecture').scrollIntoViewIfNeeded();
    await waitForAnimations(page, 500);

    // Verify architecture diagram container is visible
    const diagram = page.locator('.architecture-diagram');
    await expect(diagram).toBeVisible();

    // Verify diagram has proper ARIA attributes for accessibility
    await expect(diagram).toHaveAttribute('role', 'img');
    const ariaLabel = await diagram.getAttribute('aria-label');
    expect(ariaLabel).toContain('LSM-tree');

    // Verify diagram components are rendered
    const diagramNodes = page.locator('.diagram-node');
    const nodeCount = await diagramNodes.count();
    expect(nodeCount).toBeGreaterThanOrEqual(6); // At least 6 nodes in the LSM diagram

    // Verify specific components are present
    const writeNode = page.locator('[data-component="write"]');
    const walNode = page.locator('[data-component="wal"]');
    const memtableNode = page.locator('[data-component="memtable"]');
    const level0Node = page.locator('[data-component="level0"]');
    const levelsNode = page.locator('[data-component="levels"]');

    await expect(writeNode).toBeVisible();
    await expect(walNode).toBeVisible();
    await expect(memtableNode).toBeVisible();
    await expect(level0Node).toBeVisible();
    await expect(levelsNode).toBeVisible();

    // Verify arrows indicating data flow are present
    const arrows = page.locator('.diagram-arrow');
    const arrowCount = await arrows.count();
    expect(arrowCount).toBeGreaterThanOrEqual(5);

    // Verify tooltips work (hover/focus interaction)
    const memtableTooltip = page.locator('#tooltip-memtable');

    // Focus on memtable node to trigger tooltip
    await memtableNode.focus();
    await waitForAnimations(page, 300);

    // Tooltip should be visible on focus
    await expect(memtableTooltip).toBeVisible();

    console.log(`Architecture diagram test passed for: ${browserName}`);
  });

  // Additional test: Links and external navigation work
  test('External links have proper attributes for security', async ({ page, browserName }) => {
    // Check GitHub link in navigation
    const githubNavLink = page.locator('.nav-links a', { hasText: 'GitHub' });
    await expect(githubNavLink).toHaveAttribute('target', '_blank');
    await expect(githubNavLink).toHaveAttribute('rel', /noopener/);

    // Check GitHub link in hero section
    const githubHeroLink = page.locator('.hero-cta a', { hasText: 'GitHub' });
    await expect(githubHeroLink).toHaveAttribute('target', '_blank');
    await expect(githubHeroLink).toHaveAttribute('rel', /noopener/);

    console.log(`External links security test passed for: ${browserName}`);
  });

  // Additional test: Responsive meta viewport is set
  test('Viewport meta tag is properly configured', async ({ page, browserName }) => {
    // Verify viewport meta tag exists and is properly configured
    const viewport = page.locator('meta[name="viewport"]');
    await expect(viewport).toHaveCount(1);

    const content = await viewport.getAttribute('content');
    expect(content).toContain('width=device-width');
    expect(content).toContain('initial-scale=1');

    console.log(`Viewport meta tag test passed for: ${browserName}`);
  });

  // Additional test: Form elements and interactive controls work
  test('Interactive controls are accessible and functional', async ({ page, browserName }) => {
    // Test configuration toggle button
    const configToggle = page.locator('#config-toggle-btn');
    if (await configToggle.count() > 0) {
      // Verify button is focusable
      await configToggle.focus();
      const isFocused = await configToggle.evaluate(
        (el) => document.activeElement === el
      );
      expect(isFocused).toBe(true);

      // Verify keyboard interaction (Enter key)
      const initialExpanded = await configToggle.getAttribute('aria-expanded');
      await page.keyboard.press('Enter');
      await waitForAnimations(page, 300);

      const newExpanded = await configToggle.getAttribute('aria-expanded');
      expect(newExpanded).not.toBe(initialExpanded);
    }

    console.log(`Interactive controls test passed for: ${browserName}`);
  });

  // Additional test: Font rendering
  test('Fonts are loaded and text is readable', async ({ page, browserName }) => {
    // Verify main heading is visible and has appropriate font
    const heroTitle = page.locator('#hero-title');
    await expect(heroTitle).toBeVisible();

    // Check font size is reasonable (at least 24px for main heading)
    const fontSize = await heroTitle.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return parseFloat(style.fontSize);
    });
    expect(fontSize).toBeGreaterThanOrEqual(24);

    // Verify body text is readable (at least 14px)
    const bodyText = page.locator('.hero-value-proposition');
    const bodyFontSize = await bodyText.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return parseFloat(style.fontSize);
    });
    expect(bodyFontSize).toBeGreaterThanOrEqual(14);

    console.log(`Font rendering test passed for: ${browserName}`);
  });
});
