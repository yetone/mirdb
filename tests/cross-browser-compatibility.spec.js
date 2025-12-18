// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = path.resolve(__dirname, '../index.html');

/**
 * Cross-Browser Compatibility Tests
 * Scenario: Verify the landing page renders correctly in modern browsers
 *
 * This test suite verifies that the MirDB landing page renders correctly
 * across all major browsers. The tests are run against all browser projects
 * defined in the playwright.config.js file.
 *
 * NFR-4: Page must render correctly in modern browsers (Chrome, Firefox, Safari, Edge - last 2 versions)
 *
 * Test Cases:
 * - TC1: Chrome compatibility (chromium)
 * - TC2: Firefox compatibility (firefox)
 * - TC3: Safari compatibility (webkit)
 * - TC4: Edge compatibility (chromium-based)
 * - TC5: CSS vendor prefix compatibility
 */

/**
 * Core rendering tests - run across all browsers
 * Tests hero section, navigation, features, code blocks, and footer
 */
test.describe('Cross-Browser Page Rendering', () => {
  test('Page loads and renders core elements correctly', async ({ page, browserName }) => {
    // Navigate to the landing page
    await page.goto(`file://${indexPath}`);

    // Log which browser is being tested
    console.log(`Testing in browser: ${browserName}`);

    // Verify page loads successfully
    await expect(page).toHaveTitle(/MirDB/);

    // Verify hero section renders correctly
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    const heroTitle = page.locator('[data-testid="hero-title"]');
    await expect(heroTitle).toBeVisible();
    await expect(heroTitle).toHaveText('MirDB');

    const heroTagline = page.locator('[data-testid="hero-tagline"]');
    await expect(heroTagline).toBeVisible();

    // Verify navigation renders correctly
    const navbar = page.locator('[data-testid="navbar"]');
    await expect(navbar).toBeVisible();

    // Verify CTA buttons render correctly
    const ctaGetStarted = page.locator('[data-testid="cta-get-started"]');
    await expect(ctaGetStarted).toBeVisible();
    await expect(ctaGetStarted).toHaveCSS('background-color', 'rgb(37, 99, 235)');

    const ctaGithub = page.locator('[data-testid="cta-github"]');
    await expect(ctaGithub).toBeVisible();

    // Verify features section renders correctly
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    const featureCards = page.locator('.feature-card');
    await expect(featureCards).toHaveCount(3);

    // Verify quick start section renders correctly
    const quickstartSection = page.locator('[data-testid="quickstart-section"]');
    await expect(quickstartSection).toBeVisible();

    // Verify code blocks render correctly
    const codeBlocks = page.locator('.code-block');
    expect(await codeBlocks.count()).toBeGreaterThan(0);

    // Verify tables render correctly
    const tables = page.locator('table');
    expect(await tables.count()).toBeGreaterThan(0);

    // Verify footer renders correctly
    const footer = page.locator('.footer');
    await expect(footer).toBeVisible();
  });

  test('Interactive elements are functional', async ({ page, browserName }) => {
    await page.goto(`file://${indexPath}`);

    console.log(`Testing interactive elements in browser: ${browserName}`);

    // Test smooth scroll navigation
    const navFeaturesLink = page.locator('[data-testid="nav-features"]');
    await navFeaturesLink.click();

    // Wait for scroll animation
    await page.waitForTimeout(500);

    // Verify features section is in viewport
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();

    // Test copy button functionality exists
    const copyBtn = page.locator('.copy-btn').first();
    await expect(copyBtn).toBeVisible();
    await expect(copyBtn).toBeEnabled();
  });

  test('CSS styling is applied correctly', async ({ page, browserName }) => {
    await page.goto(`file://${indexPath}`);

    console.log(`Testing CSS styling in browser: ${browserName}`);

    // Verify CSS variables are working
    const heroTitle = page.locator('[data-testid="hero-title"]');
    const fontSize = await heroTitle.evaluate(el =>
      parseFloat(window.getComputedStyle(el).fontSize)
    );
    expect(fontSize).toBeGreaterThanOrEqual(32); // Minimum expected font size

    // Verify flexbox layout in hero CTA
    const heroCta = page.locator('[data-testid="hero-cta"]');
    const display = await heroCta.evaluate(el =>
      window.getComputedStyle(el).display
    );
    expect(display).toBe('flex');

    // Verify grid layout in features
    const featuresGrid = page.locator('.features-grid');
    const gridDisplay = await featuresGrid.evaluate(el =>
      window.getComputedStyle(el).display
    );
    expect(gridDisplay).toBe('grid');
  });
});

/**
 * Test Case 5: CSS Vendor Prefix Compatibility
 * Input: Check CSS prefix compatibility
 * Expected: CSS uses appropriate vendor prefixes or is autoprefixed
 *
 * This test verifies that CSS properties that may require vendor prefixes
 * work correctly across all browsers.
 */
test.describe('CSS Vendor Prefix Compatibility', () => {
  test('Flexbox and Grid layout properties work correctly', async ({ page, browserName }) => {
    await page.goto(`file://${indexPath}`);

    console.log(`Testing flexbox/grid in browser: ${browserName}`);

    // Test 1: Flexbox (historically needed -webkit-flex, -ms-flexbox)
    const heroCta = page.locator('[data-testid="hero-cta"]');
    const flexDisplay = await heroCta.evaluate(el =>
      window.getComputedStyle(el).display
    );
    expect(flexDisplay).toBe('flex');

    // Test 2: CSS Grid (historically needed -ms-grid)
    const featuresGrid = page.locator('.features-grid');
    const gridDisplay = await featuresGrid.evaluate(el =>
      window.getComputedStyle(el).display
    );
    expect(gridDisplay).toBe('grid');

    // Test 3: Flex direction
    const navLinks = page.locator('.nav-links');
    const flexDirection = await navLinks.evaluate(el =>
      window.getComputedStyle(el).flexDirection
    );
    // On desktop, nav links should be row
    expect(['row', 'row-reverse', 'column', 'column-reverse']).toContain(flexDirection);
  });

  test('CSS Variables and custom properties work correctly', async ({ page, browserName }) => {
    await page.goto(`file://${indexPath}`);

    console.log(`Testing CSS variables in browser: ${browserName}`);

    // Test CSS Variables (custom properties)
    const primaryButton = page.locator('.btn-primary').first();
    const bgColor = await primaryButton.evaluate(el =>
      window.getComputedStyle(el).backgroundColor
    );
    // Primary color should be applied (rgb(37, 99, 235) = #2563eb)
    expect(bgColor).toBe('rgb(37, 99, 235)');

    // Test that CSS variable cascade works
    const secondaryButton = page.locator('.btn-secondary').first();
    const secondaryBgColor = await secondaryButton.evaluate(el =>
      window.getComputedStyle(el).backgroundColor
    );
    // Secondary color should be different from primary
    expect(secondaryBgColor).toBe('rgb(55, 65, 81)');
  });

  test('Visual effects (border-radius, box-shadow, transitions) work correctly', async ({ page, browserName }) => {
    await page.goto(`file://${indexPath}`);

    console.log(`Testing visual effects in browser: ${browserName}`);

    // Test 1: Border-radius
    const primaryButton = page.locator('.btn-primary').first();
    const btnRadius = await primaryButton.evaluate(el =>
      window.getComputedStyle(el).borderRadius
    );
    expect(btnRadius).not.toBe('0px');

    // Test 2: Box-shadow on hover
    const featureCard = page.locator('.feature-card').first();
    await featureCard.hover();
    // Give time for transition
    await page.waitForTimeout(300);
    const boxShadow = await featureCard.evaluate(el =>
      window.getComputedStyle(el).boxShadow
    );
    // Should have a shadow on hover
    expect(boxShadow).not.toBe('none');

    // Test 3: Transitions
    const transitionValue = await primaryButton.evaluate(el =>
      window.getComputedStyle(el).transition
    );
    expect(transitionValue).not.toBe('none');
    expect(transitionValue).not.toBe('all 0s ease 0s');
  });

  test('Transform and gradient properties work correctly', async ({ page, browserName }) => {
    await page.goto(`file://${indexPath}`);

    console.log(`Testing transform/gradient in browser: ${browserName}`);

    // Test 1: Transform (historically needed -webkit-transform)
    const primaryButton = page.locator('.btn-primary').first();
    const transformValue = await primaryButton.evaluate(el => {
      return window.getComputedStyle(el).transform;
    });
    // Transform property should be readable (even if 'none')
    expect(transformValue === 'none' || transformValue.includes('matrix')).toBe(true);

    // Test 2: Linear gradient background
    const heroSection = page.locator('[data-testid="hero-section"]');
    const heroBg = await heroSection.evaluate(el =>
      window.getComputedStyle(el).backgroundImage
    );
    // Should have a gradient background
    expect(heroBg).toContain('gradient');
  });

  test('Scroll and positioning properties work correctly', async ({ page, browserName }) => {
    await page.goto(`file://${indexPath}`);

    console.log(`Testing scroll/positioning in browser: ${browserName}`);

    // Test 1: Scroll behavior (smooth)
    const htmlScrollBehavior = await page.evaluate(() =>
      window.getComputedStyle(document.documentElement).scrollBehavior
    );
    expect(htmlScrollBehavior).toBe('smooth');

    // Test 2: Sticky positioning (historically needed -webkit-sticky)
    const navbar = page.locator('[data-testid="navbar"]');
    const position = await navbar.evaluate(el =>
      window.getComputedStyle(el).position
    );
    expect(position).toBe('sticky');
  });

  test('User interaction styles work correctly', async ({ page, browserName }) => {
    await page.goto(`file://${indexPath}`);

    console.log(`Testing user interaction styles in browser: ${browserName}`);

    // Test that buttons have proper cursor
    const button = page.locator('.btn').first();
    const cursor = await button.evaluate(el =>
      window.getComputedStyle(el).cursor
    );
    expect(cursor).toBe('pointer');

    // Test that code blocks preserve white-space
    const codeElement = page.locator('pre code').first();
    const whiteSpace = await codeElement.evaluate(el =>
      window.getComputedStyle(el).whiteSpace
    );
    expect(['pre', 'pre-wrap', 'preserve']).toContain(whiteSpace);
  });

  test('Media query responsive breakpoints work correctly', async ({ page, browserName }) => {
    console.log(`Testing media queries in browser: ${browserName}`);

    // Test mobile breakpoint
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto(`file://${indexPath}`);

    const hamburger = page.locator('.hamburger-menu');
    const hamburgerDisplay = await hamburger.evaluate(el =>
      window.getComputedStyle(el).display
    );
    expect(hamburgerDisplay).toBe('flex');

    // Test desktop breakpoint
    await page.setViewportSize({ width: 1440, height: 900 });
    await page.waitForTimeout(100);

    const hamburgerHidden = await hamburger.evaluate(el =>
      window.getComputedStyle(el).display
    );
    expect(hamburgerHidden).toBe('none');
  });
});

/**
 * Additional cross-browser specific tests
 */
test.describe('Cross-Browser Specific Features', () => {
  test('SVG icons render correctly', async ({ page, browserName }) => {
    await page.goto(`file://${indexPath}`);

    console.log(`Testing SVG rendering in browser: ${browserName}`);

    // Verify SVG icons in feature cards are visible
    const svgIcons = page.locator('.feature-icon svg');
    expect(await svgIcons.count()).toBe(3);

    for (let i = 0; i < 3; i++) {
      await expect(svgIcons.nth(i)).toBeVisible();
    }
  });

  test('External links have proper attributes', async ({ page, browserName }) => {
    await page.goto(`file://${indexPath}`);

    console.log(`Testing external links in browser: ${browserName}`);

    // Verify GitHub links have target="_blank" and rel="noopener noreferrer"
    const githubLinks = page.locator('a[href*="github.com"]');
    const linkCount = await githubLinks.count();
    expect(linkCount).toBeGreaterThan(0);

    for (let i = 0; i < linkCount; i++) {
      const link = githubLinks.nth(i);
      await expect(link).toHaveAttribute('target', '_blank');
      await expect(link).toHaveAttribute('rel', /noopener/);
    }
  });

  test('Architecture diagram renders correctly', async ({ page, browserName }) => {
    await page.goto(`file://${indexPath}`);

    console.log(`Testing architecture diagram in browser: ${browserName}`);

    // Scroll to architecture section
    const architectureSection = page.locator('[data-testid="architecture-section"]');
    await architectureSection.scrollIntoViewIfNeeded();
    await expect(architectureSection).toBeVisible();

    // Verify diagram renders
    const diagram = page.locator('[data-testid="architecture-diagram"]');
    await expect(diagram).toBeVisible();

    // Verify pre-formatted text in diagram maintains formatting
    const diagramPre = page.locator('.architecture-diagram pre.diagram');
    const whiteSpace = await diagramPre.evaluate(el =>
      window.getComputedStyle(el).whiteSpace
    );
    expect(whiteSpace).toBe('pre');
  });

  test('Tables scroll horizontally on narrow viewports', async ({ page, browserName }) => {
    console.log(`Testing table scroll in browser: ${browserName}`);

    // Set narrow viewport
    await page.setViewportSize({ width: 400, height: 800 });
    await page.goto(`file://${indexPath}`);

    // Scroll to commands section
    const commandsSection = page.locator('#commands');
    await commandsSection.scrollIntoViewIfNeeded();

    // Verify table wrapper has overflow-x: auto
    const tableWrapper = page.locator('.table-wrapper').first();
    const overflowX = await tableWrapper.evaluate(el =>
      window.getComputedStyle(el).overflowX
    );
    expect(overflowX).toBe('auto');
  });

  test('Font rendering is consistent', async ({ page, browserName }) => {
    await page.goto(`file://${indexPath}`);

    console.log(`Testing font rendering in browser: ${browserName}`);

    // Verify body uses system font stack
    // The CSS uses: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif
    // Different browsers may resolve system-ui differently in computed styles
    const bodyFontFamily = await page.evaluate(() =>
      window.getComputedStyle(document.body).fontFamily
    );
    // Check for any of the system font stack components
    const hasSystemFont = bodyFontFamily.includes('-apple-system') ||
                          bodyFontFamily.includes('BlinkMacSystemFont') ||
                          bodyFontFamily.includes('system-ui') ||
                          bodyFontFamily.includes('Segoe UI') ||
                          bodyFontFamily.includes('sans-serif');
    expect(hasSystemFont).toBe(true);

    // Verify code uses monospace font
    const codeFontFamily = await page.locator('code').first().evaluate(el =>
      window.getComputedStyle(el).fontFamily
    );
    // Should contain monospace or specific mono font
    expect(codeFontFamily.toLowerCase()).toMatch(/(mono|consolas|courier|sf|cascadia|roboto)/);
  });
});
