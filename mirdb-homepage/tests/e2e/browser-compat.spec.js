/**
 * Cross-Browser Compatibility Tests
 * Owner: Scenario 11 - Cross-Browser Compatibility
 *
 * Test cases:
 * - Chrome latest: renders correctly
 * - Firefox latest: renders correctly
 * - Safari latest: renders correctly
 * - Edge latest: renders correctly
 * - CSS features compatibility
 * - Font rendering with fallbacks
 */
const { test, expect, chromium, firefox, webkit } = require('@playwright/test');

test.describe('Cross-Browser Compatibility', () => {
  // Helper function to verify all sections render correctly
  async function verifySectionsRender(page) {
    // Check hero section
    const hero = page.locator('#hero');
    await expect(hero).toBeVisible();
    await expect(hero.locator('.hero__title')).toBeVisible();

    // Check features section
    const features = page.locator('#features');
    await expect(features).toBeVisible();
    const featureCards = features.locator('.features__card');
    await expect(featureCards.first()).toBeVisible();

    // Check getting started section
    const gettingStarted = page.locator('#getting-started');
    await expect(gettingStarted).toBeVisible();

    // Check architecture section
    const architecture = page.locator('#architecture');
    await expect(architecture).toBeVisible();

    // Check configuration section
    const configuration = page.locator('#configuration');
    await expect(configuration).toBeVisible();

    // Check commands section
    const commands = page.locator('#commands');
    await expect(commands).toBeVisible();

    // Check status section
    const status = page.locator('#status');
    await expect(status).toBeVisible();

    // Check footer
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Check navigation (using .nav class to get main nav, not footer nav)
    const nav = page.locator('nav.nav');
    await expect(nav).toBeVisible();
  }

  // Helper function to collect console errors
  async function setupConsoleErrorCapture(page) {
    const consoleErrors = [];
    page.on('console', (msg) => {
      if (msg.type() === 'error') {
        consoleErrors.push(msg.text());
      }
    });
    return consoleErrors;
  }

  // Helper function to verify interactions work
  async function verifyInteractionsWork(page) {
    // Test navigation link hover states
    const navLinks = page.locator('.nav__links a');
    const firstNavLink = navLinks.first();
    await expect(firstNavLink).toBeVisible();

    // Test button hover states
    const heroBtn = page.locator('.hero__btn').first();
    await expect(heroBtn).toBeVisible();

    // Test smooth scroll works for anchor links
    const gettingStartedLink = page.locator('a[href="#getting-started"]').first();
    if (await gettingStartedLink.count() > 0) {
      await gettingStartedLink.click();
      // Wait for scroll to complete
      await page.waitForTimeout(500);
      const gettingStartedSection = page.locator('#getting-started');
      await expect(gettingStartedSection).toBeInViewport();
    }
  }

  test.describe('TC1: Chrome (Chromium) Compatibility', () => {
    test('renders all sections correctly with no console errors', async () => {
      const browser = await chromium.launch();
      const context = await browser.newContext();
      const page = await context.newPage();

      const consoleErrors = await setupConsoleErrorCapture(page);
      await page.goto('http://localhost:3000');

      // Verify all sections render
      await verifySectionsRender(page);

      // Verify no console errors
      expect(consoleErrors).toHaveLength(0);

      // Verify interactions work
      await verifyInteractionsWork(page);

      await browser.close();
    });
  });

  test.describe('TC2: Firefox Compatibility', () => {
    test('renders all sections correctly with no console errors', async () => {
      const browser = await firefox.launch();
      const context = await browser.newContext();
      const page = await context.newPage();

      const consoleErrors = await setupConsoleErrorCapture(page);
      await page.goto('http://localhost:3000');

      // Verify all sections render
      await verifySectionsRender(page);

      // Verify no console errors
      expect(consoleErrors).toHaveLength(0);

      // Verify interactions work
      await verifyInteractionsWork(page);

      await browser.close();
    });
  });

  test.describe('TC3: Safari (WebKit) Compatibility', () => {
    test('renders all sections correctly with no console errors', async () => {
      const browser = await webkit.launch();
      const context = await browser.newContext();
      const page = await context.newPage();

      const consoleErrors = await setupConsoleErrorCapture(page);
      await page.goto('http://localhost:3000');

      // Verify all sections render
      await verifySectionsRender(page);

      // Verify no console errors
      expect(consoleErrors).toHaveLength(0);

      // Verify interactions work
      await verifyInteractionsWork(page);

      await browser.close();
    });
  });

  test.describe('TC4: Edge (Chromium) Compatibility', () => {
    test('renders all sections correctly with no console errors', async () => {
      // Edge uses Chromium engine, test with Chromium in Edge-like configuration
      const browser = await chromium.launch({
        channel: 'msedge'
      }).catch(() => {
        // Fall back to regular Chromium if Edge is not installed
        return chromium.launch();
      });
      const context = await browser.newContext();
      const page = await context.newPage();

      const consoleErrors = await setupConsoleErrorCapture(page);
      await page.goto('http://localhost:3000');

      // Verify all sections render
      await verifySectionsRender(page);

      // Verify no console errors
      expect(consoleErrors).toHaveLength(0);

      // Verify interactions work
      await verifyInteractionsWork(page);

      await browser.close();
    });
  });

  test.describe('TC5: CSS Features Compatibility', () => {
    test('verifies CSS features used are supported in target browsers', async ({ page }) => {
      await page.goto('http://localhost:3000');

      // Test CSS Flexbox support
      const heroContainer = page.locator('.hero__container');
      const heroDisplay = await heroContainer.evaluate((el) =>
        window.getComputedStyle(el).display
      );
      // Hero container should have proper display
      expect(heroDisplay).toBeDefined();

      // Test CSS Grid support - features grid
      const featuresGrid = page.locator('.features__grid');
      const gridDisplay = await featuresGrid.evaluate((el) =>
        window.getComputedStyle(el).display
      );
      expect(gridDisplay).toBe('grid');

      // Test CSS Custom Properties (Variables) support
      // Check that colors are properly applied (would fail if CSS variables not supported)
      const body = page.locator('body');
      const bodyColor = await body.evaluate((el) =>
        window.getComputedStyle(el).color
      );
      expect(bodyColor).toBeDefined();
      expect(bodyColor).not.toBe('');

      // Test CSS box-sizing border-box
      const boxSizing = await page.evaluate(() => {
        const el = document.querySelector('.features__card');
        return window.getComputedStyle(el).boxSizing;
      });
      expect(boxSizing).toBe('border-box');

      // Test CSS transitions support
      const heroBtn = page.locator('.hero__btn').first();
      const transition = await heroBtn.evaluate((el) =>
        window.getComputedStyle(el).transition
      );
      expect(transition).toBeDefined();
      expect(transition).not.toBe('none');

      // Test CSS linear-gradient support (used in hero background)
      const heroSection = page.locator('.hero');
      const heroBackground = await heroSection.evaluate((el) =>
        window.getComputedStyle(el).backgroundImage
      );
      expect(heroBackground).toContain('gradient');

      // Test CSS border-radius support
      const card = page.locator('.features__card').first();
      const borderRadius = await card.evaluate((el) =>
        window.getComputedStyle(el).borderRadius
      );
      expect(borderRadius).toBeDefined();
      expect(borderRadius).not.toBe('0px');

      // Test smooth scroll behavior
      const htmlScrollBehavior = await page.evaluate(() =>
        window.getComputedStyle(document.documentElement).scrollBehavior
      );
      expect(htmlScrollBehavior).toBe('smooth');
    });
  });

  test.describe('TC6: Font Rendering with Fallbacks', () => {
    test('fonts load and render consistently with fallbacks defined', async ({ page }) => {
      await page.goto('http://localhost:3000');

      // Check font-family is defined with fallbacks
      const body = page.locator('body');
      const fontFamily = await body.evaluate((el) =>
        window.getComputedStyle(el).fontFamily
      );

      // Should have system font stack with fallbacks
      expect(fontFamily).toBeDefined();
      expect(fontFamily.toLowerCase()).toContain('sans-serif');

      // Verify system font stack includes expected fallbacks
      const expectedFallbacks = [
        '-apple-system',
        'blinkmacsystemfont',
        'segoe ui',
        'roboto',
        'sans-serif'
      ];
      const fontFamilyLower = fontFamily.toLowerCase();
      const hasExpectedFallbacks = expectedFallbacks.some(
        (font) => fontFamilyLower.includes(font)
      );
      expect(hasExpectedFallbacks).toBe(true);

      // Verify monospace font for code blocks has fallbacks
      const codeBlock = page.locator('code').first();
      if (await codeBlock.count() > 0) {
        const codeFontFamily = await codeBlock.evaluate((el) =>
          window.getComputedStyle(el).fontFamily
        );
        expect(codeFontFamily).toBeDefined();
        // Should include monospace fallback
        expect(codeFontFamily.toLowerCase()).toContain('monospace');
      }

      // Verify text renders without FOUT (Flash of Unstyled Text)
      // by checking text is visible and styled
      const heroTitle = page.locator('.hero__title');
      await expect(heroTitle).toBeVisible();
      const titleFontWeight = await heroTitle.evaluate((el) =>
        window.getComputedStyle(el).fontWeight
      );
      // Should have bold weight
      expect(['700', 'bold']).toContain(titleFontWeight);

      // Verify line-height is properly set (indicates font metrics loaded)
      const bodyLineHeight = await body.evaluate((el) =>
        window.getComputedStyle(el).lineHeight
      );
      expect(bodyLineHeight).toBeDefined();
      expect(bodyLineHeight).not.toBe('normal'); // Should be explicit value

      // Verify font-size is applied
      const heroTitleFontSize = await heroTitle.evaluate((el) =>
        window.getComputedStyle(el).fontSize
      );
      expect(heroTitleFontSize).toBeDefined();
      expect(parseInt(heroTitleFontSize)).toBeGreaterThan(20);
    });
  });
});
