/**
 * Browser Compatibility E2E Tests
 * Owner: Scenario 16 - Browser Compatibility
 *
 * Tests for verifying the landing page works correctly across
 * different browsers (Chrome, Firefox, Safari/WebKit, Edge).
 *
 * Note: Playwright uses Chromium for Chrome/Edge and WebKit for Safari.
 * These tests verify cross-browser compatibility for core functionality.
 */
const { test, expect } = require('@playwright/test');

test.describe('Browser Compatibility - Core Functionality', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Page loads correctly with all main sections visible', async ({ page, browserName }) => {
    // Log which browser we're testing
    console.log(`Testing in browser: ${browserName}`);

    // Verify page title
    await expect(page).toHaveTitle(/MirDB/);

    // Verify all main sections are present and visible
    const hero = page.locator('#hero');
    await expect(hero).toBeVisible();

    const features = page.locator('#features');
    await expect(features).toBeVisible();

    const usage = page.locator('#usage');
    await expect(usage).toBeVisible();

    const architecture = page.locator('#architecture');
    await expect(architecture).toBeVisible();

    const gettingStarted = page.locator('#getting-started');
    await expect(gettingStarted).toBeVisible();

    const footer = page.locator('footer');
    await expect(footer).toBeVisible();
  });

  test('TC2: Navigation works correctly', async ({ page, browserName }) => {
    console.log(`Testing navigation in browser: ${browserName}`);

    // Verify navigation bar is visible
    const nav = page.locator('.main-nav');
    await expect(nav).toBeVisible();

    // Get viewport width to determine if mobile
    const viewport = page.viewportSize();
    const isMobile = viewport && viewport.width < 768;

    if (isMobile) {
      // On mobile, open hamburger menu first
      const navToggle = page.locator('.nav-toggle');
      await expect(navToggle).toBeVisible();
      await navToggle.click();
      await page.waitForTimeout(300); // Wait for menu animation

      // Verify menu is open
      const navMenu = page.locator('.nav-menu');
      await expect(navMenu).toHaveClass(/is-open/);
    }

    // Verify nav links are present
    const navLinks = page.locator('.nav-links a');
    await expect(navLinks).toHaveCount(4);

    // Click Features link and verify smooth scroll
    await page.locator('.nav-link[href="#features"]').click();
    await page.waitForTimeout(500); // Wait for smooth scroll

    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();
  });

  test('TC3: CSS styles render correctly', async ({ page, browserName }) => {
    console.log(`Testing CSS styles in browser: ${browserName}`);

    // Verify body background color (dark theme)
    const body = page.locator('body');
    const bgColor = await body.evaluate(el => getComputedStyle(el).backgroundColor);
    // #1a1a2e = rgb(26, 26, 46)
    expect(bgColor).toBe('rgb(26, 26, 46)');

    // Verify text color
    const textColor = await body.evaluate(el => getComputedStyle(el).color);
    // #eaeaea = rgb(234, 234, 234)
    expect(textColor).toBe('rgb(234, 234, 234)');

    // Verify hero title is styled correctly
    const heroTitle = page.locator('.hero-title');
    await expect(heroTitle).toBeVisible();
    const fontSize = await heroTitle.evaluate(el => getComputedStyle(el).fontSize);
    // Font size should be set (3rem = 48px at default browser settings)
    expect(parseFloat(fontSize)).toBeGreaterThan(30);
  });

  test('TC4: Interactive elements work', async ({ page, browserName }) => {
    console.log(`Testing interactive elements in browser: ${browserName}`);

    // Verify CTA buttons are clickable
    const primaryBtn = page.locator('.hero-cta .btn-primary');
    await expect(primaryBtn).toBeVisible();
    await expect(primaryBtn).toHaveCSS('cursor', 'pointer');

    // Verify hover states work (transform on hover)
    await primaryBtn.hover();
    await page.waitForTimeout(200); // Wait for transition

    // Verify feature cards have proper styling
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBe(6);
  });

  test('TC5: Images and assets load correctly', async ({ page, browserName }) => {
    console.log(`Testing assets in browser: ${browserName}`);

    // Verify logo image loads
    const heroLogo = page.locator('.hero-logo');
    await expect(heroLogo).toBeVisible();

    // Check that the image has loaded (naturalWidth > 0)
    const logoLoaded = await heroLogo.evaluate(img => {
      return img.complete && img.naturalWidth > 0;
    });
    expect(logoLoaded).toBe(true);

    // Verify usage GIF loads
    const usageGif = page.locator('.usage__gif');
    await usageGif.scrollIntoViewIfNeeded();
    await expect(usageGif).toBeVisible();
  });

  test('TC6: Flexbox and Grid layouts work', async ({ page, browserName }) => {
    console.log(`Testing layouts in browser: ${browserName}`);

    // Verify features grid uses CSS Grid
    const featuresGrid = page.locator('.features__grid');
    const display = await featuresGrid.evaluate(el => getComputedStyle(el).display);
    expect(display).toBe('grid');

    // Verify navigation uses flexbox
    const navContainer = page.locator('.nav-container');
    const navDisplay = await navContainer.evaluate(el => getComputedStyle(el).display);
    expect(navDisplay).toBe('flex');

    // Verify hero CTA uses flexbox
    const heroCta = page.locator('.hero-cta');
    const ctaDisplay = await heroCta.evaluate(el => getComputedStyle(el).display);
    expect(ctaDisplay).toBe('flex');
  });

  test('TC7: CSS custom properties (variables) work', async ({ page, browserName }) => {
    console.log(`Testing CSS variables in browser: ${browserName}`);

    // Verify CSS custom properties are applied
    const root = page.locator(':root');

    // Get computed style for a color variable
    const accentColor = await page.evaluate(() => {
      return getComputedStyle(document.documentElement).getPropertyValue('--color-accent-coral').trim();
    });
    expect(accentColor).toBe('#e94560');

    // Verify variable is used in computed style
    const primaryBtn = page.locator('.btn-primary').first();
    const btnBgColor = await primaryBtn.evaluate(el => getComputedStyle(el).backgroundColor);
    // #e94560 = rgb(233, 69, 96)
    expect(btnBgColor).toBe('rgb(233, 69, 96)');
  });

  test('TC8: Fonts load correctly', async ({ page, browserName }) => {
    console.log(`Testing fonts in browser: ${browserName}`);

    // Verify font-family is applied
    const heroTitle = page.locator('.hero-title');
    const fontFamily = await heroTitle.evaluate(el => getComputedStyle(el).fontFamily);

    // Should include Inter or fallback system fonts
    expect(fontFamily).toMatch(/Inter|system-ui|-apple-system|BlinkMacSystemFont|Segoe UI|Roboto|sans-serif/i);
  });

  test('TC9: Scroll behavior works', async ({ page, browserName }) => {
    console.log(`Testing scroll behavior in browser: ${browserName}`);

    // Verify smooth scroll CSS is applied
    const html = page.locator('html');
    const scrollBehavior = await html.evaluate(el => getComputedStyle(el).scrollBehavior);
    expect(scrollBehavior).toBe('smooth');

    // Get viewport width to determine if mobile
    const viewport = page.viewportSize();
    const isMobile = viewport && viewport.width < 768;

    if (isMobile) {
      // On mobile, open hamburger menu first
      const navToggle = page.locator('.nav-toggle');
      await expect(navToggle).toBeVisible();
      await navToggle.click();
      await page.waitForTimeout(300); // Wait for menu animation
    }

    // Test actual scrolling
    await page.locator('.nav-link[href="#architecture"]').click();
    await page.waitForTimeout(800); // Wait for scroll

    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeInViewport();
  });

  test('TC10: Transitions and animations work', async ({ page, browserName }) => {
    console.log(`Testing transitions in browser: ${browserName}`);

    // Verify button has transition property
    const primaryBtn = page.locator('.btn-primary').first();
    const transition = await primaryBtn.evaluate(el => getComputedStyle(el).transition);

    // Should have transition defined
    expect(transition).not.toBe('none');
    expect(transition.length).toBeGreaterThan(0);
  });
});

test.describe('Browser Compatibility - Visual Consistency', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Hero section renders consistently', async ({ page, browserName }) => {
    console.log(`Testing hero visual consistency in: ${browserName}`);

    const hero = page.locator('#hero');
    await expect(hero).toBeVisible();

    // Verify hero content is centered
    const heroContent = page.locator('.hero-content');
    const textAlign = await heroContent.evaluate(el => getComputedStyle(el).textAlign);
    expect(textAlign).toBe('center');

    // Verify proper spacing
    const paddingTop = await hero.evaluate(el => getComputedStyle(el).paddingTop);
    expect(parseFloat(paddingTop)).toBeGreaterThan(0);
  });

  test('Feature cards render consistently', async ({ page, browserName }) => {
    console.log(`Testing feature cards in: ${browserName}`);

    const featureCard = page.locator('.feature-card').first();
    await expect(featureCard).toBeVisible();

    // Verify card has background color
    const bgColor = await featureCard.evaluate(el => getComputedStyle(el).backgroundColor);
    expect(bgColor).not.toBe('rgba(0, 0, 0, 0)');

    // Verify border radius is applied
    const borderRadius = await featureCard.evaluate(el => getComputedStyle(el).borderRadius);
    expect(parseFloat(borderRadius)).toBeGreaterThan(0);
  });

  test('Code blocks render consistently', async ({ page, browserName }) => {
    console.log(`Testing code blocks in: ${browserName}`);

    const codeBlock = page.locator('.code-block').first();
    await codeBlock.scrollIntoViewIfNeeded();
    await expect(codeBlock).toBeVisible();

    // Verify code block has proper styling
    const pre = codeBlock.locator('pre');
    const fontFamily = await pre.evaluate(el => getComputedStyle(el).fontFamily);

    // Should use monospace font
    expect(fontFamily).toMatch(/JetBrains Mono|Fira Code|Monaco|Consolas|monospace/i);
  });

  test('Footer renders consistently', async ({ page, browserName }) => {
    console.log(`Testing footer in: ${browserName}`);

    const footer = page.locator('footer');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Verify footer links are visible
    const footerLinks = page.locator('.footer__link');
    const count = await footerLinks.count();
    expect(count).toBeGreaterThan(0);
  });
});

test.describe('Browser Compatibility - Responsive Behavior', () => {
  test('Page is responsive at different viewports', async ({ page, browserName }) => {
    console.log(`Testing responsive behavior in: ${browserName}`);

    // Test at desktop viewport
    await page.setViewportSize({ width: 1200, height: 800 });
    await page.goto('/');

    const featuresGrid = page.locator('.features__grid');
    await expect(featuresGrid).toBeVisible();

    // Test at tablet viewport
    await page.setViewportSize({ width: 768, height: 1024 });
    await page.waitForTimeout(300); // Wait for CSS to update
    await expect(featuresGrid).toBeVisible();

    // Test at mobile viewport
    await page.setViewportSize({ width: 375, height: 667 });
    await page.waitForTimeout(300);
    await expect(featuresGrid).toBeVisible();
  });
});
