// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Test Suite: Cross-Browser Compatibility - Safari
 *
 * Verifies page works correctly in Apple Safari (NFR-4)
 * Tests page rendering, link navigation, and CSS styling including webkit prefixes
 * Safari uses the WebKit engine, so we test with Playwright's webkit browser
 */

test.describe('Safari Browser Compatibility', () => {
  test.beforeEach(async ({ page, browserName }) => {
    // Confirm we're running in WebKit (Safari engine)
    expect(browserName).toBe('webkit');
    await page.goto('/');
  });

  /**
   * Test Case 1: Page renders correctly in Safari
   * Input: Load page in Safari latest
   * Expected: Page renders correctly without visual issues
   */
  test('should render page correctly in Safari without visual issues', async ({ page }) => {
    // Verify page has loaded with correct title
    await expect(page).toHaveTitle(/MirDB/);

    // Verify no JavaScript errors occurred during page load
    const errors = [];
    page.on('pageerror', error => errors.push(error.message));

    // Check that main structural elements are visible and properly rendered
    const header = page.locator('header');
    await expect(header).toBeVisible();

    const navbar = page.locator('.navbar');
    await expect(navbar).toBeVisible();

    const hero = page.locator('.hero, [data-testid="hero"]').first();
    await expect(hero).toBeVisible();

    const h1 = page.locator('h1');
    await expect(h1).toBeVisible();
    await expect(h1).toContainText('MirDB');

    // Verify tagline is visible
    const tagline = page.locator('.tagline');
    await expect(tagline).toBeVisible();
    await expect(tagline).toContainText('Persistent Key-Value Store');

    // Verify features section renders
    const featuresSection = page.locator('#features, .features').first();
    await expect(featuresSection).toBeVisible();

    // Verify all feature cards render
    const featureCards = page.locator('.feature-card');
    await expect(featureCards).toHaveCount(4);

    // Verify getting started section renders
    const gettingStartedSection = page.locator('#getting-started, .getting-started').first();
    await expect(gettingStartedSection).toBeVisible();

    // Verify commands section renders
    const commandsSection = page.locator('.commands');
    await expect(commandsSection).toBeVisible();

    // Verify footer renders
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Check no console errors occurred
    expect(errors).toHaveLength(0);
  });

  /**
   * Test Case 2: All links navigate correctly in Safari
   * Input: Test all links in Safari
   * Expected: All links navigate correctly
   */
  test('should have all links navigating correctly in Safari', async ({ page }) => {
    // Test internal anchor links (navigation)
    const featuresLink = page.locator('.nav-links a[href="#features"]');
    await expect(featuresLink).toBeVisible();
    await featuresLink.click();

    // Verify scrolled to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();

    // Navigate to getting started
    const gettingStartedLink = page.locator('.nav-links a[href="#getting-started"]');
    await expect(gettingStartedLink).toBeVisible();
    await gettingStartedLink.click();

    // Verify scrolled to getting started section
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeInViewport();

    // Test CTA button links
    const primaryCTA = page.locator('.cta-primary[href="#getting-started"]');
    await expect(primaryCTA).toBeVisible();
    const ctaHref = await primaryCTA.getAttribute('href');
    expect(ctaHref).toBe('#getting-started');

    // Test secondary CTA (GitHub link)
    const secondaryCTA = page.locator('.cta-secondary');
    await expect(secondaryCTA).toBeVisible();
    const secondaryHref = await secondaryCTA.getAttribute('href');
    expect(secondaryHref).toBe('https://github.com/yetone/mirdb');

    // Verify external links have correct href attributes
    const githubLinks = page.locator('a[href="https://github.com/yetone/mirdb"]');
    const githubCount = await githubLinks.count();
    expect(githubCount).toBeGreaterThan(0);

    // Verify all external links have proper attributes for security
    for (let i = 0; i < githubCount; i++) {
      const link = githubLinks.nth(i);
      const href = await link.getAttribute('href');
      expect(href).toBe('https://github.com/yetone/mirdb');

      const target = await link.getAttribute('target');
      expect(target).toBe('_blank');

      const rel = await link.getAttribute('rel');
      expect(rel).toContain('noopener');
    }

    // Verify docs link
    const docsLink = page.locator('.nav-links a[href="https://github.com/yetone/mirdb#readme"]');
    await expect(docsLink).toBeVisible();
    const docsHref = await docsLink.getAttribute('href');
    expect(docsHref).toBe('https://github.com/yetone/mirdb#readme');

    // Verify footer issue link
    const issueLink = page.locator('footer a[href="https://github.com/yetone/mirdb/issues"]');
    await expect(issueLink).toBeVisible();
    const issueHref = await issueLink.getAttribute('href');
    expect(issueHref).toBe('https://github.com/yetone/mirdb/issues');
  });

  /**
   * Test Case 3: CSS styles render correctly in Safari (including webkit prefixes)
   * Input: Check CSS rendering in Safari
   * Expected: All CSS styles render as expected, including webkit prefixes if needed
   */
  test('should render all CSS styles correctly in Safari including webkit prefixes', async ({ page }) => {
    // Verify CSS custom properties are applied (test computed styles)
    const h1 = page.locator('h1');
    const h1Color = await h1.evaluate(el => getComputedStyle(el).color);
    // Should be primary-color (#2563eb) which computes to rgb(37, 99, 235)
    expect(h1Color).toBe('rgb(37, 99, 235)');

    // Verify navigation styles
    const navbar = page.locator('.navbar');
    const navDisplay = await navbar.evaluate(el => getComputedStyle(el).display);
    expect(navDisplay).toBe('flex');

    const navPosition = await navbar.evaluate(el => getComputedStyle(el).position);
    expect(navPosition).toBe('sticky');

    // Verify hero section gradient background (Safari supports linear-gradient)
    const hero = page.locator('.hero');
    const heroBg = await hero.evaluate(el => getComputedStyle(el).backgroundImage);
    expect(heroBg).toContain('linear-gradient');

    // Verify hero section uses flexbox for centering
    const heroDisplay = await hero.evaluate(el => getComputedStyle(el).display);
    expect(heroDisplay).toBe('flex');

    // Verify CTA button styles
    const ctaPrimary = page.locator('.cta-primary').first();
    const ctaBgColor = await ctaPrimary.evaluate(el => getComputedStyle(el).backgroundColor);
    // Primary color (#2563eb) = rgb(37, 99, 235)
    expect(ctaBgColor).toBe('rgb(37, 99, 235)');

    const ctaBorderRadius = await ctaPrimary.evaluate(el => getComputedStyle(el).borderRadius);
    expect(ctaBorderRadius).toBe('8px'); // 0.5rem = 8px

    // Verify feature cards grid layout (CSS Grid is well-supported in Safari)
    const featureGrid = page.locator('.feature-grid');
    const gridDisplay = await featureGrid.evaluate(el => getComputedStyle(el).display);
    expect(gridDisplay).toBe('grid');

    // Verify feature cards have proper styling
    const featureCard = page.locator('.feature-card').first();
    const cardBgColor = await featureCard.evaluate(el => getComputedStyle(el).backgroundColor);
    // bg-light (#f3f4f6) = rgb(243, 244, 246)
    expect(cardBgColor).toBe('rgb(243, 244, 246)');

    const cardBorderRadius = await featureCard.evaluate(el => getComputedStyle(el).borderRadius);
    expect(cardBorderRadius).toBe('12px'); // 0.75rem = 12px

    // Verify code example dark background
    const codeExample = page.locator('.code-example').first();
    const codeBgColor = await codeExample.evaluate(el => getComputedStyle(el).backgroundColor);
    // code-bg (#1e293b) = rgb(30, 41, 59)
    expect(codeBgColor).toBe('rgb(30, 41, 59)');

    // Verify footer dark background
    const footer = page.locator('footer');
    const footerBgColor = await footer.evaluate(el => getComputedStyle(el).backgroundColor);
    expect(footerBgColor).toBe('rgb(30, 41, 59)');

    // Verify footer flex layout
    const footerContent = page.locator('.footer-content');
    const footerDisplay = await footerContent.evaluate(el => getComputedStyle(el).display);
    expect(footerDisplay).toBe('flex');

    // Verify command group code styling
    const commandCode = page.locator('.command-group code').first();
    const commandCodeColor = await commandCode.evaluate(el => getComputedStyle(el).color);
    // primary-color (#2563eb) = rgb(37, 99, 235)
    expect(commandCodeColor).toBe('rgb(37, 99, 235)');

    // Verify smooth scroll behavior on html element (Safari supports scroll-behavior)
    const htmlScrollBehavior = await page.evaluate(() =>
      getComputedStyle(document.documentElement).scrollBehavior
    );
    expect(htmlScrollBehavior).toBe('smooth');

    // Verify body font-family is applied (font stack from CSS)
    // Safari uses -apple-system as the preferred font
    const bodyFontFamily = await page.evaluate(() =>
      getComputedStyle(document.body).fontFamily
    );
    // Font stack: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, etc.
    expect(bodyFontFamily).toMatch(/(-apple-system|BlinkMacSystemFont|Segoe UI|Roboto|sans-serif)/i);

    // Verify box-sizing is correctly applied (critical for Safari compatibility)
    const boxSizing = await page.evaluate(() =>
      getComputedStyle(document.body).boxSizing
    );
    expect(boxSizing).toBe('border-box');

    // Test transition property (Safari webkit transition support)
    const ctaTransition = await ctaPrimary.evaluate(el => getComputedStyle(el).transition);
    // Should have a transition defined
    expect(ctaTransition).not.toBe('none');
    expect(ctaTransition.length).toBeGreaterThan(0);
  });
});
