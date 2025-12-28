// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Test Suite: Browser Compatibility - Safari
 * Scenario: Verify page renders correctly in Safari (last 2 versions per NFR-3)
 *
 * NFR-3: Must support modern browsers (Chrome, Firefox, Safari, Edge - last 2 versions)
 */

test.describe('Browser Compatibility - Safari', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  /**
   * Test Case 1: Page renders correctly in Safari without console errors
   * Input: Load page in Safari latest version
   * Expected: Page renders correctly without console errors
   */
  test('TC1: Page loads and renders correctly in Safari without console errors', async ({ page, browserName }) => {
    // Skip if not webkit (Safari)
    test.skip(browserName !== 'webkit', 'This test is specific to Safari/WebKit');

    // Track console errors
    const consoleErrors = [];
    page.on('console', msg => {
      if (msg.type() === 'error') {
        consoleErrors.push(msg.text());
      }
    });

    // Track page errors (uncaught exceptions)
    const pageErrors = [];
    page.on('pageerror', error => {
      pageErrors.push(error.message);
    });

    // Reload the page to capture any errors during load
    await page.reload();
    await page.waitForLoadState('networkidle');

    // Verify main structural elements are visible
    const header = page.locator('header');
    const main = page.locator('main');
    const footer = page.locator('footer');

    await expect(header).toBeVisible();
    await expect(main).toBeVisible();
    await expect(footer).toBeVisible();

    // Verify hero section renders
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Verify product name is displayed
    const productName = page.locator('#product-name');
    await expect(productName).toBeVisible();
    await expect(productName).toHaveText('MirDB');

    // Verify tagline is displayed
    const tagline = page.locator('#tagline');
    await expect(tagline).toBeVisible();

    // Verify value propositions section renders
    const valuePropsSection = page.locator('[data-testid="value-propositions-section"]');
    await expect(valuePropsSection).toBeVisible();

    // Verify features section renders
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Verify quick-start section renders
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    // Verify commands section renders
    const commandsSection = page.locator('#commands');
    await expect(commandsSection).toBeVisible();

    // Verify configuration section renders
    const configSection = page.locator('#configuration');
    await expect(configSection).toBeVisible();

    // Check for console errors (filter out known benign warnings if any)
    const criticalErrors = consoleErrors.filter(error =>
      !error.includes('favicon') && // Ignore missing favicon
      !error.includes('404') // Ignore 404 for non-critical resources
    );

    expect(criticalErrors).toHaveLength(0);
    expect(pageErrors).toHaveLength(0);
  });

  /**
   * Test Case 2: All interactive elements work correctly in Safari
   * Input: Test all interactive elements in Safari
   * Expected: All buttons, links, and navigation work correctly
   */
  test('TC2: All interactive elements work correctly in Safari', async ({ page, browserName }) => {
    // Skip if not webkit (Safari)
    test.skip(browserName !== 'webkit', 'This test is specific to Safari/WebKit');

    // Test navigation links are clickable
    const navLinks = page.locator('.nav-links a');
    const navLinksCount = await navLinks.count();
    expect(navLinksCount).toBeGreaterThan(0);

    // Verify Features link works
    const featuresLink = page.locator('.nav-links a[href="#features"]');
    await expect(featuresLink).toBeVisible();
    await featuresLink.click();

    // Wait for scroll animation
    await page.waitForTimeout(500);

    // Verify we scrolled to features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();

    // Navigate back to top
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Test CTA buttons
    const getStartedBtn = page.locator('#get-started-btn');
    await expect(getStartedBtn).toBeVisible();

    // Verify Get Started button navigates to quick-start
    await getStartedBtn.click();
    await page.waitForTimeout(500);

    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeInViewport();

    // Navigate back
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Test GitHub button (should have correct href)
    const githubBtn = page.locator('#github-btn');
    await expect(githubBtn).toBeVisible();
    await expect(githubBtn).toHaveAttribute('href', 'https://github.com/yetone/mirdb');
    await expect(githubBtn).toHaveAttribute('target', '_blank');

    // Test footer GitHub link
    const footerGithubLink = page.locator('[data-testid="footer-github-link"]');
    await expect(footerGithubLink).toBeVisible();
    await expect(footerGithubLink).toHaveAttribute('href', 'https://github.com/yetone/mirdb');

    // Test skip link accessibility
    const skipLink = page.locator('.skip-link');
    await expect(skipLink).toHaveAttribute('href', '#main');

    // Test that all anchor links work properly
    const internalLinks = page.locator('a[href^="#"]');
    const internalLinksCount = await internalLinks.count();

    for (let i = 0; i < Math.min(internalLinksCount, 5); i++) {
      const link = internalLinks.nth(i);
      const href = await link.getAttribute('href');

      // Skip empty hrefs or just '#'
      if (!href || href === '#') continue;

      const targetId = href.substring(1);
      const targetElement = page.locator(`#${targetId}`);

      // Verify target element exists
      const targetExists = await targetElement.count() > 0;
      if (targetExists) {
        await link.click({ force: true });
        await page.waitForTimeout(300);
        // Navigate back for next iteration
        await page.goto('/');
        await page.waitForLoadState('domcontentloaded');
      }
    }
  });

  /**
   * Additional Test: CSS rendering and styling in Safari
   * Verifies that CSS flexbox, grid, and custom properties work correctly
   */
  test('TC3: CSS features render correctly in Safari', async ({ page, browserName }) => {
    // Skip if not webkit (Safari)
    test.skip(browserName !== 'webkit', 'This test is specific to Safari/WebKit');

    // Test CSS Grid in value propositions
    const valuePropsGrid = page.locator('[data-testid="value-props-grid"]');
    await expect(valuePropsGrid).toBeVisible();

    const gridDisplay = await valuePropsGrid.evaluate(el =>
      window.getComputedStyle(el).display
    );
    expect(gridDisplay).toBe('grid');

    // Test CSS Flexbox in navigation
    const navContainer = page.locator('.nav-container');
    const navDisplay = await navContainer.evaluate(el =>
      window.getComputedStyle(el).display
    );
    expect(navDisplay).toBe('flex');

    // Test CSS custom properties (variables) are working
    const body = page.locator('body');
    const fontFamily = await body.evaluate(el =>
      window.getComputedStyle(el).fontFamily
    );
    // Should contain system fonts
    expect(fontFamily).toContain('system-ui');

    // Test that buttons have proper styling
    const primaryBtn = page.locator('.btn-primary').first();
    await expect(primaryBtn).toBeVisible();

    const btnBgColor = await primaryBtn.evaluate(el =>
      window.getComputedStyle(el).backgroundColor
    );
    // Should have a blue background (primary color)
    expect(btnBgColor).toMatch(/rgb\(37,\s*99,\s*235\)|#2563eb/i);

    // Test border-radius is applied
    const featureCard = page.locator('.feature-card').first();
    await featureCard.scrollIntoViewIfNeeded();

    const borderRadius = await featureCard.evaluate(el =>
      window.getComputedStyle(el).borderRadius
    );
    expect(borderRadius).toBe('8px');

    // Test smooth scroll behavior
    const htmlElement = page.locator('html');
    const scrollBehavior = await htmlElement.evaluate(el =>
      window.getComputedStyle(el).scrollBehavior
    );
    expect(scrollBehavior).toBe('smooth');
  });

  /**
   * Additional Test: Images and SVG rendering in Safari
   */
  test('TC4: Images and SVGs render correctly in Safari', async ({ page, browserName }) => {
    // Skip if not webkit (Safari)
    test.skip(browserName !== 'webkit', 'This test is specific to Safari/WebKit');

    // Test hero logo renders
    const heroLogo = page.locator('#hero-logo');
    await expect(heroLogo).toBeVisible();

    // Verify logo has proper dimensions
    const logoBox = await heroLogo.boundingBox();
    expect(logoBox).not.toBeNull();
    expect(logoBox.width).toBeGreaterThan(0);
    expect(logoBox.height).toBeGreaterThan(0);

    // Test navigation logo
    const navLogo = page.locator('.nav-logo');
    await expect(navLogo).toBeVisible();

    // Test SVG icons in value propositions render correctly
    const valuePropIcons = page.locator('.value-prop-icon svg');
    const iconCount = await valuePropIcons.count();
    expect(iconCount).toBe(3);

    // Verify each SVG icon is visible
    for (let i = 0; i < iconCount; i++) {
      const icon = valuePropIcons.nth(i);
      await expect(icon).toBeVisible();

      // Check SVG dimensions
      const iconBox = await icon.boundingBox();
      expect(iconBox).not.toBeNull();
      expect(iconBox.width).toBeGreaterThan(0);
      expect(iconBox.height).toBeGreaterThan(0);
    }
  });

  /**
   * Additional Test: Code blocks and syntax highlighting in Safari
   */
  test('TC5: Code blocks render correctly in Safari', async ({ page, browserName }) => {
    // Skip if not webkit (Safari)
    test.skip(browserName !== 'webkit', 'This test is specific to Safari/WebKit');

    // Navigate to quick-start section
    await page.locator('#quick-start').scrollIntoViewIfNeeded();

    // Get all code blocks
    const codeBlocks = page.locator('.code-block');
    const codeBlockCount = await codeBlocks.count();
    expect(codeBlockCount).toBeGreaterThan(0);

    // Verify first few code blocks render correctly
    for (let i = 0; i < Math.min(codeBlockCount, 3); i++) {
      const codeBlock = codeBlocks.nth(i);
      await expect(codeBlock).toBeVisible();

      // Check monospace font is applied
      const preElement = codeBlock.locator('pre');
      const codeElement = codeBlock.locator('code');

      await expect(preElement).toBeVisible();
      await expect(codeElement).toBeVisible();

      const fontFamily = await codeElement.evaluate(el =>
        window.getComputedStyle(el).fontFamily
      );
      // Should contain monospace fonts
      expect(fontFamily.toLowerCase()).toMatch(/mono|consolas|monaco/);

      // Verify code block has proper background
      const bgColor = await codeBlock.evaluate(el =>
        window.getComputedStyle(el).backgroundColor
      );
      // Should have dark background
      expect(bgColor).toMatch(/rgb\(30,\s*41,\s*59\)|#1e293b/i);
    }
  });

  /**
   * Additional Test: Form of layout doesn't break in Safari
   */
  test('TC6: Page layout maintains integrity in Safari', async ({ page, browserName }) => {
    // Skip if not webkit (Safari)
    test.skip(browserName !== 'webkit', 'This test is specific to Safari/WebKit');

    // Verify no horizontal overflow
    const scrollWidth = await page.evaluate(() => document.documentElement.scrollWidth);
    const clientWidth = await page.evaluate(() => document.documentElement.clientWidth);
    expect(scrollWidth).toBeLessThanOrEqual(clientWidth + 1);

    // Verify sections are properly stacked vertically
    const sections = ['#hero', '#value-propositions', '#features', '#quick-start', '#commands', '#configuration'];
    let previousBottom = 0;

    for (const sectionSelector of sections) {
      const section = page.locator(sectionSelector);
      const isVisible = await section.isVisible();

      if (isVisible) {
        const box = await section.boundingBox();
        if (box) {
          // Each section should start at or after the previous section ends
          expect(box.y).toBeGreaterThanOrEqual(previousBottom - 1);
          previousBottom = box.y + box.height;
        }
      }
    }

    // Verify footer is at the bottom
    const footer = page.locator('footer');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();
  });
});
