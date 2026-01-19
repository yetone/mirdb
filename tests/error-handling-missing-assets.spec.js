// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * E2E tests for Error Handling - Missing Assets
 * Scenario: Verify graceful handling when assets fail to load
 *
 * Tests behavior when images or CSS fail to load, ensuring the page
 * remains usable and content remains accessible.
 */

test.describe('Error Handling - Missing Assets', () => {

  /**
   * Test Case 1: Block image loading and reload page
   * Input: Block image loading and reload page
   * Expected: Page content remains readable, alt text displayed for images
   */
  test('TC1: Page remains readable when images fail to load - alt text displayed', async ({ page }) => {
    // Block all image requests before navigating
    await page.route('**/*.gif', route => route.abort());
    await page.route('**/*.png', route => route.abort());
    await page.route('**/*.jpg', route => route.abort());
    await page.route('**/*.jpeg', route => route.abort());
    await page.route('**/*.webp', route => route.abort());
    await page.route('**/*.svg', route => route.abort());

    // Navigate to the page with images blocked
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Verify the main content is still readable
    // Check that the main heading (h1) is visible and contains expected text
    const mainHeading = page.locator('h1');
    await expect(mainHeading).toBeVisible();
    await expect(mainHeading).toHaveText('MirDB');

    // Check that the tagline is visible
    const tagline = page.locator('[data-testid="hero-tagline"]');
    await expect(tagline).toBeVisible();
    await expect(tagline).toContainText('Persistent Key-Value Store');

    // Check that the description is visible
    const description = page.locator('[data-testid="hero-description"]');
    await expect(description).toBeVisible();

    // Verify alt text is present for images (critical for accessibility when images fail)
    const heroLogo = page.locator('[data-testid="hero-logo"]');
    const heroLogoAlt = await heroLogo.getAttribute('alt');
    expect(heroLogoAlt).toBeTruthy();
    expect(heroLogoAlt.toLowerCase()).toContain('logo');

    // Check usage demo image alt text
    const usageDemo = page.locator('[data-testid="usage-demo"]');
    const usageDemoAlt = await usageDemo.getAttribute('alt');
    expect(usageDemoAlt).toBeTruthy();
    expect(usageDemoAlt.toLowerCase()).toMatch(/demo|usage|mirdb/i);

    // Verify the page navigation is still functional
    const primaryCta = page.locator('[data-testid="primary-cta"]');
    await expect(primaryCta).toBeVisible();
    await expect(primaryCta).toHaveAttribute('href', 'https://github.com/yetone/mirdb');

    const secondaryCta = page.locator('[data-testid="secondary-cta"]');
    await expect(secondaryCta).toBeVisible();

    // Verify core sections are visible and readable
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();
    await featuresSection.scrollIntoViewIfNeeded();

    // Check feature cards are displayed
    const featureCards = page.locator('.feature-card');
    await expect(featureCards.first()).toBeVisible();
    const featureCount = await featureCards.count();
    expect(featureCount).toBeGreaterThan(0);

    // Verify architecture section is visible
    const architectureSection = page.locator('#architecture');
    await expect(architectureSection).toBeVisible();

    // Verify getting-started section is visible
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeVisible();

    // Verify footer is visible
    const footer = page.locator('[data-testid="footer-section"]');
    await expect(footer).toBeVisible();

    // Verify code examples are still displayed
    const codeBlocks = page.locator('pre code');
    const codeBlockCount = await codeBlocks.count();
    expect(codeBlockCount).toBeGreaterThan(0);
  });

  /**
   * Test Case 1 (continued): Verify specific alt text content
   */
  test('TC1b: All images have meaningful alt text for screen readers', async ({ page }) => {
    // Block all image requests
    await page.route('**/*.gif', route => route.abort());
    await page.route('**/*.png', route => route.abort());
    await page.route('**/*.jpg', route => route.abort());

    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Get all images on the page
    const images = page.locator('img');
    const imageCount = await images.count();

    // Verify each image has alt text
    for (let i = 0; i < imageCount; i++) {
      const img = images.nth(i);
      const alt = await img.getAttribute('alt');
      expect(alt, `Image ${i + 1} should have alt text`).toBeTruthy();
      // Alt text should be more than just empty or whitespace
      expect(alt.trim().length, `Image ${i + 1} alt text should not be empty`).toBeGreaterThan(0);
    }
  });

  /**
   * Test Case 2: Block CSS loading
   * Input: Block CSS loading
   * Expected: Page content remains accessible in unstyled state (semantic HTML)
   */
  test('TC2: Page content remains accessible when CSS fails to load', async ({ page }) => {
    // Block CSS requests before navigating
    await page.route('**/*.css', route => route.abort());

    // Navigate to the page with CSS blocked
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Verify the page still renders and content is accessible
    // Check main heading
    const mainHeading = page.locator('h1');
    await expect(mainHeading).toBeVisible();
    const headingText = await mainHeading.textContent();
    expect(headingText).toBe('MirDB');

    // Verify semantic HTML structure - check for proper heading hierarchy
    const h1Count = await page.locator('h1').count();
    expect(h1Count).toBeGreaterThan(0);

    const h2Count = await page.locator('h2').count();
    expect(h2Count).toBeGreaterThan(0);

    const h3Count = await page.locator('h3').count();
    expect(h3Count).toBeGreaterThan(0);

    // Verify main content area exists (semantic HTML)
    const mainContent = page.locator('main, #main-content');
    await expect(mainContent).toBeVisible();

    // Verify navigation exists (semantic HTML)
    const nav = page.locator('nav');
    await expect(nav).toBeVisible();

    // Verify footer exists (semantic HTML)
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Verify all text content is still readable
    const bodyText = await page.locator('body').textContent();
    expect(bodyText).toContain('MirDB');
    expect(bodyText).toContain('Persistent Key-Value Store');
    expect(bodyText).toContain('Memcached');
    expect(bodyText).toContain('GitHub');

    // Verify links are functional
    const links = page.locator('a');
    const linkCount = await links.count();
    expect(linkCount).toBeGreaterThan(0);

    // Verify the primary CTA link is present and has correct href
    const primaryCta = page.locator('[data-testid="primary-cta"]');
    await expect(primaryCta).toBeVisible();
    const href = await primaryCta.getAttribute('href');
    expect(href).toContain('github.com');

    // Verify code blocks are visible (content remains accessible)
    const preBlocks = page.locator('pre');
    const preCount = await preBlocks.count();
    expect(preCount).toBeGreaterThan(0);

    // Verify lists are present (semantic HTML for features, etc.)
    const listItems = page.locator('li');
    const listItemCount = await listItems.count();
    expect(listItemCount).toBeGreaterThan(0);

    // Verify buttons are accessible
    const buttons = page.locator('button, [role="button"], .btn');
    const buttonCount = await buttons.count();
    expect(buttonCount).toBeGreaterThan(0);
  });

  /**
   * Test Case 2 (continued): Verify keyboard navigation works without CSS
   */
  test('TC2b: Keyboard navigation remains functional without CSS', async ({ page }) => {
    // Block CSS requests
    await page.route('**/*.css', route => route.abort());

    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Test keyboard navigation - use Tab to navigate through focusable elements
    // First, get the skip link
    const skipLink = page.locator('.skip-link');

    // Press Tab to start keyboard navigation
    await page.keyboard.press('Tab');

    // Verify we can reach interactive elements via keyboard
    const focusableElements = page.locator('a, button, [tabindex]:not([tabindex="-1"])');
    const focusableCount = await focusableElements.count();
    expect(focusableCount).toBeGreaterThan(5); // Should have multiple focusable elements

    // Navigate to the GitHub link and verify it's reachable
    let foundGitHubLink = false;
    for (let i = 0; i < Math.min(focusableCount, 15); i++) {
      const focused = page.locator(':focus');
      const tagName = await focused.evaluate(el => el.tagName.toLowerCase()).catch(() => '');
      const href = await focused.getAttribute('href').catch(() => null);

      if (href && href.includes('github.com')) {
        foundGitHubLink = true;
        break;
      }
      await page.keyboard.press('Tab');
    }

    expect(foundGitHubLink).toBe(true);
  });

  /**
   * Test Case 2 (continued): Verify section anchors work without CSS
   */
  test('TC2c: Section anchor links work without CSS', async ({ page }) => {
    // Block CSS requests
    await page.route('**/*.css', route => route.abort());

    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Verify anchor links to sections work
    const featuresLink = page.locator('a[href="#features"]');
    if (await featuresLink.count() > 0) {
      await featuresLink.first().click();

      // Wait a moment for scroll to complete
      await page.waitForTimeout(500);

      // Verify the URL hash changed
      const url = page.url();
      expect(url).toContain('#features');

      // Verify the features section is now visible in the viewport
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeVisible();
    }

    // Test navigation to getting-started section
    const getStartedLink = page.locator('a[href="#getting-started"]');
    if (await getStartedLink.count() > 0) {
      await getStartedLink.first().click();
      await page.waitForTimeout(500);

      const url = page.url();
      expect(url).toContain('#getting-started');

      const gettingStartedSection = page.locator('#getting-started');
      await expect(gettingStartedSection).toBeVisible();
    }
  });

  /**
   * Additional test: Verify graceful degradation with both images AND CSS blocked
   */
  test('Page remains functional with both images and CSS blocked', async ({ page }) => {
    // Block both images and CSS
    await page.route('**/*.gif', route => route.abort());
    await page.route('**/*.png', route => route.abort());
    await page.route('**/*.jpg', route => route.abort());
    await page.route('**/*.css', route => route.abort());

    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Verify core content is still accessible
    const mainHeading = page.locator('h1');
    await expect(mainHeading).toBeVisible();
    await expect(mainHeading).toHaveText('MirDB');

    // Verify semantic structure
    const main = page.locator('main');
    await expect(main).toBeVisible();

    const nav = page.locator('nav');
    await expect(nav).toBeVisible();

    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Verify all major sections are present
    const sections = ['#value-proposition', '#features', '#architecture', '#getting-started'];
    for (const sectionId of sections) {
      const section = page.locator(sectionId);
      await expect(section).toBeVisible();
    }

    // Verify text content is readable
    const pageText = await page.locator('body').textContent();
    expect(pageText).toContain('MirDB');
    expect(pageText).toContain('Persistence');
    expect(pageText).toContain('Compatibility');
    expect(pageText).toContain('Performance');
    expect(pageText).toContain('Getting Started');
  });

  /**
   * Additional test: Verify form/interactive elements work without styling
   */
  test('Interactive elements remain functional without CSS', async ({ page }) => {
    // Block CSS
    await page.route('**/*.css', route => route.abort());

    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Verify the back-to-top button exists and has proper aria attributes
    const backToTop = page.locator('[data-testid="back-to-top"]');
    await expect(backToTop).toBeAttached();

    const ariaLabel = await backToTop.getAttribute('aria-label');
    expect(ariaLabel).toBeTruthy();
    expect(ariaLabel.toLowerCase()).toContain('top');

    // Verify external links have proper attributes
    const externalLinks = page.locator('a[target="_blank"]');
    const externalLinkCount = await externalLinks.count();

    for (let i = 0; i < externalLinkCount; i++) {
      const link = externalLinks.nth(i);
      const rel = await link.getAttribute('rel');
      // External links should have noopener for security
      expect(rel).toContain('noopener');
    }
  });

  /**
   * Additional test: Verify page doesn't throw JavaScript errors when assets fail
   */
  test('No JavaScript errors when assets fail to load', async ({ page }) => {
    const jsErrors = [];

    // Listen for page errors
    page.on('pageerror', error => {
      jsErrors.push(error.message);
    });

    // Block both images and CSS
    await page.route('**/*.gif', route => route.abort());
    await page.route('**/*.png', route => route.abort());
    await page.route('**/*.css', route => route.abort());

    await page.goto('/');
    await page.waitForLoadState('networkidle');

    // Scroll through the page to trigger any lazy-loaded content
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
    await page.waitForTimeout(500);
    await page.evaluate(() => window.scrollTo(0, 0));
    await page.waitForTimeout(500);

    // Verify no JavaScript errors occurred
    expect(jsErrors, 'Page should not have JavaScript errors when assets fail').toHaveLength(0);
  });

});
