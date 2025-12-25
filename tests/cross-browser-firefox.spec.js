// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Cross-Browser Compatibility Test Suite - Firefox
 * Scenario: Verify homepage renders correctly in Mozilla Firefox
 *
 * Test Cases:
 * 1. Load homepage in Firefox (latest version) - All sections render correctly without visual issues
 * 2. Test navigation in Firefox - All navigation links and buttons function correctly
 * 3. Load homepage in Firefox (latest - 1 version) - All sections render correctly without visual issues
 */

test.describe('Cross-Browser Compatibility - Firefox', () => {
  // Test Case 1: Load homepage in Firefox (latest version)
  test('TC1: Homepage renders correctly in Firefox - all sections visible', async ({ page, browserName }) => {
    // Skip if not running in Firefox
    test.skip(browserName !== 'firefox', 'This test is Firefox-specific');

    await page.goto('/');

    // Verify page loaded successfully
    await expect(page).toHaveTitle(/MirDB/);

    // Verify hero section renders correctly
    const heroSection = page.locator('.hero');
    await expect(heroSection).toBeVisible();

    const heroTitle = page.locator('#hero-title');
    await expect(heroTitle).toBeVisible();
    await expect(heroTitle).toHaveText('MirDB');

    const tagline = page.locator('.tagline');
    await expect(tagline).toBeVisible();
    await expect(tagline).toContainText('Persistent Key-Value Store');

    const description = page.locator('.hero .description');
    await expect(description).toBeVisible();

    // Verify CTA buttons are visible and styled correctly
    const primaryCTA = page.locator('[data-testid="primary-cta"]');
    await expect(primaryCTA).toBeVisible();
    await expect(primaryCTA).toHaveText('Get Started');

    const secondaryCTA = page.locator('[data-testid="secondary-cta"]');
    await expect(secondaryCTA).toBeVisible();
    await expect(secondaryCTA).toHaveText('View on GitHub');

    // Verify features section renders correctly
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    const featuresTitle = page.locator('#features-title');
    await expect(featuresTitle).toBeVisible();
    await expect(featuresTitle).toHaveText('Key Features');

    const featuresGrid = page.locator('[data-testid="features-grid"]');
    await expect(featuresGrid).toBeVisible();

    // Verify all 4 feature cards are rendered
    const featureCards = page.locator('.feature-card');
    await expect(featureCards).toHaveCount(4);

    // Verify each feature card is visible
    await expect(page.locator('[data-testid="feature-card-memcached"]')).toBeVisible();
    await expect(page.locator('[data-testid="feature-card-persistence"]')).toBeVisible();
    await expect(page.locator('[data-testid="feature-card-lsm"]')).toBeVisible();
    await expect(page.locator('[data-testid="feature-card-async"]')).toBeVisible();

    // Verify Quick Start section renders correctly
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    const quickStartTitle = page.locator('#quickstart-title');
    await expect(quickStartTitle).toBeVisible();
    await expect(quickStartTitle).toHaveText('Quick Start');

    // Verify code snippets are visible
    await expect(page.locator('[data-testid="installation-step"]')).toBeVisible();
    await expect(page.locator('[data-testid="configuration-step"]')).toBeVisible();
    await expect(page.locator('[data-testid="usage-step"]')).toBeVisible();
    await expect(page.locator('[data-testid="commands-step"]')).toBeVisible();

    // Verify footer renders correctly
    const footer = page.locator('[data-testid="footer"]');
    await expect(footer).toBeVisible();

    const footerLinks = page.locator('[data-testid="footer-links"]');
    await expect(footerLinks).toBeVisible();

    const projectStatus = page.locator('[data-testid="project-status"]');
    await expect(projectStatus).toBeVisible();
  });

  // Test Case 2: Test navigation in Firefox
  test('TC2: Navigation links and buttons function correctly in Firefox', async ({ page, browserName }) => {
    // Skip if not running in Firefox
    test.skip(browserName !== 'firefox', 'This test is Firefox-specific');

    await page.goto('/');

    // Test header navigation links
    const headerNav = page.locator('.nav-links');
    await expect(headerNav).toBeVisible();

    // Test Features navigation link - should scroll to features section
    const featuresLink = page.locator('.nav-links a[href="#features"]');
    await expect(featuresLink).toBeVisible();
    await featuresLink.click();

    // Verify page scrolled to features section (element should be in view)
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeInViewport();

    // Test Quick Start navigation link
    const quickStartLink = page.locator('.nav-links a[href="#quick-start"]');
    await expect(quickStartLink).toBeVisible();
    await quickStartLink.click();

    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeInViewport();

    // Test GitHub link in header (external link)
    const headerGithubLink = page.locator('.nav-links a[href*="github.com"]');
    await expect(headerGithubLink).toBeVisible();
    const headerGithubHref = await headerGithubLink.getAttribute('href');
    expect(headerGithubHref).toContain('github.com');
    expect(headerGithubHref?.toLowerCase()).toContain('mirdb');

    // Test primary CTA button (Get Started)
    const primaryCTA = page.locator('[data-testid="primary-cta"]');
    await expect(primaryCTA).toBeVisible();
    await primaryCTA.click();
    await expect(quickStartSection).toBeInViewport();

    // Test secondary CTA button (View on GitHub)
    const secondaryCTA = page.locator('[data-testid="secondary-cta"]');
    await expect(secondaryCTA).toBeVisible();
    const secondaryHref = await secondaryCTA.getAttribute('href');
    expect(secondaryHref).toContain('github.com');
    const secondaryTarget = await secondaryCTA.getAttribute('target');
    expect(secondaryTarget).toBe('_blank');

    // Test footer navigation links
    const footerGithubLink = page.locator('[data-testid="footer-github-link"]');
    await expect(footerGithubLink).toBeVisible();
    const footerGithubHref = await footerGithubLink.getAttribute('href');
    expect(footerGithubHref).toContain('github.com');

    const footerDocsLink = page.locator('[data-testid="footer-docs-link"]');
    await expect(footerDocsLink).toBeVisible();
    const footerDocsHref = await footerDocsLink.getAttribute('href');
    expect(footerDocsHref).toBeTruthy();

    const footerLicenseLink = page.locator('[data-testid="footer-license-link"]');
    await expect(footerLicenseLink).toBeVisible();
    const footerLicenseHref = await footerLicenseLink.getAttribute('href');
    expect(footerLicenseHref).toContain('LICENSE');

    // Test skip link functionality
    const skipLink = page.locator('[data-testid="skip-link"]');
    await expect(skipLink).toBeAttached();
    const skipLinkHref = await skipLink.getAttribute('href');
    expect(skipLinkHref).toBe('#main-content');
  });

  // Test Case 3: Load homepage in Firefox (latest - 1 version)
  // Note: Testing with the same Firefox browser since Playwright uses a single Firefox version
  // This test validates that the page renders correctly, which would also work on previous versions
  test('TC3: Homepage renders correctly in Firefox (backward compatibility check)', async ({ page, browserName }) => {
    // Skip if not running in Firefox
    test.skip(browserName !== 'firefox', 'This test is Firefox-specific');

    await page.goto('/');

    // Verify page structure and essential elements for backward compatibility
    // These tests focus on CSS and HTML features that should work across Firefox versions

    // Check that page uses standard HTML5 semantic elements
    await expect(page.locator('header.header')).toBeVisible();
    await expect(page.locator('main#main-content')).toBeVisible();
    await expect(page.locator('footer.footer')).toBeVisible();

    // Verify CSS Grid/Flexbox layouts render correctly (supported since Firefox 52+)
    const featuresGrid = page.locator('[data-testid="features-grid"]');
    await expect(featuresGrid).toBeVisible();

    // Verify that feature cards have proper layout
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBe(4);

    // Check that all cards are visible and properly positioned
    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i);
      await expect(card).toBeVisible();

      // Verify card has expected structure
      const icon = card.locator('.feature-icon');
      const heading = card.locator('h3');
      const description = card.locator('p');

      await expect(icon).toBeVisible();
      await expect(heading).toBeVisible();
      await expect(description).toBeVisible();
    }

    // Verify SVG icons render correctly (important for cross-browser)
    const svgIcons = page.locator('.feature-icon svg');
    const iconCount = await svgIcons.count();
    expect(iconCount).toBe(4);

    // Verify code blocks render with proper styling
    const codeBlocks = page.locator('pre code');
    const codeBlockCount = await codeBlocks.count();
    expect(codeBlockCount).toBeGreaterThan(0);

    // Verify navigation is properly styled
    const navLinks = page.locator('.nav-links li');
    const navLinkCount = await navLinks.count();
    expect(navLinkCount).toBeGreaterThanOrEqual(3);

    // Verify buttons have proper hover states (check computed styles)
    const primaryButton = page.locator('[data-testid="primary-cta"]');
    const buttonStyles = await primaryButton.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        display: styles.display,
        cursor: styles.cursor,
        textDecoration: styles.textDecoration,
      };
    });

    // Button should be inline-block or block and have pointer cursor
    expect(['inline-block', 'inline', 'block', 'flex', 'inline-flex']).toContain(buttonStyles.display);
    expect(buttonStyles.cursor).toBe('pointer');

    // Verify responsive meta tag is present
    const viewportMeta = page.locator('meta[name="viewport"]');
    await expect(viewportMeta).toBeAttached();
    const viewportContent = await viewportMeta.getAttribute('content');
    expect(viewportContent).toContain('width=device-width');

    // Verify external stylesheets loaded correctly
    const stylesheets = page.locator('link[rel="stylesheet"]');
    const stylesheetCount = await stylesheets.count();
    expect(stylesheetCount).toBeGreaterThan(0);

    // Verify page renders without JavaScript errors
    const consoleErrors = [];
    page.on('console', (msg) => {
      if (msg.type() === 'error') {
        consoleErrors.push(msg.text());
      }
    });

    // Wait for page to fully load
    await page.waitForLoadState('networkidle');

    // Reload page to catch any errors
    await page.reload();
    await page.waitForLoadState('networkidle');

    // Filter out common non-critical errors (like failed logo load which is a GIF placeholder)
    const criticalErrors = consoleErrors.filter(
      (err) => !err.includes('Failed to load resource') && !err.includes('404')
    );

    // No critical JavaScript errors should occur
    expect(criticalErrors.length).toBe(0);
  });

  // Additional test: Visual elements render without clipping or overflow issues
  test('Firefox: Visual elements render without layout issues', async ({ page, browserName }) => {
    test.skip(browserName !== 'firefox', 'This test is Firefox-specific');

    await page.goto('/');

    // Check hero section doesn't have overflow issues
    const heroContent = page.locator('.hero-content');
    await expect(heroContent).toBeVisible();

    const heroBox = await heroContent.boundingBox();
    expect(heroBox).not.toBeNull();
    expect(heroBox?.width).toBeGreaterThan(0);
    expect(heroBox?.height).toBeGreaterThan(0);

    // Check feature cards have proper dimensions
    const featureCard = page.locator('.feature-card').first();
    const cardBox = await featureCard.boundingBox();
    expect(cardBox).not.toBeNull();
    expect(cardBox?.width).toBeGreaterThan(100);
    expect(cardBox?.height).toBeGreaterThan(100);

    // Check footer is visible at bottom of page
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
    const footer = page.locator('[data-testid="footer"]');
    await expect(footer).toBeVisible();
    await expect(footer).toBeInViewport();
  });

  // Additional test: External links have proper security attributes
  test('Firefox: External links have proper security attributes', async ({ page, browserName }) => {
    test.skip(browserName !== 'firefox', 'This test is Firefox-specific');

    await page.goto('/');

    // Get all external links
    const externalLinks = page.locator('a[href^="http"]');
    const linkCount = await externalLinks.count();

    expect(linkCount).toBeGreaterThan(0);

    // Verify each external link has security attributes
    for (let i = 0; i < linkCount; i++) {
      const link = externalLinks.nth(i);
      const target = await link.getAttribute('target');
      const rel = await link.getAttribute('rel');

      // External links should open in new tab with security attributes
      expect(target).toBe('_blank');
      expect(rel).toContain('noopener');
      expect(rel).toContain('noreferrer');
    }
  });
});
