// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

const indexPath = 'file://' + path.resolve(__dirname, '../index.html');

/**
 * Cross-Browser Compatibility Tests
 *
 * These tests verify that the MirDB landing page renders correctly
 * across Chrome, Firefox, Safari (WebKit), and Edge browsers.
 *
 * Each test validates core functionality that must work consistently
 * across all browsers to meet the success criteria.
 */
test.describe('Cross-Browser Compatibility', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto(indexPath);
  });

  test('page loads and displays hero section correctly', async ({ page, browserName }) => {
    // Verify page title loads
    await expect(page).toHaveTitle('MirDB - Persistent Key-Value Store');

    // Verify hero section is visible
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Verify product name is displayed
    const heroTitle = page.locator('.hero-title');
    await expect(heroTitle).toBeVisible();
    await expect(heroTitle).toHaveText('MirDB');

    // Verify subtitle with value proposition
    const heroSubtitle = page.locator('.hero-subtitle');
    await expect(heroSubtitle).toBeVisible();
    const subtitleText = await heroSubtitle.textContent();
    expect(subtitleText.toLowerCase()).toContain('persistent');
    expect(subtitleText.toLowerCase()).toContain('memcached');
  });

  test('logo image loads successfully', async ({ page, browserName }) => {
    const logo = page.locator('#logo');
    await expect(logo).toBeVisible();

    // Verify logo source
    const logoSrc = await logo.getAttribute('src');
    expect(logoSrc).toContain('logo.gif');

    // Verify image has loaded (natural dimensions > 0)
    const naturalWidth = await logo.evaluate((img) => img.naturalWidth);
    expect(naturalWidth).toBeGreaterThan(0);
  });

  test('CTA buttons are functional', async ({ page, browserName }) => {
    // Primary CTA - View on GitHub
    const primaryCta = page.locator('#cta-primary');
    await expect(primaryCta).toBeVisible();
    await expect(primaryCta).toBeEnabled();
    const primaryHref = await primaryCta.getAttribute('href');
    expect(primaryHref).toContain('github.com');

    // Secondary CTA - View Documentation
    const secondaryCta = page.locator('#cta-secondary');
    await expect(secondaryCta).toBeVisible();
    const secondaryHref = await secondaryCta.getAttribute('href');
    expect(secondaryHref).toBe('#quick-start');
  });

  test('features section renders with all feature cards', async ({ page, browserName }) => {
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    // Verify all 4 feature cards are present
    const featureCards = page.locator('.feature-card');
    await expect(featureCards).toHaveCount(4);

    // Verify each feature card has title and description
    for (let i = 0; i < 4; i++) {
      const card = featureCards.nth(i);
      await expect(card).toBeVisible();
      await expect(card.locator('h3')).toBeVisible();
      await expect(card.locator('p')).toBeVisible();
    }
  });

  test('code examples section displays correctly', async ({ page, browserName }) => {
    const codeSection = page.locator('#code-examples');
    await expect(codeSection).toBeVisible();

    // Verify code block is present
    const codeBlock = page.locator('#code-example');
    await expect(codeBlock).toBeVisible();

    // Verify copy button exists
    const copyBtn = page.locator('#copy-code-btn');
    await expect(copyBtn).toBeVisible();

    // Verify syntax highlighting elements exist
    const syntaxElements = page.locator('.syntax-highlight');
    await expect(syntaxElements).toBeVisible();
  });

  test('quick start section is accessible', async ({ page, browserName }) => {
    const quickStartSection = page.locator('#quick-start');
    await expect(quickStartSection).toBeVisible();

    // Verify installation instructions
    const installation = quickStartSection.locator('.installation');
    await expect(installation).toBeVisible();

    // Verify usage instructions
    const usage = quickStartSection.locator('.usage');
    await expect(usage).toBeVisible();

    // Verify default configuration info
    const configInfo = quickStartSection.locator('.config-info');
    await expect(configInfo).toBeVisible();
  });

  test('project status section displays correctly', async ({ page, browserName }) => {
    const statusSection = page.locator('#status');
    await expect(statusSection).toBeVisible();

    // Verify implemented features list
    const implemented = statusSection.locator('.implemented');
    await expect(implemented).toBeVisible();
    const implementedItems = implemented.locator('li');
    expect(await implementedItems.count()).toBeGreaterThan(0);

    // Verify planned features list
    const planned = statusSection.locator('.planned');
    await expect(planned).toBeVisible();
    const plannedItems = planned.locator('li');
    expect(await plannedItems.count()).toBeGreaterThan(0);
  });

  test('footer section is present with links', async ({ page, browserName }) => {
    const footer = page.locator('footer.footer');
    await expect(footer).toBeVisible();

    // Verify footer links exist
    const footerLinks = footer.locator('.footer-links');
    await expect(footerLinks).toBeVisible();

    // Verify license info
    const license = footer.locator('.license');
    await expect(license).toBeVisible();
    await expect(license).toContainText('ISC');
  });

  test('page has proper semantic HTML structure', async ({ page, browserName }) => {
    // Verify header exists
    const header = page.locator('header');
    await expect(header).toBeVisible();

    // Verify main content area
    const main = page.locator('main');
    await expect(main).toBeVisible();

    // Verify footer exists
    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Verify proper heading hierarchy
    const h1 = page.locator('h1');
    await expect(h1).toHaveCount(1);

    const h2Elements = page.locator('h2');
    expect(await h2Elements.count()).toBeGreaterThan(0);
  });

  test('CSS styles are applied correctly', async ({ page, browserName }) => {
    // Verify hero section has expected styling (uses gradient)
    const heroSection = page.locator('#hero');
    const heroBackgroundImage = await heroSection.evaluate(
      (el) => window.getComputedStyle(el).backgroundImage
    );
    // Background should have gradient applied
    expect(heroBackgroundImage).toContain('gradient');

    // Verify buttons have pointer cursor
    const primaryBtn = page.locator('#cta-primary');
    const cursor = await primaryBtn.evaluate(
      (el) => window.getComputedStyle(el).cursor
    );
    expect(cursor).toBe('pointer');

    // Verify font-family is applied
    const body = page.locator('body');
    const fontFamily = await body.evaluate(
      (el) => window.getComputedStyle(el).fontFamily
    );
    expect(fontFamily).toBeTruthy();
  });

  test('demo section displays usage GIF', async ({ page, browserName }) => {
    const demoSection = page.locator('#demo');
    await expect(demoSection).toBeVisible();

    // Verify demo GIF is present
    const demoGif = page.locator('.demo-gif');
    await expect(demoGif).toBeVisible();

    // Verify src contains usage.gif
    const gifSrc = await demoGif.getAttribute('src');
    expect(gifSrc).toContain('usage.gif');
  });

  test('all internal navigation links work', async ({ page, browserName }) => {
    // Click on View Documentation (links to #quick-start)
    const docLink = page.locator('#cta-secondary');
    await docLink.click();

    // Verify quick-start section is now in view
    const quickStart = page.locator('#quick-start');
    await expect(quickStart).toBeInViewport();
  });

  test('viewport meta tag is present for responsive design', async ({ page, browserName }) => {
    const viewport = await page.locator('meta[name="viewport"]').getAttribute('content');
    expect(viewport).toContain('width=device-width');
    expect(viewport).toContain('initial-scale=1');
  });

  test('page is accessible via keyboard navigation', async ({ page, browserName }) => {
    // Tab through the page to verify keyboard accessibility
    await page.keyboard.press('Tab');

    // First focusable element should receive focus
    const focusedElement = await page.evaluate(() => document.activeElement?.tagName);
    expect(focusedElement).toBeTruthy();

    // Verify primary CTA can receive focus
    const primaryCta = page.locator('#cta-primary');
    await primaryCta.focus();
    const isFocused = await primaryCta.evaluate((el) => el === document.activeElement);
    expect(isFocused).toBe(true);
  });
});
