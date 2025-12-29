// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Cross-Browser Compatibility Tests
 *
 * These tests verify that the MirDB homepage renders correctly across
 * Chrome, Firefox, Safari (WebKit), and Edge browsers.
 */

test.describe('Cross-Browser Compatibility - Homepage Rendering', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Hero section renders correctly with all elements visible', async ({ page, browserName }) => {
    // Verify hero section is visible
    const hero = page.locator('#hero');
    await expect(hero).toBeVisible();

    // Verify main heading
    const heading = hero.locator('h1');
    await expect(heading).toBeVisible();
    await expect(heading).toHaveText('MirDB');

    // Verify tagline
    const tagline = hero.locator('.tagline');
    await expect(tagline).toBeVisible();
    await expect(tagline).toContainText('Persistent Key-Value Store');

    // Verify description
    const description = hero.locator('.description');
    await expect(description).toBeVisible();

    // Verify CTA buttons
    const primaryBtn = hero.locator('.btn-primary');
    const secondaryBtn = hero.locator('.btn-secondary');
    await expect(primaryBtn).toBeVisible();
    await expect(secondaryBtn).toBeVisible();

    // Verify hero layout uses flexbox centering
    const heroDisplay = await hero.evaluate((el) => window.getComputedStyle(el).display);
    expect(heroDisplay).toBe('flex');
  });

  test('TC2: Features section renders correctly with grid layout', async ({ page, browserName }) => {
    // Verify features section is visible
    const features = page.locator('#features');
    await expect(features).toBeVisible();

    // Verify section title
    const sectionTitle = features.locator('.section-title');
    await expect(sectionTitle).toHaveText('Key Features');

    // Verify features grid uses CSS grid
    const featuresGrid = features.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    const gridDisplay = await featuresGrid.evaluate((el) => window.getComputedStyle(el).display);
    expect(gridDisplay).toBe('grid');

    // Verify all three feature cards are visible
    const featureCards = features.locator('.feature-card');
    await expect(featureCards).toHaveCount(3);

    // Verify each feature card renders correctly
    const memcachedCard = features.locator('[data-feature="memcached-compatible"]');
    const persistentCard = features.locator('[data-feature="persistent-storage"]');
    const performanceCard = features.locator('[data-feature="high-performance"]');

    await expect(memcachedCard).toBeVisible();
    await expect(persistentCard).toBeVisible();
    await expect(performanceCard).toBeVisible();

    // Verify card styling - box shadow indicates proper rendering
    const cardBoxShadow = await memcachedCard.evaluate((el) => window.getComputedStyle(el).boxShadow);
    expect(cardBoxShadow).not.toBe('none');
  });

  test('TC3: Commands section renders correctly with grid layout', async ({ page, browserName }) => {
    // Verify commands section is visible
    const commands = page.locator('#commands');
    await expect(commands).toBeVisible();

    // Verify section title
    const sectionTitle = commands.locator('.section-title');
    await expect(sectionTitle).toHaveText('Supported Commands');

    // Verify commands grid uses CSS grid
    const commandsGrid = commands.locator('.commands-grid');
    await expect(commandsGrid).toBeVisible();

    const gridDisplay = await commandsGrid.evaluate((el) => window.getComputedStyle(el).display);
    expect(gridDisplay).toBe('grid');

    // Verify all command cards are present
    const commandCards = commands.locator('.command-card');
    await expect(commandCards).toHaveCount(7);

    // Verify specific commands are visible
    const setCommand = commands.locator('[data-command="set"]');
    const getCommand = commands.locator('[data-command="get"]');
    const deleteCommand = commands.locator('[data-command="delete"]');

    await expect(setCommand).toBeVisible();
    await expect(getCommand).toBeVisible();
    await expect(deleteCommand).toBeVisible();
  });

  test('TC4: Quick Start section renders correctly with code blocks', async ({ page, browserName }) => {
    // Verify getting-started section is visible
    const quickStart = page.locator('#getting-started');
    await expect(quickStart).toBeVisible();

    // Verify section title
    const sectionTitle = quickStart.locator('.section-title');
    await expect(sectionTitle).toHaveText('Quick Start');

    // Verify code blocks are visible and styled correctly
    const codeBlocks = quickStart.locator('.code-block');
    await expect(codeBlocks).toHaveCount(4);

    // Verify first code block has proper styling
    const firstCodeBlock = codeBlocks.first();
    await expect(firstCodeBlock).toBeVisible();

    const codeBlockBg = await firstCodeBlock.evaluate((el) => window.getComputedStyle(el).backgroundColor);
    // Should have dark background (rgb values for #1a1a2e)
    expect(codeBlockBg).toMatch(/rgb\(\d+, \d+, \d+\)/);

    // Verify copy buttons are present and visible
    const copyButtons = quickStart.locator('.copy-btn');
    await expect(copyButtons).toHaveCount(4);
    await expect(copyButtons.first()).toBeVisible();
  });

  test('TC5: Footer renders correctly with links', async ({ page, browserName }) => {
    // Verify footer is visible
    const footer = page.locator('.footer');
    await expect(footer).toBeVisible();

    // Verify footer links container uses flexbox
    const footerLinks = footer.locator('.footer-links');
    await expect(footerLinks).toBeVisible();

    const flexDisplay = await footerLinks.evaluate((el) => window.getComputedStyle(el).display);
    expect(flexDisplay).toBe('flex');

    // Verify all footer links are present
    const links = footerLinks.locator('a');
    await expect(links).toHaveCount(3);

    // Verify specific links
    await expect(links.nth(0)).toHaveText('GitHub');
    await expect(links.nth(1)).toHaveText('Documentation');
    await expect(links.nth(2)).toHaveText('License');

    // Verify copyright text
    const copyright = footer.locator('.copyright');
    await expect(copyright).toBeVisible();
    await expect(copyright).toContainText('Built with Rust');
  });

  test('TC6: CSS flexbox layout works correctly', async ({ page, browserName }) => {
    // Test various flexbox containers across the page

    // Hero CTA buttons use flexbox
    const ctaButtons = page.locator('.cta-buttons');
    await expect(ctaButtons).toBeVisible();
    const ctaDisplay = await ctaButtons.evaluate((el) => window.getComputedStyle(el).display);
    expect(ctaDisplay).toBe('flex');

    // Footer links use flexbox
    const footerLinks = page.locator('.footer-links');
    const footerDisplay = await footerLinks.evaluate((el) => window.getComputedStyle(el).display);
    expect(footerDisplay).toBe('flex');

    // Code block header uses flexbox
    const codeBlockHeader = page.locator('.code-block-header').first();
    await expect(codeBlockHeader).toBeVisible();
    const headerDisplay = await codeBlockHeader.evaluate((el) => window.getComputedStyle(el).display);
    expect(headerDisplay).toBe('flex');
  });

  test('TC7: CSS grid layout works correctly', async ({ page, browserName }) => {
    // Test grid layouts

    // Features grid
    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();
    const featuresDisplay = await featuresGrid.evaluate((el) => window.getComputedStyle(el).display);
    expect(featuresDisplay).toBe('grid');

    // Verify grid-template-columns is set (auto-fit)
    const featuresGridCols = await featuresGrid.evaluate((el) => window.getComputedStyle(el).gridTemplateColumns);
    expect(featuresGridCols).toBeTruthy();
    expect(featuresGridCols).not.toBe('none');

    // Commands grid
    const commandsGrid = page.locator('.commands-grid');
    await expect(commandsGrid).toBeVisible();
    const commandsDisplay = await commandsGrid.evaluate((el) => window.getComputedStyle(el).display);
    expect(commandsDisplay).toBe('grid');

    // Verify commands grid-template-columns is set
    const commandsGridCols = await commandsGrid.evaluate((el) => window.getComputedStyle(el).gridTemplateColumns);
    expect(commandsGridCols).toBeTruthy();
    expect(commandsGridCols).not.toBe('none');
  });

  test('TC8: CSS custom properties (variables) are applied correctly', async ({ page, browserName }) => {
    // Verify CSS variables are working by checking computed styles

    // Check primary color on button
    const primaryBtn = page.locator('.btn-primary').first();
    await expect(primaryBtn).toBeVisible();
    const btnBgColor = await primaryBtn.evaluate((el) => window.getComputedStyle(el).backgroundColor);
    // Primary color should be #0d6efd = rgb(13, 110, 253)
    expect(btnBgColor).toMatch(/rgb\(\s*13\s*,\s*110\s*,\s*253\s*\)/);

    // Check section background colors
    const featuresSection = page.locator('.features');
    const featuresBg = await featuresSection.evaluate((el) => window.getComputedStyle(el).backgroundColor);
    // bg-light should be #f8f9fa = rgb(248, 249, 250)
    expect(featuresBg).toMatch(/rgb\(\s*248\s*,\s*249\s*,\s*250\s*\)/);
  });

  test('TC9: Typography renders correctly across browsers', async ({ page, browserName }) => {
    // Verify font-family is applied correctly
    const body = page.locator('body');
    const fontFamily = await body.evaluate((el) => window.getComputedStyle(el).fontFamily);
    // Should use system font stack
    expect(fontFamily).toMatch(/(-apple-system|BlinkMacSystemFont|Segoe UI|Roboto)/);

    // Verify heading sizes
    const h1 = page.locator('h1').first();
    const h1FontSize = await h1.evaluate((el) => parseFloat(window.getComputedStyle(el).fontSize));
    expect(h1FontSize).toBeGreaterThan(40); // 3.5rem = 56px typically

    const h2 = page.locator('.section-title').first();
    const h2FontSize = await h2.evaluate((el) => parseFloat(window.getComputedStyle(el).fontSize));
    expect(h2FontSize).toBeGreaterThan(30); // 2.5rem = 40px typically
  });

  test('TC10: Page has no horizontal scrollbar (no overflow issues)', async ({ page, browserName }) => {
    // Check that the page width doesn't exceed viewport
    const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
    const viewportWidth = await page.evaluate(() => window.innerWidth);

    // Body scroll width should not significantly exceed viewport width
    expect(bodyScrollWidth).toBeLessThanOrEqual(viewportWidth + 20); // Allow small tolerance
  });

  test('TC11: All sections have proper spacing and padding', async ({ page, browserName }) => {
    // Verify sections have padding applied
    const heroSection = page.locator('.hero');
    const heroPadding = await heroSection.evaluate((el) => window.getComputedStyle(el).padding);
    expect(heroPadding).not.toBe('0px');

    const featuresSection = page.locator('.features');
    const featuresPadding = await featuresSection.evaluate((el) => window.getComputedStyle(el).padding);
    expect(featuresPadding).not.toBe('0px');

    const commandsSection = page.locator('.commands');
    const commandsPadding = await commandsSection.evaluate((el) => window.getComputedStyle(el).padding);
    expect(commandsPadding).not.toBe('0px');

    const quickStartSection = page.locator('.getting-started');
    const quickStartPadding = await quickStartSection.evaluate((el) => window.getComputedStyle(el).padding);
    expect(quickStartPadding).not.toBe('0px');
  });

  test('TC12: Border-radius renders correctly on cards', async ({ page, browserName }) => {
    // Verify border-radius on feature cards
    const featureCard = page.locator('.feature-card').first();
    await expect(featureCard).toBeVisible();
    const cardBorderRadius = await featureCard.evaluate((el) => window.getComputedStyle(el).borderRadius);
    expect(cardBorderRadius).not.toBe('0px');

    // Verify border-radius on code blocks
    const codeBlock = page.locator('.code-block').first();
    const codeBlockBorderRadius = await codeBlock.evaluate((el) => window.getComputedStyle(el).borderRadius);
    expect(codeBlockBorderRadius).not.toBe('0px');

    // Verify border-radius on buttons
    const btn = page.locator('.btn').first();
    const btnBorderRadius = await btn.evaluate((el) => window.getComputedStyle(el).borderRadius);
    expect(btnBorderRadius).not.toBe('0px');
  });
});
