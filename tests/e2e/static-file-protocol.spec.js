// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

/**
 * Integration tests for Static Page Functionality - File Protocol
 * Verifies the page is viewable when opened directly in browser (file:// protocol)
 */

test.describe('Static Page - File Protocol', () => {
  const indexPath = path.resolve(__dirname, '../../index.html');
  const fileUrl = `file://${indexPath}`;

  test('page loads correctly via file:// protocol', async ({ page }) => {
    // Navigate directly to the file
    await page.goto(fileUrl);

    // Verify the page loaded successfully
    await expect(page).toHaveTitle(/MirDB/);

    // Verify main content is visible
    await expect(page.locator('h1')).toContainText('MirDB');
    await expect(page.locator('.tagline')).toContainText('Persistent Key-Value Store');
  });

  test('hero section displays correctly via file:// protocol', async ({ page }) => {
    await page.goto(fileUrl);

    const hero = page.locator('.hero');
    await expect(hero).toBeVisible();

    // Check hero content
    await expect(page.locator('.hero h1')).toContainText('MirDB');
    await expect(page.locator('.hero .tagline')).toContainText('Persistent Key-Value Store with Memcached Protocol');
    await expect(page.locator('.hero .description')).toContainText('drop-in replacement');

    // Check CTA buttons are present
    await expect(page.locator('.cta-primary')).toBeVisible();
    await expect(page.locator('.cta-secondary')).toBeVisible();
  });

  test('features section displays correctly via file:// protocol', async ({ page }) => {
    await page.goto(fileUrl);

    const features = page.locator('#features');
    await expect(features).toBeVisible();

    // All 4 feature cards should be visible
    const featureCards = page.locator('.feature-card');
    await expect(featureCards).toHaveCount(4);

    // Verify feature titles
    await expect(featureCards.nth(0)).toContainText('Memcached Compatibility');
    await expect(featureCards.nth(1)).toContainText('Persistent Storage');
    await expect(featureCards.nth(2)).toContainText('LSM Tree Architecture');
    await expect(featureCards.nth(3)).toContainText('Built with Rust');
  });

  test('getting started section displays correctly via file:// protocol', async ({ page }) => {
    await page.goto(fileUrl);

    const gettingStarted = page.locator('#getting-started');
    await expect(gettingStarted).toBeVisible();

    // All 3 step cards should be visible
    const stepCards = page.locator('.step-card');
    await expect(stepCards).toHaveCount(3);

    // Check code examples are visible
    const codeExamples = page.locator('.code-example');
    await expect(codeExamples.first()).toBeVisible();

    // Check default configuration is visible
    const configInfo = page.locator('.config-info');
    await expect(configInfo).toBeVisible();
  });

  test('commands section displays correctly via file:// protocol', async ({ page }) => {
    await page.goto(fileUrl);

    const commands = page.locator('.commands');
    await expect(commands).toBeVisible();

    // All 4 command groups should be visible
    const commandGroups = page.locator('.command-group');
    await expect(commandGroups).toHaveCount(4);
  });

  test('footer displays correctly via file:// protocol', async ({ page }) => {
    await page.goto(fileUrl);

    const footer = page.locator('footer');
    await expect(footer).toBeVisible();

    // Footer links should be present
    const footerLinks = footer.locator('a');
    await expect(footerLinks).toHaveCount(2); // GitHub and Report Issue
  });

  test('CSS styles are applied correctly via file:// protocol', async ({ page }) => {
    await page.goto(fileUrl);

    // The CSS should load from the relative path
    const heroSection = page.locator('.hero');

    // Check that the hero has background styling
    const backgroundColor = await heroSection.evaluate(el => {
      return window.getComputedStyle(el).background;
    });

    // The hero should have a gradient background
    expect(backgroundColor).toContain('linear-gradient');

    // Check text color is applied
    const h1 = page.locator('h1');
    const h1Color = await h1.evaluate(el => {
      return window.getComputedStyle(el).color;
    });

    // Should have the primary color (blue)
    expect(h1Color).not.toBe('rgb(0, 0, 0)'); // Not default black
  });

  test('navigation is functional via file:// protocol', async ({ page }) => {
    await page.goto(fileUrl);

    // Navigation links should be present
    const navLinks = page.locator('.nav-links a');
    await expect(navLinks).toHaveCount(4); // Features, Getting Started, Docs, GitHub

    // Internal anchors should work
    await page.click('a[href="#features"]');
    await expect(page).toHaveURL(/#features/);

    await page.click('a[href="#getting-started"]');
    await expect(page).toHaveURL(/#getting-started/);
  });

  test('page structure is complete via file:// protocol', async ({ page }) => {
    await page.goto(fileUrl);

    // Check all major structural elements exist
    await expect(page.locator('header')).toBeVisible();
    await expect(page.locator('main')).toBeVisible();
    await expect(page.locator('footer')).toBeVisible();
    await expect(page.locator('nav')).toBeVisible();

    // Check proper heading hierarchy
    const h1 = page.locator('h1');
    const h2 = page.locator('h2');
    const h3 = page.locator('h3');

    await expect(h1).toHaveCount(1); // Only one h1
    expect(await h2.count()).toBeGreaterThanOrEqual(3); // Multiple h2s
    expect(await h3.count()).toBeGreaterThanOrEqual(4); // Multiple h3s
  });
});
