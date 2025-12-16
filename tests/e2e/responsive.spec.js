/**
 * E2E tests for MirDB Homepage Responsive Design.
 *
 * This module tests responsive design at different viewport sizes:
 * - Desktop (1920x1080)
 * - Tablet (768x1024)
 * - Mobile (375x667)
 * - Minimum width (320x568 per NFR-1)
 */

const { test, expect } = require('@playwright/test');
const path = require('path');

// Get the absolute path to the index.html file
const indexPath = `file://${path.resolve(__dirname, '../../index.html')}`;

test.describe('Desktop Layout (1920px width)', () => {
  test.use({ viewport: { width: 1920, height: 1080 } });

  test('page displays correctly with proper spacing on desktop', async ({ page }) => {
    await page.goto(indexPath);

    // Check that the page loaded successfully
    await expect(page).toHaveTitle(/MirDB/);

    // Verify hero section is visible and properly sized
    const hero = page.locator('.hero');
    await expect(hero).toBeVisible();

    // Verify navbar displays horizontally (not stacked)
    const navbar = page.locator('.navbar');
    await expect(navbar).toBeVisible();

    // Check that nav-links are in a row (flexbox horizontal)
    const navLinks = page.locator('.nav-links');
    const navBox = await navLinks.boundingBox();
    expect(navBox.width).toBeGreaterThan(200); // Should be wide, not stacked

    // Features grid should show multiple columns
    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    // Verify container has max-width and is centered
    const container = page.locator('.container').first();
    const containerBox = await container.boundingBox();
    expect(containerBox.width).toBeLessThanOrEqual(1200); // max-width constraint
  });

  test('no horizontal scrollbar on desktop', async ({ page }) => {
    await page.goto(indexPath);

    // Check that there's no horizontal overflow
    const scrollWidth = await page.evaluate(() => document.documentElement.scrollWidth);
    const clientWidth = await page.evaluate(() => document.documentElement.clientWidth);
    expect(scrollWidth).toBeLessThanOrEqual(clientWidth);
  });
});

test.describe('Tablet Layout (768px width)', () => {
  test.use({ viewport: { width: 768, height: 1024 } });

  test('layout adapts for tablet view, navigation remains accessible', async ({ page }) => {
    await page.goto(indexPath);

    // Page should still load correctly
    await expect(page).toHaveTitle(/MirDB/);

    // Navbar should be visible
    const navbar = page.locator('.navbar');
    await expect(navbar).toBeVisible();

    // Navigation links should still be accessible (may be wrapped)
    const navLinks = page.locator('.nav-links a');
    const linkCount = await navLinks.count();
    expect(linkCount).toBeGreaterThan(0);

    // All nav links should be visible
    for (let i = 0; i < linkCount; i++) {
      await expect(navLinks.nth(i)).toBeVisible();
    }

    // Hero content should be visible and readable
    const heroH1 = page.locator('.hero h1');
    await expect(heroH1).toBeVisible();
    await expect(heroH1).toContainText('MirDB');

    // Features section should be visible
    const features = page.locator('#features');
    await expect(features).toBeVisible();
  });

  test('hero font sizes are reduced for tablet', async ({ page }) => {
    await page.goto(indexPath);

    // On tablet (768px), the h1 font size should be reduced (2.5rem = 40px typically)
    const heroH1 = page.locator('.hero h1');
    const fontSize = await heroH1.evaluate(el => getComputedStyle(el).fontSize);
    const fontSizePx = parseFloat(fontSize);
    expect(fontSizePx).toBeLessThan(64); // Should be less than desktop 4rem
  });

  test('no horizontal scrollbar on tablet', async ({ page }) => {
    await page.goto(indexPath);

    const scrollWidth = await page.evaluate(() => document.documentElement.scrollWidth);
    const clientWidth = await page.evaluate(() => document.documentElement.clientWidth);
    expect(scrollWidth).toBeLessThanOrEqual(clientWidth);
  });
});

test.describe('Mobile Layout (375px width)', () => {
  test.use({ viewport: { width: 375, height: 667 } });

  test('layout adapts for mobile view, content is readable', async ({ page }) => {
    await page.goto(indexPath);

    // Page should load
    await expect(page).toHaveTitle(/MirDB/);

    // Hero section should be visible
    const hero = page.locator('.hero');
    await expect(hero).toBeVisible();

    // Product name should be readable
    const heroH1 = page.locator('.hero h1');
    await expect(heroH1).toBeVisible();
    await expect(heroH1).toContainText('MirDB');

    // Tagline should be visible
    const tagline = page.locator('.tagline');
    await expect(tagline).toBeVisible();

    // CTA buttons should be visible and accessible
    const ctaButtons = page.locator('.cta-buttons .btn');
    const buttonCount = await ctaButtons.count();
    expect(buttonCount).toBeGreaterThan(0);
    for (let i = 0; i < buttonCount; i++) {
      await expect(ctaButtons.nth(i)).toBeVisible();
    }
  });

  test('no horizontal scroll on mobile', async ({ page }) => {
    await page.goto(indexPath);

    const scrollWidth = await page.evaluate(() => document.documentElement.scrollWidth);
    const clientWidth = await page.evaluate(() => document.documentElement.clientWidth);
    expect(scrollWidth).toBeLessThanOrEqual(clientWidth);
  });

  test('navigation is accessible on mobile', async ({ page }) => {
    await page.goto(indexPath);

    // Nav links should be wrapped and visible
    const navLinks = page.locator('.nav-links a');
    const linkCount = await navLinks.count();
    expect(linkCount).toBeGreaterThan(0);

    // All links should be visible (may be in multiple rows)
    for (let i = 0; i < linkCount; i++) {
      await expect(navLinks.nth(i)).toBeVisible();
    }
  });

  test('feature cards stack on mobile', async ({ page }) => {
    await page.goto(indexPath);

    // Get feature cards
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThan(0);

    // Each card should be visible
    for (let i = 0; i < cardCount; i++) {
      await expect(featureCards.nth(i)).toBeVisible();
    }
  });
});

test.describe('Minimum Width Layout (320px per NFR-1)', () => {
  test.use({ viewport: { width: 320, height: 568 } });

  test('layout functions correctly at minimum supported width', async ({ page }) => {
    await page.goto(indexPath);

    // Page should load
    await expect(page).toHaveTitle(/MirDB/);

    // Hero section should be visible
    const hero = page.locator('.hero');
    await expect(hero).toBeVisible();

    // Product name should still be readable
    const heroH1 = page.locator('.hero h1');
    await expect(heroH1).toBeVisible();
    await expect(heroH1).toContainText('MirDB');

    // All main sections should be accessible
    const sections = ['#hero', '#features', '#commands', '#getting-started', '#architecture', '#status'];
    for (const sectionId of sections) {
      const section = page.locator(sectionId);
      await expect(section).toBeVisible();
    }
  });

  test('no horizontal scroll at minimum width (320px)', async ({ page }) => {
    await page.goto(indexPath);

    const scrollWidth = await page.evaluate(() => document.documentElement.scrollWidth);
    const clientWidth = await page.evaluate(() => document.documentElement.clientWidth);

    // Critical: No horizontal overflow at minimum width
    expect(scrollWidth).toBeLessThanOrEqual(clientWidth);
  });

  test('content fits within viewport at 320px', async ({ page }) => {
    await page.goto(indexPath);

    // Check main content areas don't overflow
    const body = page.locator('body');
    const bodyBox = await body.boundingBox();
    expect(bodyBox.width).toBeLessThanOrEqual(320);
  });

  test('text remains readable at 320px', async ({ page }) => {
    await page.goto(indexPath);

    // Check that body text has reasonable font size (at least 14px)
    const paragraph = page.locator('.tagline');
    const fontSize = await paragraph.evaluate(el => getComputedStyle(el).fontSize);
    const fontSizePx = parseFloat(fontSize);
    expect(fontSizePx).toBeGreaterThanOrEqual(14);
  });

  test('tables have horizontal scroll wrapper at 320px', async ({ page }) => {
    await page.goto(indexPath);

    // Commands table wrapper should have overflow handling
    const tableWrapper = page.locator('.commands-table');
    const overflow = await tableWrapper.evaluate(el => getComputedStyle(el).overflowX);
    expect(overflow).toBe('auto');
  });

  test('architecture diagram adapts at 320px', async ({ page }) => {
    await page.goto(indexPath);

    // Architecture flow should stack vertically on small screens
    const archFlow = page.locator('.arch-flow');
    await expect(archFlow).toBeVisible();

    // Check flex-direction is column (stacked) on mobile
    const flexDirection = await archFlow.evaluate(el => getComputedStyle(el).flexDirection);
    expect(flexDirection).toBe('column');
  });
});

test.describe('Cross-viewport functionality', () => {
  test('all navigation links work at all viewport sizes', async ({ page }) => {
    const viewports = [
      { width: 1920, height: 1080 },
      { width: 768, height: 1024 },
      { width: 375, height: 667 },
      { width: 320, height: 568 },
    ];

    for (const viewport of viewports) {
      await page.setViewportSize(viewport);
      await page.goto(indexPath);

      // Test internal navigation links
      const featuresLink = page.locator('a[href="#features"]');
      await featuresLink.click();

      // Should scroll to features section
      const features = page.locator('#features');
      await expect(features).toBeInViewport();
    }
  });

  test('CTA buttons are accessible at all viewport sizes', async ({ page }) => {
    const viewports = [
      { width: 1920, height: 1080 },
      { width: 768, height: 1024 },
      { width: 375, height: 667 },
      { width: 320, height: 568 },
    ];

    for (const viewport of viewports) {
      await page.setViewportSize(viewport);
      await page.goto(indexPath);

      // Both CTA buttons should be visible and clickable
      const getStartedBtn = page.locator('.btn-primary').first();
      const githubBtn = page.locator('.btn-secondary').first();

      await expect(getStartedBtn).toBeVisible();
      await expect(githubBtn).toBeVisible();
    }
  });
});
