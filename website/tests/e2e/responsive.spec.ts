/**
 * Responsive Design Tests
 * Owner: Scenarios 7, 8 - Responsive Design (Mobile & Tablet)
 *
 * Tests for mobile and tablet responsiveness as specified in NFR-1:
 * - Mobile: No horizontal overflow, touch targets, readable content
 * - Tablet: 2-column feature grid, proper image sizing, layout adaptation
 */

import { test, expect } from '@playwright/test';

// Mobile viewport dimensions (iPhone SE)
const MOBILE_VIEWPORT = { width: 375, height: 667 };
const MIN_TOUCH_TARGET = 44;

// ============================
// Mobile Tests (Scenario 7)
// ============================
test.describe('Responsive Design - Mobile', () => {
  test.beforeEach(async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize(MOBILE_VIEWPORT);
    await page.goto('/');
  });

  test('Page renders without horizontal overflow at 375x667 viewport', async ({ page }) => {
    // Wait for page to load
    await page.waitForLoadState('domcontentloaded');

    // Check that the document doesn't have horizontal overflow
    const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
    const windowWidth = await page.evaluate(() => window.innerWidth);

    expect(bodyScrollWidth).toBeLessThanOrEqual(windowWidth);

    // Also check html element
    const htmlScrollWidth = await page.evaluate(() => document.documentElement.scrollWidth);
    expect(htmlScrollWidth).toBeLessThanOrEqual(windowWidth);

    // Verify no horizontal scrollbar is present
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);
  });

  test('All buttons and links have minimum 44x44px touch area', async ({ page }) => {
    await page.waitForLoadState('domcontentloaded');

    // Get all interactive elements (buttons and links)
    const interactiveElements = await page.locator('a, button').all();

    for (const element of interactiveElements) {
      // Skip hidden elements
      const isVisible = await element.isVisible();
      if (!isVisible) continue;

      const box = await element.boundingBox();
      if (box) {
        // Calculate effective touch target size (including padding)
        // Touch target should be at least 44x44px
        expect(box.width, `Element width should be at least ${MIN_TOUCH_TARGET}px`).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
        expect(box.height, `Element height should be at least ${MIN_TOUCH_TARGET}px`).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
      }
    }
  });

  test('Hero content is readable and logo is visible on mobile', async ({ page }) => {
    await page.waitForLoadState('domcontentloaded');

    // Check hero section is visible
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Check logo is visible
    const logo = page.locator('.hero-logo');
    await expect(logo).toBeVisible();

    // Verify logo has reasonable size on mobile
    const logoBox = await logo.boundingBox();
    expect(logoBox).not.toBeNull();
    if (logoBox) {
      // Logo should be visible and not too large for mobile
      expect(logoBox.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width - 32); // Leave some padding
      expect(logoBox.width).toBeGreaterThan(50); // But still visible
    }

    // Check h1 is visible and readable
    const heading = page.locator('.hero h1');
    await expect(heading).toBeVisible();

    // Check subtitle is visible
    const subtitle = page.locator('.hero-subtitle');
    await expect(subtitle).toBeVisible();

    // Check CTA button is visible
    const ctaButton = page.locator('.hero .btn-primary');
    await expect(ctaButton).toBeVisible();
  });

  test('Code blocks scroll horizontally or wrap without breaking layout', async ({ page }) => {
    await page.waitForLoadState('domcontentloaded');

    // Navigate to quickstart section
    await page.locator('#quickstart').scrollIntoViewIfNeeded();

    // Get all code blocks
    const codeBlocks = await page.locator('.code-block').all();
    expect(codeBlocks.length).toBeGreaterThan(0);

    for (const codeBlock of codeBlocks) {
      await expect(codeBlock).toBeVisible();

      // Get code block dimensions
      const box = await codeBlock.boundingBox();
      expect(box).not.toBeNull();

      if (box) {
        // Code block should not exceed viewport width
        expect(box.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width);

        // Code block should be scrollable (has overflow-x: auto)
        const overflowX = await codeBlock.evaluate((el) => {
          return window.getComputedStyle(el).overflowX;
        });

        // Should have scroll or auto for horizontal overflow handling
        expect(['auto', 'scroll']).toContain(overflowX);
      }
    }

    // Verify page still has no horizontal overflow after scrolling to code
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);
  });

  test('All sections (hero, features, quickstart, usage, footer) are visible and readable', async ({ page }) => {
    await page.waitForLoadState('domcontentloaded');

    // Define all sections that should be present
    const sections = [
      { selector: '#hero', name: 'Hero' },
      { selector: '#features', name: 'Features' },
      { selector: '#quickstart', name: 'Quickstart' },
      { selector: '#usage', name: 'Usage' },
      { selector: 'footer', name: 'Footer' }
    ];

    for (const section of sections) {
      const element = page.locator(section.selector);

      // Scroll to section
      await element.scrollIntoViewIfNeeded();

      // Verify section is visible
      await expect(element, `${section.name} section should be visible`).toBeVisible();

      // Verify section has content (not empty)
      const content = await element.textContent();
      expect(content?.trim().length, `${section.name} section should have content`).toBeGreaterThan(0);

      // Verify section fits within viewport width
      const box = await element.boundingBox();
      if (box) {
        expect(box.width, `${section.name} section should fit viewport`).toBeLessThanOrEqual(MOBILE_VIEWPORT.width);
      }
    }
  });

  test('Navigation elements respond to touch and work correctly', async ({ page }) => {
    await page.waitForLoadState('domcontentloaded');

    // On mobile, nav links may be hidden (hamburger menu pattern)
    // First check if nav links are visible or hidden
    const navLinks = page.locator('.nav-links');
    const navLinksVisible = await navLinks.isVisible();

    if (navLinksVisible) {
      // If nav links are visible, test they work
      const featureLink = page.locator('nav a[href="#features"]');
      await expect(featureLink).toBeVisible();

      // Click the features link
      await featureLink.click();

      // Verify we scrolled to features section
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeInViewport();
    }

    // Test the main CTA button
    const ctaButton = page.locator('.hero .btn-primary');
    await expect(ctaButton).toBeVisible();

    // Verify CTA button has proper touch target size
    const ctaBox = await ctaButton.boundingBox();
    expect(ctaBox).not.toBeNull();
    if (ctaBox) {
      expect(ctaBox.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
      expect(ctaBox.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
    }

    // Click CTA button and verify navigation
    await ctaButton.click();

    // Verify quickstart section is in viewport
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeInViewport();

    // Test copy buttons have proper touch targets
    const copyButtons = page.locator('.copy-btn');
    const copyButtonCount = await copyButtons.count();

    if (copyButtonCount > 0) {
      const firstCopyBtn = copyButtons.first();
      await firstCopyBtn.scrollIntoViewIfNeeded();

      const copyBtnBox = await firstCopyBtn.boundingBox();
      expect(copyBtnBox).not.toBeNull();
      if (copyBtnBox) {
        expect(copyBtnBox.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
        expect(copyBtnBox.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
      }
    }
  });

  test('Feature cards stack vertically on mobile', async ({ page }) => {
    await page.waitForLoadState('domcontentloaded');

    // Navigate to features section
    await page.locator('#features').scrollIntoViewIfNeeded();

    // Get feature cards
    const featureCards = await page.locator('.feature-card').all();
    expect(featureCards.length).toBe(4);

    // Verify cards stack vertically (each card takes full width or nearly)
    for (const card of featureCards) {
      const box = await card.boundingBox();
      expect(box).not.toBeNull();
      if (box) {
        // Cards should be nearly full width on mobile (accounting for container padding)
        expect(box.width).toBeGreaterThan(MOBILE_VIEWPORT.width * 0.7);
      }
    }

    // Check grid is single column by verifying computed columns is a single value
    // Browser computes "1fr" as a pixel value, so we check for single column
    const featuresGrid = page.locator('.features-grid');
    const gridStyle = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).gridTemplateColumns;
    });

    // Should be single column - computed style will be a single pixel value like "343px"
    // Split by space and check only one column value (not multiple like "343px 343px")
    const columns = gridStyle.trim().split(/\s+/);
    expect(columns.length, `Expected single column but got: ${gridStyle}`).toBe(1);
  });

  test('Footer content is accessible on mobile', async ({ page }) => {
    await page.waitForLoadState('domcontentloaded');

    // Scroll to footer
    const footer = page.locator('footer');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Check footer links are clickable
    const footerLinks = await page.locator('footer a').all();
    for (const link of footerLinks) {
      const box = await link.boundingBox();
      if (box) {
        expect(box.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
        expect(box.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
      }
    }
  });
});

// ============================
// Tablet Tests (Scenario 8)
// ============================
test.describe('Responsive Design - Tablet (768x1024)', () => {
  test.use({ viewport: { width: 768, height: 1024 } });

  test('page renders with appropriate tablet layout', async ({ page }) => {
    await page.goto('/');

    // Verify page loads successfully
    await expect(page).toHaveTitle(/MirDB/);

    // Verify body is visible and fills viewport width
    const body = page.locator('body');
    await expect(body).toBeVisible();

    // Verify no horizontal scrollbar (content fits within viewport)
    const documentWidth = await page.evaluate(() => document.documentElement.scrollWidth);
    const viewportWidth = await page.evaluate(() => window.innerWidth);
    expect(documentWidth).toBeLessThanOrEqual(viewportWidth);

    // Verify main sections are visible
    await expect(page.locator('#hero')).toBeVisible();
    await expect(page.locator('#features')).toBeVisible();
    await expect(page.locator('#quickstart')).toBeVisible();
    await expect(page.locator('#usage')).toBeVisible();
    await expect(page.locator('footer')).toBeVisible();
  });

  test('feature cards display in 2-column grid layout', async ({ page }) => {
    await page.goto('/');

    // Navigate to features section
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();
    await expect(featuresSection).toBeVisible();

    // Get the features grid
    const featuresGrid = page.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    // Verify grid-template-columns is set for 2 columns
    const gridStyle = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).gridTemplateColumns;
    });

    // Should have 2 columns (pattern: "XXXpx XXXpx")
    const columnCount = gridStyle.split(' ').length;
    expect(columnCount).toBe(2);

    // Verify all 4 feature cards are present
    const featureCards = page.locator('.feature-card');
    await expect(featureCards).toHaveCount(4);

    // Verify each card is visible
    for (let i = 0; i < 4; i++) {
      await expect(featureCards.nth(i)).toBeVisible();
    }
  });

  test('logo and usage.gif are properly sized and visible', async ({ page }) => {
    await page.goto('/');

    // Check hero logo
    const heroLogo = page.locator('.hero-logo');
    await expect(heroLogo).toBeVisible();

    // Verify logo dimensions are appropriate for tablet
    const logoBox = await heroLogo.boundingBox();
    expect(logoBox).not.toBeNull();
    if (logoBox) {
      // Logo should be reasonably sized (not too small, not overflowing)
      expect(logoBox.width).toBeGreaterThan(50);
      expect(logoBox.width).toBeLessThanOrEqual(768); // Should not exceed viewport
    }

    // Navigate to usage section
    const usageSection = page.locator('#usage');
    await usageSection.scrollIntoViewIfNeeded();
    await expect(usageSection).toBeVisible();

    // Check usage.gif
    const usageImage = page.locator('.usage-figure img');
    await expect(usageImage).toBeVisible();

    // Verify usage image has proper alt text
    await expect(usageImage).toHaveAttribute('alt', /MirDB usage/);

    // Verify usage image is within viewport bounds
    const usageBox = await usageImage.boundingBox();
    expect(usageBox).not.toBeNull();
    if (usageBox) {
      // Image should not exceed viewport width
      expect(usageBox.width).toBeLessThanOrEqual(768);
      expect(usageBox.width).toBeGreaterThan(100); // Should be reasonably sized
    }
  });

  test('all content sections are fully visible without overlap', async ({ page }) => {
    await page.goto('/');

    // Test each major section for proper visibility
    const sections = ['#hero', '#features', '#quickstart', '#usage', 'footer'];

    for (const sectionSelector of sections) {
      const section = page.locator(sectionSelector);
      await section.scrollIntoViewIfNeeded();
      await expect(section).toBeVisible();

      // Verify section has non-zero dimensions
      const box = await section.boundingBox();
      expect(box).not.toBeNull();
      if (box) {
        expect(box.width).toBeGreaterThan(0);
        expect(box.height).toBeGreaterThan(0);
        // Section should not exceed viewport width
        expect(box.width).toBeLessThanOrEqual(768);
      }
    }
  });

  test('quickstart code blocks are properly displayed', async ({ page }) => {
    await page.goto('/');

    // Navigate to quickstart section
    const quickstartSection = page.locator('#quickstart');
    await quickstartSection.scrollIntoViewIfNeeded();
    await expect(quickstartSection).toBeVisible();

    // Verify code blocks are visible
    const codeBlocks = page.locator('.code-block');
    const count = await codeBlocks.count();
    expect(count).toBeGreaterThan(0);

    // Each code block should be visible and fit within viewport
    for (let i = 0; i < count; i++) {
      const block = codeBlocks.nth(i);
      await expect(block).toBeVisible();

      const blockBox = await block.boundingBox();
      expect(blockBox).not.toBeNull();
      if (blockBox) {
        // Code block should not overflow viewport
        expect(blockBox.width).toBeLessThanOrEqual(768);
      }
    }
  });

  test('navigation is appropriately displayed for tablet', async ({ page }) => {
    await page.goto('/');

    // Verify header navigation exists
    const header = page.locator('header');
    await expect(header).toBeVisible();

    // Check nav logo
    const navLogo = page.locator('.nav-logo');
    await expect(navLogo).toBeVisible();

    // Nav links should still be visible on tablet (768px)
    const navLinks = page.locator('.nav-links');
    // Based on responsive.css, nav-links are hidden only below 576px
    // At 768px they should be visible
    await expect(navLinks).toBeVisible();
  });

  test('footer displays correctly on tablet', async ({ page }) => {
    await page.goto('/');

    // Scroll to footer
    const footer = page.locator('footer');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Footer should contain copyright and links
    await expect(footer).toContainText('MirDB');
    await expect(footer).toContainText('2024');

    // Footer links should be visible
    const footerLinks = page.locator('.footer-links a');
    const linksCount = await footerLinks.count();
    expect(linksCount).toBeGreaterThan(0);
  });
});
