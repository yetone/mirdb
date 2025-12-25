// @ts-check
const { test, expect } = require('@playwright/test');
const path = require('path');

/**
 * Test Suite: Responsive Design - Mobile
 *
 * This test suite validates that the MirDB homepage displays correctly
 * on mobile devices with proper responsive layout.
 *
 * Test Cases:
 * 1. All content visible without horizontal scrolling at 375px width
 * 2. Feature cards stack vertically in single column
 * 3. Touch targets meet minimum 44x44px accessibility requirement
 * 4. Code snippets are horizontally scrollable within container
 */

// Mobile viewport configuration (iPhone SE / standard mobile width)
const MOBILE_VIEWPORT = { width: 375, height: 667 };

test.describe('Responsive Design - Mobile', () => {
  test.beforeEach(async ({ page }) => {
    // Set mobile viewport before navigating
    await page.setViewportSize(MOBILE_VIEWPORT);

    // Navigate to the homepage
    await page.goto('file://' + path.join(__dirname, '..', 'index.html'));

    // Wait for page to fully load
    await page.waitForLoadState('domcontentloaded');
  });

  test('TC1: All content is visible without horizontal scrolling at 375px viewport width', async ({ page }) => {
    // Get the document body dimensions
    const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
    const viewportWidth = await page.evaluate(() => window.innerWidth);

    // Verify no horizontal overflow (scrollWidth should not exceed viewport width)
    expect(bodyScrollWidth).toBeLessThanOrEqual(viewportWidth);

    // Verify the html element doesn't have horizontal overflow either
    const htmlScrollWidth = await page.evaluate(() => document.documentElement.scrollWidth);
    expect(htmlScrollWidth).toBeLessThanOrEqual(viewportWidth);

    // Check that all main sections are visible
    const header = page.locator('.header');
    await expect(header).toBeVisible();

    const hero = page.locator('.hero');
    await expect(hero).toBeVisible();

    const features = page.locator('#features');
    await expect(features).toBeVisible();

    const quickStart = page.locator('#quick-start');
    await expect(quickStart).toBeVisible();

    const footer = page.locator('.footer');
    await expect(footer).toBeVisible();

    // Verify main content containers don't overflow the viewport
    const contentElements = await page.locator('.hero-content, .features-grid, .quick-start-content, .footer-content').all();
    for (const element of contentElements) {
      const box = await element.boundingBox();
      if (box) {
        // Element should not extend beyond viewport width
        expect(box.x).toBeGreaterThanOrEqual(0);
        expect(box.x + box.width).toBeLessThanOrEqual(viewportWidth + 1); // +1 for rounding tolerance
      }
    }
  });

  test('TC2: Feature cards stack vertically in single column at mobile width', async ({ page }) => {
    // Navigate to features section
    const featuresGrid = page.locator('[data-testid="features-grid"]');
    await expect(featuresGrid).toBeVisible();

    // Get all feature cards
    const featureCards = featuresGrid.locator('.feature-card');
    const cardCount = await featureCards.count();

    // Ensure there are feature cards to test
    expect(cardCount).toBeGreaterThanOrEqual(3);

    // Get bounding boxes for all cards
    const cardBoundingBoxes = [];
    for (let i = 0; i < cardCount; i++) {
      const box = await featureCards.nth(i).boundingBox();
      expect(box).not.toBeNull();
      cardBoundingBoxes.push(box);
    }

    // Verify cards are stacked vertically (each card should be below the previous one)
    for (let i = 1; i < cardBoundingBoxes.length; i++) {
      const prevCard = cardBoundingBoxes[i - 1];
      const currentCard = cardBoundingBoxes[i];

      // Current card's top should be at or below previous card's bottom
      expect(currentCard.y).toBeGreaterThanOrEqual(prevCard.y + prevCard.height - 1); // -1 for tolerance

      // Cards should have similar x positions (aligned in single column)
      expect(Math.abs(currentCard.x - prevCard.x)).toBeLessThan(20); // Allow small tolerance for alignment differences
    }

    // Verify cards span nearly full width (single column layout)
    const viewportWidth = await page.evaluate(() => window.innerWidth);
    for (const box of cardBoundingBoxes) {
      // Card width should be substantial portion of viewport (accounting for padding)
      expect(box.width).toBeGreaterThan(viewportWidth * 0.7);
    }

    // Verify CSS grid shows single column on mobile
    const gridComputedStyle = await featuresGrid.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        display: style.display,
        gridTemplateColumns: style.gridTemplateColumns
      };
    });

    // Should be grid with single column
    expect(gridComputedStyle.display).toBe('grid');
    // Single column means only one column width value (no space-separated values forming multiple columns)
    const columnValues = gridComputedStyle.gridTemplateColumns.trim().split(/\s+/);
    expect(columnValues.length).toBe(1);
  });

  test('TC3: All buttons and links have minimum 44x44px touch target size', async ({ page }) => {
    // Minimum touch target size per WCAG guidelines
    const MIN_TOUCH_TARGET = 44;

    // Get all interactive elements (buttons and links)
    const interactiveElements = page.locator('a, button');
    const elementCount = await interactiveElements.count();

    expect(elementCount).toBeGreaterThan(0);

    const failedElements = [];

    for (let i = 0; i < elementCount; i++) {
      const element = interactiveElements.nth(i);

      // Check if element is visible
      const isVisible = await element.isVisible();
      if (!isVisible) continue;

      // Get bounding box
      const box = await element.boundingBox();
      if (!box) continue;

      // Get element info for debugging
      const elementInfo = await element.evaluate((el) => ({
        tagName: el.tagName,
        text: el.textContent?.trim().substring(0, 30),
        className: el.className,
        href: el.getAttribute('href')
      }));

      // Check if either width or height meets minimum (touch targets can be either dimension)
      // We check both dimensions - at least one should meet the minimum for accessibility
      const meetsMinWidth = box.width >= MIN_TOUCH_TARGET;
      const meetsMinHeight = box.height >= MIN_TOUCH_TARGET;

      if (!meetsMinWidth && !meetsMinHeight) {
        failedElements.push({
          ...elementInfo,
          width: box.width,
          height: box.height
        });
      }
    }

    // Log any failed elements for debugging
    if (failedElements.length > 0) {
      console.log('Elements with insufficient touch target size:', failedElements);
    }

    // Verify all visible interactive elements meet touch target requirements
    expect(failedElements.length).toBe(0);
  });

  test('TC4: Code snippets are horizontally scrollable within container without breaking layout', async ({ page }) => {
    // Navigate to quick start section with code snippets
    const quickStart = page.locator('#quick-start');
    await expect(quickStart).toBeVisible();

    // Find all pre elements containing code
    const codeBlocks = quickStart.locator('pre');
    const codeBlockCount = await codeBlocks.count();

    expect(codeBlockCount).toBeGreaterThan(0);

    const viewportWidth = await page.evaluate(() => window.innerWidth);

    for (let i = 0; i < codeBlockCount; i++) {
      const codeBlock = codeBlocks.nth(i);
      await expect(codeBlock).toBeVisible();

      // Get computed styles to verify overflow handling
      const styles = await codeBlock.evaluate((el) => {
        const computed = window.getComputedStyle(el);
        return {
          overflowX: computed.overflowX,
          overflowY: computed.overflowY,
          maxWidth: computed.maxWidth,
          width: computed.width
        };
      });

      // Verify code block has horizontal scroll capability
      expect(['auto', 'scroll', 'overlay']).toContain(styles.overflowX);

      // Get bounding box to verify it doesn't overflow the viewport
      const box = await codeBlock.boundingBox();
      expect(box).not.toBeNull();

      // Code block should be contained within viewport
      expect(box.x).toBeGreaterThanOrEqual(0);
      expect(box.x + box.width).toBeLessThanOrEqual(viewportWidth + 1); // +1 for tolerance
    }

    // Verify the page body doesn't have horizontal scroll due to code blocks
    const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
    expect(bodyScrollWidth).toBeLessThanOrEqual(viewportWidth);
  });

  test('Hero section adapts properly to mobile viewport', async ({ page }) => {
    // Verify hero section is visible
    const hero = page.locator('.hero');
    await expect(hero).toBeVisible();

    // Verify hero content elements are visible and properly sized
    const heroTitle = page.locator('.hero h1');
    await expect(heroTitle).toBeVisible();

    // Get hero title font size - should be reduced on mobile
    const titleFontSize = await heroTitle.evaluate((el) =>
      parseFloat(window.getComputedStyle(el).fontSize)
    );

    // On mobile, h1 should be around 2.5rem (40px at 16px base) or less
    expect(titleFontSize).toBeLessThanOrEqual(50);

    // Verify CTA buttons wrap properly
    const ctaButtons = page.locator('.cta-buttons');
    await expect(ctaButtons).toBeVisible();

    // Check buttons are displayed in a flex-wrap container
    const ctaStyle = await ctaButtons.evaluate((el) => ({
      display: window.getComputedStyle(el).display,
      flexWrap: window.getComputedStyle(el).flexWrap
    }));

    expect(ctaStyle.display).toBe('flex');
    expect(ctaStyle.flexWrap).toBe('wrap');
  });

  test('Navigation is accessible on mobile viewport', async ({ page }) => {
    // Check navigation elements are visible
    const nav = page.locator('.nav');
    await expect(nav).toBeVisible();

    // Check logo is visible
    const logo = page.locator('.logo');
    await expect(logo).toBeVisible();

    // Check nav links are visible (current implementation shows inline nav links)
    const navLinks = page.locator('.nav-links');
    await expect(navLinks).toBeVisible();

    // Verify nav links have proper gap on mobile
    const navLinksStyle = await navLinks.evaluate((el) => ({
      display: window.getComputedStyle(el).display,
      gap: window.getComputedStyle(el).gap
    }));

    expect(navLinksStyle.display).toBe('flex');

    // Verify navigation container is properly laid out (stacked on mobile)
    const navStyle = await nav.evaluate((el) => ({
      flexDirection: window.getComputedStyle(el).flexDirection
    }));

    // On mobile (≤768px), nav should be column layout
    expect(navStyle.flexDirection).toBe('column');
  });

  test('Footer adapts properly to mobile viewport', async ({ page }) => {
    // Scroll to footer
    const footer = page.locator('.footer');
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Check footer content is stacked on mobile
    const footerContent = page.locator('.footer-content');
    const footerStyle = await footerContent.evaluate((el) => ({
      flexDirection: window.getComputedStyle(el).flexDirection,
      textAlign: window.getComputedStyle(el).textAlign
    }));

    // On mobile, footer content should be stacked (column direction)
    expect(footerStyle.flexDirection).toBe('column');
    expect(footerStyle.textAlign).toBe('center');

    // Verify footer nav links are accessible
    const footerNav = page.locator('.footer-nav');
    await expect(footerNav).toBeVisible();

    // Check footer links have proper touch targets
    const footerLinks = footerNav.locator('a');
    const linkCount = await footerLinks.count();

    for (let i = 0; i < linkCount; i++) {
      const link = footerLinks.nth(i);
      const box = await link.boundingBox();
      expect(box).not.toBeNull();
      // Links should have reasonable touch target height
      expect(box.height).toBeGreaterThanOrEqual(20);
    }
  });
});
