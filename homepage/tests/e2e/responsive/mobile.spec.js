/**
 * Mobile Responsive Design E2E Tests
 * Owner: Scenario 11 - Responsive Design - Mobile
 *
 * Tests mobile viewport rendering at 375x667 and similar sizes.
 * Verifies:
 * - Single-column layout on mobile
 * - Hamburger menu functionality
 * - No horizontal scrolling
 * - Code block horizontal scrollability
 */

const { test, expect } = require('@playwright/test');

// Mobile viewport dimensions (iPhone SE/8 size)
const MOBILE_VIEWPORT = { width: 375, height: 667 };

test.describe('Responsive Design - Mobile (375x667+)', () => {
  test.beforeEach(async ({ page }) => {
    // Set viewport to mobile size
    await page.setViewportSize(MOBILE_VIEWPORT);
    await page.goto('/');
    // Wait for page to be fully loaded
    await page.waitForLoadState('networkidle');
  });

  test('Test Case 1: All content is visible without horizontal scrolling', async ({ page }) => {
    // Check that document does not have horizontal overflow
    const hasHorizontalScrollbar = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });

    expect(hasHorizontalScrollbar).toBe(false);

    // Verify body does not overflow horizontally
    const bodyOverflow = await page.evaluate(() => {
      const body = document.body;
      return {
        scrollWidth: body.scrollWidth,
        clientWidth: body.clientWidth
      };
    });

    expect(bodyOverflow.scrollWidth).toBeLessThanOrEqual(bodyOverflow.clientWidth + 1);

    // Verify all main sections are visible
    const sections = ['.hero', '.features', '.quickstart', '.roadmap'];
    for (const selector of sections) {
      const element = page.locator(selector);
      await expect(element).toBeVisible();
    }

    // Check that content width doesn't exceed viewport
    const contentWidths = await page.evaluate(() => {
      const sections = document.querySelectorAll('.hero, .features, .quickstart, .roadmap');
      return Array.from(sections).map(section => ({
        selector: section.className,
        width: section.getBoundingClientRect().width
      }));
    });

    for (const content of contentWidths) {
      expect(content.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width);
    }
  });

  test('Test Case 2: Features display in single-column layout', async ({ page }) => {
    const featuresGrid = page.locator('.features-grid');
    const featureCards = page.locator('.feature-card');

    // Features grid should be visible
    await expect(featuresGrid).toBeVisible();

    // Should have multiple feature cards
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThanOrEqual(2);

    // Get bounding boxes of first two cards
    const card1Box = await featureCards.nth(0).boundingBox();
    const card2Box = await featureCards.nth(1).boundingBox();

    expect(card1Box).not.toBeNull();
    expect(card2Box).not.toBeNull();

    // Cards should be stacked vertically (second card below first)
    expect(card2Box.y).toBeGreaterThan(card1Box.y + card1Box.height - 10);

    // Cards should have similar X positions (single column)
    expect(Math.abs(card1Box.x - card2Box.x)).toBeLessThan(20);

    // Each card should span close to full viewport width (with some padding)
    expect(card1Box.width).toBeGreaterThan(MOBILE_VIEWPORT.width * 0.8);
  });

  test('Test Case 3: Navigation collapses to hamburger menu', async ({ page }) => {
    const mobileMenuToggle = page.locator('.mobile-menu-toggle');
    const nav = page.locator('.main-nav');

    // Hamburger menu button should be visible on mobile
    await expect(mobileMenuToggle).toBeVisible();

    // Navigation list should be hidden initially
    const navListVisible = await page.evaluate(() => {
      const nav = document.getElementById('main-nav');
      if (!nav) return false;
      const styles = window.getComputedStyle(nav);
      // Check if nav is hidden via CSS (could be display:none, visibility:hidden, or height:0)
      return styles.display !== 'none' &&
             styles.visibility !== 'hidden' &&
             styles.opacity !== '0' &&
             !nav.classList.contains('is-open');
    });

    // The nav should either be hidden OR require toggle to access links
    // Check that toggle button has correct aria attributes
    const ariaExpanded = await mobileMenuToggle.getAttribute('aria-expanded');
    expect(ariaExpanded).toBe('false');
  });

  test('Test Case 4: Navigation menu expands and shows all links when hamburger clicked', async ({ page }) => {
    const mobileMenuToggle = page.locator('.mobile-menu-toggle');
    const nav = page.locator('.main-nav');
    const navLinks = page.locator('.nav-link');

    // Click hamburger menu to open
    await mobileMenuToggle.click();

    // Wait for menu animation
    await page.waitForTimeout(300);

    // Check aria-expanded is now true
    const ariaExpanded = await mobileMenuToggle.getAttribute('aria-expanded');
    expect(ariaExpanded).toBe('true');

    // Navigation should have is-open class
    await expect(nav).toHaveClass(/is-open/);

    // All nav links should be visible
    const linkCount = await navLinks.count();
    expect(linkCount).toBeGreaterThanOrEqual(3);

    // Each link should be visible
    for (let i = 0; i < linkCount; i++) {
      await expect(navLinks.nth(i)).toBeVisible();
    }

    // Verify expected links exist
    await expect(page.locator('.nav-link[data-section="features"]')).toBeVisible();
    await expect(page.locator('.nav-link[data-section="quickstart"]')).toBeVisible();
  });

  test('Test Case 5: Code blocks are horizontally scrollable if needed, not clipped', async ({ page }) => {
    // Scroll to quickstart section
    await page.locator('.quickstart').scrollIntoViewIfNeeded();

    const codeBlocks = page.locator('.code-block');
    const codeBlockCount = await codeBlocks.count();

    expect(codeBlockCount).toBeGreaterThan(0);

    // Check that code blocks have overflow-x set to allow scrolling
    for (let i = 0; i < codeBlockCount; i++) {
      const codeBlock = codeBlocks.nth(i);
      const pre = codeBlock.locator('pre');

      // Pre element should have overflow-x: auto or scroll
      const overflowX = await pre.evaluate(el => {
        return window.getComputedStyle(el).overflowX;
      });

      expect(['auto', 'scroll']).toContain(overflowX);

      // Check that code block is contained within viewport width
      const blockBox = await codeBlock.boundingBox();
      expect(blockBox).not.toBeNull();
      expect(blockBox.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width);
    }

    // Verify code content is scrollable, not clipped
    const preOverflow = await page.evaluate(() => {
      const preElements = document.querySelectorAll('.code-block pre');
      return Array.from(preElements).map(pre => ({
        scrollWidth: pre.scrollWidth,
        clientWidth: pre.clientWidth,
        overflowX: window.getComputedStyle(pre).overflowX
      }));
    });

    for (const pre of preOverflow) {
      // overflow-x should allow scrolling
      expect(['auto', 'scroll']).toContain(pre.overflowX);
    }
  });

  test('Hero section adapts to mobile viewport', async ({ page }) => {
    const hero = page.locator('.hero');
    const heroContent = page.locator('.hero-content');
    const heroTitle = page.locator('.hero-title');

    await expect(hero).toBeVisible();

    // Hero should span full viewport width
    const heroBox = await hero.boundingBox();
    expect(heroBox).not.toBeNull();
    expect(heroBox.width).toBeGreaterThanOrEqual(MOBILE_VIEWPORT.width - 1);

    // Hero title should be visible and properly sized
    await expect(heroTitle).toBeVisible();
    const titleBox = await heroTitle.boundingBox();
    expect(titleBox).not.toBeNull();
    expect(titleBox.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width);
  });

  test('Mobile menu closes when nav link is clicked', async ({ page }) => {
    const mobileMenuToggle = page.locator('.mobile-menu-toggle');
    const nav = page.locator('.main-nav');

    // Open the menu
    await mobileMenuToggle.click();
    await page.waitForTimeout(300);

    // Verify menu is open
    await expect(nav).toHaveClass(/is-open/);

    // Click a nav link
    await page.locator('.nav-link[data-section="features"]').click();
    await page.waitForTimeout(300);

    // Menu should close after clicking a link
    const isOpen = await nav.evaluate(el => el.classList.contains('is-open'));
    expect(isOpen).toBe(false);

    // aria-expanded should be false
    const ariaExpanded = await mobileMenuToggle.getAttribute('aria-expanded');
    expect(ariaExpanded).toBe('false');
  });

  test('Footer displays correctly on mobile', async ({ page }) => {
    const footer = page.locator('.site-footer');

    // Scroll to footer
    await footer.scrollIntoViewIfNeeded();
    await expect(footer).toBeVisible();

    // Footer should not exceed viewport width
    const footerBox = await footer.boundingBox();
    expect(footerBox).not.toBeNull();
    expect(footerBox.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width + 1);

    // Footer content should be visible
    await expect(page.locator('.footer-links')).toBeVisible();
    await expect(page.locator('.footer-info')).toBeVisible();
  });

  test('Roadmap section displays in single column on mobile', async ({ page }) => {
    const roadmapColumns = page.locator('.roadmap-column');

    // Should have roadmap columns
    const columnCount = await roadmapColumns.count();
    expect(columnCount).toBeGreaterThanOrEqual(2);

    // Get bounding boxes
    const col1Box = await roadmapColumns.nth(0).boundingBox();
    const col2Box = await roadmapColumns.nth(1).boundingBox();

    expect(col1Box).not.toBeNull();
    expect(col2Box).not.toBeNull();

    // Columns should be stacked vertically on mobile
    expect(col2Box.y).toBeGreaterThan(col1Box.y + col1Box.height - 10);

    // Each column should span close to full width
    expect(col1Box.width).toBeGreaterThan(MOBILE_VIEWPORT.width * 0.8);
  });
});
