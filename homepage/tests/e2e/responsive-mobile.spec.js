/**
 * Mobile Responsive E2E Tests
 * Owner: Scenario 9 - Responsive Design Mobile
 *
 * Viewport: < 768px (375px iPhone)
 *
 * Tests:
 * - Single column layout
 * - Hamburger menu
 * - Touch target sizes (44px min)
 * - No horizontal scrolling
 *
 * Requirements: NFR-2
 */

const { test, expect } = require('@playwright/test');

// Mobile viewport configuration (iPhone 12/13 size)
const mobileViewport = { width: 375, height: 812 };

test.describe('Mobile Responsive Design (<768px)', () => {
  test.beforeEach(async ({ page }) => {
    // Set mobile viewport
    await page.setViewportSize(mobileViewport);
    await page.goto('/');
    // Wait for page to fully load
    await page.waitForLoadState('networkidle');
  });

  test('TC1: Page renders without horizontal scrolling at 375px', async ({ page }) => {
    // Check that there is no horizontal scrollbar
    const scrollWidth = await page.evaluate(() => document.documentElement.scrollWidth);
    const clientWidth = await page.evaluate(() => document.documentElement.clientWidth);

    // Allow for 1px tolerance due to rounding
    expect(scrollWidth).toBeLessThanOrEqual(clientWidth + 1);

    // Verify main content is visible
    await expect(page.locator('#hero')).toBeVisible();
    await expect(page.locator('#features')).toBeVisible();

    // Check that hero section is fully visible
    const heroBox = await page.locator('#hero').boundingBox();
    expect(heroBox).not.toBeNull();
    expect(heroBox.width).toBeLessThanOrEqual(mobileViewport.width);
  });

  test('TC2: Hamburger menu is visible and horizontal nav is hidden on mobile', async ({ page }) => {
    // Hamburger menu should be visible
    const hamburgerMenu = page.locator('.hamburger-menu');
    await expect(hamburgerMenu).toBeVisible();

    // Horizontal navigation links should be hidden
    const navLinks = page.locator('.nav-links');
    await expect(navLinks).toBeHidden();
  });

  test('TC3: Tap hamburger menu expands mobile navigation', async ({ page }) => {
    const hamburgerMenu = page.locator('.hamburger-menu');

    // Click hamburger menu
    await hamburgerMenu.click();

    // Mobile navigation should be visible
    const mobileNav = page.locator('.mobile-nav');
    await expect(mobileNav).toBeVisible();

    // All navigation links should be visible in mobile menu
    const mobileNavLinks = mobileNav.locator('a');
    const linkCount = await mobileNavLinks.count();
    expect(linkCount).toBeGreaterThanOrEqual(4); // Features, Architecture, Quick Start, Comparison, GitHub

    // Each link should be visible
    for (let i = 0; i < linkCount; i++) {
      await expect(mobileNavLinks.nth(i)).toBeVisible();
    }

    // Verify aria-expanded attribute is updated
    await expect(hamburgerMenu).toHaveAttribute('aria-expanded', 'true');

    // Click again to close
    await hamburgerMenu.click();
    // Wait for menu to close - check that aria-expanded is false and class is-open is removed
    await expect(hamburgerMenu).toHaveAttribute('aria-expanded', 'false');
    await expect(mobileNav).not.toHaveClass(/is-open/);
  });

  test('TC4: Feature cards display in single-column layout on mobile', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();

    // Get all feature cards
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBe(6); // Should have 6 feature cards

    // Get positions of first two cards to verify vertical stacking
    const firstCard = await featureCards.nth(0).boundingBox();
    const secondCard = await featureCards.nth(1).boundingBox();

    expect(firstCard).not.toBeNull();
    expect(secondCard).not.toBeNull();

    // Cards should be stacked vertically (second card below first)
    expect(secondCard.y).toBeGreaterThan(firstCard.y + firstCard.height - 10);

    // Cards should have similar x position (single column)
    expect(Math.abs(firstCard.x - secondCard.x)).toBeLessThan(10);

    // Each card should span most of the viewport width (single column)
    expect(firstCard.width).toBeGreaterThan(mobileViewport.width * 0.8);
  });

  test('TC5: CTA buttons have minimum 44px touch target height', async ({ page }) => {
    // Get the Get Started button
    const getStartedBtn = page.locator('[data-testid="cta-get-started"]');
    await expect(getStartedBtn).toBeVisible();
    const getStartedBox = await getStartedBtn.boundingBox();

    // Get the GitHub button
    const githubBtn = page.locator('[data-testid="cta-github"]');
    await expect(githubBtn).toBeVisible();
    const githubBox = await githubBtn.boundingBox();

    // Both buttons should have minimum 44px height (accessibility requirement)
    expect(getStartedBox.height).toBeGreaterThanOrEqual(44);
    expect(githubBox.height).toBeGreaterThanOrEqual(44);

    // Verify the min-height CSS property
    const getStartedMinHeight = await getStartedBtn.evaluate(el =>
      parseInt(getComputedStyle(el).minHeight, 10)
    );
    const githubMinHeight = await githubBtn.evaluate(el =>
      parseInt(getComputedStyle(el).minHeight, 10)
    );

    expect(getStartedMinHeight).toBeGreaterThanOrEqual(44);
    expect(githubMinHeight).toBeGreaterThanOrEqual(44);
  });

  test('TC6: Code blocks have horizontal scroll on mobile', async ({ page }) => {
    // Navigate to Quick Start section which has code blocks
    const quickstartSection = page.locator('#quickstart');
    await quickstartSection.scrollIntoViewIfNeeded();

    // Get code blocks
    const codeBlocks = page.locator('#quickstart pre');
    const blockCount = await codeBlocks.count();
    expect(blockCount).toBeGreaterThan(0);

    // Check that code blocks have overflow-x set to auto or scroll
    const firstCodeBlock = codeBlocks.first();
    const overflowX = await firstCodeBlock.evaluate(el =>
      getComputedStyle(el).overflowX
    );

    expect(['auto', 'scroll']).toContain(overflowX);

    // Verify code block width doesn't exceed viewport
    const codeBlockBox = await firstCodeBlock.boundingBox();
    expect(codeBlockBox.width).toBeLessThanOrEqual(mobileViewport.width);
  });

  test('Header is sticky and visible while scrolling on mobile', async ({ page }) => {
    // Scroll down the page
    await page.evaluate(() => window.scrollTo(0, 500));
    await page.waitForTimeout(300);

    // Header should still be visible (fixed position)
    const header = page.locator('.header');
    await expect(header).toBeVisible();

    // Header should be at the top of viewport
    const headerBox = await header.boundingBox();
    expect(headerBox.y).toBe(0);
  });

  test('Touch target sizes meet accessibility standards for all interactive elements', async ({ page }) => {
    // Check hamburger menu touch target
    const hamburgerMenu = page.locator('.hamburger-menu');
    const hamburgerBox = await hamburgerMenu.boundingBox();
    expect(hamburgerBox.width).toBeGreaterThanOrEqual(44);
    expect(hamburgerBox.height).toBeGreaterThanOrEqual(44);

    // Check navigation links in mobile menu
    await hamburgerMenu.click();
    const mobileNavLinks = page.locator('.mobile-nav a');
    const linkCount = await mobileNavLinks.count();

    for (let i = 0; i < linkCount; i++) {
      const linkBox = await mobileNavLinks.nth(i).boundingBox();
      // Mobile nav links should have adequate touch target
      expect(linkBox.height).toBeGreaterThanOrEqual(44);
    }
  });

  test('Footer adapts to single column on mobile', async ({ page }) => {
    // Scroll to footer
    const footer = page.locator('footer');
    await footer.scrollIntoViewIfNeeded();

    // Footer should be visible
    await expect(footer).toBeVisible();

    // Check footer sections are stacked
    const footerContent = footer.locator('.footer-content');
    const footerWidth = await footerContent.evaluate(el => el.offsetWidth);

    // Footer content should be close to full width on mobile
    expect(footerWidth).toBeLessThanOrEqual(mobileViewport.width);
  });

  test('Hero section adapts typography for mobile', async ({ page }) => {
    const heroTitle = page.locator('.hero h1');
    await expect(heroTitle).toBeVisible();

    // Check font size is reduced on mobile
    const fontSize = await heroTitle.evaluate(el =>
      parseFloat(getComputedStyle(el).fontSize)
    );

    // Mobile font size should be smaller than desktop (which is var(--font-size-5xl) = 3rem = 48px)
    // On mobile it should be var(--font-size-4xl) = 2.25rem = 36px
    expect(fontSize).toBeLessThanOrEqual(40); // Some tolerance for different calculations
    expect(fontSize).toBeGreaterThanOrEqual(30); // But still readable
  });

  test('Configuration table scrolls horizontally on mobile', async ({ page }) => {
    // Navigate to configuration section
    const configSection = page.locator('#configuration');
    await configSection.scrollIntoViewIfNeeded();

    // Get table wrapper
    const tableWrapper = page.locator('.config-table-wrapper');

    // Check that the wrapper allows horizontal scroll
    const overflowX = await tableWrapper.evaluate(el =>
      getComputedStyle(el).overflowX
    );

    expect(['auto', 'scroll']).toContain(overflowX);

    // Table wrapper shouldn't exceed viewport width
    const wrapperBox = await tableWrapper.boundingBox();
    expect(wrapperBox.width).toBeLessThanOrEqual(mobileViewport.width);
  });
});
