/**
 * Mobile Responsive Design E2E Tests
 * Owner: Scenario 8 - Responsive Design - Mobile
 *
 * Verifies landing page displays correctly on mobile devices (< 768px)
 * Tests include:
 * - Single-column layout
 * - Hamburger menu functionality
 * - Touch-friendly elements
 * - Code blocks horizontal scrolling
 * - Hero section CTA button stacking
 */

const { test, expect } = require('@playwright/test');

// Mobile viewport configuration (375px - iPhone SE/standard mobile)
const MOBILE_VIEWPORT = { width: 375, height: 667 };

test.describe('Responsive Design - Mobile (< 768px)', () => {
  test.use({ viewport: MOBILE_VIEWPORT });

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');
  });

  test('TC1: Page renders with single-column layout at 375px viewport width', async ({ page }) => {
    // Verify viewport is set correctly
    const viewportWidth = await page.evaluate(() => window.innerWidth);
    expect(viewportWidth).toBe(375);

    // Check main content sections are visible and properly sized
    const hero = page.locator('#hero');
    await expect(hero).toBeVisible();
    const heroBox = await hero.boundingBox();
    expect(heroBox.x).toBeGreaterThanOrEqual(0);

    // Features grid should be single column on mobile
    const featuresGrid = page.locator('.features__grid');
    if (await featuresGrid.count() > 0) {
      const gridStyle = await featuresGrid.evaluate(el =>
        window.getComputedStyle(el).gridTemplateColumns
      );
      // On mobile, should be single column (computed value will be a single pixel value)
      const columnCount = gridStyle.split(' ').filter(col => col !== '').length;
      expect(columnCount).toBe(1);
    }

    // Usage content should be single column
    const usageContent = page.locator('.usage__content');
    if (await usageContent.count() > 0) {
      const usageStyle = await usageContent.evaluate(el =>
        window.getComputedStyle(el).gridTemplateColumns
      );
      const columnCount = usageStyle.split(' ').filter(col => col !== '').length;
      expect(columnCount).toBe(1);
    }

    // Architecture components grid should be single column
    const archGrid = page.locator('.architecture__components-grid');
    if (await archGrid.count() > 0) {
      const archStyle = await archGrid.evaluate(el =>
        window.getComputedStyle(el).gridTemplateColumns
      );
      const columnCount = archStyle.split(' ').filter(col => col !== '').length;
      expect(columnCount).toBe(1);
    }
  });

  test('TC2: Hamburger menu appears on mobile and expands to full menu on tap', async ({ page }) => {
    // Hamburger menu should be visible
    const hamburger = page.locator('.nav-toggle');
    await expect(hamburger).toBeVisible();

    // Verify hamburger has proper aria attributes
    await expect(hamburger).toHaveAttribute('aria-expanded', 'false');
    await expect(hamburger).toHaveAttribute('aria-controls', 'nav-menu');
    await expect(hamburger).toHaveAttribute('aria-label', 'Toggle navigation menu');

    // Navigation menu should be initially hidden (off-screen)
    const navMenu = page.locator('.nav-menu');
    const initialBox = await navMenu.boundingBox();
    if (initialBox) {
      // Menu should be positioned off-screen to the right
      expect(initialBox.x).toBeGreaterThanOrEqual(MOBILE_VIEWPORT.width - 10);
    }

    // Tap hamburger to open menu
    await hamburger.click();
    await page.waitForTimeout(400); // Wait for animation

    // Menu should now be visible with is-open class
    await expect(navMenu).toHaveClass(/is-open/);
    await expect(hamburger).toHaveAttribute('aria-expanded', 'true');

    // Verify all nav links are visible in the expanded menu
    const navLinks = ['Features', 'Usage', 'Architecture', 'Get Started'];
    for (const linkText of navLinks) {
      const link = page.locator('.nav-menu .nav-link', { hasText: linkText });
      await expect(link).toBeVisible();
    }

    // Verify GitHub link is visible
    const githubLink = page.locator('.nav-github');
    await expect(githubLink).toBeVisible();

    // Overlay should be visible
    const overlay = page.locator('.nav-overlay');
    await expect(overlay).toHaveClass(/is-visible/);

    // Tap hamburger again to close menu
    await hamburger.click();
    await page.waitForTimeout(400);

    // Menu should be closed
    await expect(navMenu).not.toHaveClass(/is-open/);
    await expect(hamburger).toHaveAttribute('aria-expanded', 'false');
  });

  test('TC3: Touch-friendly elements meet accessibility requirements', async ({ page }) => {
    // Test CTA buttons in hero section - these must be 44px or larger
    const ctaButtons = page.locator('.hero-cta .btn');
    const buttonCount = await ctaButtons.count();
    expect(buttonCount).toBeGreaterThan(0);

    for (let i = 0; i < buttonCount; i++) {
      const button = ctaButtons.nth(i);
      const buttonBox = await button.boundingBox();
      // Hero buttons should have adequate touch target (44px minimum)
      expect(buttonBox.height).toBeGreaterThanOrEqual(44);
      // Buttons should also be wide enough for touch
      expect(buttonBox.width).toBeGreaterThanOrEqual(100);
    }

    // Test hamburger menu button has adequate size
    const hamburger = page.locator('.nav-toggle');
    const hamburgerBox = await hamburger.boundingBox();
    // Hamburger should be at least 30x30 (common minimum)
    expect(hamburgerBox.width).toBeGreaterThanOrEqual(30);
    expect(hamburgerBox.height).toBeGreaterThanOrEqual(30);

    // Test navigation links (when menu is open) have adequate touch targets
    await hamburger.click();
    await page.waitForTimeout(400);

    const navLinks = page.locator('.nav-menu .nav-link');
    const navLinkCount = await navLinks.count();
    for (let i = 0; i < navLinkCount; i++) {
      const link = navLinks.nth(i);
      const linkBox = await link.boundingBox();
      // Navigation links should have adequate height for touch
      expect(linkBox.height).toBeGreaterThanOrEqual(36);
    }

    // Close menu
    await hamburger.click();
    await page.waitForTimeout(400);
  });

  test('TC4: Code blocks have horizontal scroll capability', async ({ page }) => {
    // Navigate to usage section
    await page.locator('#usage').scrollIntoViewIfNeeded();
    await page.waitForTimeout(300);

    // Check code blocks exist and have proper overflow handling
    const codeBlocks = page.locator('.code-block');
    const codeBlockCount = await codeBlocks.count();
    expect(codeBlockCount).toBeGreaterThan(0);

    for (let i = 0; i < codeBlockCount; i++) {
      const codeBlock = codeBlocks.nth(i);
      const pre = codeBlock.locator('pre');

      // Check that pre element allows horizontal overflow
      const overflowX = await pre.evaluate(el =>
        window.getComputedStyle(el).overflowX
      );
      expect(['auto', 'scroll']).toContain(overflowX);

      // Code block container should be visible
      await expect(codeBlock).toBeVisible();
    }
  });

  test('TC5: Hero section CTA buttons stack vertically on mobile', async ({ page }) => {
    // Check hero CTA container
    const heroCta = page.locator('.hero-cta');
    await expect(heroCta).toBeVisible();

    // Check flex direction is column on mobile
    const flexDirection = await heroCta.evaluate(el =>
      window.getComputedStyle(el).flexDirection
    );
    expect(flexDirection).toBe('column');

    // Get both buttons
    const buttons = page.locator('.hero-cta .btn');
    await expect(buttons).toHaveCount(2);

    const primaryBtn = page.locator('.hero-cta .btn-primary');
    const secondaryBtn = page.locator('.hero-cta .btn-secondary');

    // Verify both buttons are visible
    await expect(primaryBtn).toBeVisible();
    await expect(secondaryBtn).toBeVisible();

    // Get button positions
    const primaryBox = await primaryBtn.boundingBox();
    const secondaryBox = await secondaryBtn.boundingBox();

    // Primary button should be above secondary button (lower Y = higher on page)
    expect(primaryBox.y).toBeLessThan(secondaryBox.y);

    // Buttons should be centered (similar X position, not side-by-side)
    const xDifference = Math.abs(primaryBox.x - secondaryBox.x);
    expect(xDifference).toBeLessThan(50); // Should be roughly aligned

    // Buttons should be accessible (visible and clickable)
    await expect(primaryBtn).toBeEnabled();
    await expect(secondaryBtn).toBeEnabled();
  });

  test('Hero logo scales down on mobile', async ({ page }) => {
    const heroLogo = page.locator('.hero-logo');
    await expect(heroLogo).toBeVisible();

    const logoBox = await heroLogo.boundingBox();
    // On mobile (max-width: 767px), logo should be 100x100
    expect(logoBox.width).toBeLessThanOrEqual(110);
    expect(logoBox.height).toBeLessThanOrEqual(110);
  });

  test('Hero title font size is reduced on mobile', async ({ page }) => {
    const heroTitle = page.locator('.hero-title');
    await expect(heroTitle).toBeVisible();

    const fontSize = await heroTitle.evaluate(el =>
      window.getComputedStyle(el).fontSize
    );

    // On mobile, font-size should be var(--font-size-4xl) = 2.25rem = 36px
    const fontSizePx = parseFloat(fontSize);
    expect(fontSizePx).toBeLessThanOrEqual(40); // Should be around 36px on mobile
  });

  test('Navigation overlay closes menu when clicked', async ({ page }) => {
    const hamburger = page.locator('.nav-toggle');
    const navMenu = page.locator('.nav-menu');
    const overlay = page.locator('.nav-overlay');

    // Open menu
    await hamburger.click();
    await page.waitForTimeout(400);

    await expect(navMenu).toHaveClass(/is-open/);
    await expect(overlay).toHaveClass(/is-visible/);

    // Click on the overlay element directly using JavaScript to ensure the click event fires
    await overlay.evaluate(el => el.click());
    await page.waitForTimeout(400);

    // Menu should be closed
    await expect(navMenu).not.toHaveClass(/is-open/);
  });

  test('Features section cards stack vertically on mobile', async ({ page }) => {
    // Scroll to features section
    await page.locator('#features').scrollIntoViewIfNeeded();
    await page.waitForTimeout(300);

    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThan(0);

    // Get positions of first two cards
    if (cardCount >= 2) {
      const firstCardBox = await featureCards.nth(0).boundingBox();
      const secondCardBox = await featureCards.nth(1).boundingBox();

      // Second card should be below first (higher Y value)
      expect(secondCardBox.y).toBeGreaterThan(firstCardBox.y);

      // Cards should have same X position (left-aligned in single column)
      const xDifference = Math.abs(firstCardBox.x - secondCardBox.x);
      expect(xDifference).toBeLessThan(10);
    }
  });

  test('All sections are reachable by scrolling', async ({ page }) => {
    // Scroll through entire page sections
    const sections = ['#hero', '#features', '#usage', '#architecture', '#getting-started', 'footer'];

    for (const selector of sections) {
      const section = page.locator(selector);
      if (await section.count() > 0) {
        await section.scrollIntoViewIfNeeded();
        await expect(section).toBeInViewport();
      }
    }
  });

  test('Text is readable on mobile without zooming', async ({ page }) => {
    // Check that base font size is at least 14px (minimum readable size)
    const bodyFontSize = await page.evaluate(() =>
      window.getComputedStyle(document.body).fontSize
    );
    const fontSizePx = parseFloat(bodyFontSize);
    expect(fontSizePx).toBeGreaterThanOrEqual(14);

    // Check hero tagline is readable
    const heroTagline = page.locator('.hero-tagline');
    if (await heroTagline.count() > 0) {
      const taglineFontSize = await heroTagline.evaluate(el =>
        window.getComputedStyle(el).fontSize
      );
      const taglinePx = parseFloat(taglineFontSize);
      expect(taglinePx).toBeGreaterThanOrEqual(14);
    }
  });

  test('Fixed navigation stays at top when scrolling', async ({ page }) => {
    // Get nav initial position
    const nav = page.locator('.main-nav');
    const navBoxInitial = await nav.boundingBox();
    expect(navBoxInitial.y).toBe(0);

    // Scroll down and verify nav stays fixed at top
    await page.evaluate(() => window.scrollTo(0, 500));
    await page.waitForTimeout(100);

    const navBoxAfterScroll = await nav.boundingBox();
    expect(navBoxAfterScroll.y).toBe(0); // Should still be at top
  });
});

test.describe('Mobile Responsive - Additional Viewports', () => {
  test('Page works on large mobile (414px width)', async ({ page }) => {
    await page.setViewportSize({ width: 414, height: 896 });
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Hamburger visible (still below 768px breakpoint)
    const hamburger = page.locator('.nav-toggle');
    await expect(hamburger).toBeVisible();

    // Hero content visible
    await expect(page.locator('.hero-title')).toBeVisible();

    // Single column layout for features
    const featuresGrid = page.locator('.features__grid');
    if (await featuresGrid.count() > 0) {
      const gridStyle = await featuresGrid.evaluate(el =>
        window.getComputedStyle(el).gridTemplateColumns
      );
      const columnCount = gridStyle.split(' ').filter(col => col !== '').length;
      expect(columnCount).toBe(1);
    }
  });

  test('Page correctly shows mobile layout at 767px breakpoint', async ({ page }) => {
    await page.setViewportSize({ width: 767, height: 1024 });
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // At 767px (just below tablet), hamburger should still be visible
    const hamburger = page.locator('.nav-toggle');
    await expect(hamburger).toBeVisible();

    // Hero CTA should be stacked
    const heroCta = page.locator('.hero-cta');
    const flexDirection = await heroCta.evaluate(el =>
      window.getComputedStyle(el).flexDirection
    );
    expect(flexDirection).toBe('column');
  });

  test('Navigation menu works correctly on iPhone 12 viewport', async ({ page }) => {
    await page.setViewportSize({ width: 390, height: 844 });
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    const hamburger = page.locator('.nav-toggle');
    await expect(hamburger).toBeVisible();

    // Open menu
    await hamburger.click();
    await page.waitForTimeout(400);

    const navMenu = page.locator('.nav-menu');
    await expect(navMenu).toHaveClass(/is-open/);

    // All nav links should be visible
    await expect(page.locator('.nav-menu .nav-link', { hasText: 'Features' })).toBeVisible();
    await expect(page.locator('.nav-menu .nav-link', { hasText: 'Usage' })).toBeVisible();
    await expect(page.locator('.nav-menu .nav-link', { hasText: 'Architecture' })).toBeVisible();
    await expect(page.locator('.nav-menu .nav-link', { hasText: 'Get Started' })).toBeVisible();
  });
});
