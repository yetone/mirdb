// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Mobile Responsive Design E2E Tests (NFR-1)
 * Verifies the page is fully responsive on mobile devices
 */

test.describe('Mobile Responsive Design', () => {
  // Test Case 1: Page renders without horizontal scrollbar at 320px viewport
  test('TC1: Page renders without horizontal scrollbar at 320px viewport width', async ({ page }) => {
    // Set viewport to smallest mobile width (320px)
    await page.setViewportSize({ width: 320, height: 568 });
    await page.goto('/');

    // Wait for page to fully load
    await page.waitForLoadState('networkidle');

    // Check that body/html doesn't overflow horizontally
    const hasHorizontalScrollbar = await page.evaluate(() => {
      const body = document.body;
      const html = document.documentElement;
      // Check if content width exceeds viewport width
      return body.scrollWidth > window.innerWidth || html.scrollWidth > window.innerWidth;
    });

    expect(hasHorizontalScrollbar).toBe(false);
  });

  // Test Case 2: All content is visible and readable at 375px viewport (iPhone)
  test('TC2: All content is visible and readable at 375px viewport width (iPhone)', async ({ page }) => {
    // Set viewport to iPhone width (375px)
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');

    // Wait for page to fully load
    await page.waitForLoadState('networkidle');

    // Check main sections are visible
    await expect(page.locator('nav')).toBeVisible();
    await expect(page.locator('#hero')).toBeVisible();
    await expect(page.locator('#features')).toBeVisible();
    await expect(page.locator('#quick-start')).toBeVisible();
    await expect(page.locator('#commands')).toBeVisible();
    await expect(page.locator('#configuration')).toBeVisible();
    await expect(page.locator('footer')).toBeVisible();

    // Verify text is readable (font size is at least 12px for body text)
    const minFontSize = await page.evaluate(() => {
      const paragraphs = document.querySelectorAll('p, li, td');
      let smallestFont = Infinity;
      paragraphs.forEach(el => {
        const fontSize = parseFloat(window.getComputedStyle(el).fontSize);
        if (fontSize < smallestFont) smallestFont = fontSize;
      });
      return smallestFont;
    });

    expect(minFontSize).toBeGreaterThanOrEqual(12);
  });

  // Test Case 3: Navigation collapses to hamburger menu at mobile viewport
  test('TC3: Navigation collapses to hamburger menu or mobile-friendly alternative at mobile viewport', async ({ page }) => {
    // Set viewport to mobile width
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');

    // Wait for page to fully load
    await page.waitForLoadState('networkidle');

    // Check for hamburger menu button OR mobile-friendly navigation
    const hamburgerButton = page.locator('.hamburger-menu, .mobile-menu-button, [aria-label*="menu"], button[class*="menu"]');
    const mobileNav = page.locator('.mobile-nav, .nav-mobile, nav.mobile');
    const navLinks = page.locator('.nav-links');

    // Either hamburger menu should be visible, OR nav links should be styled for mobile
    const hasHamburger = await hamburgerButton.isVisible().catch(() => false);
    const hasMobileNav = await mobileNav.isVisible().catch(() => false);

    if (hasHamburger) {
      // Hamburger menu pattern
      await expect(hamburgerButton).toBeVisible();
    } else if (hasMobileNav) {
      // Mobile navigation component pattern
      await expect(mobileNav).toBeVisible();
    } else {
      // Fallback: nav links should exist and be mobile-friendly
      // Mobile-friendly means they wrap or stack vertically
      const navLinksStyles = await navLinks.evaluate(el => {
        const styles = window.getComputedStyle(el);
        return {
          flexWrap: styles.flexWrap,
          flexDirection: styles.flexDirection
        };
      });

      // Should wrap or be in column direction for mobile
      const isMobileFriendly = navLinksStyles.flexWrap === 'wrap' || navLinksStyles.flexDirection === 'column';
      expect(isMobileFriendly).toBe(true);
    }
  });

  // Test Case 4: Mobile navigation menu opens and displays all navigation links
  test('TC4: Mobile navigation menu opens and displays all navigation links', async ({ page }) => {
    // Set viewport to mobile width
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');

    // Wait for page to fully load
    await page.waitForLoadState('networkidle');

    // Check for hamburger menu button
    const hamburgerButton = page.locator('.hamburger-menu, .mobile-menu-button, [aria-label*="menu"], button[class*="menu"]');
    const hasHamburger = await hamburgerButton.isVisible().catch(() => false);

    if (hasHamburger) {
      // Click hamburger to open menu
      await hamburgerButton.click();

      // Wait for menu animation
      await page.waitForTimeout(300);

      // Verify all navigation links are visible
      const featuresLink = page.locator('a[href="#features"]');
      const quickStartLink = page.locator('a[href="#quick-start"]');
      const commandsLink = page.locator('a[href="#commands"]');
      const configLink = page.locator('a[href="#configuration"]');

      await expect(featuresLink).toBeVisible();
      await expect(quickStartLink).toBeVisible();
      await expect(commandsLink).toBeVisible();
      await expect(configLink).toBeVisible();
    } else {
      // If no hamburger, all links should already be visible in mobile-friendly layout
      const featuresLink = page.locator('nav a[href="#features"]');
      const quickStartLink = page.locator('nav a[href="#quick-start"]');
      const commandsLink = page.locator('nav a[href="#commands"]');
      const configLink = page.locator('nav a[href="#configuration"]');

      await expect(featuresLink).toBeVisible();
      await expect(quickStartLink).toBeVisible();
      await expect(commandsLink).toBeVisible();
      await expect(configLink).toBeVisible();
    }
  });

  // Test Case 5: Code blocks scroll horizontally within their containers at mobile viewport
  test('TC5: Code blocks scroll horizontally within their containers at mobile viewport', async ({ page }) => {
    // Set viewport to mobile width
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');

    // Navigate to Quick Start section where code blocks are
    await page.locator('#quick-start').scrollIntoViewIfNeeded();

    // Check that code blocks have overflow-x: auto or scroll
    const codeBlocks = page.locator('pre');
    const count = await codeBlocks.count();
    expect(count).toBeGreaterThan(0);

    // Verify code blocks are scrollable horizontally
    for (let i = 0; i < Math.min(count, 3); i++) {
      const codeBlock = codeBlocks.nth(i);
      const overflowX = await codeBlock.evaluate(el => {
        const styles = window.getComputedStyle(el);
        return styles.overflowX;
      });

      // Should have horizontal scroll capability
      expect(['auto', 'scroll']).toContain(overflowX);
    }
  });

  // Test Case 6: Hero section content stacks vertically and remains readable at mobile viewport
  test('TC6: Hero section content stacks vertically and remains readable at mobile viewport', async ({ page }) => {
    // Set viewport to mobile width
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');

    // Verify hero section is visible and readable
    const hero = page.locator('#hero');
    await expect(hero).toBeVisible();

    // Check hero h1
    const heroH1 = hero.locator('h1');
    await expect(heroH1).toBeVisible();
    const h1Text = await heroH1.textContent();
    expect(h1Text).toBeTruthy();

    // Check tagline
    const tagline = hero.locator('.tagline');
    await expect(tagline).toBeVisible();

    // Check description
    const description = hero.locator('.description');
    await expect(description).toBeVisible();

    // Check CTA buttons stack vertically
    const ctaButtons = hero.locator('.cta-buttons');
    const ctaFlexDirection = await ctaButtons.evaluate(el => {
      const styles = window.getComputedStyle(el);
      return styles.flexDirection;
    });

    // CTA buttons should stack vertically (column) on mobile
    expect(ctaFlexDirection).toBe('column');

    // Verify hero elements are within viewport width (not overflowing)
    const heroBox = await hero.boundingBox();
    expect(heroBox?.width).toBeLessThanOrEqual(375);
  });

  // Test Case 7: Feature cards stack vertically in single column at mobile viewport
  test('TC7: Feature cards stack vertically in single column at mobile viewport', async ({ page }) => {
    // Set viewport to mobile width
    await page.setViewportSize({ width: 375, height: 667 });
    await page.goto('/');

    // Navigate to features section
    const featuresSection = page.locator('#features');
    await featuresSection.scrollIntoViewIfNeeded();

    // Get the features grid
    const featuresGrid = featuresSection.locator('.features-grid');
    await expect(featuresGrid).toBeVisible();

    // Get feature cards
    const featureCards = featuresGrid.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBeGreaterThan(0);

    // Check that cards are stacked vertically (single column)
    // Get positions of first two cards
    if (cardCount >= 2) {
      const card1Box = await featureCards.nth(0).boundingBox();
      const card2Box = await featureCards.nth(1).boundingBox();

      // Cards should be stacked vertically - card2 should be below card1
      // (card2's y position should be greater than card1's y + height)
      expect(card2Box?.y).toBeGreaterThan((card1Box?.y ?? 0) + (card1Box?.height ?? 0) - 10);

      // Cards should have similar x positions (single column)
      expect(Math.abs((card1Box?.x ?? 0) - (card2Box?.x ?? 0))).toBeLessThan(50);
    }

    // Verify each card fits within viewport width
    for (let i = 0; i < Math.min(cardCount, 3); i++) {
      const cardBox = await featureCards.nth(i).boundingBox();
      expect(cardBox?.width).toBeLessThanOrEqual(375);
    }
  });
});
