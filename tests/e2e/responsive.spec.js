/**
 * Responsive Design E2E Tests
 * Owner: Scenario 7 - Responsive Design
 *
 * Tests:
 * - Mobile viewport (375px): no horizontal scroll, single column, hamburger menu, touch targets >= 44x44px
 * - Tablet viewport (768px): adapted layout, 2-column features, navigation visible
 * - Desktop viewport (1024px+): full layout, horizontal nav, multi-column features
 * - Orientation change from portrait to landscape
 * - Text readability at all breakpoints
 */

const { test, expect } = require('@playwright/test');

test.describe('Responsive Design', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for page to be fully loaded
    await page.waitForLoadState('networkidle');
  });

  test('should have no horizontal scrolling and single column layout on mobile (375px)', async ({ page }) => {
    // Set viewport to mobile size
    await page.setViewportSize({ width: 375, height: 812 });

    // Wait for layout to settle
    await page.waitForTimeout(200);

    // Verify no horizontal scrolling
    const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
    const viewportWidth = await page.evaluate(() => window.innerWidth);
    expect(bodyWidth).toBeLessThanOrEqual(viewportWidth + 1);

    // Verify document doesn't overflow horizontally
    const htmlWidth = await page.evaluate(() => document.documentElement.scrollWidth);
    expect(htmlWidth).toBeLessThanOrEqual(viewportWidth + 1);

    // Feature cards should be in a single column (stacked vertically)
    const cards = page.locator('#features article.feature-card');
    const count = await cards.count();
    expect(count).toBeGreaterThanOrEqual(3);

    const firstCard = await cards.nth(0).boundingBox();
    const secondCard = await cards.nth(1).boundingBox();

    // Cards should be stacked vertically
    expect(secondCard.y).toBeGreaterThan(firstCard.y);

    // Each card should roughly span the full width minus padding
    expect(firstCard.width).toBeGreaterThan(viewportWidth * 0.7);
  });

  test('should show hamburger menu and hide desktop nav on mobile (375px)', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 812 });
    await page.waitForTimeout(200);

    // Hamburger menu toggle should be visible
    const hamburgerToggle = page.locator('[data-testid="mobile-menu-toggle"]');
    await expect(hamburgerToggle).toBeVisible();

    // Hamburger toggle should have minimum touch target size (44x44px)
    const toggleBox = await hamburgerToggle.boundingBox();
    expect(toggleBox.width).toBeGreaterThanOrEqual(44);
    expect(toggleBox.height).toBeGreaterThanOrEqual(44);

    // Desktop nav should be hidden on mobile
    const desktopNav = page.locator('[data-testid="desktop-nav"]');
    await expect(desktopNav).not.toBeVisible();

    // Mobile menu should be hidden by default
    const mobileMenu = page.locator('[data-testid="mobile-menu"]');
    const isVisible = await mobileMenu.isVisible().catch(() => false);
    expect(isVisible).toBe(false);
  });

  test('should open mobile menu when hamburger is clicked', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 812 });
    await page.waitForTimeout(200);

    const hamburgerToggle = page.locator('[data-testid="mobile-menu-toggle"]');
    await expect(hamburgerToggle).toBeVisible();

    // Click the hamburger toggle
    await hamburgerToggle.click();
    await page.waitForTimeout(100);

    // Mobile menu should now be visible
    const mobileMenu = page.locator('[data-testid="mobile-menu"]');
    await expect(mobileMenu).toBeVisible();

    // Mobile nav links should be visible
    const mobileLogin = page.locator('[data-testid="mobile-login"]');
    const mobileSignup = page.locator('[data-testid="mobile-signup"]');
    await expect(mobileLogin).toBeVisible();
    await expect(mobileSignup).toBeVisible();

    // Mobile nav links should have minimum touch target size
    const loginBox = await mobileLogin.boundingBox();
    expect(loginBox.width).toBeGreaterThanOrEqual(44);
    expect(loginBox.height).toBeGreaterThanOrEqual(44);
  });

  test('should have touch-friendly buttons (>= 44x44px) on mobile (375px)', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 812 });
    await page.waitForTimeout(200);

    // Check all buttons on the page
    const buttons = page.locator('.btn, button, a.btn');
    const count = await buttons.count();

    for (let i = 0; i < count; i++) {
      const btn = buttons.nth(i);
      const isVisible = await btn.isVisible().catch(() => false);
      if (!isVisible) continue;

      const box = await btn.boundingBox();
      if (box && box.width > 0 && box.height > 0) {
        // Touch targets should be at least 44x44px
        expect(box.width).toBeGreaterThanOrEqual(44);
        expect(box.height).toBeGreaterThanOrEqual(44);
      }
    }

    // Check footer links
    const footerLinks = page.locator('footer a');
    const footerCount = await footerLinks.count();
    for (let i = 0; i < footerCount; i++) {
      const link = footerLinks.nth(i);
      const isVisible = await link.isVisible().catch(() => false);
      if (!isVisible) continue;

      const box = await link.boundingBox();
      if (box && box.width > 0 && box.height > 0) {
        expect(box.height).toBeGreaterThanOrEqual(44);
      }
    }
  });

  test('should have adapted 2-column layout on tablet (768px)', async ({ page }) => {
    await page.setViewportSize({ width: 768, height: 1024 });
    await page.waitForTimeout(200);

    // No horizontal scrolling
    const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
    const viewportWidth = await page.evaluate(() => window.innerWidth);
    expect(bodyWidth).toBeLessThanOrEqual(viewportWidth + 1);

    // Navigation should be visible (horizontal nav)
    const navLogin = page.locator('[data-testid="nav-login"]');
    const navSignup = page.locator('[data-testid="nav-signup"]');
    await expect(navLogin).toBeVisible();
    await expect(navSignup).toBeVisible();

    // Hamburger menu should be hidden on tablet
    const hamburgerToggle = page.locator('[data-testid="mobile-menu-toggle"]');
    const isHamburgerVisible = await hamburgerToggle.isVisible().catch(() => false);
    expect(isHamburgerVisible).toBe(false);

    // Feature cards should be in 2 columns
    const cards = page.locator('#features article.feature-card');
    const count = await cards.count();
    expect(count).toBeGreaterThanOrEqual(3);

    // On tablet, cards should be in roughly 2 columns
    const firstCard = await cards.nth(0).boundingBox();
    const secondCard = await cards.nth(1).boundingBox();
    const thirdCard = await cards.nth(2).boundingBox();

    // First and second cards should be side by side (different x)
    expect(secondCard.x).not.toBe(firstCard.x);

    // First and third cards should be in same column (similar x)
    expect(Math.abs(thirdCard.x - firstCard.x)).toBeLessThanOrEqual(20);
  });

  test('should show full desktop layout with horizontal nav on desktop (1280px)', async ({ page }) => {
    await page.setViewportSize({ width: 1280, height: 800 });
    await page.waitForTimeout(200);

    // No horizontal scrolling
    const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
    const viewportWidth = await page.evaluate(() => window.innerWidth);
    expect(bodyWidth).toBeLessThanOrEqual(viewportWidth + 1);

    // Navigation links should be visible horizontally
    const navLogin = page.locator('[data-testid="nav-login"]');
    const navSignup = page.locator('[data-testid="nav-signup"]');
    await expect(navLogin).toBeVisible();
    await expect(navSignup).toBeVisible();

    // Hamburger menu should be hidden on desktop
    const hamburgerToggle = page.locator('[data-testid="mobile-menu-toggle"]');
    const isHamburgerVisible = await hamburgerToggle.isVisible().catch(() => false);
    expect(isHamburgerVisible).toBe(false);

    // Feature cards should be in multi-column layout (4 columns)
    const cards = page.locator('#features article.feature-card');
    const count = await cards.count();
    expect(count).toBeGreaterThanOrEqual(4);

    // On desktop, all cards should be in a single row (similar y positions)
    const firstCard = await cards.nth(0).boundingBox();
    const secondCard = await cards.nth(1).boundingBox();
    const thirdCard = await cards.nth(2).boundingBox();
    const fourthCard = await cards.nth(3).boundingBox();

    // All cards should be in the same row (similar y position)
    expect(Math.abs(secondCard.y - firstCard.y)).toBeLessThanOrEqual(20);
    expect(Math.abs(thirdCard.y - firstCard.y)).toBeLessThanOrEqual(20);
    expect(Math.abs(fourthCard.y - firstCard.y)).toBeLessThanOrEqual(20);
  });

  test('should adapt layout correctly on orientation change from portrait to landscape on mobile', async ({ page }) => {
    // Start in portrait
    await page.setViewportSize({ width: 375, height: 812 });
    await page.waitForTimeout(300);

    // Verify portrait layout
    const bodyWidthPortrait = await page.evaluate(() => document.body.scrollWidth);
    const viewportWidthPortrait = await page.evaluate(() => window.innerWidth);
    expect(bodyWidthPortrait).toBeLessThanOrEqual(viewportWidthPortrait + 1);

    // Switch to landscape
    await page.setViewportSize({ width: 812, height: 375 });
    await page.waitForTimeout(300);

    // Verify landscape layout - no horizontal scrolling
    const bodyWidthLandscape = await page.evaluate(() => document.body.scrollWidth);
    const viewportWidthLandscape = await page.evaluate(() => window.innerWidth);
    expect(bodyWidthLandscape).toBeLessThanOrEqual(viewportWidthLandscape + 1);

    // All content should still be visible
    const heroSection = page.locator('[data-testid="hero-section"]');
    await expect(heroSection).toBeVisible();

    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    const footer = page.locator('footer');
    await expect(footer).toBeVisible();
  });

  test('should have readable font sizes at mobile breakpoint (375px)', async ({ page }) => {
    await page.setViewportSize({ width: 375, height: 812 });
    await page.waitForTimeout(200);

    // Hero title should be readable
    const heroTitle = page.locator('h1#hero-title');
    await expect(heroTitle).toBeVisible();

    const titleFontSize = await heroTitle.evaluate(el => {
      const style = window.getComputedStyle(el);
      return parseFloat(style.fontSize);
    });
    expect(titleFontSize).toBeGreaterThanOrEqual(16);

    // Hero tagline should be readable
    const heroTagline = page.locator('[data-testid="hero-tagline"]');
    const taglineFontSize = await heroTagline.evaluate(el => {
      const style = window.getComputedStyle(el);
      return parseFloat(style.fontSize);
    });
    expect(taglineFontSize).toBeGreaterThanOrEqual(14);

    // Feature section heading should be readable
    const featuresHeading = page.locator('#features-heading');
    const featuresHeadingSize = await featuresHeading.evaluate(el => {
      const style = window.getComputedStyle(el);
      return parseFloat(style.fontSize);
    });
    expect(featuresHeadingSize).toBeGreaterThanOrEqual(16);

    // Feature card titles should be readable
    const featureTitles = page.locator('#features article.feature-card h3');
    const firstTitleSize = await featureTitles.nth(0).evaluate(el => {
      const style = window.getComputedStyle(el);
      return parseFloat(style.fontSize);
    });
    expect(firstTitleSize).toBeGreaterThanOrEqual(14);

    // Feature card descriptions should be readable
    const featureDescs = page.locator('#features article.feature-card p');
    const firstDescSize = await featureDescs.nth(0).evaluate(el => {
      const style = window.getComputedStyle(el);
      return parseFloat(style.fontSize);
    });
    expect(firstDescSize).toBeGreaterThanOrEqual(14);

    // CTA section heading should be readable
    const ctaHeading = page.locator('#cta-title');
    const ctaHeadingSize = await ctaHeading.evaluate(el => {
      const style = window.getComputedStyle(el);
      return parseFloat(style.fontSize);
    });
    expect(ctaHeadingSize).toBeGreaterThanOrEqual(16);

    // Footer links should be readable
    const footerLinks = page.locator('footer a');
    const firstLinkSize = await footerLinks.nth(0).evaluate(el => {
      const style = window.getComputedStyle(el);
      return parseFloat(style.fontSize);
    });
    expect(firstLinkSize).toBeGreaterThanOrEqual(14);
  });

  test('should have readable font sizes at tablet breakpoint (768px)', async ({ page }) => {
    await page.setViewportSize({ width: 768, height: 1024 });
    await page.waitForTimeout(200);

    const heroTitle = page.locator('h1#hero-title');
    const titleFontSize = await heroTitle.evaluate(el => {
      const style = window.getComputedStyle(el);
      return parseFloat(style.fontSize);
    });
    expect(titleFontSize).toBeGreaterThanOrEqual(16);

    const heroTagline = page.locator('[data-testid="hero-tagline"]');
    const taglineFontSize = await heroTagline.evaluate(el => {
      const style = window.getComputedStyle(el);
      return parseFloat(style.fontSize);
    });
    expect(taglineFontSize).toBeGreaterThanOrEqual(14);

    const featuresHeading = page.locator('#features-heading');
    const featuresHeadingSize = await featuresHeading.evaluate(el => {
      const style = window.getComputedStyle(el);
      return parseFloat(style.fontSize);
    });
    expect(featuresHeadingSize).toBeGreaterThanOrEqual(16);
  });

  test('should have readable font sizes at desktop breakpoint (1280px)', async ({ page }) => {
    await page.setViewportSize({ width: 1280, height: 800 });
    await page.waitForTimeout(200);

    const heroTitle = page.locator('h1#hero-title');
    const titleFontSize = await heroTitle.evaluate(el => {
      const style = window.getComputedStyle(el);
      return parseFloat(style.fontSize);
    });
    expect(titleFontSize).toBeGreaterThanOrEqual(16);

    const heroTagline = page.locator('[data-testid="hero-tagline"]');
    const taglineFontSize = await heroTagline.evaluate(el => {
      const style = window.getComputedStyle(el);
      return parseFloat(style.fontSize);
    });
    expect(taglineFontSize).toBeGreaterThanOrEqual(14);

    const featuresHeading = page.locator('#features-heading');
    const featuresHeadingSize = await featuresHeading.evaluate(el => {
      const style = window.getComputedStyle(el);
      return parseFloat(style.fontSize);
    });
    expect(featuresHeadingSize).toBeGreaterThanOrEqual(16);
  });

  test('should maintain layout integrity across all breakpoints', async ({ page }) => {
    const breakpoints = [
      { name: 'mobile', width: 375, height: 812 },
      { name: 'tablet', width: 768, height: 1024 },
      { name: 'desktop', width: 1280, height: 800 },
    ];

    for (const bp of breakpoints) {
      await page.setViewportSize({ width: bp.width, height: bp.height });
      await page.waitForTimeout(200);

      // No horizontal overflow
      const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
      const viewportWidth = await page.evaluate(() => window.innerWidth);
      expect(bodyWidth, `Horizontal overflow at ${bp.name} (${bp.width}px)`).toBeLessThanOrEqual(viewportWidth + 1);

      // All main sections should be visible
      await expect(page.locator('[data-testid="hero-section"]'), `Hero not visible at ${bp.name}`).toBeVisible();
      await expect(page.locator('#features'), `Features not visible at ${bp.name}`).toBeVisible();
      await expect(page.locator('footer'), `Footer not visible at ${bp.name}`).toBeVisible();
    }
  });
});
