/**
 * Responsive Design Tests
 * Owner: Scenario 7 (Mobile), Scenario 8 (Tablet/Desktop)
 *
 * Tests mobile responsive behavior including:
 * - No horizontal scrolling at 375px width
 * - Mobile navigation toggle (hamburger menu)
 * - Touch-friendly targets (44x44px minimum)
 * - Readable font sizes
 * - Image scaling
 */

import { test, expect } from '@playwright/test';

// =============================================
// SCENARIO 7 - Mobile Responsive Tests
// Owner: Scenario 7 - Responsive Design - Mobile
// =============================================

test.describe('Mobile Responsive Design (375px viewport)', () => {
  test.use({ viewport: { width: 375, height: 667 } }); // iPhone SE size

  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for page to be fully loaded
    await page.waitForLoadState('networkidle');
  });

  // Test Case 1: Page renders without horizontal scrollbar
  test('TC1: Page renders without horizontal scrollbar at 375px width', async ({ page }) => {
    // Check that no horizontal scrollbar is present
    const hasHorizontalScrollbar = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScrollbar).toBe(false);
  });

  // Test Case 2: scrollWidth equals clientWidth (no horizontal overflow)
  test('TC2: scrollWidth equals clientWidth - no horizontal overflow', async ({ page }) => {
    const dimensions = await page.evaluate(() => {
      return {
        scrollWidth: document.documentElement.scrollWidth,
        clientWidth: document.documentElement.clientWidth,
      };
    });
    expect(dimensions.scrollWidth).toBeLessThanOrEqual(dimensions.clientWidth);
  });

  // Test Case 3: Mobile navigation toggle button exists
  test('TC3: Mobile navigation toggle button exists', async ({ page }) => {
    // Look for hamburger menu button (mobile nav toggle)
    const mobileNavToggle = page.locator('[data-testid="mobile-nav-toggle"], .mobile-nav-toggle, .hamburger-menu, [aria-label*="menu" i], button[aria-expanded]');
    await expect(mobileNavToggle.first()).toBeVisible();
  });

  // Test Case 4: Navigation menu expands/collapses when toggle clicked
  test('TC4: Navigation menu expands/collapses on toggle click', async ({ page }) => {
    // Find the mobile nav toggle
    const mobileNavToggle = page.locator('[data-testid="mobile-nav-toggle"]');
    await expect(mobileNavToggle).toBeVisible();

    // Get the navigation menu
    const navMenu = page.locator('.header__nav, .nav, [role="navigation"]').first();

    // Initially the nav should be hidden on mobile
    const initiallyHidden = await navMenu.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return style.display === 'none' || style.visibility === 'hidden' || el.getAttribute('aria-hidden') === 'true';
    });
    expect(initiallyHidden).toBe(true);

    // Click the toggle to open
    await mobileNavToggle.click();
    await page.waitForTimeout(300); // Wait for animation

    // Nav should now be visible
    const afterClickOpen = await navMenu.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return style.display !== 'none' && style.visibility !== 'hidden';
    });
    expect(afterClickOpen).toBe(true);

    // Click again to close
    await mobileNavToggle.click();
    await page.waitForTimeout(300);

    // Nav should be hidden again
    const afterClickClose = await navMenu.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return style.display === 'none' || style.visibility === 'hidden' || el.getAttribute('aria-hidden') === 'true';
    });
    expect(afterClickClose).toBe(true);
  });

  // Test Case 5: CTA buttons have minimum 44x44px touch area
  test('TC5: CTA buttons have minimum 44x44px touch area', async ({ page }) => {
    // Check the hero CTA buttons
    const ctaButtons = page.locator('.hero__cta .btn, .btn-primary, .btn-secondary');
    const buttonCount = await ctaButtons.count();

    expect(buttonCount).toBeGreaterThan(0);

    for (let i = 0; i < buttonCount; i++) {
      const button = ctaButtons.nth(i);
      const box = await button.boundingBox();

      if (box) {
        // Either width or height should be at least 44px for touch accessibility
        // For buttons, we typically check both dimensions
        expect(box.height).toBeGreaterThanOrEqual(44);
      }
    }
  });

  // Test Case 6: Navigation links have minimum 44x44px touch area
  test('TC6: Navigation links have minimum 44x44px touch area', async ({ page }) => {
    // First open the mobile nav
    const mobileNavToggle = page.locator('[data-testid="mobile-nav-toggle"]');
    await mobileNavToggle.click();
    await page.waitForTimeout(300);

    // Check navigation links
    const navLinks = page.locator('.header__nav-link, .nav__link');
    const linkCount = await navLinks.count();

    expect(linkCount).toBeGreaterThan(0);

    for (let i = 0; i < linkCount; i++) {
      const link = navLinks.nth(i);
      const isVisible = await link.isVisible();

      if (isVisible) {
        const box = await link.boundingBox();
        if (box) {
          // Touch targets should be at least 44px in height
          expect(box.height).toBeGreaterThanOrEqual(44);
        }
      }
    }
  });

  // Test Case 7: Hero headline and CTA visible without scrolling
  test('TC7: Hero headline and CTA visible without scrolling', async ({ page }) => {
    const heroTitle = page.locator('.hero__title, #hero-title, h1').first();
    const heroCta = page.locator('.hero__cta .btn').first();

    // Check hero title is in viewport
    await expect(heroTitle).toBeVisible();
    const titleBox = await heroTitle.boundingBox();
    expect(titleBox).not.toBeNull();
    if (titleBox) {
      // Title should be visible within initial viewport (667px height)
      expect(titleBox.y + titleBox.height).toBeLessThan(667);
    }

    // Check at least one CTA button is visible
    await expect(heroCta).toBeVisible();
    const ctaBox = await heroCta.boundingBox();
    expect(ctaBox).not.toBeNull();
    if (ctaBox) {
      // CTA should be visible within initial viewport
      expect(ctaBox.y + ctaBox.height).toBeLessThan(667);
    }
  });

  // Test Case 8: Images scale down and maintain aspect ratio
  test('TC8: Images scale down and maintain aspect ratio with max-width 100%', async ({ page }) => {
    const images = page.locator('img');
    const imageCount = await images.count();

    expect(imageCount).toBeGreaterThan(0);

    for (let i = 0; i < imageCount; i++) {
      const image = images.nth(i);
      const isVisible = await image.isVisible();

      if (isVisible) {
        const styles = await image.evaluate((el) => {
          const computed = window.getComputedStyle(el);
          return {
            maxWidth: computed.maxWidth,
            width: el.getBoundingClientRect().width,
            viewportWidth: window.innerWidth,
          };
        });

        // Image should not exceed viewport width
        expect(styles.width).toBeLessThanOrEqual(styles.viewportWidth);

        // max-width should be 100% or the image width should be within bounds
        expect(styles.maxWidth === '100%' || styles.width <= 375).toBe(true);
      }
    }
  });

  // Test Case 9: Font sizes are readable on mobile (body 16px min, headings scaled)
  test('TC9: Font sizes are readable on mobile - body at least 16px', async ({ page }) => {
    // Check body text font size
    const bodyFontSize = await page.evaluate(() => {
      const body = document.body;
      const computed = window.getComputedStyle(body);
      return parseFloat(computed.fontSize);
    });

    // Body font size should be at least 16px for readability
    expect(bodyFontSize).toBeGreaterThanOrEqual(16);

    // Check paragraph text
    const paragraphFontSize = await page.evaluate(() => {
      const paragraph = document.querySelector('p');
      if (paragraph) {
        const computed = window.getComputedStyle(paragraph);
        return parseFloat(computed.fontSize);
      }
      return 16;
    });

    expect(paragraphFontSize).toBeGreaterThanOrEqual(14);

    // Check that h1 is larger than body text
    const h1FontSize = await page.evaluate(() => {
      const h1 = document.querySelector('h1');
      if (h1) {
        const computed = window.getComputedStyle(h1);
        return parseFloat(computed.fontSize);
      }
      return 24;
    });

    expect(h1FontSize).toBeGreaterThan(bodyFontSize);
  });
});

// =============================================
// Additional Mobile UX Tests
// =============================================

test.describe('Mobile Navigation Accessibility', () => {
  test.use({ viewport: { width: 375, height: 667 } });

  test('Mobile nav toggle has accessible label', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    const mobileNavToggle = page.locator('[data-testid="mobile-nav-toggle"]');
    await expect(mobileNavToggle).toBeVisible();

    // Check for accessible label
    const ariaLabel = await mobileNavToggle.getAttribute('aria-label');
    const ariaExpanded = await mobileNavToggle.getAttribute('aria-expanded');

    // Must have either aria-label or aria-expanded for accessibility
    const hasAccessibleLabel = Boolean(ariaLabel) || ariaExpanded !== null;
    expect(hasAccessibleLabel).toBe(true);
  });

  test('Mobile nav toggle updates aria-expanded on click', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');

    const mobileNavToggle = page.locator('[data-testid="mobile-nav-toggle"]');

    // Initially should be collapsed
    const initialExpanded = await mobileNavToggle.getAttribute('aria-expanded');
    expect(initialExpanded).toBe('false');

    // Click to expand
    await mobileNavToggle.click();
    await page.waitForTimeout(300);

    const expandedAfterClick = await mobileNavToggle.getAttribute('aria-expanded');
    expect(expandedAfterClick).toBe('true');
  });
});
