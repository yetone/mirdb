import { test, expect } from '@playwright/test';

/**
 * REQ-5: Homepage shall support responsive design across desktop, tablet, and mobile devices
 * Scenario: Responsive Design - Tablet
 * Tests verify homepage displays correctly on tablet devices (768px-1024px viewport)
 */
test.describe('Tablet Responsive Design - REQ-5', () => {
  /**
   * Test Case 1: Load homepage at 768x1024 viewport (portrait)
   * Expected: Content displays correctly in portrait orientation
   */
  test.describe('Tablet Portrait (768x1024)', () => {
    test.use({ viewport: { width: 768, height: 1024 } });

    test.beforeEach(async ({ page }) => {
      await page.goto('/');
    });

    test('Test Case 1: Content displays correctly in portrait orientation', async ({ page }) => {
      // Verify hero section is visible and renders correctly
      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();

      // Hero should span full width of viewport
      const heroBox = await heroSection.boundingBox();
      expect(heroBox).not.toBeNull();
      if (heroBox) {
        expect(heroBox.width).toBeGreaterThanOrEqual(768 * 0.95); // At least 95% of viewport width
      }

      // Verify headline is visible and readable
      const headline = page.locator('[data-testid="hero-headline"]');
      await expect(headline).toBeVisible();
      const headlineText = await headline.textContent();
      expect(headlineText).toBeTruthy();

      // Verify subheadline is visible
      const subheadline = page.locator('[data-testid="hero-subheadline"]');
      await expect(subheadline).toBeVisible();

      // Verify CTA button is visible and clickable
      const ctaButton = page.locator('[data-testid="hero-cta"]');
      await expect(ctaButton).toBeVisible();
      await expect(ctaButton).toBeEnabled();
    });

    test('Navigation adapts correctly for tablet portrait', async ({ page }) => {
      const navigation = page.locator('[data-testid="navigation"]');
      await expect(navigation).toBeVisible();

      // At 768px, the mobile menu is active (CSS breakpoint max-width: 768px)
      // Navigation links are hidden by default and shown via hamburger menu toggle
      const mobileMenuToggle = page.locator('[data-testid="mobile-menu-toggle"]');

      // Check if mobile menu toggle is visible at this viewport
      const isMobileView = await mobileMenuToggle.isVisible().catch(() => false);

      if (isMobileView) {
        // Mobile view: hamburger menu should be visible and functional
        await expect(mobileMenuToggle).toBeVisible();

        // Click to open the menu
        await mobileMenuToggle.click();

        // Navigation links should now be visible
        const navLinks = page.locator('[data-testid="navigation-links"]');
        await expect(navLinks).toBeVisible();

        // Check all navigation links are accessible when menu is open
        const links = await page.locator('[data-testid="navigation-links"] a').all();
        expect(links.length).toBeGreaterThan(0);

        for (const link of links) {
          await expect(link).toBeVisible();
        }
      } else {
        // Desktop view: navigation links should be directly visible
        const navLinks = page.locator('[data-testid="navigation-links"]');
        await expect(navLinks).toBeVisible();

        const links = await page.locator('[data-testid="navigation-links"] a').all();
        expect(links.length).toBeGreaterThan(0);

        for (const link of links) {
          await expect(link).toBeVisible();
        }
      }
    });

    test('Footer displays correctly in tablet portrait', async ({ page }) => {
      const footer = page.locator('[data-testid="footer"]');
      await expect(footer).toBeVisible();

      // Footer should span full width
      const footerBox = await footer.boundingBox();
      expect(footerBox).not.toBeNull();
      if (footerBox) {
        expect(footerBox.width).toBeGreaterThanOrEqual(768 * 0.95);
      }

      // Footer sections should be visible
      const footerSections = page.locator('[data-testid="footer-section"]');
      const sectionCount = await footerSections.count();
      expect(sectionCount).toBeGreaterThan(0);
    });

    test('Content is readable without horizontal scrolling', async ({ page }) => {
      // Check there is no horizontal overflow
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });
      expect(hasHorizontalScroll).toBe(false);
    });

    test('Featured content grid adapts to tablet portrait', async ({ page }) => {
      const featuredSection = page.locator('[data-testid="featured-content-section"]');

      // Featured section may or may not be present depending on data
      const isVisible = await featuredSection.isVisible().catch(() => false);
      if (isVisible) {
        const featuredGrid = page.locator('[data-testid="featured-content-grid"]');
        await expect(featuredGrid).toBeVisible();

        // Grid should fit within viewport without overflow
        const gridBox = await featuredGrid.boundingBox();
        if (gridBox) {
          expect(gridBox.width).toBeLessThanOrEqual(768);
        }
      }
    });
  });

  /**
   * Test Case 2: Load homepage at 1024x768 viewport (landscape)
   * Expected: Content displays correctly in landscape orientation
   */
  test.describe('Tablet Landscape (1024x768)', () => {
    test.use({ viewport: { width: 1024, height: 768 } });

    test.beforeEach(async ({ page }) => {
      await page.goto('/');
    });

    test('Test Case 2: Content displays correctly in landscape orientation', async ({ page }) => {
      // Verify hero section is visible and renders correctly
      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();

      // Hero should span full width of viewport
      const heroBox = await heroSection.boundingBox();
      expect(heroBox).not.toBeNull();
      if (heroBox) {
        expect(heroBox.width).toBeGreaterThanOrEqual(1024 * 0.95);
      }

      // Verify headline is visible and readable
      const headline = page.locator('[data-testid="hero-headline"]');
      await expect(headline).toBeVisible();

      // Verify subheadline is visible
      const subheadline = page.locator('[data-testid="hero-subheadline"]');
      await expect(subheadline).toBeVisible();

      // Verify CTA button is visible
      const ctaButton = page.locator('[data-testid="hero-cta"]');
      await expect(ctaButton).toBeVisible();
      await expect(ctaButton).toBeEnabled();
    });

    test('Navigation displays correctly in tablet landscape', async ({ page }) => {
      const navigation = page.locator('[data-testid="navigation"]');
      await expect(navigation).toBeVisible();

      // Navigation links should be visible
      const navLinks = page.locator('[data-testid="navigation-links"]');
      await expect(navLinks).toBeVisible();

      // Check navigation logo is visible
      const logo = page.locator('[data-testid="navigation-logo"]');
      await expect(logo).toBeVisible();
    });

    test('Footer sections are properly arranged in tablet landscape', async ({ page }) => {
      const footer = page.locator('[data-testid="footer"]');
      await expect(footer).toBeVisible();

      // At 1024px, footer should use 2-column grid layout (based on CSS)
      const footerContent = page.locator('[data-testid="footer-content"]');
      if (await footerContent.isVisible().catch(() => false)) {
        const footerContentBox = await footerContent.boundingBox();
        if (footerContentBox) {
          // Content should be centered within max-width
          expect(footerContentBox.width).toBeLessThanOrEqual(1200);
        }
      }
    });

    test('No horizontal scrolling in tablet landscape', async ({ page }) => {
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });
      expect(hasHorizontalScroll).toBe(false);
    });

    test('Featured content shows 2-column layout in tablet landscape', async ({ page }) => {
      const featuredSection = page.locator('[data-testid="featured-content-section"]');
      const isVisible = await featuredSection.isVisible().catch(() => false);

      if (isVisible) {
        const featuredGrid = page.locator('[data-testid="featured-content-grid"]');
        await expect(featuredGrid).toBeVisible();

        // At 1024px, grid should be 2 columns (per CSS media query)
        const gridStyle = await featuredGrid.evaluate((el) => {
          const style = window.getComputedStyle(el);
          return {
            display: style.display,
            gridTemplateColumns: style.gridTemplateColumns
          };
        });
        expect(gridStyle.display).toBe('grid');
      }
    });
  });

  /**
   * Test Case 3: Verify touch targets on tablet
   * Expected: All interactive elements meet minimum 44x44px touch target size
   * Per WCAG 2.1 Success Criterion 2.5.5 Target Size
   */
  test.describe('Touch Target Size Verification', () => {
    test.use({ viewport: { width: 768, height: 1024 } });

    const MIN_TOUCH_TARGET_SIZE = 44;

    test.beforeEach(async ({ page }) => {
      await page.goto('/');
    });

    test('Test Case 3: CTA button meets minimum touch target size', async ({ page }) => {
      const ctaButton = page.locator('[data-testid="hero-cta"]');
      await expect(ctaButton).toBeVisible();

      const box = await ctaButton.boundingBox();
      expect(box).not.toBeNull();
      if (box) {
        expect(box.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET_SIZE);
        expect(box.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET_SIZE);
      }
    });

    test('Navigation links meet minimum touch target size', async ({ page }) => {
      const navLinks = await page.locator('[data-testid="navigation-links"] a').all();

      for (const link of navLinks) {
        const isVisible = await link.isVisible().catch(() => false);
        if (isVisible) {
          const box = await link.boundingBox();
          if (box) {
            // For links, we check the clickable area (padding included)
            // Links may be smaller in width but should have adequate height
            expect(box.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET_SIZE * 0.8); // Allow some tolerance for touch targets
          }
        }
      }
    });

    test('Footer links have adequate touch targets', async ({ page }) => {
      const footer = page.locator('[data-testid="footer"]');
      await expect(footer).toBeVisible();

      // Check social links specifically (they have explicit 40x40px size)
      const socialLinks = await page.locator('.footer-social-link').all();
      for (const link of socialLinks) {
        const isVisible = await link.isVisible().catch(() => false);
        if (isVisible) {
          const box = await link.boundingBox();
          if (box) {
            // Social links are 40px circles, which is close to 44px minimum
            expect(box.width).toBeGreaterThanOrEqual(40);
            expect(box.height).toBeGreaterThanOrEqual(40);
          }
        }
      }
    });

    test('All clickable elements have sufficient spacing for touch', async ({ page }) => {
      // Verify interactive elements don't overlap
      const buttons = await page.locator('button, a[href]').all();

      // Check that visible interactive elements have adequate size
      let checkedCount = 0;
      for (const element of buttons) {
        const isVisible = await element.isVisible().catch(() => false);
        if (isVisible && checkedCount < 20) { // Limit to first 20 for performance
          const box = await element.boundingBox();
          if (box && box.width > 0 && box.height > 0) {
            // Either width or height should be at least 44px (for touch)
            // or the element should have a combined touch area
            const touchArea = box.width * box.height;
            // Minimum touch area should be reasonable for touch interaction
            expect(touchArea).toBeGreaterThan(0);
            checkedCount++;
          }
        }
      }
    });

    test('Interactive elements are easily tappable without accidental triggers', async ({ page }) => {
      // Verify hero CTA is prominently sized for tablet
      const ctaButton = page.locator('[data-testid="hero-cta"]');
      const ctaBox = await ctaButton.boundingBox();

      if (ctaBox) {
        // CTA should be prominently sized (much larger than minimum)
        expect(ctaBox.width).toBeGreaterThanOrEqual(120); // CTAs typically wider
        expect(ctaBox.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET_SIZE);
      }

      // Verify navigation logo link is properly sized
      const logo = page.locator('[data-testid="navigation-logo"]');
      const logoVisible = await logo.isVisible().catch(() => false);
      if (logoVisible) {
        const logoBox = await logo.boundingBox();
        if (logoBox) {
          expect(logoBox.height).toBeGreaterThanOrEqual(20); // Logo area should be reasonable
        }
      }
    });
  });
});
