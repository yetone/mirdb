import { test, expect } from '@playwright/test';

test.describe('Responsive Design - Mobile', () => {
  test.describe('Test Case 1: iPhone SE Viewport (375x667)', () => {
    test.use({ viewport: { width: 375, height: 667 } });

    test('All content displays without horizontal scrolling at 375x667', async ({ page }) => {
      await page.goto('/');

      // Wait for page to fully load
      await page.waitForLoadState('networkidle');

      // Check that page width does not exceed viewport width (no horizontal scrolling)
      const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
      const viewportWidth = 375;

      expect(bodyWidth).toBeLessThanOrEqual(viewportWidth);

      // Verify no horizontal overflow on the document
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });

      expect(hasHorizontalScroll).toBe(false);

      // Verify hero section is visible and contained
      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();

      const heroBoundingBox = await heroSection.boundingBox();
      expect(heroBoundingBox).not.toBeNull();
      if (heroBoundingBox) {
        expect(heroBoundingBox.width).toBeLessThanOrEqual(viewportWidth);
      }
    });

    test('Navigation is properly contained at 375x667', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      const navigation = page.locator('[data-testid="navigation"]');
      await expect(navigation).toBeVisible();

      const navBoundingBox = await navigation.boundingBox();
      expect(navBoundingBox).not.toBeNull();
      if (navBoundingBox) {
        expect(navBoundingBox.width).toBeLessThanOrEqual(375);
      }
    });
  });

  test.describe('Test Case 2: iPhone 12/13 Viewport (390x844)', () => {
    test.use({ viewport: { width: 390, height: 844 } });

    test('Layout is optimized for modern smartphone screens at 390x844', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      const viewportWidth = 390;

      // Check that page width does not exceed viewport width
      const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
      expect(bodyWidth).toBeLessThanOrEqual(viewportWidth);

      // Verify hero section is visible and optimized
      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();

      // Verify headline is visible and readable
      const headline = page.locator('[data-testid="hero-headline"]');
      await expect(headline).toBeVisible();

      // Check headline font size is appropriate for mobile (should be smaller than desktop)
      const headlineFontSize = await headline.evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });

      // Mobile headline should be 24px (1.5rem at 16px base) or similar
      expect(headlineFontSize).toBeLessThanOrEqual(48); // Reasonable mobile size
      expect(headlineFontSize).toBeGreaterThanOrEqual(16); // Still readable

      // Verify CTA button is visible and usable
      const ctaButton = page.locator('[data-testid="hero-cta"]');
      await expect(ctaButton).toBeVisible();
    });

    test('Hero section takes full width on mobile at 390x844', async ({ page }) => {
      await page.goto('/');

      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();

      const heroBoundingBox = await heroSection.boundingBox();
      expect(heroBoundingBox).not.toBeNull();
      if (heroBoundingBox) {
        // Hero should be nearly full viewport width (accounting for any minimal margin)
        expect(heroBoundingBox.width).toBeGreaterThanOrEqual(390 * 0.95);
        expect(heroBoundingBox.width).toBeLessThanOrEqual(390);
      }
    });
  });

  test.describe('Test Case 3: Mobile Navigation Toggle', () => {
    test.use({ viewport: { width: 375, height: 667 } });

    test('Mobile menu opens and displays navigation links when tapping toggle', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Find the mobile menu toggle button (hamburger)
      const mobileMenuToggle = page.locator('[data-testid="mobile-menu-toggle"]');
      await expect(mobileMenuToggle).toBeVisible();

      // Verify navigation links are initially hidden (mobile menu closed)
      const navigationLinks = page.locator('[data-testid="navigation-links"]');
      const isInitiallyHidden = await navigationLinks.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return style.display === 'none' || style.visibility === 'hidden' || !el.checkVisibility();
      });
      expect(isInitiallyHidden).toBe(true);

      // Tap the mobile menu toggle
      await mobileMenuToggle.click();

      // Verify navigation links are now visible
      await expect(navigationLinks).toBeVisible();

      // Verify all expected navigation links are present
      const homeLink = page.locator('[data-testid="navigation-links"] a[href="/"]');
      const featuresLink = page.locator('[data-testid="navigation-links"] a[href="/features"]');
      const aboutLink = page.locator('[data-testid="navigation-links"] a[href="/about"]');
      const contactLink = page.locator('[data-testid="navigation-links"] a[href="/contact"]');

      await expect(homeLink).toBeVisible();
      await expect(featuresLink).toBeVisible();
      await expect(aboutLink).toBeVisible();
      await expect(contactLink).toBeVisible();
    });

    test('Mobile menu can be closed after opening', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      const mobileMenuToggle = page.locator('[data-testid="mobile-menu-toggle"]');
      const navigationLinks = page.locator('[data-testid="navigation-links"]');

      // Open menu
      await mobileMenuToggle.click();
      await expect(navigationLinks).toBeVisible();

      // Close menu
      await mobileMenuToggle.click();

      // Verify menu is closed
      const isHidden = await navigationLinks.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return style.display === 'none' || style.visibility === 'hidden' || !el.checkVisibility();
      });
      expect(isHidden).toBe(true);
    });

    test('Mobile menu toggle has accessible label', async ({ page }) => {
      await page.goto('/');

      const mobileMenuToggle = page.locator('[data-testid="mobile-menu-toggle"]');
      await expect(mobileMenuToggle).toBeVisible();

      // Check for aria-label or aria-expanded attributes
      const ariaLabel = await mobileMenuToggle.getAttribute('aria-label');
      const ariaExpanded = await mobileMenuToggle.getAttribute('aria-expanded');

      expect(ariaLabel).toBeTruthy();
      expect(ariaExpanded).toBeTruthy();
    });
  });

  test.describe('Test Case 4: Touch Target Sizes', () => {
    test.use({ viewport: { width: 375, height: 667 } });

    test('All interactive elements are minimum 44x44 pixels', async ({ page }) => {
      await page.goto('/');
      await page.waitForLoadState('networkidle');

      // Open mobile menu to test all interactive elements
      const mobileMenuToggle = page.locator('[data-testid="mobile-menu-toggle"]');
      await expect(mobileMenuToggle).toBeVisible();

      // Check mobile menu toggle size
      const toggleBox = await mobileMenuToggle.boundingBox();
      expect(toggleBox).not.toBeNull();
      if (toggleBox) {
        expect(toggleBox.width).toBeGreaterThanOrEqual(44);
        expect(toggleBox.height).toBeGreaterThanOrEqual(44);
      }

      // Open menu to access links
      await mobileMenuToggle.click();

      // Check all navigation links
      const navLinks = page.locator('[data-testid="navigation-links"] a');
      const linkCount = await navLinks.count();

      for (let i = 0; i < linkCount; i++) {
        const link = navLinks.nth(i);
        const linkBox = await link.boundingBox();
        expect(linkBox).not.toBeNull();
        if (linkBox) {
          expect(linkBox.width).toBeGreaterThanOrEqual(44);
          expect(linkBox.height).toBeGreaterThanOrEqual(44);
        }
      }
    });

    test('CTA button meets touch target requirements', async ({ page }) => {
      await page.goto('/');

      const ctaButton = page.locator('[data-testid="hero-cta"]');
      await expect(ctaButton).toBeVisible();

      const ctaBox = await ctaButton.boundingBox();
      expect(ctaBox).not.toBeNull();
      if (ctaBox) {
        expect(ctaBox.width).toBeGreaterThanOrEqual(44);
        expect(ctaBox.height).toBeGreaterThanOrEqual(44);
      }
    });

    test('Navigation logo is properly sized as touch target', async ({ page }) => {
      await page.goto('/');

      const logo = page.locator('[data-testid="navigation-logo"]');
      await expect(logo).toBeVisible();

      const logoBox = await logo.boundingBox();
      expect(logoBox).not.toBeNull();
      if (logoBox) {
        // Logo should have at least 44px height as a touch target
        expect(logoBox.height).toBeGreaterThanOrEqual(44);
      }
    });
  });

  test.describe('Additional Mobile Responsive Tests', () => {
    test.use({ viewport: { width: 375, height: 667 } });

    test('Page does not require horizontal scrolling on mobile', async ({ page }) => {
      await page.goto('/');

      // Scroll to the bottom to check all content
      await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));

      // Check horizontal scroll at various points
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });

      expect(hasHorizontalScroll).toBe(false);
    });

    test('Content remains readable with mobile font sizes', async ({ page }) => {
      await page.goto('/');

      // Check body font size
      const bodyFontSize = await page.evaluate(() => {
        return parseFloat(window.getComputedStyle(document.body).fontSize);
      });

      // Body font should be at least 14px for readability
      expect(bodyFontSize).toBeGreaterThanOrEqual(14);
    });
  });
});
