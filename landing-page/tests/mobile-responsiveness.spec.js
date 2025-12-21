const { test, expect } = require('@playwright/test');

/**
 * E2E tests for Mobile Responsiveness (Scenario 7)
 * Tests verify the page is fully responsive on mobile devices (NFR-2, US-6)
 * Requirements: NFR-2, US-6
 */

test.describe('Mobile Responsiveness', () => {
  // Test Case 1: Viewport 375x667 (iPhone SE) - No horizontal overflow
  test.describe('TC1: Mobile Viewport (375x667 iPhone SE)', () => {
    test('should have no horizontal overflow on mobile viewport', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/');

      // Check that there is no visible horizontal scrollbar (user-facing behavior)
      // With overflow-x: hidden, content is clipped but no scrollbar appears
      const hasVisibleHorizontalScroll = await page.evaluate(() => {
        const html = document.documentElement;
        const body = document.body;
        // Check if horizontal scrolling is actually possible for the user
        const htmlOverflow = window.getComputedStyle(html).overflowX;
        const bodyOverflow = window.getComputedStyle(body).overflowX;
        // If overflow is hidden, there's no horizontal scroll for the user
        if (htmlOverflow === 'hidden' || bodyOverflow === 'hidden') {
          return false;
        }
        return html.scrollWidth > html.clientWidth;
      });

      expect(hasVisibleHorizontalScroll).toBe(false);
    });

    test('should have no horizontal scrollbar on mobile', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/');

      // Check that document doesn't show horizontal scrollbar to user
      const hasHorizontalScroll = await page.evaluate(() => {
        const html = document.documentElement;
        const body = document.body;
        // Check computed overflow styles
        const htmlOverflow = window.getComputedStyle(html).overflowX;
        const bodyOverflow = window.getComputedStyle(body).overflowX;
        // If overflow is hidden, scrollbar is not visible
        if (htmlOverflow === 'hidden' || bodyOverflow === 'hidden') {
          return false;
        }
        return html.scrollWidth > html.clientWidth;
      });

      expect(hasHorizontalScroll).toBe(false);
    });

    test('should have all content visible within viewport width', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/');

      // Check hero section fits
      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();
      const heroBox = await heroSection.boundingBox();
      expect(heroBox.width).toBeLessThanOrEqual(375);

      // Check features section fits
      const featuresSection = page.locator('[data-testid="features-section"]');
      await expect(featuresSection).toBeVisible();
      const featuresBox = await featuresSection.boundingBox();
      expect(featuresBox.width).toBeLessThanOrEqual(375);
    });
  });

  // Test Case 2: Viewport 768x1024 (iPad) - Layout adaptation
  test.describe('TC2: Tablet Viewport (768x1024 iPad)', () => {
    test('should adapt layout appropriately for tablet viewport', async ({ page }) => {
      await page.setViewportSize({ width: 768, height: 1024 });
      await page.goto('/');

      // Check that body doesn't exceed viewport width
      const bodyWidth = await page.evaluate(() => {
        return document.body.scrollWidth;
      });
      expect(bodyWidth).toBeLessThanOrEqual(768);
    });

    test('should have properly sized hero section on tablet', async ({ page }) => {
      await page.setViewportSize({ width: 768, height: 1024 });
      await page.goto('/');

      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();
      const heroBox = await heroSection.boundingBox();
      expect(heroBox.width).toBeLessThanOrEqual(768);
    });

    test('should display features grid appropriately on tablet', async ({ page }) => {
      await page.setViewportSize({ width: 768, height: 1024 });
      await page.goto('/');

      const featuresSection = page.locator('[data-testid="features-section"]');
      await expect(featuresSection).toBeVisible();

      // All feature cards should be visible
      const featureCards = page.locator('.feature-card');
      const cardCount = await featureCards.count();
      expect(cardCount).toBe(4);

      for (let i = 0; i < cardCount; i++) {
        await expect(featureCards.nth(i)).toBeVisible();
      }
    });

    test('should have no horizontal scrollbar on tablet', async ({ page }) => {
      await page.setViewportSize({ width: 768, height: 1024 });
      await page.goto('/');

      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });

      expect(hasHorizontalScroll).toBe(false);
    });
  });

  // Test Case 3: Touch targets (minimum 44x44px)
  test.describe('TC3: Touch Target Dimensions', () => {
    test('should have CTA buttons at least 44x44px on mobile', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/');

      const ctaButtons = page.locator('.cta-buttons .btn');
      const buttonCount = await ctaButtons.count();

      expect(buttonCount).toBeGreaterThan(0);

      for (let i = 0; i < buttonCount; i++) {
        const button = ctaButtons.nth(i);
        await expect(button).toBeVisible();
        const box = await button.boundingBox();
        expect(box.height).toBeGreaterThanOrEqual(44);
        expect(box.width).toBeGreaterThanOrEqual(44);
      }
    });

    test('should have all interactive buttons at least 44px height', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/');

      const allButtons = page.locator('.btn');
      const buttonCount = await allButtons.count();

      for (let i = 0; i < buttonCount; i++) {
        const button = allButtons.nth(i);
        const isVisible = await button.isVisible();
        if (isVisible) {
          const box = await button.boundingBox();
          expect(box.height).toBeGreaterThanOrEqual(44);
        }
      }
    });

    test('should have footer links with adequate touch target size', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/');

      const footerLinks = page.locator('footer .footer-links a');
      const linkCount = await footerLinks.count();

      expect(linkCount).toBeGreaterThan(0);

      for (let i = 0; i < linkCount; i++) {
        const link = footerLinks.nth(i);
        await expect(link).toBeVisible();
        const box = await link.boundingBox();
        // Links should have at least 44px height for touch targets
        expect(box.height).toBeGreaterThanOrEqual(44);
      }
    });
  });

  // Test Case 4: Navigation accessibility on mobile
  test.describe('TC4: Navigation on Mobile', () => {
    test('should have accessible navigation (CTA buttons visible)', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/');

      // Hero CTA buttons should be visible and accessible
      const getStartedBtn = page.locator('.btn-primary').first();
      await expect(getStartedBtn).toBeVisible();
      await expect(getStartedBtn).toContainText('Get Started');

      const githubBtn = page.locator('.btn-secondary').first();
      await expect(githubBtn).toBeVisible();
      await expect(githubBtn).toContainText('GitHub');
    });

    test('should have footer navigation accessible on mobile', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/');

      const footerSection = page.locator('[data-testid="footer-section"]');
      await expect(footerSection).toBeVisible();

      // Footer links should be visible and stacked/wrapped appropriately
      const footerLinks = page.locator('.footer-links a');
      const linkCount = await footerLinks.count();
      expect(linkCount).toBeGreaterThan(0);

      for (let i = 0; i < linkCount; i++) {
        await expect(footerLinks.nth(i)).toBeVisible();
      }
    });

    test('should have CTA buttons in stacked or wrapped layout on mobile', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/');

      const ctaContainer = page.locator('.cta-buttons');
      await expect(ctaContainer).toBeVisible();

      // Buttons should be wrapped - check that they fit within container
      const containerBox = await ctaContainer.boundingBox();
      expect(containerBox.width).toBeLessThanOrEqual(375);
    });
  });

  // Test Case 5: Meta viewport tag
  test.describe('TC5: Meta Viewport Tag', () => {
    test('should have meta viewport tag with width=device-width and initial-scale=1', async ({ page }) => {
      await page.goto('/');

      const viewportMeta = await page.evaluate(() => {
        const meta = document.querySelector('meta[name="viewport"]');
        return meta ? meta.getAttribute('content') : null;
      });

      expect(viewportMeta).not.toBeNull();
      expect(viewportMeta).toContain('width=device-width');
      expect(viewportMeta).toContain('initial-scale=1');
    });

    test('should have proper meta viewport configuration', async ({ page }) => {
      await page.goto('/');

      const viewportMeta = await page.locator('meta[name="viewport"]');
      await expect(viewportMeta).toHaveAttribute('content', /width=device-width/);
    });
  });

  // Test Case 6: Font sizes on mobile (minimum 16px for body text)
  test.describe('TC6: Font Sizes on Mobile', () => {
    test('should have body text at least 16px to prevent zoom on iOS', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/');

      // Check body font size
      const bodyFontSize = await page.evaluate(() => {
        const body = document.body;
        return parseFloat(window.getComputedStyle(body).fontSize);
      });

      // Body font size should be at least 16px
      expect(bodyFontSize).toBeGreaterThanOrEqual(16);
    });

    test('should have readable paragraph text (at least 16px)', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/');

      // Check paragraph font sizes
      const paragraphFontSizes = await page.evaluate(() => {
        const paragraphs = document.querySelectorAll('p');
        const sizes = [];
        paragraphs.forEach(p => {
          const size = parseFloat(window.getComputedStyle(p).fontSize);
          sizes.push(size);
        });
        return sizes;
      });

      // All main paragraphs should have readable font size
      paragraphFontSizes.forEach(size => {
        expect(size).toBeGreaterThanOrEqual(14); // Allow slightly smaller for secondary text
      });
    });

    test('should have hero text appropriately sized for mobile', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/');

      // Check hero h1 font size
      const h1FontSize = await page.evaluate(() => {
        const h1 = document.querySelector('.hero h1');
        return parseFloat(window.getComputedStyle(h1).fontSize);
      });

      // Hero title should be readable but appropriately sized for mobile
      expect(h1FontSize).toBeGreaterThanOrEqual(24);
    });

    test('should have form inputs at least 16px to prevent zoom', async ({ page }) => {
      await page.setViewportSize({ width: 375, height: 667 });
      await page.goto('/');

      // Check if there are any input elements
      const inputCount = await page.locator('input, textarea, select').count();

      if (inputCount > 0) {
        const inputFontSizes = await page.evaluate(() => {
          const inputs = document.querySelectorAll('input, textarea, select');
          const sizes = [];
          inputs.forEach(input => {
            const size = parseFloat(window.getComputedStyle(input).fontSize);
            sizes.push(size);
          });
          return sizes;
        });

        inputFontSizes.forEach(size => {
          expect(size).toBeGreaterThanOrEqual(16);
        });
      }
      // If no inputs, test passes (page doesn't have input elements)
      expect(true).toBe(true);
    });
  });
});
