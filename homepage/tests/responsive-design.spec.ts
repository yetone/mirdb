import { test, expect } from '@playwright/test';

test.describe('Responsive Design Across Viewports', () => {
  test.describe('Desktop Viewport (1280px)', () => {
    test.use({ viewport: { width: 1280, height: 800 } });

    test('TC1: No horizontal scrollbar at 1280px viewport width', async ({ page }) => {
      // Navigate to homepage
      await page.goto('/');

      // Wait for page to fully load
      await page.waitForLoadState('networkidle');

      // Check that there's no horizontal overflow (scrollbar)
      const scrollWidth = await page.evaluate(() => document.documentElement.scrollWidth);
      const clientWidth = await page.evaluate(() => document.documentElement.clientWidth);

      // Page content should not exceed viewport width
      expect(scrollWidth).toBeLessThanOrEqual(clientWidth);

      // Verify all main sections are visible
      const heroSection = page.locator('[data-testid="hero-section"]');
      const featuresSection = page.locator('[data-testid="features-section"]');
      const quickStartSection = page.locator('[data-testid="quick-start-section"]');

      await expect(heroSection).toBeVisible();
      await expect(featuresSection).toBeVisible();
      await expect(quickStartSection).toBeVisible();

      // Verify hero content is visible and properly positioned
      const heroLogo = page.locator('[data-testid="hero-logo"]');
      const heroTagline = page.locator('[data-testid="hero-tagline"]');
      await expect(heroLogo).toBeVisible();
      await expect(heroTagline).toBeVisible();
    });
  });

  test.describe('Tablet Viewport (768px)', () => {
    test.use({ viewport: { width: 768, height: 1024 } });

    test('TC2: Layout adapts at 768px viewport width with no content overflow', async ({ page }) => {
      // Navigate to homepage
      await page.goto('/');

      // Wait for page to fully load
      await page.waitForLoadState('networkidle');

      // Check that there's no horizontal overflow
      const scrollWidth = await page.evaluate(() => document.documentElement.scrollWidth);
      const clientWidth = await page.evaluate(() => document.documentElement.clientWidth);

      expect(scrollWidth).toBeLessThanOrEqual(clientWidth);

      // Verify all main sections are visible
      const heroSection = page.locator('[data-testid="hero-section"]');
      const featuresSection = page.locator('[data-testid="features-section"]');

      await expect(heroSection).toBeVisible();
      await expect(featuresSection).toBeVisible();

      // Verify the features grid adapts (should have 2 columns at md breakpoint)
      const featuresGrid = page.locator('[data-testid="features-grid"]');
      await expect(featuresGrid).toBeVisible();

      // Get computed grid-template-columns style
      const gridStyle = await featuresGrid.evaluate((el) => {
        return window.getComputedStyle(el).gridTemplateColumns;
      });

      // At 768px (md breakpoint), should have 2 columns
      // gridTemplateColumns returns something like "300px 300px" for 2 cols
      const columnCount = gridStyle.split(' ').filter(col => col !== '').length;
      expect(columnCount).toBe(2);
    });

    test('TC4: Feature cards reflow to 2-column grid on tablet viewport', async ({ page }) => {
      // Navigate to homepage
      await page.goto('/');

      // Wait for page to fully load
      await page.waitForLoadState('networkidle');

      // Verify features section exists
      const featuresSection = page.locator('[data-testid="features-section"]');
      await expect(featuresSection).toBeVisible();

      // Verify the features grid is visible
      const featuresGrid = page.locator('[data-testid="features-grid"]');
      await expect(featuresGrid).toBeVisible();

      // Get computed grid-template-columns style
      const gridStyle = await featuresGrid.evaluate((el) => {
        return window.getComputedStyle(el).gridTemplateColumns;
      });

      // At 768px (md breakpoint), should have 2 columns
      const columnCount = gridStyle.split(' ').filter(col => col !== '').length;
      expect(columnCount).toBe(2);

      // Verify feature cards are present
      const featureCards = page.locator('.feature-card');
      const cardCount = await featureCards.count();
      expect(cardCount).toBeGreaterThanOrEqual(4);
      expect(cardCount).toBeLessThanOrEqual(6);

      // Verify cards are properly sized within the grid
      const firstCard = featureCards.first();
      const cardBox = await firstCard.boundingBox();
      expect(cardBox).not.toBeNull();

      // Card width should be less than viewport width (since we have 2 columns)
      if (cardBox) {
        expect(cardBox.width).toBeLessThan(768 / 1.5); // Should be roughly half or less
      }
    });
  });

  test.describe('Mobile Viewport (375px)', () => {
    test.use({ viewport: { width: 375, height: 667 } });

    test('TC3: Single-column layout at 375px with no horizontal scroll and readable text', async ({ page }) => {
      // Navigate to homepage
      await page.goto('/');

      // Wait for page to fully load
      await page.waitForLoadState('networkidle');

      // Check that there's no horizontal overflow
      const scrollWidth = await page.evaluate(() => document.documentElement.scrollWidth);
      const clientWidth = await page.evaluate(() => document.documentElement.clientWidth);

      expect(scrollWidth).toBeLessThanOrEqual(clientWidth);

      // Verify hero content is readable
      const heroTagline = page.locator('[data-testid="hero-tagline"]');
      await expect(heroTagline).toBeVisible();

      // Check font size is readable (at least 14px for mobile)
      const taglineFontSize = await heroTagline.evaluate((el) => {
        return parseInt(window.getComputedStyle(el).fontSize);
      });
      expect(taglineFontSize).toBeGreaterThanOrEqual(14);

      // Verify features grid is single column
      const featuresGrid = page.locator('[data-testid="features-grid"]');
      await expect(featuresGrid).toBeVisible();

      // Get computed grid-template-columns style
      const gridStyle = await featuresGrid.evaluate((el) => {
        return window.getComputedStyle(el).gridTemplateColumns;
      });

      // At mobile size, should have 1 column
      const columnCount = gridStyle.split(' ').filter(col => col !== '').length;
      expect(columnCount).toBe(1);

      // Verify CTA buttons are visible and properly positioned
      const getStartedButton = page.locator('[data-testid="cta-get-started"]');
      await expect(getStartedButton).toBeVisible();

      // Verify button is touch-friendly (at least 44px tall, per Apple HIG)
      const buttonBox = await getStartedButton.boundingBox();
      expect(buttonBox).not.toBeNull();
      if (buttonBox) {
        expect(buttonBox.height).toBeGreaterThanOrEqual(44);
      }
    });

    test('TC5: Feature cards stack vertically in single column on mobile viewport', async ({ page }) => {
      // Navigate to homepage
      await page.goto('/');

      // Wait for page to fully load
      await page.waitForLoadState('networkidle');

      // Verify features section exists
      const featuresSection = page.locator('[data-testid="features-section"]');
      await expect(featuresSection).toBeVisible();

      // Verify the features grid is visible
      const featuresGrid = page.locator('[data-testid="features-grid"]');
      await expect(featuresGrid).toBeVisible();

      // Get computed grid-template-columns style
      const gridStyle = await featuresGrid.evaluate((el) => {
        return window.getComputedStyle(el).gridTemplateColumns;
      });

      // At mobile size (375px), should have 1 column
      const columnCount = gridStyle.split(' ').filter(col => col !== '').length;
      expect(columnCount).toBe(1);

      // Verify feature cards are present and stacked
      const featureCards = page.locator('.feature-card');
      const cardCount = await featureCards.count();
      expect(cardCount).toBeGreaterThanOrEqual(4);

      // Get bounding boxes of first two cards to verify vertical stacking
      const firstCard = featureCards.nth(0);
      const secondCard = featureCards.nth(1);

      const firstBox = await firstCard.boundingBox();
      const secondBox = await secondCard.boundingBox();

      expect(firstBox).not.toBeNull();
      expect(secondBox).not.toBeNull();

      if (firstBox && secondBox) {
        // Second card should be below first card (vertical stacking)
        expect(secondBox.y).toBeGreaterThan(firstBox.y);

        // Both cards should have similar x positions (aligned left)
        expect(Math.abs(secondBox.x - firstBox.x)).toBeLessThan(10);

        // Cards should span full width (accounting for padding)
        // Card width should be close to viewport width minus padding
        expect(firstBox.width).toBeGreaterThan(300); // Should be most of 375px
      }
    });
  });
});
