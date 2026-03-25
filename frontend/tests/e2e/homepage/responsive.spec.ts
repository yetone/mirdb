/**
 * Responsive Design E2E Tests
 * Owner: Scenario 7 - Responsive Design
 *
 * Validates homepage displays correctly on mobile (375px), tablet (768px),
 * and desktop (1920px+) viewports (REQ-7, US-7, NFR-1)
 */

import { test, expect } from '@playwright/test';

// Viewport definitions per scenario requirements
const VIEWPORTS = {
  mobile: { width: 375, height: 667 },
  tablet: { width: 768, height: 1024 },
  desktop: { width: 1920, height: 1080 },
};

// Minimum touch target size per WCAG guidelines
const MIN_TOUCH_TARGET = 44;

// Minimum readable font size (16px is recommended for mobile)
const MIN_FONT_SIZE = 14;

test.describe('Responsive Design - Mobile (375px)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.mobile);
    await page.goto('/');
  });

  test('Test Case 1: All content vertically stacked, no horizontal scroll', async ({ page }) => {
    // Wait for homepage to load
    await expect(page.getByTestId('home-page')).toBeVisible();

    // Check that page width matches viewport (no horizontal overflow)
    const bodyWidth = await page.evaluate(() => document.body.scrollWidth);
    expect(bodyWidth).toBeLessThanOrEqual(VIEWPORTS.mobile.width);

    // Verify no horizontal scrollbar exists
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);

    // Verify feature cards are stacked vertically (single column)
    const featuresSection = page.getByTestId('features-section');
    await expect(featuresSection).toBeVisible();

    const featureCards = page.locator('[data-testid^="feature-card-"]');
    const cardCount = await featureCards.count();
    expect(cardCount).toBe(3);

    // Verify cards are stacked (not side by side)
    if (cardCount >= 2) {
      const firstCard = await featureCards.nth(0).boundingBox();
      const secondCard = await featureCards.nth(1).boundingBox();

      if (firstCard && secondCard) {
        // In vertical stack, second card should be below first card
        expect(secondCard.y).toBeGreaterThan(firstCard.y);
        // Cards should not be side by side (x positions should be similar)
        expect(Math.abs(firstCard.x - secondCard.x)).toBeLessThan(50);
      }
    }

    // Verify how-it-works steps are stacked vertically
    const stepsSection = page.getByTestId('how-it-works-section');
    await expect(stepsSection).toBeVisible();

    // Use specific step containers (step-1, step-2, step-3) not nested elements
    const step1 = page.getByTestId('step-1');
    const step2 = page.getByTestId('step-2');
    const step3 = page.getByTestId('step-3');

    await expect(step1).toBeVisible();
    await expect(step2).toBeVisible();
    await expect(step3).toBeVisible();

    const step1Box = await step1.boundingBox();
    const step2Box = await step2.boundingBox();

    if (step1Box && step2Box) {
      // Steps should be stacked vertically on mobile
      expect(step2Box.y).toBeGreaterThan(step1Box.y);
    }
  });

  test('Test Case 4: CTA buttons are at least 44x44px for touch targets', async ({ page }) => {
    // Wait for hero section to load
    await expect(page.getByTestId('home-page')).toBeVisible();

    // Check Get Started button dimensions
    const getStartedButton = page.getByTestId('hero-get-started-button');
    await expect(getStartedButton).toBeVisible();

    const buttonBox = await getStartedButton.boundingBox();
    expect(buttonBox).not.toBeNull();

    if (buttonBox) {
      expect(buttonBox.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
      expect(buttonBox.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET);
    }
  });

  test('Test Case 5: Text is readable without zooming (font size >= 14px)', async ({ page }) => {
    await expect(page.getByTestId('home-page')).toBeVisible();

    // Check hero headline font size
    const heroHeadline = page.locator('h1').first();
    await expect(heroHeadline).toBeVisible();

    const headlineFontSize = await heroHeadline.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    expect(headlineFontSize).toBeGreaterThanOrEqual(MIN_FONT_SIZE);

    // Check hero paragraph font size
    const heroParagraph = page.locator('section[aria-label="Hero section"] p').first();
    await expect(heroParagraph).toBeVisible();

    const paragraphFontSize = await heroParagraph.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    expect(paragraphFontSize).toBeGreaterThanOrEqual(MIN_FONT_SIZE);

    // Check feature card description font size
    const featureDescription = page.getByTestId('feature-description-0');
    await expect(featureDescription).toBeVisible();

    const featureDescFontSize = await featureDescription.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    expect(featureDescFontSize).toBeGreaterThanOrEqual(MIN_FONT_SIZE);
  });
});

test.describe('Responsive Design - Tablet (768px)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.tablet);
    await page.goto('/');
  });

  test('Test Case 2: Tablet layout displays correctly', async ({ page }) => {
    // Wait for homepage to load
    await expect(page.getByTestId('home-page')).toBeVisible();

    // Verify no horizontal scrollbar on tablet
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth;
    });
    expect(hasHorizontalScroll).toBe(false);

    // Verify feature cards are visible
    const featuresSection = page.getByTestId('features-section');
    await expect(featuresSection).toBeVisible();

    const featureCards = page.locator('[data-testid^="feature-card-"]');
    const cardCount = await featureCards.count();
    expect(cardCount).toBe(3);

    // At 768px, grid should switch to md:grid-cols-3 (3 columns)
    // Check that cards are horizontally aligned (in a row)
    const firstCard = await featureCards.nth(0).boundingBox();
    const secondCard = await featureCards.nth(1).boundingBox();
    const thirdCard = await featureCards.nth(2).boundingBox();

    if (firstCard && secondCard && thirdCard) {
      // All cards should be on approximately the same row
      const yTolerance = 20; // Allow some vertical variance for grid gaps
      expect(Math.abs(firstCard.y - secondCard.y)).toBeLessThan(yTolerance);
      expect(Math.abs(secondCard.y - thirdCard.y)).toBeLessThan(yTolerance);

      // Cards should be arranged horizontally
      expect(secondCard.x).toBeGreaterThan(firstCard.x);
      expect(thirdCard.x).toBeGreaterThan(secondCard.x);
    }

    // Verify how-it-works section is visible and properly laid out
    const stepsSection = page.getByTestId('how-it-works-section');
    await expect(stepsSection).toBeVisible();

    // At md breakpoint, steps should be in a row
    const steps = page.locator('[data-testid^="step-"]:not([data-testid*="-icon"]):not([data-testid*="-title"]):not([data-testid*="-description"]):not([data-testid*="-number"])');
    const step1 = page.getByTestId('step-1');
    const step2 = page.getByTestId('step-2');

    const step1Box = await step1.boundingBox();
    const step2Box = await step2.boundingBox();

    if (step1Box && step2Box) {
      // Steps should be horizontally arranged at tablet size
      expect(step2Box.x).toBeGreaterThan(step1Box.x);
    }
  });
});

test.describe('Responsive Design - Desktop (1920px)', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(VIEWPORTS.desktop);
    await page.goto('/');
  });

  test('Test Case 3: Desktop layout with horizontal feature grid', async ({ page }) => {
    // Wait for homepage to load
    await expect(page.getByTestId('home-page')).toBeVisible();

    // Verify feature cards are in horizontal grid layout
    const featuresSection = page.getByTestId('features-section');
    await expect(featuresSection).toBeVisible();

    const featureCards = page.locator('[data-testid^="feature-card-"]');
    const cardCount = await featureCards.count();
    expect(cardCount).toBe(3);

    // Verify horizontal arrangement on desktop
    const firstCard = await featureCards.nth(0).boundingBox();
    const secondCard = await featureCards.nth(1).boundingBox();
    const thirdCard = await featureCards.nth(2).boundingBox();

    if (firstCard && secondCard && thirdCard) {
      // All cards should be on the same row
      const yTolerance = 10;
      expect(Math.abs(firstCard.y - secondCard.y)).toBeLessThan(yTolerance);
      expect(Math.abs(secondCard.y - thirdCard.y)).toBeLessThan(yTolerance);

      // Cards should be spread horizontally
      expect(secondCard.x).toBeGreaterThan(firstCard.x);
      expect(thirdCard.x).toBeGreaterThan(secondCard.x);
    }

    // Verify how-it-works section with horizontal flow
    const step1 = page.getByTestId('step-1');
    const step2 = page.getByTestId('step-2');
    const step3 = page.getByTestId('step-3');

    await expect(step1).toBeVisible();
    await expect(step2).toBeVisible();
    await expect(step3).toBeVisible();

    const step1Box = await step1.boundingBox();
    const step2Box = await step2.boundingBox();
    const step3Box = await step3.boundingBox();

    if (step1Box && step2Box && step3Box) {
      // Steps should be horizontally arranged
      expect(step2Box.x).toBeGreaterThan(step1Box.x);
      expect(step3Box.x).toBeGreaterThan(step2Box.x);
    }

    // Verify desktop-specific hero section layout
    const heroSection = page.locator('section[aria-label="Hero section"]');
    await expect(heroSection).toBeVisible();

    // Hero content should be centered and properly spaced
    const heroBox = await heroSection.boundingBox();
    if (heroBox) {
      // Hero should take up significant viewport width
      expect(heroBox.width).toBeGreaterThanOrEqual(VIEWPORTS.desktop.width * 0.5);
    }
  });
});

test.describe('Responsive Design - No Content Overflow (All Viewports)', () => {
  test('Test Case 6: No horizontal scrollbar on any viewport size', async ({ page }) => {
    for (const [viewportName, viewport] of Object.entries(VIEWPORTS)) {
      await page.setViewportSize(viewport);
      await page.goto('/');

      // Wait for homepage to load
      await expect(page.getByTestId('home-page')).toBeVisible();

      // Check for horizontal scrollbar
      const hasHorizontalScroll = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });

      expect(
        hasHorizontalScroll,
        `Horizontal scroll found on ${viewportName} viewport (${viewport.width}x${viewport.height})`
      ).toBe(false);

      // Verify body doesn't overflow
      const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth);
      expect(
        bodyScrollWidth,
        `Body scroll width (${bodyScrollWidth}) exceeds viewport width (${viewport.width}) on ${viewportName}`
      ).toBeLessThanOrEqual(viewport.width);

      // Check that main content doesn't overflow
      const mainContent = page.locator('main');
      const mainBox = await mainContent.boundingBox();

      if (mainBox) {
        expect(
          mainBox.width,
          `Main content width (${mainBox.width}) exceeds viewport width (${viewport.width}) on ${viewportName}`
        ).toBeLessThanOrEqual(viewport.width);
      }
    }
  });
});
