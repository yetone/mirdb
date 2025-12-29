import { test, expect } from '@playwright/test';

/**
 * E2E Tests for MirDB Homepage Tablet Responsive Design (768px - 1024px)
 *
 * These tests verify that the homepage renders correctly on tablet devices:
 * - Proper layout transitions at tablet breakpoints
 * - Feature grid displays in 2-column layout
 * - Content is readable and well-spaced
 * - All sections are accessible and functional
 */

test.describe('Responsive Design - Tablet (768px)', () => {
  test.beforeEach(async ({ page }) => {
    // Set viewport to tablet portrait size (iPad portrait - 768px)
    await page.setViewportSize({ width: 768, height: 1024 });
    await page.goto('/');
  });

  test('TC1: Layout transitions appropriately from mobile to tablet view at 768px', async ({ page }) => {
    // Verify page loads correctly at tablet width
    await expect(page.locator('body')).toBeVisible();

    // Hero section should be visible and centered
    const heroSection = page.locator('.hero, header').first();
    await expect(heroSection).toBeVisible();

    // Check that hero heading is visible
    const heroHeading = page.locator('h1').filter({ hasText: 'MirDB' });
    await expect(heroHeading).toBeVisible();

    // Verify hero section uses full width appropriately
    const heroBox = await heroSection.boundingBox();
    expect(heroBox).not.toBeNull();
    if (heroBox) {
      expect(heroBox.width).toBe(768);
    }

    // Verify the CTA buttons are visible and accessible
    const ctaButtons = page.locator('.cta-buttons');
    await expect(ctaButtons).toBeVisible();

    // CTA buttons should be side by side (not stacked like mobile)
    const ctaButtonsBox = await ctaButtons.boundingBox();
    const getStartedBtn = page.locator('.btn-primary').first();
    const githubBtn = page.locator('.btn-secondary').first();

    const getStartedBox = await getStartedBtn.boundingBox();
    const githubBox = await githubBtn.boundingBox();

    // Buttons should be horizontally aligned (same Y position approximately)
    if (getStartedBox && githubBox) {
      const yDiff = Math.abs(getStartedBox.y - githubBox.y);
      expect(yDiff).toBeLessThan(10); // Allow small variance for flex alignment
    }

    // Verify features section is visible
    const featuresSection = page.locator('#features, .features');
    await expect(featuresSection).toBeVisible();

    // Verify all main sections are present
    const sections = ['#features', '#getting-started', '#architecture'];
    for (const sectionId of sections) {
      const section = page.locator(sectionId);
      await expect(section).toBeVisible();
    }
  });

  test('TC3-768px: Feature cards display in appropriate layout at 768px', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features, .features');
    await expect(featuresSection).toBeVisible();

    // Find the feature grid
    const featuresGrid = page.locator('.features-grid, .feature-grid');
    await expect(featuresGrid).toBeVisible();

    // Get all feature cards
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBe(4);

    // Verify grid layout is applied
    const gridDisplay = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });
    expect(gridDisplay).toBe('grid');

    // Get positions of first two cards to verify 2-column layout
    const firstCard = featureCards.nth(0);
    const secondCard = featureCards.nth(1);

    const firstCardBox = await firstCard.boundingBox();
    const secondCardBox = await secondCard.boundingBox();

    // At tablet width (768px), with minmax(280px, 1fr) the grid should show 2 columns
    // Cards should be side by side (similar Y position)
    if (firstCardBox && secondCardBox) {
      // Check if cards are on the same row (Y difference is small)
      const yDiff = Math.abs(firstCardBox.y - secondCardBox.y);
      // They should be on the same row
      expect(yDiff).toBeLessThan(10);

      // Cards should not overlap
      expect(secondCardBox.x).toBeGreaterThan(firstCardBox.x);
    }

    // Verify each card is visible and has proper content structure
    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i);
      await expect(card).toBeVisible();
      await expect(card.locator('h3')).toBeVisible();
      await expect(card.locator('p')).toBeVisible();
    }
  });
});

test.describe('Responsive Design - Tablet (1024px)', () => {
  test.beforeEach(async ({ page }) => {
    // Set viewport to larger tablet / small desktop size (iPad landscape - 1024px)
    await page.setViewportSize({ width: 1024, height: 768 });
    await page.goto('/');
  });

  test('TC2: Layout uses available space effectively at 1024px', async ({ page }) => {
    // Verify page loads correctly at 1024px width
    await expect(page.locator('body')).toBeVisible();

    // Hero section should be visible
    const heroSection = page.locator('.hero, header').first();
    await expect(heroSection).toBeVisible();

    // Verify the container uses max-width appropriately
    const container = page.locator('.container').first();
    const containerBox = await container.boundingBox();
    expect(containerBox).not.toBeNull();
    if (containerBox) {
      // Container should be at most 1200px (max-width from CSS)
      expect(containerBox.width).toBeLessThanOrEqual(1200);
      // At 1024px viewport, container should use most of the available space
      expect(containerBox.width).toBeGreaterThan(900);
    }

    // Verify feature cards take advantage of available space
    const featuresGrid = page.locator('.features-grid, .feature-grid');
    await expect(featuresGrid).toBeVisible();

    const featureCards = page.locator('.feature-card');
    const firstCard = featureCards.first();
    const firstCardBox = await firstCard.boundingBox();

    if (firstCardBox) {
      // Cards should have reasonable width at 1024px (not too narrow)
      expect(firstCardBox.width).toBeGreaterThan(200);
    }

    // Verify all main sections use the space well
    const sections = page.locator('section');
    const sectionsCount = await sections.count();

    for (let i = 0; i < sectionsCount; i++) {
      const section = sections.nth(i);
      const isVisible = await section.isVisible();
      if (isVisible) {
        const sectionBox = await section.boundingBox();
        if (sectionBox) {
          // Each section should span the full viewport width
          expect(sectionBox.width).toBe(1024);
        }
      }
    }

    // Verify Getting Started section has proper grid layout
    const gettingStartedContent = page.locator('.getting-started-content');
    if (await gettingStartedContent.isVisible()) {
      const gridDisplay = await gettingStartedContent.evaluate((el) => {
        return window.getComputedStyle(el).display;
      });
      expect(gridDisplay).toBe('grid');

      // At 1024px, the 3 steps should display in a row
      const steps = page.locator('.step');
      const stepCount = await steps.count();
      if (stepCount >= 2) {
        const firstStep = steps.nth(0);
        const secondStep = steps.nth(1);
        const firstStepBox = await firstStep.boundingBox();
        const secondStepBox = await secondStep.boundingBox();

        if (firstStepBox && secondStepBox) {
          // Steps should be on the same row
          const yDiff = Math.abs(firstStepBox.y - secondStepBox.y);
          expect(yDiff).toBeLessThan(10);
        }
      }
    }
  });

  test('TC3-1024px: Feature cards display in 2+ column grid at 1024px', async ({ page }) => {
    // Navigate to features section
    const featuresSection = page.locator('#features, .features');
    await expect(featuresSection).toBeVisible();

    // Find the feature grid
    const featuresGrid = page.locator('.features-grid, .feature-grid');
    await expect(featuresGrid).toBeVisible();

    // Verify grid layout
    const gridDisplay = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).display;
    });
    expect(gridDisplay).toBe('grid');

    // Get all feature cards
    const featureCards = page.locator('.feature-card');
    const cardCount = await featureCards.count();
    expect(cardCount).toBe(4);

    // Verify at least 2 cards are on the same row (2-column or more layout)
    const firstCard = featureCards.nth(0);
    const secondCard = featureCards.nth(1);

    const firstCardBox = await firstCard.boundingBox();
    const secondCardBox = await secondCard.boundingBox();

    if (firstCardBox && secondCardBox) {
      // Cards should be on the same row (Y position is similar)
      const yDiff = Math.abs(firstCardBox.y - secondCardBox.y);
      expect(yDiff).toBeLessThan(10);

      // Cards should be side by side
      expect(secondCardBox.x).toBeGreaterThan(firstCardBox.x);
    }

    // Check if all 4 cards fit in 2 rows (2x2 grid) at 1024px
    const thirdCard = featureCards.nth(2);
    const fourthCard = featureCards.nth(3);

    const thirdCardBox = await thirdCard.boundingBox();
    const fourthCardBox = await fourthCard.boundingBox();

    if (firstCardBox && thirdCardBox && fourthCardBox) {
      // Third and fourth cards should be on the same row (second row)
      const thirdFourthYDiff = Math.abs(thirdCardBox.y - fourthCardBox.y);
      expect(thirdFourthYDiff).toBeLessThan(10);

      // Third card should be below first card (different row)
      expect(thirdCardBox.y).toBeGreaterThan(firstCardBox.y);
    }
  });
});

test.describe('Responsive Design - Tablet Content Readability', () => {
  test('Content remains readable at tablet breakpoints', async ({ page }) => {
    // Test at 768px
    await page.setViewportSize({ width: 768, height: 1024 });
    await page.goto('/');

    // Verify text is readable (font size is not too small)
    const heroHeading = page.locator('h1').filter({ hasText: 'MirDB' });
    const fontSize = await heroHeading.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    // Font size should be at least 32px for tablet
    expect(fontSize).toBeGreaterThanOrEqual(32);

    // Verify tagline is visible and readable
    const tagline = page.locator('.tagline');
    await expect(tagline).toBeVisible();
    const taglineFontSize = await tagline.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });
    expect(taglineFontSize).toBeGreaterThanOrEqual(18);

    // Verify code blocks don't overflow
    const codeBlocks = page.locator('pre');
    const codeBlockCount = await codeBlocks.count();

    for (let i = 0; i < codeBlockCount; i++) {
      const codeBlock = codeBlocks.nth(i);
      if (await codeBlock.isVisible()) {
        const overflowX = await codeBlock.evaluate((el) => {
          return window.getComputedStyle(el).overflowX;
        });
        expect(['auto', 'scroll', 'hidden']).toContain(overflowX);

        const codeBlockBox = await codeBlock.boundingBox();
        if (codeBlockBox) {
          expect(codeBlockBox.width).toBeLessThanOrEqual(768);
        }
      }
    }
  });

  test('Navigation elements are accessible at tablet size', async ({ page }) => {
    await page.setViewportSize({ width: 768, height: 1024 });
    await page.goto('/');

    // Verify CTA buttons are clickable
    const getStartedBtn = page.locator('a.btn-primary, .btn-primary').first();
    await expect(getStartedBtn).toBeVisible();

    const btnBox = await getStartedBtn.boundingBox();
    if (btnBox) {
      // Button should have minimum touch target size (44x44 for accessibility)
      expect(btnBox.height).toBeGreaterThanOrEqual(40);
      expect(btnBox.width).toBeGreaterThanOrEqual(44);
    }

    // Verify anchor links work (smooth scroll to sections)
    await getStartedBtn.click();

    // Wait for scroll animation
    await page.waitForTimeout(500);

    // Verify Getting Started section is now in view
    const gettingStartedSection = page.locator('#getting-started');
    await expect(gettingStartedSection).toBeInViewport();
  });
});
