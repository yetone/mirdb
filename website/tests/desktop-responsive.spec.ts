import { test, expect } from '@playwright/test';

test.describe('Responsive Design - Desktop', () => {
  // Desktop viewport widths
  const DESKTOP_VIEWPORT_WIDTH = 1280;
  const DESKTOP_VIEWPORT_HEIGHT = 800;
  const FULL_HD_VIEWPORT_WIDTH = 1920;
  const FULL_HD_VIEWPORT_HEIGHT = 1080;
  const MAX_CONTENT_WIDTH = 1200; // Max width for content containers

  test.describe('Standard Desktop (1280px)', () => {
    test.beforeEach(async ({ page }) => {
      // Set viewport to desktop dimensions (1280px width)
      await page.setViewportSize({
        width: DESKTOP_VIEWPORT_WIDTH,
        height: DESKTOP_VIEWPORT_HEIGHT,
      });
      await page.goto('/');
    });

    test('TC1: Page displays with full desktop layout at 1280px width', async ({ page }) => {
      // Set viewport to 1280px width and load page
      // Expected: Page displays with full desktop layout

      // Check that the document width doesn't exceed viewport width (no horizontal scrollbar)
      const hasHorizontalScrollbar = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });
      expect(hasHorizontalScrollbar).toBe(false);

      // Verify page loads successfully
      const title = await page.title();
      expect(title).toContain('MirDB');

      // Check all main sections are present and visible
      const heroSection = page.locator('.hero-section');
      await expect(heroSection).toBeVisible();

      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeVisible();

      const architectureSection = page.locator('#architecture');
      await expect(architectureSection).toBeVisible();

      const quickstartSection = page.locator('#quickstart');
      await expect(quickstartSection).toBeVisible();

      const statusSection = page.locator('#project-status');
      await expect(statusSection).toBeVisible();
    });

    test('TC2: Content has appropriate max-width and is centered', async ({ page }) => {
      // Check content max-width constraints
      // Expected: Content has appropriate max-width and is centered

      // Check features container
      const featuresContainer = page.locator('.features-container');
      await featuresContainer.scrollIntoViewIfNeeded();
      await expect(featuresContainer).toBeVisible();

      const featuresBox = await featuresContainer.boundingBox();
      expect(featuresBox).not.toBeNull();

      if (featuresBox) {
        // Content should have max-width constraint (not spanning full viewport)
        expect(featuresBox.width).toBeLessThanOrEqual(MAX_CONTENT_WIDTH);

        // Content should be centered (left offset should be positive)
        const expectedLeftOffset = (DESKTOP_VIEWPORT_WIDTH - featuresBox.width) / 2;
        // Allow some tolerance for padding
        expect(featuresBox.x).toBeGreaterThanOrEqual(expectedLeftOffset - 50);
      }

      // Check architecture container
      const architectureContainer = page.locator('.architecture-container');
      await architectureContainer.scrollIntoViewIfNeeded();

      const archBox = await architectureContainer.boundingBox();
      expect(archBox).not.toBeNull();

      if (archBox) {
        expect(archBox.width).toBeLessThanOrEqual(MAX_CONTENT_WIDTH);
      }

      // Check quickstart container
      const quickstartContainer = page.locator('.quickstart-container');
      await quickstartContainer.scrollIntoViewIfNeeded();

      const quickstartBox = await quickstartContainer.boundingBox();
      expect(quickstartBox).not.toBeNull();

      if (quickstartBox) {
        // Quickstart container has a smaller max-width (900px)
        expect(quickstartBox.width).toBeLessThanOrEqual(900);
      }

      // Check status container
      const statusContainer = page.locator('.status-container');
      await statusContainer.scrollIntoViewIfNeeded();

      const statusBox = await statusContainer.boundingBox();
      expect(statusBox).not.toBeNull();

      if (statusBox) {
        expect(statusBox.width).toBeLessThanOrEqual(MAX_CONTENT_WIDTH);
      }
    });

    test('TC3: Features display in multi-column grid layout', async ({ page }) => {
      // Check feature grid on desktop
      // Expected: Features display in multi-column grid layout

      const featuresSection = page.locator('#features');
      await featuresSection.scrollIntoViewIfNeeded();

      const featuresGrid = page.locator('.features-grid');
      await expect(featuresGrid).toBeVisible();

      const featureCards = page.locator('.feature-card');
      const cardCount = await featureCards.count();

      // Should have 7 feature cards
      expect(cardCount).toBe(7);

      // Get positions of first few cards to determine column layout
      const firstCard = featureCards.nth(0);
      const secondCard = featureCards.nth(1);
      const thirdCard = featureCards.nth(2);
      const fourthCard = featureCards.nth(3);

      const firstBox = await firstCard.boundingBox();
      const secondBox = await secondCard.boundingBox();
      const thirdBox = await thirdCard.boundingBox();
      const fourthBox = await fourthCard.boundingBox();

      expect(firstBox).not.toBeNull();
      expect(secondBox).not.toBeNull();
      expect(thirdBox).not.toBeNull();
      expect(fourthBox).not.toBeNull();

      if (firstBox && secondBox && thirdBox && fourthBox) {
        // Check if cards are on the same row (indicating multi-column layout)
        // On desktop (1280px), we expect at least 3 columns based on minmax(300px, 1fr) grid
        // 1200px container / 300px min width = at least 3 columns possible
        const firstRowCards = [firstBox, secondBox, thirdBox].filter(
          (box) => Math.abs(box.y - firstBox.y) < 10
        );

        // On desktop viewport, we should have at least 3 columns
        expect(firstRowCards.length).toBeGreaterThanOrEqual(3);
      }

      // Verify all cards have consistent width
      const cardWidths: number[] = [];
      for (let i = 0; i < Math.min(6, cardCount); i++) {
        const card = featureCards.nth(i);
        const box = await card.boundingBox();
        if (box) {
          cardWidths.push(box.width);
        }
      }

      // Cards should have similar widths (within tolerance)
      if (cardWidths.length > 1) {
        const maxWidth = Math.max(...cardWidths);
        const minWidth = Math.min(...cardWidths);
        expect(maxWidth - minWidth).toBeLessThan(50); // Allow small variance
      }
    });

    test('TC4: All navigation items are visible without hamburger menu', async ({ page }) => {
      // Verify navigation is fully visible
      // Expected: All navigation items are visible without hamburger menu

      // Check that CTA navigation buttons are visible and not collapsed
      const ctaButtons = page.locator('.cta-buttons');
      await expect(ctaButtons).toBeVisible();

      // Check both CTA buttons are visible
      const getStartedButton = page.locator('a.cta-button', { hasText: 'Get Started' });
      await expect(getStartedButton).toBeVisible();

      const githubButton = page.locator('a.cta-button', { hasText: 'View on GitHub' });
      await expect(githubButton).toBeVisible();

      // Verify buttons are side-by-side (not stacked)
      const getStartedBox = await getStartedButton.boundingBox();
      const githubBox = await githubButton.boundingBox();

      expect(getStartedBox).not.toBeNull();
      expect(githubBox).not.toBeNull();

      if (getStartedBox && githubBox) {
        // Buttons should be on the same row (y positions similar)
        const buttonsOnSameRow = Math.abs(getStartedBox.y - githubBox.y) < 10;
        expect(buttonsOnSameRow).toBe(true);

        // Buttons should not overlap
        const buttonsDoNotOverlap =
          getStartedBox.x + getStartedBox.width <= githubBox.x ||
          githubBox.x + githubBox.width <= getStartedBox.x;
        expect(buttonsDoNotOverlap).toBe(true);
      }

      // Verify there is no hamburger menu element visible (desktop should show full nav)
      const hamburgerMenu = page.locator('.hamburger, .mobile-menu, .menu-toggle');
      const hamburgerCount = await hamburgerMenu.count();

      // If hamburger elements exist, they should be hidden on desktop
      if (hamburgerCount > 0) {
        for (let i = 0; i < hamburgerCount; i++) {
          await expect(hamburgerMenu.nth(i)).not.toBeVisible();
        }
      }
    });
  });

  test.describe('Full HD Desktop (1920px)', () => {
    test.beforeEach(async ({ page }) => {
      // Set viewport to Full HD dimensions (1920px width)
      await page.setViewportSize({
        width: FULL_HD_VIEWPORT_WIDTH,
        height: FULL_HD_VIEWPORT_HEIGHT,
      });
      await page.goto('/');
    });

    test('TC5: Page remains well-formatted at large desktop sizes (1920px)', async ({ page }) => {
      // Check page at 1920px width (full HD)
      // Expected: Page remains well-formatted at large desktop sizes

      // Check no horizontal scrollbar
      const hasHorizontalScrollbar = await page.evaluate(() => {
        return document.documentElement.scrollWidth > document.documentElement.clientWidth;
      });
      expect(hasHorizontalScrollbar).toBe(false);

      // Hero section should span full width
      const heroSection = page.locator('.hero-section');
      await expect(heroSection).toBeVisible();

      const heroBox = await heroSection.boundingBox();
      expect(heroBox).not.toBeNull();

      if (heroBox) {
        expect(heroBox.width).toBe(FULL_HD_VIEWPORT_WIDTH);
      }

      // Content containers should still be constrained to max-width
      const featuresContainer = page.locator('.features-container');
      await featuresContainer.scrollIntoViewIfNeeded();

      const featuresBox = await featuresContainer.boundingBox();
      expect(featuresBox).not.toBeNull();

      if (featuresBox) {
        // Content should be centered and constrained
        expect(featuresBox.width).toBeLessThanOrEqual(MAX_CONTENT_WIDTH);

        // Should be centered (have significant left offset)
        const expectedLeftOffset = (FULL_HD_VIEWPORT_WIDTH - featuresBox.width) / 2;
        expect(featuresBox.x).toBeGreaterThanOrEqual(expectedLeftOffset - 50);
      }

      // Check architecture container
      const archContainer = page.locator('.architecture-container');
      await archContainer.scrollIntoViewIfNeeded();

      const archBox = await archContainer.boundingBox();
      expect(archBox).not.toBeNull();

      if (archBox) {
        expect(archBox.width).toBeLessThanOrEqual(MAX_CONTENT_WIDTH);
      }

      // Check status container
      const statusContainer = page.locator('.status-container');
      await statusContainer.scrollIntoViewIfNeeded();

      const statusBox = await statusContainer.boundingBox();
      expect(statusBox).not.toBeNull();

      if (statusBox) {
        expect(statusBox.width).toBeLessThanOrEqual(MAX_CONTENT_WIDTH);
      }

      // Feature grid should show multi-column layout
      const featureCards = page.locator('.feature-card');
      const firstCard = featureCards.nth(0);
      const secondCard = featureCards.nth(1);
      const thirdCard = featureCards.nth(2);

      await firstCard.scrollIntoViewIfNeeded();

      const firstBox = await firstCard.boundingBox();
      const secondBox = await secondCard.boundingBox();
      const thirdBox = await thirdCard.boundingBox();

      if (firstBox && secondBox && thirdBox) {
        // At Full HD, should have at least 3 columns
        const firstRowCards = [firstBox, secondBox, thirdBox].filter(
          (box) => Math.abs(box.y - firstBox.y) < 10
        );
        expect(firstRowCards.length).toBeGreaterThanOrEqual(3);
      }
    });

    test('TC6: Hero section utilizes space effectively on large screens', async ({ page }) => {
      // Additional test: Hero section layout on large screens
      const heroSection = page.locator('.hero-section');
      const heroContent = page.locator('.hero-content');

      await expect(heroSection).toBeVisible();
      await expect(heroContent).toBeVisible();

      const heroBox = await heroSection.boundingBox();
      const contentBox = await heroContent.boundingBox();

      expect(heroBox).not.toBeNull();
      expect(contentBox).not.toBeNull();

      if (heroBox && contentBox) {
        // Hero section should be full width
        expect(heroBox.width).toBe(FULL_HD_VIEWPORT_WIDTH);

        // Content should have max-width constraint
        expect(contentBox.width).toBeLessThanOrEqual(800); // max-width: 800px in CSS

        // Content should be centered
        const expectedLeftOffset = (FULL_HD_VIEWPORT_WIDTH - contentBox.width) / 2;
        expect(contentBox.x).toBeGreaterThanOrEqual(expectedLeftOffset - 50);
      }

      // Check title is appropriately sized
      const heroTitle = page.locator('.hero-section h1');
      const titleFontSize = await heroTitle.evaluate((el) => {
        return parseFloat(window.getComputedStyle(el).fontSize);
      });

      // Title should be large but not excessively so (clamp should work)
      expect(titleFontSize).toBeGreaterThanOrEqual(48);
      expect(titleFontSize).toBeLessThanOrEqual(80);
    });

    test('TC7: Status grid displays in multi-column layout on large screens', async ({ page }) => {
      // Test status grid layout at large screen size
      const statusSection = page.locator('#project-status');
      await statusSection.scrollIntoViewIfNeeded();

      const statusGrid = page.locator('.status-grid');
      await expect(statusGrid).toBeVisible();

      const completedCategory = page.locator('.completed-features');
      const plannedCategory = page.locator('.planned-features');

      const completedBox = await completedCategory.boundingBox();
      const plannedBox = await plannedCategory.boundingBox();

      expect(completedBox).not.toBeNull();
      expect(plannedBox).not.toBeNull();

      if (completedBox && plannedBox) {
        // At 1920px, both categories should be side by side
        const onSameRow = Math.abs(completedBox.y - plannedBox.y) < 10;
        expect(onSameRow).toBe(true);

        // Categories should have good width
        expect(completedBox.width).toBeGreaterThanOrEqual(350);
        expect(plannedBox.width).toBeGreaterThanOrEqual(350);
      }
    });

    test('TC8: Architecture explanation cards display side by side', async ({ page }) => {
      // Test architecture explanation layout
      const architectureSection = page.locator('#architecture');
      await architectureSection.scrollIntoViewIfNeeded();

      const explanationCards = page.locator('.explanation-card');
      const cardCount = await explanationCards.count();

      expect(cardCount).toBe(2); // Write Path and Read Path

      const firstCard = explanationCards.nth(0);
      const secondCard = explanationCards.nth(1);

      const firstBox = await firstCard.boundingBox();
      const secondBox = await secondCard.boundingBox();

      expect(firstBox).not.toBeNull();
      expect(secondBox).not.toBeNull();

      if (firstBox && secondBox) {
        // At 1920px (container 1200px), cards should be side by side
        // Grid is minmax(350px, 1fr), so 2 cards fit in 1200px
        const onSameRow = Math.abs(firstBox.y - secondBox.y) < 10;
        expect(onSameRow).toBe(true);

        // Each card should have good width
        expect(firstBox.width).toBeGreaterThanOrEqual(350);
        expect(secondBox.width).toBeGreaterThanOrEqual(350);
      }
    });
  });

  test.describe('Desktop Interaction Tests', () => {
    test.beforeEach(async ({ page }) => {
      await page.setViewportSize({
        width: DESKTOP_VIEWPORT_WIDTH,
        height: DESKTOP_VIEWPORT_HEIGHT,
      });
      await page.goto('/');
    });

    test('TC9: Smooth scrolling works for navigation links', async ({ page }) => {
      // Test navigation link functionality
      const getStartedButton = page.locator('a.cta-button', { hasText: 'Get Started' });
      await expect(getStartedButton).toBeVisible();

      // Click Get Started
      await getStartedButton.click();

      // Wait for smooth scroll animation
      await page.waitForTimeout(600);

      // Verify quickstart section is in view
      const quickstartSection = page.locator('#quickstart');
      await expect(quickstartSection).toBeInViewport();
    });

    test('TC10: Feature cards have hover effects on desktop', async ({ page }) => {
      const featuresSection = page.locator('#features');
      await featuresSection.scrollIntoViewIfNeeded();

      const featureCard = page.locator('.feature-card').first();
      await featureCard.scrollIntoViewIfNeeded();

      // Get initial transform and box-shadow
      const initialStyles = await featureCard.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          transform: styles.transform,
          boxShadow: styles.boxShadow,
        };
      });

      // Hover over the card
      await featureCard.hover();

      // Wait for transition
      await page.waitForTimeout(400);

      // Get hover styles
      const hoverStyles = await featureCard.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          transform: styles.transform,
          boxShadow: styles.boxShadow,
        };
      });

      // On hover, card should have different transform or box-shadow
      // The CSS has: transform: translateY(-4px); box-shadow on hover
      expect(hoverStyles.transform !== initialStyles.transform || hoverStyles.boxShadow !== initialStyles.boxShadow).toBe(true);
    });
  });
});
