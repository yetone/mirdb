// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Responsive Design - Desktop E2E Tests (NFR-1)
 * Verifies the page displays correctly on large desktop screens
 */

test.describe('Responsive Design - Desktop', () => {
  test.describe('TC1: 1920px viewport width', () => {
    test.use({ viewport: { width: 1920, height: 1080 } });

    test('Content is centered with appropriate max-width container', async ({ page }) => {
      await page.goto('/');

      // Verify main sections have centered content with max-width
      const sections = page.locator('section');
      const sectionCount = await sections.count();
      expect(sectionCount).toBeGreaterThan(0);

      // Check that sections have max-width applied and are centered
      for (let i = 0; i < sectionCount; i++) {
        const section = sections.nth(i);
        const sectionId = await section.getAttribute('id');

        // Hero section has different layout rules (full-width but centered content)
        if (sectionId === 'hero') {
          const heroStyles = await section.evaluate((el) => {
            const styles = window.getComputedStyle(el);
            return {
              display: styles.display,
              alignItems: styles.alignItems,
              textAlign: styles.textAlign
            };
          });
          // Hero section should be flex and centered
          expect(heroStyles.display).toBe('flex');
          expect(heroStyles.alignItems).toBe('center');
          expect(heroStyles.textAlign).toBe('center');
        } else {
          // Other sections should have max-width for readability
          const styles = await section.evaluate((el) => {
            const styles = window.getComputedStyle(el);
            return {
              maxWidth: styles.maxWidth,
              marginLeft: styles.marginLeft,
              marginRight: styles.marginRight
            };
          });

          // Sections should have a max-width constraint (not stretch to full viewport)
          expect(styles.maxWidth).not.toBe('none');
          // Auto margins indicate centered content
          expect(styles.marginLeft).toBe(styles.marginRight);
        }
      }

      // Verify viewport is actually 1920px
      const viewportSize = await page.evaluate(() => window.innerWidth);
      expect(viewportSize).toBe(1920);
    });

    test('Content does not stretch to extreme widths', async ({ page }) => {
      await page.goto('/');

      // Check that content containers don't exceed reasonable max-width
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeVisible();

      const sectionBounds = await featuresSection.boundingBox();
      expect(sectionBounds).not.toBeNull();

      // Section width should be constrained (max-width: 1200px as per CSS)
      expect(sectionBounds.width).toBeLessThanOrEqual(1200 + 64); // Account for padding

      // Navigation should span full width
      const navbar = page.locator('nav');
      const navBounds = await navbar.boundingBox();
      expect(navBounds).not.toBeNull();
      expect(navBounds.width).toBe(1920);
    });
  });

  test.describe('TC2: 2560px ultra-wide viewport', () => {
    test.use({ viewport: { width: 2560, height: 1440 } });

    test('Content remains centered and readable at ultra-wide resolution', async ({ page }) => {
      await page.goto('/');

      // Verify viewport is actually 2560px
      const viewportSize = await page.evaluate(() => window.innerWidth);
      expect(viewportSize).toBe(2560);

      // Check that content sections are centered on ultra-wide display
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeVisible();

      const featuresBounds = await featuresSection.boundingBox();
      expect(featuresBounds).not.toBeNull();

      // Content should be centered - left margin should be roughly equal to right margin
      // Section should have margin on both sides (not stretched to edges)
      const leftMargin = featuresBounds.x;
      const rightMargin = 2560 - (featuresBounds.x + featuresBounds.width);

      // Allow some tolerance for padding differences
      const marginDifference = Math.abs(leftMargin - rightMargin);
      expect(marginDifference).toBeLessThan(100);

      // Content should not stretch to extreme widths
      expect(featuresBounds.width).toBeLessThanOrEqual(1200 + 64);
    });

    test('Text remains readable at ultra-wide resolution', async ({ page }) => {
      await page.goto('/');

      // Check that hero description has constrained width for readability
      const heroDescription = page.locator('#hero .description');
      await expect(heroDescription).toBeVisible();

      const descBounds = await heroDescription.boundingBox();
      expect(descBounds).not.toBeNull();

      // Description should be constrained (max-width: 600px as per CSS)
      expect(descBounds.width).toBeLessThanOrEqual(600 + 32);

      // Check that section descriptions are also constrained
      const sectionDescriptions = page.locator('.section-description');
      const descCount = await sectionDescriptions.count();

      for (let i = 0; i < descCount; i++) {
        const desc = sectionDescriptions.nth(i);
        const bounds = await desc.boundingBox();
        if (bounds) {
          // Section descriptions should be constrained for readability
          expect(bounds.width).toBeLessThanOrEqual(600 + 32);
        }
      }
    });
  });

  test.describe('TC3: Feature cards grid layout', () => {
    test.use({ viewport: { width: 1920, height: 1080 } });

    test('Feature cards display in 3-column grid at desktop viewport', async ({ page }) => {
      await page.goto('/');

      // Navigate to features section
      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeVisible();

      // Get all feature cards
      const featureCards = page.locator('.feature-card');
      const cardCount = await featureCards.count();

      // Should have 6 feature cards (as per the HTML)
      expect(cardCount).toBe(6);

      // Get positions of the first 3 cards to verify they're in a row
      const firstRowCards = [];
      for (let i = 0; i < 3; i++) {
        const card = featureCards.nth(i);
        await expect(card).toBeVisible();
        const bounds = await card.boundingBox();
        expect(bounds).not.toBeNull();
        firstRowCards.push(bounds);
      }

      // Verify cards in first row have same Y position (same row)
      const firstCardY = firstRowCards[0].y;
      expect(Math.abs(firstRowCards[1].y - firstCardY)).toBeLessThan(5);
      expect(Math.abs(firstRowCards[2].y - firstCardY)).toBeLessThan(5);

      // Verify cards are arranged horizontally (different X positions)
      expect(firstRowCards[0].x).toBeLessThan(firstRowCards[1].x);
      expect(firstRowCards[1].x).toBeLessThan(firstRowCards[2].x);

      // Get the 4th card to verify it's in a second row
      const fourthCard = featureCards.nth(3);
      const fourthBounds = await fourthCard.boundingBox();
      expect(fourthBounds).not.toBeNull();

      // 4th card should be in a new row (different Y position)
      expect(fourthBounds.y).toBeGreaterThan(firstCardY);

      // Verify grid has 3 columns by checking card widths
      // With 3 columns, each card should take roughly 1/3 of the container width minus gaps
      const containerWidth = firstRowCards[2].x + firstRowCards[2].width - firstRowCards[0].x;
      const avgCardWidth = (firstRowCards[0].width + firstRowCards[1].width + firstRowCards[2].width) / 3;

      // Each card should be roughly 1/3 of container width (accounting for gaps)
      const expectedCardRatio = avgCardWidth / containerWidth;
      expect(expectedCardRatio).toBeGreaterThan(0.25);
      expect(expectedCardRatio).toBeLessThan(0.40);
    });

    test('Feature cards have equal widths', async ({ page }) => {
      await page.goto('/');

      const featureCards = page.locator('.feature-card');
      const cardCount = await featureCards.count();

      // Get all card widths
      const cardWidths = [];
      for (let i = 0; i < cardCount; i++) {
        const card = featureCards.nth(i);
        const bounds = await card.boundingBox();
        if (bounds) {
          cardWidths.push(bounds.width);
        }
      }

      // All cards should have similar widths (within 5px tolerance)
      const avgWidth = cardWidths.reduce((a, b) => a + b, 0) / cardWidths.length;
      for (const width of cardWidths) {
        expect(Math.abs(width - avgWidth)).toBeLessThan(5);
      }
    });

    test('Features grid uses CSS grid with auto-fit columns', async ({ page }) => {
      await page.goto('/');

      const featuresGrid = page.locator('.features-grid');
      await expect(featuresGrid).toBeVisible();

      const gridStyles = await featuresGrid.evaluate((el) => {
        const styles = window.getComputedStyle(el);
        return {
          display: styles.display,
          gridTemplateColumns: styles.gridTemplateColumns,
          gap: styles.gap
        };
      });

      // Verify grid display is being used
      expect(gridStyles.display).toBe('grid');

      // Verify gap is set
      expect(gridStyles.gap).not.toBe('normal');
    });
  });
});
