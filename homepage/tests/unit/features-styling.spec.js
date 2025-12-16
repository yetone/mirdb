// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Unit Tests for Features Section Styling Consistency
 * Test Case 7: Verify feature cards have consistent styling
 * Expected: All feature cards have uniform dimensions, spacing, and typography
 */

test.describe('Features Section Styling - Unit Tests', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to homepage
    await page.goto('/');
    // Scroll to features section
    await page.locator('#features').scrollIntoViewIfNeeded();
  });

  /**
   * Test Case 7: Verify feature cards have consistent styling
   * Input: Verify feature cards have consistent styling
   * Expected: All feature cards have uniform dimensions, spacing, and typography
   */
  test('TC7: Feature cards have consistent styling (dimensions, spacing, typography)', async ({ page }) => {
    const featureCards = page.locator('.feature-card');
    const count = await featureCards.count();

    expect(count).toBe(6);

    // Collect computed styles for all cards
    const cardStyles = [];
    for (let i = 0; i < count; i++) {
      const card = featureCards.nth(i);
      const styles = await card.evaluate((el) => {
        const computed = window.getComputedStyle(el);
        return {
          padding: computed.padding,
          paddingTop: computed.paddingTop,
          paddingBottom: computed.paddingBottom,
          paddingLeft: computed.paddingLeft,
          paddingRight: computed.paddingRight,
          borderRadius: computed.borderRadius,
          backgroundColor: computed.backgroundColor,
          boxShadow: computed.boxShadow,
        };
      });
      cardStyles.push(styles);
    }

    // Verify all cards have the same padding
    const firstCardPadding = cardStyles[0].padding;
    for (let i = 1; i < cardStyles.length; i++) {
      expect(cardStyles[i].padding).toBe(firstCardPadding);
    }

    // Verify all cards have the same border-radius
    const firstCardBorderRadius = cardStyles[0].borderRadius;
    for (let i = 1; i < cardStyles.length; i++) {
      expect(cardStyles[i].borderRadius).toBe(firstCardBorderRadius);
    }

    // Verify all cards have the same background color
    const firstCardBgColor = cardStyles[0].backgroundColor;
    for (let i = 1; i < cardStyles.length; i++) {
      expect(cardStyles[i].backgroundColor).toBe(firstCardBgColor);
    }

    // Verify all cards have box shadow applied
    for (let i = 0; i < cardStyles.length; i++) {
      expect(cardStyles[i].boxShadow).not.toBe('none');
    }
  });

  /**
   * Test: Verify feature card titles have consistent typography
   */
  test('TC7-B: Feature card titles have consistent typography', async ({ page }) => {
    const cardTitles = page.locator('.feature-card-title');
    const count = await cardTitles.count();

    expect(count).toBe(6);

    // Collect computed styles for all titles
    const titleStyles = [];
    for (let i = 0; i < count; i++) {
      const title = cardTitles.nth(i);
      const styles = await title.evaluate((el) => {
        const computed = window.getComputedStyle(el);
        return {
          fontSize: computed.fontSize,
          fontWeight: computed.fontWeight,
          color: computed.color,
          marginBottom: computed.marginBottom,
        };
      });
      titleStyles.push(styles);
    }

    // Verify all titles have the same font size
    const firstTitleFontSize = titleStyles[0].fontSize;
    for (let i = 1; i < titleStyles.length; i++) {
      expect(titleStyles[i].fontSize).toBe(firstTitleFontSize);
    }

    // Verify all titles have the same font weight
    const firstTitleFontWeight = titleStyles[0].fontWeight;
    for (let i = 1; i < titleStyles.length; i++) {
      expect(titleStyles[i].fontWeight).toBe(firstTitleFontWeight);
    }

    // Verify all titles have the same color (primary color)
    const firstTitleColor = titleStyles[0].color;
    for (let i = 1; i < titleStyles.length; i++) {
      expect(titleStyles[i].color).toBe(firstTitleColor);
    }

    // Verify all titles have the same margin bottom
    const firstTitleMarginBottom = titleStyles[0].marginBottom;
    for (let i = 1; i < titleStyles.length; i++) {
      expect(titleStyles[i].marginBottom).toBe(firstTitleMarginBottom);
    }
  });

  /**
   * Test: Verify feature card descriptions have consistent typography
   */
  test('TC7-C: Feature card descriptions have consistent typography', async ({ page }) => {
    const cardDescriptions = page.locator('.feature-card-description');
    const count = await cardDescriptions.count();

    expect(count).toBe(6);

    // Collect computed styles for all descriptions
    const descStyles = [];
    for (let i = 0; i < count; i++) {
      const desc = cardDescriptions.nth(i);
      const styles = await desc.evaluate((el) => {
        const computed = window.getComputedStyle(el);
        return {
          fontSize: computed.fontSize,
          color: computed.color,
          lineHeight: computed.lineHeight,
        };
      });
      descStyles.push(styles);
    }

    // Verify all descriptions have the same font size
    const firstDescFontSize = descStyles[0].fontSize;
    for (let i = 1; i < descStyles.length; i++) {
      expect(descStyles[i].fontSize).toBe(firstDescFontSize);
    }

    // Verify all descriptions have the same color
    const firstDescColor = descStyles[0].color;
    for (let i = 1; i < descStyles.length; i++) {
      expect(descStyles[i].color).toBe(firstDescColor);
    }

    // Verify all descriptions have the same line height
    const firstDescLineHeight = descStyles[0].lineHeight;
    for (let i = 1; i < descStyles.length; i++) {
      expect(descStyles[i].lineHeight).toBe(firstDescLineHeight);
    }
  });

  /**
   * Test: Verify feature grid has proper grid layout
   */
  test('TC7-D: Feature grid uses CSS grid with proper gap', async ({ page }) => {
    const featureGrid = page.locator('[data-testid="feature-grid"]');
    await expect(featureGrid).toBeVisible();

    const gridStyles = await featureGrid.evaluate((el) => {
      const computed = window.getComputedStyle(el);
      return {
        display: computed.display,
        gap: computed.gap,
        gridTemplateColumns: computed.gridTemplateColumns,
      };
    });

    // Verify grid display is used
    expect(gridStyles.display).toBe('grid');

    // Verify gap is applied (should be 2rem = 32px)
    expect(gridStyles.gap).not.toBe('0px');
    expect(gridStyles.gap).not.toBe('normal');
  });

  /**
   * Test: Verify features section renders without JavaScript errors
   */
  test('TC7-E: Features section renders without JavaScript errors', async ({ page }) => {
    // Collect console errors during page load
    const consoleErrors = [];
    const pageErrors = [];

    page.on('console', (msg) => {
      if (msg.type() === 'error') {
        consoleErrors.push(msg.text());
      }
    });

    page.on('pageerror', (error) => {
      pageErrors.push(error.message);
    });

    // Navigate to homepage
    await page.goto('/');

    // Wait for page to fully load
    await page.waitForLoadState('domcontentloaded');

    // Verify features section is visible
    const featuresSection = page.locator('[data-testid="features-section"]');
    await expect(featuresSection).toBeVisible();

    // Filter for features-related errors only
    const featuresRelatedErrors = consoleErrors.filter(error =>
      error.toLowerCase().includes('feature') ||
      error.toLowerCase().includes('grid')
    );

    const featuresPageErrors = pageErrors.filter(error =>
      error.toLowerCase().includes('feature') ||
      error.toLowerCase().includes('grid')
    );

    // Verify no features-related console errors
    expect(featuresRelatedErrors).toHaveLength(0);
    expect(featuresPageErrors).toHaveLength(0);
  });
});
