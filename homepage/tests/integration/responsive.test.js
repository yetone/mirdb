// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Responsive Design Integration Tests - Scenario 7
 *
 * Tests component-level responsive behavior including grid layouts,
 * typography scaling, and section-specific responsive patterns.
 *
 * Test Cases:
 * 4: Check features grid at 320px (single column)
 * 5: Check features grid at 1024px (3-4 columns)
 * 8: Check hero section on mobile
 */

test.describe('Responsive Design - Integration Tests', () => {
  /**
   * Test Case 4: Check features grid at 320px
   * Expected: Features stack in single column layout
   */
  test('TC4: Features grid displays single column on mobile (320px)', async ({ page }) => {
    await page.setViewportSize({ width: 320, height: 568 });
    await page.goto('/index.html');

    const featuresGrid = page.locator('.features__grid');
    await expect(featuresGrid).toBeVisible();

    const gridStyle = await featuresGrid.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        display: styles.display,
        gridTemplateColumns: styles.gridTemplateColumns,
      };
    });

    // Should be using CSS Grid
    expect(gridStyle.display).toBe('grid');

    // Should have single column
    const columnValues = gridStyle.gridTemplateColumns.split(' ').filter(v => v.trim() !== '');
    expect(columnValues.length).toBe(1);

    // Verify all feature cards are visible and properly stacked
    const featureCards = page.locator('.feature-card');
    const count = await featureCards.count();
    expect(count).toBeGreaterThan(0);

    // All cards should be full width (single column)
    for (let i = 0; i < count; i++) {
      const card = featureCards.nth(i);
      await expect(card).toBeVisible();
    }
  });

  /**
   * Test Case 5: Check features grid at 1024px
   * Expected: Features display in multi-column grid (3-4 columns)
   */
  test('TC5: Features grid displays 3+ columns on desktop (1024px)', async ({ page }) => {
    await page.setViewportSize({ width: 1024, height: 768 });
    await page.goto('/index.html');

    const featuresGrid = page.locator('.features__grid');
    await expect(featuresGrid).toBeVisible();

    const gridStyle = await featuresGrid.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        display: styles.display,
        gridTemplateColumns: styles.gridTemplateColumns,
      };
    });

    expect(gridStyle.display).toBe('grid');

    // Should have 3 or 4 columns
    const columnValues = gridStyle.gridTemplateColumns.split(' ').filter(v => v.trim() !== '');
    expect(columnValues.length).toBeGreaterThanOrEqual(3);
    expect(columnValues.length).toBeLessThanOrEqual(4);
  });

  /**
   * Test Case 8: Check hero section on mobile
   * Expected: Hero content is readable with appropriate text sizing
   */
  test('TC8: Hero section content is readable on mobile with appropriate text sizing', async ({ page }) => {
    await page.setViewportSize({ width: 320, height: 568 });
    await page.goto('/index.html');

    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Check hero title font size
    const heroTitle = page.locator('.hero__title');
    await expect(heroTitle).toBeVisible();
    const titleStyles = await heroTitle.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        fontSize: parseFloat(styles.fontSize),
        lineHeight: styles.lineHeight,
      };
    });

    // Title should be at least 20px for readability
    expect(titleStyles.fontSize).toBeGreaterThanOrEqual(20);

    // Check hero tagline font size
    const heroTagline = page.locator('.hero__tagline');
    await expect(heroTagline).toBeVisible();
    const taglineStyles = await heroTagline.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });

    // Tagline should be at least 14px for readability
    expect(taglineStyles).toBeGreaterThanOrEqual(14);

    // Check hero subtitle font size
    const heroSubtitle = page.locator('.hero__subtitle');
    await expect(heroSubtitle).toBeVisible();
    const subtitleStyles = await heroSubtitle.evaluate((el) => {
      return parseFloat(window.getComputedStyle(el).fontSize);
    });

    // Subtitle should be at least 12px for readability
    expect(subtitleStyles).toBeGreaterThanOrEqual(12);

    // Verify hero content fits within viewport
    const heroMetrics = await heroSection.evaluate((el) => {
      const rect = el.getBoundingClientRect();
      return {
        width: rect.width,
        viewportWidth: window.innerWidth,
      };
    });

    expect(heroMetrics.width).toBeLessThanOrEqual(heroMetrics.viewportWidth);
  });

  /**
   * Additional test: Verify responsive container widths
   */
  test('Containers have appropriate max-width at different viewports', async ({ page }) => {
    // Test at large desktop
    await page.setViewportSize({ width: 1920, height: 1080 });
    await page.goto('/index.html');

    const mainElement = page.locator('main');
    const mainWidth = await mainElement.evaluate((el) => {
      const rect = el.getBoundingClientRect();
      const styles = window.getComputedStyle(el);
      return {
        actualWidth: rect.width,
        maxWidth: styles.maxWidth,
        viewportWidth: window.innerWidth,
      };
    });

    // Main content should be constrained even on large viewports
    expect(mainWidth.actualWidth).toBeLessThan(mainWidth.viewportWidth);
  });

  /**
   * Additional test: Verify responsive spacing and padding
   */
  test('Sections have appropriate padding at mobile viewport', async ({ page }) => {
    await page.setViewportSize({ width: 320, height: 568 });
    await page.goto('/index.html');

    const heroSection = page.locator('#hero');
    const heroPadding = await heroSection.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        paddingLeft: parseFloat(styles.paddingLeft),
        paddingRight: parseFloat(styles.paddingRight),
      };
    });

    // Should have some padding on mobile
    expect(heroPadding.paddingLeft).toBeGreaterThan(0);
    expect(heroPadding.paddingRight).toBeGreaterThan(0);
  });

  /**
   * Additional test: Navigation wrapping behavior on narrow viewports
   */
  test('Navigation adapts properly at 320px viewport', async ({ page }) => {
    await page.setViewportSize({ width: 320, height: 568 });
    await page.goto('/index.html');

    const nav = page.locator('.nav');
    await expect(nav).toBeVisible();

    // Verify nav is using flex with wrap
    const navStyles = await nav.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        display: styles.display,
        flexWrap: styles.flexWrap,
      };
    });

    expect(navStyles.display).toBe('flex');
    expect(navStyles.flexWrap).toBe('wrap');
  });

  /**
   * Test: Quick start section responsive behavior
   */
  test('Quick start section adapts to mobile viewport', async ({ page }) => {
    await page.setViewportSize({ width: 320, height: 568 });
    await page.goto('/index.html');

    const quickstartCommand = page.locator('.quickstart__command').first();
    await expect(quickstartCommand).toBeVisible();

    const commandStyles = await quickstartCommand.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        flexDirection: styles.flexDirection,
        marginLeft: styles.marginLeft,
      };
    });

    // Commands should stack vertically on mobile
    expect(commandStyles.flexDirection).toBe('column');
    // Should have no left margin on mobile
    expect(parseFloat(commandStyles.marginLeft)).toBe(0);
  });

  /**
   * Test: Status section responsive behavior
   */
  test('Status section items adapt to mobile viewport', async ({ page }) => {
    await page.setViewportSize({ width: 320, height: 568 });
    await page.goto('/index.html');

    const statusItem = page.locator('.status__item').first();
    await expect(statusItem).toBeVisible();

    const itemStyles = await statusItem.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      const rect = el.getBoundingClientRect();
      return {
        minHeight: parseFloat(styles.minHeight),
        height: rect.height,
      };
    });

    // Should have minimum touch target height
    expect(itemStyles.height).toBeGreaterThanOrEqual(44);
  });

  /**
   * Test: Footer responsive behavior
   */
  test('Footer adapts to mobile viewport', async ({ page }) => {
    await page.setViewportSize({ width: 320, height: 568 });
    await page.goto('/index.html');

    const footerContainer = page.locator('.footer__container');
    await expect(footerContainer).toBeVisible();

    const footerStyles = await footerContainer.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        flexDirection: styles.flexDirection,
      };
    });

    // Footer should stack vertically on mobile
    expect(footerStyles.flexDirection).toBe('column');
  });
});
