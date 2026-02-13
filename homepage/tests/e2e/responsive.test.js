// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Responsive Design E2E Tests - Scenario 7
 *
 * Validates the homepage displays correctly and is usable across
 * all viewport sizes from 320px to 1920px+
 *
 * Test Cases:
 * 1: Load page at 320px viewport width
 * 2: Load page at 768px viewport width
 * 3: Load page at 1440px viewport width
 * 6: Check code block at 320px viewport
 * 7: Check navigation on mobile (320px)
 * 9: Touch targets on mobile
 */

test.describe('Responsive Design - E2E Tests', () => {
  /**
   * Test Case 1: Load page at 320px viewport width
   * Expected: All content is visible without horizontal page scroll; elements stack vertically
   */
  test('TC1: Page loads correctly at 320px viewport without horizontal scroll', async ({ page }) => {
    // Set mobile viewport BEFORE navigating
    await page.setViewportSize({ width: 320, height: 568 });
    await page.goto('/index.html');

    // Verify page loads
    await expect(page.locator('body')).toBeVisible();

    // Check that document width doesn't exceed viewport (no horizontal scroll)
    const pageMetrics = await page.evaluate(() => {
      return {
        documentWidth: document.documentElement.scrollWidth,
        viewportWidth: window.innerWidth,
        bodyOverflowX: window.getComputedStyle(document.body).overflowX,
        htmlOverflowX: window.getComputedStyle(document.documentElement).overflowX,
      };
    });

    // Document should not be wider than viewport
    expect(pageMetrics.documentWidth).toBeLessThanOrEqual(pageMetrics.viewportWidth + 1); // +1 for rounding

    // Verify all major sections are visible
    await expect(page.locator('#hero')).toBeVisible();
    await expect(page.locator('#features')).toBeVisible();
    await expect(page.locator('#code-example')).toBeVisible();
    await expect(page.locator('#quickstart')).toBeVisible();
    await expect(page.locator('#status')).toBeVisible();
    await expect(page.locator('.footer')).toBeVisible();

    // Verify hero CTA buttons stack vertically on mobile
    const ctaGroup = page.locator('.hero__cta-group');
    const ctaFlexDirection = await ctaGroup.evaluate((el) => {
      return window.getComputedStyle(el).flexDirection;
    });
    expect(ctaFlexDirection).toBe('column');
  });

  /**
   * Test Case 2: Load page at 768px viewport width
   * Expected: Layout adapts to tablet view with appropriate spacing
   */
  test('TC2: Page loads correctly at 768px tablet viewport', async ({ page }) => {
    await page.setViewportSize({ width: 768, height: 1024 });
    await page.goto('/index.html');

    // Verify page loads
    await expect(page.locator('body')).toBeVisible();

    // Verify no horizontal scroll
    const pageMetrics = await page.evaluate(() => {
      return {
        documentWidth: document.documentElement.scrollWidth,
        viewportWidth: window.innerWidth,
      };
    });
    expect(pageMetrics.documentWidth).toBeLessThanOrEqual(pageMetrics.viewportWidth + 1);

    // Verify features grid has 2 columns on tablet
    const featuresGrid = page.locator('.features__grid');
    await expect(featuresGrid).toBeVisible();

    const gridStyle = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).gridTemplateColumns;
    });

    const columnCount = gridStyle.split(' ').filter(v => v.trim() !== '').length;
    expect(columnCount).toBe(2);

    // Verify hero CTA buttons are in a row on tablet
    const ctaGroup = page.locator('.hero__cta-group');
    const ctaFlexDirection = await ctaGroup.evaluate((el) => {
      return window.getComputedStyle(el).flexDirection;
    });
    expect(ctaFlexDirection).toBe('row');
  });

  /**
   * Test Case 3: Load page at 1440px viewport width
   * Expected: Full desktop layout displays with max-width container
   */
  test('TC3: Page loads correctly at 1440px desktop viewport with max-width container', async ({ page }) => {
    await page.setViewportSize({ width: 1440, height: 900 });
    await page.goto('/index.html');

    // Verify page loads
    await expect(page.locator('body')).toBeVisible();

    // Verify no horizontal scroll
    const pageMetrics = await page.evaluate(() => {
      return {
        documentWidth: document.documentElement.scrollWidth,
        viewportWidth: window.innerWidth,
      };
    });
    expect(pageMetrics.documentWidth).toBeLessThanOrEqual(pageMetrics.viewportWidth + 1);

    // Verify main content has max-width applied and is centered
    const mainElement = page.locator('main');
    const mainStyles = await mainElement.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        maxWidth: styles.maxWidth,
        marginLeft: styles.marginLeft,
        marginRight: styles.marginRight,
      };
    });

    // Max-width should be set (not 'none')
    expect(mainStyles.maxWidth).not.toBe('none');
    // Should be centered (auto margins)
    expect(mainStyles.marginLeft).toBe(mainStyles.marginRight);

    // Verify features grid has 3 columns on desktop
    const featuresGrid = page.locator('.features__grid');
    const gridStyle = await featuresGrid.evaluate((el) => {
      return window.getComputedStyle(el).gridTemplateColumns;
    });

    const columnCount = gridStyle.split(' ').filter(v => v.trim() !== '').length;
    expect(columnCount).toBeGreaterThanOrEqual(3);
  });

  /**
   * Test Case 6: Check code block at 320px viewport
   * Expected: Code block has horizontal scroll if needed; font remains readable
   */
  test('TC6: Code block has horizontal scroll and readable font on mobile', async ({ page }) => {
    await page.setViewportSize({ width: 320, height: 568 });
    await page.goto('/index.html');

    const codeBlock = page.locator('.code-block');
    await expect(codeBlock).toBeVisible();

    const codeContent = page.locator('.code-block__content');
    await expect(codeContent).toBeVisible();

    // Check overflow-x is auto or scroll for horizontal scrolling
    const codeStyles = await codeContent.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return {
        overflowX: styles.overflowX,
        fontSize: styles.fontSize,
        fontSizeValue: parseFloat(styles.fontSize),
      };
    });

    // Should allow horizontal scroll
    expect(['auto', 'scroll']).toContain(codeStyles.overflowX);

    // Font should be readable (at least 12px)
    expect(codeStyles.fontSizeValue).toBeGreaterThanOrEqual(12);
  });

  /**
   * Test Case 7: Check navigation on mobile (320px)
   * Expected: Navigation is accessible (hamburger menu if needed, or compact layout)
   */
  test('TC7: Navigation is accessible on mobile viewport', async ({ page }) => {
    await page.setViewportSize({ width: 320, height: 568 });
    await page.goto('/index.html');

    const nav = page.locator('.nav');
    await expect(nav).toBeVisible();

    // Check navigation links are present and visible (compact layout approach)
    const navLinks = page.locator('.nav__links');
    await expect(navLinks).toBeVisible();

    // Verify the logo is visible
    const logo = page.locator('.nav__logo');
    await expect(logo).toBeVisible();

    // Verify at least one navigation link is accessible
    const featureLink = page.locator('.nav__link').first();
    await expect(featureLink).toBeVisible();

    // Verify navigation doesn't cause overflow
    const navStyles = await nav.evaluate((el) => {
      const rect = el.getBoundingClientRect();
      return {
        width: rect.width,
        viewportWidth: window.innerWidth,
      };
    });

    expect(navStyles.width).toBeLessThanOrEqual(navStyles.viewportWidth);
  });

  /**
   * Test Case 9: Touch targets on mobile
   * Expected: All interactive elements have minimum 44x44px touch target
   */
  test('TC9: All interactive elements have minimum 44x44px touch target on mobile', async ({ page }) => {
    await page.setViewportSize({ width: 320, height: 568 });
    await page.goto('/index.html');

    // Check primary CTA button
    const primaryCta = page.locator('.hero__cta--primary');
    await expect(primaryCta).toBeVisible();
    const primaryCtaSize = await primaryCta.evaluate((el) => {
      const rect = el.getBoundingClientRect();
      return { width: rect.width, height: rect.height };
    });
    expect(primaryCtaSize.height).toBeGreaterThanOrEqual(44);

    // Check secondary CTA button
    const secondaryCta = page.locator('.hero__cta--secondary');
    await expect(secondaryCta).toBeVisible();
    const secondaryCtaSize = await secondaryCta.evaluate((el) => {
      const rect = el.getBoundingClientRect();
      return { width: rect.width, height: rect.height };
    });
    expect(secondaryCtaSize.height).toBeGreaterThanOrEqual(44);

    // Check navigation links
    const navLink = page.locator('.nav__link').first();
    await expect(navLink).toBeVisible();
    const navLinkSize = await navLink.evaluate((el) => {
      const rect = el.getBoundingClientRect();
      return { width: rect.width, height: rect.height };
    });
    expect(navLinkSize.height).toBeGreaterThanOrEqual(44);

    // Check theme toggle button
    const themeToggle = page.locator('.theme-toggle');
    await expect(themeToggle).toBeVisible();
    const themeToggleSize = await themeToggle.evaluate((el) => {
      const rect = el.getBoundingClientRect();
      return { width: rect.width, height: rect.height };
    });
    expect(themeToggleSize.width).toBeGreaterThanOrEqual(44);
    expect(themeToggleSize.height).toBeGreaterThanOrEqual(44);

    // Check code copy button
    const copyButton = page.locator('.code-block__copy').first();
    await expect(copyButton).toBeVisible();
    const copyButtonSize = await copyButton.evaluate((el) => {
      const rect = el.getBoundingClientRect();
      return { width: rect.width, height: rect.height };
    });
    expect(copyButtonSize.width).toBeGreaterThanOrEqual(44);
    expect(copyButtonSize.height).toBeGreaterThanOrEqual(44);
  });
});
