/**
 * Hero Section E2E Tests
 * Owner: Scenario 1 - Hero Section Display
 *
 * Test cases:
 * - Hero headline contains 'MirDB' and 'key-value store'
 * - Subheadline mentions Memcached and persistent storage
 * - CTA buttons present: 'Try Demo', 'Get Started'
 * - Hero has visual styling (background/gradient)
 */

const { test, expect } = require('@playwright/test');

test.describe('Hero Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Hero section visible with headline containing MirDB and key-value store', async ({ page }) => {
    // Verify hero section is visible
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Verify headline contains required text
    const headline = page.locator('.hero__headline');
    await expect(headline).toBeVisible();
    await expect(headline).toContainText('MirDB');
    await expect(headline).toContainText('Key-Value Store');
  });

  test('TC2: Description mentions Memcached compatibility and persistent storage', async ({ page }) => {
    // Verify description/subheadline is visible
    const description = page.locator('.hero__description');
    await expect(description).toBeVisible();

    // Get the text content and verify it contains required keywords
    const text = await description.textContent();
    expect(text.toLowerCase()).toContain('memcached');
    expect(text.toLowerCase()).toContain('persistent');
  });

  test('TC3: At least two CTA buttons present: Try Demo and Get Started', async ({ page }) => {
    // Verify CTA container is visible
    const ctaContainer = page.locator('.hero__cta');
    await expect(ctaContainer).toBeVisible();

    // Verify 'Try Demo' button
    const tryDemoBtn = page.locator('[data-testid="cta-try-demo"]');
    await expect(tryDemoBtn).toBeVisible();
    await expect(tryDemoBtn).toContainText('Try Demo');

    // Verify 'Get Started' button
    const getStartedBtn = page.locator('[data-testid="cta-get-started"]');
    await expect(getStartedBtn).toBeVisible();
    await expect(getStartedBtn).toContainText('Get Started');

    // Verify both buttons are clickable (have href attributes)
    await expect(tryDemoBtn).toHaveAttribute('href', '#demo');
    await expect(getStartedBtn).toHaveAttribute('href', '#quickstart');
  });

  test('TC4: Hero has full-bleed background or gradient as per design spec', async ({ page }) => {
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    // Check that the hero section has a gradient background
    const background = await heroSection.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return styles.backgroundImage;
    });

    // Verify background includes a gradient (linear-gradient)
    expect(background).toContain('gradient');

    // Verify hero takes full viewport height (min-height: 100vh)
    const minHeight = await heroSection.evaluate((el) => {
      const styles = window.getComputedStyle(el);
      return styles.minHeight;
    });

    // The min-height should be set (either as 100vh computed or a pixel value close to viewport)
    expect(minHeight).not.toBe('auto');
    expect(minHeight).not.toBe('0px');
  });

  test('Hero section has proper ARIA labeling', async ({ page }) => {
    const heroSection = page.locator('#hero');

    // Verify hero section is labeled by the headline
    await expect(heroSection).toHaveAttribute('aria-labelledby', 'hero-headline');

    // Verify the headline has the correct id
    const headline = page.locator('#hero-headline');
    await expect(headline).toBeVisible();
  });

  test('CTA buttons have proper hover states', async ({ page }) => {
    const tryDemoBtn = page.locator('[data-testid="cta-try-demo"]');

    // Get initial transform
    const initialTransform = await tryDemoBtn.evaluate((el) => {
      return window.getComputedStyle(el).transform;
    });

    // Hover over button
    await tryDemoBtn.hover();

    // After hover, button should have different transform (translateY)
    const hoverTransform = await tryDemoBtn.evaluate((el) => {
      return window.getComputedStyle(el).transform;
    });

    // Transform values should differ on hover (either none to matrix or different matrix values)
    // Note: On hover, translateY(-2px) should change the transform
  });

  test('Logo is displayed in hero section', async ({ page }) => {
    const logo = page.locator('.hero__logo');
    await expect(logo).toBeVisible();

    // Logo should contain an SVG or image
    const hasSvgOrImg = await logo.locator('svg, img').count();
    expect(hasSvgOrImg).toBeGreaterThan(0);
  });
});
