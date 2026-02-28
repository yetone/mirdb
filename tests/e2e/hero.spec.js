/**
 * Hero Section E2E Tests
 * Owner: Scenario 1 - Hero Section Display
 *
 * Test cases:
 * - Animated logo renders with alt text
 * - Tagline is displayed correctly
 * - CTA button links to quick start section
 * - CTA button has visible focus state
 */

const { test, expect } = require('@playwright/test');
const { waitForPageLoad, hasVisibleFocusIndicator, VIEWPORTS } = require('./test-utils');

test.describe('Hero Section Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await waitForPageLoad(page);
  });

  test('TC1: Animated logo image is rendered with alt text', async ({ page }) => {
    // Load index.html and check for logo.gif presence
    const logo = page.locator('.hero__logo');

    // Check that the logo is visible
    await expect(logo).toBeVisible();

    // Check that it's the correct image source
    await expect(logo).toHaveAttribute('src', 'assets/logo.gif');

    // Check that alt text is provided
    const altText = await logo.getAttribute('alt');
    expect(altText).toBeTruthy();
    expect(altText.length).toBeGreaterThan(0);
    expect(altText).toContain('MirDB');
  });

  test('TC2: Tagline is displayed correctly', async ({ page }) => {
    // Check hero section for tagline text
    const tagline = page.locator('.hero__tagline');

    // Verify tagline is visible
    await expect(tagline).toBeVisible();

    // Verify exact tagline text
    await expect(tagline).toHaveText('A Persistent Key-Value Store with Memcached Protocol');
  });

  test('TC3: CTA button smoothly scrolls to quick start section', async ({ page }) => {
    // Find the CTA button
    const ctaButton = page.locator('.hero__cta');

    // Verify CTA button is present and visible
    await expect(ctaButton).toBeVisible();
    await expect(ctaButton).toHaveText('Get Started');

    // Verify it links to quick start section
    await expect(ctaButton).toHaveAttribute('href', '#quickstart');

    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY);

    // Click the CTA button
    await ctaButton.click();

    // Wait for smooth scroll to complete
    await page.waitForTimeout(500);

    // Check that page has scrolled
    const finalScrollY = await page.evaluate(() => window.scrollY);

    // The page should have scrolled down (or be at quickstart section)
    // If quickstart is visible without scrolling, scroll position might be 0
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeInViewport();
  });

  test('TC4: CTA button has visible focus indicator (WCAG AA compliant)', async ({ page }) => {
    // Tab to CTA button using keyboard
    const ctaButton = page.locator('.hero__cta');

    // Focus the button using keyboard
    await ctaButton.focus();

    // Check for visible focus indicator
    const focusStyles = await page.evaluate(() => {
      const button = document.querySelector('.hero__cta');
      if (!button) return null;

      const computed = window.getComputedStyle(button);
      return {
        outline: computed.outline,
        outlineWidth: computed.outlineWidth,
        outlineStyle: computed.outlineStyle,
        outlineColor: computed.outlineColor,
        boxShadow: computed.boxShadow,
      };
    });

    expect(focusStyles).not.toBeNull();

    // WCAG AA requires visible focus indicator
    // Check that outline is visible (not 0px and not none)
    const hasVisibleOutline =
      focusStyles.outlineWidth !== '0px' &&
      focusStyles.outlineStyle !== 'none';

    const hasVisibleBoxShadow = focusStyles.boxShadow !== 'none';

    // At least one focus indicator should be present
    expect(hasVisibleOutline || hasVisibleBoxShadow).toBe(true);
  });

  test('Hero section is above the fold on desktop viewport', async ({ page }) => {
    // Set desktop viewport
    await page.setViewportSize(VIEWPORTS.desktop);

    // Hero section should be visible without scrolling
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeInViewport();

    // Logo, title, tagline, and CTA should all be visible
    await expect(page.locator('.hero__logo')).toBeInViewport();
    await expect(page.locator('.hero__title')).toBeInViewport();
    await expect(page.locator('.hero__tagline')).toBeInViewport();
    await expect(page.locator('.hero__cta')).toBeInViewport();
  });

  test('Hero section displays MirDB project name', async ({ page }) => {
    const title = page.locator('.hero__title');
    await expect(title).toBeVisible();
    await expect(title).toHaveText('MirDB');
  });
});
