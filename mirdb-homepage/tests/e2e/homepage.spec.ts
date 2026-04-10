/**
 * Homepage E2E Tests
 * Owner: Shared across Scenarios 1, 2, 3, 4, 9, 16
 *
 * Playwright tests for homepage functionality.
 */

import { test, expect } from '@playwright/test';

/**
 * Scenario 1: Hero Section and Value Proposition Tests
 */
test.describe('Hero Section', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 1: Project name MirDB is visible in large typography (48px+ on desktop)', async ({ page }) => {
    // Set desktop viewport
    await page.setViewportSize({ width: 1280, height: 720 });

    const heroTitle = page.getByTestId('hero-title');
    await expect(heroTitle).toBeVisible();
    await expect(heroTitle).toHaveText('MirDB');

    // Verify font size is at least 48px
    const fontSize = await heroTitle.evaluate((el) => {
      return window.getComputedStyle(el).fontSize;
    });
    const fontSizeNum = parseFloat(fontSize);
    expect(fontSizeNum).toBeGreaterThanOrEqual(48);
  });

  test('Test Case 2: Tagline describing MirDB as a high-performance Rust key-value store is visible', async ({ page }) => {
    const tagline = page.getByTestId('hero-tagline');
    await expect(tagline).toBeVisible();

    const taglineText = await tagline.textContent();
    expect(taglineText?.toLowerCase()).toContain('high-performance');
    expect(taglineText?.toLowerCase()).toContain('rust');
    expect(taglineText?.toLowerCase()).toContain('key-value');
  });

  test('Test Case 3: Click Get Started button navigates to quick start documentation', async ({ page }) => {
    const ctaButton = page.getByTestId('cta-button');
    await expect(ctaButton).toBeVisible();
    await expect(ctaButton).toHaveText('Get Started');

    // Get the href attribute
    const href = await ctaButton.getAttribute('href');
    expect(href).toBe('/docs/quickstart');

    // Verify the button is clickable (link element)
    const tagName = await ctaButton.evaluate((el) => el.tagName.toLowerCase());
    expect(tagName).toBe('a');
  });

  test('Test Case 4: Button displays hover state with visual feedback', async ({ page }) => {
    const ctaButton = page.getByTestId('cta-button');
    await expect(ctaButton).toBeVisible();

    // Get initial styles
    const initialBgColor = await ctaButton.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });

    // Hover over the button
    await ctaButton.hover();

    // Wait for transition
    await page.waitForTimeout(300);

    // Get hover styles
    const hoverBgColor = await ctaButton.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });

    // Verify visual change occurred (background color changed or transform applied)
    const hasVisualChange = initialBgColor !== hoverBgColor;
    expect(hasVisualChange).toBe(true);
  });

  test('Test Case 5: Button displays visible focus indicator when focused via keyboard', async ({ page }) => {
    const ctaButton = page.getByTestId('cta-button');

    // Tab to the button to focus it
    await page.keyboard.press('Tab');

    // Ensure the button is focused
    const isFocused = await ctaButton.evaluate((el) => {
      return document.activeElement === el;
    });

    // If not focused, tab again (might have skip link)
    if (!isFocused) {
      await page.keyboard.press('Tab');
    }

    // Check for visible focus indicator
    const outlineStyle = await ctaButton.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        outline: style.outline,
        outlineWidth: style.outlineWidth,
        outlineColor: style.outlineColor,
        boxShadow: style.boxShadow,
      };
    });

    // Verify there's a visible focus indicator (outline or box-shadow)
    const hasOutline = outlineStyle.outlineWidth !== '0px' && outlineStyle.outline !== 'none';
    const hasBoxShadow = outlineStyle.boxShadow !== 'none';
    expect(hasOutline || hasBoxShadow).toBe(true);
  });
});
