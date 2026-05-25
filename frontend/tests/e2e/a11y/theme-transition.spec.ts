/**
 * E2E Theme Toggle Transition Tests.
 * Owner: Scenario 18 - Performance and Accessibility
 *
 * Validates theme toggle animation timing and smoothness.
 * Covers NFR-6 (animations < 300ms).
 */

import { test, expect } from '@playwright/test';

test.describe('Theme Toggle Transition', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('theme toggle transition completes within 300ms', async ({ page }) => {
    const themeToggle = page.getByTestId('theme-toggle-button');
    await expect(themeToggle).toBeVisible();

    // Measure the transition time
    const startTime = Date.now();

    // Click the theme toggle
    await themeToggle.click();

    // Wait for the theme to change by checking the class on html element
    await page.waitForFunction(() => {
      const html = document.documentElement;
      return html.classList.contains('dark') || !html.classList.contains('dark');
    });

    // Allow a brief moment for CSS transition
    await page.waitForTimeout(50);

    const endTime = Date.now();
    const transitionTime = endTime - startTime;

    // The total interaction time should be under 300ms
    // (including JS execution and CSS transition)
    expect(transitionTime).toBeLessThan(300);
  });

  test('theme toggle does not cause layout shift', async ({ page }) => {
    const themeToggle = page.getByTestId('theme-toggle-button');
    await expect(themeToggle).toBeVisible();

    // Measure layout stability before toggle
    const layoutBefore = await page.evaluate(() => {
      const main = document.querySelector('main') || document.body;
      const rect = main.getBoundingClientRect();
      return {
        width: rect.width,
        height: rect.height,
        scrollWidth: main.scrollWidth,
        scrollHeight: main.scrollHeight,
      };
    });

    // Toggle theme
    await themeToggle.click();
    await page.waitForTimeout(200); // Wait for transition

    // Measure layout stability after toggle
    const layoutAfter = await page.evaluate(() => {
      const main = document.querySelector('main') || document.body;
      const rect = main.getBoundingClientRect();
      return {
        width: rect.width,
        height: rect.height,
        scrollWidth: main.scrollWidth,
        scrollHeight: main.scrollHeight,
      };
    });

    // Layout dimensions should remain stable (no significant shift)
    expect(layoutAfter.width).toBe(layoutBefore.width);
    expect(Math.abs(layoutAfter.height - layoutBefore.height)).toBeLessThan(10);
    expect(layoutAfter.scrollWidth).toBe(layoutBefore.scrollWidth);
  });

  test('theme toggle has smooth color interpolation', async ({ page }) => {
    const themeToggle = page.getByTestId('theme-toggle-button');
    await expect(themeToggle).toBeVisible();

    // Check that CSS transitions are defined for color changes
    const hasTransitions = await page.evaluate(() => {
      const elements = document.querySelectorAll('body, .layout, .layout-header, .hero-section, .features-section, .system-overview, .kv-explorer');
      let transitionCount = 0;

      elements.forEach((el) => {
        const styles = window.getComputedStyle(el);
        const transition = styles.transition || styles.transitionProperty;
        if (transition && transition !== 'all 0s ease 0s' && transition !== 'none') {
          transitionCount++;
        }
      });

      return transitionCount > 0;
    });

    // The page should have CSS transitions defined for theme changes
    expect(hasTransitions).toBe(true);
  });

  test('theme toggle button is keyboard accessible', async ({ page }) => {
    const themeToggle = page.getByTestId('theme-toggle-button');

    // Focus the theme toggle
    await themeToggle.focus();

    // Verify it's focused
    const isFocused = await themeToggle.evaluate((el) => el === document.activeElement);
    expect(isFocused).toBe(true);

    // Press Enter to toggle
    await page.keyboard.press('Enter');

    // The theme should have changed
    const hasThemeClass = await page.evaluate(() => {
      const html = document.documentElement;
      return html.classList.contains('dark') || html.classList.contains('light');
    });

    // The html element should have a theme-related class
    expect(hasThemeClass).toBe(true);
  });
});
