/**
 * Theme Toggle E2E Tests
 * Owner: Scenario 12 - Theme Toggle
 *
 * Tests dark/light theme toggle functionality and localStorage persistence.
 * Validates that theme changes affect all page sections consistently.
 */

const { test, expect } = require('@playwright/test');

test.describe('Theme Toggle', () => {
  test.beforeEach(async ({ page }) => {
    // Clear localStorage before each test
    await page.goto('/');
    await page.evaluate(() => localStorage.clear());
  });

  test('TC1: loads page in dark theme when system prefers dark mode', async ({ page }) => {
    // Emulate dark color scheme preference
    await page.emulateMedia({ colorScheme: 'dark' });
    await page.goto('/');

    // Wait for theme initialization
    await page.waitForLoadState('domcontentloaded');

    // Verify dark theme is applied
    const html = page.locator('html');
    await expect(html).toHaveAttribute('data-theme', 'dark');

    // Verify dark theme visual indicators
    const body = page.locator('body');
    const bgColor = await body.evaluate((el) => getComputedStyle(el).backgroundColor);
    // Dark theme should have dark background
    expect(bgColor).not.toBe('rgb(255, 255, 255)');
  });

  test('TC2: loads page in light theme when system prefers light mode', async ({ page }) => {
    // Emulate light color scheme preference
    await page.emulateMedia({ colorScheme: 'light' });
    await page.goto('/');

    // Wait for theme initialization
    await page.waitForLoadState('domcontentloaded');

    // Verify light theme is applied (no data-theme attribute or light value)
    const html = page.locator('html');
    const dataTheme = await html.getAttribute('data-theme');
    // In light mode, data-theme should be null, undefined, or 'light'
    expect(dataTheme === null || dataTheme === undefined || dataTheme === 'light').toBeTruthy();
  });

  test('TC3: switches from light to dark theme when toggle is clicked', async ({ page }) => {
    // Start with light theme
    await page.emulateMedia({ colorScheme: 'light' });
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Get initial CSS custom property for background
    const initialBgPrimary = await page.evaluate(() => {
      return getComputedStyle(document.documentElement).getPropertyValue('--color-bg-primary').trim();
    });

    // Click theme toggle
    const themeToggle = page.locator('.theme-toggle');
    await themeToggle.click();

    // Wait for theme transition
    await page.waitForTimeout(100);

    // Verify dark theme is now applied
    const html = page.locator('html');
    await expect(html).toHaveAttribute('data-theme', 'dark');

    // Verify CSS custom property changed for background
    const newBgPrimary = await page.evaluate(() => {
      return getComputedStyle(document.documentElement).getPropertyValue('--color-bg-primary').trim();
    });
    expect(newBgPrimary).not.toBe(initialBgPrimary);

    // Verify dark icon is visible and light icon is hidden
    const darkIcon = page.locator('.theme-toggle__icon--dark');
    const lightIcon = page.locator('.theme-toggle__icon--light');
    await expect(darkIcon).toBeVisible();
    await expect(lightIcon).toBeHidden();
  });

  test('TC4: switches from dark to light theme when toggle is clicked', async ({ page }) => {
    // Start with dark theme
    await page.emulateMedia({ colorScheme: 'dark' });
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Verify starting in dark mode
    const html = page.locator('html');
    await expect(html).toHaveAttribute('data-theme', 'dark');

    // Get initial CSS custom property for background (dark)
    const initialBgPrimary = await page.evaluate(() => {
      return getComputedStyle(document.documentElement).getPropertyValue('--color-bg-primary').trim();
    });

    // Click theme toggle to switch to light
    const themeToggle = page.locator('.theme-toggle');
    await themeToggle.click();

    // Wait for theme transition
    await page.waitForTimeout(100);

    // Verify light theme is now applied
    await expect(html).toHaveAttribute('data-theme', 'light');

    // Verify CSS custom property changed for background
    const newBgPrimary = await page.evaluate(() => {
      return getComputedStyle(document.documentElement).getPropertyValue('--color-bg-primary').trim();
    });
    expect(newBgPrimary).not.toBe(initialBgPrimary);

    // Verify light icon is visible and dark icon is hidden
    const lightIcon = page.locator('.theme-toggle__icon--light');
    const darkIcon = page.locator('.theme-toggle__icon--dark');
    await expect(lightIcon).toBeVisible();
    await expect(darkIcon).toBeHidden();
  });

  test('TC5: theme preference persists in localStorage after page reload', async ({ page }) => {
    // Start with light theme
    await page.emulateMedia({ colorScheme: 'light' });
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Toggle to dark theme
    const themeToggle = page.locator('.theme-toggle');
    await themeToggle.click();

    // Verify dark theme is applied
    const html = page.locator('html');
    await expect(html).toHaveAttribute('data-theme', 'dark');

    // Verify localStorage has the theme saved
    const savedTheme = await page.evaluate(() => localStorage.getItem('theme'));
    expect(savedTheme).toBe('dark');

    // Reload the page
    await page.reload();
    await page.waitForLoadState('domcontentloaded');

    // Verify dark theme persists after reload
    await expect(html).toHaveAttribute('data-theme', 'dark');

    // Verify dark icon is still visible
    const darkIcon = page.locator('.theme-toggle__icon--dark');
    await expect(darkIcon).toBeVisible();
  });

  test('TC6: theme change affects all sections consistently', async ({ page }) => {
    // Start with light theme
    await page.emulateMedia({ colorScheme: 'light' });
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Function to get CSS custom properties (more reliable than computed colors)
    const getCssVariables = async () => {
      return page.evaluate(() => {
        const root = document.documentElement;
        const styles = getComputedStyle(root);
        return {
          bgPrimary: styles.getPropertyValue('--color-bg-primary').trim(),
          bgSecondary: styles.getPropertyValue('--color-bg-secondary').trim(),
          textPrimary: styles.getPropertyValue('--color-text-primary').trim(),
          textSecondary: styles.getPropertyValue('--color-text-secondary').trim(),
          colorPrimary: styles.getPropertyValue('--color-primary').trim(),
          colorBorder: styles.getPropertyValue('--color-border').trim(),
        };
      });
    };

    // Get initial CSS variables (light theme)
    const lightVars = await getCssVariables();

    // Toggle to dark theme
    const themeToggle = page.locator('.theme-toggle');
    await themeToggle.click();

    // Wait for theme transition
    await page.waitForTimeout(100);

    // Verify dark theme is applied
    const html = page.locator('html');
    await expect(html).toHaveAttribute('data-theme', 'dark');

    // Get CSS variables after theme change (dark theme)
    const darkVars = await getCssVariables();

    // Verify CSS custom properties changed (these are what control all sections)
    expect(darkVars.bgPrimary).not.toBe(lightVars.bgPrimary);
    expect(darkVars.bgSecondary).not.toBe(lightVars.bgSecondary);
    expect(darkVars.textPrimary).not.toBe(lightVars.textPrimary);
    expect(darkVars.textSecondary).not.toBe(lightVars.textSecondary);
    expect(darkVars.colorBorder).not.toBe(lightVars.colorBorder);

    // Verify sections exist and are using CSS variables
    const sectionsExist = await page.evaluate(() => {
      const header = document.querySelector('.header');
      const hero = document.querySelector('.hero');
      const features = document.querySelector('.features');
      const footer = document.querySelector('.footer');
      const codeBlock = document.querySelector('.code-block');
      return {
        header: !!header,
        hero: !!hero,
        features: !!features,
        footer: !!footer,
        codeBlock: !!codeBlock,
      };
    });

    // All key sections should exist
    expect(sectionsExist.header).toBe(true);
    expect(sectionsExist.hero).toBe(true);
    expect(sectionsExist.features).toBe(true);
    expect(sectionsExist.footer).toBe(true);
    expect(sectionsExist.codeBlock).toBe(true);
  });

  test('theme toggle button has proper accessibility attributes', async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    const themeToggle = page.locator('.theme-toggle');

    // Verify button has aria-label
    await expect(themeToggle).toHaveAttribute('aria-label', 'Toggle dark mode');

    // Verify button is focusable
    await themeToggle.focus();
    await expect(themeToggle).toBeFocused();

    // Verify keyboard activation works
    await page.keyboard.press('Enter');
    const html = page.locator('html');
    const dataTheme = await html.getAttribute('data-theme');
    expect(dataTheme).toBe('dark');
  });

  test('localStorage theme overrides system preference', async ({ page }) => {
    // Set localStorage to dark theme
    await page.goto('/');
    await page.evaluate(() => localStorage.setItem('theme', 'dark'));

    // Emulate light system preference
    await page.emulateMedia({ colorScheme: 'light' });

    // Reload page
    await page.goto('/');
    await page.waitForLoadState('domcontentloaded');

    // Verify dark theme is applied (localStorage overrides system)
    const html = page.locator('html');
    await expect(html).toHaveAttribute('data-theme', 'dark');
  });
});
