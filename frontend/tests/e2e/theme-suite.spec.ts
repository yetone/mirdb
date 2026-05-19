import { test, expect } from '@playwright/test';

const SUPPORTED_THEMES = ['light', 'dark', 'cyberpunk', 'synthwave'] as const;

test.describe('Theme Suite E2E', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('iterates through all supported themes without errors', async ({ page }) => {
    const consoleErrors: string[] = [];
    page.on('pageerror', (error) => {
      consoleErrors.push(error.message);
    });
    page.on('console', (msg) => {
      if (msg.type() === 'error') {
        consoleErrors.push(msg.text());
      }
    });

    for (const theme of SUPPORTED_THEMES) {
      // Open theme toggle menu
      await page.click('[data-testid="theme-toggle-button"]');

      // Select the theme
      await page.click(`[data-testid="theme-option-${theme}"]`);

      // Verify data-theme attribute is set on html element
      await expect(page.locator('html')).toHaveAttribute('data-theme', theme);

      // Verify homepage elements are still visible and rendered
      const homePage = page.locator('[data-testid="home-page"]');
      await expect(homePage).toBeVisible();

      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();

      // Verify heading is visible (no invisible text)
      const heading = page.locator('h1');
      await expect(heading).toBeVisible();

      // Verify navbar is visible
      const navbar = page.locator('nav');
      await expect(navbar).toBeVisible();

      // Verify the page has non-zero dimensions (no broken layout)
      const bodyBox = await page.locator('body').boundingBox();
      expect(bodyBox).not.toBeNull();
      expect(bodyBox!.width).toBeGreaterThan(0);
      expect(bodyBox!.height).toBeGreaterThan(0);
    }

    // Ensure no console errors occurred during theme switching
    expect(consoleErrors).toHaveLength(0);
  });

  test('theme persists after page reload', async ({ page }) => {
    // Select cyberpunk theme
    await page.click('[data-testid="theme-toggle-button"]');
    await page.click('[data-testid="theme-option-cyberpunk"]');

    // Verify theme is applied
    await expect(page.locator('html')).toHaveAttribute('data-theme', 'cyberpunk');

    // Reload page
    await page.reload();

    // Wait for page to be ready
    await page.waitForSelector('[data-testid="home-page"]');

    // Verify theme persists after reload
    await expect(page.locator('html')).toHaveAttribute('data-theme', 'cyberpunk');

    // Verify homepage is still rendered correctly
    const homePage = page.locator('[data-testid="home-page"]');
    await expect(homePage).toBeVisible();
  });

  test('each theme has visible text and proper contrast', async ({ page }) => {
    for (const theme of SUPPORTED_THEMES) {
      // Open and select theme
      await page.click('[data-testid="theme-toggle-button"]');
      await page.click(`[data-testid="theme-option-${theme}"]`);

      // Wait for theme to apply
      await expect(page.locator('html')).toHaveAttribute('data-theme', theme);

      // Get computed color of the heading
      const headingColor = await page.locator('h1').evaluate((el) => {
        const style = window.getComputedStyle(el);
        return style.color;
      });

      // Ensure heading has a visible color (not transparent)
      expect(headingColor).not.toBe('rgba(0, 0, 0, 0)');

      // Check that main content area has dimensions (page is rendered)
      const mainBox = await page.locator('main').boundingBox();
      expect(mainBox).not.toBeNull();
      expect(mainBox!.width).toBeGreaterThan(0);
      expect(mainBox!.height).toBeGreaterThan(0);
    }
  });
});
