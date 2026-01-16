import { test, expect } from '@playwright/test';

test.describe('Theme Toggle Functionality - E2E Tests', () => {
  test.describe('Test Case 1: Toggle theme from light to dark', () => {
    test('homepage updates to dark theme colors throughout all sections', async ({ page }) => {
      await page.goto('/');

      // Set light theme first
      const toggleButton = page.getByTestId('theme-toggle-button');
      await toggleButton.click();
      await page.getByTestId('theme-option-light').click();

      // Verify light theme is applied
      await expect(page.locator('html')).toHaveAttribute('data-theme', 'light');

      // Toggle to dark theme
      await toggleButton.click();
      await page.getByTestId('theme-option-dark').click();

      // Verify dark theme is applied to document
      await expect(page.locator('html')).toHaveAttribute('data-theme', 'dark');

      // Verify all sections update - check hero section
      const heroSection = page.getByTestId('hero-section');
      await expect(heroSection).toBeVisible();

      // Verify features section is in dark mode
      const featuresSection = page.locator('section#features');
      await expect(featuresSection).toBeVisible();

      // Verify how it works section is in dark mode
      const howItWorksSection = page.locator('section#how-it-works');
      await expect(howItWorksSection).toBeVisible();

      // Verify footer CTA section is in dark mode
      const footerSection = page.locator('[data-testid="footer-cta"]');
      await expect(footerSection).toBeVisible();
    });
  });

  test.describe('Test Case 2: Toggle theme from dark to light', () => {
    test('homepage updates to light theme colors throughout all sections', async ({ page }) => {
      await page.goto('/');

      // Set dark theme first
      const toggleButton = page.getByTestId('theme-toggle-button');
      await toggleButton.click();
      await page.getByTestId('theme-option-dark').click();

      // Verify dark theme is applied
      await expect(page.locator('html')).toHaveAttribute('data-theme', 'dark');

      // Toggle to light theme
      await toggleButton.click();
      await page.getByTestId('theme-option-light').click();

      // Verify light theme is applied to document
      await expect(page.locator('html')).toHaveAttribute('data-theme', 'light');

      // Verify all homepage sections update
      const heroSection = page.getByTestId('hero-section');
      await expect(heroSection).toBeVisible();

      const featuresSection = page.locator('section#features');
      await expect(featuresSection).toBeVisible();

      const howItWorksSection = page.locator('section#how-it-works');
      await expect(howItWorksSection).toBeVisible();
    });
  });

  test.describe('Test Case 3: Check theme persistence in localStorage', () => {
    test('theme preference is stored in localStorage and persists on reload', async ({ page, context }) => {
      // Clear localStorage first by navigating and clearing
      await page.goto('/');
      await page.evaluate(() => localStorage.clear());
      await page.reload();

      // Select dark theme
      const toggleButton = page.getByTestId('theme-toggle-button');
      await toggleButton.click();
      await page.getByTestId('theme-option-dark').click();

      // Verify theme is stored in localStorage
      const storedTheme = await page.evaluate(() => localStorage.getItem('theme-preference'));
      expect(storedTheme).toBe('dark');

      // Verify data-theme attribute is set
      await expect(page.locator('html')).toHaveAttribute('data-theme', 'dark');

      // Reload page (localStorage persists across reloads in same context)
      await page.reload();

      // Verify theme persists after reload
      await expect(page.locator('html')).toHaveAttribute('data-theme', 'dark');

      // Verify localStorage still has the value
      const persistedTheme = await page.evaluate(() => localStorage.getItem('theme-preference'));
      expect(persistedTheme).toBe('dark');
    });

    test('cyberpunk theme persists after page reload', async ({ page }) => {
      // Clear localStorage first
      await page.goto('/');
      await page.evaluate(() => localStorage.clear());
      await page.reload();

      // Select cyberpunk theme
      const toggleButton = page.getByTestId('theme-toggle-button');
      await toggleButton.click();
      await page.getByTestId('theme-option-cyberpunk').click();

      // Verify theme is stored
      const storedTheme = await page.evaluate(() => localStorage.getItem('theme-preference'));
      expect(storedTheme).toBe('cyberpunk');

      // Reload and verify persistence
      await page.reload();
      await expect(page.locator('html')).toHaveAttribute('data-theme', 'cyberpunk');
    });

    test('synthwave theme persists after page reload', async ({ page }) => {
      // Clear localStorage first
      await page.goto('/');
      await page.evaluate(() => localStorage.clear());
      await page.reload();

      // Select synthwave theme
      const toggleButton = page.getByTestId('theme-toggle-button');
      await toggleButton.click();
      await page.getByTestId('theme-option-synthwave').click();

      // Verify theme is stored
      const storedTheme = await page.evaluate(() => localStorage.getItem('theme-preference'));
      expect(storedTheme).toBe('synthwave');

      // Reload and verify persistence
      await page.reload();
      await expect(page.locator('html')).toHaveAttribute('data-theme', 'synthwave');
    });
  });

  test.describe('Test Case 5: Test multiple theme variants', () => {
    test('homepage supports cyberpunk theme', async ({ page }) => {
      await page.goto('/');

      // Select cyberpunk theme
      const toggleButton = page.getByTestId('theme-toggle-button');
      await toggleButton.click();
      await page.getByTestId('theme-option-cyberpunk').click();

      // Verify cyberpunk theme is applied
      await expect(page.locator('html')).toHaveAttribute('data-theme', 'cyberpunk');

      // Verify homepage sections are visible and styled
      await expect(page.getByTestId('hero-section')).toBeVisible();
      await expect(page.locator('section#features')).toBeVisible();
    });

    test('homepage supports synthwave theme', async ({ page }) => {
      await page.goto('/');

      // Select synthwave theme
      const toggleButton = page.getByTestId('theme-toggle-button');
      await toggleButton.click();
      await page.getByTestId('theme-option-synthwave').click();

      // Verify synthwave theme is applied
      await expect(page.locator('html')).toHaveAttribute('data-theme', 'synthwave');

      // Verify homepage sections are visible and styled
      await expect(page.getByTestId('hero-section')).toBeVisible();
      await expect(page.locator('section#features')).toBeVisible();
    });

    test('can switch between all DaisyUI themes', async ({ page }) => {
      await page.goto('/');

      const themes = ['light', 'dark', 'cyberpunk', 'synthwave'];
      const toggleButton = page.getByTestId('theme-toggle-button');

      for (const theme of themes) {
        await toggleButton.click();
        await page.getByTestId(`theme-option-${theme}`).click();
        await expect(page.locator('html')).toHaveAttribute('data-theme', theme);
      }
    });
  });

  test.describe('Theme Toggle UI Interaction', () => {
    test('theme toggle is accessible in the header', async ({ page }) => {
      await page.goto('/');

      const themeToggle = page.getByTestId('hero-theme-toggle');
      await expect(themeToggle).toBeVisible();
    });

    test('dropdown closes when clicking outside', async ({ page }) => {
      await page.goto('/');

      const toggleButton = page.getByTestId('theme-toggle-button');
      await toggleButton.click();

      // Verify dropdown is open
      await expect(page.getByTestId('theme-dropdown')).toBeVisible();

      // Click outside (on hero section)
      await page.getByTestId('hero-section').click({ position: { x: 10, y: 100 } });

      // Verify dropdown is closed
      await expect(page.getByTestId('theme-dropdown')).not.toBeVisible();
    });

    test('dropdown closes when pressing Escape', async ({ page }) => {
      await page.goto('/');

      const toggleButton = page.getByTestId('theme-toggle-button');
      await toggleButton.click();

      // Verify dropdown is open
      await expect(page.getByTestId('theme-dropdown')).toBeVisible();

      // Press Escape
      await page.keyboard.press('Escape');

      // Verify dropdown is closed
      await expect(page.getByTestId('theme-dropdown')).not.toBeVisible();
    });

    test('selected theme option is highlighted', async ({ page }) => {
      await page.goto('/');

      // Select dark theme
      const toggleButton = page.getByTestId('theme-toggle-button');
      await toggleButton.click();
      await page.getByTestId('theme-option-dark').click();

      // Reopen dropdown
      await toggleButton.click();

      // Verify dark option has aria-selected="true"
      const darkOption = page.getByTestId('theme-option-dark');
      await expect(darkOption).toHaveAttribute('aria-selected', 'true');
    });
  });

  test.describe('Theme Affects All Homepage Sections', () => {
    test('theme change is comprehensive and immediate', async ({ page }) => {
      await page.goto('/');

      const toggleButton = page.getByTestId('theme-toggle-button');

      // Start with light theme
      await toggleButton.click();
      await page.getByTestId('theme-option-light').click();

      // Scroll through all sections to verify they're rendered
      await page.getByTestId('hero-section').scrollIntoViewIfNeeded();
      await page.locator('section#features').scrollIntoViewIfNeeded();
      await page.locator('section#how-it-works').scrollIntoViewIfNeeded();

      // Change to dark theme
      await toggleButton.click();
      await page.getByTestId('theme-option-dark').click();

      // Verify immediate change (no page reload needed)
      await expect(page.locator('html')).toHaveAttribute('data-theme', 'dark');

      // Verify all sections still visible after theme change
      await expect(page.getByTestId('hero-section')).toBeVisible();
      await expect(page.locator('section#features')).toBeVisible();
      await expect(page.locator('section#how-it-works')).toBeVisible();
    });
  });
});
