/**
 * E2E tests for homepage - Hero Section Display
 * Scenario 1 - Test Case 4: Navigate to homepage as unauthenticated user
 *
 * Tests that the hero section loads within 2 seconds with all elements visible
 */

import { test, expect, measurePageLoadTime, waitForHeroSection } from './fixtures';

test.describe('Hero Section Display - E2E', () => {
  test.describe('Test Case 4: Navigate to homepage as unauthenticated user', () => {
    test('should load hero section within 2 seconds with all elements visible', async ({ page }) => {
      const startTime = Date.now();

      // Navigate to homepage
      await page.goto('/');

      // Wait for hero section to be visible
      await waitForHeroSection(page);

      // Measure total load time
      const loadTime = Date.now() - startTime;

      // Verify load time is under 2 seconds (2000ms)
      expect(loadTime).toBeLessThan(2000);

      // Verify all hero section elements are visible
      // Headline
      const headline = page.locator('h1');
      await expect(headline).toBeVisible();
      await expect(headline).toContainText(/shorten.*url/i);

      // Primary CTA button
      const ctaButton = page.getByTestId('hero-cta');
      await expect(ctaButton).toBeVisible();
      await expect(ctaButton).toContainText(/get started/i);

      // Login link
      const loginLink = page.getByTestId('hero-login-link');
      await expect(loginLink).toBeVisible();
      await expect(loginLink).toContainText(/login/i);
    });

    test('should display headline with URL shortening messaging', async ({ page }) => {
      await page.goto('/');
      await waitForHeroSection(page);

      const headline = page.locator('h1');
      await expect(headline).toBeVisible();
      await expect(headline).toContainText('Shorten URLs. Track Every Click.');
    });

    test('should display subheadline with value proposition', async ({ page }) => {
      await page.goto('/');
      await waitForHeroSection(page);

      // Check for subheadline text
      const subheadline = page.locator('text=Transform long, unwieldy URLs');
      await expect(subheadline).toBeVisible();
    });

    test('should have clickable Get Started CTA that navigates to registration', async ({
      page,
    }) => {
      await page.goto('/');
      await waitForHeroSection(page);

      const ctaButton = page.getByTestId('hero-cta');
      await expect(ctaButton).toBeVisible();

      // Click the CTA button
      await ctaButton.click();

      // Verify navigation to registration page
      await expect(page).toHaveURL(/\/register/);
    });

    test('should have clickable Login link that navigates to login page', async ({ page }) => {
      await page.goto('/');
      await waitForHeroSection(page);

      const loginLink = page.getByTestId('hero-login-link');
      await expect(loginLink).toBeVisible();

      // Click the login link
      await loginLink.click();

      // Verify navigation to login page
      await expect(page).toHaveURL(/\/login/);
    });

    test('should display hero section with proper visual hierarchy', async ({ page }) => {
      await page.goto('/');
      await waitForHeroSection(page);

      // Check hero section exists
      const heroSection = page.locator('.hero');
      await expect(heroSection).toBeVisible();

      // Check h1 is present
      const h1 = page.locator('h1');
      await expect(h1).toBeVisible();

      // Check CTA buttons container
      const ctaContainer = page.locator('.flex.flex-col.sm\\:flex-row');
      await expect(ctaContainer).toBeVisible();
    });
  });
});

/**
 * E2E tests for Theme Toggle Functionality
 * Scenario 12 - Test Case 4: E2E test - Toggle theme and verify all sections update
 *
 * Tests that theme toggle works correctly and all homepage sections reflect theme changes
 */
test.describe('Theme Toggle Functionality - E2E', () => {
  test.describe('Test Case 4: Toggle theme and verify all sections update', () => {
    test('should toggle theme from light to dark and verify all sections update', async ({
      page,
    }) => {
      // Clear localStorage to start fresh
      await page.goto('/');
      await page.evaluate(() => {
        localStorage.setItem('theme', 'light');
      });

      // Reload to apply the theme
      await page.reload();
      await waitForHeroSection(page);

      // Verify initial light theme
      const initialTheme = await page.evaluate(() =>
        document.documentElement.getAttribute('data-theme')
      );
      expect(initialTheme).toBe('light');

      // Verify all sections are visible before toggle
      await expect(page.locator('.hero')).toBeVisible();
      await expect(page.getByTestId('features-section')).toBeVisible();
      await expect(page.getByTestId('how-it-works-section')).toBeVisible();
      await expect(page.getByTestId('footer')).toBeVisible();

      // Find and click the theme toggle button
      const themeToggle = page.getByRole('button', { name: /switch to dark mode/i });
      await expect(themeToggle).toBeVisible();
      await themeToggle.click();

      // Verify theme changed to dark
      const newTheme = await page.evaluate(() =>
        document.documentElement.getAttribute('data-theme')
      );
      expect(newTheme).toBe('dark');

      // Verify all sections are still visible after theme change
      await expect(page.locator('.hero')).toBeVisible();
      await expect(page.getByTestId('features-section')).toBeVisible();
      await expect(page.getByTestId('how-it-works-section')).toBeVisible();
      await expect(page.getByTestId('footer')).toBeVisible();

      // Verify localStorage was updated
      const storedTheme = await page.evaluate(() => localStorage.getItem('theme'));
      expect(storedTheme).toBe('dark');
    });

    test('should toggle theme from dark to light and verify all sections update', async ({
      page,
    }) => {
      // Set initial theme to dark
      await page.goto('/');
      await page.evaluate(() => {
        localStorage.setItem('theme', 'dark');
      });

      // Reload to apply the theme
      await page.reload();
      await waitForHeroSection(page);

      // Verify initial dark theme
      const initialTheme = await page.evaluate(() =>
        document.documentElement.getAttribute('data-theme')
      );
      expect(initialTheme).toBe('dark');

      // Find and click the theme toggle button
      const themeToggle = page.getByRole('button', { name: /switch to light mode/i });
      await expect(themeToggle).toBeVisible();
      await themeToggle.click();

      // Verify theme changed to light
      const newTheme = await page.evaluate(() =>
        document.documentElement.getAttribute('data-theme')
      );
      expect(newTheme).toBe('light');

      // Verify all sections are still visible after theme change
      await expect(page.locator('.hero')).toBeVisible();
      await expect(page.getByTestId('features-section')).toBeVisible();
      await expect(page.getByTestId('how-it-works-section')).toBeVisible();
      await expect(page.getByTestId('footer')).toBeVisible();
    });

    test('should show correct icon when toggling themes', async ({ page }) => {
      // Start with light theme
      await page.goto('/');
      await page.evaluate(() => {
        localStorage.setItem('theme', 'light');
      });
      await page.reload();
      await waitForHeroSection(page);

      // In light mode, should show moon icon (to switch to dark)
      let themeToggle = page.getByRole('button', { name: /switch to dark mode/i });
      await expect(themeToggle).toBeVisible();
      let svg = themeToggle.locator('svg');
      await expect(svg).toBeVisible();

      // Click to switch to dark mode
      await themeToggle.click();

      // In dark mode, should show sun icon (to switch to light)
      themeToggle = page.getByRole('button', { name: /switch to light mode/i });
      await expect(themeToggle).toBeVisible();
      svg = themeToggle.locator('svg');
      await expect(svg).toBeVisible();
    });

    test('should persist theme preference after page reload', async ({ page }) => {
      // Start with light theme
      await page.goto('/');
      await page.evaluate(() => {
        localStorage.setItem('theme', 'light');
      });
      await page.reload();
      await waitForHeroSection(page);

      // Toggle to dark
      const themeToggle = page.getByRole('button', { name: /switch to dark mode/i });
      await themeToggle.click();

      // Verify dark theme
      let currentTheme = await page.evaluate(() =>
        document.documentElement.getAttribute('data-theme')
      );
      expect(currentTheme).toBe('dark');

      // Reload page
      await page.reload();
      await waitForHeroSection(page);

      // Verify theme persisted
      currentTheme = await page.evaluate(() =>
        document.documentElement.getAttribute('data-theme')
      );
      expect(currentTheme).toBe('dark');
    });

    test('should update Hero section styling on theme change', async ({ page }) => {
      await page.goto('/');
      await page.evaluate(() => {
        localStorage.setItem('theme', 'light');
      });
      await page.reload();
      await waitForHeroSection(page);

      // Verify hero headline is visible in light mode
      const headline = page.locator('h1');
      await expect(headline).toBeVisible();
      await expect(headline).toContainText('Shorten URLs. Track Every Click.');

      // Toggle to dark mode
      const themeToggle = page.getByRole('button', { name: /switch to dark mode/i });
      await themeToggle.click();

      // Verify hero headline is still visible in dark mode
      await expect(headline).toBeVisible();
      await expect(headline).toContainText('Shorten URLs. Track Every Click.');
    });

    test('should update Features section styling on theme change', async ({ page }) => {
      await page.goto('/');
      await page.evaluate(() => {
        localStorage.setItem('theme', 'light');
      });
      await page.reload();
      await waitForHeroSection(page);

      // Scroll to features section
      const featuresSection = page.getByTestId('features-section');
      await featuresSection.scrollIntoViewIfNeeded();
      await expect(featuresSection).toBeVisible();

      // Toggle theme
      const themeToggle = page.getByRole('button', { name: /switch to dark mode/i });
      await themeToggle.click();

      // Verify features section is still visible after theme change
      await expect(featuresSection).toBeVisible();

      // Verify feature cards are still present
      await expect(page.getByTestId('feature-card-url-shortening')).toBeVisible();
      await expect(page.getByTestId('feature-card-click-analytics')).toBeVisible();
    });

    test('should update How It Works section styling on theme change', async ({ page }) => {
      await page.goto('/');
      await page.evaluate(() => {
        localStorage.setItem('theme', 'light');
      });
      await page.reload();
      await waitForHeroSection(page);

      // Scroll to how it works section
      const howItWorksSection = page.getByTestId('how-it-works-section');
      await howItWorksSection.scrollIntoViewIfNeeded();
      await expect(howItWorksSection).toBeVisible();

      // Toggle theme
      const themeToggle = page.getByRole('button', { name: /switch to dark mode/i });
      await themeToggle.click();

      // Verify section is still visible after theme change
      await expect(howItWorksSection).toBeVisible();

      // Verify steps are still present
      await expect(page.getByTestId('step-1')).toBeVisible();
      await expect(page.getByTestId('step-2')).toBeVisible();
      await expect(page.getByTestId('step-3')).toBeVisible();
    });

    test('should update Footer section styling on theme change', async ({ page }) => {
      await page.goto('/');
      await page.evaluate(() => {
        localStorage.setItem('theme', 'light');
      });
      await page.reload();
      await waitForHeroSection(page);

      // Scroll to footer
      const footer = page.getByTestId('footer');
      await footer.scrollIntoViewIfNeeded();
      await expect(footer).toBeVisible();

      // Toggle theme
      const themeToggle = page.getByRole('button', { name: /switch to dark mode/i });
      await themeToggle.click();

      // Verify footer is still visible after theme change
      await expect(footer).toBeVisible();

      // Verify copyright is still present
      await expect(page.getByTestId('footer-copyright')).toBeVisible();
    });
  });
});
