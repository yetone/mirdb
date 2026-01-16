import { test, expect } from '@playwright/test';

test.describe('BackgroundEffect Component Integration - E2E Tests', () => {
  test.describe('Test Case 1: Render hero section with BackgroundEffect', () => {
    test('BackgroundEffect component renders in hero section', async ({ page }) => {
      await page.goto('/');

      // Verify hero section is rendered
      const heroSection = page.getByTestId('hero-section');
      await expect(heroSection).toBeVisible();

      // Verify BackgroundEffect component is present within the page
      const backgroundEffect = page.getByTestId('hero-background');
      await expect(backgroundEffect).toBeVisible();

      // Verify BackgroundEffect has the correct structure (fixed positioning)
      await expect(backgroundEffect).toHaveClass(/fixed/);
      await expect(backgroundEffect).toHaveClass(/inset-0/);

      // Verify it's positioned behind content (negative z-index)
      await expect(backgroundEffect).toHaveClass(/-z-10/);

      // Verify it's not interactive
      await expect(backgroundEffect).toHaveClass(/pointer-events-none/);

      // Verify aria-hidden for accessibility
      await expect(backgroundEffect).toHaveAttribute('aria-hidden', 'true');
    });

    test('BackgroundEffect contains gradient orb elements', async ({ page }) => {
      await page.goto('/');

      const backgroundEffect = page.getByTestId('hero-background');
      await expect(backgroundEffect).toBeVisible();

      // Count gradient orbs (elements with blur-2xl and rounded-full classes)
      const gradientOrbs = backgroundEffect.locator('.blur-2xl.rounded-full');
      await expect(gradientOrbs).toHaveCount(3);

      // Verify orbs have gradient backgrounds
      const orbsWithGradient = backgroundEffect.locator('.bg-gradient-to-br');
      await expect(orbsWithGradient).toHaveCount(3);
    });
  });

  test.describe('Test Case 2: Check background effect in light theme', () => {
    test('background effect is visible and appropriate for light theme', async ({ page }) => {
      await page.goto('/');

      // Clear any stored theme preference
      await page.evaluate(() => localStorage.clear());
      await page.reload();

      // Set light theme explicitly (use hero-theme-toggle to avoid multiple matches)
      const toggleButton = page.getByTestId('hero-theme-toggle').getByTestId('theme-toggle-button');
      await toggleButton.click();
      await page.getByTestId('theme-option-light').first().click();

      // Verify light theme is applied
      await expect(page.locator('html')).toHaveAttribute('data-theme', 'light');

      // Verify BackgroundEffect is visible
      const backgroundEffect = page.getByTestId('hero-background');
      await expect(backgroundEffect).toBeVisible();

      // Verify light theme gradient colors are applied (blue-400/30, purple-400/30 for primary)
      const gradientOrbs = backgroundEffect.locator('.blur-2xl.rounded-full');
      const firstOrb = gradientOrbs.first();
      await expect(firstOrb).toBeVisible();

      // Check the orb has light theme colors (from-blue-400/30 to-purple-400/30)
      await expect(firstOrb).toHaveClass(/from-blue-400\/30/);
      await expect(firstOrb).toHaveClass(/to-purple-400\/30/);
    });

    test('all gradient orbs are visible in light theme', async ({ page }) => {
      await page.goto('/');

      // Set light theme (use hero-theme-toggle to avoid multiple matches)
      const toggleButton = page.getByTestId('hero-theme-toggle').getByTestId('theme-toggle-button');
      await toggleButton.click();
      await page.getByTestId('theme-option-light').first().click();

      await expect(page.locator('html')).toHaveAttribute('data-theme', 'light');

      const backgroundEffect = page.getByTestId('hero-background');
      const gradientOrbs = backgroundEffect.locator('.blur-2xl.rounded-full');

      // All 3 orbs should be visible
      for (let i = 0; i < 3; i++) {
        await expect(gradientOrbs.nth(i)).toBeVisible();
      }
    });
  });

  test.describe('Test Case 3: Check background effect in dark theme', () => {
    test('background effect is visible and appropriate for dark theme', async ({ page }) => {
      await page.goto('/');

      // Clear any stored theme preference
      await page.evaluate(() => localStorage.clear());
      await page.reload();

      // Set dark theme explicitly (use hero-theme-toggle to avoid multiple matches)
      const toggleButton = page.getByTestId('hero-theme-toggle').getByTestId('theme-toggle-button');
      await toggleButton.click();
      await page.getByTestId('theme-option-dark').first().click();

      // Verify dark theme is applied
      await expect(page.locator('html')).toHaveAttribute('data-theme', 'dark');

      // Verify BackgroundEffect is visible
      const backgroundEffect = page.getByTestId('hero-background');
      await expect(backgroundEffect).toBeVisible();

      // Verify dark theme gradient colors are applied (blue-600/40, purple-600/40 for primary)
      const gradientOrbs = backgroundEffect.locator('.blur-2xl.rounded-full');
      const firstOrb = gradientOrbs.first();
      await expect(firstOrb).toBeVisible();

      // Check the orb has dark theme colors (from-blue-600/40 to-purple-600/40)
      await expect(firstOrb).toHaveClass(/from-blue-600\/40/);
      await expect(firstOrb).toHaveClass(/to-purple-600\/40/);
    });

    test('all gradient orbs are visible in dark theme', async ({ page }) => {
      await page.goto('/');

      // Set dark theme (use hero-theme-toggle to avoid multiple matches)
      const toggleButton = page.getByTestId('hero-theme-toggle').getByTestId('theme-toggle-button');
      await toggleButton.click();
      await page.getByTestId('theme-option-dark').first().click();

      await expect(page.locator('html')).toHaveAttribute('data-theme', 'dark');

      const backgroundEffect = page.getByTestId('hero-background');
      const gradientOrbs = backgroundEffect.locator('.blur-2xl.rounded-full');

      // All 3 orbs should be visible
      for (let i = 0; i < 3; i++) {
        await expect(gradientOrbs.nth(i)).toBeVisible();
      }
    });
  });

  test.describe('Test Case 4: Verify background effect performance', () => {
    test('animation runs smoothly without impacting page performance', async ({ page }) => {
      await page.goto('/');

      // Verify hero section loads quickly
      const heroSection = page.getByTestId('hero-section');
      await expect(heroSection).toBeVisible({ timeout: 2000 });

      // Verify BackgroundEffect is rendered
      const backgroundEffect = page.getByTestId('hero-background');
      await expect(backgroundEffect).toBeVisible();

      // Measure initial render performance using Performance API
      const metrics = await page.evaluate(() => {
        const entries = performance.getEntriesByType('navigation') as PerformanceNavigationTiming[];
        if (entries.length > 0) {
          const navEntry = entries[0];
          return {
            domContentLoaded: navEntry.domContentLoadedEventEnd - navEntry.domContentLoadedEventStart,
            loadComplete: navEntry.loadEventEnd - navEntry.loadEventStart,
            firstPaint: performance.getEntriesByName('first-paint')[0]?.startTime || 0,
            firstContentfulPaint: performance.getEntriesByName('first-contentful-paint')[0]?.startTime || 0,
          };
        }
        return null;
      });

      // Verify page loaded within acceptable performance threshold
      if (metrics) {
        // First Contentful Paint should be under 3 seconds
        expect(metrics.firstContentfulPaint).toBeLessThan(3000);
      }

      // Verify no layout shifts caused by BackgroundEffect (it uses fixed positioning)
      const layoutShiftEntries = await page.evaluate(() => {
        return new Promise((resolve) => {
          const observer = new PerformanceObserver((list) => {
            const entries = list.getEntries();
            observer.disconnect();
            resolve(entries.map(e => (e as PerformanceEntry & { value: number }).value || 0));
          });
          observer.observe({ type: 'layout-shift', buffered: true });
          // Give it a moment to collect entries
          setTimeout(() => {
            observer.disconnect();
            resolve([]);
          }, 500);
        });
      });

      // Cumulative Layout Shift should be minimal (under 0.25 is good, under 0.1 is excellent)
      const totalCLS = (layoutShiftEntries as number[]).reduce((sum, val) => sum + val, 0);
      expect(totalCLS).toBeLessThan(0.25);
    });

    test('background effect does not block user interaction', async ({ page }) => {
      await page.goto('/');

      // Verify BackgroundEffect has pointer-events-none (doesn't block clicks)
      const backgroundEffect = page.getByTestId('hero-background');
      await expect(backgroundEffect).toHaveClass(/pointer-events-none/);

      // Verify CTA buttons are still clickable (not blocked by background)
      const getStartedButton = page.getByTestId('cta-get-started');
      await expect(getStartedButton).toBeVisible();
      await expect(getStartedButton).toBeEnabled();

      // Click should work - verify navigation occurs
      await getStartedButton.click();
      await expect(page).toHaveURL(/\/register/);
    });

    test('background effect renders efficiently with CSS-based animations', async ({ page }) => {
      await page.goto('/');

      const backgroundEffect = page.getByTestId('hero-background');
      await expect(backgroundEffect).toBeVisible();

      // Verify the background uses CSS-based blur (not JavaScript animation)
      const gradientOrbs = backgroundEffect.locator('.blur-2xl');
      await expect(gradientOrbs).toHaveCount(3);

      // Verify orbs use opacity for visual effect (GPU-accelerated)
      const orbsWithOpacity = backgroundEffect.locator('.opacity-70');
      await expect(orbsWithOpacity).toHaveCount(3);
    });
  });

  test.describe('Theme Compatibility - Background adapts to theme changes', () => {
    test('background effect updates when switching from light to dark', async ({ page }) => {
      await page.goto('/');

      // Clear localStorage and reload
      await page.evaluate(() => localStorage.clear());
      await page.reload();

      // Start with light theme (use hero-theme-toggle to avoid multiple matches)
      const toggleButton = page.getByTestId('hero-theme-toggle').getByTestId('theme-toggle-button');
      await toggleButton.click();
      await page.getByTestId('theme-option-light').first().click();

      await expect(page.locator('html')).toHaveAttribute('data-theme', 'light');

      // Verify light theme colors
      const backgroundEffect = page.getByTestId('hero-background');
      const firstOrb = backgroundEffect.locator('.blur-2xl.rounded-full').first();
      await expect(firstOrb).toHaveClass(/from-blue-400\/30/);

      // Switch to dark theme
      await toggleButton.click();
      await page.getByTestId('theme-option-dark').first().click();

      await expect(page.locator('html')).toHaveAttribute('data-theme', 'dark');

      // Verify dark theme colors are now applied
      await expect(firstOrb).toHaveClass(/from-blue-600\/40/);
    });

    test('background effect supports cyberpunk theme', async ({ page }) => {
      await page.goto('/');

      // Use hero-theme-toggle to avoid multiple matches
      const toggleButton = page.getByTestId('hero-theme-toggle').getByTestId('theme-toggle-button');
      await toggleButton.click();
      await page.getByTestId('theme-option-cyberpunk').first().click();

      await expect(page.locator('html')).toHaveAttribute('data-theme', 'cyberpunk');

      const backgroundEffect = page.getByTestId('hero-background');
      await expect(backgroundEffect).toBeVisible();

      // Verify cyberpunk theme colors (yellow-400/40, pink-500/40 for primary)
      const firstOrb = backgroundEffect.locator('.blur-2xl.rounded-full').first();
      await expect(firstOrb).toHaveClass(/from-yellow-400\/40/);
      await expect(firstOrb).toHaveClass(/to-pink-500\/40/);
    });

    test('background effect supports synthwave theme', async ({ page }) => {
      await page.goto('/');

      // Use hero-theme-toggle to avoid multiple matches
      const toggleButton = page.getByTestId('hero-theme-toggle').getByTestId('theme-toggle-button');
      await toggleButton.click();
      await page.getByTestId('theme-option-synthwave').first().click();

      await expect(page.locator('html')).toHaveAttribute('data-theme', 'synthwave');

      const backgroundEffect = page.getByTestId('hero-background');
      await expect(backgroundEffect).toBeVisible();

      // Verify synthwave theme colors (purple-500/40, pink-500/40 for primary)
      const firstOrb = backgroundEffect.locator('.blur-2xl.rounded-full').first();
      await expect(firstOrb).toHaveClass(/from-purple-500\/40/);
      await expect(firstOrb).toHaveClass(/to-pink-500\/40/);
    });
  });
});
