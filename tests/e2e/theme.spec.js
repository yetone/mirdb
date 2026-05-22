/**
 * Dark Mode Theme E2E Tests
 * Owner: Scenario 8 - Dark Mode Theme
 *
 * Tests:
 * - Default light mode on page load
 * - Theme toggle switches between light and dark
 * - Theme preference persists across page reloads
 * - System prefers-color-scheme is respected on initial load
 * - All page sections have proper dark mode styling
 */

const { test, expect } = require('@playwright/test');

/**
 * Extract a normalized brightness (0-255) from a computed color string.
 * Handles rgb, rgba, oklch, hsl, and other CSS color formats.
 */
function getBrightness(colorStr) {
  if (!colorStr) return null;

  // Try rgb/rgba format: rgb(255, 255, 255) or rgba(255, 255, 255, 0.5)
  const rgbMatch = colorStr.match(/rgba?\((\d+(?:\.\d+)?),\s*(\d+(?:\.\d+)?),\s*(\d+(?:\.\d+)?)/);
  if (rgbMatch) {
    const r = parseFloat(rgbMatch[1]);
    const g = parseFloat(rgbMatch[2]);
    const b = parseFloat(rgbMatch[3]);
    return (r + g + b) / 3;
  }

  // Try oklch format: oklch(0.278078 0.029596 256.848)
  // First value is lightness (0-1)
  const oklchMatch = colorStr.match(/oklch\(([\d.]+)/);
  if (oklchMatch) {
    const l = parseFloat(oklchMatch[1]);
    return l * 255;
  }

  // Try oklab format: oklab(0.419385 0.00478227 -0.0474926)
  // First value is lightness (0-1)
  const oklabMatch = colorStr.match(/oklab\(([\d.]+)/);
  if (oklabMatch) {
    const l = parseFloat(oklabMatch[1]);
    return l * 255;
  }

  // Try hsl format: hsl(210, 100%, 50%)
  const hslMatch = colorStr.match(/hsl\([\d.]+,\s*[\d.]+%?,\s*([\d.]+)%?\)/);
  if (hslMatch) {
    const l = parseFloat(hslMatch[1]);
    return (l / 100) * 255;
  }

  return null;
}

test.describe('Dark Mode Theme', () => {
  async function clearStorageAndGoto(page, path = '/') {
    await page.goto(path);
    await page.waitForLoadState('networkidle');
    // Clear localStorage after navigation to avoid cross-origin restrictions
    await page.evaluate(() => {
      try {
        localStorage.clear();
      } catch (e) {
        // localStorage may not be available in some contexts
      }
    });
    // Reload to apply the cleared state
    await page.reload();
    await page.waitForLoadState('networkidle');
    await page.waitForTimeout(200);
  }

  test.describe('Default Light Mode', () => {
    test('should render in light mode by default when no preference is stored', async ({ page }) => {
      await clearStorageAndGoto(page, '/');

      // Check the html element has data-theme="light"
      const html = page.locator('html');
      const theme = await html.getAttribute('data-theme');
      expect(theme).toBe('light');

      // Verify light mode background colors
      const body = page.locator('body');
      const bodyBg = await body.evaluate(el => {
        const computed = window.getComputedStyle(el);
        return computed.backgroundColor;
      });

      const brightness = getBrightness(bodyBg);
      expect(brightness).not.toBeNull();
      // Light backgrounds have high brightness
      expect(brightness).toBeGreaterThan(200);
    });

    test('should have dark text on light background by default', async ({ page }) => {
      await clearStorageAndGoto(page, '/');

      const heroTitle = page.locator('h1#hero-title');
      await expect(heroTitle).toBeVisible();

      const textColor = await heroTitle.evaluate(el => {
        const computed = window.getComputedStyle(el);
        return computed.color;
      });

      const brightness = getBrightness(textColor);
      expect(brightness).not.toBeNull();
      // Dark text has low brightness
      expect(brightness).toBeLessThan(120);
    });
  });

  test.describe('Theme Toggle', () => {
    test('should toggle to dark mode when theme button is clicked', async ({ page }) => {
      await clearStorageAndGoto(page, '/');

      const toggleBtn = page.locator('[data-testid="theme-toggle"]:visible');
      await expect(toggleBtn).toBeVisible();

      // Click the toggle button
      await toggleBtn.click();

      // Wait a bit for transition
      await page.waitForTimeout(350);

      // Verify dark mode is active
      const html = page.locator('html');
      const theme = await html.getAttribute('data-theme');
      expect(theme).toBe('dark');
    });

    test('should apply dark background colors in dark mode', async ({ page }) => {
      await clearStorageAndGoto(page, '/');

      const toggleBtn = page.locator('[data-testid="theme-toggle"]:visible');
      await toggleBtn.click();
      await page.waitForTimeout(350);

      // Check body background in dark mode
      const body = page.locator('body');
      const bodyBg = await body.evaluate(el => {
        const computed = window.getComputedStyle(el);
        return computed.backgroundColor;
      });

      const brightness = getBrightness(bodyBg);
      expect(brightness).not.toBeNull();
      // Dark backgrounds have low brightness
      expect(brightness).toBeLessThan(100);
    });

    test('should apply light text colors in dark mode', async ({ page }) => {
      await clearStorageAndGoto(page, '/');

      const toggleBtn = page.locator('[data-testid="theme-toggle"]:visible');
      await toggleBtn.click();
      await page.waitForTimeout(350);

      const heroTitle = page.locator('h1#hero-title');
      const textColor = await heroTitle.evaluate(el => {
        const computed = window.getComputedStyle(el);
        return computed.color;
      });

      const brightness = getBrightness(textColor);
      expect(brightness).not.toBeNull();
      // Light text has high brightness
      expect(brightness).toBeGreaterThan(180);
    });

    test('should have smooth transition when toggling theme', async ({ page }) => {
      await clearStorageAndGoto(page, '/');

      // Check that theme transition styles are applied
      const html = page.locator('html');
      const transition = await html.evaluate(el => {
        const computed = window.getComputedStyle(el);
        return computed.transition;
      });

      // Should have background-color transition
      expect(transition).toContain('background-color');
    });

    test('should toggle back to light mode when clicked again', async ({ page }) => {
      await clearStorageAndGoto(page, '/');

      const toggleBtn = page.locator('[data-testid="theme-toggle"]:visible');

      // Toggle to dark
      await toggleBtn.click();
      await page.waitForTimeout(350);

      let theme = await page.locator('html').getAttribute('data-theme');
      expect(theme).toBe('dark');

      // Toggle back to light
      await toggleBtn.click();
      await page.waitForTimeout(350);

      theme = await page.locator('html').getAttribute('data-theme');
      expect(theme).toBe('light');
    });
  });

  test.describe('Theme Persistence', () => {
    test('should persist dark mode preference across page reloads', async ({ page }) => {
      await clearStorageAndGoto(page, '/');

      // Toggle to dark mode
      const toggleBtn = page.locator('[data-testid="theme-toggle"]:visible');
      await toggleBtn.click();
      await page.waitForTimeout(350);

      // Verify dark mode is stored in localStorage
      const storedTheme = await page.evaluate(() => localStorage.getItem('theme'));
      expect(storedTheme).toBe('dark');

      // Reload the page
      await page.reload();
      await page.waitForLoadState('networkidle');
      await page.waitForTimeout(200);

      // Verify dark mode is still active after reload
      const theme = await page.locator('html').getAttribute('data-theme');
      expect(theme).toBe('dark');
    });

    test('should persist light mode preference across page reloads', async ({ page }) => {
      await clearStorageAndGoto(page, '/');

      // First toggle to dark, then back to light to set explicit preference
      const toggleBtn = page.locator('[data-testid="theme-toggle"]:visible');
      await toggleBtn.click();
      await page.waitForTimeout(350);
      await toggleBtn.click();
      await page.waitForTimeout(350);

      // Verify light mode is stored
      const storedTheme = await page.evaluate(() => localStorage.getItem('theme'));
      expect(storedTheme).toBe('light');

      // Reload
      await page.reload();
      await page.waitForLoadState('networkidle');
      await page.waitForTimeout(200);

      const theme = await page.locator('html').getAttribute('data-theme');
      expect(theme).toBe('light');
    });
  });

  test.describe('System Preference Detection', () => {
    test.use({ colorScheme: 'dark' });

    test('should respect system dark mode preference on initial load', async ({ page }) => {
      // With colorScheme: 'dark', the browser should report prefers-color-scheme: dark
      await clearStorageAndGoto(page, '/');
      await page.waitForTimeout(200);

      // Check that the page detected the dark preference
      // Note: localStorage is cleared in beforeEach, so the system preference should apply
      const theme = await page.locator('html').getAttribute('data-theme');

      // The page should respect the system preference when no localStorage is set
      expect(theme).toBe('dark');
    });
  });

  test.describe('System Light Preference', () => {
    test.use({ colorScheme: 'light' });

    test('should respect system light mode preference on initial load', async ({ page }) => {
      await clearStorageAndGoto(page, '/');
      await page.waitForTimeout(200);

      const theme = await page.locator('html').getAttribute('data-theme');
      expect(theme).toBe('light');
    });
  });

  test.describe('All Sections Dark Mode Styling', () => {
    test('hero section has proper dark mode styling', async ({ page }) => {
      await clearStorageAndGoto(page, '/');

      const toggleBtn = page.locator('[data-testid="theme-toggle"]:visible');
      await toggleBtn.click();
      await page.waitForTimeout(350);

      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();

      const heroBg = await heroSection.evaluate(el => {
        const computed = window.getComputedStyle(el);
        return computed.backgroundColor;
      });

      const brightness = getBrightness(heroBg);
      expect(brightness).not.toBeNull();
      // Dark hero background
      expect(brightness).toBeLessThan(150);
    });

    test('features section has proper dark mode styling', async ({ page }) => {
      await clearStorageAndGoto(page, '/');

      const toggleBtn = page.locator('[data-testid="theme-toggle"]:visible');
      await toggleBtn.click();
      await page.waitForTimeout(350);

      const featuresSection = page.locator('#features');
      await expect(featuresSection).toBeVisible();

      const featuresBg = await featuresSection.evaluate(el => {
        const computed = window.getComputedStyle(el);
        return computed.backgroundColor;
      });

      const brightness = getBrightness(featuresBg);
      expect(brightness).not.toBeNull();
      // Dark features background
      expect(brightness).toBeLessThan(100);

      // Feature cards should have dark styling
      const featureCards = page.locator('.feature-card');
      const firstCard = featureCards.first();
      await expect(firstCard).toBeVisible();

      const cardBg = await firstCard.evaluate(el => {
        const computed = window.getComputedStyle(el);
        return computed.backgroundColor;
      });

      const cardBrightness = getBrightness(cardBg);
      expect(cardBrightness).not.toBeNull();
      expect(cardBrightness).toBeLessThan(150);
    });

    test('CTA section has proper dark mode styling', async ({ page }) => {
      await clearStorageAndGoto(page, '/');

      const toggleBtn = page.locator('[data-testid="theme-toggle"]:visible');
      await toggleBtn.click();
      await page.waitForTimeout(350);

      const ctaSection = page.locator('[data-testid="cta-section"]');
      await expect(ctaSection).toBeVisible();

      const ctaHeading = ctaSection.locator('h2#cta-title');
      const headingColor = await ctaHeading.evaluate(el => {
        const computed = window.getComputedStyle(el);
        return computed.color;
      });

      // Heading text should be visible in dark mode
      const brightness = getBrightness(headingColor);
      expect(brightness).not.toBeNull();
      expect(brightness).toBeGreaterThan(100);
    });

    test('footer has proper dark mode styling', async ({ page }) => {
      await clearStorageAndGoto(page, '/');

      const toggleBtn = page.locator('[data-testid="theme-toggle"]:visible');
      await toggleBtn.click();
      await page.waitForTimeout(350);

      const footer = page.locator('[data-testid="footer"]');
      await expect(footer).toBeVisible();

      const footerBg = await footer.evaluate(el => {
        const computed = window.getComputedStyle(el);
        return computed.backgroundColor;
      });

      const brightness = getBrightness(footerBg);
      expect(brightness).not.toBeNull();
      // Dark footer background
      expect(brightness).toBeLessThan(150);
    });

    test('navigation bar has proper dark mode styling', async ({ page }) => {
      await clearStorageAndGoto(page, '/');

      const toggleBtn = page.locator('[data-testid="theme-toggle"]:visible');
      await toggleBtn.click();
      await page.waitForTimeout(350);

      const navbar = page.locator('nav.navbar');
      await expect(navbar).toBeVisible();

      const navBg = await navbar.evaluate(el => {
        const computed = window.getComputedStyle(el);
        return computed.backgroundColor;
      });

      const brightness = getBrightness(navBg);
      expect(brightness).not.toBeNull();
      // Dark navbar background
      expect(brightness).toBeLessThan(150);
    });
  });
});
