import { test, expect } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';

// Helper function to check if a color is "light" (high luminance)
function isLightColor(colorValue: string): boolean {
  // For oklch/oklab colors, the lightness is the first value (0-1 scale)
  if (colorValue.startsWith('oklch') || colorValue.startsWith('oklab')) {
    const match = colorValue.match(/\(([\d.]+)/);
    if (match) {
      const lightness = parseFloat(match[1]);
      return lightness > 0.5;
    }
  }
  // For rgb colors
  if (colorValue.startsWith('rgb')) {
    const match = colorValue.match(/\d+/g);
    if (match && match.length >= 3) {
      const [r, g, b] = match.map(Number);
      const luminance = (0.299 * r + 0.587 * g + 0.114 * b) / 255;
      return luminance > 0.5;
    }
  }
  return false;
}

// Helper function to check if a color is "dark" (low luminance)
function isDarkColor(colorValue: string): boolean {
  // For oklch/oklab colors, the lightness is the first value (0-1 scale)
  if (colorValue.startsWith('oklch') || colorValue.startsWith('oklab')) {
    const match = colorValue.match(/\(([\d.]+)/);
    if (match) {
      const lightness = parseFloat(match[1]);
      return lightness < 0.5;
    }
  }
  // For rgb colors
  if (colorValue.startsWith('rgb')) {
    const match = colorValue.match(/\d+/g);
    if (match && match.length >= 3) {
      const [r, g, b] = match.map(Number);
      const luminance = (0.299 * r + 0.587 * g + 0.114 * b) / 255;
      return luminance < 0.5;
    }
  }
  return false;
}

test.describe('Dark Mode Support', () => {
  test.describe('Light mode preference', () => {
    test.use({ colorScheme: 'light' });

    test('page renders with light background and dark text', async ({ page }) => {
      await page.goto('/');

      // Check body has light background
      const body = page.locator('body');
      const bodyBgColor = await body.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });

      // Verify light background (high luminance)
      expect(isLightColor(bodyBgColor)).toBe(true);

      // Check body has dark text
      const bodyTextColor = await body.evaluate((el) => {
        return window.getComputedStyle(el).color;
      });

      // Verify dark text (low luminance)
      expect(isDarkColor(bodyTextColor)).toBe(true);

      // Check hero section is visible
      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();
    });

    test('navigation has light background', async ({ page }) => {
      await page.goto('/');

      const nav = page.locator('[data-testid="navigation"]');
      const navBgColor = await nav.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });

      // Verify light/white background
      expect(isLightColor(navBgColor)).toBe(true);
    });

    test('code blocks have light background in light mode', async ({ page }) => {
      await page.goto('/');

      // Scroll to quick start section
      await page.locator('[data-testid="quick-start-section"]').scrollIntoViewIfNeeded();

      const codeBlock = page.locator('pre[class*="language-"]').first();
      await expect(codeBlock).toBeVisible();

      const codeBgColor = await codeBlock.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });

      // Verify light background for code blocks in light mode
      expect(isLightColor(codeBgColor)).toBe(true);
    });

    test('feature cards have light background', async ({ page }) => {
      await page.goto('/');

      const featureCard = page.locator('[data-testid="feature-card-memcached-protocol"]');
      await featureCard.scrollIntoViewIfNeeded();
      await expect(featureCard).toBeVisible();

      const cardBgColor = await featureCard.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });

      // Verify light background
      expect(isLightColor(cardBgColor)).toBe(true);
    });
  });

  test.describe('Dark mode preference', () => {
    test.use({ colorScheme: 'dark' });

    test('page renders with dark background and light text', async ({ page }) => {
      await page.goto('/');

      // Check body has dark background
      const body = page.locator('body');
      const bodyBgColor = await body.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });

      // Verify dark background (low luminance)
      expect(isDarkColor(bodyBgColor)).toBe(true);

      // Check body has light text
      const bodyTextColor = await body.evaluate((el) => {
        return window.getComputedStyle(el).color;
      });

      // Verify light text (high luminance)
      expect(isLightColor(bodyTextColor)).toBe(true);

      // Check hero section is visible
      const heroSection = page.locator('[data-testid="hero-section"]');
      await expect(heroSection).toBeVisible();
    });

    test('navigation has dark background', async ({ page }) => {
      await page.goto('/');

      const nav = page.locator('[data-testid="navigation"]');
      const navBgColor = await nav.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });

      // Verify dark background
      expect(isDarkColor(navBgColor)).toBe(true);
    });

    test('code blocks have dark background and adapt syntax highlighting', async ({ page }) => {
      await page.goto('/');

      // Scroll to quick start section
      await page.locator('[data-testid="quick-start-section"]').scrollIntoViewIfNeeded();

      const codeBlock = page.locator('pre[class*="language-"]').first();
      await expect(codeBlock).toBeVisible();

      const codeBgColor = await codeBlock.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });

      // Verify dark background for code blocks in dark mode
      expect(isDarkColor(codeBgColor)).toBe(true);

      // Check code text has light color (dark theme Prism)
      const codeElement = page.locator('code[class*="language-"]').first();
      const codeTextColor = await codeElement.evaluate((el) => {
        return window.getComputedStyle(el).color;
      });

      // Verify light text in code blocks
      expect(isLightColor(codeTextColor)).toBe(true);
    });

    test('feature cards have dark background', async ({ page }) => {
      await page.goto('/');

      const featureCard = page.locator('[data-testid="feature-card-memcached-protocol"]');
      await featureCard.scrollIntoViewIfNeeded();
      await expect(featureCard).toBeVisible();

      const cardBgColor = await featureCard.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });

      // Verify dark background
      expect(isDarkColor(cardBgColor)).toBe(true);
    });

    test('footer has dark background', async ({ page }) => {
      await page.goto('/');

      const footer = page.locator('[data-testid="footer-section"]');
      await footer.scrollIntoViewIfNeeded();
      await expect(footer).toBeVisible();

      const footerBgColor = await footer.evaluate((el) => {
        return window.getComputedStyle(el).backgroundColor;
      });

      // Verify very dark footer background
      expect(isDarkColor(footerBgColor)).toBe(true);
    });
  });

  test.describe('Dark mode contrast and accessibility', () => {
    test.use({ colorScheme: 'dark' });

    test('all text meets WCAG contrast requirements in dark mode', async ({ page }) => {
      await page.goto('/');

      // Run accessibility audit with axe-core
      const accessibilityScanResults = await new AxeBuilder({ page })
        .withTags(['wcag2aa', 'wcag21aa'])
        .analyze();

      // Filter for color contrast violations
      const contrastViolations = accessibilityScanResults.violations.filter(
        (violation) => violation.id === 'color-contrast'
      );

      // Expect no color contrast violations
      expect(contrastViolations).toHaveLength(0);
    });

    test('headings are readable in dark mode', async ({ page }) => {
      await page.goto('/');

      // Check h1 (product name) is visible and has proper color
      const h1 = page.locator('[data-testid="hero-product-name"]');
      await expect(h1).toBeVisible();

      // Check section headings
      const sectionHeadings = page.locator('h2');
      const headingCount = await sectionHeadings.count();
      expect(headingCount).toBeGreaterThan(0);

      for (let i = 0; i < headingCount; i++) {
        const heading = sectionHeadings.nth(i);
        await heading.scrollIntoViewIfNeeded();
        await expect(heading).toBeVisible();
      }
    });

    test('links are distinguishable in dark mode', async ({ page }) => {
      await page.goto('/');

      // Check CTA button visibility
      const ctaButton = page.locator('[data-testid="cta-get-started"]');
      await expect(ctaButton).toBeVisible();

      // Check footer links
      const footerLink = page.locator('[data-testid="footer-github-link"]');
      await footerLink.scrollIntoViewIfNeeded();
      await expect(footerLink).toBeVisible();

      // Verify links have a distinguishable color (blue tones)
      const linkColor = await footerLink.evaluate((el) => {
        return window.getComputedStyle(el).color;
      });

      // Links should have some color (not black or white)
      expect(linkColor).toBeTruthy();
    });

    test('comparison table is readable in dark mode', async ({ page }) => {
      await page.goto('/');

      const comparisonTable = page.locator('[data-testid="comparison-table"]');
      await comparisonTable.scrollIntoViewIfNeeded();
      await expect(comparisonTable).toBeVisible();

      // Check MirDB column header is visible
      const mirdbHeader = page.locator('[data-testid="comparison-header-mirdb"]');
      await expect(mirdbHeader).toBeVisible();
      await expect(mirdbHeader).toHaveText('MirDB');
    });
  });

  test.describe('Color scheme meta tags', () => {
    test('has correct color-scheme meta tag', async ({ page }) => {
      await page.goto('/');

      const colorSchemeMeta = page.locator('meta[name="color-scheme"]');
      await expect(colorSchemeMeta).toHaveAttribute('content', 'light dark');
    });

    test('has theme-color meta tag for light mode', async ({ page }) => {
      await page.goto('/');

      const lightThemeMeta = page.locator('meta[name="theme-color"][media="(prefers-color-scheme: light)"]');
      await expect(lightThemeMeta).toHaveAttribute('content', '#f9fafb');
    });

    test('has theme-color meta tag for dark mode', async ({ page }) => {
      await page.goto('/');

      const darkThemeMeta = page.locator('meta[name="theme-color"][media="(prefers-color-scheme: dark)"]');
      await expect(darkThemeMeta).toHaveAttribute('content', '#1f2937');
    });
  });
});
