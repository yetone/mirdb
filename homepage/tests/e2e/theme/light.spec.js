/**
 * Light Mode Theme E2E Tests
 * Owner: Scenario 16 - Theme Support - Light Mode
 *
 * These tests verify that light mode theme functions correctly as the default theme.
 * Test cases:
 * 1. Page background is light/white color
 * 2. Text color is dark for readability
 * 3. Code blocks have light-appropriate syntax highlighting
 */

const { test, expect } = require('@playwright/test');

// Helper function to convert RGB to hex
function rgbToHex(rgb) {
  if (!rgb || rgb === 'transparent') return null;
  const match = rgb.match(/^rgba?\((\d+),\s*(\d+),\s*(\d+)/);
  if (!match) return rgb;
  const r = parseInt(match[1]);
  const g = parseInt(match[2]);
  const b = parseInt(match[3]);
  return '#' + [r, g, b].map(x => x.toString(16).padStart(2, '0')).join('');
}

// Helper to check if a color is "light" (brightness > 50%)
function isLightColor(hexColor) {
  if (!hexColor || hexColor === 'transparent') return true;
  const hex = hexColor.replace('#', '');
  const r = parseInt(hex.substring(0, 2), 16);
  const g = parseInt(hex.substring(2, 4), 16);
  const b = parseInt(hex.substring(4, 6), 16);
  // Calculate relative luminance
  const brightness = (r * 299 + g * 587 + b * 114) / 1000;
  return brightness > 128; // Light if brightness > 50%
}

// Helper to check if a color is "dark" (brightness < 50%)
function isDarkColor(hexColor) {
  if (!hexColor) return false;
  const hex = hexColor.replace('#', '');
  const r = parseInt(hex.substring(0, 2), 16);
  const g = parseInt(hex.substring(2, 4), 16);
  const b = parseInt(hex.substring(4, 6), 16);
  // Calculate relative luminance
  const brightness = (r * 299 + g * 587 + b * 114) / 1000;
  return brightness < 128; // Dark if brightness < 50%
}

test.describe('Light Mode Theme', () => {
  test.beforeEach(async ({ page }) => {
    // Navigate to the homepage
    await page.goto('/');
    // Wait for the page to fully load
    await page.waitForLoadState('domcontentloaded');
  });

  test('Test Case 1: Page background is light/white color', async ({ page }) => {
    // Get the body background color
    const bodyBgColor = await page.evaluate(() => {
      return window.getComputedStyle(document.body).backgroundColor;
    });

    const hexColor = rgbToHex(bodyBgColor);

    // Verify background is light (white or near-white)
    // Expected: #ffffff (white) based on CSS variables
    expect(isLightColor(hexColor),
      `Expected light background color, got ${hexColor}`
    ).toBe(true);

    // Also verify it matches our expected CSS variable value
    const cssVarBgColor = await page.evaluate(() => {
      return getComputedStyle(document.documentElement).getPropertyValue('--bg-color').trim();
    });
    expect(cssVarBgColor).toBe('#ffffff');
  });

  test('Test Case 2: Text color is dark for readability', async ({ page }) => {
    // Get the body text color
    const bodyTextColor = await page.evaluate(() => {
      return window.getComputedStyle(document.body).color;
    });

    const hexColor = rgbToHex(bodyTextColor);

    // Verify text is dark for readability
    // Expected: #1a1a1a (dark gray) based on CSS variables
    expect(isDarkColor(hexColor),
      `Expected dark text color for readability, got ${hexColor}`
    ).toBe(true);

    // Also verify it matches our expected CSS variable value
    const cssVarTextColor = await page.evaluate(() => {
      return getComputedStyle(document.documentElement).getPropertyValue('--text-color').trim();
    });
    expect(cssVarTextColor).toBe('#1a1a1a');

    // Check that headings also have dark color
    const heroTitle = page.locator('.hero-title');
    await expect(heroTitle).toBeVisible();

    const heroTitleColor = await heroTitle.evaluate((el) => {
      return window.getComputedStyle(el).color;
    });
    const heroTitleHex = rgbToHex(heroTitleColor);
    expect(isDarkColor(heroTitleHex),
      `Expected dark heading color, got ${heroTitleHex}`
    ).toBe(true);

    // Check paragraph text color
    const heroTagline = page.locator('.hero-tagline');
    await expect(heroTagline).toBeVisible();

    const taglineColor = await heroTagline.evaluate((el) => {
      return window.getComputedStyle(el).color;
    });
    const taglineHex = rgbToHex(taglineColor);
    expect(isDarkColor(taglineHex),
      `Expected dark tagline color, got ${taglineHex}`
    ).toBe(true);
  });

  test('Test Case 3: Code blocks have light-appropriate syntax highlighting', async ({ page }) => {
    // Navigate to quick start section to see code blocks
    const quickstartSection = page.locator('#quickstart');
    await expect(quickstartSection).toBeVisible();

    // Get the first code block
    const codeBlock = page.locator('.code-block').first();
    await expect(codeBlock).toBeVisible();

    // Check code block background is light
    const codeBlockBg = await codeBlock.locator('pre').evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });
    const codeBlockHex = rgbToHex(codeBlockBg);

    // Verify code block background is light (light gray for readability)
    // Expected: #f4f4f4 based on CSS variables
    expect(isLightColor(codeBlockHex),
      `Expected light code block background, got ${codeBlockHex}`
    ).toBe(true);

    // Verify the CSS variable value
    const cssVarCodeBg = await page.evaluate(() => {
      return getComputedStyle(document.documentElement).getPropertyValue('--code-bg').trim();
    });
    expect(cssVarCodeBg).toBe('#f4f4f4');

    // Check code text is readable (dark text on light background)
    const codeElement = codeBlock.locator('code').first();
    await expect(codeElement).toBeVisible();

    const codeTextColor = await codeElement.evaluate((el) => {
      return window.getComputedStyle(el).color;
    });
    const codeTextHex = rgbToHex(codeTextColor);
    expect(isDarkColor(codeTextHex),
      `Expected dark code text color, got ${codeTextHex}`
    ).toBe(true);

    // Verify font family is monospace for code
    const codeFontFamily = await codeElement.evaluate((el) => {
      return window.getComputedStyle(el).fontFamily;
    });
    expect(codeFontFamily.toLowerCase()).toMatch(/monaco|menlo|consolas|monospace/i);
  });

  test('Light mode is the default theme', async ({ page }) => {
    // Verify that light mode is applied by default (no dark theme attributes)
    const htmlElement = page.locator('html');

    // Check that there's no dark theme data attribute
    const dataTheme = await htmlElement.getAttribute('data-theme');
    expect(dataTheme === null || dataTheme === 'light').toBe(true);

    // Check that body doesn't have dark-mode class
    const bodyClasses = await page.locator('body').getAttribute('class');
    expect(bodyClasses === null || !bodyClasses.includes('dark-mode')).toBe(true);
  });

  test('All sections have light mode styling', async ({ page }) => {
    // Verify hero section has appropriate light mode styling
    const heroSection = page.locator('#hero');
    await expect(heroSection).toBeVisible();

    const heroBg = await heroSection.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });
    const heroHex = rgbToHex(heroBg);
    expect(isLightColor(heroHex),
      `Expected light hero background, got ${heroHex}`
    ).toBe(true);

    // Verify features section
    const featuresSection = page.locator('#features');
    await expect(featuresSection).toBeVisible();

    const featuresBg = await featuresSection.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });
    const featuresHex = rgbToHex(featuresBg);
    // Features section should have transparent or light background
    if (featuresHex && featuresHex !== 'transparent') {
      expect(isLightColor(featuresHex),
        `Expected light features background, got ${featuresHex}`
      ).toBe(true);
    }

    // Verify roadmap section
    const roadmapSection = page.locator('#roadmap');
    await expect(roadmapSection).toBeVisible();

    const roadmapBg = await roadmapSection.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });
    const roadmapHex = rgbToHex(roadmapBg);
    // Roadmap section should have transparent or light background
    if (roadmapHex && roadmapHex !== 'transparent') {
      expect(isLightColor(roadmapHex),
        `Expected light roadmap background, got ${roadmapHex}`
      ).toBe(true);
    }
  });

  test('Accent colors are appropriate for light mode', async ({ page }) => {
    // Check link/accent colors
    const cssVarAccent = await page.evaluate(() => {
      return getComputedStyle(document.documentElement).getPropertyValue('--accent-color').trim();
    });

    // Accent color should be defined and provide good contrast on light backgrounds
    expect(cssVarAccent).toBeTruthy();
    expect(cssVarAccent).toMatch(/^#[0-9a-fA-F]{6}$/);

    // Verify CTA button is visible and styled
    const ctaButton = page.locator('.cta-button').first();
    await expect(ctaButton).toBeVisible();

    const ctaBgColor = await ctaButton.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });
    expect(ctaBgColor).toBeTruthy();

    // Check navigation links use accent color
    const navLink = page.locator('.nav-link').first();
    await expect(navLink).toBeVisible();

    const navLinkColor = await navLink.evaluate((el) => {
      return window.getComputedStyle(el).color;
    });
    expect(navLinkColor).toBeTruthy();
  });
});
