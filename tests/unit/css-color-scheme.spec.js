/**
 * CSS Color Scheme Unit Tests
 * Owner: Scenario 1 - Hero Section and Branding (CSS color validation)
 *
 * Tests:
 * - Dark background color (near black) defined in CSS variables
 * - Light text colors defined in CSS variables
 * - Color contrast ratio meets WCAG 2.1 AA (4.5:1)
 */

const { test, expect } = require('@playwright/test');
const fs = require('fs');
const path = require('path');

/**
 * Calculate relative luminance of an RGB color
 * Based on WCAG 2.1 formula
 */
function getRelativeLuminance(r, g, b) {
  const [rs, gs, bs] = [r, g, b].map((c) => {
    const sRGB = c / 255;
    return sRGB <= 0.03928 ? sRGB / 12.92 : Math.pow((sRGB + 0.055) / 1.055, 2.4);
  });
  return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
}

/**
 * Calculate contrast ratio between two colors
 * Returns value between 1 and 21
 */
function getContrastRatio(rgb1, rgb2) {
  const l1 = getRelativeLuminance(rgb1.r, rgb1.g, rgb1.b);
  const l2 = getRelativeLuminance(rgb2.r, rgb2.g, rgb2.b);
  const lighter = Math.max(l1, l2);
  const darker = Math.min(l1, l2);
  return (lighter + 0.05) / (darker + 0.05);
}

/**
 * Parse hex color to {r, g, b} object
 */
function parseHex(hex) {
  const cleanHex = hex.replace('#', '');
  return {
    r: parseInt(cleanHex.substring(0, 2), 16),
    g: parseInt(cleanHex.substring(2, 4), 16),
    b: parseInt(cleanHex.substring(4, 6), 16),
  };
}

/**
 * Check if a color is "dark" (low luminance)
 */
function isDarkColor(rgb) {
  const luminance = getRelativeLuminance(rgb.r, rgb.g, rgb.b);
  return luminance < 0.2; // Less than 20% luminance is considered dark
}

/**
 * Check if a color is "light" (high luminance)
 */
function isLightColor(rgb) {
  const luminance = getRelativeLuminance(rgb.r, rgb.g, rgb.b);
  return luminance > 0.5; // More than 50% luminance is considered light
}

test.describe('CSS Color Scheme Validation', () => {
  let cssContent;

  test.beforeAll(async () => {
    // Read the CSS file
    const cssPath = path.join(process.cwd(), 'docs', 'css', 'styles.css');
    cssContent = fs.readFileSync(cssPath, 'utf-8');
  });

  test('TC-4: Dark background color defined in CSS variables', async () => {
    // Extract background color variables from CSS
    const bgPrimaryMatch = cssContent.match(/--color-bg-primary:\s*(#[0-9a-fA-F]{6})/);
    const bgSecondaryMatch = cssContent.match(/--color-bg-secondary:\s*(#[0-9a-fA-F]{6})/);
    const bgTertiaryMatch = cssContent.match(/--color-bg-tertiary:\s*(#[0-9a-fA-F]{6})/);

    // Verify CSS variables are defined
    expect(bgPrimaryMatch).not.toBeNull();
    expect(bgSecondaryMatch).not.toBeNull();
    expect(bgTertiaryMatch).not.toBeNull();

    const bgPrimary = bgPrimaryMatch[1];
    const bgSecondary = bgSecondaryMatch[1];
    const bgTertiary = bgTertiaryMatch[1];

    // Parse colors
    const bgPrimaryRgb = parseHex(bgPrimary);
    const bgSecondaryRgb = parseHex(bgSecondary);
    const bgTertiaryRgb = parseHex(bgTertiary);

    // Verify background colors are dark (near black)
    expect(isDarkColor(bgPrimaryRgb)).toBe(true);
    expect(isDarkColor(bgSecondaryRgb)).toBe(true);
    expect(isDarkColor(bgTertiaryRgb)).toBe(true);

    // Additional check: primary background should be the darkest
    expect(bgPrimaryRgb.r + bgPrimaryRgb.g + bgPrimaryRgb.b).toBeLessThan(100);
  });

  test('TC-4: Light text colors defined in CSS variables', async () => {
    // Extract text color variables from CSS
    const textPrimaryMatch = cssContent.match(/--color-text-primary:\s*(#[0-9a-fA-F]{6})/);
    const textSecondaryMatch = cssContent.match(/--color-text-secondary:\s*(#[0-9a-fA-F]{6})/);

    // Verify CSS variables are defined
    expect(textPrimaryMatch).not.toBeNull();
    expect(textSecondaryMatch).not.toBeNull();

    const textPrimary = textPrimaryMatch[1];
    const textSecondary = textSecondaryMatch[1];

    // Parse colors
    const textPrimaryRgb = parseHex(textPrimary);
    const textSecondaryRgb = parseHex(textSecondary);

    // Verify primary text color is light (near white)
    expect(isLightColor(textPrimaryRgb)).toBe(true);

    // Secondary text should have reasonable brightness (not too dark)
    const secondaryLuminance = getRelativeLuminance(
      textSecondaryRgb.r,
      textSecondaryRgb.g,
      textSecondaryRgb.b
    );
    expect(secondaryLuminance).toBeGreaterThan(0.2);
  });

  test('TC-5: Color contrast ratio meets WCAG 2.1 AA minimum (4.5:1)', async () => {
    // Extract color variables
    const bgPrimaryMatch = cssContent.match(/--color-bg-primary:\s*(#[0-9a-fA-F]{6})/);
    const textPrimaryMatch = cssContent.match(/--color-text-primary:\s*(#[0-9a-fA-F]{6})/);
    const textSecondaryMatch = cssContent.match(/--color-text-secondary:\s*(#[0-9a-fA-F]{6})/);

    expect(bgPrimaryMatch).not.toBeNull();
    expect(textPrimaryMatch).not.toBeNull();
    expect(textSecondaryMatch).not.toBeNull();

    const bgPrimaryRgb = parseHex(bgPrimaryMatch[1]);
    const textPrimaryRgb = parseHex(textPrimaryMatch[1]);
    const textSecondaryRgb = parseHex(textSecondaryMatch[1]);

    // Calculate contrast ratios
    const primaryTextContrast = getContrastRatio(bgPrimaryRgb, textPrimaryRgb);
    const secondaryTextContrast = getContrastRatio(bgPrimaryRgb, textSecondaryRgb);

    // WCAG 2.1 AA requires 4.5:1 for normal text
    // NFR-11 specifies minimum 4.5:1
    expect(primaryTextContrast).toBeGreaterThanOrEqual(4.5);
    expect(secondaryTextContrast).toBeGreaterThanOrEqual(4.5);

    // Log actual contrast values for documentation
    console.log(`Primary text contrast ratio: ${primaryTextContrast.toFixed(2)}:1`);
    console.log(`Secondary text contrast ratio: ${secondaryTextContrast.toFixed(2)}:1`);
  });

  test('CSS defines tech accent colors for dark theme', async () => {
    // Verify accent colors are defined (tech aesthetic)
    const accentPrimaryMatch = cssContent.match(/--color-accent-primary:\s*(#[0-9a-fA-F]{6})/);
    const accentGreenMatch = cssContent.match(/--color-accent-green:\s*(#[0-9a-fA-F]{6})/);
    const accentPurpleMatch = cssContent.match(/--color-accent-purple:\s*(#[0-9a-fA-F]{6})/);

    expect(accentPrimaryMatch).not.toBeNull();
    expect(accentGreenMatch).not.toBeNull();
    expect(accentPurpleMatch).not.toBeNull();
  });

  test('CSS uses CSS custom properties for theming', async () => {
    // Verify :root block with CSS variables exists
    expect(cssContent).toContain(':root');
    expect(cssContent).toContain('--color-bg-primary');
    expect(cssContent).toContain('--color-text-primary');
    expect(cssContent).toContain('--font-sans');
    expect(cssContent).toContain('--font-mono');
  });

  test('Body uses dark background color variable', async () => {
    // Verify body background uses the CSS variable
    const bodyBgMatch = cssContent.match(/body\s*\{[^}]*background-color:\s*var\(--color-bg-primary\)/s);
    expect(bodyBgMatch).not.toBeNull();
  });

  test('Body uses light text color variable', async () => {
    // Verify body color uses the CSS variable
    const bodyColorMatch = cssContent.match(/body\s*\{[^}]*color:\s*var\(--color-text-primary\)/s);
    expect(bodyColorMatch).not.toBeNull();
  });
});
