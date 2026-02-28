/**
 * Dark Theme Implementation E2E Tests
 * Owner: Scenario 15 - Dark Theme Implementation
 *
 * Test cases:
 * 1. Body background color is dark (luminance < 0.2)
 * 2. Primary text color is light (luminance > 0.8) contrasting with background
 * 3. CTA button has visible accent color distinguishable from background
 * 4. Code blocks have appropriate dark theme syntax highlighting
 */

const { test, expect } = require('@playwright/test');
const {
  waitForPageLoad,
  parseRgb,
  getLuminance,
  measureContrastRatio,
  WCAG_AA_NORMAL_TEXT,
} = require('./test-utils');

test.describe('Dark Theme Implementation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await waitForPageLoad(page);
  });

  test('body background color is dark (luminance < 0.2)', async ({ page }) => {
    // Get the body element's background color
    const body = page.locator('body');
    const backgroundColor = await body.evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });

    // Parse the RGB color
    const rgb = parseRgb(backgroundColor);

    // Calculate luminance (WCAG relative luminance formula)
    const luminance = getLuminance(rgb);

    // Verify background is dark (luminance < 0.2)
    expect(
      luminance,
      `Body background should be dark (luminance < 0.2), got ${luminance.toFixed(3)} for color ${backgroundColor}`
    ).toBeLessThan(0.2);

    // Additional check: Verify the background color is the expected dark theme color
    // Expected: #0d1117 (rgb(13, 17, 23))
    expect(rgb.r, 'Red component should be low').toBeLessThan(50);
    expect(rgb.g, 'Green component should be low').toBeLessThan(50);
    expect(rgb.b, 'Blue component should be low').toBeLessThan(50);
  });

  test('primary text color is light (luminance > 0.8) contrasting with background', async ({ page }) => {
    // Get primary text color from body
    const body = page.locator('body');
    const textColor = await body.evaluate((el) => {
      return window.getComputedStyle(el).color;
    });

    // Parse the RGB color
    const rgb = parseRgb(textColor);

    // Calculate luminance
    const luminance = getLuminance(rgb);

    // Verify text is light (luminance > 0.8)
    expect(
      luminance,
      `Primary text should be light (luminance > 0.8), got ${luminance.toFixed(3)} for color ${textColor}`
    ).toBeGreaterThan(0.8);

    // Verify text contrasts with background (high RGB values)
    expect(rgb.r, 'Red component should be high').toBeGreaterThan(200);
    expect(rgb.g, 'Green component should be high').toBeGreaterThan(200);
    expect(rgb.b, 'Blue component should be high').toBeGreaterThan(200);

    // Verify contrast ratio between text and background meets WCAG AA
    const { ratio } = await measureContrastRatio(page, body);
    expect(
      ratio,
      `Primary text contrast ratio should be >= ${WCAG_AA_NORMAL_TEXT}:1, got ${ratio.toFixed(2)}:1`
    ).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
  });

  test('CTA button has visible accent color distinguishable from background', async ({ page }) => {
    // Find the CTA button
    const ctaButton = page.locator('.hero__cta');
    await expect(ctaButton).toBeVisible();

    // Get button styles
    const buttonStyles = await ctaButton.evaluate((el) => {
      const computed = window.getComputedStyle(el);
      return {
        backgroundColor: computed.backgroundColor,
        color: computed.color,
        borderColor: computed.borderColor,
      };
    });

    // Get page background for comparison
    const pageBackground = await page.locator('body').evaluate((el) => {
      return window.getComputedStyle(el).backgroundColor;
    });

    // Parse colors
    const buttonBgRgb = parseRgb(buttonStyles.backgroundColor);
    const pageBgRgb = parseRgb(pageBackground);

    // Calculate luminance difference to ensure button is distinguishable
    const buttonLuminance = getLuminance(buttonBgRgb);
    const pageLuminance = getLuminance(pageBgRgb);
    const luminanceDifference = Math.abs(buttonLuminance - pageLuminance);

    // Button should have noticeable luminance difference from background
    expect(
      luminanceDifference,
      `CTA button should be distinguishable from background (luminance diff >= 0.1), got ${luminanceDifference.toFixed(3)}`
    ).toBeGreaterThanOrEqual(0.1);

    // Verify button text has good contrast against button background
    const textRgb = parseRgb(buttonStyles.color);
    const textLuminance = getLuminance(textRgb);

    // Calculate contrast ratio using WCAG formula
    const lighter = Math.max(textLuminance, buttonLuminance);
    const darker = Math.min(textLuminance, buttonLuminance);
    const contrastRatio = (lighter + 0.05) / (darker + 0.05);

    expect(
      contrastRatio,
      `CTA button text should have good contrast (>= ${WCAG_AA_NORMAL_TEXT}:1), got ${contrastRatio.toFixed(2)}:1`
    ).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);

    // Verify button uses accent color (typically blue for dark themes)
    // Check if background is an accent/highlight color (not gray/neutral)
    const isAccentColor = (
      Math.abs(buttonBgRgb.r - buttonBgRgb.g) > 20 ||
      Math.abs(buttonBgRgb.g - buttonBgRgb.b) > 20 ||
      Math.abs(buttonBgRgb.r - buttonBgRgb.b) > 20 ||
      buttonBgRgb.r > 100 || buttonBgRgb.g > 100 || buttonBgRgb.b > 100
    );

    expect(
      isAccentColor,
      `CTA button should use an accent color, got rgb(${buttonBgRgb.r}, ${buttonBgRgb.g}, ${buttonBgRgb.b})`
    ).toBe(true);
  });

  test('code blocks have appropriate dark theme syntax highlighting', async ({ page }) => {
    // Scroll to quickstart section to see code blocks
    await page.locator('#quickstart').scrollIntoViewIfNeeded();

    // Verify code block background is dark
    const codeBlock = page.locator('.code-block').first();
    await expect(codeBlock).toBeVisible();

    const codeBlockStyles = await codeBlock.evaluate((el) => {
      const computed = window.getComputedStyle(el);
      // Walk up the DOM to find the actual background color
      let current = el;
      let bgColor = computed.backgroundColor;
      while (current && (bgColor === 'rgba(0, 0, 0, 0)' || bgColor === 'transparent')) {
        current = current.parentElement;
        if (current) {
          bgColor = window.getComputedStyle(current).backgroundColor;
        }
      }
      return {
        backgroundColor: bgColor || 'rgba(0, 0, 0, 0)',
        color: computed.color,
      };
    });

    // Parse and verify code block background is dark
    const codeBgRgb = parseRgb(codeBlockStyles.backgroundColor);
    const codeBgLuminance = getLuminance(codeBgRgb);

    expect(
      codeBgLuminance,
      `Code block background should be dark (luminance < 0.15), got ${codeBgLuminance.toFixed(3)}`
    ).toBeLessThan(0.15);

    // Verify code text has good contrast
    const codeTextRgb = parseRgb(codeBlockStyles.color);
    const codeTextLuminance = getLuminance(codeTextRgb);

    expect(
      codeTextLuminance,
      `Code text should be light (luminance > 0.7), got ${codeTextLuminance.toFixed(3)}`
    ).toBeGreaterThan(0.7);

    // Check syntax highlighting colors
    const syntaxElements = [
      { selector: '.code-comment', name: 'comment', expectedLuminance: { min: 0.15, max: 0.6 } },
      { selector: '.code-keyword', name: 'keyword', expectedLuminance: { min: 0.15, max: 0.8 } },
      { selector: '.code-number', name: 'number', expectedLuminance: { min: 0.2, max: 0.8 } },
      { selector: '.code-response', name: 'response', expectedLuminance: { min: 0.2, max: 0.8 } },
    ];

    for (const { selector, name, expectedLuminance } of syntaxElements) {
      const element = page.locator(selector).first();
      const isVisible = await element.isVisible().catch(() => false);

      if (isVisible) {
        const syntaxColor = await element.evaluate((el) => {
          return window.getComputedStyle(el).color;
        });

        const syntaxRgb = parseRgb(syntaxColor);
        const syntaxLuminance = getLuminance(syntaxRgb);

        // Verify syntax color has appropriate luminance for readability
        expect(
          syntaxLuminance,
          `${name} syntax color should have luminance between ${expectedLuminance.min} and ${expectedLuminance.max}, got ${syntaxLuminance.toFixed(3)}`
        ).toBeGreaterThanOrEqual(expectedLuminance.min);
        expect(
          syntaxLuminance,
          `${name} syntax color should not be too bright (max ${expectedLuminance.max}), got ${syntaxLuminance.toFixed(3)}`
        ).toBeLessThanOrEqual(expectedLuminance.max);

        // Verify contrast against code block background
        const lighter = Math.max(syntaxLuminance, codeBgLuminance);
        const darker = Math.min(syntaxLuminance, codeBgLuminance);
        const contrastRatio = (lighter + 0.05) / (darker + 0.05);

        expect(
          contrastRatio,
          `${name} syntax should have contrast >= ${WCAG_AA_NORMAL_TEXT}:1 against code background, got ${contrastRatio.toFixed(2)}:1`
        ).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
      }
    }
  });

  test('all major sections maintain dark theme consistency', async ({ page }) => {
    // Define sections to check
    const sections = [
      { selector: 'body', name: 'Body' },
      { selector: '#hero', name: 'Hero' },
      { selector: '#features', name: 'Features' },
      { selector: '#quickstart', name: 'Quick Start' },
      { selector: '#status', name: 'Status' },
    ];

    for (const { selector, name } of sections) {
      const section = page.locator(selector);
      await section.scrollIntoViewIfNeeded();

      // Get the effective background color (may be inherited)
      const bgColor = await section.evaluate((el) => {
        let current = el;
        while (current) {
          const style = window.getComputedStyle(current);
          const bg = style.backgroundColor;
          if (bg !== 'rgba(0, 0, 0, 0)' && bg !== 'transparent') {
            return bg;
          }
          current = current.parentElement;
        }
        return 'rgb(13, 17, 23)'; // Default dark theme
      });

      const bgRgb = parseRgb(bgColor);
      const luminance = getLuminance(bgRgb);

      // All sections should have dark backgrounds (luminance < 0.2)
      expect(
        luminance,
        `${name} section should have dark background (luminance < 0.2), got ${luminance.toFixed(3)}`
      ).toBeLessThan(0.2);
    }
  });

  test('dark theme CSS custom properties are correctly defined', async ({ page }) => {
    // Get CSS custom property values from :root
    const cssVars = await page.evaluate(() => {
      const root = document.documentElement;
      const styles = getComputedStyle(root);
      return {
        bgPrimary: styles.getPropertyValue('--color-bg-primary').trim(),
        bgSecondary: styles.getPropertyValue('--color-bg-secondary').trim(),
        bgTertiary: styles.getPropertyValue('--color-bg-tertiary').trim(),
        textPrimary: styles.getPropertyValue('--color-text-primary').trim(),
        textSecondary: styles.getPropertyValue('--color-text-secondary').trim(),
        accent: styles.getPropertyValue('--color-accent').trim(),
        codeBg: styles.getPropertyValue('--color-code-bg').trim(),
        codeText: styles.getPropertyValue('--color-code-text').trim(),
      };
    });

    // Verify all required CSS variables are defined
    expect(cssVars.bgPrimary, 'Primary background color should be defined').toBeTruthy();
    expect(cssVars.bgSecondary, 'Secondary background color should be defined').toBeTruthy();
    expect(cssVars.textPrimary, 'Primary text color should be defined').toBeTruthy();
    expect(cssVars.accent, 'Accent color should be defined').toBeTruthy();
    expect(cssVars.codeBg, 'Code background color should be defined').toBeTruthy();
    expect(cssVars.codeText, 'Code text color should be defined').toBeTruthy();

    // Verify primary background is a hex color starting with # and is dark (low values)
    // Check hex format
    expect(cssVars.bgPrimary, 'Primary background should be a hex color').toMatch(/^#[0-9a-fA-F]{6}$/);

    // Parse hex to verify it's dark
    const bgHex = cssVars.bgPrimary.replace('#', '');
    const bgR = parseInt(bgHex.substring(0, 2), 16);
    const bgG = parseInt(bgHex.substring(2, 4), 16);
    const bgB = parseInt(bgHex.substring(4, 6), 16);

    // Dark colors should have low RGB values
    expect(bgR, 'Background red channel should be low').toBeLessThan(50);
    expect(bgG, 'Background green channel should be low').toBeLessThan(50);
    expect(bgB, 'Background blue channel should be low').toBeLessThan(50);

    // Verify primary text is a hex color and is light (high values)
    expect(cssVars.textPrimary, 'Primary text should be a hex color').toMatch(/^#[0-9a-fA-F]{6}$/);

    const textHex = cssVars.textPrimary.replace('#', '');
    const textR = parseInt(textHex.substring(0, 2), 16);
    const textG = parseInt(textHex.substring(2, 4), 16);
    const textB = parseInt(textHex.substring(4, 6), 16);

    // Light colors should have high RGB values
    expect(textR, 'Text red channel should be high').toBeGreaterThan(200);
    expect(textG, 'Text green channel should be high').toBeGreaterThan(200);
    expect(textB, 'Text blue channel should be high').toBeGreaterThan(200);
  });
});
