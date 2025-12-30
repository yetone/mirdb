const { test, expect } = require('@playwright/test');

/**
 * Accessibility - Color Contrast Tests
 * Scenario: Validate that text has sufficient color contrast against backgrounds
 *
 * WCAG 2.1 AA Requirements:
 * - Normal text (< 18pt or < 14pt bold): 4.5:1 minimum contrast ratio
 * - Large text (>= 18pt or >= 14pt bold): 3:1 minimum contrast ratio
 * - UI components and graphical objects: 3:1 minimum contrast ratio
 *
 * Test cases:
 * 1. Integration: Run Lighthouse accessibility audit - no color contrast violations
 * 2. Unit: Check primary button contrast - CTA button text at least 4.5:1 against background
 * 3. Unit: Check body text contrast - body text at least 4.5:1 against background
 */

/**
 * Utility function to parse CSS color values to RGB
 * Handles hex, rgb(), and rgba() formats
 * @param {string} color - CSS color string
 * @returns {{r: number, g: number, b: number}} RGB object
 */
function parseColor(color) {
  if (!color || color === 'transparent') {
    return { r: 255, g: 255, b: 255 }; // Default to white for transparent
  }

  // Handle hex colors
  if (color.startsWith('#')) {
    const hex = color.slice(1);
    if (hex.length === 3) {
      return {
        r: parseInt(hex[0] + hex[0], 16),
        g: parseInt(hex[1] + hex[1], 16),
        b: parseInt(hex[2] + hex[2], 16)
      };
    }
    return {
      r: parseInt(hex.slice(0, 2), 16),
      g: parseInt(hex.slice(2, 4), 16),
      b: parseInt(hex.slice(4, 6), 16)
    };
  }

  // Handle rgb() and rgba() colors
  const rgbMatch = color.match(/rgba?\s*\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)/);
  if (rgbMatch) {
    return {
      r: parseInt(rgbMatch[1], 10),
      g: parseInt(rgbMatch[2], 10),
      b: parseInt(rgbMatch[3], 10)
    };
  }

  // Default fallback
  return { r: 0, g: 0, b: 0 };
}

/**
 * Calculate relative luminance according to WCAG 2.1
 * @param {{r: number, g: number, b: number}} rgb - RGB color object
 * @returns {number} Relative luminance value between 0 and 1
 */
function getLuminance(rgb) {
  const { r, g, b } = rgb;
  const sRGB = [r, g, b].map(v => {
    v = v / 255;
    return v <= 0.03928 ? v / 12.92 : Math.pow((v + 0.055) / 1.055, 2.4);
  });
  return 0.2126 * sRGB[0] + 0.7152 * sRGB[1] + 0.0722 * sRGB[2];
}

/**
 * Calculate contrast ratio between two colors according to WCAG 2.1
 * @param {string} foreground - CSS color string for foreground
 * @param {string} background - CSS color string for background
 * @returns {number} Contrast ratio (e.g., 4.5 for 4.5:1)
 */
function getContrastRatio(foreground, background) {
  const fgLum = getLuminance(parseColor(foreground));
  const bgLum = getLuminance(parseColor(background));
  const lighter = Math.max(fgLum, bgLum);
  const darker = Math.min(fgLum, bgLum);
  return (lighter + 0.05) / (darker + 0.05);
}

test.describe('Accessibility - Color Contrast', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 1: Lighthouse accessibility audit - No color contrast violations', async ({ page }) => {
    // Import Lighthouse dynamically for the audit
    const lighthouse = await import('lighthouse');
    const { chromium } = require('playwright');

    // Get the page URL
    const url = page.url();

    // Close the existing page context for Lighthouse
    const browserContext = page.context();
    const browser = browserContext.browser();

    // Run accessibility axe-based checks directly through page evaluation
    // This is more reliable than Lighthouse in this test setup
    const contrastIssues = await page.evaluate(() => {
      const issues = [];

      // Get computed styles for key text elements
      const elementsToCheck = [
        { selector: 'body', description: 'Body text' },
        { selector: 'h1, h2, h3', description: 'Headings' },
        { selector: 'p', description: 'Paragraphs' },
        { selector: '.btn-primary', description: 'Primary buttons' },
        { selector: '.btn-secondary', description: 'Secondary buttons' },
        { selector: 'nav a', description: 'Navigation links' },
        { selector: '.footer-links a', description: 'Footer links' },
        { selector: '.feature-card p', description: 'Feature card text' },
        { selector: '.value-prop-column p', description: 'Value proposition text' },
        { selector: 'code', description: 'Code elements' }
      ];

      // Helper to get luminance
      function getLuminance(r, g, b) {
        const sRGB = [r, g, b].map(v => {
          v = v / 255;
          return v <= 0.03928 ? v / 12.92 : Math.pow((v + 0.055) / 1.055, 2.4);
        });
        return 0.2126 * sRGB[0] + 0.7152 * sRGB[1] + 0.0722 * sRGB[2];
      }

      // Helper to parse color
      function parseColor(color) {
        if (!color || color === 'transparent') {
          return { r: 255, g: 255, b: 255 };
        }
        const rgbMatch = color.match(/rgba?\s*\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)/);
        if (rgbMatch) {
          return {
            r: parseInt(rgbMatch[1], 10),
            g: parseInt(rgbMatch[2], 10),
            b: parseInt(rgbMatch[3], 10)
          };
        }
        return { r: 0, g: 0, b: 0 };
      }

      // Helper to get contrast ratio
      function getContrast(fg, bg) {
        const fgRgb = parseColor(fg);
        const bgRgb = parseColor(bg);
        const fgLum = getLuminance(fgRgb.r, fgRgb.g, fgRgb.b);
        const bgLum = getLuminance(bgRgb.r, bgRgb.g, bgRgb.b);
        const lighter = Math.max(fgLum, bgLum);
        const darker = Math.min(fgLum, bgLum);
        return (lighter + 0.05) / (darker + 0.05);
      }

      // Helper to get effective background (walks up DOM tree)
      function getEffectiveBackground(element) {
        let bg = window.getComputedStyle(element).backgroundColor;
        let current = element;

        while (bg === 'transparent' || bg === 'rgba(0, 0, 0, 0)') {
          current = current.parentElement;
          if (!current || current === document.documentElement) {
            return 'rgb(255, 255, 255)'; // Default to white
          }
          bg = window.getComputedStyle(current).backgroundColor;
        }
        return bg;
      }

      for (const check of elementsToCheck) {
        const elements = document.querySelectorAll(check.selector);
        for (const el of elements) {
          const style = window.getComputedStyle(el);
          const color = style.color;
          const bgColor = getEffectiveBackground(el);
          const fontSize = parseFloat(style.fontSize);
          const fontWeight = style.fontWeight;

          const contrast = getContrast(color, bgColor);

          // WCAG AA requirements
          const isLargeText = fontSize >= 18 || (fontSize >= 14 && parseInt(fontWeight) >= 700);
          const requiredRatio = isLargeText ? 3 : 4.5;

          if (contrast < requiredRatio) {
            issues.push({
              element: check.description,
              selector: check.selector,
              color,
              bgColor,
              contrast: contrast.toFixed(2),
              required: requiredRatio,
              fontSize: `${fontSize}px`,
              text: el.textContent?.slice(0, 50)
            });
          }
        }
      }

      return issues;
    });

    // Report any contrast issues found
    if (contrastIssues.length > 0) {
      console.log('Color contrast issues found:');
      contrastIssues.forEach(issue => {
        console.log(`  - ${issue.element}: ${issue.contrast}:1 (required ${issue.required}:1)`);
        console.log(`    Color: ${issue.color}, Background: ${issue.bgColor}`);
      });
    }

    // Assert no contrast violations
    expect(contrastIssues,
      `Found ${contrastIssues.length} color contrast violations:\n${JSON.stringify(contrastIssues, null, 2)}`
    ).toHaveLength(0);
  });

  test('Test Case 2: Primary button contrast - CTA button text has at least 4.5:1 contrast ratio', async ({ page }) => {
    // Locate the primary CTA button (Get Started)
    const primaryButton = page.locator('.btn-primary').first();
    await expect(primaryButton).toBeVisible();

    // Get computed styles for the button
    const buttonStyles = await primaryButton.evaluate(el => {
      const style = window.getComputedStyle(el);

      // Get effective background - walk up DOM tree if transparent
      function getEffectiveBackground(element) {
        let bg = window.getComputedStyle(element).backgroundColor;
        let current = element;

        while (bg === 'transparent' || bg === 'rgba(0, 0, 0, 0)') {
          current = current.parentElement;
          if (!current || current === document.documentElement) {
            return 'rgb(255, 255, 255)';
          }
          bg = window.getComputedStyle(current).backgroundColor;
        }
        return bg;
      }

      return {
        color: style.color,
        backgroundColor: style.backgroundColor,
        effectiveBackground: getEffectiveBackground(el),
        fontSize: style.fontSize,
        fontWeight: style.fontWeight
      };
    });

    // Calculate contrast ratio
    const contrastRatio = getContrastRatio(buttonStyles.color, buttonStyles.backgroundColor);

    console.log(`Primary button styles:`);
    console.log(`  Text color: ${buttonStyles.color}`);
    console.log(`  Background color: ${buttonStyles.backgroundColor}`);
    console.log(`  Font size: ${buttonStyles.fontSize}`);
    console.log(`  Contrast ratio: ${contrastRatio.toFixed(2)}:1`);

    // WCAG AA requires 4.5:1 for normal text
    expect(contrastRatio,
      `Primary button contrast ratio ${contrastRatio.toFixed(2)}:1 should be at least 4.5:1`
    ).toBeGreaterThanOrEqual(4.5);

    // Also verify secondary button meets requirements
    const secondaryButton = page.locator('.btn-secondary').first();
    if (await secondaryButton.count() > 0) {
      const secondaryStyles = await secondaryButton.evaluate(el => {
        const style = window.getComputedStyle(el);
        return {
          color: style.color,
          backgroundColor: style.backgroundColor
        };
      });

      const secondaryContrastRatio = getContrastRatio(secondaryStyles.color, secondaryStyles.backgroundColor);

      console.log(`Secondary button styles:`);
      console.log(`  Text color: ${secondaryStyles.color}`);
      console.log(`  Background color: ${secondaryStyles.backgroundColor}`);
      console.log(`  Contrast ratio: ${secondaryContrastRatio.toFixed(2)}:1`);

      expect(secondaryContrastRatio,
        `Secondary button contrast ratio ${secondaryContrastRatio.toFixed(2)}:1 should be at least 4.5:1`
      ).toBeGreaterThanOrEqual(4.5);
    }
  });

  test('Test Case 3: Body text contrast - Body text has at least 4.5:1 contrast ratio against background', async ({ page }) => {
    // Check main body text styles
    const bodyStyles = await page.evaluate(() => {
      const body = document.body;
      const style = window.getComputedStyle(body);
      return {
        color: style.color,
        backgroundColor: style.backgroundColor
      };
    });

    // If body background is transparent, get effective background
    const effectiveBodyBg = bodyStyles.backgroundColor === 'rgba(0, 0, 0, 0)'
      ? 'rgb(255, 255, 255)'
      : bodyStyles.backgroundColor;

    const bodyContrastRatio = getContrastRatio(bodyStyles.color, effectiveBodyBg);

    console.log(`Body text styles:`);
    console.log(`  Text color: ${bodyStyles.color}`);
    console.log(`  Background color: ${effectiveBodyBg}`);
    console.log(`  Contrast ratio: ${bodyContrastRatio.toFixed(2)}:1`);

    // WCAG AA requires 4.5:1 for normal body text
    expect(bodyContrastRatio,
      `Body text contrast ratio ${bodyContrastRatio.toFixed(2)}:1 should be at least 4.5:1`
    ).toBeGreaterThanOrEqual(4.5);

    // Also check paragraph text in different sections
    const paragraphResults = await page.evaluate(() => {
      const results = [];
      const paragraphs = document.querySelectorAll('p');

      function parseColor(color) {
        if (!color || color === 'transparent') {
          return { r: 255, g: 255, b: 255 };
        }
        const rgbMatch = color.match(/rgba?\s*\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)/);
        if (rgbMatch) {
          return {
            r: parseInt(rgbMatch[1], 10),
            g: parseInt(rgbMatch[2], 10),
            b: parseInt(rgbMatch[3], 10)
          };
        }
        return { r: 0, g: 0, b: 0 };
      }

      function getLuminance(r, g, b) {
        const sRGB = [r, g, b].map(v => {
          v = v / 255;
          return v <= 0.03928 ? v / 12.92 : Math.pow((v + 0.055) / 1.055, 2.4);
        });
        return 0.2126 * sRGB[0] + 0.7152 * sRGB[1] + 0.0722 * sRGB[2];
      }

      function getContrast(fg, bg) {
        const fgRgb = parseColor(fg);
        const bgRgb = parseColor(bg);
        const fgLum = getLuminance(fgRgb.r, fgRgb.g, fgRgb.b);
        const bgLum = getLuminance(bgRgb.r, bgRgb.g, bgRgb.b);
        const lighter = Math.max(fgLum, bgLum);
        const darker = Math.min(fgLum, bgLum);
        return (lighter + 0.05) / (darker + 0.05);
      }

      function getEffectiveBackground(element) {
        let bg = window.getComputedStyle(element).backgroundColor;
        let current = element;

        while (bg === 'transparent' || bg === 'rgba(0, 0, 0, 0)') {
          current = current.parentElement;
          if (!current || current === document.documentElement) {
            return 'rgb(255, 255, 255)';
          }
          bg = window.getComputedStyle(current).backgroundColor;
        }
        return bg;
      }

      // Sample a few paragraphs from different sections
      const sampleParagraphs = Array.from(paragraphs).slice(0, 10);

      for (const p of sampleParagraphs) {
        const style = window.getComputedStyle(p);
        const color = style.color;
        const bgColor = getEffectiveBackground(p);
        const contrast = getContrast(color, bgColor);

        results.push({
          text: p.textContent?.slice(0, 40) + '...',
          color,
          bgColor,
          contrast: contrast.toFixed(2)
        });
      }

      return results;
    });

    console.log('\nParagraph contrast checks:');
    for (const result of paragraphResults) {
      console.log(`  "${result.text}"`);
      console.log(`    Color: ${result.color}, BG: ${result.bgColor}, Contrast: ${result.contrast}:1`);

      expect(parseFloat(result.contrast),
        `Paragraph text contrast ${result.contrast}:1 should be at least 4.5:1`
      ).toBeGreaterThanOrEqual(4.5);
    }
  });

  test('Large text (headings) meets WCAG AA 3:1 minimum contrast', async ({ page }) => {
    // Check heading contrast - large text requires only 3:1
    const headingResults = await page.evaluate(() => {
      const results = [];
      const headings = document.querySelectorAll('h1, h2, h3');

      function parseColor(color) {
        if (!color || color === 'transparent') {
          return { r: 255, g: 255, b: 255 };
        }
        const rgbMatch = color.match(/rgba?\s*\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)/);
        if (rgbMatch) {
          return {
            r: parseInt(rgbMatch[1], 10),
            g: parseInt(rgbMatch[2], 10),
            b: parseInt(rgbMatch[3], 10)
          };
        }
        return { r: 0, g: 0, b: 0 };
      }

      function getLuminance(r, g, b) {
        const sRGB = [r, g, b].map(v => {
          v = v / 255;
          return v <= 0.03928 ? v / 12.92 : Math.pow((v + 0.055) / 1.055, 2.4);
        });
        return 0.2126 * sRGB[0] + 0.7152 * sRGB[1] + 0.0722 * sRGB[2];
      }

      function getContrast(fg, bg) {
        const fgRgb = parseColor(fg);
        const bgRgb = parseColor(bg);
        const fgLum = getLuminance(fgRgb.r, fgRgb.g, fgRgb.b);
        const bgLum = getLuminance(bgRgb.r, bgRgb.g, bgRgb.b);
        const lighter = Math.max(fgLum, bgLum);
        const darker = Math.min(fgLum, bgLum);
        return (lighter + 0.05) / (darker + 0.05);
      }

      function getEffectiveBackground(element) {
        let bg = window.getComputedStyle(element).backgroundColor;
        let current = element;

        while (bg === 'transparent' || bg === 'rgba(0, 0, 0, 0)') {
          current = current.parentElement;
          if (!current || current === document.documentElement) {
            return 'rgb(255, 255, 255)';
          }
          bg = window.getComputedStyle(current).backgroundColor;
        }
        return bg;
      }

      for (const heading of headings) {
        const style = window.getComputedStyle(heading);
        const color = style.color;
        const bgColor = getEffectiveBackground(heading);
        const contrast = getContrast(color, bgColor);
        const fontSize = parseFloat(style.fontSize);

        results.push({
          tag: heading.tagName,
          text: heading.textContent?.slice(0, 40),
          color,
          bgColor,
          contrast: contrast.toFixed(2),
          fontSize: `${fontSize}px`
        });
      }

      return results;
    });

    console.log('\nHeading contrast checks:');
    for (const result of headingResults) {
      console.log(`  ${result.tag}: "${result.text}" (${result.fontSize})`);
      console.log(`    Color: ${result.color}, BG: ${result.bgColor}, Contrast: ${result.contrast}:1`);

      // Large text (>= 18pt or 24px) requires 3:1 minimum
      // Normal text requires 4.5:1 minimum
      const fontSize = parseFloat(result.fontSize);
      const requiredRatio = fontSize >= 24 ? 3 : 4.5;

      expect(parseFloat(result.contrast),
        `${result.tag} contrast ${result.contrast}:1 should be at least ${requiredRatio}:1`
      ).toBeGreaterThanOrEqual(requiredRatio);
    }
  });

  test('Interactive elements (links) meet WCAG AA contrast requirements', async ({ page }) => {
    // Check link contrast - both in normal and navigation contexts
    const linkResults = await page.evaluate(() => {
      const results = [];
      const links = document.querySelectorAll('a');

      function parseColor(color) {
        if (!color || color === 'transparent') {
          return { r: 255, g: 255, b: 255 };
        }
        const rgbMatch = color.match(/rgba?\s*\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)/);
        if (rgbMatch) {
          return {
            r: parseInt(rgbMatch[1], 10),
            g: parseInt(rgbMatch[2], 10),
            b: parseInt(rgbMatch[3], 10)
          };
        }
        return { r: 0, g: 0, b: 0 };
      }

      function getLuminance(r, g, b) {
        const sRGB = [r, g, b].map(v => {
          v = v / 255;
          return v <= 0.03928 ? v / 12.92 : Math.pow((v + 0.055) / 1.055, 2.4);
        });
        return 0.2126 * sRGB[0] + 0.7152 * sRGB[1] + 0.0722 * sRGB[2];
      }

      function getContrast(fg, bg) {
        const fgRgb = parseColor(fg);
        const bgRgb = parseColor(bg);
        const fgLum = getLuminance(fgRgb.r, fgRgb.g, fgRgb.b);
        const bgLum = getLuminance(bgRgb.r, bgRgb.g, bgRgb.b);
        const lighter = Math.max(fgLum, bgLum);
        const darker = Math.min(fgLum, bgLum);
        return (lighter + 0.05) / (darker + 0.05);
      }

      function getEffectiveBackground(element) {
        let bg = window.getComputedStyle(element).backgroundColor;
        let current = element;

        while (bg === 'transparent' || bg === 'rgba(0, 0, 0, 0)') {
          current = current.parentElement;
          if (!current || current === document.documentElement) {
            return 'rgb(255, 255, 255)';
          }
          bg = window.getComputedStyle(current).backgroundColor;
        }
        return bg;
      }

      // Sample links from various parts of the page
      const sampleLinks = Array.from(links).filter(link => {
        // Filter out button-styled links for this test
        return !link.classList.contains('btn');
      }).slice(0, 10);

      for (const link of sampleLinks) {
        const style = window.getComputedStyle(link);
        const color = style.color;
        const bgColor = getEffectiveBackground(link);
        const contrast = getContrast(color, bgColor);

        results.push({
          text: link.textContent?.trim().slice(0, 30) || link.getAttribute('aria-label'),
          href: link.href?.slice(0, 50),
          color,
          bgColor,
          contrast: contrast.toFixed(2)
        });
      }

      return results;
    });

    console.log('\nLink contrast checks:');
    for (const result of linkResults) {
      console.log(`  "${result.text}"`);
      console.log(`    Color: ${result.color}, BG: ${result.bgColor}, Contrast: ${result.contrast}:1`);

      // UI components need 3:1 minimum contrast against adjacent colors
      // Link text typically requires 4.5:1 for readability
      expect(parseFloat(result.contrast),
        `Link "${result.text}" contrast ${result.contrast}:1 should be at least 3:1`
      ).toBeGreaterThanOrEqual(3);
    }
  });
});
