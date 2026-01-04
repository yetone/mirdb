/**
 * Accessibility Tests: Color Contrast
 * Tests NFR-3: WCAG 2.1 AA compliance - color contrast requirements
 * WCAG 2.1 AA requires:
 * - 4.5:1 contrast ratio for normal text (< 18pt or < 14pt bold)
 * - 3:1 contrast ratio for large text (>= 18pt or >= 14pt bold)
 */
const { test, expect } = require('@playwright/test');

/**
 * Calculate relative luminance of an RGB color
 * Based on WCAG 2.1 formula: https://www.w3.org/TR/WCAG21/#dfn-relative-luminance
 */
function getRelativeLuminance(r, g, b) {
  const [rs, gs, bs] = [r, g, b].map(c => {
    c = c / 255;
    return c <= 0.03928 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4);
  });
  return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
}

/**
 * Calculate contrast ratio between two colors
 * Based on WCAG 2.1 formula: https://www.w3.org/TR/WCAG21/#dfn-contrast-ratio
 */
function getContrastRatio(rgb1, rgb2) {
  const l1 = getRelativeLuminance(rgb1.r, rgb1.g, rgb1.b);
  const l2 = getRelativeLuminance(rgb2.r, rgb2.g, rgb2.b);
  const lighter = Math.max(l1, l2);
  const darker = Math.min(l1, l2);
  return (lighter + 0.05) / (darker + 0.05);
}

/**
 * Parse RGB color string to object
 */
function parseRgb(rgbString) {
  // Handle rgba and rgb formats
  const match = rgbString.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/);
  if (!match) return null;
  return {
    r: parseInt(match[1], 10),
    g: parseInt(match[2], 10),
    b: parseInt(match[3], 10)
  };
}

/**
 * Check if text is considered "large" per WCAG
 * Large text: 18pt+ (24px) or 14pt+ bold (18.66px)
 */
function isLargeText(fontSize, fontWeight) {
  const size = parseFloat(fontSize);
  const weight = parseInt(fontWeight, 10);
  const isBold = weight >= 700;

  // 18pt = 24px, 14pt ≈ 18.67px
  return size >= 24 || (isBold && size >= 18.67);
}

test.describe('Accessibility - Color Contrast', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('Test Case 1: Body Text Contrast', () => {
    test('Body text has minimum 4.5:1 contrast ratio with background', async ({ page }) => {
      // Test paragraph text in various sections
      const paragraphs = await page.locator('p').all();
      const results = [];

      for (const p of paragraphs) {
        const isVisible = await p.isVisible();
        if (!isVisible) continue;

        const { color, bgColor, fontSize, fontWeight, text } = await p.evaluate(el => {
          const style = window.getComputedStyle(el);
          // Get the effective background by traversing up the DOM
          let currentEl = el;
          let bg = style.backgroundColor;
          while (currentEl && (bg === 'rgba(0, 0, 0, 0)' || bg === 'transparent')) {
            currentEl = currentEl.parentElement;
            if (currentEl) {
              bg = window.getComputedStyle(currentEl).backgroundColor;
            }
          }
          return {
            color: style.color,
            bgColor: bg || 'rgb(255, 255, 255)',
            fontSize: style.fontSize,
            fontWeight: style.fontWeight,
            text: el.textContent.slice(0, 50)
          };
        });

        const fgRgb = parseRgb(color);
        const bgRgb = parseRgb(bgColor);

        if (fgRgb && bgRgb) {
          const ratio = getContrastRatio(fgRgb, bgRgb);
          const isLarge = isLargeText(fontSize, fontWeight);
          const requiredRatio = isLarge ? 3.0 : 4.5;

          results.push({
            text,
            ratio: ratio.toFixed(2),
            required: requiredRatio,
            passes: ratio >= requiredRatio,
            isLarge
          });
        }
      }

      // All paragraphs should pass contrast requirements
      const failingElements = results.filter(r => !r.passes);
      expect(failingElements, `Body text failing contrast: ${JSON.stringify(failingElements)}`).toHaveLength(0);
    });

    test('Description text in hero section meets contrast requirements', async ({ page }) => {
      const description = page.locator('.hero .description');
      await expect(description).toBeVisible();

      const { color, bgColor, fontSize, fontWeight } = await description.evaluate(el => {
        const style = window.getComputedStyle(el);
        let currentEl = el;
        let bg = style.backgroundColor;
        while (currentEl && (bg === 'rgba(0, 0, 0, 0)' || bg === 'transparent')) {
          currentEl = currentEl.parentElement;
          if (currentEl) {
            bg = window.getComputedStyle(currentEl).backgroundColor;
          }
        }
        return {
          color: style.color,
          bgColor: bg || 'rgb(255, 255, 255)',
          fontSize: style.fontSize,
          fontWeight: style.fontWeight
        };
      });

      const fgRgb = parseRgb(color);
      const bgRgb = parseRgb(bgColor);

      expect(fgRgb).not.toBeNull();
      expect(bgRgb).not.toBeNull();

      const ratio = getContrastRatio(fgRgb, bgRgb);
      const isLarge = isLargeText(fontSize, fontWeight);
      const requiredRatio = isLarge ? 3.0 : 4.5;

      expect(ratio, `Description text contrast ratio (${ratio.toFixed(2)}) should be at least ${requiredRatio}:1`).toBeGreaterThanOrEqual(requiredRatio);
    });

    test('Feature card description text meets contrast requirements', async ({ page }) => {
      const featureDescriptions = await page.locator('.feature-card p').all();

      for (const desc of featureDescriptions) {
        const isVisible = await desc.isVisible();
        if (!isVisible) continue;

        const { color, bgColor, fontSize, fontWeight, text } = await desc.evaluate(el => {
          const style = window.getComputedStyle(el);
          let currentEl = el;
          let bg = style.backgroundColor;
          while (currentEl && (bg === 'rgba(0, 0, 0, 0)' || bg === 'transparent')) {
            currentEl = currentEl.parentElement;
            if (currentEl) {
              bg = window.getComputedStyle(currentEl).backgroundColor;
            }
          }
          return {
            color: style.color,
            bgColor: bg || 'rgb(255, 255, 255)',
            fontSize: style.fontSize,
            fontWeight: style.fontWeight,
            text: el.textContent.slice(0, 30)
          };
        });

        const fgRgb = parseRgb(color);
        const bgRgb = parseRgb(bgColor);

        if (fgRgb && bgRgb) {
          const ratio = getContrastRatio(fgRgb, bgRgb);
          const isLarge = isLargeText(fontSize, fontWeight);
          const requiredRatio = isLarge ? 3.0 : 4.5;

          expect(ratio, `Feature card text "${text}" contrast (${ratio.toFixed(2)}) should be >= ${requiredRatio}:1`).toBeGreaterThanOrEqual(requiredRatio);
        }
      }
    });
  });

  test.describe('Test Case 2: Heading Contrast', () => {
    test('Large headings have minimum 3:1 contrast ratio', async ({ page }) => {
      const headings = await page.locator('h1, h2, h3').all();
      const results = [];

      for (const heading of headings) {
        const isVisible = await heading.isVisible();
        if (!isVisible) continue;

        const { color, bgColor, fontSize, fontWeight, tagName, text } = await heading.evaluate(el => {
          const style = window.getComputedStyle(el);
          let currentEl = el;
          let bg = style.backgroundColor;
          while (currentEl && (bg === 'rgba(0, 0, 0, 0)' || bg === 'transparent')) {
            currentEl = currentEl.parentElement;
            if (currentEl) {
              bg = window.getComputedStyle(currentEl).backgroundColor;
            }
          }
          return {
            color: style.color,
            bgColor: bg || 'rgb(255, 255, 255)',
            fontSize: style.fontSize,
            fontWeight: style.fontWeight,
            tagName: el.tagName,
            text: el.textContent.slice(0, 30)
          };
        });

        const fgRgb = parseRgb(color);
        const bgRgb = parseRgb(bgColor);

        if (fgRgb && bgRgb) {
          const ratio = getContrastRatio(fgRgb, bgRgb);
          // Headings are typically large text, so 3:1 minimum
          // But we check based on actual computed size
          const isLarge = isLargeText(fontSize, fontWeight);
          const requiredRatio = isLarge ? 3.0 : 4.5;

          results.push({
            tagName,
            text,
            fontSize,
            ratio: ratio.toFixed(2),
            required: requiredRatio,
            passes: ratio >= requiredRatio
          });
        }
      }

      const failingHeadings = results.filter(r => !r.passes);
      expect(failingHeadings, `Headings failing contrast: ${JSON.stringify(failingHeadings)}`).toHaveLength(0);
    });

    test('H1 heading (MirDB) has sufficient contrast', async ({ page }) => {
      const h1 = page.locator('h1');
      await expect(h1).toBeVisible();

      const { color, bgColor, fontSize, fontWeight } = await h1.evaluate(el => {
        const style = window.getComputedStyle(el);
        let currentEl = el;
        let bg = style.backgroundColor;
        while (currentEl && (bg === 'rgba(0, 0, 0, 0)' || bg === 'transparent')) {
          currentEl = currentEl.parentElement;
          if (currentEl) {
            bg = window.getComputedStyle(currentEl).backgroundColor;
          }
        }
        return {
          color: style.color,
          bgColor: bg || 'rgb(255, 255, 255)',
          fontSize: style.fontSize,
          fontWeight: style.fontWeight
        };
      });

      const fgRgb = parseRgb(color);
      const bgRgb = parseRgb(bgColor);

      expect(fgRgb).not.toBeNull();
      expect(bgRgb).not.toBeNull();

      const ratio = getContrastRatio(fgRgb, bgRgb);
      // H1 is definitely large text
      expect(ratio, `H1 contrast ratio (${ratio.toFixed(2)}) should be at least 3:1`).toBeGreaterThanOrEqual(3.0);
    });

    test('Section headings (h2) meet contrast requirements', async ({ page }) => {
      const h2Elements = await page.locator('h2').all();

      for (const h2 of h2Elements) {
        const isVisible = await h2.isVisible();
        if (!isVisible) continue;

        const { color, bgColor, text } = await h2.evaluate(el => {
          const style = window.getComputedStyle(el);
          let currentEl = el;
          let bg = style.backgroundColor;
          while (currentEl && (bg === 'rgba(0, 0, 0, 0)' || bg === 'transparent')) {
            currentEl = currentEl.parentElement;
            if (currentEl) {
              bg = window.getComputedStyle(currentEl).backgroundColor;
            }
          }
          return {
            color: style.color,
            bgColor: bg || 'rgb(255, 255, 255)',
            text: el.textContent
          };
        });

        const fgRgb = parseRgb(color);
        const bgRgb = parseRgb(bgColor);

        if (fgRgb && bgRgb) {
          const ratio = getContrastRatio(fgRgb, bgRgb);
          expect(ratio, `H2 "${text}" contrast (${ratio.toFixed(2)}) should be >= 3:1`).toBeGreaterThanOrEqual(3.0);
        }
      }
    });
  });

  test.describe('Test Case 3: Link Contrast', () => {
    test('Links are distinguishable from surrounding text (color + underline or 3:1 contrast)', async ({ page }) => {
      const navLinks = await page.locator('.nav-links a').all();

      for (const link of navLinks) {
        const isVisible = await link.isVisible();
        if (!isVisible) continue;

        const { color, textDecoration, bgColor } = await link.evaluate(el => {
          const style = window.getComputedStyle(el);
          let currentEl = el;
          let bg = style.backgroundColor;
          while (currentEl && (bg === 'rgba(0, 0, 0, 0)' || bg === 'transparent')) {
            currentEl = currentEl.parentElement;
            if (currentEl) {
              bg = window.getComputedStyle(currentEl).backgroundColor;
            }
          }
          return {
            color: style.color,
            textDecoration: style.textDecorationLine,
            bgColor: bg || 'rgb(255, 255, 255)'
          };
        });

        const fgRgb = parseRgb(color);
        const bgRgb = parseRgb(bgColor);

        if (fgRgb && bgRgb) {
          const ratio = getContrastRatio(fgRgb, bgRgb);
          // Links should either have underline OR have 3:1 contrast with surrounding text
          const hasUnderline = textDecoration.includes('underline');
          const hasGoodContrast = ratio >= 4.5;

          // For non-underlined links, verify they have sufficient contrast with background
          expect(hasUnderline || hasGoodContrast,
            `Nav link should have underline or >= 4.5:1 contrast with background (has ${ratio.toFixed(2)})`
          ).toBe(true);
        }
      }
    });

    test('Footer links meet contrast requirements', async ({ page }) => {
      const footerLinks = await page.locator('.footer-links a').all();

      for (const link of footerLinks) {
        const isVisible = await link.isVisible();
        if (!isVisible) continue;

        const { color, bgColor, text } = await link.evaluate(el => {
          const style = window.getComputedStyle(el);
          let currentEl = el;
          let bg = style.backgroundColor;
          while (currentEl && (bg === 'rgba(0, 0, 0, 0)' || bg === 'transparent')) {
            currentEl = currentEl.parentElement;
            if (currentEl) {
              bg = window.getComputedStyle(currentEl).backgroundColor;
            }
          }
          return {
            color: style.color,
            bgColor: bg || 'rgb(255, 255, 255)',
            text: el.textContent
          };
        });

        const fgRgb = parseRgb(color);
        const bgRgb = parseRgb(bgColor);

        if (fgRgb && bgRgb) {
          const ratio = getContrastRatio(fgRgb, bgRgb);
          // Footer links should have at least 4.5:1 contrast
          expect(ratio, `Footer link "${text}" contrast (${ratio.toFixed(2)}) should be >= 4.5:1`).toBeGreaterThanOrEqual(4.5);
        }
      }
    });

    test('CTA links/buttons in hero have proper contrast', async ({ page }) => {
      const ctaLinks = await page.locator('.cta').all();

      for (const cta of ctaLinks) {
        const isVisible = await cta.isVisible();
        if (!isVisible) continue;

        const { color, bgColor, text, className } = await cta.evaluate(el => {
          const style = window.getComputedStyle(el);
          return {
            color: style.color,
            bgColor: style.backgroundColor,
            text: el.textContent,
            className: el.className
          };
        });

        const fgRgb = parseRgb(color);
        const bgRgb = parseRgb(bgColor);

        if (fgRgb && bgRgb) {
          const ratio = getContrastRatio(fgRgb, bgRgb);
          expect(ratio, `CTA "${text}" contrast (${ratio.toFixed(2)}) should be >= 4.5:1`).toBeGreaterThanOrEqual(4.5);
        }
      }
    });
  });

  test.describe('Test Case 4: Button Contrast', () => {
    test('Button text has sufficient contrast with button background', async ({ page }) => {
      // Test primary CTA button
      const primaryCta = page.locator('.cta-primary');
      await expect(primaryCta).toBeVisible();

      const primaryStyles = await primaryCta.evaluate(el => {
        const style = window.getComputedStyle(el);
        return {
          color: style.color,
          bgColor: style.backgroundColor
        };
      });

      const primaryFg = parseRgb(primaryStyles.color);
      const primaryBg = parseRgb(primaryStyles.bgColor);

      expect(primaryFg).not.toBeNull();
      expect(primaryBg).not.toBeNull();

      const primaryRatio = getContrastRatio(primaryFg, primaryBg);
      expect(primaryRatio, `Primary button contrast (${primaryRatio.toFixed(2)}) should be >= 4.5:1`).toBeGreaterThanOrEqual(4.5);
    });

    test('Secondary button has sufficient contrast', async ({ page }) => {
      const secondaryCta = page.locator('.cta-secondary');
      await expect(secondaryCta).toBeVisible();

      const secondaryStyles = await secondaryCta.evaluate(el => {
        const style = window.getComputedStyle(el);
        return {
          color: style.color,
          bgColor: style.backgroundColor
        };
      });

      const secondaryFg = parseRgb(secondaryStyles.color);
      const secondaryBg = parseRgb(secondaryStyles.bgColor);

      expect(secondaryFg).not.toBeNull();
      expect(secondaryBg).not.toBeNull();

      const secondaryRatio = getContrastRatio(secondaryFg, secondaryBg);
      expect(secondaryRatio, `Secondary button contrast (${secondaryRatio.toFixed(2)}) should be >= 4.5:1`).toBeGreaterThanOrEqual(4.5);
    });

    test('All interactive elements have sufficient contrast', async ({ page }) => {
      // Test all clickable elements (buttons and links styled as buttons)
      const buttons = await page.locator('button, [role="button"], .cta').all();
      const results = [];

      for (const button of buttons) {
        const isVisible = await button.isVisible();
        if (!isVisible) continue;

        const { color, bgColor, text } = await button.evaluate(el => {
          const style = window.getComputedStyle(el);
          return {
            color: style.color,
            bgColor: style.backgroundColor,
            text: el.textContent.slice(0, 30)
          };
        });

        const fgRgb = parseRgb(color);
        const bgRgb = parseRgb(bgColor);

        if (fgRgb && bgRgb) {
          const ratio = getContrastRatio(fgRgb, bgRgb);
          results.push({
            text,
            ratio: ratio.toFixed(2),
            passes: ratio >= 4.5
          });
        }
      }

      const failingButtons = results.filter(r => !r.passes);
      expect(failingButtons, `Buttons failing contrast: ${JSON.stringify(failingButtons)}`).toHaveLength(0);
    });

    test('Code blocks have sufficient text contrast', async ({ page }) => {
      const codeBlocks = await page.locator('.code-example code').all();

      for (const code of codeBlocks) {
        const isVisible = await code.isVisible();
        if (!isVisible) continue;

        const { color, bgColor } = await code.evaluate(el => {
          const style = window.getComputedStyle(el);
          let currentEl = el;
          let bg = style.backgroundColor;
          while (currentEl && (bg === 'rgba(0, 0, 0, 0)' || bg === 'transparent')) {
            currentEl = currentEl.parentElement;
            if (currentEl) {
              bg = window.getComputedStyle(currentEl).backgroundColor;
            }
          }
          return {
            color: style.color,
            bgColor: bg || 'rgb(255, 255, 255)'
          };
        });

        const fgRgb = parseRgb(color);
        const bgRgb = parseRgb(bgColor);

        if (fgRgb && bgRgb) {
          const ratio = getContrastRatio(fgRgb, bgRgb);
          expect(ratio, `Code block contrast (${ratio.toFixed(2)}) should be >= 4.5:1`).toBeGreaterThanOrEqual(4.5);
        }
      }
    });
  });
});
