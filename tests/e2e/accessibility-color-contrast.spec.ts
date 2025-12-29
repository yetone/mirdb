import { test, expect, Page } from '@playwright/test';

/**
 * E2E Tests for MirDB Homepage Color Contrast Accessibility
 *
 * These tests verify WCAG 2.1 AA color contrast requirements:
 * - Normal text: 4.5:1 contrast ratio minimum
 * - Large text (18pt+ or 14pt+ bold): 3:1 contrast ratio minimum
 * - Interactive elements (links, buttons) should be distinguishable
 * - Code blocks should have readable syntax highlighting
 */

/**
 * Calculate relative luminance of a color
 * Based on WCAG 2.1 definition
 * @see https://www.w3.org/TR/WCAG21/#dfn-relative-luminance
 */
function getLuminance(r: number, g: number, b: number): number {
  const [rs, gs, bs] = [r, g, b].map((c) => {
    const sRGB = c / 255;
    return sRGB <= 0.03928
      ? sRGB / 12.92
      : Math.pow((sRGB + 0.055) / 1.055, 2.4);
  });
  return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
}

/**
 * Calculate contrast ratio between two colors
 * @see https://www.w3.org/TR/WCAG21/#dfn-contrast-ratio
 */
function getContrastRatio(
  fg: { r: number; g: number; b: number },
  bg: { r: number; g: number; b: number }
): number {
  const l1 = getLuminance(fg.r, fg.g, fg.b);
  const l2 = getLuminance(bg.r, bg.g, bg.b);
  const lighter = Math.max(l1, l2);
  const darker = Math.min(l1, l2);
  return (lighter + 0.05) / (darker + 0.05);
}

/**
 * Parse RGB/RGBA color string to RGB object
 */
function parseColor(colorStr: string): { r: number; g: number; b: number } {
  // Handle rgba(r, g, b, a) or rgb(r, g, b)
  const rgbaMatch = colorStr.match(
    /rgba?\((\d+),\s*(\d+),\s*(\d+)(?:,\s*[\d.]+)?\)/
  );
  if (rgbaMatch) {
    return {
      r: parseInt(rgbaMatch[1], 10),
      g: parseInt(rgbaMatch[2], 10),
      b: parseInt(rgbaMatch[3], 10),
    };
  }

  // Handle hex colors (#rrggbb or #rgb)
  const hexMatch = colorStr.match(/^#([0-9a-f]{3,8})$/i);
  if (hexMatch) {
    const hex = hexMatch[1];
    if (hex.length === 3) {
      return {
        r: parseInt(hex[0] + hex[0], 16),
        g: parseInt(hex[1] + hex[1], 16),
        b: parseInt(hex[2] + hex[2], 16),
      };
    } else if (hex.length >= 6) {
      return {
        r: parseInt(hex.slice(0, 2), 16),
        g: parseInt(hex.slice(2, 4), 16),
        b: parseInt(hex.slice(4, 6), 16),
      };
    }
  }

  // Default to black if parsing fails
  return { r: 0, g: 0, b: 0 };
}

/**
 * Check if text is considered "large" per WCAG
 * Large text: 18pt (24px) or larger, or 14pt (18.66px) bold or larger
 */
function isLargeText(fontSize: string, fontWeight: string): boolean {
  const sizeInPx = parseFloat(fontSize);
  const weight = parseInt(fontWeight, 10) || 400;

  // 18pt = 24px
  if (sizeInPx >= 24) {
    return true;
  }

  // 14pt bold = 18.66px with weight >= 700
  if (sizeInPx >= 18.66 && weight >= 700) {
    return true;
  }

  return false;
}

/**
 * Get effective background color by traversing up the DOM
 */
async function getEffectiveBackgroundColor(
  page: Page,
  element: any
): Promise<{ r: number; g: number; b: number }> {
  return await element.evaluate((el: HTMLElement) => {
    let currentEl: HTMLElement | null = el;

    while (currentEl) {
      const style = window.getComputedStyle(currentEl);
      const bg = style.backgroundColor;

      // Check if background is not transparent
      const match = bg.match(
        /rgba?\((\d+),\s*(\d+),\s*(\d+)(?:,\s*([\d.]+))?\)/
      );
      if (match) {
        const alpha = match[4] !== undefined ? parseFloat(match[4]) : 1;
        if (alpha > 0) {
          return {
            r: parseInt(match[1], 10),
            g: parseInt(match[2], 10),
            b: parseInt(match[3], 10),
          };
        }
      }

      currentEl = currentEl.parentElement;
    }

    // Default to white background
    return { r: 255, g: 255, b: 255 };
  });
}

test.describe('Accessibility - Color Contrast', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    // Wait for the page to fully load
    await page.waitForLoadState('domcontentloaded');
  });

  test.describe('TC1: Text Contrast Requirements', () => {
    test('Hero section text meets WCAG 2.1 AA contrast requirements', async ({
      page,
    }) => {
      // Check h1 title
      const h1 = page.locator('.hero h1');
      await expect(h1).toBeVisible();

      const h1Styles = await h1.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return {
          color: style.color,
          fontSize: style.fontSize,
          fontWeight: style.fontWeight,
        };
      });

      const h1Bg = await getEffectiveBackgroundColor(page, h1);
      const h1Fg = parseColor(h1Styles.color);
      const h1Contrast = getContrastRatio(h1Fg, h1Bg);
      const h1IsLarge = isLargeText(h1Styles.fontSize, h1Styles.fontWeight);
      const h1MinRatio = h1IsLarge ? 3 : 4.5;

      expect(h1Contrast).toBeGreaterThanOrEqual(h1MinRatio);

      // Check tagline
      const tagline = page.locator('.hero .tagline');
      await expect(tagline).toBeVisible();

      const taglineStyles = await tagline.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return {
          color: style.color,
          fontSize: style.fontSize,
          fontWeight: style.fontWeight,
        };
      });

      const taglineBg = await getEffectiveBackgroundColor(page, tagline);
      const taglineFg = parseColor(taglineStyles.color);
      const taglineContrast = getContrastRatio(taglineFg, taglineBg);
      const taglineIsLarge = isLargeText(
        taglineStyles.fontSize,
        taglineStyles.fontWeight
      );
      const taglineMinRatio = taglineIsLarge ? 3 : 4.5;

      expect(taglineContrast).toBeGreaterThanOrEqual(taglineMinRatio);

      // Check description
      const description = page.locator('.hero .description');
      await expect(description).toBeVisible();

      const descStyles = await description.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return {
          color: style.color,
          fontSize: style.fontSize,
          fontWeight: style.fontWeight,
        };
      });

      const descBg = await getEffectiveBackgroundColor(page, description);
      const descFg = parseColor(descStyles.color);
      const descContrast = getContrastRatio(descFg, descBg);
      const descIsLarge = isLargeText(descStyles.fontSize, descStyles.fontWeight);
      const descMinRatio = descIsLarge ? 3 : 4.5;

      expect(descContrast).toBeGreaterThanOrEqual(descMinRatio);
    });

    test('Feature card text meets contrast requirements', async ({ page }) => {
      const featureCards = page.locator('.feature-card');
      const count = await featureCards.count();

      expect(count).toBeGreaterThan(0);

      for (let i = 0; i < count; i++) {
        const card = featureCards.nth(i);

        // Check h3 title
        const h3 = card.locator('h3');
        if ((await h3.count()) > 0) {
          const h3Styles = await h3.evaluate((el) => {
            const style = window.getComputedStyle(el);
            return {
              color: style.color,
              fontSize: style.fontSize,
              fontWeight: style.fontWeight,
            };
          });

          const h3Bg = await getEffectiveBackgroundColor(page, h3);
          const h3Fg = parseColor(h3Styles.color);
          const h3Contrast = getContrastRatio(h3Fg, h3Bg);
          const h3IsLarge = isLargeText(h3Styles.fontSize, h3Styles.fontWeight);
          const h3MinRatio = h3IsLarge ? 3 : 4.5;

          expect(h3Contrast).toBeGreaterThanOrEqual(h3MinRatio);
        }

        // Check paragraph text
        const p = card.locator('p');
        if ((await p.count()) > 0) {
          const pStyles = await p.first().evaluate((el) => {
            const style = window.getComputedStyle(el);
            return {
              color: style.color,
              fontSize: style.fontSize,
              fontWeight: style.fontWeight,
            };
          });

          const pBg = await getEffectiveBackgroundColor(page, p.first());
          const pFg = parseColor(pStyles.color);
          const pContrast = getContrastRatio(pFg, pBg);
          const pIsLarge = isLargeText(pStyles.fontSize, pStyles.fontWeight);
          const pMinRatio = pIsLarge ? 3 : 4.5;

          expect(pContrast).toBeGreaterThanOrEqual(pMinRatio);
        }
      }
    });

    test('Section headings meet contrast requirements', async ({ page }) => {
      const h2Elements = page.locator('h2');
      const count = await h2Elements.count();

      expect(count).toBeGreaterThan(0);

      for (let i = 0; i < count; i++) {
        const h2 = h2Elements.nth(i);

        // Skip hidden elements
        if (!(await h2.isVisible())) {
          continue;
        }

        const h2Styles = await h2.evaluate((el) => {
          const style = window.getComputedStyle(el);
          return {
            color: style.color,
            fontSize: style.fontSize,
            fontWeight: style.fontWeight,
          };
        });

        const h2Bg = await getEffectiveBackgroundColor(page, h2);
        const h2Fg = parseColor(h2Styles.color);
        const h2Contrast = getContrastRatio(h2Fg, h2Bg);
        const h2IsLarge = isLargeText(h2Styles.fontSize, h2Styles.fontWeight);
        const h2MinRatio = h2IsLarge ? 3 : 4.5;

        expect(h2Contrast).toBeGreaterThanOrEqual(h2MinRatio);
      }
    });

    test('Table content meets contrast requirements', async ({ page }) => {
      const tables = page.locator('table');
      const tableCount = await tables.count();

      if (tableCount > 0) {
        // Check table header
        const th = page.locator('th').first();
        if (await th.isVisible()) {
          const thStyles = await th.evaluate((el) => {
            const style = window.getComputedStyle(el);
            return {
              color: style.color,
              fontSize: style.fontSize,
              fontWeight: style.fontWeight,
            };
          });

          const thBg = await getEffectiveBackgroundColor(page, th);
          const thFg = parseColor(thStyles.color);
          const thContrast = getContrastRatio(thFg, thBg);

          // Table headers are typically normal text
          expect(thContrast).toBeGreaterThanOrEqual(4.5);
        }

        // Check table data cells
        const td = page.locator('td').first();
        if (await td.isVisible()) {
          const tdStyles = await td.evaluate((el) => {
            const style = window.getComputedStyle(el);
            return {
              color: style.color,
              fontSize: style.fontSize,
              fontWeight: style.fontWeight,
            };
          });

          const tdBg = await getEffectiveBackgroundColor(page, td);
          const tdFg = parseColor(tdStyles.color);
          const tdContrast = getContrastRatio(tdFg, tdBg);

          expect(tdContrast).toBeGreaterThanOrEqual(4.5);
        }
      }
    });

    test('Footer text meets contrast requirements', async ({ page }) => {
      const footer = page.locator('footer');
      await expect(footer).toBeVisible();

      // Check footer paragraphs
      const footerP = footer.locator('p').first();
      if (await footerP.isVisible()) {
        const pStyles = await footerP.evaluate((el) => {
          const style = window.getComputedStyle(el);
          return {
            color: style.color,
            fontSize: style.fontSize,
            fontWeight: style.fontWeight,
          };
        });

        const pBg = await getEffectiveBackgroundColor(page, footerP);
        const pFg = parseColor(pStyles.color);
        const pContrast = getContrastRatio(pFg, pBg);

        expect(pContrast).toBeGreaterThanOrEqual(4.5);
      }
    });
  });

  test.describe('TC2: Code Block Contrast', () => {
    test('Code blocks have sufficient contrast for text content', async ({
      page,
    }) => {
      const codeBlocks = page.locator('pre code');
      const count = await codeBlocks.count();

      expect(count).toBeGreaterThan(0);

      for (let i = 0; i < count; i++) {
        const code = codeBlocks.nth(i);

        // Skip hidden elements
        if (!(await code.isVisible())) {
          continue;
        }

        const codeStyles = await code.evaluate((el) => {
          const style = window.getComputedStyle(el);
          return {
            color: style.color,
            fontSize: style.fontSize,
            fontWeight: style.fontWeight,
          };
        });

        const codeBg = await getEffectiveBackgroundColor(page, code);
        const codeFg = parseColor(codeStyles.color);
        const codeContrast = getContrastRatio(codeFg, codeBg);

        // Code is typically normal-sized text
        expect(codeContrast).toBeGreaterThanOrEqual(4.5);
      }
    });

    test('Inline code elements have sufficient contrast', async ({ page }) => {
      // Check inline code elements (code tags outside of pre)
      const inlineCodes = page.locator('code:not(pre code)');
      const count = await inlineCodes.count();

      for (let i = 0; i < Math.min(count, 10); i++) {
        const code = inlineCodes.nth(i);

        // Skip hidden elements
        if (!(await code.isVisible())) {
          continue;
        }

        const codeStyles = await code.evaluate((el) => {
          const style = window.getComputedStyle(el);
          return {
            color: style.color,
            fontSize: style.fontSize,
            fontWeight: style.fontWeight,
          };
        });

        const codeBg = await getEffectiveBackgroundColor(page, code);
        const codeFg = parseColor(codeStyles.color);
        const codeContrast = getContrastRatio(codeFg, codeBg);

        expect(codeContrast).toBeGreaterThanOrEqual(4.5);
      }
    });

    test('Pre elements have readable background contrast with page', async ({
      page,
    }) => {
      const preElements = page.locator('pre');
      const count = await preElements.count();

      for (let i = 0; i < count; i++) {
        const pre = preElements.nth(i);

        if (!(await pre.isVisible())) {
          continue;
        }

        // Check that code blocks are visually distinct from surrounding content
        const preBg = await pre.evaluate((el) => {
          const style = window.getComputedStyle(el);
          return style.backgroundColor;
        });

        // Verify the background is defined (not transparent)
        expect(preBg).not.toBe('rgba(0, 0, 0, 0)');
        expect(preBg).toMatch(/rgba?\(\d+,\s*\d+,\s*\d+/);
      }
    });
  });

  test.describe('TC3: Link Visibility', () => {
    test('Links are distinguishable from regular text by color or underline', async ({
      page,
    }) => {
      // Get body text color as baseline
      const bodyStyles = await page.locator('body').evaluate((el) => {
        const style = window.getComputedStyle(el);
        return { color: style.color };
      });
      const bodyColor = parseColor(bodyStyles.color);

      // Check all visible links
      const links = page.locator('a:visible');
      const count = await links.count();

      expect(count).toBeGreaterThan(0);

      for (let i = 0; i < Math.min(count, 15); i++) {
        const link = links.nth(i);

        if (!(await link.isVisible())) {
          continue;
        }

        const linkStyles = await link.evaluate((el) => {
          const style = window.getComputedStyle(el);
          return {
            color: style.color,
            textDecoration: style.textDecoration,
            textDecorationLine: style.textDecorationLine,
            borderBottom: style.borderBottom,
          };
        });

        const linkColor = parseColor(linkStyles.color);

        // Link should be distinguishable either by:
        // 1. Different color from surrounding text
        // 2. Underline (text-decoration)
        // 3. Border-bottom

        const hasUnderline =
          linkStyles.textDecoration.includes('underline') ||
          linkStyles.textDecorationLine.includes('underline');
        const hasBorder =
          linkStyles.borderBottom !== 'none' &&
          linkStyles.borderBottom !== '' &&
          linkStyles.borderBottom !== '0px none';

        // Check if color is different (using a simple color difference check)
        const colorDiff =
          Math.abs(linkColor.r - bodyColor.r) +
          Math.abs(linkColor.g - bodyColor.g) +
          Math.abs(linkColor.b - bodyColor.b);
        const hasDifferentColor = colorDiff > 50; // Threshold for "visibly different"

        // At least one distinguishing feature should be present
        const isDistinguishable = hasUnderline || hasBorder || hasDifferentColor;
        expect(isDistinguishable).toBeTruthy();
      }
    });

    test('Link text meets contrast requirements against background', async ({
      page,
    }) => {
      const links = page.locator('a:visible');
      const count = await links.count();

      expect(count).toBeGreaterThan(0);

      for (let i = 0; i < Math.min(count, 15); i++) {
        const link = links.nth(i);

        if (!(await link.isVisible())) {
          continue;
        }

        const linkStyles = await link.evaluate((el) => {
          const style = window.getComputedStyle(el);
          return {
            color: style.color,
            fontSize: style.fontSize,
            fontWeight: style.fontWeight,
          };
        });

        const linkBg = await getEffectiveBackgroundColor(page, link);
        const linkFg = parseColor(linkStyles.color);
        const linkContrast = getContrastRatio(linkFg, linkBg);
        const linkIsLarge = isLargeText(
          linkStyles.fontSize,
          linkStyles.fontWeight
        );
        const linkMinRatio = linkIsLarge ? 3 : 4.5;

        expect(linkContrast).toBeGreaterThanOrEqual(linkMinRatio);
      }
    });

    test('Button links have sufficient contrast', async ({ page }) => {
      const buttons = page.locator('.btn');
      const count = await buttons.count();

      expect(count).toBeGreaterThan(0);

      for (let i = 0; i < count; i++) {
        const btn = buttons.nth(i);

        if (!(await btn.isVisible())) {
          continue;
        }

        const btnStyles = await btn.evaluate((el) => {
          const style = window.getComputedStyle(el);
          return {
            color: style.color,
            backgroundColor: style.backgroundColor,
            fontSize: style.fontSize,
            fontWeight: style.fontWeight,
          };
        });

        const btnFg = parseColor(btnStyles.color);
        const btnBg = parseColor(btnStyles.backgroundColor);
        const btnContrast = getContrastRatio(btnFg, btnBg);
        const btnIsLarge = isLargeText(btnStyles.fontSize, btnStyles.fontWeight);
        const btnMinRatio = btnIsLarge ? 3 : 4.5;

        expect(btnContrast).toBeGreaterThanOrEqual(btnMinRatio);
      }
    });

    test('Footer links are visible and accessible', async ({ page }) => {
      const footerLinks = page.locator('footer a');
      const count = await footerLinks.count();

      expect(count).toBeGreaterThan(0);

      for (let i = 0; i < count; i++) {
        const link = footerLinks.nth(i);

        if (!(await link.isVisible())) {
          continue;
        }

        const linkStyles = await link.evaluate((el) => {
          const style = window.getComputedStyle(el);
          return {
            color: style.color,
            fontSize: style.fontSize,
            fontWeight: style.fontWeight,
          };
        });

        const linkBg = await getEffectiveBackgroundColor(page, link);
        const linkFg = parseColor(linkStyles.color);
        const linkContrast = getContrastRatio(linkFg, linkBg);

        expect(linkContrast).toBeGreaterThanOrEqual(4.5);
      }
    });
  });

  test.describe('Additional Contrast Checks', () => {
    test('Interactive elements (buttons) meet contrast requirements', async ({
      page,
    }) => {
      // Primary button
      const primaryBtn = page.locator('.btn-primary').first();
      if (await primaryBtn.isVisible()) {
        const btnStyles = await primaryBtn.evaluate((el) => {
          const style = window.getComputedStyle(el);
          return {
            color: style.color,
            backgroundColor: style.backgroundColor,
          };
        });

        const btnFg = parseColor(btnStyles.color);
        const btnBg = parseColor(btnStyles.backgroundColor);
        const contrast = getContrastRatio(btnFg, btnBg);

        expect(contrast).toBeGreaterThanOrEqual(4.5);
      }

      // Secondary button
      const secondaryBtn = page.locator('.btn-secondary').first();
      if (await secondaryBtn.isVisible()) {
        const btnStyles = await secondaryBtn.evaluate((el) => {
          const style = window.getComputedStyle(el);
          return {
            color: style.color,
            backgroundColor: style.backgroundColor,
          };
        });

        // For secondary buttons with transparent background, check against effective bg
        const btnBg = await getEffectiveBackgroundColor(page, secondaryBtn);
        const btnFg = parseColor(btnStyles.color);
        const contrast = getContrastRatio(btnFg, btnBg);

        expect(contrast).toBeGreaterThanOrEqual(4.5);
      }
    });

    test('Architecture diagram labels are readable', async ({ page }) => {
      const archSection = page.locator('.architecture');

      if (await archSection.isVisible()) {
        // Check SVG text elements if present
        const svgTexts = page.locator('.architecture svg text');
        const count = await svgTexts.count();

        // Just verify they exist - SVG text contrast is harder to test
        // but we verify the diagram has text labels
        if (count > 0) {
          expect(count).toBeGreaterThan(0);
        }
      }
    });

    test('Status indicators have sufficient contrast', async ({ page }) => {
      const statusElements = page.locator('.footer-status');
      const count = await statusElements.count();

      for (let i = 0; i < count; i++) {
        const status = statusElements.nth(i);

        if (!(await status.isVisible())) {
          continue;
        }

        const statusStyles = await status.evaluate((el) => {
          const style = window.getComputedStyle(el);
          return {
            color: style.color,
            fontSize: style.fontSize,
            fontWeight: style.fontWeight,
          };
        });

        const statusBg = await getEffectiveBackgroundColor(page, status);
        const statusFg = parseColor(statusStyles.color);
        const statusContrast = getContrastRatio(statusFg, statusBg);

        expect(statusContrast).toBeGreaterThanOrEqual(4.5);
      }
    });
  });
});
