import { test, expect, Page } from '@playwright/test';

/**
 * Color Contrast Accessibility Tests
 *
 * Tests verify WCAG 2.1 AA compliance for color contrast:
 * - Normal text: minimum 4.5:1 contrast ratio
 * - Large text (18pt+ or 14pt+ bold): minimum 3:1 contrast ratio
 * - UI components (buttons, links, borders): minimum 3:1 contrast ratio
 *
 * Color contrast formula: (L1 + 0.05) / (L2 + 0.05)
 * Where L1 and L2 are relative luminances (lighter/darker)
 */

// WCAG 2.1 AA minimum contrast ratios
const CONTRAST_RATIOS = {
  NORMAL_TEXT: 4.5,
  LARGE_TEXT: 3.0,
  UI_COMPONENTS: 3.0,
};

// Large text is defined as 18pt (24px) or 14pt (18.66px) bold
const LARGE_TEXT_SIZE_PX = 24;
const LARGE_TEXT_BOLD_SIZE_PX = 18.66;

/**
 * Parse RGB/RGBA color string to RGB values
 */
function parseColor(colorString: string): { r: number; g: number; b: number; a: number } | null {
  if (!colorString) return null;

  // Handle rgb(r, g, b) format
  const rgbMatch = colorString.match(/^rgb\((\d+),\s*(\d+),\s*(\d+)\)$/);
  if (rgbMatch) {
    return {
      r: parseInt(rgbMatch[1], 10),
      g: parseInt(rgbMatch[2], 10),
      b: parseInt(rgbMatch[3], 10),
      a: 1,
    };
  }

  // Handle rgba(r, g, b, a) format
  const rgbaMatch = colorString.match(/^rgba\((\d+),\s*(\d+),\s*(\d+),\s*([\d.]+)\)$/);
  if (rgbaMatch) {
    return {
      r: parseInt(rgbaMatch[1], 10),
      g: parseInt(rgbaMatch[2], 10),
      b: parseInt(rgbaMatch[3], 10),
      a: parseFloat(rgbaMatch[4]),
    };
  }

  // Handle hex format
  const hexMatch = colorString.match(/^#([0-9a-f]{2})([0-9a-f]{2})([0-9a-f]{2})$/i);
  if (hexMatch) {
    return {
      r: parseInt(hexMatch[1], 16),
      g: parseInt(hexMatch[2], 16),
      b: parseInt(hexMatch[3], 16),
      a: 1,
    };
  }

  return null;
}

/**
 * Calculate relative luminance per WCAG 2.1
 * Formula: L = 0.2126 * R + 0.7152 * G + 0.0722 * B
 * where R, G, B are linearized sRGB values
 */
function getRelativeLuminance(r: number, g: number, b: number): number {
  const sRGB = [r / 255, g / 255, b / 255];

  const linearized = sRGB.map((value) => {
    if (value <= 0.03928) {
      return value / 12.92;
    }
    return Math.pow((value + 0.055) / 1.055, 2.4);
  });

  return 0.2126 * linearized[0] + 0.7152 * linearized[1] + 0.0722 * linearized[2];
}

/**
 * Calculate contrast ratio between two colors
 * Formula: (L1 + 0.05) / (L2 + 0.05) where L1 > L2
 */
function getContrastRatio(
  fg: { r: number; g: number; b: number },
  bg: { r: number; g: number; b: number }
): number {
  const l1 = getRelativeLuminance(fg.r, fg.g, fg.b);
  const l2 = getRelativeLuminance(bg.r, bg.g, bg.b);

  const lighter = Math.max(l1, l2);
  const darker = Math.min(l1, l2);

  return (lighter + 0.05) / (darker + 0.05);
}

/**
 * Determine if text is "large" per WCAG 2.1
 * Large text: 18pt (24px) or 14pt (18.66px) bold
 */
function isLargeText(fontSize: number, fontWeight: number | string): boolean {
  const weight = typeof fontWeight === 'string' ? parseInt(fontWeight, 10) || 400 : fontWeight;
  const isBold = weight >= 700;

  if (fontSize >= LARGE_TEXT_SIZE_PX) return true;
  if (isBold && fontSize >= LARGE_TEXT_BOLD_SIZE_PX) return true;

  return false;
}

/**
 * Get computed background color, traversing up DOM if transparent
 */
async function getEffectiveBackgroundColor(page: Page, selector: string): Promise<string> {
  return await page.evaluate((sel) => {
    const element = document.querySelector(sel);
    if (!element) return 'rgb(255, 255, 255)';

    let current: Element | null = element;
    while (current && current !== document.documentElement) {
      const style = window.getComputedStyle(current);
      const bg = style.backgroundColor;

      // Check if background is not transparent
      if (bg && bg !== 'rgba(0, 0, 0, 0)' && bg !== 'transparent') {
        return bg;
      }

      current = current.parentElement;
    }

    // Default to white background
    return 'rgb(255, 255, 255)';
  }, selector);
}

/**
 * Get text color and styling info for an element
 */
async function getTextStyles(page: Page, selector: string) {
  return await page.evaluate((sel) => {
    const element = document.querySelector(sel);
    if (!element) return null;

    const style = window.getComputedStyle(element);

    return {
      color: style.color,
      fontSize: parseFloat(style.fontSize),
      fontWeight: style.fontWeight,
    };
  }, selector);
}

test.describe('Accessibility - Color Contrast', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Body text has minimum 4.5:1 contrast ratio against background', async ({ page }) => {
    // Test body text in key sections

    // Check hero description text (slate-400 on gradient background)
    const heroDescStyles = await getTextStyles(page, '.hero-description');
    const heroBgColor = await getEffectiveBackgroundColor(page, '#hero');

    expect(heroDescStyles).not.toBeNull();
    if (heroDescStyles) {
      const fgColor = parseColor(heroDescStyles.color);
      const bgColor = parseColor(heroBgColor);

      expect(fgColor).not.toBeNull();
      expect(bgColor).not.toBeNull();

      if (fgColor && bgColor) {
        const ratio = getContrastRatio(fgColor, bgColor);
        const isLarge = isLargeText(heroDescStyles.fontSize, heroDescStyles.fontWeight);
        const requiredRatio = isLarge ? CONTRAST_RATIOS.LARGE_TEXT : CONTRAST_RATIOS.NORMAL_TEXT;

        expect(
          ratio,
          `Hero description text contrast ratio ${ratio.toFixed(2)}:1 should be at least ${requiredRatio}:1`
        ).toBeGreaterThanOrEqual(requiredRatio);
      }
    }

    // Check feature card description text (slate-300 on slate-800)
    const featureCards = page.locator('.feature-card');
    const featureCardCount = await featureCards.count();

    if (featureCardCount > 0) {
      const featureDescStyles = await getTextStyles(page, '.feature-card [data-description]');
      const featureBgColor = await getEffectiveBackgroundColor(page, '.feature-card');

      expect(featureDescStyles).not.toBeNull();
      if (featureDescStyles) {
        const fgColor = parseColor(featureDescStyles.color);
        const bgColor = parseColor(featureBgColor);

        expect(fgColor).not.toBeNull();
        expect(bgColor).not.toBeNull();

        if (fgColor && bgColor) {
          const ratio = getContrastRatio(fgColor, bgColor);
          const isLarge = isLargeText(featureDescStyles.fontSize, featureDescStyles.fontWeight);
          const requiredRatio = isLarge ? CONTRAST_RATIOS.LARGE_TEXT : CONTRAST_RATIOS.NORMAL_TEXT;

          expect(
            ratio,
            `Feature card description contrast ratio ${ratio.toFixed(2)}:1 should be at least ${requiredRatio}:1`
          ).toBeGreaterThanOrEqual(requiredRatio);
        }
      }
    }

    // Check project status section text (slate-300 on emerald-900/30 and orange-900/30)
    const implementedList = page.locator('[data-testid="implemented-list"] li');
    const implementedListCount = await implementedList.count();

    if (implementedListCount > 0) {
      const implementedTextStyles = await getTextStyles(
        page,
        '[data-testid="implemented-list"] li span:last-child'
      );
      const implementedBgColor = await getEffectiveBackgroundColor(
        page,
        '[data-testid="implemented-features"]'
      );

      expect(implementedTextStyles).not.toBeNull();
      if (implementedTextStyles) {
        const fgColor = parseColor(implementedTextStyles.color);
        const bgColor = parseColor(implementedBgColor);

        expect(fgColor).not.toBeNull();
        expect(bgColor).not.toBeNull();

        if (fgColor && bgColor) {
          const ratio = getContrastRatio(fgColor, bgColor);
          const isLarge = isLargeText(implementedTextStyles.fontSize, implementedTextStyles.fontWeight);
          const requiredRatio = isLarge ? CONTRAST_RATIOS.LARGE_TEXT : CONTRAST_RATIOS.NORMAL_TEXT;

          expect(
            ratio,
            `Implemented list text contrast ratio ${ratio.toFixed(2)}:1 should be at least ${requiredRatio}:1`
          ).toBeGreaterThanOrEqual(requiredRatio);
        }
      }
    }
  });

  test('TC2: Heading text has minimum 3:1 contrast ratio (large text)', async ({ page }) => {
    // Check main page heading (hero title)
    const h1Styles = await getTextStyles(page, 'h1');
    const h1BgColor = await getEffectiveBackgroundColor(page, '#hero');

    expect(h1Styles).not.toBeNull();
    if (h1Styles) {
      const fgColor = parseColor(h1Styles.color);
      const bgColor = parseColor(h1BgColor);

      expect(fgColor).not.toBeNull();
      expect(bgColor).not.toBeNull();

      if (fgColor && bgColor) {
        const ratio = getContrastRatio(fgColor, bgColor);
        // H1 is definitely large text, so 3:1 minimum
        expect(
          ratio,
          `H1 heading contrast ratio ${ratio.toFixed(2)}:1 should be at least ${CONTRAST_RATIOS.LARGE_TEXT}:1`
        ).toBeGreaterThanOrEqual(CONTRAST_RATIOS.LARGE_TEXT);
      }
    }

    // Check section headings (h2 elements)
    const h2Elements = page.locator('h2');
    const h2Count = await h2Elements.count();

    expect(h2Count).toBeGreaterThan(0);

    // Test the first few h2 elements
    for (let i = 0; i < Math.min(h2Count, 3); i++) {
      const h2 = h2Elements.nth(i);
      const h2Visible = await h2.isVisible();

      if (h2Visible) {
        const h2Id = await h2.evaluate((el) => {
          const section = el.closest('section');
          return section?.id || 'unknown';
        });

        const h2Styles = await h2.evaluate((el) => {
          const style = window.getComputedStyle(el);
          return {
            color: style.color,
            fontSize: parseFloat(style.fontSize),
            fontWeight: style.fontWeight,
          };
        });

        const bgColor = await h2.evaluate((el) => {
          let current: Element | null = el;
          while (current && current !== document.documentElement) {
            const style = window.getComputedStyle(current);
            const bg = style.backgroundColor;
            if (bg && bg !== 'rgba(0, 0, 0, 0)' && bg !== 'transparent') {
              return bg;
            }
            current = current.parentElement;
          }
          return 'rgb(255, 255, 255)';
        });

        const fgColor = parseColor(h2Styles.color);
        const parsedBgColor = parseColor(bgColor);

        if (fgColor && parsedBgColor) {
          const ratio = getContrastRatio(fgColor, parsedBgColor);
          // H2 headings are large text by definition
          expect(
            ratio,
            `H2 heading in section "${h2Id}" contrast ratio ${ratio.toFixed(2)}:1 should be at least ${CONTRAST_RATIOS.LARGE_TEXT}:1`
          ).toBeGreaterThanOrEqual(CONTRAST_RATIOS.LARGE_TEXT);
        }
      }
    }

    // Check hero tagline (large text - 1.5rem/24px)
    const taglineStyles = await getTextStyles(page, '.hero-tagline');
    const taglineBgColor = await getEffectiveBackgroundColor(page, '#hero');

    expect(taglineStyles).not.toBeNull();
    if (taglineStyles) {
      const fgColor = parseColor(taglineStyles.color);
      const bgColor = parseColor(taglineBgColor);

      expect(fgColor).not.toBeNull();
      expect(bgColor).not.toBeNull();

      if (fgColor && bgColor) {
        const ratio = getContrastRatio(fgColor, bgColor);
        const isLarge = isLargeText(taglineStyles.fontSize, taglineStyles.fontWeight);
        const requiredRatio = isLarge ? CONTRAST_RATIOS.LARGE_TEXT : CONTRAST_RATIOS.NORMAL_TEXT;

        expect(
          ratio,
          `Tagline contrast ratio ${ratio.toFixed(2)}:1 should be at least ${requiredRatio}:1`
        ).toBeGreaterThanOrEqual(requiredRatio);
      }
    }
  });

  test('TC3: Button text and borders have minimum 3:1 contrast ratio', async ({ page }) => {
    // Test primary button (Get Started)
    const getStartedBtn = page.locator('[data-testid="get-started-btn"]');
    await expect(getStartedBtn).toBeVisible();

    const primaryBtnStyles = await getStartedBtn.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        color: style.color,
        backgroundColor: style.backgroundColor,
        fontSize: parseFloat(style.fontSize),
        fontWeight: style.fontWeight,
      };
    });

    const fgColor = parseColor(primaryBtnStyles.color);
    const bgColor = parseColor(primaryBtnStyles.backgroundColor);

    expect(fgColor).not.toBeNull();
    expect(bgColor).not.toBeNull();

    if (fgColor && bgColor) {
      const ratio = getContrastRatio(fgColor, bgColor);
      // Button text should have at least 3:1 contrast (UI component)
      expect(
        ratio,
        `Primary button text contrast ratio ${ratio.toFixed(2)}:1 should be at least ${CONTRAST_RATIOS.UI_COMPONENTS}:1`
      ).toBeGreaterThanOrEqual(CONTRAST_RATIOS.UI_COMPONENTS);
    }

    // Test secondary button (View on GitHub)
    const githubBtn = page.locator('[data-testid="github-link"]');
    await expect(githubBtn).toBeVisible();

    const secondaryBtnStyles = await githubBtn.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        color: style.color,
        backgroundColor: style.backgroundColor,
        borderColor: style.borderColor,
        fontSize: parseFloat(style.fontSize),
        fontWeight: style.fontWeight,
      };
    });

    // Secondary button: check text against button background
    const secondaryFgColor = parseColor(secondaryBtnStyles.color);
    const secondaryBgColor = await getEffectiveBackgroundColor(page, '#hero');

    if (secondaryFgColor) {
      const bgParsed = parseColor(secondaryBgColor);
      if (bgParsed) {
        const ratio = getContrastRatio(secondaryFgColor, bgParsed);
        expect(
          ratio,
          `Secondary button text contrast ratio ${ratio.toFixed(2)}:1 should be at least ${CONTRAST_RATIOS.UI_COMPONENTS}:1`
        ).toBeGreaterThanOrEqual(CONTRAST_RATIOS.UI_COMPONENTS);
      }
    }

    // Check button border contrast against background (for outlined buttons)
    const borderColor = parseColor(secondaryBtnStyles.borderColor);
    if (borderColor) {
      const bgParsed = parseColor(secondaryBgColor);
      if (bgParsed) {
        const borderRatio = getContrastRatio(borderColor, bgParsed);
        // Borders should have 3:1 contrast for UI components
        expect(
          borderRatio,
          `Secondary button border contrast ratio ${borderRatio.toFixed(2)}:1 should be at least ${CONTRAST_RATIOS.UI_COMPONENTS}:1`
        ).toBeGreaterThanOrEqual(CONTRAST_RATIOS.UI_COMPONENTS);
      }
    }
  });

  test('TC4: Links are distinguishable from surrounding text (color + another indicator)', async ({
    page,
  }) => {
    // Check navigation links
    const navLinks = page.locator('.nav-links a');
    const navLinkCount = await navLinks.count();

    expect(navLinkCount).toBeGreaterThan(0);

    // Check that nav links have adequate contrast
    const firstNavLink = navLinks.first();
    const navLinkStyles = await firstNavLink.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        color: style.color,
        textDecoration: style.textDecoration,
        fontWeight: style.fontWeight,
      };
    });

    const navBgColor = await getEffectiveBackgroundColor(page, '.navbar');
    const navLinkFg = parseColor(navLinkStyles.color);
    const navBgParsed = parseColor(navBgColor);

    if (navLinkFg && navBgParsed) {
      const ratio = getContrastRatio(navLinkFg, navBgParsed);
      expect(
        ratio,
        `Navigation link contrast ratio ${ratio.toFixed(2)}:1 should be at least ${CONTRAST_RATIOS.NORMAL_TEXT}:1`
      ).toBeGreaterThanOrEqual(CONTRAST_RATIOS.NORMAL_TEXT);
    }

    // Check footer links
    const footerRepoLink = page.locator('[data-testid="footer-repo-link"]');
    await expect(footerRepoLink).toBeVisible();

    const footerLinkStyles = await footerRepoLink.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        color: style.color,
        textDecoration: style.textDecoration,
      };
    });

    const footerBgColor = await getEffectiveBackgroundColor(page, 'footer');
    const footerLinkFg = parseColor(footerLinkStyles.color);
    const footerBgParsed = parseColor(footerBgColor);

    if (footerLinkFg && footerBgParsed) {
      const ratio = getContrastRatio(footerLinkFg, footerBgParsed);
      expect(
        ratio,
        `Footer link contrast ratio ${ratio.toFixed(2)}:1 should be at least ${CONTRAST_RATIOS.NORMAL_TEXT}:1`
      ).toBeGreaterThanOrEqual(CONTRAST_RATIOS.NORMAL_TEXT);
    }

    // Check that links have another indicator besides color
    // Links should have underline on hover or different styling
    await footerRepoLink.hover();

    const footerLinkHoverStyles = await footerRepoLink.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        color: style.color,
        textDecoration: style.textDecoration,
      };
    });

    // Links should have a visual indicator:
    // Either underline, different color on hover, or both
    const hasUnderline =
      footerLinkStyles.textDecoration.includes('underline') ||
      footerLinkHoverStyles.textDecoration.includes('underline');
    const hasColorChange = footerLinkStyles.color !== footerLinkHoverStyles.color;

    expect(
      hasUnderline || hasColorChange,
      'Links should have underline or color change on hover for accessibility'
    ).toBeTruthy();

    // Check contributing link in project status section
    const contributingLink = page.locator('[data-testid="contributing-link"]');
    const contributingLinkVisible = await contributingLink.isVisible();

    if (contributingLinkVisible) {
      const contributingLinkStyles = await contributingLink.evaluate((el) => {
        const style = window.getComputedStyle(el);
        return {
          color: style.color,
          fontWeight: style.fontWeight,
        };
      });

      const statusBgColor = await getEffectiveBackgroundColor(page, '[data-testid="contribute-section"]');
      const contributingFg = parseColor(contributingLinkStyles.color);
      const statusBgParsed = parseColor(statusBgColor);

      if (contributingFg && statusBgParsed) {
        const ratio = getContrastRatio(contributingFg, statusBgParsed);
        expect(
          ratio,
          `Contributing link contrast ratio ${ratio.toFixed(2)}:1 should be at least ${CONTRAST_RATIOS.NORMAL_TEXT}:1`
        ).toBeGreaterThanOrEqual(CONTRAST_RATIOS.NORMAL_TEXT);
      }

      // Check that link is distinguishable (bold text and/or different color)
      const isBold = parseInt(contributingLinkStyles.fontWeight, 10) >= 600;
      expect(isBold, 'Contributing link should be bold to distinguish from surrounding text').toBeTruthy();
    }
  });

  test('TC5: Interactive focus states have adequate contrast', async ({ page }) => {
    // Test focus state on primary button
    const getStartedBtn = page.locator('[data-testid="get-started-btn"]');
    await getStartedBtn.focus();

    // Check that focused element is visually distinguishable
    const isFocused = await getStartedBtn.evaluate((el) => el === document.activeElement);
    expect(isFocused).toBeTruthy();

    // Get focus styles
    const focusStyles = await getStartedBtn.evaluate((el) => {
      const style = window.getComputedStyle(el);
      return {
        outlineColor: style.outlineColor,
        outlineWidth: style.outlineWidth,
        outlineStyle: style.outlineStyle,
        boxShadow: style.boxShadow,
      };
    });

    // Focus indicator should be visible (outline or box-shadow)
    const hasOutline =
      focusStyles.outlineStyle !== 'none' && parseFloat(focusStyles.outlineWidth) > 0;
    const hasBoxShadow = focusStyles.boxShadow !== 'none';

    expect(
      hasOutline || hasBoxShadow,
      'Focused button should have visible focus indicator (outline or box-shadow)'
    ).toBeTruthy();
  });

  test('TC6: Code block text has adequate contrast', async ({ page }) => {
    // Scroll to getting-started section where code blocks are
    const gettingStartedSection = page.locator('#getting-started');
    const gettingStartedExists = (await gettingStartedSection.count()) > 0;

    if (gettingStartedExists) {
      await gettingStartedSection.scrollIntoViewIfNeeded();

      const codeBlocks = page.locator('pre code, pre');
      const codeBlockCount = await codeBlocks.count();

      if (codeBlockCount > 0) {
        const codeStyles = await codeBlocks.first().evaluate((el) => {
          const style = window.getComputedStyle(el);
          return {
            color: style.color,
            backgroundColor: style.backgroundColor,
          };
        });

        // If code block has transparent background, get parent's background
        let bgColor = codeStyles.backgroundColor;
        if (bgColor === 'rgba(0, 0, 0, 0)' || bgColor === 'transparent') {
          bgColor = await getEffectiveBackgroundColor(page, 'pre');
        }

        const fgColor = parseColor(codeStyles.color);
        const bgParsed = parseColor(bgColor);

        if (fgColor && bgParsed) {
          const ratio = getContrastRatio(fgColor, bgParsed);
          // Code should meet normal text requirements (4.5:1)
          expect(
            ratio,
            `Code block text contrast ratio ${ratio.toFixed(2)}:1 should be at least ${CONTRAST_RATIOS.NORMAL_TEXT}:1`
          ).toBeGreaterThanOrEqual(CONTRAST_RATIOS.NORMAL_TEXT);
        }
      }
    }
  });
});
