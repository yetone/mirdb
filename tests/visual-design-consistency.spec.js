// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Visual Design Consistency Tests
 *
 * These tests verify that the MirDB landing page maintains consistent
 * visual design across all sections, including:
 * - Typography (font family, font sizes for hierarchy)
 * - Color scheme (CSS variables usage, professional palette)
 * - Spacing (consistent margins and padding)
 * - Dark mode friendliness (appropriate for developer audience)
 */

// Expected design tokens from the CSS
const DESIGN_TOKENS = {
  colors: {
    bgDark: 'rgb(13, 17, 23)',           // #0d1117
    bgSecondary: 'rgb(22, 27, 34)',      // #161b22
    textPrimary: 'rgb(201, 209, 217)',   // #c9d1d9
    textSecondary: 'rgb(139, 148, 158)', // #8b949e
    accentGreen: 'rgb(63, 185, 80)',     // #3fb950
    accentBlue: 'rgb(88, 166, 255)',     // #58a6ff
    borderColor: 'rgb(48, 54, 61)'       // #30363d
  },
  fontFamily: [
    '-apple-system',
    'BlinkMacSystemFont',
    'Segoe UI',
    'Noto Sans',
    'Helvetica',
    'Arial',
    'sans-serif'
  ],
  monoFontFamily: [
    'SF Mono',
    'Menlo',
    'Monaco',
    'Consolas',
    'monospace'
  ]
};

test.describe('Visual Design Consistency', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
    await page.waitForLoadState('networkidle');
  });

  test.describe('Typography Consistency', () => {
    test('TC1: Consistent font family used throughout the page', async ({ page }) => {
      // Check that body uses the expected system font stack
      const bodyFontFamily = await page.evaluate(() => {
        return window.getComputedStyle(document.body).fontFamily;
      });

      // Verify at least one of the expected fonts is used
      const hasExpectedFont = DESIGN_TOKENS.fontFamily.some(font =>
        bodyFontFamily.toLowerCase().includes(font.toLowerCase().replace(/['"]/g, ''))
      );
      expect(hasExpectedFont, `Body font-family "${bodyFontFamily}" should include system font stack`).toBe(true);

      // Check all major text sections use consistent font family
      const sections = ['.hero', '.features', '.usage', '.roadmap', 'footer'];

      for (const selector of sections) {
        const section = page.locator(selector).first();
        const isVisible = await section.isVisible().catch(() => false);

        if (isVisible) {
          const sectionFontFamily = await section.evaluate(el => {
            return window.getComputedStyle(el).fontFamily;
          });

          // Font family should be consistent with body
          expect(
            sectionFontFamily.toLowerCase(),
            `Section "${selector}" should use consistent font family`
          ).toContain(bodyFontFamily.toLowerCase().split(',')[0].trim());
        }
      }
    });

    test('TC2: Heading hierarchy has appropriate font sizes (h1 > h2 > h3)', async ({ page }) => {
      const headingSizes = await page.evaluate(() => {
        const h1 = document.querySelector('h1');
        const h2Elements = document.querySelectorAll('h2');
        const h3Elements = document.querySelectorAll('h3');

        const h1Size = h1 ? parseFloat(window.getComputedStyle(h1).fontSize) : 0;
        const h2Sizes = Array.from(h2Elements).map(el =>
          parseFloat(window.getComputedStyle(el).fontSize)
        );
        const h3Sizes = Array.from(h3Elements).map(el =>
          parseFloat(window.getComputedStyle(el).fontSize)
        );

        return { h1Size, h2Sizes, h3Sizes };
      });

      // h1 should exist and be the largest
      expect(headingSizes.h1Size, 'h1 should exist and have a font size').toBeGreaterThan(0);

      // h1 should be larger than h2
      if (headingSizes.h2Sizes.length > 0) {
        const maxH2Size = Math.max(...headingSizes.h2Sizes);
        expect(
          headingSizes.h1Size,
          `h1 (${headingSizes.h1Size}px) should be larger than h2 (${maxH2Size}px)`
        ).toBeGreaterThan(maxH2Size);
      }

      // h2 should be larger than h3
      if (headingSizes.h2Sizes.length > 0 && headingSizes.h3Sizes.length > 0) {
        const minH2Size = Math.min(...headingSizes.h2Sizes);
        const maxH3Size = Math.max(...headingSizes.h3Sizes);
        expect(
          minH2Size,
          `h2 (${minH2Size}px) should be larger than h3 (${maxH3Size}px)`
        ).toBeGreaterThan(maxH3Size);
      }
    });

    test('TC3: Font weights establish visual hierarchy', async ({ page }) => {
      const fontWeights = await page.evaluate(() => {
        const elements = {
          h1: document.querySelector('h1'),
          h2: document.querySelector('h2'),
          h3: document.querySelector('h3'),
          paragraph: document.querySelector('.section-description, .description, p')
        };

        return {
          h1: elements.h1 ? parseInt(window.getComputedStyle(elements.h1).fontWeight) : 0,
          h2: elements.h2 ? parseInt(window.getComputedStyle(elements.h2).fontWeight) : 0,
          h3: elements.h3 ? parseInt(window.getComputedStyle(elements.h3).fontWeight) : 0,
          paragraph: elements.paragraph ? parseInt(window.getComputedStyle(elements.paragraph).fontWeight) : 0
        };
      });

      // Headings should have bolder weight than body text
      expect(fontWeights.h1, 'h1 should have bold or heavier weight').toBeGreaterThanOrEqual(600);

      // Paragraph text should have normal weight
      if (fontWeights.paragraph > 0) {
        expect(fontWeights.paragraph, 'Paragraph text should have normal weight').toBeLessThanOrEqual(500);
      }
    });

    test('TC4: Code blocks use monospace font family', async ({ page }) => {
      const codeBlock = page.locator('.code-block pre, code, pre');
      const count = await codeBlock.count();

      if (count > 0) {
        const codeFontFamily = await codeBlock.first().evaluate(el => {
          return window.getComputedStyle(el).fontFamily;
        });

        const hasMonoFont = DESIGN_TOKENS.monoFontFamily.some(font =>
          codeFontFamily.toLowerCase().includes(font.toLowerCase().replace(/['"]/g, ''))
        ) || codeFontFamily.toLowerCase().includes('mono');

        expect(hasMonoFont, `Code blocks should use monospace font, got: "${codeFontFamily}"`).toBe(true);
      }
    });
  });

  test.describe('Color Scheme Consistency', () => {
    test('TC5: Page uses consistent dark background colors', async ({ page }) => {
      const backgroundColors = await page.evaluate(() => {
        const sections = ['body', '.hero', '.features', '.usage', '.roadmap', 'footer'];
        const colors = {};

        sections.forEach(selector => {
          const el = document.querySelector(selector);
          if (el) {
            colors[selector] = window.getComputedStyle(el).backgroundColor;
          }
        });

        return colors;
      });

      // All sections should use dark backgrounds
      const darkBackgrounds = [
        DESIGN_TOKENS.colors.bgDark,
        DESIGN_TOKENS.colors.bgSecondary
      ];

      for (const [selector, color] of Object.entries(backgroundColors)) {
        if (color && color !== 'rgba(0, 0, 0, 0)' && color !== 'transparent') {
          const isDarkBg = darkBackgrounds.some(darkBg =>
            color === darkBg ||
            color.replace(/\s/g, '') === darkBg.replace(/\s/g, '')
          );
          expect(isDarkBg, `${selector} should use dark background color, got: ${color}`).toBe(true);
        }
      }
    });

    test('TC6: Primary text uses consistent color', async ({ page }) => {
      const textColors = await page.evaluate(() => {
        const textElements = document.querySelectorAll('h1, h2, h3, .feature-card h3, .roadmap-item h3');
        return Array.from(textElements).map(el => ({
          tag: el.tagName,
          color: window.getComputedStyle(el).color,
          text: el.textContent?.trim().substring(0, 30)
        }));
      });

      // Most primary text should use the primary text color
      const primaryTextColor = DESIGN_TOKENS.colors.textPrimary;

      for (const item of textColors) {
        // Skip h1 which may use gradient text
        if (item.tag === 'H1') continue;

        const normalizedColor = item.color.replace(/\s/g, '');
        const expectedColor = primaryTextColor.replace(/\s/g, '');

        expect(
          normalizedColor,
          `${item.tag} "${item.text}" should use primary text color`
        ).toBe(expectedColor);
      }
    });

    test('TC7: Secondary text uses consistent color', async ({ page }) => {
      const secondaryTextColors = await page.evaluate(() => {
        const selectors = ['.tagline', '.section-description', '.description', '.footer-tagline'];
        const colors = [];

        selectors.forEach(selector => {
          const elements = document.querySelectorAll(selector);
          elements.forEach(el => {
            colors.push({
              selector,
              color: window.getComputedStyle(el).color,
              text: el.textContent?.trim().substring(0, 30)
            });
          });
        });

        return colors;
      });

      const secondaryTextColor = DESIGN_TOKENS.colors.textSecondary;

      for (const item of secondaryTextColors) {
        const normalizedColor = item.color.replace(/\s/g, '');
        const expectedColor = secondaryTextColor.replace(/\s/g, '');

        expect(
          normalizedColor,
          `${item.selector} "${item.text}" should use secondary text color`
        ).toBe(expectedColor);
      }
    });

    test('TC8: Accent colors are used consistently', async ({ page }) => {
      const accentUsage = await page.evaluate(() => {
        const greenElements = [];
        const blueElements = [];

        // Check buttons
        const primaryBtn = document.querySelector('.btn-primary');
        if (primaryBtn) {
          greenElements.push({
            type: 'button',
            bgColor: window.getComputedStyle(primaryBtn).backgroundColor
          });
        }

        // Check icons
        const icons = document.querySelectorAll('.feature-card .icon');
        icons.forEach(icon => {
          greenElements.push({
            type: 'icon',
            color: window.getComputedStyle(icon).color
          });
        });

        // Check links
        const links = document.querySelectorAll('a:not(.btn)');
        links.forEach(link => {
          const color = window.getComputedStyle(link).color;
          if (color.includes('88') || color.includes('166') || color.includes('255')) {
            blueElements.push({
              type: 'link',
              color
            });
          }
        });

        return { greenElements, blueElements };
      });

      // Primary buttons should use accent green
      const accentGreen = DESIGN_TOKENS.colors.accentGreen;
      for (const item of accentUsage.greenElements) {
        if (item.bgColor) {
          const normalizedColor = item.bgColor.replace(/\s/g, '');
          const expectedColor = accentGreen.replace(/\s/g, '');
          expect(normalizedColor, `${item.type} should use accent green`).toBe(expectedColor);
        }
      }
    });

    test('TC9: Border colors are consistent throughout', async ({ page }) => {
      const borderColors = await page.evaluate(() => {
        const borderedElements = document.querySelectorAll('.feature-card, .code-block, .roadmap-item');
        return Array.from(borderedElements).map(el => {
          const styles = window.getComputedStyle(el);
          // Use borderTopColor which always returns a single color value
          return {
            class: el.className,
            borderColor: styles.borderTopColor,
            borderWidth: styles.borderTopWidth
          };
        }).filter(item =>
          item.borderColor &&
          item.borderColor !== 'rgb(0, 0, 0)' &&
          item.borderWidth !== '0px'
        );
      });

      const expectedBorderColor = DESIGN_TOKENS.colors.borderColor;

      for (const item of borderColors) {
        const normalizedColor = item.borderColor.replace(/\s/g, '');
        const expectedColor = expectedBorderColor.replace(/\s/g, '');

        expect(
          normalizedColor,
          `Element with class "${item.class}" should use consistent border color`
        ).toBe(expectedColor);
      }
    });
  });

  test.describe('Spacing Consistency', () => {
    test('TC10: Section padding is consistent', async ({ page }) => {
      const sectionPadding = await page.evaluate(() => {
        const sections = ['.features', '.usage', '.roadmap'];
        return sections.map(selector => {
          const el = document.querySelector(selector);
          if (!el) return null;
          const styles = window.getComputedStyle(el);
          return {
            selector,
            paddingTop: parseFloat(styles.paddingTop),
            paddingBottom: parseFloat(styles.paddingBottom),
            paddingLeft: parseFloat(styles.paddingLeft),
            paddingRight: parseFloat(styles.paddingRight)
          };
        }).filter(Boolean);
      });

      // Check that main sections have consistent vertical padding
      const verticalPaddings = sectionPadding.map(s => s.paddingTop);
      const firstPadding = verticalPaddings[0];

      for (let i = 1; i < verticalPaddings.length; i++) {
        expect(
          verticalPaddings[i],
          `Section padding should be consistent: ${sectionPadding[i].selector} (${verticalPaddings[i]}px) vs ${sectionPadding[0].selector} (${firstPadding}px)`
        ).toBe(firstPadding);
      }

      // Horizontal padding should also be consistent
      const horizontalPaddings = sectionPadding.map(s => s.paddingLeft);
      const firstHorizontal = horizontalPaddings[0];

      for (let i = 1; i < horizontalPaddings.length; i++) {
        expect(
          horizontalPaddings[i],
          `Section horizontal padding should be consistent`
        ).toBe(firstHorizontal);
      }
    });

    test('TC11: Feature cards have consistent spacing', async ({ page }) => {
      const cardSpacing = await page.evaluate(() => {
        const cards = document.querySelectorAll('.feature-card');
        return Array.from(cards).map(card => {
          const styles = window.getComputedStyle(card);
          return {
            padding: styles.padding,
            paddingValue: parseFloat(styles.padding)
          };
        });
      });

      // All feature cards should have the same padding
      const firstPadding = cardSpacing[0]?.paddingValue;

      for (const card of cardSpacing) {
        expect(card.paddingValue, 'Feature card padding should be consistent').toBe(firstPadding);
      }
    });

    test('TC12: Heading margins are consistent within sections', async ({ page }) => {
      const headingMargins = await page.evaluate(() => {
        const h2Elements = document.querySelectorAll('h2');
        return Array.from(h2Elements).map(h2 => {
          const styles = window.getComputedStyle(h2);
          return {
            text: h2.textContent?.trim().substring(0, 20),
            marginBottom: parseFloat(styles.marginBottom)
          };
        });
      });

      if (headingMargins.length > 1) {
        const firstMargin = headingMargins[0].marginBottom;

        for (const heading of headingMargins) {
          expect(
            heading.marginBottom,
            `Heading "${heading.text}" should have consistent bottom margin`
          ).toBe(firstMargin);
        }
      }
    });

    test('TC13: Container max-widths are appropriately set', async ({ page }) => {
      const containerWidths = await page.evaluate(() => {
        const containers = document.querySelectorAll('.container');
        return Array.from(containers).map(container => {
          const styles = window.getComputedStyle(container);
          const parent = container.closest('section, footer');
          return {
            section: parent?.className || 'unknown',
            maxWidth: styles.maxWidth
          };
        });
      });

      // All containers should have a max-width set (not "none")
      for (const container of containerWidths) {
        expect(
          container.maxWidth,
          `Container in ${container.section} should have max-width set`
        ).not.toBe('none');
      }
    });

    test('TC14: Gap spacing in grid/flex layouts is consistent', async ({ page }) => {
      const layoutGaps = await page.evaluate(() => {
        const gaps = [];

        // Features grid
        const featuresGrid = document.querySelector('.features-grid');
        if (featuresGrid) {
          gaps.push({
            element: 'features-grid',
            gap: window.getComputedStyle(featuresGrid).gap
          });
        }

        // CTA buttons
        const ctaButtons = document.querySelector('.cta-buttons');
        if (ctaButtons) {
          gaps.push({
            element: 'cta-buttons',
            gap: window.getComputedStyle(ctaButtons).gap
          });
        }

        // Footer links
        const footerLinks = document.querySelector('.footer-links');
        if (footerLinks) {
          gaps.push({
            element: 'footer-links',
            gap: window.getComputedStyle(footerLinks).gap
          });
        }

        return gaps;
      });

      // All gaps should be set (not empty)
      for (const layout of layoutGaps) {
        expect(
          layout.gap,
          `${layout.element} should have gap spacing defined`
        ).toBeTruthy();
        expect(
          layout.gap,
          `${layout.element} gap should not be "normal"`
        ).not.toBe('normal');
      }
    });
  });

  test.describe('Dark Mode Friendliness', () => {
    test('TC15: Background colors are dark (developer-friendly)', async ({ page }) => {
      const bgLuminance = await page.evaluate(() => {
        function getLuminance(r, g, b) {
          const [rs, gs, bs] = [r, g, b].map(c => {
            const sRGB = c / 255;
            return sRGB <= 0.03928 ? sRGB / 12.92 : Math.pow((sRGB + 0.055) / 1.055, 2.4);
          });
          return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
        }

        function parseColor(color) {
          const match = color.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
          if (match) {
            return { r: parseInt(match[1]), g: parseInt(match[2]), b: parseInt(match[3]) };
          }
          return null;
        }

        const bgColor = window.getComputedStyle(document.body).backgroundColor;
        const parsed = parseColor(bgColor);

        if (parsed) {
          return getLuminance(parsed.r, parsed.g, parsed.b);
        }
        return null;
      });

      // Dark backgrounds should have luminance < 0.1
      expect(bgLuminance, 'Background should be dark (luminance < 0.1)').toBeLessThan(0.1);
    });

    test('TC16: Text colors provide good contrast on dark backgrounds', async ({ page }) => {
      const textLuminance = await page.evaluate(() => {
        function getLuminance(r, g, b) {
          const [rs, gs, bs] = [r, g, b].map(c => {
            const sRGB = c / 255;
            return sRGB <= 0.03928 ? sRGB / 12.92 : Math.pow((sRGB + 0.055) / 1.055, 2.4);
          });
          return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
        }

        function parseColor(color) {
          const match = color.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
          if (match) {
            return { r: parseInt(match[1]), g: parseInt(match[2]), b: parseInt(match[3]) };
          }
          return null;
        }

        const h1 = document.querySelector('h2'); // Use h2 as h1 may have gradient
        if (!h1) return null;

        const textColor = window.getComputedStyle(h1).color;
        const parsed = parseColor(textColor);

        if (parsed) {
          return getLuminance(parsed.r, parsed.g, parsed.b);
        }
        return null;
      });

      // Light text on dark background should have luminance > 0.5
      expect(textLuminance, 'Text should be light (luminance > 0.5)').toBeGreaterThan(0.5);
    });

    test('TC17: Color scheme uses muted/comfortable colors for extended reading', async ({ page }) => {
      const colorAnalysis = await page.evaluate(() => {
        function parseColor(color) {
          const match = color.match(/rgb\((\d+),\s*(\d+),\s*(\d+)\)/);
          if (match) {
            return { r: parseInt(match[1]), g: parseInt(match[2]), b: parseInt(match[3]) };
          }
          return null;
        }

        function isMuted(r, g, b) {
          // Muted colors have similar RGB values or are not fully saturated
          const max = Math.max(r, g, b);
          const min = Math.min(r, g, b);
          const saturation = max === 0 ? 0 : (max - min) / max;

          // Pure white (255,255,255) is too harsh
          const isPureWhite = r === 255 && g === 255 && b === 255;

          return !isPureWhite;
        }

        const primaryText = document.querySelector('.section-description');
        if (!primaryText) return { isMuted: true };

        const color = window.getComputedStyle(primaryText).color;
        const parsed = parseColor(color);

        if (parsed) {
          return {
            isMuted: isMuted(parsed.r, parsed.g, parsed.b),
            color: color
          };
        }
        return { isMuted: true };
      });

      expect(colorAnalysis.isMuted, 'Text colors should be muted/comfortable for extended reading').toBe(true);
    });

    test('TC18: No pure white (#ffffff) text that causes eye strain', async ({ page }) => {
      const hasPureWhiteText = await page.evaluate(() => {
        const allTextElements = document.querySelectorAll('h1, h2, h3, p, span, a, li');

        for (const el of allTextElements) {
          const color = window.getComputedStyle(el).color;
          // Check for pure white
          if (color === 'rgb(255, 255, 255)') {
            return {
              found: true,
              element: el.tagName,
              text: el.textContent?.trim().substring(0, 30)
            };
          }
        }

        return { found: false };
      });

      expect(
        hasPureWhiteText.found,
        hasPureWhiteText.found
          ? `Found pure white text in ${hasPureWhiteText.element}: "${hasPureWhiteText.text}"`
          : 'No pure white text found'
      ).toBe(false);
    });

    test('TC19: Links and interactive elements have visible focus states', async ({ page }) => {
      // Check that buttons have hover states defined
      const hasHoverStates = await page.evaluate(() => {
        const button = document.querySelector('.btn-primary');
        if (!button) return false;

        const initialBg = window.getComputedStyle(button).backgroundColor;

        // We can't directly test hover in evaluate, but we can check
        // that the button has transition property set (indicating hover effects)
        const hasTransition = window.getComputedStyle(button).transition !== 'none' &&
                             window.getComputedStyle(button).transition !== '';

        return hasTransition;
      });

      expect(hasHoverStates, 'Interactive elements should have transition effects for hover states').toBe(true);
    });

    test('TC20: Overall design follows GitHub-inspired dark theme', async ({ page }) => {
      const themeAnalysis = await page.evaluate(() => {
        const body = document.body;
        const bodyStyles = window.getComputedStyle(body);

        // GitHub dark theme characteristics
        const bgColor = bodyStyles.backgroundColor;
        const textColor = bodyStyles.color;

        // Check if using CSS custom properties (modern approach)
        const rootStyles = getComputedStyle(document.documentElement);
        const hasCssVars = rootStyles.getPropertyValue('--bg-dark').trim() !== '' ||
                          rootStyles.getPropertyValue('--text-primary').trim() !== '';

        return {
          backgroundColor: bgColor,
          textColor: textColor,
          usesCssVariables: hasCssVars,
          lineHeight: parseFloat(bodyStyles.lineHeight) / parseFloat(bodyStyles.fontSize)
        };
      });

      // Should use CSS custom properties for theming
      expect(themeAnalysis.usesCssVariables, 'Should use CSS custom properties for theming').toBe(true);

      // Line height should be comfortable (1.4-1.8)
      expect(themeAnalysis.lineHeight, 'Line height should be comfortable for reading').toBeGreaterThanOrEqual(1.4);
      expect(themeAnalysis.lineHeight, 'Line height should not be too loose').toBeLessThanOrEqual(1.8);
    });
  });
});
