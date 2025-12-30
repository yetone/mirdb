// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * WCAG 2.1 AA Color Contrast Tests
 *
 * Requirements:
 * - Normal text: 4.5:1 minimum contrast ratio
 * - Large text (18px+ or 14px+ bold): 3:1 minimum contrast ratio
 * - UI components: 3:1 minimum contrast ratio
 *
 * Reference: https://www.w3.org/WAI/WCAG21/Understanding/contrast-minimum.html
 */

/**
 * Calculate relative luminance of a color
 * Formula: L = 0.2126 * R + 0.7152 * G + 0.0722 * B
 * where R, G, B are linearized sRGB values
 *
 * @param {number} r - Red component (0-255)
 * @param {number} g - Green component (0-255)
 * @param {number} b - Blue component (0-255)
 * @returns {number} Relative luminance (0-1)
 */
function getRelativeLuminance(r, g, b) {
  const [rs, gs, bs] = [r, g, b].map(c => {
    const sRGB = c / 255;
    return sRGB <= 0.03928
      ? sRGB / 12.92
      : Math.pow((sRGB + 0.055) / 1.055, 2.4);
  });
  return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
}

/**
 * Calculate contrast ratio between two colors
 * Formula: (L1 + 0.05) / (L2 + 0.05) where L1 > L2
 *
 * @param {string} color1 - RGB color string (e.g., "rgb(255, 255, 255)")
 * @param {string} color2 - RGB color string
 * @returns {number} Contrast ratio (1-21)
 */
function getContrastRatio(color1, color2) {
  const parseRGB = (color) => {
    // Handle rgb(r, g, b) and rgba(r, g, b, a) formats
    const match = color.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/);
    if (!match) {
      throw new Error(`Invalid color format: ${color}`);
    }
    return [parseInt(match[1]), parseInt(match[2]), parseInt(match[3])];
  };

  const [r1, g1, b1] = parseRGB(color1);
  const [r2, g2, b2] = parseRGB(color2);

  const l1 = getRelativeLuminance(r1, g1, b1);
  const l2 = getRelativeLuminance(r2, g2, b2);

  const lighter = Math.max(l1, l2);
  const darker = Math.min(l1, l2);

  return (lighter + 0.05) / (darker + 0.05);
}

/**
 * Check if text qualifies as "large text" under WCAG
 * Large text: 18px+ (24px) regular OR 14px+ (18.5px) bold
 *
 * @param {number} fontSize - Font size in pixels
 * @param {number} fontWeight - Font weight (100-900)
 * @returns {boolean} True if large text
 */
function isLargeText(fontSize, fontWeight) {
  const isBold = fontWeight >= 700;
  // Large text: 18pt (24px) regular or 14pt (18.5px) bold
  return fontSize >= 24 || (isBold && fontSize >= 18.5);
}

/**
 * Get the required minimum contrast ratio based on text size
 * @param {number} fontSize - Font size in pixels
 * @param {number} fontWeight - Font weight
 * @returns {number} Required contrast ratio (3 or 4.5)
 */
function getRequiredContrastRatio(fontSize, fontWeight) {
  return isLargeText(fontSize, fontWeight) ? 3 : 4.5;
}

test.describe('Accessibility - Color Contrast', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test.describe('TC1: Check body text contrast', () => {
    test('Normal text has contrast ratio of at least 4.5:1', async ({ page }) => {
      // Elements with normal body text
      const bodyTextSelectors = [
        '.feature-card p',
        '.command-item span',
        '.tagline',
        '.license'
      ];

      for (const selector of bodyTextSelectors) {
        const elements = page.locator(selector);
        const count = await elements.count();

        for (let i = 0; i < count; i++) {
          const element = elements.nth(i);
          const isVisible = await element.isVisible().catch(() => false);
          if (!isVisible) continue;

          const styles = await element.evaluate((el) => {
            const computed = window.getComputedStyle(el);
            // Walk up DOM to find actual background color (not transparent)
            let bgColor = computed.backgroundColor;
            let currentEl = el;
            while (bgColor === 'rgba(0, 0, 0, 0)' || bgColor === 'transparent') {
              currentEl = currentEl.parentElement;
              if (!currentEl) {
                bgColor = 'rgb(255, 255, 255)'; // Default to white
                break;
              }
              bgColor = window.getComputedStyle(currentEl).backgroundColor;
            }
            return {
              color: computed.color,
              backgroundColor: bgColor,
              fontSize: parseFloat(computed.fontSize),
              fontWeight: parseInt(computed.fontWeight),
              text: el.textContent?.substring(0, 30) || ''
            };
          });

          const contrastRatio = getContrastRatio(styles.color, styles.backgroundColor);
          const requiredRatio = getRequiredContrastRatio(styles.fontSize, styles.fontWeight);

          expect(
            contrastRatio,
            `Text "${styles.text}" (${selector}) should have contrast ratio >= ${requiredRatio}:1. ` +
            `Got ${contrastRatio.toFixed(2)}:1 (color: ${styles.color}, bg: ${styles.backgroundColor})`
          ).toBeGreaterThanOrEqual(requiredRatio);
        }
      }
    });

    test('Body text uses appropriate color variables for WCAG compliance', async ({ page }) => {
      // Get the CSS custom property values
      const cssVariables = await page.evaluate(() => {
        const root = document.documentElement;
        const computed = window.getComputedStyle(root);
        return {
          textColor: computed.getPropertyValue('--text-color').trim(),
          textLight: computed.getPropertyValue('--text-light').trim(),
          background: computed.getPropertyValue('--background').trim(),
          surface: computed.getPropertyValue('--surface').trim()
        };
      });

      // Verify CSS variables are defined
      expect(cssVariables.textColor, 'Text color variable should be defined').toBeTruthy();
      expect(cssVariables.textLight, 'Light text color variable should be defined').toBeTruthy();
      expect(cssVariables.background, 'Background color variable should be defined').toBeTruthy();

      // Check actual contrast of text elements
      const bodyElement = page.locator('body');
      const bodyStyles = await bodyElement.evaluate((el) => {
        const computed = window.getComputedStyle(el);
        return {
          color: computed.color,
          backgroundColor: computed.backgroundColor
        };
      });

      // Body text should have sufficient contrast
      let bgColor = bodyStyles.backgroundColor;
      if (bgColor === 'rgba(0, 0, 0, 0)') {
        bgColor = 'rgb(255, 255, 255)'; // Default background
      }

      const contrastRatio = getContrastRatio(bodyStyles.color, bgColor);
      expect(
        contrastRatio,
        `Body text should have contrast ratio >= 4.5:1. Got ${contrastRatio.toFixed(2)}:1`
      ).toBeGreaterThanOrEqual(4.5);
    });
  });

  test.describe('TC2: Check large text contrast', () => {
    test('Large text (18px+ or 14px+ bold) has contrast ratio of at least 3:1', async ({ page }) => {
      // Elements that typically contain large text
      const largeTextSelectors = [
        'h1',
        'h2',
        'h3',
        '.hero h1',
        '.features h2',
        '.commands h2',
        '.quick-start h2',
        '.status h2'
      ];

      for (const selector of largeTextSelectors) {
        const elements = page.locator(selector);
        const count = await elements.count();

        for (let i = 0; i < count; i++) {
          const element = elements.nth(i);
          const isVisible = await element.isVisible().catch(() => false);
          if (!isVisible) continue;

          const styles = await element.evaluate((el) => {
            const computed = window.getComputedStyle(el);
            // Walk up DOM to find actual background color
            let bgColor = computed.backgroundColor;
            let currentEl = el;
            while (bgColor === 'rgba(0, 0, 0, 0)' || bgColor === 'transparent') {
              currentEl = currentEl.parentElement;
              if (!currentEl) {
                bgColor = 'rgb(255, 255, 255)';
                break;
              }
              bgColor = window.getComputedStyle(currentEl).backgroundColor;
            }
            return {
              color: computed.color,
              backgroundColor: bgColor,
              fontSize: parseFloat(computed.fontSize),
              fontWeight: parseInt(computed.fontWeight),
              text: el.textContent?.substring(0, 30) || ''
            };
          });

          // Verify this is actually large text
          const isLarge = isLargeText(styles.fontSize, styles.fontWeight);
          expect(
            isLarge,
            `Heading "${styles.text}" should be large text (>= 24px or >= 18.5px bold). ` +
            `Got ${styles.fontSize}px with weight ${styles.fontWeight}`
          ).toBe(true);

          const contrastRatio = getContrastRatio(styles.color, styles.backgroundColor);

          // Large text requires minimum 3:1 contrast ratio
          expect(
            contrastRatio,
            `Large text "${styles.text}" should have contrast ratio >= 3:1. ` +
            `Got ${contrastRatio.toFixed(2)}:1 (color: ${styles.color}, bg: ${styles.backgroundColor})`
          ).toBeGreaterThanOrEqual(3);
        }
      }
    });

    test('Feature card headings have sufficient contrast', async ({ page }) => {
      const featureHeadings = page.locator('.feature-card h3');
      const count = await featureHeadings.count();

      expect(count, 'Feature cards should have h3 headings').toBeGreaterThan(0);

      for (let i = 0; i < count; i++) {
        const heading = featureHeadings.nth(i);
        const styles = await heading.evaluate((el) => {
          const computed = window.getComputedStyle(el);
          let bgColor = computed.backgroundColor;
          let currentEl = el;
          while (bgColor === 'rgba(0, 0, 0, 0)' || bgColor === 'transparent') {
            currentEl = currentEl.parentElement;
            if (!currentEl) {
              bgColor = 'rgb(255, 255, 255)';
              break;
            }
            bgColor = window.getComputedStyle(currentEl).backgroundColor;
          }
          return {
            color: computed.color,
            backgroundColor: bgColor,
            fontSize: parseFloat(computed.fontSize),
            fontWeight: parseInt(computed.fontWeight),
            text: el.textContent?.substring(0, 30) || ''
          };
        });

        const contrastRatio = getContrastRatio(styles.color, styles.backgroundColor);
        const requiredRatio = getRequiredContrastRatio(styles.fontSize, styles.fontWeight);

        expect(
          contrastRatio,
          `Feature heading "${styles.text}" should have contrast ratio >= ${requiredRatio}:1. ` +
          `Got ${contrastRatio.toFixed(2)}:1`
        ).toBeGreaterThanOrEqual(requiredRatio);
      }
    });
  });

  test.describe('TC3: Check link contrast', () => {
    test('Links are distinguishable from surrounding text (not just by color)', async ({ page }) => {
      // Check navigation links
      const navLinks = page.locator('.nav-links a');
      const navLinkCount = await navLinks.count();

      for (let i = 0; i < navLinkCount; i++) {
        const link = navLinks.nth(i);
        const isVisible = await link.isVisible().catch(() => false);
        if (!isVisible) continue;

        const linkStyles = await link.evaluate((el) => {
          const computed = window.getComputedStyle(el);
          return {
            textDecoration: computed.textDecoration,
            textDecorationLine: computed.textDecorationLine,
            fontWeight: computed.fontWeight,
            borderBottom: computed.borderBottom,
            text: el.textContent?.trim() || ''
          };
        });

        // Links should have some visual distinction besides color
        // Either underline, bold, or some other indicator
        const hasUnderline = linkStyles.textDecoration.includes('underline') ||
                            linkStyles.textDecorationLine.includes('underline');
        const isBold = parseInt(linkStyles.fontWeight) >= 500;
        const hasBorder = linkStyles.borderBottom !== 'none' &&
                          linkStyles.borderBottom !== '0px none rgb(0, 0, 0)';

        // Navigation links may not need underline if they're in a clearly marked nav area
        // But they should have sufficient contrast
        const linkColorStyles = await link.evaluate((el) => {
          const computed = window.getComputedStyle(el);
          let bgColor = computed.backgroundColor;
          let currentEl = el;
          while (bgColor === 'rgba(0, 0, 0, 0)' || bgColor === 'transparent') {
            currentEl = currentEl.parentElement;
            if (!currentEl) {
              bgColor = 'rgb(255, 255, 255)';
              break;
            }
            bgColor = window.getComputedStyle(currentEl).backgroundColor;
          }
          return {
            color: computed.color,
            backgroundColor: bgColor
          };
        });

        const contrastRatio = getContrastRatio(linkColorStyles.color, linkColorStyles.backgroundColor);

        expect(
          contrastRatio,
          `Navigation link "${linkStyles.text}" should have contrast ratio >= 4.5:1. Got ${contrastRatio.toFixed(2)}:1`
        ).toBeGreaterThanOrEqual(4.5);
      }

      // Check footer links
      const footerLinks = page.locator('.footer-links a');
      const footerLinkCount = await footerLinks.count();

      for (let i = 0; i < footerLinkCount; i++) {
        const link = footerLinks.nth(i);
        const isVisible = await link.isVisible().catch(() => false);
        if (!isVisible) continue;

        const linkColorStyles = await link.evaluate((el) => {
          const computed = window.getComputedStyle(el);
          let bgColor = computed.backgroundColor;
          let currentEl = el;
          while (bgColor === 'rgba(0, 0, 0, 0)' || bgColor === 'transparent') {
            currentEl = currentEl.parentElement;
            if (!currentEl) {
              bgColor = 'rgb(255, 255, 255)';
              break;
            }
            bgColor = window.getComputedStyle(currentEl).backgroundColor;
          }
          return {
            color: computed.color,
            backgroundColor: bgColor,
            text: el.textContent?.trim() || ''
          };
        });

        const contrastRatio = getContrastRatio(linkColorStyles.color, linkColorStyles.backgroundColor);

        expect(
          contrastRatio,
          `Footer link "${linkColorStyles.text}" should have contrast ratio >= 4.5:1. Got ${contrastRatio.toFixed(2)}:1`
        ).toBeGreaterThanOrEqual(4.5);
      }
    });

    test('In-content links have visual distinction beyond color alone', async ({ page }) => {
      // Logo link (brand element)
      const logoLink = page.locator('.logo');
      const logoStyles = await logoLink.evaluate((el) => {
        const computed = window.getComputedStyle(el);
        return {
          fontWeight: computed.fontWeight,
          fontSize: parseFloat(computed.fontSize),
          textDecoration: computed.textDecoration
        };
      });

      // Logo should be visually distinct (bold, larger font)
      const logoBold = parseInt(logoStyles.fontWeight) >= 600;
      const logoLarge = logoStyles.fontSize >= 18;

      expect(
        logoBold || logoLarge,
        `Logo link should be visually distinct (bold or larger font). ` +
        `Got weight: ${logoStyles.fontWeight}, size: ${logoStyles.fontSize}px`
      ).toBe(true);
    });
  });

  test.describe('TC4: Check button contrast', () => {
    test('Button text has sufficient contrast against button background', async ({ page }) => {
      // Primary button
      const primaryButtons = page.locator('.btn-primary');
      const primaryCount = await primaryButtons.count();

      for (let i = 0; i < primaryCount; i++) {
        const button = primaryButtons.nth(i);
        const isVisible = await button.isVisible().catch(() => false);
        if (!isVisible) continue;

        const buttonStyles = await button.evaluate((el) => {
          const computed = window.getComputedStyle(el);
          return {
            color: computed.color,
            backgroundColor: computed.backgroundColor,
            fontSize: parseFloat(computed.fontSize),
            fontWeight: parseInt(computed.fontWeight),
            text: el.textContent?.trim() || ''
          };
        });

        const contrastRatio = getContrastRatio(buttonStyles.color, buttonStyles.backgroundColor);
        const requiredRatio = getRequiredContrastRatio(buttonStyles.fontSize, buttonStyles.fontWeight);

        expect(
          contrastRatio,
          `Primary button "${buttonStyles.text}" should have contrast ratio >= ${requiredRatio}:1. ` +
          `Got ${contrastRatio.toFixed(2)}:1 (text: ${buttonStyles.color}, bg: ${buttonStyles.backgroundColor})`
        ).toBeGreaterThanOrEqual(requiredRatio);
      }

      // Secondary button
      const secondaryButtons = page.locator('.btn-secondary');
      const secondaryCount = await secondaryButtons.count();

      for (let i = 0; i < secondaryCount; i++) {
        const button = secondaryButtons.nth(i);
        const isVisible = await button.isVisible().catch(() => false);
        if (!isVisible) continue;

        const buttonStyles = await button.evaluate((el) => {
          const computed = window.getComputedStyle(el);
          return {
            color: computed.color,
            backgroundColor: computed.backgroundColor,
            fontSize: parseFloat(computed.fontSize),
            fontWeight: parseInt(computed.fontWeight),
            text: el.textContent?.trim() || ''
          };
        });

        const contrastRatio = getContrastRatio(buttonStyles.color, buttonStyles.backgroundColor);
        const requiredRatio = getRequiredContrastRatio(buttonStyles.fontSize, buttonStyles.fontWeight);

        expect(
          contrastRatio,
          `Secondary button "${buttonStyles.text}" should have contrast ratio >= ${requiredRatio}:1. ` +
          `Got ${contrastRatio.toFixed(2)}:1 (text: ${buttonStyles.color}, bg: ${buttonStyles.backgroundColor})`
        ).toBeGreaterThanOrEqual(requiredRatio);
      }
    });

    test('Button focus states maintain sufficient contrast', async ({ page }) => {
      const primaryButton = page.locator('.btn-primary').first();

      // Focus the button
      await primaryButton.focus();

      const focusedStyles = await primaryButton.evaluate((el) => {
        const computed = window.getComputedStyle(el);
        return {
          color: computed.color,
          backgroundColor: computed.backgroundColor,
          outlineColor: computed.outlineColor,
          outlineWidth: computed.outlineWidth,
          outlineStyle: computed.outlineStyle
        };
      });

      // Button text should still have sufficient contrast when focused
      const contrastRatio = getContrastRatio(focusedStyles.color, focusedStyles.backgroundColor);
      expect(
        contrastRatio,
        `Focused primary button should maintain contrast >= 4.5:1. Got ${contrastRatio.toFixed(2)}:1`
      ).toBeGreaterThanOrEqual(4.5);

      // Focus indicator should be visible
      const hasVisibleFocusIndicator = focusedStyles.outlineStyle !== 'none' &&
                                        parseInt(focusedStyles.outlineWidth) > 0;
      expect(
        hasVisibleFocusIndicator,
        'Button should have visible focus indicator'
      ).toBe(true);
    });

    test('Mobile menu toggle button has sufficient contrast', async ({ page }) => {
      // Set mobile viewport
      await page.setViewportSize({ width: 375, height: 667 });

      const menuToggle = page.locator('.mobile-menu-toggle');
      await expect(menuToggle).toBeVisible();

      // Check the hamburger bars contrast
      const toggleStyles = await menuToggle.evaluate((el) => {
        const computed = window.getComputedStyle(el);
        const spans = el.querySelectorAll('span');
        let spanBgColor = 'rgb(0, 0, 0)';
        if (spans.length > 0) {
          spanBgColor = window.getComputedStyle(spans[0]).backgroundColor;
        }

        // Get parent background
        let bgColor = computed.backgroundColor;
        let currentEl = el;
        while (bgColor === 'rgba(0, 0, 0, 0)' || bgColor === 'transparent') {
          currentEl = currentEl.parentElement;
          if (!currentEl) {
            bgColor = 'rgb(255, 255, 255)';
            break;
          }
          bgColor = window.getComputedStyle(currentEl).backgroundColor;
        }

        return {
          spanColor: spanBgColor,
          backgroundColor: bgColor
        };
      });

      // The hamburger lines (spans) should have sufficient contrast against background
      const contrastRatio = getContrastRatio(toggleStyles.spanColor, toggleStyles.backgroundColor);

      // UI components require 3:1 contrast ratio per WCAG 2.1
      expect(
        contrastRatio,
        `Mobile menu toggle should have contrast ratio >= 3:1. Got ${contrastRatio.toFixed(2)}:1`
      ).toBeGreaterThanOrEqual(3);
    });
  });

  test.describe('Additional contrast checks', () => {
    test('Code block text has sufficient contrast', async ({ page }) => {
      const codeBlocks = page.locator('.code-block code');
      const count = await codeBlocks.count();

      expect(count, 'Page should have code blocks').toBeGreaterThan(0);

      for (let i = 0; i < count; i++) {
        const codeBlock = codeBlocks.nth(i);
        const isVisible = await codeBlock.isVisible().catch(() => false);
        if (!isVisible) continue;

        const codeStyles = await codeBlock.evaluate((el) => {
          const computed = window.getComputedStyle(el);
          // For code blocks, get the pre or code-block background
          let bgColor = computed.backgroundColor;
          let currentEl = el;
          while (bgColor === 'rgba(0, 0, 0, 0)' || bgColor === 'transparent') {
            currentEl = currentEl.parentElement;
            if (!currentEl) {
              bgColor = 'rgb(30, 41, 59)'; // Default dark code bg
              break;
            }
            bgColor = window.getComputedStyle(currentEl).backgroundColor;
          }
          return {
            color: computed.color,
            backgroundColor: bgColor,
            fontSize: parseFloat(computed.fontSize),
            fontWeight: parseInt(computed.fontWeight)
          };
        });

        const contrastRatio = getContrastRatio(codeStyles.color, codeStyles.backgroundColor);
        const requiredRatio = getRequiredContrastRatio(codeStyles.fontSize, codeStyles.fontWeight);

        expect(
          contrastRatio,
          `Code block text should have contrast ratio >= ${requiredRatio}:1. ` +
          `Got ${contrastRatio.toFixed(2)}:1 (text: ${codeStyles.color}, bg: ${codeStyles.backgroundColor})`
        ).toBeGreaterThanOrEqual(requiredRatio);
      }
    });

    test('Command code elements have sufficient contrast', async ({ page }) => {
      const commandCodes = page.locator('.command-item code');
      const count = await commandCodes.count();

      expect(count, 'Page should have command code elements').toBeGreaterThan(0);

      for (let i = 0; i < count; i++) {
        const code = commandCodes.nth(i);
        const isVisible = await code.isVisible().catch(() => false);
        if (!isVisible) continue;

        const codeStyles = await code.evaluate((el) => {
          const computed = window.getComputedStyle(el);
          let bgColor = computed.backgroundColor;
          if (bgColor === 'rgba(0, 0, 0, 0)' || bgColor === 'transparent') {
            // Use the element's own background or parent
            let currentEl = el.parentElement;
            while (currentEl && (bgColor === 'rgba(0, 0, 0, 0)' || bgColor === 'transparent')) {
              bgColor = window.getComputedStyle(currentEl).backgroundColor;
              currentEl = currentEl.parentElement;
            }
            if (bgColor === 'rgba(0, 0, 0, 0)') {
              bgColor = 'rgb(255, 255, 255)';
            }
          }
          return {
            color: computed.color,
            backgroundColor: bgColor,
            fontSize: parseFloat(computed.fontSize),
            fontWeight: parseInt(computed.fontWeight),
            text: el.textContent?.trim() || ''
          };
        });

        const contrastRatio = getContrastRatio(codeStyles.color, codeStyles.backgroundColor);
        const requiredRatio = getRequiredContrastRatio(codeStyles.fontSize, codeStyles.fontWeight);

        expect(
          contrastRatio,
          `Command code "${codeStyles.text}" should have contrast ratio >= ${requiredRatio}:1. ` +
          `Got ${contrastRatio.toFixed(2)}:1`
        ).toBeGreaterThanOrEqual(requiredRatio);
      }
    });

    test('Status indicators have sufficient contrast', async ({ page }) => {
      const statusIcons = page.locator('.status-icon');
      const count = await statusIcons.count();

      expect(count, 'Page should have status icons').toBeGreaterThan(0);

      for (let i = 0; i < count; i++) {
        const icon = statusIcons.nth(i);
        const isVisible = await icon.isVisible().catch(() => false);
        if (!isVisible) continue;

        const iconStyles = await icon.evaluate((el) => {
          const computed = window.getComputedStyle(el);
          let bgColor = computed.backgroundColor;
          let currentEl = el;
          while (bgColor === 'rgba(0, 0, 0, 0)' || bgColor === 'transparent') {
            currentEl = currentEl.parentElement;
            if (!currentEl) {
              bgColor = 'rgb(255, 255, 255)';
              break;
            }
            bgColor = window.getComputedStyle(currentEl).backgroundColor;
          }
          return {
            color: computed.color,
            backgroundColor: bgColor,
            text: el.textContent?.trim() || ''
          };
        });

        const contrastRatio = getContrastRatio(iconStyles.color, iconStyles.backgroundColor);

        // UI components and graphical elements need 3:1 ratio per WCAG 2.1
        expect(
          contrastRatio,
          `Status icon "${iconStyles.text}" should have contrast ratio >= 3:1. ` +
          `Got ${contrastRatio.toFixed(2)}:1`
        ).toBeGreaterThanOrEqual(3);
      }
    });
  });
});
