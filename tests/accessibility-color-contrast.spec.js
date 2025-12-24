// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Accessibility - Color Contrast Tests
 *
 * These tests verify sufficient color contrast ratios as specified in NFR-3 (WCAG 2.1 AA).
 * WCAG AA requirements:
 * - Normal text: 4.5:1 contrast ratio
 * - Large text (18pt+ or 14pt+ bold): 3:1 contrast ratio
 * - UI components and graphical objects: 3:1 contrast ratio
 */

test.describe('Accessibility - Color Contrast', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('should have body text with at least 4.5:1 contrast ratio', async ({ page }) => {
    // Test Case 1: Measure body text contrast ratio
    // Expected: Body text has at least 4.5:1 contrast ratio (WCAG AA for normal text)

    const bodyTextContrasts = await page.evaluate(() => {
      const results = [];

      // Helper functions for WCAG contrast calculations
      function getRelativeLuminance(r, g, b) {
        const [rs, gs, bs] = [r, g, b].map(c => {
          const sRGB = c / 255;
          return sRGB <= 0.03928 ? sRGB / 12.92 : Math.pow((sRGB + 0.055) / 1.055, 2.4);
        });
        return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
      }

      function getContrastRatio(l1, l2) {
        const lighter = Math.max(l1, l2);
        const darker = Math.min(l1, l2);
        return (lighter + 0.05) / (darker + 0.05);
      }

      function parseColor(color) {
        if (!color || color === 'transparent' || color === 'rgba(0, 0, 0, 0)') {
          return null;
        }
        const rgbMatch = color.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/);
        if (rgbMatch) {
          return {
            r: parseInt(rgbMatch[1], 10),
            g: parseInt(rgbMatch[2], 10),
            b: parseInt(rgbMatch[3], 10)
          };
        }
        return null;
      }

      // Check if element or ancestors have gradient background (hero section)
      function hasGradientBackground(element) {
        let el = element;
        while (el) {
          const style = window.getComputedStyle(el);
          if (style.backgroundImage && style.backgroundImage.includes('gradient')) {
            return true;
          }
          el = el.parentElement;
        }
        return false;
      }

      function getEffectiveBackgroundColor(element) {
        let el = element;
        while (el) {
          const style = window.getComputedStyle(el);

          // Check for gradient background - use darkest color in gradient for conservative estimate
          if (style.backgroundImage && style.backgroundImage.includes('gradient')) {
            // Hero section gradient goes from #1e3a5f to #0f172a - use the darker one
            return { r: 15, g: 23, b: 42 }; // #0f172a
          }

          const bgColor = parseColor(style.backgroundColor);
          if (bgColor) {
            return bgColor;
          }
          el = el.parentElement;
        }
        // Default to white if no background found
        return { r: 255, g: 255, b: 255 };
      }

      // Select body text elements (paragraphs) - exclude those in hero/gradient areas for this test
      const bodyTextElements = document.querySelectorAll(
        '.features p, .feature-item p, .arch-detail p, .architecture-intro, .config-note, ' +
        '.getting-started p, .configuration > .container > p, .footer-note, .copyright'
      );

      bodyTextElements.forEach(element => {
        const style = window.getComputedStyle(element);
        const fontSize = parseFloat(style.fontSize);

        // Only check normal text (less than 18px or less than 14px if not bold)
        const fontWeight = parseInt(style.fontWeight, 10) || 400;
        const isLargeText = fontSize >= 24 || (fontSize >= 18.66 && fontWeight >= 700);

        if (!isLargeText) {
          const textColor = parseColor(style.color);
          const bgColor = getEffectiveBackgroundColor(element);

          if (textColor && bgColor) {
            const textLum = getRelativeLuminance(textColor.r, textColor.g, textColor.b);
            const bgLum = getRelativeLuminance(bgColor.r, bgColor.g, bgColor.b);
            const ratio = getContrastRatio(textLum, bgLum);

            results.push({
              element: element.tagName + (element.className ? '.' + element.className.split(' ')[0] : ''),
              textContent: element.textContent?.slice(0, 50) + '...',
              fontSize: fontSize,
              textColor: style.color,
              effectiveBgColor: `rgb(${bgColor.r}, ${bgColor.g}, ${bgColor.b})`,
              contrastRatio: Math.round(ratio * 100) / 100,
              passes: ratio >= 4.5
            });
          }
        }
      });

      return results;
    });

    // Verify all body text passes 4.5:1 contrast requirement
    const failingElements = bodyTextContrasts.filter(item => !item.passes);

    if (failingElements.length > 0) {
      console.log('Failing body text elements:', JSON.stringify(failingElements, null, 2));
    }

    expect(failingElements.length).toBe(0);

    // Additional assertion: verify we actually tested some elements
    expect(bodyTextContrasts.length).toBeGreaterThan(0);
  });

  test('should have headings with at least 3:1 contrast ratio (large text)', async ({ page }) => {
    // Test Case 2: Measure heading contrast ratio
    // Expected: Headings have at least 3:1 contrast ratio (large text requirement)

    const headingContrasts = await page.evaluate(() => {
      const results = [];

      function getRelativeLuminance(r, g, b) {
        const [rs, gs, bs] = [r, g, b].map(c => {
          const sRGB = c / 255;
          return sRGB <= 0.03928 ? sRGB / 12.92 : Math.pow((sRGB + 0.055) / 1.055, 2.4);
        });
        return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
      }

      function getContrastRatio(l1, l2) {
        const lighter = Math.max(l1, l2);
        const darker = Math.min(l1, l2);
        return (lighter + 0.05) / (darker + 0.05);
      }

      function parseColor(color) {
        if (!color || color === 'transparent' || color === 'rgba(0, 0, 0, 0)') {
          return null;
        }
        const rgbMatch = color.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/);
        if (rgbMatch) {
          return {
            r: parseInt(rgbMatch[1], 10),
            g: parseInt(rgbMatch[2], 10),
            b: parseInt(rgbMatch[3], 10)
          };
        }
        return null;
      }

      function getEffectiveBackgroundColor(element) {
        let el = element;
        while (el) {
          const style = window.getComputedStyle(el);

          // Check for gradient background
          if (style.backgroundImage && style.backgroundImage.includes('gradient')) {
            // Hero section gradient - use the darkest color (#0f172a)
            return { r: 15, g: 23, b: 42 };
          }

          const bgColor = parseColor(style.backgroundColor);
          if (bgColor) {
            return bgColor;
          }
          el = el.parentElement;
        }
        return { r: 255, g: 255, b: 255 };
      }

      // Select all headings
      const headings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');

      headings.forEach(heading => {
        const style = window.getComputedStyle(heading);
        const fontSize = parseFloat(style.fontSize);
        const textColor = parseColor(style.color);
        const bgColor = getEffectiveBackgroundColor(heading);

        if (textColor && bgColor) {
          const textLum = getRelativeLuminance(textColor.r, textColor.g, textColor.b);
          const bgLum = getRelativeLuminance(bgColor.r, bgColor.g, bgColor.b);
          const ratio = getContrastRatio(textLum, bgLum);

          // Large text requires 3:1 ratio
          results.push({
            element: heading.tagName,
            text: heading.textContent?.trim().slice(0, 50),
            fontSize: fontSize,
            textColor: style.color,
            effectiveBgColor: `rgb(${bgColor.r}, ${bgColor.g}, ${bgColor.b})`,
            contrastRatio: Math.round(ratio * 100) / 100,
            passes: ratio >= 3.0
          });
        }
      });

      return results;
    });

    // Verify all headings pass 3:1 contrast requirement
    const failingHeadings = headingContrasts.filter(item => !item.passes);

    if (failingHeadings.length > 0) {
      console.log('Failing headings:', JSON.stringify(failingHeadings, null, 2));
    }

    expect(failingHeadings.length).toBe(0);

    // Verify we tested some headings
    expect(headingContrasts.length).toBeGreaterThan(0);
  });

  test('should have buttons with sufficient contrast ratios', async ({ page }) => {
    // Test Case 3: Check button contrast
    // Expected: Button text and backgrounds meet contrast requirements

    const buttonContrasts = await page.evaluate(() => {
      const results = [];

      function getRelativeLuminance(r, g, b) {
        const [rs, gs, bs] = [r, g, b].map(c => {
          const sRGB = c / 255;
          return sRGB <= 0.03928 ? sRGB / 12.92 : Math.pow((sRGB + 0.055) / 1.055, 2.4);
        });
        return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
      }

      function getContrastRatio(l1, l2) {
        const lighter = Math.max(l1, l2);
        const darker = Math.min(l1, l2);
        return (lighter + 0.05) / (darker + 0.05);
      }

      function parseColor(color) {
        if (!color || color === 'transparent' || color === 'rgba(0, 0, 0, 0)') {
          return null;
        }
        const rgbMatch = color.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/);
        if (rgbMatch) {
          return {
            r: parseInt(rgbMatch[1], 10),
            g: parseInt(rgbMatch[2], 10),
            b: parseInt(rgbMatch[3], 10)
          };
        }
        return null;
      }

      function getEffectiveBackgroundColor(element) {
        let el = element;
        while (el) {
          const style = window.getComputedStyle(el);

          // Check for gradient background
          if (style.backgroundImage && style.backgroundImage.includes('gradient')) {
            return { r: 15, g: 23, b: 42 }; // #0f172a (darkest in hero gradient)
          }

          const bgColor = parseColor(style.backgroundColor);
          if (bgColor) {
            return bgColor;
          }
          el = el.parentElement;
        }
        return { r: 255, g: 255, b: 255 };
      }

      // Select buttons and button-like elements
      const buttons = document.querySelectorAll('button, .btn, [role="button"], a.btn-primary, a.btn-secondary');

      buttons.forEach(button => {
        const style = window.getComputedStyle(button);
        const textColor = parseColor(style.color);
        let bgColor = parseColor(style.backgroundColor);

        // If button has transparent background (like btn-secondary), use the effective background
        if (!bgColor) {
          bgColor = getEffectiveBackgroundColor(button);
        }

        if (textColor && bgColor) {
          const textLum = getRelativeLuminance(textColor.r, textColor.g, textColor.b);
          const bgLum = getRelativeLuminance(bgColor.r, bgColor.g, bgColor.b);
          const textBgRatio = getContrastRatio(textLum, bgLum);

          // Buttons need 4.5:1 for text-background contrast
          results.push({
            element: button.tagName + (button.className ? '.' + button.className.split(' ')[0] : ''),
            text: button.textContent?.trim(),
            textColor: style.color,
            effectiveBgColor: `rgb(${bgColor.r}, ${bgColor.g}, ${bgColor.b})`,
            textBgContrastRatio: Math.round(textBgRatio * 100) / 100,
            passesTextContrast: textBgRatio >= 4.5
          });
        }
      });

      return results;
    });

    // Verify all buttons pass contrast requirements
    const failingButtons = buttonContrasts.filter(item => !item.passesTextContrast);

    if (failingButtons.length > 0) {
      console.log('Failing buttons:', JSON.stringify(failingButtons, null, 2));
    }

    expect(failingButtons.length).toBe(0);

    // Verify we tested some buttons
    expect(buttonContrasts.length).toBeGreaterThan(0);
  });

  test('should have navigation links with sufficient contrast', async ({ page }) => {
    // Additional check for navigation link contrast in header and footer

    const navLinkContrasts = await page.evaluate(() => {
      const results = [];

      function getRelativeLuminance(r, g, b) {
        const [rs, gs, bs] = [r, g, b].map(c => {
          const sRGB = c / 255;
          return sRGB <= 0.03928 ? sRGB / 12.92 : Math.pow((sRGB + 0.055) / 1.055, 2.4);
        });
        return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
      }

      function getContrastRatio(l1, l2) {
        const lighter = Math.max(l1, l2);
        const darker = Math.min(l1, l2);
        return (lighter + 0.05) / (darker + 0.05);
      }

      function parseColor(color) {
        if (!color || color === 'transparent' || color === 'rgba(0, 0, 0, 0)') {
          return null;
        }
        const rgbMatch = color.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/);
        if (rgbMatch) {
          return {
            r: parseInt(rgbMatch[1], 10),
            g: parseInt(rgbMatch[2], 10),
            b: parseInt(rgbMatch[3], 10)
          };
        }
        return null;
      }

      function getEffectiveBackgroundColor(element) {
        let el = element;
        while (el) {
          const style = window.getComputedStyle(el);

          // Check for gradient background
          if (style.backgroundImage && style.backgroundImage.includes('gradient')) {
            return { r: 15, g: 23, b: 42 }; // #0f172a
          }

          const bgColor = parseColor(style.backgroundColor);
          if (bgColor) {
            return bgColor;
          }
          el = el.parentElement;
        }
        return { r: 255, g: 255, b: 255 };
      }

      // Check navigation links (excluding buttons)
      const navLinks = document.querySelectorAll('nav a:not(.btn), .header-nav a:not(.btn), .footer-links a');

      navLinks.forEach(link => {
        const style = window.getComputedStyle(link);
        const textColor = parseColor(style.color);
        const bgColor = getEffectiveBackgroundColor(link);

        if (textColor && bgColor) {
          const textLum = getRelativeLuminance(textColor.r, textColor.g, textColor.b);
          const bgLum = getRelativeLuminance(bgColor.r, bgColor.g, bgColor.b);
          const ratio = getContrastRatio(textLum, bgLum);

          results.push({
            text: link.textContent?.trim(),
            textColor: style.color,
            effectiveBgColor: `rgb(${bgColor.r}, ${bgColor.g}, ${bgColor.b})`,
            contrastRatio: Math.round(ratio * 100) / 100,
            passes: ratio >= 4.5
          });
        }
      });

      return results;
    });

    // Navigation links should pass 4.5:1 contrast
    const failingLinks = navLinkContrasts.filter(item => !item.passes);

    if (failingLinks.length > 0) {
      console.log('Failing nav links:', JSON.stringify(failingLinks, null, 2));
    }

    expect(failingLinks.length).toBe(0);
  });

  test('should have code blocks with sufficient contrast', async ({ page }) => {
    // Check code blocks for contrast (code-bg against code-text)

    const codeContrasts = await page.evaluate(() => {
      const results = [];

      function getRelativeLuminance(r, g, b) {
        const [rs, gs, bs] = [r, g, b].map(c => {
          const sRGB = c / 255;
          return sRGB <= 0.03928 ? sRGB / 12.92 : Math.pow((sRGB + 0.055) / 1.055, 2.4);
        });
        return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
      }

      function getContrastRatio(l1, l2) {
        const lighter = Math.max(l1, l2);
        const darker = Math.min(l1, l2);
        return (lighter + 0.05) / (darker + 0.05);
      }

      function parseColor(color) {
        if (!color || color === 'transparent' || color === 'rgba(0, 0, 0, 0)') {
          return null;
        }
        const rgbMatch = color.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/);
        if (rgbMatch) {
          return {
            r: parseInt(rgbMatch[1], 10),
            g: parseInt(rgbMatch[2], 10),
            b: parseInt(rgbMatch[3], 10)
          };
        }
        return null;
      }

      // Check code blocks - only those with both explicit text and background colors
      const codeBlocks = document.querySelectorAll('.code-block, .diagram, pre');

      codeBlocks.forEach(block => {
        const style = window.getComputedStyle(block);
        const textColor = parseColor(style.color);
        const bgColor = parseColor(style.backgroundColor);

        if (textColor && bgColor) {
          const textLum = getRelativeLuminance(textColor.r, textColor.g, textColor.b);
          const bgLum = getRelativeLuminance(bgColor.r, bgColor.g, bgColor.b);
          const ratio = getContrastRatio(textLum, bgLum);

          results.push({
            element: block.className || block.tagName,
            textColor: style.color,
            bgColor: style.backgroundColor,
            contrastRatio: Math.round(ratio * 100) / 100,
            passes: ratio >= 4.5
          });
        }
      });

      return results;
    });

    // Code blocks should pass 4.5:1 contrast
    const failingCode = codeContrasts.filter(item => !item.passes);

    if (failingCode.length > 0) {
      console.log('Failing code blocks:', JSON.stringify(failingCode, null, 2));
    }

    expect(failingCode.length).toBe(0);
  });

  test('should have table elements with sufficient contrast', async ({ page }) => {
    // Check configuration table for contrast

    const tableContrasts = await page.evaluate(() => {
      const results = [];

      function getRelativeLuminance(r, g, b) {
        const [rs, gs, bs] = [r, g, b].map(c => {
          const sRGB = c / 255;
          return sRGB <= 0.03928 ? sRGB / 12.92 : Math.pow((sRGB + 0.055) / 1.055, 2.4);
        });
        return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
      }

      function getContrastRatio(l1, l2) {
        const lighter = Math.max(l1, l2);
        const darker = Math.min(l1, l2);
        return (lighter + 0.05) / (darker + 0.05);
      }

      function parseColor(color) {
        if (!color || color === 'transparent' || color === 'rgba(0, 0, 0, 0)') {
          return null;
        }
        const rgbMatch = color.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/);
        if (rgbMatch) {
          return {
            r: parseInt(rgbMatch[1], 10),
            g: parseInt(rgbMatch[2], 10),
            b: parseInt(rgbMatch[3], 10)
          };
        }
        return null;
      }

      function getEffectiveBackgroundColor(element) {
        let el = element;
        while (el) {
          const style = window.getComputedStyle(el);
          const bgColor = parseColor(style.backgroundColor);
          if (bgColor) {
            return bgColor;
          }
          el = el.parentElement;
        }
        return { r: 255, g: 255, b: 255 };
      }

      // Check table headers and cells
      const tableElements = document.querySelectorAll('.config-table th, .config-table td');

      tableElements.forEach(element => {
        const style = window.getComputedStyle(element);
        const textColor = parseColor(style.color);
        const bgColor = parseColor(style.backgroundColor) || getEffectiveBackgroundColor(element);

        if (textColor && bgColor) {
          const textLum = getRelativeLuminance(textColor.r, textColor.g, textColor.b);
          const bgLum = getRelativeLuminance(bgColor.r, bgColor.g, bgColor.b);
          const ratio = getContrastRatio(textLum, bgLum);

          results.push({
            element: element.tagName,
            text: element.textContent?.slice(0, 30),
            textColor: style.color,
            effectiveBgColor: `rgb(${bgColor.r}, ${bgColor.g}, ${bgColor.b})`,
            contrastRatio: Math.round(ratio * 100) / 100,
            passes: ratio >= 4.5
          });
        }
      });

      return results;
    });

    // Table content should pass 4.5:1 contrast
    const failingTable = tableContrasts.filter(item => !item.passes);

    if (failingTable.length > 0) {
      console.log('Failing table elements:', JSON.stringify(failingTable, null, 2));
    }

    expect(failingTable.length).toBe(0);
  });
});
