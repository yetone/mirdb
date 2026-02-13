/**
 * Integration Tests for Accessibility Compliance (WCAG 2.1 AA)
 * Owner: Scenario 9 - Accessibility Compliance
 *
 * Tests:
 * - Color contrast ratios
 * - axe-core accessibility audit
 */

const { test, expect } = require('@playwright/test');
const AxeBuilder = require('@axe-core/playwright').default;

/**
 * Calculate relative luminance of an RGB color
 * @param {number} r - Red component (0-255)
 * @param {number} g - Green component (0-255)
 * @param {number} b - Blue component (0-255)
 * @returns {number} Relative luminance
 */
function getLuminance(r, g, b) {
  const [rs, gs, bs] = [r, g, b].map(c => {
    c = c / 255;
    return c <= 0.03928 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4);
  });
  return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
}

/**
 * Parse RGB or hex color string to RGB array
 * @param {string} color - Color string
 * @returns {number[]} RGB array [r, g, b]
 */
function parseColor(color) {
  // Handle rgb format
  const rgbMatch = color.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/);
  if (rgbMatch) {
    return [parseInt(rgbMatch[1]), parseInt(rgbMatch[2]), parseInt(rgbMatch[3])];
  }

  // Handle hex format
  const hexMatch = color.match(/#([0-9a-f]{2})([0-9a-f]{2})([0-9a-f]{2})/i);
  if (hexMatch) {
    return [parseInt(hexMatch[1], 16), parseInt(hexMatch[2], 16), parseInt(hexMatch[3], 16)];
  }

  return [0, 0, 0];
}

/**
 * Calculate contrast ratio between two colors
 * @param {string} foreground - Foreground color string
 * @param {string} background - Background color string
 * @returns {number} Contrast ratio
 */
function getContrastRatio(foreground, background) {
  const [r1, g1, b1] = parseColor(foreground);
  const [r2, g2, b2] = parseColor(background);

  const l1 = getLuminance(r1, g1, b1);
  const l2 = getLuminance(r2, g2, b2);

  const lighter = Math.max(l1, l2);
  const darker = Math.min(l1, l2);

  return (lighter + 0.05) / (darker + 0.05);
}

test.describe('Accessibility Integration Tests', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Test Case 7: Body text meets 4.5:1 contrast ratio', async ({ page }) => {
    // Test in light theme
    await page.evaluate(() => {
      if (window.setTheme) {
        window.setTheme('light');
      }
    });

    await page.waitForTimeout(300);

    const lightColors = await page.evaluate(() => {
      const body = document.body;
      const style = getComputedStyle(body);
      return {
        color: style.color,
        background: style.backgroundColor
      };
    });

    const lightContrast = getContrastRatio(lightColors.color, lightColors.background);
    expect(lightContrast).toBeGreaterThanOrEqual(4.5);

    // Test in dark theme
    await page.evaluate(() => {
      if (window.setTheme) {
        window.setTheme('dark');
      }
    });

    await page.waitForFunction(() => document.documentElement.getAttribute('data-theme') === 'dark');
    await page.waitForTimeout(300);

    const darkColors = await page.evaluate(() => {
      const body = document.body;
      const style = getComputedStyle(body);
      return {
        color: style.color,
        background: style.backgroundColor
      };
    });

    const darkContrast = getContrastRatio(darkColors.color, darkColors.background);
    expect(darkContrast).toBeGreaterThanOrEqual(4.5);
  });

  test('Test Case 7: Large text meets 3:1 contrast ratio (hero title)', async ({ page }) => {
    const heroColors = await page.evaluate(() => {
      const hero = document.querySelector('.hero__title, h1');
      if (!hero) return null;

      const style = getComputedStyle(hero);

      // Walk up to find background color
      let bgColor = 'rgb(255, 255, 255)';
      let element = hero;
      while (element && element !== document.documentElement) {
        const parentStyle = getComputedStyle(element);
        const bg = parentStyle.backgroundColor;
        if (bg && bg !== 'rgba(0, 0, 0, 0)' && bg !== 'transparent') {
          bgColor = bg;
          break;
        }
        element = element.parentElement;
      }

      // Fall back to body background
      if (bgColor === 'rgb(255, 255, 255)' || bgColor === 'rgba(0, 0, 0, 0)') {
        bgColor = getComputedStyle(document.body).backgroundColor;
      }

      return {
        color: style.color,
        background: bgColor
      };
    });

    if (heroColors) {
      const contrastRatio = getContrastRatio(heroColors.color, heroColors.background);
      // Large text needs 3:1 minimum
      expect(contrastRatio).toBeGreaterThanOrEqual(3);
    }
  });

  test('Test Case 10: axe-core audit - zero critical or serious violations', async ({ page }) => {
    // Run axe accessibility scan
    const accessibilityScanResults = await new AxeBuilder({ page })
      .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
      .analyze();

    // Filter for critical and serious violations only
    const criticalViolations = accessibilityScanResults.violations.filter(
      v => v.impact === 'critical' || v.impact === 'serious'
    );

    // Log violations for debugging
    if (criticalViolations.length > 0) {
      console.log('Critical/Serious Accessibility Violations:');
      criticalViolations.forEach(violation => {
        console.log(`- ${violation.id}: ${violation.description}`);
        console.log(`  Impact: ${violation.impact}`);
        console.log(`  Elements: ${violation.nodes.length}`);
        violation.nodes.forEach(node => {
          console.log(`    - ${node.html.substring(0, 100)}`);
        });
      });
    }

    expect(criticalViolations).toHaveLength(0);
  });

  test('Links have sufficient contrast in both themes', async ({ page }) => {
    // Check link contrast in light theme
    const linkContrast = await page.evaluate(() => {
      const links = document.querySelectorAll('a:not(.nav__logo)');
      const results = [];

      links.forEach(link => {
        const style = getComputedStyle(link);
        const color = style.color;

        // Get background color by walking up the DOM
        let bgColor = 'rgb(255, 255, 255)';
        let element = link;
        while (element && element !== document.documentElement) {
          const parentStyle = getComputedStyle(element);
          const bg = parentStyle.backgroundColor;
          if (bg && bg !== 'rgba(0, 0, 0, 0)' && bg !== 'transparent') {
            bgColor = bg;
            break;
          }
          element = element.parentElement;
        }

        if (bgColor === 'rgb(255, 255, 255)' || bgColor === 'rgba(0, 0, 0, 0)') {
          bgColor = getComputedStyle(document.body).backgroundColor;
        }

        results.push({
          text: link.textContent.substring(0, 30),
          color,
          background: bgColor
        });
      });

      return results;
    });

    linkContrast.forEach(link => {
      const ratio = getContrastRatio(link.color, link.background);
      // Links should meet at least 3:1 for large text or 4.5:1 for normal
      expect(ratio).toBeGreaterThanOrEqual(3);
    });
  });

  test('Buttons have sufficient contrast', async ({ page }) => {
    const buttonContrast = await page.evaluate(() => {
      const buttons = document.querySelectorAll('button, .hero__cta');
      const results = [];

      buttons.forEach(button => {
        const style = getComputedStyle(button);
        const color = style.color;
        let bgColor = style.backgroundColor;

        // If transparent, walk up
        if (bgColor === 'rgba(0, 0, 0, 0)' || bgColor === 'transparent') {
          let element = button.parentElement;
          while (element && element !== document.documentElement) {
            const parentStyle = getComputedStyle(element);
            const bg = parentStyle.backgroundColor;
            if (bg && bg !== 'rgba(0, 0, 0, 0)' && bg !== 'transparent') {
              bgColor = bg;
              break;
            }
            element = element.parentElement;
          }
        }

        if (bgColor === 'rgba(0, 0, 0, 0)' || bgColor === 'transparent') {
          bgColor = getComputedStyle(document.body).backgroundColor;
        }

        results.push({
          text: button.textContent?.substring(0, 30) || 'unnamed',
          color,
          background: bgColor
        });
      });

      return results;
    });

    buttonContrast.forEach(button => {
      const ratio = getContrastRatio(button.color, button.background);
      // Buttons should meet contrast requirements
      expect(ratio).toBeGreaterThanOrEqual(3);
    });
  });
});
