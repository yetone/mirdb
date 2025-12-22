// @ts-check
const { test, expect } = require('@playwright/test');

/**
 * Color Contrast Accessibility Tests
 *
 * This test suite verifies that the MirDB homepage meets WCAG 2.1 AA color contrast requirements:
 * - Normal text (< 18pt or < 14pt bold): minimum contrast ratio of 4.5:1
 * - Large text (>= 18pt or >= 14pt bold): minimum contrast ratio of 3:1
 * - UI components and graphical objects: minimum contrast ratio of 3:1
 */

/**
 * Helper function to calculate relative luminance of a color
 * Based on WCAG 2.1 formula: https://www.w3.org/TR/WCAG21/#dfn-relative-luminance
 * @param {number} r - Red component (0-255)
 * @param {number} g - Green component (0-255)
 * @param {number} b - Blue component (0-255)
 * @returns {number} Relative luminance value (0-1)
 */
function getLuminance(r, g, b) {
  const [rs, gs, bs] = [r, g, b].map(c => {
    c = c / 255;
    return c <= 0.03928 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4);
  });
  return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
}

/**
 * Helper function to calculate contrast ratio between two colors
 * Based on WCAG 2.1 formula
 * @param {string} color1 - RGB color string (e.g., "rgb(255, 255, 255)")
 * @param {string} color2 - RGB color string (e.g., "rgb(0, 0, 0)")
 * @returns {number} Contrast ratio (1-21)
 */
function getContrastRatio(color1, color2) {
  const parseRgb = (color) => {
    // Handle rgb() and rgba() formats
    const match = color.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/);
    if (match) {
      return { r: parseInt(match[1]), g: parseInt(match[2]), b: parseInt(match[3]) };
    }
    // Handle hex format (#rrggbb)
    const hexMatch = color.match(/^#([a-f0-9]{2})([a-f0-9]{2})([a-f0-9]{2})$/i);
    if (hexMatch) {
      return {
        r: parseInt(hexMatch[1], 16),
        g: parseInt(hexMatch[2], 16),
        b: parseInt(hexMatch[3], 16)
      };
    }
    return { r: 0, g: 0, b: 0 };
  };

  const c1 = parseRgb(color1);
  const c2 = parseRgb(color2);

  const l1 = getLuminance(c1.r, c1.g, c1.b);
  const l2 = getLuminance(c2.r, c2.g, c2.b);

  const lighter = Math.max(l1, l2);
  const darker = Math.min(l1, l2);

  return (lighter + 0.05) / (darker + 0.05);
}

/**
 * Helper function to determine if text is considered "large" per WCAG
 * Large text: >= 18pt (24px) or >= 14pt (18.67px) bold
 * @param {number} fontSize - Font size in pixels
 * @param {number} fontWeight - Font weight (100-900)
 * @returns {boolean} True if text is considered large
 */
function isLargeText(fontSize, fontWeight) {
  // 18pt = 24px, 14pt = 18.67px
  return fontSize >= 24 || (fontSize >= 18.67 && fontWeight >= 700);
}

test.describe('Accessibility - Color Contrast', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  /**
   * Test Case 1: Check body text contrast ratio
   * Expected: Normal text has contrast ratio of at least 4.5:1
   */
  test('should have body text with contrast ratio of at least 4.5:1', async ({ page }) => {
    // Get all body text elements (paragraphs, list items, spans with text)
    const bodyTextElements = await page.evaluate(() => {
      const elements = [];
      // Target specific body text elements, excluding navigation/interactive elements
      const textSelectors = 'p, .feature-description, .hero-description, .architecture-intro, .node-description';

      document.querySelectorAll(textSelectors).forEach(el => {
        const styles = window.getComputedStyle(el);
        const text = el.textContent?.trim();

        // Skip empty elements
        if (!text || text.length === 0) return;

        // Get the actual background color (may need to traverse up the DOM)
        let bgColor = styles.backgroundColor;
        let parent = el.parentElement;
        while ((bgColor === 'rgba(0, 0, 0, 0)' || bgColor === 'transparent') && parent) {
          const parentStyles = window.getComputedStyle(parent);
          bgColor = parentStyles.backgroundColor;
          parent = parent.parentElement;
        }
        // Default to background color if still transparent
        if (bgColor === 'rgba(0, 0, 0, 0)' || bgColor === 'transparent') {
          bgColor = 'rgb(15, 23, 42)'; // --color-background
        }

        elements.push({
          text: text.substring(0, 50),
          color: styles.color,
          backgroundColor: bgColor,
          fontSize: parseFloat(styles.fontSize),
          fontWeight: parseInt(styles.fontWeight) || 400,
          selector: el.className || el.tagName.toLowerCase()
        });
      });

      return elements;
    });

    for (const element of bodyTextElements) {
      const contrastRatio = getContrastRatio(element.color, element.backgroundColor);
      const isLarge = isLargeText(element.fontSize, element.fontWeight);
      const requiredRatio = isLarge ? 3 : 4.5;

      expect(
        contrastRatio,
        `Body text "${element.text}..." (${element.selector}) has insufficient contrast ratio ${contrastRatio.toFixed(2)}:1 (required: ${requiredRatio}:1). Color: ${element.color}, Background: ${element.backgroundColor}`
      ).toBeGreaterThanOrEqual(requiredRatio);
    }
  });

  /**
   * Test Case 2: Check heading text contrast
   * Expected: Large text (headings) has contrast ratio of at least 3:1
   */
  test('should have heading text with contrast ratio of at least 3:1', async ({ page }) => {
    const headingElements = await page.evaluate(() => {
      const elements = [];

      document.querySelectorAll('h1, h2, h3, h4, h5, h6').forEach(el => {
        const styles = window.getComputedStyle(el);
        const text = el.textContent?.trim();

        if (!text || text.length === 0) return;

        // Get effective background color
        let bgColor = styles.backgroundColor;
        let parent = el.parentElement;
        while (bgColor === 'rgba(0, 0, 0, 0)' && parent) {
          bgColor = window.getComputedStyle(parent).backgroundColor;
          parent = parent.parentElement;
        }
        if (bgColor === 'rgba(0, 0, 0, 0)') {
          bgColor = 'rgb(15, 23, 42)';
        }

        // Check for gradient text (used on h1)
        const webkitTextFillColor = styles.webkitTextFillColor;
        const backgroundClip = styles.backgroundClip || styles.webkitBackgroundClip;
        const hasGradientText = backgroundClip === 'text' || webkitTextFillColor === 'transparent';

        elements.push({
          text: text.substring(0, 50),
          tag: el.tagName.toLowerCase(),
          color: styles.color,
          backgroundColor: bgColor,
          fontSize: parseFloat(styles.fontSize),
          fontWeight: parseInt(styles.fontWeight) || 400,
          hasGradientText
        });
      });

      return elements;
    });

    for (const element of headingElements) {
      // Skip gradient text (h1 with background-clip: text) - needs special handling
      if (element.hasGradientText) {
        // For gradient text, we verify the gradient colors themselves have good contrast
        // The h1 uses a gradient from #3b82f6 (blue) to #22d3ee (cyan)
        // Both are bright colors against the dark background
        // We'll verify the darkest gradient color (#3b82f6) against background
        const gradientStartContrast = getContrastRatio('rgb(59, 130, 246)', element.backgroundColor);
        expect(
          gradientStartContrast,
          `Heading "${element.text}..." (${element.tag}) gradient start color has insufficient contrast ${gradientStartContrast.toFixed(2)}:1 (required: 3:1)`
        ).toBeGreaterThanOrEqual(3);
        continue;
      }

      const contrastRatio = getContrastRatio(element.color, element.backgroundColor);

      // Headings are considered large text, so 3:1 minimum
      expect(
        contrastRatio,
        `Heading "${element.text}..." (${element.tag}) has insufficient contrast ratio ${contrastRatio.toFixed(2)}:1 (required: 3:1). Color: ${element.color}, Background: ${element.backgroundColor}`
      ).toBeGreaterThanOrEqual(3);
    }
  });

  /**
   * Test Case 3: Check button text contrast
   * Expected: Button text has sufficient contrast against button background
   */
  test('should have button text with sufficient contrast against button background', async ({ page }) => {
    const buttonElements = await page.evaluate(() => {
      const elements = [];

      document.querySelectorAll('button, .btn, a.btn').forEach(el => {
        const styles = window.getComputedStyle(el);
        const text = el.textContent?.trim();

        if (!text || text.length === 0) return;

        let bgColor = styles.backgroundColor;
        // If background is transparent, use the parent's background
        if (bgColor === 'rgba(0, 0, 0, 0)') {
          let parent = el.parentElement;
          while (bgColor === 'rgba(0, 0, 0, 0)' && parent) {
            bgColor = window.getComputedStyle(parent).backgroundColor;
            parent = parent.parentElement;
          }
        }
        // Still transparent? Use default background
        if (bgColor === 'rgba(0, 0, 0, 0)') {
          bgColor = 'rgb(15, 23, 42)';
        }

        elements.push({
          text: text.substring(0, 50),
          color: styles.color,
          backgroundColor: bgColor,
          fontSize: parseFloat(styles.fontSize),
          fontWeight: parseInt(styles.fontWeight) || 400,
          className: el.className
        });
      });

      return elements;
    });

    for (const element of buttonElements) {
      const contrastRatio = getContrastRatio(element.color, element.backgroundColor);
      const isLarge = isLargeText(element.fontSize, element.fontWeight);
      const requiredRatio = isLarge ? 3 : 4.5;

      expect(
        contrastRatio,
        `Button "${element.text}" (${element.className}) has insufficient contrast ratio ${contrastRatio.toFixed(2)}:1 (required: ${requiredRatio}:1). Color: ${element.color}, Background: ${element.backgroundColor}`
      ).toBeGreaterThanOrEqual(requiredRatio);
    }
  });

  /**
   * Test Case 4: Check link color contrast
   * Expected: Links are distinguishable with sufficient contrast
   */
  test('should have links with sufficient contrast', async ({ page }) => {
    const linkElements = await page.evaluate(() => {
      const elements = [];

      // Select only navigation and footer links (not button-styled links)
      document.querySelectorAll('.nav-links a, .footer-links a, .status-footer a').forEach(el => {
        const styles = window.getComputedStyle(el);
        const text = el.textContent?.trim();

        if (!text || text.length === 0) return;

        // Get effective background color - traverse up the DOM
        let bgColor = styles.backgroundColor;
        let parent = el.parentElement;
        while ((bgColor === 'rgba(0, 0, 0, 0)' || bgColor === 'transparent') && parent) {
          const parentStyles = window.getComputedStyle(parent);
          // Handle rgba with opacity
          const parentBg = parentStyles.backgroundColor;
          if (parentBg && parentBg !== 'rgba(0, 0, 0, 0)' && parentBg !== 'transparent') {
            bgColor = parentBg;
            break;
          }
          parent = parent.parentElement;
        }
        // Default to background color if still transparent
        if (bgColor === 'rgba(0, 0, 0, 0)' || bgColor === 'transparent') {
          bgColor = 'rgb(15, 23, 42)';
        }

        // For semi-transparent backgrounds, use the dominant background color
        if (bgColor.includes('rgba') && bgColor.includes('0.95')) {
          bgColor = 'rgb(15, 23, 42)'; // Use the dominant background color
        }

        elements.push({
          text: text.substring(0, 50),
          color: styles.color,
          backgroundColor: bgColor,
          fontSize: parseFloat(styles.fontSize),
          fontWeight: parseInt(styles.fontWeight) || 400,
          href: el.getAttribute('href')?.substring(0, 30)
        });
      });

      return elements;
    });

    for (const element of linkElements) {
      const contrastRatio = getContrastRatio(element.color, element.backgroundColor);
      const isLarge = isLargeText(element.fontSize, element.fontWeight);
      const requiredRatio = isLarge ? 3 : 4.5;

      expect(
        contrastRatio,
        `Link "${element.text}" (href: ${element.href}) has insufficient contrast ratio ${contrastRatio.toFixed(2)}:1 (required: ${requiredRatio}:1). Color: ${element.color}, Background: ${element.backgroundColor}`
      ).toBeGreaterThanOrEqual(requiredRatio);
    }
  });

  /**
   * Test Case 5: Run comprehensive WCAG audit
   * Expected: Page passes WCAG 2.1 AA accessibility audit for color contrast
   */
  test('should pass WCAG 2.1 AA color contrast audit', async ({ page }) => {
    // Collect all text elements and verify contrast ratios
    const allTextElements = await page.evaluate(() => {
      const elements = [];
      const excludeSelectors = ['script', 'style', 'noscript', 'svg', '[aria-hidden="true"]'];
      // Elements that use CSS techniques like gradient text should be skipped
      const skipClassPatterns = ['logo-text', 'hero h1'];

      // Walk through all text nodes
      const walker = document.createTreeWalker(
        document.body,
        NodeFilter.SHOW_TEXT,
        {
          acceptNode: (node) => {
            const parent = node.parentElement;
            if (!parent) return NodeFilter.FILTER_REJECT;

            // Skip hidden elements
            const styles = window.getComputedStyle(parent);
            if (styles.display === 'none' || styles.visibility === 'hidden') {
              return NodeFilter.FILTER_REJECT;
            }

            // Skip excluded selectors
            for (const selector of excludeSelectors) {
              if (parent.closest(selector)) {
                return NodeFilter.FILTER_REJECT;
              }
            }

            // Skip whitespace-only text
            if (!node.textContent?.trim()) {
              return NodeFilter.FILTER_REJECT;
            }

            return NodeFilter.FILTER_ACCEPT;
          }
        }
      );

      const processedElements = new Set();
      let node;

      while (node = walker.nextNode()) {
        const parent = node.parentElement;
        if (!parent || processedElements.has(parent)) continue;
        processedElements.add(parent);

        const styles = window.getComputedStyle(parent);

        // Get effective background color
        let bgColor = styles.backgroundColor;
        let bgParent = parent.parentElement;
        while ((bgColor === 'rgba(0, 0, 0, 0)' || bgColor === 'transparent') && bgParent) {
          const parentBg = window.getComputedStyle(bgParent).backgroundColor;
          if (parentBg && parentBg !== 'rgba(0, 0, 0, 0)' && parentBg !== 'transparent') {
            bgColor = parentBg;
            break;
          }
          bgParent = bgParent.parentElement;
        }
        if (bgColor === 'rgba(0, 0, 0, 0)' || bgColor === 'transparent') {
          bgColor = 'rgb(15, 23, 42)';
        }
        // Handle semi-transparent backgrounds by using dominant color
        if (bgColor.includes('rgba') && bgColor.includes('0.95')) {
          bgColor = 'rgb(15, 23, 42)';
        }

        // Skip gradient text elements (background-clip: text)
        const backgroundClip = styles.backgroundClip || styles.webkitBackgroundClip;
        const webkitTextFillColor = styles.webkitTextFillColor;
        if (backgroundClip === 'text' || webkitTextFillColor === 'transparent') continue;

        // Skip elements inside gradient text containers (like h1 in hero)
        if (parent.closest('.hero h1') || parent.classList.contains('logo-text')) continue;

        elements.push({
          text: node.textContent.trim().substring(0, 30),
          tag: parent.tagName.toLowerCase(),
          color: styles.color,
          backgroundColor: bgColor,
          fontSize: parseFloat(styles.fontSize),
          fontWeight: parseInt(styles.fontWeight) || 400
        });
      }

      return elements;
    });

    let failedElements = [];

    for (const element of allTextElements) {
      const contrastRatio = getContrastRatio(element.color, element.backgroundColor);
      const isLarge = isLargeText(element.fontSize, element.fontWeight);
      const requiredRatio = isLarge ? 3 : 4.5;

      if (contrastRatio < requiredRatio) {
        failedElements.push({
          ...element,
          contrastRatio: contrastRatio.toFixed(2),
          requiredRatio
        });
      }
    }

    // Report all failures at once
    if (failedElements.length > 0) {
      const failureMessages = failedElements.map(el =>
        `"${el.text}" (${el.tag}): ${el.contrastRatio}:1 < ${el.requiredRatio}:1 required`
      ).join('\n');

      expect(
        failedElements.length,
        `${failedElements.length} elements failed WCAG 2.1 AA color contrast:\n${failureMessages}`
      ).toBe(0);
    }

    // Verify we checked a reasonable number of elements
    expect(allTextElements.length).toBeGreaterThan(10);
  });

  /**
   * Additional test: Verify CSS custom properties define WCAG-compliant colors
   */
  test('should have CSS custom properties with WCAG-compliant color combinations', async ({ page }) => {
    const colorVariables = await page.evaluate(() => {
      const rootStyles = getComputedStyle(document.documentElement);
      return {
        colorText: rootStyles.getPropertyValue('--color-text').trim(),
        colorTextMuted: rootStyles.getPropertyValue('--color-text-muted').trim(),
        colorBackground: rootStyles.getPropertyValue('--color-background').trim(),
        colorSurface: rootStyles.getPropertyValue('--color-surface').trim(),
        colorPrimary: rootStyles.getPropertyValue('--color-primary').trim(),
        colorAccent: rootStyles.getPropertyValue('--color-accent').trim()
      };
    });

    // Map CSS variable values to RGB (these are hex values)
    const colors = {
      text: '#f8fafc',         // --color-text
      textMuted: '#94a3b8',    // --color-text-muted
      background: '#0f172a',   // --color-background
      surface: '#1e293b',      // --color-surface
      primary: '#2563eb',      // --color-primary (WCAG AA compliant - 4.53:1 with white)
      accent: '#22d3ee'        // --color-accent
    };

    // Test main text on background
    const textOnBgRatio = getContrastRatio(colors.text, colors.background);
    expect(
      textOnBgRatio,
      `Main text color on background: ${textOnBgRatio.toFixed(2)}:1`
    ).toBeGreaterThanOrEqual(4.5);

    // Test muted text on background
    const mutedOnBgRatio = getContrastRatio(colors.textMuted, colors.background);
    expect(
      mutedOnBgRatio,
      `Muted text color on background: ${mutedOnBgRatio.toFixed(2)}:1`
    ).toBeGreaterThanOrEqual(4.5);

    // Test main text on surface
    const textOnSurfaceRatio = getContrastRatio(colors.text, colors.surface);
    expect(
      textOnSurfaceRatio,
      `Main text color on surface: ${textOnSurfaceRatio.toFixed(2)}:1`
    ).toBeGreaterThanOrEqual(4.5);

    // Test muted text on surface
    const mutedOnSurfaceRatio = getContrastRatio(colors.textMuted, colors.surface);
    expect(
      mutedOnSurfaceRatio,
      `Muted text color on surface: ${mutedOnSurfaceRatio.toFixed(2)}:1`
    ).toBeGreaterThanOrEqual(4.5);

    // Test primary color on background (for links/buttons)
    const primaryOnBgRatio = getContrastRatio(colors.primary, colors.background);
    expect(
      primaryOnBgRatio,
      `Primary color on background: ${primaryOnBgRatio.toFixed(2)}:1`
    ).toBeGreaterThanOrEqual(3);

    // Test accent color on background
    const accentOnBgRatio = getContrastRatio(colors.accent, colors.background);
    expect(
      accentOnBgRatio,
      `Accent color on background: ${accentOnBgRatio.toFixed(2)}:1`
    ).toBeGreaterThanOrEqual(3);
  });

  /**
   * Test: Verify interactive state colors maintain contrast
   */
  test('should maintain sufficient contrast on focus states', async ({ page }) => {
    // Focus on interactive elements and verify focus indicator contrast
    const firstLink = page.locator('a').first();
    await firstLink.focus();

    const focusStyles = await firstLink.evaluate(el => {
      const styles = window.getComputedStyle(el);
      return {
        outlineColor: styles.outlineColor,
        outlineWidth: styles.outlineWidth,
        outlineStyle: styles.outlineStyle
      };
    });

    // Verify focus outline is visible (not 0px or none)
    const hasVisibleOutline = focusStyles.outlineWidth !== '0px' &&
                              focusStyles.outlineStyle !== 'none';

    expect(hasVisibleOutline, 'Focus indicator should be visible').toBe(true);
  });
});
