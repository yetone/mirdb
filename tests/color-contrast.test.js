import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { JSDOM } from 'jsdom';
import { readFileSync } from 'fs';
import { resolve } from 'path';

/**
 * Calculate relative luminance according to WCAG 2.1
 * https://www.w3.org/WAI/GL/wiki/Relative_luminance
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
 * https://www.w3.org/WAI/GL/wiki/Contrast_ratio
 */
function getContrastRatio(rgb1, rgb2) {
  const l1 = getRelativeLuminance(rgb1.r, rgb1.g, rgb1.b);
  const l2 = getRelativeLuminance(rgb2.r, rgb2.g, rgb2.b);
  const lighter = Math.max(l1, l2);
  const darker = Math.min(l1, l2);
  return (lighter + 0.05) / (darker + 0.05);
}

/**
 * Parse a CSS color value to RGB object
 */
function parseColor(color) {
  if (!color || color === 'transparent' || color === 'inherit' || color === 'initial') {
    return null;
  }

  // Handle rgb/rgba format
  const rgbMatch = color.match(/rgba?\s*\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*(?:,\s*[\d.]+)?\s*\)/);
  if (rgbMatch) {
    return {
      r: parseInt(rgbMatch[1], 10),
      g: parseInt(rgbMatch[2], 10),
      b: parseInt(rgbMatch[3], 10)
    };
  }

  // Handle hex format
  const hexMatch = color.match(/^#([0-9a-fA-F]{6})$|^#([0-9a-fA-F]{3})$/);
  if (hexMatch) {
    const hex = hexMatch[1] || hexMatch[2];
    if (hex.length === 3) {
      return {
        r: parseInt(hex[0] + hex[0], 16),
        g: parseInt(hex[1] + hex[1], 16),
        b: parseInt(hex[2] + hex[2], 16)
      };
    }
    return {
      r: parseInt(hex.substring(0, 2), 16),
      g: parseInt(hex.substring(2, 4), 16),
      b: parseInt(hex.substring(4, 6), 16)
    };
  }

  return null;
}

/**
 * Get CSS variable values from the stylesheet
 */
function getCSSVariables(document) {
  const variables = {};
  const stylesheets = document.styleSheets;

  try {
    for (const stylesheet of stylesheets) {
      try {
        const rules = stylesheet.cssRules || stylesheet.rules;
        if (!rules) continue;

        for (const rule of rules) {
          if (rule.selectorText === ':root') {
            const style = rule.style;
            for (let i = 0; i < style.length; i++) {
              const prop = style[i];
              if (prop.startsWith('--')) {
                variables[prop] = style.getPropertyValue(prop).trim();
              }
            }
          }
        }
      } catch (e) {
        // Cross-origin stylesheets may throw
        continue;
      }
    }
  } catch (e) {
    // Fall back to reading CSS file directly
  }

  return variables;
}

/**
 * Parse CSS file to extract variable values
 */
function parseCSSVariables(cssContent) {
  const variables = {};
  const rootMatch = cssContent.match(/:root\s*\{([^}]+)\}/);
  if (rootMatch) {
    const propsContent = rootMatch[1];
    const propMatches = propsContent.matchAll(/\s*--([\w-]+)\s*:\s*([^;]+);/g);
    for (const match of propMatches) {
      variables[`--${match[1]}`] = match[2].trim();
    }
  }
  return variables;
}

/**
 * Resolve CSS variable to actual color value
 */
function resolveVariable(value, variables) {
  if (!value) return value;
  const varMatch = value.match(/var\((--[\w-]+)(?:\s*,\s*([^)]+))?\)/);
  if (varMatch) {
    const varName = varMatch[1];
    const fallback = varMatch[2];
    return variables[varName] || fallback || value;
  }
  return value;
}

describe('Accessibility - Color Contrast', () => {
  let dom;
  let document;
  let cssVariables;
  let cssContent;

  beforeEach(() => {
    const htmlPath = resolve(__dirname, '../index.html');
    const cssPath = resolve(__dirname, '../styles.css');
    const html = readFileSync(htmlPath, 'utf-8');
    cssContent = readFileSync(cssPath, 'utf-8');
    dom = new JSDOM(html, {
      runScripts: 'dangerously',
      resources: 'usable',
      url: 'http://localhost'
    });
    document = dom.window.document;
    cssVariables = parseCSSVariables(cssContent);
  });

  afterEach(() => {
    if (dom) {
      dom.window.close();
    }
  });

  // Test Case 1: Body text contrast (4.5:1 ratio for normal text - WCAG AA)
  describe('Test Case 1: Body text color contrast', () => {
    it('should have body text color defined in CSS variables', () => {
      const textPrimary = cssVariables['--text-primary'];
      const textSecondary = cssVariables['--text-secondary'];
      expect(textPrimary || textSecondary).toBeTruthy();
    });

    it('should have background color defined in CSS variables', () => {
      const bgColor = cssVariables['--background-color'];
      const surfaceColor = cssVariables['--surface-color'];
      expect(bgColor || surfaceColor).toBeTruthy();
    });

    it('should have primary text color with at least 4.5:1 contrast ratio against background', () => {
      const textColor = parseColor(cssVariables['--text-primary']); // #f8fafc
      const bgColor = parseColor(cssVariables['--background-color']); // #0f172a

      expect(textColor).not.toBeNull();
      expect(bgColor).not.toBeNull();

      const ratio = getContrastRatio(textColor, bgColor);
      expect(ratio).toBeGreaterThanOrEqual(4.5);
    });

    it('should have secondary text color with at least 4.5:1 contrast ratio against background', () => {
      const textColor = parseColor(cssVariables['--text-secondary']); // #94a3b8
      const bgColor = parseColor(cssVariables['--background-color']); // #0f172a

      expect(textColor).not.toBeNull();
      expect(bgColor).not.toBeNull();

      const ratio = getContrastRatio(textColor, bgColor);
      expect(ratio).toBeGreaterThanOrEqual(4.5);
    });

    it('should have primary text color with at least 4.5:1 contrast ratio against surface color', () => {
      const textColor = parseColor(cssVariables['--text-primary']); // #f8fafc
      const surfaceColor = parseColor(cssVariables['--surface-color']); // #1e293b

      expect(textColor).not.toBeNull();
      expect(surfaceColor).not.toBeNull();

      const ratio = getContrastRatio(textColor, surfaceColor);
      expect(ratio).toBeGreaterThanOrEqual(4.5);
    });

    it('should have secondary text color with at least 4.5:1 contrast ratio against surface color', () => {
      const textColor = parseColor(cssVariables['--text-secondary']); // #94a3b8
      const surfaceColor = parseColor(cssVariables['--surface-color']); // #1e293b

      expect(textColor).not.toBeNull();
      expect(surfaceColor).not.toBeNull();

      const ratio = getContrastRatio(textColor, surfaceColor);
      expect(ratio).toBeGreaterThanOrEqual(4.5);
    });
  });

  // Test Case 2: Heading contrast (3:1 ratio for large text - WCAG AA)
  describe('Test Case 2: Heading color contrast', () => {
    it('should have h1 heading visible in the page', () => {
      const h1 = document.querySelector('h1');
      expect(h1).not.toBeNull();
    });

    it('should have h2 headings visible in the page', () => {
      const h2Elements = document.querySelectorAll('h2');
      expect(h2Elements.length).toBeGreaterThan(0);
    });

    it('should have heading colors with at least 3:1 contrast ratio (large text threshold)', () => {
      // For large text (18pt/24px or 14pt/18.67px bold), WCAG AA requires 3:1
      const textPrimary = parseColor(cssVariables['--text-primary']); // #f8fafc
      const bgColor = parseColor(cssVariables['--background-color']); // #0f172a

      expect(textPrimary).not.toBeNull();
      expect(bgColor).not.toBeNull();

      const ratio = getContrastRatio(textPrimary, bgColor);
      expect(ratio).toBeGreaterThanOrEqual(3);
    });

    it('should have section headings with sufficient contrast against surface backgrounds', () => {
      const textPrimary = parseColor(cssVariables['--text-primary']); // #f8fafc
      const surfaceColor = parseColor(cssVariables['--surface-color']); // #1e293b

      expect(textPrimary).not.toBeNull();
      expect(surfaceColor).not.toBeNull();

      const ratio = getContrastRatio(textPrimary, surfaceColor);
      expect(ratio).toBeGreaterThanOrEqual(3);
    });

    it('should have feature card headings with at least 3:1 contrast', () => {
      // Feature cards use surface-color background with text-primary text
      const textPrimary = parseColor(cssVariables['--text-primary']); // #f8fafc
      const surfaceColor = parseColor(cssVariables['--surface-color']); // #1e293b

      expect(textPrimary).not.toBeNull();
      expect(surfaceColor).not.toBeNull();

      const ratio = getContrastRatio(textPrimary, surfaceColor);
      expect(ratio).toBeGreaterThanOrEqual(3);
    });
  });

  // Test Case 3: Link visibility (links must be distinguishable)
  describe('Test Case 3: Link visibility', () => {
    it('should have link color defined', () => {
      const linkColor = cssVariables['--primary-color'];
      expect(linkColor).toBeTruthy();
    });

    it('should have links in the page', () => {
      const links = document.querySelectorAll('a');
      expect(links.length).toBeGreaterThan(0);
    });

    it('should have link color with at least 4.5:1 contrast against background', () => {
      const linkColor = parseColor(cssVariables['--primary-color']); // #2563eb
      const bgColor = parseColor(cssVariables['--background-color']); // #0f172a

      expect(linkColor).not.toBeNull();
      expect(bgColor).not.toBeNull();

      const ratio = getContrastRatio(linkColor, bgColor);
      expect(ratio).toBeGreaterThanOrEqual(4.5);
    });

    it('should have link color with at least 4.5:1 contrast against surface color', () => {
      const linkColor = parseColor(cssVariables['--primary-color']); // #2563eb
      const surfaceColor = parseColor(cssVariables['--surface-color']); // #1e293b

      expect(linkColor).not.toBeNull();
      expect(surfaceColor).not.toBeNull();

      const ratio = getContrastRatio(linkColor, surfaceColor);
      expect(ratio).toBeGreaterThanOrEqual(4.5);
    });

    it('should have links distinguishable by means other than color alone', () => {
      // Check that links have underline, hover state, or other visual distinction
      // By default, links have underline or are styled with distinct color AND other cues
      const hasUnderlineRule = cssContent.includes('text-decoration') ||
                              cssContent.includes('underline');
      const hasHoverState = cssContent.includes('a:hover') ||
                           cssContent.includes(':hover');
      const hasFocusVisible = cssContent.includes(':focus-visible') ||
                              cssContent.includes(':focus');

      // Links should have some visual distinction mechanism
      expect(hasUnderlineRule || hasHoverState || hasFocusVisible).toBe(true);
    });

    it('should have hover state defined for links', () => {
      const hasHoverState = cssContent.includes('a:hover');
      expect(hasHoverState).toBe(true);
    });

    it('should have focus-visible styles for keyboard navigation', () => {
      const hasFocusVisible = cssContent.includes(':focus-visible');
      expect(hasFocusVisible).toBe(true);
    });

    it('should have navigation links with clear visual styling', () => {
      const navLinks = document.querySelectorAll('nav a, .nav-links a');
      expect(navLinks.length).toBeGreaterThan(0);

      // Navigation links should exist and be stylable
      navLinks.forEach(link => {
        expect(link.tagName.toLowerCase()).toBe('a');
        expect(link.getAttribute('href')).toBeTruthy();
      });
    });
  });

  // Test Case 4: Button text contrast
  describe('Test Case 4: Button text contrast', () => {
    it('should have buttons in the page', () => {
      const buttons = document.querySelectorAll('button, .btn, [class*="btn"]');
      expect(buttons.length).toBeGreaterThan(0);
    });

    it('should have primary button with sufficient contrast (white text on primary button background)', () => {
      // Primary buttons use --primary-button-bg for accessible contrast
      const buttonBg = parseColor(cssVariables['--primary-button-bg']) || parseColor(cssVariables['--primary-color']); // #2563eb
      const whiteText = { r: 255, g: 255, b: 255 };

      expect(buttonBg).not.toBeNull();

      const ratio = getContrastRatio(whiteText, buttonBg);
      expect(ratio).toBeGreaterThanOrEqual(4.5);
    });

    it('should have secondary button with sufficient contrast', () => {
      const textPrimary = parseColor(cssVariables['--text-primary']); // #f8fafc
      const borderColor = parseColor(cssVariables['--border-color']); // #334155

      // Secondary button has transparent bg with border, text should contrast with background
      const bgColor = parseColor(cssVariables['--background-color']); // #0f172a

      expect(textPrimary).not.toBeNull();
      expect(bgColor).not.toBeNull();

      const ratio = getContrastRatio(textPrimary, bgColor);
      expect(ratio).toBeGreaterThanOrEqual(4.5);
    });

    it('should have copy button with sufficient contrast', () => {
      const copyButtons = document.querySelectorAll('.copy-btn');
      expect(copyButtons.length).toBeGreaterThan(0);

      // Copy button uses text-secondary on surface-color background
      const textSecondary = parseColor(cssVariables['--text-secondary']); // #94a3b8
      const surfaceColor = parseColor(cssVariables['--surface-color']); // #1e293b

      expect(textSecondary).not.toBeNull();
      expect(surfaceColor).not.toBeNull();

      const ratio = getContrastRatio(textSecondary, surfaceColor);
      expect(ratio).toBeGreaterThanOrEqual(4.5);
    });

    it('should have button hover states defined', () => {
      const hasPrimaryHover = cssContent.includes('.btn-primary:hover');
      const hasSecondaryHover = cssContent.includes('.btn-secondary:hover');
      const hasCopyBtnHover = cssContent.includes('.copy-btn:hover');

      expect(hasPrimaryHover).toBe(true);
      expect(hasSecondaryHover).toBe(true);
      expect(hasCopyBtnHover).toBe(true);
    });

    it('should have mobile menu toggle button with aria-label', () => {
      const mobileToggle = document.querySelector('.mobile-menu-toggle');
      expect(mobileToggle).not.toBeNull();
      expect(mobileToggle.getAttribute('aria-label')).toBeTruthy();
    });
  });

  // Additional color contrast tests for specific components
  describe('Additional component contrast tests', () => {
    it('should have code block text with sufficient contrast', () => {
      const textPrimary = parseColor(cssVariables['--text-primary']); // #f8fafc
      const codeBg = parseColor(cssVariables['--code-bg']); // #0d1117

      expect(textPrimary).not.toBeNull();
      expect(codeBg).not.toBeNull();

      const ratio = getContrastRatio(textPrimary, codeBg);
      expect(ratio).toBeGreaterThanOrEqual(4.5);
    });

    it('should have table text with sufficient contrast', () => {
      const tables = document.querySelectorAll('table');
      expect(tables.length).toBeGreaterThan(0);

      // Table headers and cells use text-primary on surface-color
      const textPrimary = parseColor(cssVariables['--text-primary']); // #f8fafc
      const surfaceColor = parseColor(cssVariables['--surface-color']); // #1e293b

      expect(textPrimary).not.toBeNull();
      expect(surfaceColor).not.toBeNull();

      const ratio = getContrastRatio(textPrimary, surfaceColor);
      expect(ratio).toBeGreaterThanOrEqual(4.5);
    });

    it('should have footer text with sufficient contrast', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();

      // Footer uses text-secondary on surface-color
      const textSecondary = parseColor(cssVariables['--text-secondary']); // #94a3b8
      const surfaceColor = parseColor(cssVariables['--surface-color']); // #1e293b

      expect(textSecondary).not.toBeNull();
      expect(surfaceColor).not.toBeNull();

      const ratio = getContrastRatio(textSecondary, surfaceColor);
      expect(ratio).toBeGreaterThanOrEqual(4.5);
    });

    it('should have success color with sufficient contrast for copied state', () => {
      const successColor = parseColor(cssVariables['--success-color']); // #22c55e
      const whiteText = { r: 255, g: 255, b: 255 };

      expect(successColor).not.toBeNull();

      const ratio = getContrastRatio(whiteText, successColor);
      // Success button should have readable text
      expect(ratio).toBeGreaterThanOrEqual(3);
    });
  });

  // Test WCAG 2.1 specific requirements
  describe('WCAG 2.1 AA specific requirements', () => {
    it('should maintain contrast ratios with CSS variables approach', () => {
      // Verify that CSS variables approach maintains accessibility
      const criticalPairs = [
        { text: '--text-primary', bg: '--background-color', minRatio: 4.5 },
        { text: '--text-secondary', bg: '--background-color', minRatio: 4.5 },
        { text: '--text-primary', bg: '--surface-color', minRatio: 4.5 },
        { text: '--text-secondary', bg: '--surface-color', minRatio: 4.5 },
        { text: '--primary-color', bg: '--background-color', minRatio: 4.5 },
      ];

      criticalPairs.forEach(pair => {
        const textColor = parseColor(cssVariables[pair.text]);
        const bgColor = parseColor(cssVariables[pair.bg]);

        if (textColor && bgColor) {
          const ratio = getContrastRatio(textColor, bgColor);
          expect(ratio).toBeGreaterThanOrEqual(pair.minRatio);
        }
      });
    });

    it('should have proper contrast for interactive elements in all states', () => {
      // Check that button hover colors maintain contrast
      // Primary buttons use --primary-button-hover for accessible contrast
      const buttonHover = cssVariables['--primary-button-hover'] || cssVariables['--primary-hover']; // #1d4ed8
      if (buttonHover) {
        const hoverColor = parseColor(buttonHover);
        const whiteText = { r: 255, g: 255, b: 255 };

        if (hoverColor) {
          const ratio = getContrastRatio(whiteText, hoverColor);
          expect(ratio).toBeGreaterThanOrEqual(4.5);
        }
      }
    });
  });
});
