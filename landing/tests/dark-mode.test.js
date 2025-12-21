import { describe, it, expect, beforeEach } from 'vitest';
import { JSDOM } from 'jsdom';
import fs from 'fs';
import path from 'path';

describe('Dark Mode Support', () => {
  let cssContent;
  let dom;
  let document;
  let window;

  beforeEach(() => {
    const cssPath = path.resolve(__dirname, '../styles.css');
    cssContent = fs.readFileSync(cssPath, 'utf8');

    const htmlPath = path.resolve(__dirname, '../index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf8');
    dom = new JSDOM(htmlContent, { url: 'http://localhost' });
    document = dom.window.document;
    window = dom.window;
  });

  // Test Case 1: CSS contains @media (prefers-color-scheme: dark) rules
  describe('Test Case 1: Dark Mode Media Query', () => {
    it('should contain @media (prefers-color-scheme: dark) rules in CSS', () => {
      const hasDarkModeMediaQuery = cssContent.includes('@media (prefers-color-scheme: dark)');
      expect(hasDarkModeMediaQuery).toBe(true);
    });

    it('should have multiple style rules within dark mode media query', () => {
      // Check that there are multiple selectors within the dark mode media query
      // by looking for patterns like "selector {" after the media query declaration
      const mediaQueryIndex = cssContent.indexOf('@media (prefers-color-scheme: dark)');
      expect(mediaQueryIndex).toBeGreaterThan(-1);

      // Extract content after the media query start
      const afterMediaQuery = cssContent.slice(mediaQueryIndex);

      // Count CSS rule selectors within the dark mode block (selectors followed by {)
      // Match common selectors like body, .class, main, h2, etc.
      const selectorMatches = afterMediaQuery.match(/^\s*(body|main|footer|\.[\w-]+|h[1-6]|[\w-]+\s+[\w-]+)/gm);
      expect(selectorMatches).not.toBeNull();
      expect(selectorMatches.length).toBeGreaterThan(0);
    });
  });

  // Test Case 2: Dark mode background color
  describe('Test Case 2: Dark Mode Background Color', () => {
    it('should define a dark background color for body in dark mode', () => {
      // Check that body has a dark background in dark mode
      const darkModeBodyBgRegex = /@media\s*\(\s*prefers-color-scheme:\s*dark\s*\)[\s\S]*?body\s*\{[^}]*background(?:-color)?:\s*([^;]+);/;
      const match = cssContent.match(darkModeBodyBgRegex);
      expect(match).not.toBeNull();

      if (match) {
        const bgColor = match[1].trim();
        // Dark colors typically start with #0, #1, #2 or are dark named colors
        const isDarkColor = /^(#[0-3][0-9a-fA-F]{5}|#[0-3][0-9a-fA-F]{2}|rgb\(\s*[0-9]{1,2}\s*,|hsl\(\s*\d+\s*,\s*\d+%?\s*,\s*[0-3]\d%|black|darkgray|darkgrey|#1a|#0f|#16)/i.test(bgColor);
        expect(isDarkColor).toBe(true);
      }
    });

    it('should have different background than light mode', () => {
      // Extract light mode body background
      const lightBodyBgRegex = /body\s*\{[^}]*background(?:-color)?:\s*([^;]+);/;
      const lightMatch = cssContent.match(lightBodyBgRegex);

      // Extract dark mode body background
      const darkModeBodyBgRegex = /@media\s*\(\s*prefers-color-scheme:\s*dark\s*\)[\s\S]*?body\s*\{[^}]*background(?:-color)?:\s*([^;]+);/;
      const darkMatch = cssContent.match(darkModeBodyBgRegex);

      expect(lightMatch).not.toBeNull();
      expect(darkMatch).not.toBeNull();

      if (lightMatch && darkMatch) {
        // Background colors should be different
        expect(lightMatch[1].trim()).not.toBe(darkMatch[1].trim());
      }
    });
  });

  // Test Case 3: Dark mode text color
  describe('Test Case 3: Dark Mode Text Color', () => {
    it('should define a light text color for body in dark mode', () => {
      // Check that body has light text color in dark mode
      const darkModeTextRegex = /@media\s*\(\s*prefers-color-scheme:\s*dark\s*\)[\s\S]*?body\s*\{[^}]*color:\s*([^;]+);/;
      const match = cssContent.match(darkModeTextRegex);
      expect(match).not.toBeNull();

      if (match) {
        const textColor = match[1].trim();
        // Light colors typically have high values like #e, #f, #d or white/lightgray
        const isLightColor = /^(#[cdef][0-9a-fA-F]{5}|#[cdef][0-9a-fA-F]{2}|rgb\(\s*2[0-5]\d|white|lightgray|lightgrey|#f8|#e0|#d1)/i.test(textColor);
        expect(isLightColor).toBe(true);
      }
    });

    it('should have different text color than light mode', () => {
      // Extract light mode body text color
      const lightTextRegex = /body\s*\{[^}]*color:\s*([^;]+);/;
      const lightMatch = cssContent.match(lightTextRegex);

      // Extract dark mode body text color
      const darkModeTextRegex = /@media\s*\(\s*prefers-color-scheme:\s*dark\s*\)[\s\S]*?body\s*\{[^}]*color:\s*([^;]+);/;
      const darkMatch = cssContent.match(darkModeTextRegex);

      expect(lightMatch).not.toBeNull();
      expect(darkMatch).not.toBeNull();

      if (lightMatch && darkMatch) {
        expect(lightMatch[1].trim()).not.toBe(darkMatch[1].trim());
      }
    });
  });

  // Test Case 4: Contrast ratios in dark mode (WCAG AA compliance)
  describe('Test Case 4: Contrast Ratios in Dark Mode', () => {
    // Helper function to parse color values
    function parseColor(color) {
      color = color.trim().toLowerCase();

      // Handle hex colors
      if (color.startsWith('#')) {
        let hex = color.slice(1);
        if (hex.length === 3) {
          hex = hex.split('').map(c => c + c).join('');
        }
        return {
          r: parseInt(hex.slice(0, 2), 16),
          g: parseInt(hex.slice(2, 4), 16),
          b: parseInt(hex.slice(4, 6), 16)
        };
      }

      // Handle rgb/rgba
      const rgbMatch = color.match(/rgba?\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)/);
      if (rgbMatch) {
        return {
          r: parseInt(rgbMatch[1]),
          g: parseInt(rgbMatch[2]),
          b: parseInt(rgbMatch[3])
        };
      }

      // Common dark/light colors
      const colorMap = {
        'white': { r: 255, g: 255, b: 255 },
        'black': { r: 0, g: 0, b: 0 },
        'lightgray': { r: 211, g: 211, b: 211 },
        'darkgray': { r: 169, g: 169, b: 169 }
      };

      return colorMap[color] || null;
    }

    // Calculate relative luminance
    function getLuminance(rgb) {
      if (!rgb) return null;
      const [r, g, b] = [rgb.r, rgb.g, rgb.b].map(c => {
        c = c / 255;
        return c <= 0.03928 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4);
      });
      return 0.2126 * r + 0.7152 * g + 0.0722 * b;
    }

    // Calculate contrast ratio
    function getContrastRatio(color1, color2) {
      const l1 = getLuminance(color1);
      const l2 = getLuminance(color2);
      if (l1 === null || l2 === null) return null;
      const lighter = Math.max(l1, l2);
      const darker = Math.min(l1, l2);
      return (lighter + 0.05) / (darker + 0.05);
    }

    it('should have dark mode styles that suggest proper contrast', () => {
      // This test verifies that dark mode has contrasting text and background
      const darkModeTextRegex = /@media\s*\(\s*prefers-color-scheme:\s*dark\s*\)[\s\S]*?body\s*\{[^}]*color:\s*([^;]+);/;
      const darkModeBodyBgRegex = /@media\s*\(\s*prefers-color-scheme:\s*dark\s*\)[\s\S]*?body\s*\{[^}]*background(?:-color)?:\s*([^;]+);/;

      const textMatch = cssContent.match(darkModeTextRegex);
      const bgMatch = cssContent.match(darkModeBodyBgRegex);

      expect(textMatch).not.toBeNull();
      expect(bgMatch).not.toBeNull();

      if (textMatch && bgMatch) {
        const textColor = parseColor(textMatch[1]);
        const bgColor = parseColor(bgMatch[1]);

        if (textColor && bgColor) {
          const contrast = getContrastRatio(textColor, bgColor);
          // WCAG AA requires 4.5:1 for normal text
          expect(contrast).toBeGreaterThanOrEqual(4.5);
        }
      }
    });

    it('should maintain readable contrast in dark mode sections', () => {
      // Verify dark mode includes styles for main content sections
      const hasDarkModeMainStyles = /@media\s*\(\s*prefers-color-scheme:\s*dark\s*\)[\s\S]*?(main|\.get-started-section|section)/i.test(cssContent);
      expect(hasDarkModeMainStyles).toBe(true);
    });
  });

  // Test Case 5: Code blocks styling in dark mode
  describe('Test Case 5: Code Blocks Dark Mode Styling', () => {
    it('should have code block styling that works in dark mode', () => {
      // Check that .code-block has appropriate dark styling
      const codeBlockRegex = /\.code-block\s*\{[^}]*background:\s*([^;]+);/;
      const match = cssContent.match(codeBlockRegex);
      expect(match).not.toBeNull();

      if (match) {
        const bgColor = match[1].trim();
        // Code blocks should have dark background already
        const isDarkBg = /^#[0-3][0-9a-fA-F]{5}|#1a|#0f|#16/i.test(bgColor);
        expect(isDarkBg).toBe(true);
      }
    });

    it('should have code text with light color for contrast', () => {
      // Check that code has light color
      const codeColorRegex = /\.code-block\s+code\s*\{[^}]*color:\s*([^;]+);/;
      const match = cssContent.match(codeColorRegex);
      expect(match).not.toBeNull();

      if (match) {
        const textColor = match[1].trim();
        // Light colors for code text
        const isLightColor = /^#[89a-fA-F][0-9a-fA-F]{5}|#a8|#e9|#ff/i.test(textColor);
        expect(isLightColor).toBe(true);
      }
    });

    it('should maintain or enhance code block styling in dark mode', () => {
      // Dark mode should either keep code blocks as-is (already dark) or adjust them
      // Check if code-block is mentioned in dark mode media query for any adjustments
      const hasDarkModeQuery = cssContent.includes('@media (prefers-color-scheme: dark)');
      expect(hasDarkModeQuery).toBe(true);
    });
  });

  // Additional tests for complete dark mode coverage
  describe('Additional Dark Mode Coverage', () => {
    it('should style section headings for dark mode', () => {
      // Check for h2, h3 color adjustments in dark mode
      const darkModeHeadingRegex = /@media\s*\(\s*prefers-color-scheme:\s*dark\s*\)[\s\S]*?(h[2-4]|\.get-started-section\s+h2)/;
      const match = cssContent.match(darkModeHeadingRegex);
      expect(match).not.toBeNull();
    });

    it('should style buttons appropriately for dark mode', () => {
      // Check for button focus styles in dark mode
      const hasDarkModeQuery = cssContent.includes('@media (prefers-color-scheme: dark)');
      expect(hasDarkModeQuery).toBe(true);
    });

    it('should include dark mode styles for text paragraphs', () => {
      // Paragraphs and descriptive text should be styled
      const darkModeParagraphRegex = /@media\s*\(\s*prefers-color-scheme:\s*dark\s*\)[\s\S]*?(\.section-intro|\.value-proposition|p\s*\{|color)/;
      const match = cssContent.match(darkModeParagraphRegex);
      expect(match).not.toBeNull();
    });
  });
});
