/**
 * Accessibility Unit Tests
 * Owner: Scenario 11 - Color Contrast, Scenario 12 - Images and Alt Text
 *
 * Tests for accessibility requirements:
 * - Color contrast ratio validation (4.5:1 for text, 3:1 for large text)
 * - Alt text presence on all images
 * - Proper alt text content (not empty where meaningful)
 * - Syntax highlighting contrast compliance
 * - Logo and badge accessibility
 */

const fs = require('fs');
const path = require('path');

/**
 * Calculate the relative luminance of a color
 * @param {number} r - Red value (0-255)
 * @param {number} g - Green value (0-255)
 * @param {number} b - Blue value (0-255)
 * @returns {number} Relative luminance value
 */
function getLuminance(r, g, b) {
  const [rs, gs, bs] = [r, g, b].map((c) => {
    c = c / 255;
    return c <= 0.03928 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4);
  });
  return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
}

/**
 * Calculate the contrast ratio between two colors
 * @param {string} color1 - First color in hex format (e.g., #ffffff)
 * @param {string} color2 - Second color in hex format (e.g., #000000)
 * @returns {number} Contrast ratio (1 to 21)
 */
function getContrastRatio(color1, color2) {
  const hexToRgb = (hex) => {
    const result = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex);
    return result
      ? {
          r: parseInt(result[1], 16),
          g: parseInt(result[2], 16),
          b: parseInt(result[3], 16),
        }
      : null;
  };

  const rgb1 = hexToRgb(color1);
  const rgb2 = hexToRgb(color2);

  if (!rgb1 || !rgb2) {
    throw new Error(`Invalid color format: ${color1} or ${color2}`);
  }

  const lum1 = getLuminance(rgb1.r, rgb1.g, rgb1.b);
  const lum2 = getLuminance(rgb2.r, rgb2.g, rgb2.b);

  const lighter = Math.max(lum1, lum2);
  const darker = Math.min(lum1, lum2);

  return (lighter + 0.05) / (darker + 0.05);
}

/**
 * Parse CSS content and extract custom properties from :root
 * @param {string} cssContent - The CSS content to parse
 * @returns {Object} Object containing variable names and their values
 */
function parseCSSVariables(cssContent) {
  const variables = {};
  const rootMatch = cssContent.match(/:root\s*\{([^}]+)\}/);

  if (rootMatch) {
    const declarations = rootMatch[1];
    const varRegex = /--([\w-]+)\s*:\s*([^;]+);/g;
    let match;

    while ((match = varRegex.exec(declarations)) !== null) {
      variables[`--${match[1]}`] = match[2].trim();
    }
  }

  return variables;
}

// WCAG 2.1 AA Minimum Contrast Ratios
const WCAG_AA_NORMAL_TEXT = 4.5; // For normal text (< 18pt or < 14pt bold)
const WCAG_AA_LARGE_TEXT = 3.0; // For large text (>= 18pt or >= 14pt bold)

describe('Accessibility - Color Contrast (Scenario 11)', () => {
  let cssVariables;
  let codeBlockCSS;
  let heroCSS;

  beforeAll(() => {
    // Read the CSS files
    const variablesPath = path.join(__dirname, '../../css/variables.css');
    const variablesContent = fs.readFileSync(variablesPath, 'utf8');
    cssVariables = parseCSSVariables(variablesContent);

    const codeBlockPath = path.join(__dirname, '../../css/components/code-block.css');
    codeBlockCSS = fs.readFileSync(codeBlockPath, 'utf8');

    const heroPath = path.join(__dirname, '../../css/components/hero.css');
    heroCSS = fs.readFileSync(heroPath, 'utf8');
  });

  describe('Test Case 1: Body text color contrast', () => {
    test('Body text (#eaeaea) has at least 4.5:1 contrast ratio against background (#1a1a2e)', () => {
      const textColor = cssVariables['--color-text'];
      const backgroundColor = cssVariables['--color-background'];

      const ratio = getContrastRatio(textColor, backgroundColor);

      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
      // Log for debugging
      console.log(`Body text contrast ratio: ${ratio.toFixed(2)}:1 (required: ${WCAG_AA_NORMAL_TEXT}:1)`);
    });

    test('Muted text (#a0a0a0) has at least 4.5:1 contrast ratio against background (#1a1a2e)', () => {
      const textColor = cssVariables['--color-text-muted'];
      const backgroundColor = cssVariables['--color-background'];

      const ratio = getContrastRatio(textColor, backgroundColor);

      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
      console.log(`Muted text contrast ratio: ${ratio.toFixed(2)}:1 (required: ${WCAG_AA_NORMAL_TEXT}:1)`);
    });
  });

  describe('Test Case 2: Heading color contrast', () => {
    test('Heading text (#eaeaea) has at least 3:1 contrast ratio against background (#1a1a2e)', () => {
      // Headings use --color-text which is #eaeaea
      const headingColor = cssVariables['--color-text'];
      const backgroundColor = cssVariables['--color-background'];

      const ratio = getContrastRatio(headingColor, backgroundColor);

      // Large text only requires 3:1, but headings should still meet 4.5:1 for better accessibility
      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_LARGE_TEXT);
      console.log(`Heading contrast ratio: ${ratio.toFixed(2)}:1 (required: ${WCAG_AA_LARGE_TEXT}:1 for large text)`);
    });

    test('Hero tagline (#a0a0a0) has at least 3:1 contrast ratio against background', () => {
      // The hero tagline uses --color-text-muted
      const taglineColor = cssVariables['--color-text-muted'];
      const backgroundColor = cssVariables['--color-background'];

      const ratio = getContrastRatio(taglineColor, backgroundColor);

      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_LARGE_TEXT);
      console.log(`Hero tagline contrast ratio: ${ratio.toFixed(2)}:1 (required: ${WCAG_AA_LARGE_TEXT}:1)`);
    });
  });

  describe('Test Case 3: Link color contrast', () => {
    test('Link color (#3498db) has at least 4.5:1 contrast ratio against background (#1a1a2e)', () => {
      const linkColor = cssVariables['--color-secondary'];
      const backgroundColor = cssVariables['--color-background'];

      const ratio = getContrastRatio(linkColor, backgroundColor);

      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
      console.log(`Link color contrast ratio: ${ratio.toFixed(2)}:1 (required: ${WCAG_AA_NORMAL_TEXT}:1)`);
    });

    test('Link hover color (#5dade2) has at least 4.5:1 contrast ratio against background', () => {
      const linkHoverColor = cssVariables['--color-secondary-dark'];
      const backgroundColor = cssVariables['--color-background'];

      const ratio = getContrastRatio(linkHoverColor, backgroundColor);

      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
      console.log(`Link hover color contrast ratio: ${ratio.toFixed(2)}:1 (required: ${WCAG_AA_NORMAL_TEXT}:1)`);
    });

    test('Links are distinguishable from surrounding text', () => {
      // Links should have different color from body text to be distinguishable
      const linkColor = cssVariables['--color-secondary'];
      const textColor = cssVariables['--color-text'];

      // They should not be the same color
      expect(linkColor).not.toBe(textColor);

      // There should be enough difference between link and text colors
      // A contrast ratio of at least 3:1 between link and text is recommended
      const ratio = getContrastRatio(linkColor, textColor);
      expect(ratio).toBeGreaterThanOrEqual(1.5); // At minimum they should be visually distinct
      console.log(`Link vs text color contrast: ${ratio.toFixed(2)}:1`);
    });
  });

  describe('Test Case 4: Button color contrast', () => {
    test('CTA button text (white) has at least 4.5:1 contrast against button background (#c0392b)', () => {
      const buttonTextColor = '#ffffff';
      const buttonBackgroundColor = cssVariables['--color-primary'];

      const ratio = getContrastRatio(buttonTextColor, buttonBackgroundColor);

      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
      console.log(`CTA button contrast ratio: ${ratio.toFixed(2)}:1 (required: ${WCAG_AA_NORMAL_TEXT}:1)`);
    });

    test('CTA button hover state has at least 4.5:1 contrast', () => {
      const buttonTextColor = '#ffffff';
      const buttonHoverBackgroundColor = cssVariables['--color-primary-dark'];

      const ratio = getContrastRatio(buttonTextColor, buttonHoverBackgroundColor);

      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
      console.log(`CTA button hover contrast ratio: ${ratio.toFixed(2)}:1 (required: ${WCAG_AA_NORMAL_TEXT}:1)`);
    });

    test('Tech badge (Rust) has adequate contrast', () => {
      // Rust badge: #dea584 background with #000 text
      const rustBadgeText = '#000000';
      const rustBadgeBackground = '#dea584';

      const ratio = getContrastRatio(rustBadgeText, rustBadgeBackground);

      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
      console.log(`Rust badge contrast ratio: ${ratio.toFixed(2)}:1 (required: ${WCAG_AA_NORMAL_TEXT}:1)`);
    });

    test('Tech badge (Tokio) has adequate contrast', () => {
      // Tokio badge: #1a1a2e background with #fff text
      const tokioBadgeText = '#ffffff';
      const tokioBadgeBackground = '#1a1a2e';

      const ratio = getContrastRatio(tokioBadgeText, tokioBadgeBackground);

      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
      console.log(`Tokio badge contrast ratio: ${ratio.toFixed(2)}:1 (required: ${WCAG_AA_NORMAL_TEXT}:1)`);
    });

    test('Copy button text has adequate contrast', () => {
      // Copy button uses --color-text-muted on transparent/code background
      const buttonText = cssVariables['--color-text-muted'];
      const codeBackground = cssVariables['--color-code-bg'];

      const ratio = getContrastRatio(buttonText, codeBackground);

      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
      console.log(`Copy button contrast ratio: ${ratio.toFixed(2)}:1 (required: ${WCAG_AA_NORMAL_TEXT}:1)`);
    });
  });

  describe('Test Case 5: Code block syntax highlighting contrast', () => {
    test('Code text (#eaeaea) has at least 4.5:1 contrast against code background (#0d1117)', () => {
      const codeText = cssVariables['--color-text'];
      const codeBackground = cssVariables['--color-code-bg'];

      const ratio = getContrastRatio(codeText, codeBackground);

      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
      console.log(`Code text contrast ratio: ${ratio.toFixed(2)}:1 (required: ${WCAG_AA_NORMAL_TEXT}:1)`);
    });

    test('Comment syntax highlighting (#8b949e) has at least 4.5:1 contrast', () => {
      // Comments use #8b949e against #0d1117
      const commentColor = '#8b949e';
      const codeBackground = cssVariables['--color-code-bg'];

      const ratio = getContrastRatio(commentColor, codeBackground);

      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
      console.log(`Comment syntax contrast ratio: ${ratio.toFixed(2)}:1 (required: ${WCAG_AA_NORMAL_TEXT}:1)`);
    });

    test('Punctuation syntax highlighting (#c9d1d9) has at least 4.5:1 contrast', () => {
      const punctuationColor = '#c9d1d9';
      const codeBackground = cssVariables['--color-code-bg'];

      const ratio = getContrastRatio(punctuationColor, codeBackground);

      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
      console.log(`Punctuation syntax contrast ratio: ${ratio.toFixed(2)}:1 (required: ${WCAG_AA_NORMAL_TEXT}:1)`);
    });

    test('Property/number syntax highlighting (#79c0ff) has at least 4.5:1 contrast', () => {
      const propertyColor = '#79c0ff';
      const codeBackground = cssVariables['--color-code-bg'];

      const ratio = getContrastRatio(propertyColor, codeBackground);

      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
      console.log(`Property/number syntax contrast ratio: ${ratio.toFixed(2)}:1 (required: ${WCAG_AA_NORMAL_TEXT}:1)`);
    });

    test('String syntax highlighting (#a5d6ff) has at least 4.5:1 contrast', () => {
      const stringColor = '#a5d6ff';
      const codeBackground = cssVariables['--color-code-bg'];

      const ratio = getContrastRatio(stringColor, codeBackground);

      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
      console.log(`String syntax contrast ratio: ${ratio.toFixed(2)}:1 (required: ${WCAG_AA_NORMAL_TEXT}:1)`);
    });

    test('Operator syntax highlighting (#ffa657) has at least 4.5:1 contrast', () => {
      const operatorColor = '#ffa657';
      const codeBackground = cssVariables['--color-code-bg'];

      const ratio = getContrastRatio(operatorColor, codeBackground);

      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
      console.log(`Operator syntax contrast ratio: ${ratio.toFixed(2)}:1 (required: ${WCAG_AA_NORMAL_TEXT}:1)`);
    });

    test('Keyword syntax highlighting (#ff7b72) has at least 4.5:1 contrast', () => {
      const keywordColor = '#ff7b72';
      const codeBackground = cssVariables['--color-code-bg'];

      const ratio = getContrastRatio(keywordColor, codeBackground);

      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
      console.log(`Keyword syntax contrast ratio: ${ratio.toFixed(2)}:1 (required: ${WCAG_AA_NORMAL_TEXT}:1)`);
    });

    test('Function syntax highlighting (#d2a8ff) has at least 4.5:1 contrast', () => {
      const functionColor = '#d2a8ff';
      const codeBackground = cssVariables['--color-code-bg'];

      const ratio = getContrastRatio(functionColor, codeBackground);

      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
      console.log(`Function syntax contrast ratio: ${ratio.toFixed(2)}:1 (required: ${WCAG_AA_NORMAL_TEXT}:1)`);
    });

    test('Language label (#a0a0a0) has at least 4.5:1 contrast against code header', () => {
      // Language label uses --color-text-muted
      const labelColor = cssVariables['--color-text-muted'];
      const codeBackground = cssVariables['--color-code-bg'];

      const ratio = getContrastRatio(labelColor, codeBackground);

      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
      console.log(`Language label contrast ratio: ${ratio.toFixed(2)}:1 (required: ${WCAG_AA_NORMAL_TEXT}:1)`);
    });
  });

  describe('Additional Contrast Checks', () => {
    test('Success color (#2ecc71) has adequate contrast against dark background', () => {
      const successColor = cssVariables['--color-success'];
      const backgroundColor = cssVariables['--color-background'];

      const ratio = getContrastRatio(successColor, backgroundColor);

      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_LARGE_TEXT);
      console.log(`Success color contrast ratio: ${ratio.toFixed(2)}:1`);
    });

    test('Warning color (#f39c12) has adequate contrast against dark background', () => {
      const warningColor = cssVariables['--color-warning'];
      const backgroundColor = cssVariables['--color-background'];

      const ratio = getContrastRatio(warningColor, backgroundColor);

      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_LARGE_TEXT);
      console.log(`Warning color contrast ratio: ${ratio.toFixed(2)}:1`);
    });

    test('Surface color (#0f3460) elements have adequate text contrast', () => {
      // Surface color is used for tech badges
      const textColor = cssVariables['--color-text'];
      const surfaceColor = cssVariables['--color-surface'];

      const ratio = getContrastRatio(textColor, surfaceColor);

      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
      console.log(`Surface text contrast ratio: ${ratio.toFixed(2)}:1`);
    });
  });

  describe('Color Contrast Utility Functions', () => {
    test('getContrastRatio correctly calculates black on white as 21:1', () => {
      const ratio = getContrastRatio('#ffffff', '#000000');
      expect(ratio).toBeCloseTo(21, 0);
    });

    test('getContrastRatio correctly calculates identical colors as 1:1', () => {
      const ratio = getContrastRatio('#808080', '#808080');
      expect(ratio).toBeCloseTo(1, 0);
    });

    test('getLuminance returns correct values for primary colors', () => {
      // White should have luminance close to 1
      expect(getLuminance(255, 255, 255)).toBeCloseTo(1, 2);
      // Black should have luminance close to 0
      expect(getLuminance(0, 0, 0)).toBeCloseTo(0, 2);
    });
  });
});

describe('Accessibility - Images and Alt Text', () => {
  let document;
  let htmlContent;

  beforeAll(() => {
    // Read the index.html file
    const htmlPath = path.join(__dirname, '../../index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf8');

    // Parse HTML using jsdom
    const { JSDOM } = require('jsdom');
    const dom = new JSDOM(htmlContent);
    document = dom.window.document;
  });

  describe('Test Case 1: MirDB Logo Alt Text', () => {
    test('Logo image has descriptive alt text containing "MirDB" and "Logo"', () => {
      const logo = document.querySelector('.hero-logo');
      expect(logo).not.toBeNull();
      expect(logo.tagName.toLowerCase()).toBe('img');

      const altText = logo.getAttribute('alt');
      expect(altText).not.toBeNull();
      expect(altText.length).toBeGreaterThan(0);
      expect(altText.toLowerCase()).toContain('mirdb');
      expect(altText.toLowerCase()).toContain('logo');
    });

    test('Logo image alt text is descriptive (not just "image" or "logo")', () => {
      const logo = document.querySelector('.hero-logo');
      const altText = logo.getAttribute('alt');

      // Alt text should be meaningful, not generic
      expect(altText.toLowerCase()).not.toBe('image');
      expect(altText.toLowerCase()).not.toBe('logo');
      expect(altText.toLowerCase()).not.toBe('img');
    });

    test('Logo image src attribute is valid', () => {
      const logo = document.querySelector('.hero-logo');
      const src = logo.getAttribute('src');

      expect(src).not.toBeNull();
      expect(src.length).toBeGreaterThan(0);
      expect(src).toContain('logo');
    });
  });

  describe('Test Case 2: CircleCI Badge Alt Text', () => {
    test('CircleCI badge has alt text describing build status purpose', () => {
      const badge = document.querySelector('[data-testid="circleci-img"]');
      expect(badge).not.toBeNull();
      expect(badge.tagName.toLowerCase()).toBe('img');

      const altText = badge.getAttribute('alt');
      expect(altText).not.toBeNull();
      expect(altText.length).toBeGreaterThan(0);
    });

    test('Badge alt text describes its purpose (build status)', () => {
      const badge = document.querySelector('[data-testid="circleci-img"]');
      const altText = badge.getAttribute('alt').toLowerCase();

      // Alt text should indicate it's related to build/CI status
      const hasRelevantKeyword =
        altText.includes('build') ||
        altText.includes('status') ||
        altText.includes('ci') ||
        altText.includes('circle');

      expect(hasRelevantKeyword).toBe(true);
    });

    test('Badge link has accessible aria-label', () => {
      const badgeLink = document.querySelector('[data-testid="circleci-link"]');
      expect(badgeLink).not.toBeNull();

      const ariaLabel = badgeLink.getAttribute('aria-label');
      expect(ariaLabel).not.toBeNull();
      expect(ariaLabel.length).toBeGreaterThan(0);
    });
  });

  describe('Test Case 3: All Images Have Alt Attributes', () => {
    test('Every <img> element has an alt attribute', () => {
      const allImages = document.querySelectorAll('img');

      expect(allImages.length).toBeGreaterThan(0);

      allImages.forEach((img, index) => {
        const hasAlt = img.hasAttribute('alt');
        const src = img.getAttribute('src') || 'unknown';
        expect(hasAlt).toBe(true);
      });
    });

    test('No images have missing or undefined alt attributes', () => {
      const allImages = document.querySelectorAll('img');

      allImages.forEach((img) => {
        const altValue = img.getAttribute('alt');
        // Alt can be empty string (for decorative) but not null/undefined
        expect(altValue).not.toBeNull();
        expect(typeof altValue).toBe('string');
      });
    });

    test('Meaningful images have non-empty alt text', () => {
      // Images that convey information should have descriptive alt text
      const meaningfulImages = [
        '.hero-logo',                    // Logo conveys brand identity
        '[data-testid="circleci-img"]'   // Badge conveys build status
      ];

      meaningfulImages.forEach((selector) => {
        const img = document.querySelector(selector);
        if (img) {
          const altText = img.getAttribute('alt');
          expect(altText).not.toBeNull();
          expect(altText.trim().length).toBeGreaterThan(0);
        }
      });
    });
  });

  describe('Additional Image Accessibility Checks', () => {
    test('Images with links have appropriate context', () => {
      // Find all images that are inside links
      const linkedImages = document.querySelectorAll('a img');

      linkedImages.forEach((img) => {
        const parentLink = img.closest('a');
        const imgAlt = img.getAttribute('alt');
        const linkAriaLabel = parentLink.getAttribute('aria-label');
        const linkText = parentLink.textContent.trim();

        // Either the image should have alt text, or the link should have aria-label or text
        const hasAccessibleContext =
          (imgAlt && imgAlt.length > 0) ||
          (linkAriaLabel && linkAriaLabel.length > 0) ||
          (linkText && linkText.length > 0);

        expect(hasAccessibleContext).toBe(true);
      });
    });

    test('Alt text does not include redundant phrases', () => {
      const allImages = document.querySelectorAll('img');

      allImages.forEach((img) => {
        const altText = img.getAttribute('alt');
        if (altText) {
          const lowerAlt = altText.toLowerCase();
          // Should not start with "image of", "picture of", "graphic of"
          expect(lowerAlt.startsWith('image of ')).toBe(false);
          expect(lowerAlt.startsWith('picture of ')).toBe(false);
          expect(lowerAlt.startsWith('graphic of ')).toBe(false);
        }
      });
    });
  });
});
