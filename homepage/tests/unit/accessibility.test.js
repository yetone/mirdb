/**
 * Accessibility Unit Tests
 * Owner: Scenario 6 - Accessibility
 *
 * Accessibility tests for the MirDB homepage.
 *
 * Expected test coverage:
 * - Semantic HTML structure validation
 * - ARIA labels present where needed
 * - Color contrast ratios
 * - Keyboard navigation support
 * - Screen reader compatibility
 *
 * Requirements traced:
 * - REQ-5: Basic accessibility considerations
 */

const fs = require('fs');
const path = require('path');

// Read the HTML file
const htmlPath = path.resolve(__dirname, '../../index.html');
const htmlContent = fs.readFileSync(htmlPath, 'utf8');

// Helper function to parse HTML and query elements
function createDocumentFromHTML(html) {
  const { JSDOM } = require('jsdom');
  const dom = new JSDOM(html);
  return dom.window.document;
}

let document;

beforeAll(() => {
  document = createDocumentFromHTML(htmlContent);
});

describe('Accessibility Compliance - Semantic HTML', () => {
  test('TC1: Page contains <header>, <nav>, <main>, and <footer> elements', () => {
    const header = document.querySelector('header');
    const nav = document.querySelector('nav');
    const main = document.querySelector('main');
    const footer = document.querySelector('footer');

    expect(header).not.toBeNull();
    expect(nav).not.toBeNull();
    expect(main).not.toBeNull();
    expect(footer).not.toBeNull();
  });

  test('TC2: Page contains exactly one h1 element (product name)', () => {
    const h1Elements = document.querySelectorAll('h1');
    expect(h1Elements.length).toBe(1);

    // Check that the h1 contains the product name
    const h1Text = h1Elements[0].textContent.toLowerCase();
    expect(h1Text).toContain('mirdb');
  });

  test('TC3: Headings follow logical order without skipping levels', () => {
    const headings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
    const headingLevels = Array.from(headings).map(h => parseInt(h.tagName.charAt(1)));

    // Should start with h1
    expect(headingLevels[0]).toBe(1);

    // Check that no levels are skipped (e.g., h1 -> h3 without h2)
    for (let i = 1; i < headingLevels.length; i++) {
      const currentLevel = headingLevels[i];
      const previousLevel = headingLevels[i - 1];

      // When going deeper, should not skip more than one level
      if (currentLevel > previousLevel) {
        expect(currentLevel - previousLevel).toBeLessThanOrEqual(1);
      }
    }
  });

  test('TC4: Logo image has alt attribute with descriptive text', () => {
    const logoImg = document.querySelector('.logo img');
    expect(logoImg).not.toBeNull();

    const altText = logoImg.getAttribute('alt');
    expect(altText).not.toBeNull();
    expect(altText.length).toBeGreaterThan(0);
    expect(altText.toLowerCase()).toContain('logo');
  });
});

describe('Accessibility Compliance - Color Contrast', () => {
  test('TC5: Text-to-background contrast ratio meets WCAG AA (4.5:1)', () => {
    // Read the CSS file to check color values
    const cssPath = path.resolve(__dirname, '../../css/styles.css');
    const cssContent = fs.readFileSync(cssPath, 'utf8');

    // Extract color values from CSS custom properties
    const textColorMatch = cssContent.match(/--color-text:\s*([^;]+)/);
    const bgColorMatch = cssContent.match(/--color-background:\s*([^;]+)/);

    expect(textColorMatch).not.toBeNull();
    expect(bgColorMatch).not.toBeNull();

    const textColor = textColorMatch[1].trim();
    const bgColor = bgColorMatch[1].trim();

    // Calculate contrast ratio
    const contrastRatio = calculateContrastRatio(textColor, bgColor);

    // WCAG AA requires 4.5:1 for normal text
    expect(contrastRatio).toBeGreaterThanOrEqual(4.5);
  });
});

describe('Accessibility Compliance - Skip Navigation', () => {
  test('TC7: Page has a Skip to main content link for keyboard users', () => {
    const skipLink = document.querySelector('a[href="#main"], a[href="#main-content"], .skip-link, .skip-nav');

    // The skip link should exist
    expect(skipLink).not.toBeNull();

    // It should contain text about skipping to content
    const linkText = skipLink.textContent.toLowerCase();
    expect(linkText).toMatch(/skip|main|content/i);
  });
});

// Helper function to calculate color contrast ratio
function calculateContrastRatio(color1, color2) {
  const rgb1 = parseColor(color1);
  const rgb2 = parseColor(color2);

  const l1 = getRelativeLuminance(rgb1);
  const l2 = getRelativeLuminance(rgb2);

  const lighter = Math.max(l1, l2);
  const darker = Math.min(l1, l2);

  return (lighter + 0.05) / (darker + 0.05);
}

function parseColor(color) {
  // Handle hex colors
  if (color.startsWith('#')) {
    const hex = color.slice(1);
    if (hex.length === 3) {
      return {
        r: parseInt(hex[0] + hex[0], 16),
        g: parseInt(hex[1] + hex[1], 16),
        b: parseInt(hex[2] + hex[2], 16)
      };
    } else if (hex.length === 6) {
      return {
        r: parseInt(hex.slice(0, 2), 16),
        g: parseInt(hex.slice(2, 4), 16),
        b: parseInt(hex.slice(4, 6), 16)
      };
    }
  }
  // Default to white if parsing fails
  return { r: 255, g: 255, b: 255 };
}

function getRelativeLuminance(rgb) {
  const sRGB = [rgb.r / 255, rgb.g / 255, rgb.b / 255];

  const linearRGB = sRGB.map(c => {
    return c <= 0.03928 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4);
  });

  return 0.2126 * linearRGB[0] + 0.7152 * linearRGB[1] + 0.0722 * linearRGB[2];
}
