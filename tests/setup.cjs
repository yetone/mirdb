const fs = require('fs');
const path = require('path');
require('@testing-library/jest-dom');

/**
 * Setup file for Jest tests
 * Loads the HTML content before each test
 */
beforeEach(() => {
  const htmlPath = path.resolve(__dirname, '../index.html');
  const html = fs.readFileSync(htmlPath, 'utf8');
  document.documentElement.innerHTML = html;
});

/**
 * Helper to calculate color contrast ratio per WCAG 2.1
 * @param {string} color1 - First color (rgb or hex)
 * @param {string} color2 - Second color (rgb or hex)
 * @returns {number} Contrast ratio
 */
global.getContrastRatio = (color1, color2) => {
  const getLuminance = (color) => {
    let r, g, b;
    if (color.startsWith('#')) {
      const hex = color.slice(1);
      r = parseInt(hex.substr(0, 2), 16) / 255;
      g = parseInt(hex.substr(2, 2), 16) / 255;
      b = parseInt(hex.substr(4, 2), 16) / 255;
    } else if (color.startsWith('rgb')) {
      const match = color.match(/\d+/g);
      if (match) {
        r = parseInt(match[0]) / 255;
        g = parseInt(match[1]) / 255;
        b = parseInt(match[2]) / 255;
      }
    } else {
      if (color === 'white') return 1;
      if (color === 'black') return 0;
      return 0.5;
    }

    const gammaCorrect = (value) => {
      return value <= 0.03928
        ? value / 12.92
        : Math.pow((value + 0.055) / 1.055, 2.4);
    };

    return 0.2126 * gammaCorrect(r) + 0.7152 * gammaCorrect(g) + 0.0722 * gammaCorrect(b);
  };

  const lum1 = getLuminance(color1);
  const lum2 = getLuminance(color2);
  const lighter = Math.max(lum1, lum2);
  const darker = Math.min(lum1, lum2);

  return (lighter + 0.05) / (darker + 0.05);
};
