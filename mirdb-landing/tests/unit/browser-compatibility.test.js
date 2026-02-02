/**
 * Browser Compatibility Unit Tests
 * Owner: Scenario 15 - Browser Compatibility
 *
 * Tests:
 * - CSS vendor prefixes for cross-browser support
 * - Required prefixes for Chrome, Firefox, Safari, and Edge
 */

import { readFileSync } from 'fs';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const projectRoot = resolve(__dirname, '../..');

describe('Browser Compatibility - CSS Vendor Prefixes', () => {
  let resetCssContent;

  beforeAll(() => {
    const resetCssPath = resolve(projectRoot, 'css/utilities/reset.css');
    resetCssContent = readFileSync(resetCssPath, 'utf-8');
  });

  describe('Box Sizing', () => {
    test('should include webkit vendor prefix for box-sizing', () => {
      expect(resetCssContent).toContain('-webkit-box-sizing');
    });

    test('should include moz vendor prefix for box-sizing', () => {
      expect(resetCssContent).toContain('-moz-box-sizing');
    });

    test('should include standard box-sizing property', () => {
      expect(resetCssContent).toMatch(/[^-]box-sizing:\s*border-box/);
    });
  });

  describe('Text Size Adjust', () => {
    test('should include webkit vendor prefix for text-size-adjust', () => {
      expect(resetCssContent).toContain('-webkit-text-size-adjust');
    });

    test('should include ms vendor prefix for text-size-adjust', () => {
      expect(resetCssContent).toContain('-ms-text-size-adjust');
    });
  });

  describe('Font Smoothing', () => {
    test('should include webkit font smoothing', () => {
      expect(resetCssContent).toContain('-webkit-font-smoothing');
    });

    test('should include Firefox/macOS font smoothing', () => {
      expect(resetCssContent).toContain('-moz-osx-font-smoothing');
    });
  });

  describe('Appearance Property', () => {
    test('should include webkit vendor prefix for appearance', () => {
      expect(resetCssContent).toContain('-webkit-appearance');
    });

    test('should include moz vendor prefix for appearance', () => {
      expect(resetCssContent).toContain('-moz-appearance');
    });

    test('should include standard appearance property', () => {
      expect(resetCssContent).toMatch(/[^-]appearance/);
    });
  });

  describe('Placeholder Styling', () => {
    test('should include webkit input placeholder', () => {
      expect(resetCssContent).toContain('::-webkit-input-placeholder');
    });

    test('should include moz placeholder', () => {
      expect(resetCssContent).toContain('::-moz-placeholder');
    });

    test('should include ms input placeholder', () => {
      expect(resetCssContent).toContain(':-ms-input-placeholder');
    });

    test('should include standard placeholder', () => {
      expect(resetCssContent).toContain('::placeholder');
    });
  });

  describe('Hyphens Property', () => {
    test('should include webkit vendor prefix for hyphens', () => {
      expect(resetCssContent).toContain('-webkit-hyphens');
    });

    test('should include ms vendor prefix for hyphens', () => {
      expect(resetCssContent).toContain('-ms-hyphens');
    });

    test('should include standard hyphens property', () => {
      expect(resetCssContent).toMatch(/[^-]hyphens/);
    });
  });

  describe('Touch Highlight', () => {
    test('should include webkit tap highlight color removal', () => {
      expect(resetCssContent).toContain('-webkit-tap-highlight-color');
    });
  });

  describe('Selection Styling', () => {
    test('should include moz selection pseudo-element', () => {
      expect(resetCssContent).toContain('::-moz-selection');
    });

    test('should include standard selection pseudo-element', () => {
      expect(resetCssContent).toContain('::selection');
    });
  });

  describe('Scrollbar Styling', () => {
    test('should include webkit scrollbar styling', () => {
      expect(resetCssContent).toContain('::-webkit-scrollbar');
    });

    test('should include Firefox scrollbar-width property', () => {
      expect(resetCssContent).toContain('scrollbar-width');
    });

    test('should include Firefox scrollbar-color property', () => {
      expect(resetCssContent).toContain('scrollbar-color');
    });
  });

  describe('Flexbox Vendor Prefixes', () => {
    test('should include webkit-box display for old Safari/Chrome', () => {
      expect(resetCssContent).toContain('-webkit-box');
    });

    test('should include webkit-flex display', () => {
      expect(resetCssContent).toContain('-webkit-flex');
    });

    test('should include ms-flexbox display for IE/old Edge', () => {
      expect(resetCssContent).toContain('-ms-flexbox');
    });
  });

  describe('Transform Vendor Prefixes', () => {
    test('should include webkit transform', () => {
      expect(resetCssContent).toContain('-webkit-transform');
    });

    test('should include webkit backface-visibility', () => {
      expect(resetCssContent).toContain('-webkit-backface-visibility');
    });
  });

  describe('Animation Vendor Prefixes', () => {
    test('should include webkit keyframes', () => {
      expect(resetCssContent).toContain('@-webkit-keyframes');
    });

    test('should include webkit animation property', () => {
      expect(resetCssContent).toContain('-webkit-animation');
    });
  });
});

describe('Browser Compatibility - Variables CSS', () => {
  let variablesCssContent;

  beforeAll(() => {
    const variablesCssPath = resolve(projectRoot, 'css/utilities/variables.css');
    variablesCssContent = readFileSync(variablesCssPath, 'utf-8');
  });

  test('should use CSS custom properties (variables)', () => {
    expect(variablesCssContent).toMatch(/--color-\w+/);
  });

  test('should include font-family stack for cross-browser support', () => {
    // Should include system font stack for cross-platform/browser support
    expect(variablesCssContent).toContain('-apple-system');
    expect(variablesCssContent).toContain('BlinkMacSystemFont');
    expect(variablesCssContent).toContain('Segoe UI');
  });

  test('should include dark mode media query', () => {
    expect(variablesCssContent).toContain('prefers-color-scheme: dark');
  });

  test('should include reduced motion support', () => {
    expect(variablesCssContent).toContain('prefers-reduced-motion');
  });
});

describe('Browser Compatibility - Responsive CSS', () => {
  let responsiveCssContent;

  beforeAll(() => {
    const responsiveCssPath = resolve(projectRoot, 'css/utilities/responsive.css');
    responsiveCssContent = readFileSync(responsiveCssPath, 'utf-8');
  });

  test('should include webkit overflow scrolling for iOS', () => {
    expect(responsiveCssContent).toContain('-webkit-overflow-scrolling');
  });

  test('should include reduced motion media query', () => {
    expect(responsiveCssContent).toContain('prefers-reduced-motion');
  });
});
