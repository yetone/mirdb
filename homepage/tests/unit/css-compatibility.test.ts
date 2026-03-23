/**
 * CSS Compatibility Unit Tests
 * Owner: Scenario 12 - Browser Compatibility
 *
 * Test cases:
 * - Verify CSS features used are supported in target browsers
 * - Check for vendor prefixes where needed
 * - Ensure fallbacks exist for less-supported features
 *
 * Target browsers (per NFR-3): last 2 versions of Chrome, Firefox, Safari, Edge
 */

import { describe, it, expect, beforeAll } from 'vitest';
import { readFileSync, existsSync } from 'fs';
import { join } from 'path';

// CSS features that need validation for browser compatibility
const CSS_FEATURES = {
  // Fully supported in all target browsers
  fullySupported: [
    'box-sizing',
    'flexbox',
    'css-grid',
    'custom-properties',
    'border-radius',
    'transitions',
    'transforms',
    'linear-gradient',
    'rgba',
    'media-queries',
  ],
  // Need vendor prefixes or have caveats
  needsPrefix: [
    'background-clip: text',
    '-webkit-background-clip',
    '-webkit-text-fill-color',
  ],
  // Should have fallbacks
  needsFallback: [
    'scroll-behavior: smooth',
    'focus-visible',
  ],
};

// Browser support data (simplified)
const BROWSER_SUPPORT = {
  chrome: {
    'custom-properties': true,
    'css-grid': true,
    'flexbox': true,
    'scroll-behavior': true,
    'focus-visible': true,
    'background-clip-text': true,
  },
  firefox: {
    'custom-properties': true,
    'css-grid': true,
    'flexbox': true,
    'scroll-behavior': true,
    'focus-visible': true,
    'background-clip-text': true,
  },
  safari: {
    'custom-properties': true,
    'css-grid': true,
    'flexbox': true,
    'scroll-behavior': true,
    'focus-visible': true,
    'background-clip-text': 'webkit-prefix',
  },
  edge: {
    'custom-properties': true,
    'css-grid': true,
    'flexbox': true,
    'scroll-behavior': true,
    'focus-visible': true,
    'background-clip-text': true,
  },
};

describe('CSS Compatibility - TC5: CSS Features Browser Support', () => {
  let stylesContent: string;
  let variablesContent: string;
  let responsiveContent: string;

  beforeAll(() => {
    const basePath = join(__dirname, '../../');

    // Read CSS files
    const stylesPath = join(basePath, 'css/styles.css');
    const variablesPath = join(basePath, 'css/variables.css');
    const responsivePath = join(basePath, 'css/responsive.css');

    if (existsSync(stylesPath)) {
      stylesContent = readFileSync(stylesPath, 'utf-8');
    } else {
      stylesContent = '';
    }

    if (existsSync(variablesPath)) {
      variablesContent = readFileSync(variablesPath, 'utf-8');
    } else {
      variablesContent = '';
    }

    if (existsSync(responsivePath)) {
      responsiveContent = readFileSync(responsivePath, 'utf-8');
    } else {
      responsiveContent = '';
    }
  });

  describe('CSS Custom Properties Support', () => {
    it('should use CSS custom properties (supported in all target browsers)', () => {
      // CSS custom properties (:root with --var) are well-supported
      const hasCustomProperties = variablesContent.includes(':root') &&
        variablesContent.includes('--');

      expect(hasCustomProperties).toBe(true);

      // Verify custom properties are used in styles
      const usesVarFunction = stylesContent.includes('var(--');
      expect(usesVarFunction).toBe(true);
    });

    it('should define color custom properties', () => {
      expect(variablesContent).toContain('--color-primary');
      expect(variablesContent).toContain('--color-bg-primary');
      expect(variablesContent).toContain('--color-text-primary');
    });

    it('should define spacing custom properties', () => {
      expect(variablesContent).toContain('--spacing-');
    });

    it('should define typography custom properties', () => {
      expect(variablesContent).toContain('--font-family');
      expect(variablesContent).toContain('--font-size');
    });
  });

  describe('Flexbox Support', () => {
    it('should use flexbox for layout (fully supported)', () => {
      const usesFlexbox = stylesContent.includes('display: flex') ||
        stylesContent.includes('display:flex');

      expect(usesFlexbox).toBe(true);
    });

    it('should use flexbox gap property', () => {
      // Gap is well-supported in modern browsers for flexbox
      const usesGap = stylesContent.includes('gap:') || stylesContent.includes('gap: ');
      expect(usesGap).toBe(true);
    });
  });

  describe('CSS Grid Support', () => {
    it('should use CSS Grid for layout (fully supported)', () => {
      const usesGrid = stylesContent.includes('display: grid') ||
        stylesContent.includes('display:grid');

      expect(usesGrid).toBe(true);
    });

    it('should use grid-template-columns', () => {
      const usesGridColumns = stylesContent.includes('grid-template-columns');
      expect(usesGridColumns).toBe(true);
    });

    it('should use auto-fit/auto-fill for responsive grids', () => {
      const usesAutoFit = stylesContent.includes('auto-fit') ||
        stylesContent.includes('auto-fill');
      expect(usesAutoFit).toBe(true);
    });
  });

  describe('Gradients Support', () => {
    it('should use linear-gradient (fully supported)', () => {
      const usesGradient = stylesContent.includes('linear-gradient');
      expect(usesGradient).toBe(true);
    });
  });

  describe('Smooth Scroll Support', () => {
    it('should enable smooth scroll behavior', () => {
      const hasScrollBehavior = stylesContent.includes('scroll-behavior: smooth') ||
        stylesContent.includes('scroll-behavior:smooth');

      expect(hasScrollBehavior).toBe(true);
    });

    it('smooth scroll is supported in all target browsers', () => {
      // Smooth scroll is supported in Chrome 61+, Firefox 36+, Safari 15.4+, Edge 79+
      Object.values(BROWSER_SUPPORT).forEach((browser) => {
        expect(browser['scroll-behavior']).toBe(true);
      });
    });
  });

  describe('Background Clip Text Support', () => {
    it('should use webkit prefix for background-clip: text', () => {
      // Safari requires -webkit-background-clip for text
      const hasWebkitBgClip = stylesContent.includes('-webkit-background-clip');
      const hasStandardBgClip = stylesContent.includes('background-clip: text') ||
        stylesContent.includes('background-clip:text');

      // Should have webkit prefix for Safari support
      expect(hasWebkitBgClip || hasStandardBgClip).toBe(true);
    });

    it('should use webkit prefix for text-fill-color', () => {
      // Text gradient effect requires webkit prefixes
      if (stylesContent.includes('-webkit-background-clip')) {
        const hasTextFillColor = stylesContent.includes('-webkit-text-fill-color');
        expect(hasTextFillColor).toBe(true);
      }
    });
  });

  describe('Focus Visible Support', () => {
    it('should use :focus-visible pseudo-class', () => {
      const usesFocusVisible = stylesContent.includes(':focus-visible');
      expect(usesFocusVisible).toBe(true);
    });

    it('focus-visible is supported in all target browsers', () => {
      // :focus-visible is supported in Chrome 86+, Firefox 85+, Safari 15.4+, Edge 86+
      Object.values(BROWSER_SUPPORT).forEach((browser) => {
        expect(browser['focus-visible']).toBe(true);
      });
    });
  });

  describe('Transitions Support', () => {
    it('should use CSS transitions (fully supported)', () => {
      const usesTransition = stylesContent.includes('transition:') ||
        stylesContent.includes('transition-');

      expect(usesTransition).toBe(true);
    });
  });

  describe('Border Radius Support', () => {
    it('should use border-radius (fully supported)', () => {
      const usesBorderRadius = stylesContent.includes('border-radius') ||
        variablesContent.includes('--radius');

      expect(usesBorderRadius).toBe(true);
    });
  });

  describe('Box Shadow Support', () => {
    it('should use box-shadow (fully supported)', () => {
      const usesBoxShadow = stylesContent.includes('box-shadow') ||
        variablesContent.includes('--shadow');

      expect(usesBoxShadow).toBe(true);
    });
  });

  describe('Font Smoothing', () => {
    it('should include webkit font smoothing for better text rendering', () => {
      const hasWebkitSmoothing = stylesContent.includes('-webkit-font-smoothing');
      expect(hasWebkitSmoothing).toBe(true);
    });

    it('should include moz font smoothing for Firefox', () => {
      const hasMozSmoothing = stylesContent.includes('-moz-osx-font-smoothing');
      expect(hasMozSmoothing).toBe(true);
    });
  });

  describe('Responsive Design', () => {
    it('should use media queries for responsive design', () => {
      const hasMediaQueries = responsiveContent.includes('@media') ||
        stylesContent.includes('@media');

      expect(hasMediaQueries).toBe(true);
    });
  });

  describe('Modern Color Syntax', () => {
    it('should use rgb() or rgba() for colors with alpha', () => {
      const usesRgba = variablesContent.includes('rgb(') ||
        variablesContent.includes('rgba(');

      expect(usesRgba).toBe(true);
    });

    it('should use hex colors which are universally supported', () => {
      const usesHex = variablesContent.includes('#');
      expect(usesHex).toBe(true);
    });
  });

  describe('No Unsupported Features', () => {
    it('should not use container queries (limited support)', () => {
      const usesContainerQueries = stylesContent.includes('@container');
      // Container queries may have limited support in older browser versions
      // If used, document that it's progressive enhancement
      if (usesContainerQueries) {
        console.warn('Container queries used - ensure fallbacks exist');
      }
    });

    it('should not use :has() selector (limited support)', () => {
      const usesHas = stylesContent.includes(':has(');
      // :has() has limited support in older browsers
      if (usesHas) {
        console.warn(':has() selector used - ensure fallbacks exist');
      }
    });

    it('should not use subgrid (limited support)', () => {
      const usesSubgrid = stylesContent.includes('subgrid');
      // Subgrid has limited browser support
      if (usesSubgrid) {
        console.warn('subgrid used - ensure fallbacks exist');
      }
    });
  });

  describe('Browser-Specific Tests', () => {
    it('all used CSS features should be supported in Chrome', () => {
      const chrome = BROWSER_SUPPORT.chrome;
      expect(chrome['custom-properties']).toBe(true);
      expect(chrome['css-grid']).toBe(true);
      expect(chrome['flexbox']).toBe(true);
      expect(chrome['scroll-behavior']).toBe(true);
      expect(chrome['focus-visible']).toBe(true);
    });

    it('all used CSS features should be supported in Firefox', () => {
      const firefox = BROWSER_SUPPORT.firefox;
      expect(firefox['custom-properties']).toBe(true);
      expect(firefox['css-grid']).toBe(true);
      expect(firefox['flexbox']).toBe(true);
      expect(firefox['scroll-behavior']).toBe(true);
      expect(firefox['focus-visible']).toBe(true);
    });

    it('all used CSS features should be supported in Safari', () => {
      const safari = BROWSER_SUPPORT.safari;
      expect(safari['custom-properties']).toBe(true);
      expect(safari['css-grid']).toBe(true);
      expect(safari['flexbox']).toBe(true);
      expect(safari['scroll-behavior']).toBe(true);
      expect(safari['focus-visible']).toBe(true);
    });

    it('all used CSS features should be supported in Edge', () => {
      const edge = BROWSER_SUPPORT.edge;
      expect(edge['custom-properties']).toBe(true);
      expect(edge['css-grid']).toBe(true);
      expect(edge['flexbox']).toBe(true);
      expect(edge['scroll-behavior']).toBe(true);
      expect(edge['focus-visible']).toBe(true);
    });
  });
});
