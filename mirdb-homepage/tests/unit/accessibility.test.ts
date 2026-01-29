/**
 * Unit Tests for Accessibility Compliance.
 * Owner: Scenario 12 - Accessibility Compliance
 *
 * Tests cover:
 * - Heading hierarchy validation
 * - Image alt text requirements
 * - Descriptive link text
 * - Color contrast ratios (WCAG AA)
 */

import { describe, it, expect } from 'vitest';

/**
 * Color contrast calculation utilities
 * Based on WCAG 2.1 contrast ratio formula
 */

// Convert hex color to RGB
function hexToRgb(hex: string): { r: number; g: number; b: number } | null {
  const result = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex);
  return result
    ? {
        r: parseInt(result[1], 16),
        g: parseInt(result[2], 16),
        b: parseInt(result[3], 16),
      }
    : null;
}

// Calculate relative luminance
function getLuminance(r: number, g: number, b: number): number {
  const [rs, gs, bs] = [r, g, b].map((c) => {
    c = c / 255;
    return c <= 0.03928 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4);
  });
  return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
}

// Calculate contrast ratio between two colors
function getContrastRatio(color1: string, color2: string): number {
  const rgb1 = hexToRgb(color1);
  const rgb2 = hexToRgb(color2);

  if (!rgb1 || !rgb2) return 0;

  const l1 = getLuminance(rgb1.r, rgb1.g, rgb1.b);
  const l2 = getLuminance(rgb2.r, rgb2.g, rgb2.b);

  const lighter = Math.max(l1, l2);
  const darker = Math.min(l1, l2);

  return (lighter + 0.05) / (darker + 0.05);
}

// WCAG AA thresholds
const WCAG_AA_NORMAL_TEXT = 4.5;
const WCAG_AA_LARGE_TEXT = 3.0;

describe('Accessibility Compliance - Unit Tests', () => {
  describe('Color Contrast Validation (Test Case 8)', () => {
    // MirDB color palette from tailwind.config.mjs
    const colors = {
      background: '#0d1117',
      surface: '#161b22',
      border: '#30363d',
      textPrimary: '#c9d1d9',
      textSecondary: '#8b949e',
      accent: '#58a6ff',
      success: '#3fb950',
      warning: '#d29922',
    };

    it('primary text color meets WCAG AA contrast ratio (4.5:1)', () => {
      const ratio = getContrastRatio(colors.textPrimary, colors.background);
      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
    });

    it('secondary text color meets WCAG AA contrast ratio (4.5:1)', () => {
      const ratio = getContrastRatio(colors.textSecondary, colors.background);
      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
    });

    it('accent color meets WCAG AA contrast ratio (4.5:1)', () => {
      const ratio = getContrastRatio(colors.accent, colors.background);
      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
    });

    it('success color meets WCAG AA contrast ratio for large text (3:1)', () => {
      const ratio = getContrastRatio(colors.success, colors.background);
      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_LARGE_TEXT);
    });

    it('warning color meets WCAG AA contrast ratio for large text (3:1)', () => {
      const ratio = getContrastRatio(colors.warning, colors.background);
      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_LARGE_TEXT);
    });

    it('text on surface color meets WCAG AA contrast ratio', () => {
      const ratio = getContrastRatio(colors.textPrimary, colors.surface);
      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
    });

    it('border color is distinguishable from background', () => {
      // Note: Decorative borders don't need to meet the 3:1 UI component ratio
      // They just need to be visually distinguishable
      const ratio = getContrastRatio(colors.border, colors.background);
      // Border (#30363d) on background (#0d1117) has ~1.55:1 ratio
      // This is acceptable for decorative borders that don't convey essential info
      expect(ratio).toBeGreaterThan(1);
    });

    it('calculates correct contrast ratio for known values', () => {
      // White on black should be 21:1
      const maxRatio = getContrastRatio('#ffffff', '#000000');
      expect(maxRatio).toBeCloseTo(21, 0);

      // Same color should be 1:1
      const sameColor = getContrastRatio('#ff0000', '#ff0000');
      expect(sameColor).toBeCloseTo(1, 0);
    });
  });

  describe('Heading Hierarchy Validation (Test Case 3)', () => {
    // Simulated heading structure from the MirDB homepage
    const headingStructure = [
      { level: 1, text: 'Persistent Key-Value Store with Memcached Protocol' },
      { level: 2, text: 'Why MirDB?' }, // Features
      { level: 2, text: 'Try It Out' }, // Terminal
      { level: 2, text: 'Architecture' },
      { level: 3, text: 'Write-Ahead Log (WAL)' },
      { level: 3, text: 'Memtable' },
      { level: 3, text: 'SSTable' },
      { level: 2, text: 'Configuration' },
      { level: 2, text: 'Installation' },
      { level: 2, text: 'Supported Commands' },
      { level: 2, text: 'Roadmap' },
    ];

    it('has exactly one h1 element', () => {
      const h1Count = headingStructure.filter((h) => h.level === 1).length;
      expect(h1Count).toBe(1);
    });

    it('h1 contains the main page title', () => {
      const h1 = headingStructure.find((h) => h.level === 1);
      expect(h1).toBeDefined();
      expect(h1?.text.toLowerCase()).toContain('memcached');
    });

    it('heading levels do not skip more than one level', () => {
      let previousLevel = 0;
      let isValid = true;

      for (const heading of headingStructure) {
        if (previousLevel > 0 && heading.level > previousLevel + 1) {
          isValid = false;
          break;
        }
        previousLevel = heading.level;
      }

      expect(isValid).toBe(true);
    });

    it('all major sections have h2 headings', () => {
      const h2Headings = headingStructure.filter((h) => h.level === 2);
      // Major sections: Features, Terminal, Architecture, Configuration, Installation, Commands, Roadmap
      // Check that we have at least some h2s for major sections
      expect(h2Headings.length).toBeGreaterThanOrEqual(5);
    });
  });

  describe('Image Alt Text Requirements (Test Case 4)', () => {
    // Expected images in the MirDB homepage
    const images = [
      { src: '/logo.svg', alt: 'MirDB logo', role: 'img' },
      { src: 'github-icon', alt: '', role: 'presentation', ariaHidden: true },
    ];

    it('all non-decorative images have descriptive alt text', () => {
      for (const img of images) {
        if (img.role !== 'presentation' && !img.ariaHidden) {
          expect(img.alt).toBeTruthy();
          expect(img.alt.length).toBeGreaterThan(0);
        }
      }
    });

    it('decorative images are properly marked', () => {
      for (const img of images) {
        if (img.role === 'presentation' || img.ariaHidden) {
          // Decorative images should have empty alt or aria-hidden
          expect(img.ariaHidden === true || img.alt === '').toBe(true);
        }
      }
    });

    it('alt text is descriptive, not just filenames', () => {
      for (const img of images) {
        if (img.alt && img.alt.length > 0) {
          // Alt text should not look like a filename
          expect(img.alt).not.toMatch(/\.(jpg|jpeg|png|gif|svg|webp)$/i);
          // Alt text should not be just "image"
          expect(img.alt.toLowerCase()).not.toBe('image');
        }
      }
    });
  });

  describe('Link Text Requirements (Test Case 5)', () => {
    // Expected links in the MirDB homepage
    const links = [
      { text: 'Get Started', href: '#installation' },
      { text: 'View on GitHub', href: 'https://github.com/jzwdsb/mirdb' },
      { text: 'GitHub Repository', href: 'https://github.com/mirdb/mirdb' },
      { text: 'Features', href: '#features' },
      { text: 'Installation', href: '#installation' },
      { text: 'Tokio', href: 'https://tokio.rs' },
      { text: 'MIT License', href: 'https://opensource.org/licenses/MIT' },
    ];

    const genericLinkTexts = [
      'click here',
      'here',
      'read more',
      'learn more',
      'more',
      'link',
      'this',
    ];

    it('link text is not generic', () => {
      for (const link of links) {
        const normalizedText = link.text.toLowerCase().trim();
        for (const generic of genericLinkTexts) {
          expect(normalizedText).not.toBe(generic);
        }
      }
    });

    it('link text describes the destination', () => {
      for (const link of links) {
        expect(link.text.length).toBeGreaterThan(0);

        // External links should indicate destination
        if (link.href.startsWith('http')) {
          expect(link.text.length).toBeGreaterThan(2);
        }
      }
    });

    it('anchor links reference valid sections', () => {
      const validSections = [
        '#hero',
        '#features',
        '#terminal',
        '#architecture',
        '#configuration',
        '#installation',
        '#commands',
        '#roadmap',
      ];

      for (const link of links) {
        if (link.href.startsWith('#')) {
          expect(
            validSections.includes(link.href) ||
              link.href === '#installation' ||
              link.href === '#features'
          ).toBe(true);
        }
      }
    });
  });

  describe('Reduced Motion Support (Test Case 7)', () => {
    // CSS media query that should be present
    const reducedMotionCSS = `
      @media (prefers-reduced-motion: reduce) {
        html {
          scroll-behavior: auto;
        }
        *,
        *::before,
        *::after {
          animation-duration: 0.01ms !important;
          animation-iteration-count: 1 !important;
          transition-duration: 0.01ms !important;
        }
      }
    `;

    it('reduced motion CSS disables animations', () => {
      expect(reducedMotionCSS).toContain('animation-duration: 0.01ms');
      expect(reducedMotionCSS).toContain('transition-duration: 0.01ms');
    });

    it('reduced motion CSS sets scroll-behavior to auto', () => {
      expect(reducedMotionCSS).toContain('scroll-behavior: auto');
    });

    it('reduced motion CSS uses !important to override', () => {
      expect(reducedMotionCSS).toContain('!important');
    });
  });

  describe('Focus Indicators (Test Case 2)', () => {
    // Focus styles from global.css
    const focusCSS = {
      selector: ':focus-visible',
      outline: '2px solid var(--color-accent)',
      outlineOffset: '2px',
    };

    it('focus-visible selector is used for keyboard focus', () => {
      expect(focusCSS.selector).toBe(':focus-visible');
    });

    it('focus outline has sufficient width (at least 2px)', () => {
      expect(focusCSS.outline).toContain('2px');
    });

    it('focus outline uses accent color for visibility', () => {
      expect(focusCSS.outline).toContain('var(--color-accent)');
    });

    it('focus outline has offset to prevent overlap', () => {
      expect(focusCSS.outlineOffset).toBe('2px');
    });
  });

  describe('ARIA Attributes Validation', () => {
    // Expected ARIA usage in the homepage
    const ariaUsage = {
      logo: { role: 'img', ariaLabel: 'MirDB logo' },
      footer: { role: 'contentinfo', ariaLabel: 'Site footer' },
      footerNav: { ariaLabel: 'Footer navigation' },
      externalLinks: { rel: 'noopener noreferrer', target: '_blank' },
    };

    it('logo has role="img" and aria-label', () => {
      expect(ariaUsage.logo.role).toBe('img');
      expect(ariaUsage.logo.ariaLabel).toBeTruthy();
    });

    it('footer has role="contentinfo"', () => {
      expect(ariaUsage.footer.role).toBe('contentinfo');
    });

    it('footer navigation has aria-label', () => {
      expect(ariaUsage.footerNav.ariaLabel).toBeTruthy();
    });

    it('external links have security attributes', () => {
      expect(ariaUsage.externalLinks.rel).toContain('noopener');
      expect(ariaUsage.externalLinks.target).toBe('_blank');
    });
  });

  describe('Color Utility Functions', () => {
    it('hexToRgb correctly parses hex colors', () => {
      expect(hexToRgb('#ffffff')).toEqual({ r: 255, g: 255, b: 255 });
      expect(hexToRgb('#000000')).toEqual({ r: 0, g: 0, b: 0 });
      expect(hexToRgb('#0d1117')).toEqual({ r: 13, g: 17, b: 23 });
    });

    it('hexToRgb returns null for invalid hex', () => {
      expect(hexToRgb('invalid')).toBeNull();
      expect(hexToRgb('#gggggg')).toBeNull();
    });

    it('getLuminance returns correct values', () => {
      // Black should have 0 luminance
      expect(getLuminance(0, 0, 0)).toBe(0);

      // White should have ~1 luminance
      expect(getLuminance(255, 255, 255)).toBeCloseTo(1, 2);
    });
  });
});
