/**
 * Accessibility Unit Tests
 * Owner: Scenarios 13-15 - Accessibility
 *
 * Tests:
 * - Semantic HTML landmarks (header, main, nav, section, footer)
 * - Image alt text
 * - Heading hierarchy
 * - ARIA labels on code blocks
 * - Descriptive link text
 * - Color contrast validation (WCAG 2.1 AA)
 * - ARIA attribute presence
 * - Document language
 */
import { describe, it, expect, beforeAll } from 'vitest';
import { readFileSync } from 'fs';
import { JSDOM } from 'jsdom';
import { resolve } from 'path';

/**
 * Calculate relative luminance of a color
 * @param {number} r - Red component (0-255)
 * @param {number} g - Green component (0-255)
 * @param {number} b - Blue component (0-255)
 * @returns {number} Relative luminance (0-1)
 */
function getLuminance(r, g, b) {
  const [rs, gs, bs] = [r, g, b].map(c => {
    c = c / 255;
    return c <= 0.03928 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4);
  });
  return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
}

/**
 * Calculate contrast ratio between two colors
 * @param {string} color1 - Hex color (e.g., '#ffffff')
 * @param {string} color2 - Hex color (e.g., '#000000')
 * @returns {number} Contrast ratio (1-21)
 */
function getContrastRatio(color1, color2) {
  const hexToRgb = (hex) => {
    const result = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex);
    return result ? {
      r: parseInt(result[1], 16),
      g: parseInt(result[2], 16),
      b: parseInt(result[3], 16)
    } : null;
  };

  const rgb1 = hexToRgb(color1);
  const rgb2 = hexToRgb(color2);

  if (!rgb1 || !rgb2) return 1;

  const lum1 = getLuminance(rgb1.r, rgb1.g, rgb1.b);
  const lum2 = getLuminance(rgb2.r, rgb2.g, rgb2.b);

  const lighter = Math.max(lum1, lum2);
  const darker = Math.min(lum1, lum2);

  return (lighter + 0.05) / (darker + 0.05);
}

// Tailwind color palette values
const tailwindColors = {
  'gray-50': '#f9fafb',
  'gray-100': '#f3f4f6',
  'gray-200': '#e5e7eb',
  'gray-300': '#d1d5db',
  'gray-400': '#9ca3af',
  'gray-500': '#6b7280',
  'gray-600': '#4b5563',
  'gray-700': '#374151',
  'gray-800': '#1f2937',
  'gray-900': '#111827',
  'white': '#ffffff',
  'black': '#000000',
  'mirdb-primary': '#4F46E5',
  'mirdb-secondary': '#6366F1',
  'mirdb-accent': '#818CF8',
  'purple-400': '#a78bfa',
  'cyan-400': '#22d3ee',
  'green-400': '#4ade80',
  'green-500': '#22c55e',
  'green-600': '#16a34a',
  'green-700': '#15803d',
  'blue-500': '#3b82f6',
  'blue-700': '#1d4ed8',
  'yellow-500': '#eab308',
  'orange-400': '#fb923c',
  'red-500': '#ef4444'
};

// WCAG 2.1 AA minimum contrast ratios
const WCAG_AA_NORMAL_TEXT = 4.5;
const WCAG_AA_LARGE_TEXT = 3.0;

// Screen Reader Accessibility Tests (Scenario 13/14)
describe('Accessibility - Screen Reader', () => {
  let document;

  beforeAll(() => {
    const htmlPath = resolve(process.cwd(), 'src/index.html');
    const html = readFileSync(htmlPath, 'utf-8');
    const dom = new JSDOM(html);
    document = dom.window.document;
  });

  describe('TC1: Semantic HTML Landmarks', () => {
    it('should have a header element', () => {
      const header = document.querySelector('header');
      expect(header).not.toBeNull();
    });

    it('should have a main element', () => {
      const main = document.querySelector('main');
      expect(main).not.toBeNull();
    });

    it('should have a nav element', () => {
      const nav = document.querySelector('nav');
      expect(nav).not.toBeNull();
    });

    it('should have section elements', () => {
      const sections = document.querySelectorAll('section');
      expect(sections.length).toBeGreaterThan(0);
    });

    it('should have a footer element', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();
    });

    it('should have document language set', () => {
      const htmlElement = document.querySelector('html');
      expect(htmlElement.getAttribute('lang')).toBe('en');
    });
  });

  describe('TC2: Image Alt Text', () => {
    it('should have alt text on all img elements', () => {
      const images = document.querySelectorAll('img');
      expect(images.length).toBeGreaterThan(0);

      images.forEach((img, index) => {
        const alt = img.getAttribute('alt');
        expect(alt, `Image ${index + 1} missing alt attribute`).not.toBeNull();
        expect(alt.trim().length, `Image ${index + 1} has empty alt text`).toBeGreaterThan(0);
      });
    });

    it('should have meaningful alt text (not generic)', () => {
      const images = document.querySelectorAll('img');
      const genericAltPatterns = ['image', 'photo', 'picture', 'img', 'untitled'];

      images.forEach((img) => {
        const alt = img.getAttribute('alt').toLowerCase();
        genericAltPatterns.forEach(pattern => {
          if (alt === pattern) {
            expect.fail(`Image has generic alt text: "${alt}"`);
          }
        });
      });
    });
  });

  describe('TC3: Heading Hierarchy', () => {
    it('should have exactly one h1 element', () => {
      const h1s = document.querySelectorAll('h1');
      expect(h1s.length).toBe(1);
    });

    it('should follow logical heading hierarchy without skipping levels', () => {
      const headings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      let currentLevel = 0;

      headings.forEach((heading) => {
        const level = parseInt(heading.tagName.charAt(1));

        if (currentLevel === 0) {
          expect(level, 'First heading should be h1').toBe(1);
        } else {
          const levelDiff = level - currentLevel;
          expect(levelDiff, `Heading hierarchy skips levels: h${currentLevel} to h${level}`).toBeLessThanOrEqual(1);
        }

        currentLevel = level;
      });
    });

    it('should have h2 headings for major sections', () => {
      const h2s = document.querySelectorAll('h2');
      expect(h2s.length).toBeGreaterThan(0);
    });
  });

  describe('TC4: Code Blocks ARIA Labels', () => {
    it('should have aria-label on code blocks', () => {
      const allCodeRegions = document.querySelectorAll('[role="region"]');
      expect(allCodeRegions.length, 'Should have code blocks with role="region"').toBeGreaterThan(0);

      allCodeRegions.forEach((block) => {
        const ariaLabel = block.getAttribute('aria-label');
        expect(ariaLabel, 'Code block should have aria-label').not.toBeNull();
        expect(ariaLabel.trim().length, 'Code block aria-label should not be empty').toBeGreaterThan(0);
      });
    });

    it('should have architecture diagram with appropriate label', () => {
      const archDiagram = document.querySelector('.architecture-diagram, [role="img"]');
      if (archDiagram) {
        const ariaLabel = archDiagram.getAttribute('aria-label');
        expect(ariaLabel, 'Architecture diagram should have aria-label').not.toBeNull();
      }
    });
  });

  describe('TC5: Descriptive Link Text', () => {
    it('should not have generic link text like "click here"', () => {
      const links = document.querySelectorAll('a');
      const genericTexts = ['click here', 'click', 'here', 'read more', 'more', 'link'];

      links.forEach((link) => {
        const linkText = link.textContent.trim().toLowerCase();
        genericTexts.forEach(generic => {
          if (linkText === generic) {
            expect.fail(`Link has generic text: "${linkText}"`);
          }
        });
      });
    });

    it('should have descriptive text or aria-label for all links', () => {
      const links = document.querySelectorAll('a');

      links.forEach((link, index) => {
        const linkText = link.textContent.trim();
        const ariaLabel = link.getAttribute('aria-label');
        const hasDescriptiveContent = linkText.length > 0 || (ariaLabel && ariaLabel.length > 0);
        expect(hasDescriptiveContent, `Link ${index + 1} lacks descriptive text or aria-label`).toBe(true);
      });
    });
  });

  describe('Additional Screen Reader Accessibility', () => {
    it('should have skip link or proper landmark navigation', () => {
      const skipLink = document.querySelector('a[href="#main"], a[href="#content"], .skip-link');
      const main = document.querySelector('main');
      const nav = document.querySelector('nav');

      expect(main || skipLink, 'Should have main landmark or skip link').toBeTruthy();
      expect(nav, 'Should have nav landmark').not.toBeNull();
    });

    it('should have meta description', () => {
      const metaDesc = document.querySelector('meta[name="description"]');
      expect(metaDesc).not.toBeNull();
      expect(metaDesc.getAttribute('content').length).toBeGreaterThan(0);
    });

    it('should have viewport meta tag', () => {
      const viewport = document.querySelector('meta[name="viewport"]');
      expect(viewport).not.toBeNull();
    });

    it('interactive elements should be accessible', () => {
      const buttons = document.querySelectorAll('button');
      buttons.forEach((button, index) => {
        const hasName = button.textContent.trim().length > 0 ||
                       button.getAttribute('aria-label') ||
                       button.getAttribute('aria-labelledby') ||
                       button.getAttribute('title');
        expect(hasName, `Button ${index + 1} lacks accessible name`).toBeTruthy();
      });
    });
  });
});

// Color Contrast Tests (Scenario 15)
describe('Accessibility - Color Contrast (WCAG 2.1 AA)', () => {
  let document;

  beforeAll(() => {
    const htmlPath = resolve(process.cwd(), 'src/index.html');
    const html = readFileSync(htmlPath, 'utf-8');
    const dom = new JSDOM(html);
    document = dom.window.document;
  });

  describe('Test Case 1: Body text contrast ratio', () => {
    it('Body text (gray-900) on light background (gray-50) has at least 4.5:1 contrast', () => {
      const foreground = tailwindColors['gray-900'];
      const background = tailwindColors['gray-50'];
      const ratio = getContrastRatio(foreground, background);

      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
      console.log(`Body text contrast: ${ratio.toFixed(2)}:1`);
    });

    it('Feature card text (gray-600) on white background has at least 4.5:1 contrast', () => {
      const foreground = tailwindColors['gray-600'];
      const background = tailwindColors['white'];
      const ratio = getContrastRatio(foreground, background);

      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
      console.log(`Feature card text contrast: ${ratio.toFixed(2)}:1`);
    });

    it('Navigation links (gray-600) on white background have at least 4.5:1 contrast', () => {
      const foreground = tailwindColors['gray-600'];
      const background = tailwindColors['white'];
      const ratio = getContrastRatio(foreground, background);

      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
      console.log(`Navigation link contrast: ${ratio.toFixed(2)}:1`);
    });
  });

  describe('Test Case 2: Heading contrast ratio (large text)', () => {
    it('Headings (gray-900) on light background have at least 3:1 contrast', () => {
      const foreground = tailwindColors['gray-900'];
      const background = tailwindColors['gray-50'];
      const ratio = getContrastRatio(foreground, background);

      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_LARGE_TEXT);
      console.log(`Heading contrast on light bg: ${ratio.toFixed(2)}:1`);
    });

    it('Hero heading (white) on dark background has at least 3:1 contrast', () => {
      const foreground = tailwindColors['white'];
      const background = tailwindColors['gray-900'];
      const ratio = getContrastRatio(foreground, background);

      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_LARGE_TEXT);
      console.log(`Hero heading contrast: ${ratio.toFixed(2)}:1`);
    });

    it('Section headings exist and are properly structured', () => {
      const h2Headings = document.querySelectorAll('h2');
      expect(h2Headings.length).toBeGreaterThan(0);

      const sections = ['features', 'quick-start', 'architecture', 'comparison', 'commands'];
      sections.forEach(sectionId => {
        const section = document.querySelector(`#${sectionId}`);
        if (section) {
          const heading = section.querySelector('h2');
          expect(heading).not.toBeNull();
        }
      });
    });
  });

  describe('Test Case 3: Link contrast', () => {
    it('Links are distinguishable with sufficient contrast', () => {
      const foreground = tailwindColors['mirdb-primary'];
      const background = tailwindColors['white'];
      const ratio = getContrastRatio(foreground, background);

      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
      console.log(`Primary link contrast: ${ratio.toFixed(2)}:1`);
    });

    it('Footer links (gray-300) on dark background have at least 4.5:1 contrast', () => {
      const foreground = tailwindColors['gray-300'];
      const background = tailwindColors['gray-900'];
      const ratio = getContrastRatio(foreground, background);

      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
      console.log(`Footer link contrast: ${ratio.toFixed(2)}:1`);
    });

    it('Hero secondary text (gray-300) on dark background has at least 4.5:1 contrast', () => {
      const foreground = tailwindColors['gray-300'];
      const background = tailwindColors['gray-900'];
      const ratio = getContrastRatio(foreground, background);

      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
      console.log(`Hero secondary text contrast: ${ratio.toFixed(2)}:1`);
    });

    it('All navigation links exist and are accessible', () => {
      const navLinks = document.querySelectorAll('nav a');
      expect(navLinks.length).toBeGreaterThan(0);

      navLinks.forEach(link => {
        const href = link.getAttribute('href');
        expect(href).toBeTruthy();
      });
    });
  });

  describe('Test Case 4: Code block contrast', () => {
    it('Code text (gray-100) on dark background (gray-900) has at least 4.5:1 contrast', () => {
      const foreground = tailwindColors['gray-100'];
      const background = tailwindColors['gray-900'];
      const ratio = getContrastRatio(foreground, background);

      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
      console.log(`Code block text contrast: ${ratio.toFixed(2)}:1`);
    });

    it('Syntax highlighting - keyword (purple-400) on dark background has at least 4.5:1 contrast', () => {
      const foreground = tailwindColors['purple-400'];
      const background = tailwindColors['gray-900'];
      const ratio = getContrastRatio(foreground, background);

      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
      console.log(`Keyword syntax contrast: ${ratio.toFixed(2)}:1`);
    });

    it('Syntax highlighting - command (cyan-400) on dark background has at least 4.5:1 contrast', () => {
      const foreground = tailwindColors['cyan-400'];
      const background = tailwindColors['gray-900'];
      const ratio = getContrastRatio(foreground, background);

      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
      console.log(`Command syntax contrast: ${ratio.toFixed(2)}:1`);
    });

    it('Syntax highlighting - string (green-400) on dark background has at least 4.5:1 contrast', () => {
      const foreground = tailwindColors['green-400'];
      const background = tailwindColors['gray-900'];
      const ratio = getContrastRatio(foreground, background);

      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
      console.log(`String syntax contrast: ${ratio.toFixed(2)}:1`);
    });

    it('Syntax highlighting - number (orange-400) on dark background has at least 4.5:1 contrast', () => {
      const foreground = tailwindColors['orange-400'];
      const background = tailwindColors['gray-900'];
      const ratio = getContrastRatio(foreground, background);

      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
      console.log(`Number syntax contrast: ${ratio.toFixed(2)}:1`);
    });

    it('Code blocks have proper ARIA labels', () => {
      const codeBlocks = document.querySelectorAll('.code-block[role="region"]');
      expect(codeBlocks.length).toBeGreaterThan(0);

      codeBlocks.forEach(block => {
        const ariaLabel = block.getAttribute('aria-label');
        expect(ariaLabel).toBeTruthy();
      });
    });
  });

  describe('Additional Contrast Validations', () => {
    it('Document has lang attribute set to en', () => {
      const htmlElement = document.querySelector('html');
      expect(htmlElement.getAttribute('lang')).toBe('en');
    });

    it('CTA buttons have sufficient contrast', () => {
      const foreground = tailwindColors['white'];
      const background = tailwindColors['mirdb-primary'];
      const ratio = getContrastRatio(foreground, background);

      expect(ratio).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
      console.log(`CTA button contrast: ${ratio.toFixed(2)}:1`);
    });

    it('Comparison table status indicators have sufficient contrast', () => {
      // Per WCAG 1.4.11 (Non-text Contrast), graphical objects need 3:1 contrast
      const greenFg = tailwindColors['green-600'];
      const whiteBg = tailwindColors['white'];
      const greenRatio = getContrastRatio(greenFg, whiteBg);

      expect(greenRatio).toBeGreaterThanOrEqual(WCAG_AA_LARGE_TEXT);
      console.log(`Green indicator contrast: ${greenRatio.toFixed(2)}:1 (UI component, 3:1 required)`);

      const redFg = tailwindColors['red-500'];
      const redRatio = getContrastRatio(redFg, whiteBg);

      expect(redRatio).toBeGreaterThanOrEqual(WCAG_AA_LARGE_TEXT);
      console.log(`Red indicator contrast: ${redRatio.toFixed(2)}:1 (UI component, 3:1 required)`);
    });
  });
});
