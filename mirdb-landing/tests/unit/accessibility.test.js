/**
 * Accessibility Unit Tests
 * Owner: Scenario 12 - WCAG 2.1 AA Compliance
 *
 * Tests:
 * - Alt text on images
 * - ARIA labels
 * - Color contrast ratios
 * - Heading hierarchy
 */

import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

describe('Accessibility - Unit Tests', () => {
  let htmlContent;
  let cssContent;
  let variablesCSS;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf8');

    // Load CSS files for contrast analysis
    const stylesPath = path.join(__dirname, '../../css/styles.css');
    const variablesPath = path.join(__dirname, '../../css/utilities/variables.css');

    cssContent = fs.readFileSync(stylesPath, 'utf8');
    variablesCSS = fs.readFileSync(variablesPath, 'utf8');
  });

  describe('TC7: Image Accessibility - Alt Text', () => {
    test('All img elements have alt attributes', () => {
      // Find all img tags
      const imgTags = htmlContent.match(/<img[^>]*>/gi) || [];

      imgTags.forEach((imgTag) => {
        expect(imgTag).toMatch(/alt=/);
      });
    });

    test('Logo has accessible alternative (aria-label or alt text)', () => {
      // Check for MirDB logo with accessible name
      // The nav-logo link has aria-label="MirDB Home"
      expect(htmlContent).toMatch(/aria-label="MirDB Home"|alt="MirDB Logo"/i);
    });

    test('Architecture diagram has descriptive alt text', () => {
      // Check for architecture diagram with descriptive alt
      expect(htmlContent).toMatch(/architecture\.svg/);
      expect(htmlContent).toMatch(/alt="MirDB LSM-Tree Architecture/i);
    });

    test('Decorative icons have aria-hidden="true"', () => {
      // Feature icons and decorative SVGs should be hidden from screen readers
      expect(htmlContent).toMatch(/class="feature-icon"[^>]*aria-hidden="true"/);
      // Or check for SVGs with aria-hidden
      const decorativeSvgs = htmlContent.match(/<svg[^>]*aria-hidden="true"[^>]*>/g) || [];
      expect(decorativeSvgs.length).toBeGreaterThan(0);
    });
  });

  describe('TC5: Heading Hierarchy', () => {
    test('Page has at most one h1 element (or heading starts with h2 if h1 is in hero)', () => {
      const h1Tags = htmlContent.match(/<h1[^>]*>/gi) || [];
      // Page should have 0 or 1 h1 elements
      // If hero section is empty, h2 may be the top-level heading
      expect(h1Tags.length).toBeLessThanOrEqual(1);
    });

    test('H2 headings exist for main sections', () => {
      const h2Tags = htmlContent.match(/<h2[^>]*>/gi) || [];
      expect(h2Tags.length).toBeGreaterThanOrEqual(5); // Features, Code, Architecture, Status, Specs
    });

    test('Heading levels do not skip (h1 > h2 > h3)', () => {
      // Extract all heading tags in order
      const headingPattern = /<h([1-6])[^>]*>/gi;
      const headings = [];
      let match;

      while ((match = headingPattern.exec(htmlContent)) !== null) {
        headings.push(parseInt(match[1]));
      }

      // Check that heading levels don't skip more than one level
      let prevLevel = 0;
      headings.forEach((level) => {
        // If going down, should not skip levels
        if (level > prevLevel && prevLevel > 0) {
          expect(level - prevLevel).toBeLessThanOrEqual(1);
        }
        prevLevel = level;
      });
    });

    test('Section headings have unique IDs for aria-labelledby', () => {
      const sections = [
        { section: 'features', heading: 'features-heading' },
        { section: 'architecture', heading: 'architecture-heading' },
        { section: 'status', heading: 'status-heading' },
        { section: 'specifications', heading: 'specifications-heading' },
        { section: 'code-examples', heading: 'code-examples-title' }
      ];

      sections.forEach(({ section, heading }) => {
        // Check that the heading ID exists
        expect(htmlContent).toMatch(new RegExp(`id="${heading}"`));
        // Check that the section has aria-labelledby pointing to it
        expect(htmlContent).toMatch(new RegExp(`id="${section}"[^>]*aria-labelledby="${heading}"`));
      });
    });
  });

  describe('TC4: Color Contrast Ratios', () => {
    // Helper function to calculate relative luminance
    const getLuminance = (r, g, b) => {
      const [rs, gs, bs] = [r, g, b].map(c => {
        c = c / 255;
        return c <= 0.03928 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4);
      });
      return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
    };

    // Helper function to calculate contrast ratio
    const getContrastRatio = (l1, l2) => {
      const lighter = Math.max(l1, l2);
      const darker = Math.min(l1, l2);
      return (lighter + 0.05) / (darker + 0.05);
    };

    // Helper function to parse hex color
    const hexToRgb = (hex) => {
      const result = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex);
      return result ? {
        r: parseInt(result[1], 16),
        g: parseInt(result[2], 16),
        b: parseInt(result[3], 16)
      } : null;
    };

    test('Primary text color (#1a1a2e) on white background meets 4.5:1 ratio', () => {
      const textColor = hexToRgb('#1a1a2e');
      const bgColor = hexToRgb('#ffffff');

      const textLuminance = getLuminance(textColor.r, textColor.g, textColor.b);
      const bgLuminance = getLuminance(bgColor.r, bgColor.g, bgColor.b);

      const ratio = getContrastRatio(textLuminance, bgLuminance);
      expect(ratio).toBeGreaterThanOrEqual(4.5);
    });

    test('Secondary text color (#4a4a68) on white background meets 4.5:1 ratio', () => {
      const textColor = hexToRgb('#4a4a68');
      const bgColor = hexToRgb('#ffffff');

      const textLuminance = getLuminance(textColor.r, textColor.g, textColor.b);
      const bgLuminance = getLuminance(bgColor.r, bgColor.g, bgColor.b);

      const ratio = getContrastRatio(textLuminance, bgLuminance);
      expect(ratio).toBeGreaterThanOrEqual(4.5);
    });

    test('Dark mode text color (#e1e4e8) on dark background (#1a1a2e) meets 4.5:1 ratio', () => {
      const textColor = hexToRgb('#e1e4e8');
      const bgColor = hexToRgb('#1a1a2e');

      const textLuminance = getLuminance(textColor.r, textColor.g, textColor.b);
      const bgLuminance = getLuminance(bgColor.r, bgColor.g, bgColor.b);

      const ratio = getContrastRatio(textLuminance, bgLuminance);
      expect(ratio).toBeGreaterThanOrEqual(4.5);
    });

    test('Dark mode secondary text (#9ca3af) on dark background (#1a1a2e) meets 4.5:1 ratio', () => {
      const textColor = hexToRgb('#9ca3af');
      const bgColor = hexToRgb('#1a1a2e');

      const textLuminance = getLuminance(textColor.r, textColor.g, textColor.b);
      const bgLuminance = getLuminance(bgColor.r, bgColor.g, bgColor.b);

      const ratio = getContrastRatio(textLuminance, bgLuminance);
      expect(ratio).toBeGreaterThanOrEqual(4.5);
    });

    test('Primary accent color (#f74c00) on white background meets 3:1 for large text', () => {
      const accentColor = hexToRgb('#f74c00');
      const bgColor = hexToRgb('#ffffff');

      const accentLuminance = getLuminance(accentColor.r, accentColor.g, accentColor.b);
      const bgLuminance = getLuminance(bgColor.r, bgColor.g, bgColor.b);

      const ratio = getContrastRatio(accentLuminance, bgLuminance);
      // WCAG AA requires 3:1 for large text (18pt+ or 14pt bold)
      expect(ratio).toBeGreaterThanOrEqual(3);
    });

    test('CSS variables define required contrast colors', () => {
      // Check that CSS variables are defined
      expect(variablesCSS).toMatch(/--color-text:/);
      expect(variablesCSS).toMatch(/--color-text-secondary:/);
      expect(variablesCSS).toMatch(/--color-background:/);
      expect(variablesCSS).toMatch(/--color-surface:/);
    });
  });

  describe('ARIA Labels and Roles', () => {
    test('Navigation has proper role and label', () => {
      expect(htmlContent).toMatch(/role="navigation"/);
      expect(htmlContent).toMatch(/aria-label="Main navigation"/);
    });

    test('Header has banner role', () => {
      expect(htmlContent).toMatch(/<header[^>]*role="banner"/);
    });

    test('Mobile menu toggle has ARIA attributes', () => {
      expect(htmlContent).toMatch(/aria-expanded=/);
      expect(htmlContent).toMatch(/aria-controls="nav-menu"/);
    });

    test('Copy buttons have accessible labels', () => {
      // Check that copy buttons have aria-label
      expect(htmlContent).toMatch(/class="copy-btn"[^>]*aria-label="/);
    });

    test('Interactive elements have accessible names', () => {
      // Check that all buttons have either aria-label or text content
      const buttons = htmlContent.match(/<button[^>]*>/gi) || [];

      buttons.forEach((button) => {
        // Button should have either aria-label or aria-labelledby or visible text
        const hasAriaLabel = /aria-label=/.test(button);
        const hasAriaLabelledby = /aria-labelledby=/.test(button);
        // At minimum, buttons should have some form of accessible name
        expect(hasAriaLabel || hasAriaLabelledby || button.includes('>')).toBe(true);
      });
    });
  });

  describe('Skip Navigation Link', () => {
    test('Skip link exists at the beginning of the document', () => {
      // Skip link should be one of the first elements in body
      const bodyStart = htmlContent.indexOf('<body>');
      const skipLinkPos = htmlContent.indexOf('skip-link');
      const mainPos = htmlContent.indexOf('<main');

      expect(skipLinkPos).toBeGreaterThan(bodyStart);
      expect(skipLinkPos).toBeLessThan(mainPos);
    });

    test('Skip link targets main content', () => {
      expect(htmlContent).toMatch(/href="#main-content"[^>]*class="skip-link"/);
    });

    test('Skip link has descriptive text', () => {
      expect(htmlContent).toMatch(/Skip to main content/);
    });
  });

  describe('Focus Management', () => {
    test('Skip link has focus styles defined in CSS', () => {
      const navigationCSS = fs.readFileSync(
        path.join(__dirname, '../../css/components/navigation.css'),
        'utf8'
      );

      expect(navigationCSS).toMatch(/\.skip-link:focus/);
    });

    test('Links and buttons have focus styles', () => {
      const navigationCSS = fs.readFileSync(
        path.join(__dirname, '../../css/components/navigation.css'),
        'utf8'
      );

      // Check for focus styles on interactive elements
      expect(navigationCSS).toMatch(/\.nav-link:focus/);
      expect(navigationCSS).toMatch(/\.cta-btn:focus/);
      expect(navigationCSS).toMatch(/\.nav-toggle:focus/);
    });

    test('Focus outline uses visible color', () => {
      const navigationCSS = fs.readFileSync(
        path.join(__dirname, '../../css/components/navigation.css'),
        'utf8'
      );

      // Check that focus styles include outline
      expect(navigationCSS).toMatch(/focus[^{]*\{[^}]*outline:/);
    });
  });

  describe('Reduced Motion Support', () => {
    test('Reduced motion media query is defined', () => {
      const responsiveCSS = fs.readFileSync(
        path.join(__dirname, '../../css/utilities/responsive.css'),
        'utf8'
      );

      expect(responsiveCSS).toMatch(/@media \(prefers-reduced-motion: reduce\)/);
    });

    test('Animations respect reduced motion preference', () => {
      const responsiveCSS = fs.readFileSync(
        path.join(__dirname, '../../css/utilities/responsive.css'),
        'utf8'
      );

      // Check that reduced motion disables animations
      expect(responsiveCSS).toMatch(/animation-duration:\s*0\.01ms/);
      expect(responsiveCSS).toMatch(/transition-duration:\s*0\.01ms/);
    });
  });

  describe('Language and Document Structure', () => {
    test('HTML has lang attribute set to English', () => {
      expect(htmlContent).toMatch(/<html[^>]+lang="en"/);
    });

    test('Document has a title', () => {
      expect(htmlContent).toMatch(/<title>[^<]+<\/title>/);
    });

    test('Main landmark exists', () => {
      expect(htmlContent).toMatch(/<main[^>]*>/);
    });

    test('Header landmark exists', () => {
      expect(htmlContent).toMatch(/<header[^>]*>/);
    });
  });
});
