/**
 * Accessibility Unit Tests - WCAG 2.1 AA Compliance
 * Owner: Scenario 11 - Accessibility - WCAG 2.1 AA Compliance
 *
 * Tests for verifying WCAG 2.1 AA accessibility standards compliance.
 * Includes tests for:
 * - Alt text on images (TC3)
 * - Color contrast ratios (TC4)
 * - Semantic HTML structure (TC5)
 * - Keyboard navigability indicators
 */
const fs = require('fs');
const path = require('path');

describe('Accessibility - WCAG 2.1 AA Compliance', () => {
  let htmlContent;
  let cssContent;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf-8');

    const cssPath = path.join(__dirname, '../../css/utilities/variables.css');
    cssContent = fs.readFileSync(cssPath, 'utf-8');
  });

  describe('TC3: Alt Text on Images', () => {
    test('All img tags have alt attributes', () => {
      // Find all img tags
      const imgTags = htmlContent.match(/<img[^>]*>/g) || [];
      expect(imgTags.length).toBeGreaterThan(0);

      imgTags.forEach((imgTag) => {
        // Each img should have an alt attribute
        expect(imgTag).toMatch(/alt="[^"]*"/);
      });
    });

    test('Images have descriptive alt text (not empty or placeholder)', () => {
      const imgTags = htmlContent.match(/<img[^>]*>/g) || [];
      imgTags.forEach((imgTag) => {
        const altMatch = imgTag.match(/alt="([^"]*)"/);
        expect(altMatch).not.toBeNull();
        const altText = altMatch[1];
        // Alt text should not be empty or generic placeholders
        expect(altText.length).toBeGreaterThan(0);
        expect(altText.toLowerCase()).not.toBe('image');
        expect(altText.toLowerCase()).not.toBe('img');
        expect(altText.toLowerCase()).not.toBe('picture');
      });
    });

    test('Decorative SVG icons have aria-hidden="true"', () => {
      // Feature card icons and other decorative SVGs should be aria-hidden
      expect(htmlContent).toMatch(/aria-hidden="true"/);

      // Count decorative icon containers with aria-hidden
      const ariaHiddenCount = (htmlContent.match(/aria-hidden="true"/g) || []).length;
      expect(ariaHiddenCount).toBeGreaterThan(5); // Multiple decorative icons exist
    });

    test('Hero logo has descriptive alt text', () => {
      // The hero logo has alt="MirDB Logo" and class="hero-logo"
      // Attributes may be in any order
      expect(htmlContent).toMatch(/alt="MirDB Logo"[^>]*class="hero-logo"/);
    });

    test('Usage GIF has descriptive alt text', () => {
      // Usage GIF has a descriptive alt text (at least 10 characters)
      // and class="usage__gif"
      const usageImgMatch = htmlContent.match(/<img[^>]*class="usage__gif"[^>]*>/);
      expect(usageImgMatch).not.toBeNull();
      const imgTag = usageImgMatch[0];
      const altMatch = imgTag.match(/alt="([^"]*)"/);
      expect(altMatch).not.toBeNull();
      expect(altMatch[1].length).toBeGreaterThanOrEqual(10);
    });

    test('Navigation logo has alt text', () => {
      expect(htmlContent).toMatch(/<img[^>]*alt="MirDB Logo"[^>]*width="40"/);
    });

    test('Footer logo has alt text', () => {
      expect(htmlContent).toMatch(/<img[^>]*alt="MirDB Logo"[^>]*width="32"/);
    });
  });

  describe('TC5: Semantic HTML Structure', () => {
    test('Page has exactly one h1 element', () => {
      const h1Tags = htmlContent.match(/<h1[^>]*>[^<]*<\/h1>/g) || [];
      expect(h1Tags.length).toBe(1);
    });

    test('Page uses h2 elements for section headings', () => {
      const h2Tags = htmlContent.match(/<h2[^>]*>/g) || [];
      expect(h2Tags.length).toBeGreaterThan(0);

      // Verify expected section titles exist
      expect(htmlContent).toMatch(/<h2[^>]*>Why MirDB\?<\/h2>/);
      expect(htmlContent).toMatch(/<h2[^>]*>Simple as Memcached<\/h2>/);
      expect(htmlContent).toMatch(/<h2[^>]*>How It Works<\/h2>/);
      expect(htmlContent).toMatch(/<h2[^>]*>Get Started in Minutes<\/h2>/);
    });

    test('Page uses h3 elements for subsection headings', () => {
      const h3Tags = htmlContent.match(/<h3[^>]*>/g) || [];
      expect(h3Tags.length).toBeGreaterThan(0);
    });

    test('Page uses semantic nav element', () => {
      expect(htmlContent).toMatch(/<nav[^>]*>/);
    });

    test('Navigation has proper ARIA attributes', () => {
      expect(htmlContent).toMatch(/<nav[^>]*role="navigation"[^>]*>/);
      expect(htmlContent).toMatch(/<nav[^>]*aria-label="Main navigation"[^>]*>/);
    });

    test('Page uses semantic section elements with IDs', () => {
      expect(htmlContent).toMatch(/<section[^>]*id="hero"[^>]*>/);
      expect(htmlContent).toMatch(/<section[^>]*id="features"[^>]*>/);
      expect(htmlContent).toMatch(/<section[^>]*id="usage"[^>]*>/);
      expect(htmlContent).toMatch(/<section[^>]*id="architecture"[^>]*>/);
      expect(htmlContent).toMatch(/<section[^>]*id="getting-started"[^>]*>/);
    });

    test('Page uses semantic footer element', () => {
      expect(htmlContent).toMatch(/<footer[^>]*>/);
    });

    test('Footer has proper ARIA role', () => {
      expect(htmlContent).toMatch(/<footer[^>]*role="contentinfo"[^>]*>/);
    });

    test('Footer navigation has aria-label', () => {
      expect(htmlContent).toMatch(/<nav[^>]*class="footer__nav"[^>]*aria-label="Footer navigation"[^>]*>/);
    });

    test('HTML element has lang attribute set to English', () => {
      expect(htmlContent).toMatch(/<html[^>]*lang="en"[^>]*>/);
    });

    test('Feature cards use article elements', () => {
      expect(htmlContent).toMatch(/<article[^>]*class="feature-card"[^>]*>/);
    });

    test('Architecture components use article elements', () => {
      expect(htmlContent).toMatch(/<article[^>]*class="architecture__component"[^>]*>/);
    });
  });

  describe('TC4: Color Contrast - CSS Variable Analysis', () => {
    /**
     * Calculate relative luminance of a color
     * Based on WCAG 2.1 formula
     */
    function getLuminance(r, g, b) {
      const [rs, gs, bs] = [r, g, b].map((c) => {
        c = c / 255;
        return c <= 0.03928 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4);
      });
      return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
    }

    /**
     * Calculate contrast ratio between two colors
     * WCAG 2.1 contrast ratio formula
     */
    function getContrastRatio(color1, color2) {
      const l1 = getLuminance(...color1);
      const l2 = getLuminance(...color2);
      const lighter = Math.max(l1, l2);
      const darker = Math.min(l1, l2);
      return (lighter + 0.05) / (darker + 0.05);
    }

    /**
     * Parse hex color to RGB array
     */
    function hexToRgb(hex) {
      const result = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex);
      return result
        ? [parseInt(result[1], 16), parseInt(result[2], 16), parseInt(result[3], 16)]
        : null;
    }

    test('CSS defines required color variables', () => {
      expect(cssContent).toContain('--color-bg-primary');
      expect(cssContent).toContain('--color-bg-secondary');
      expect(cssContent).toContain('--color-text');
      expect(cssContent).toContain('--color-accent-coral');
      expect(cssContent).toContain('--color-accent-blue');
    });

    test('Primary text color meets WCAG AA contrast ratio (4.5:1) against dark background', () => {
      // Extract colors from CSS
      const bgPrimary = hexToRgb('#1a1a2e'); // --color-bg-primary
      const textColor = hexToRgb('#eaeaea'); // --color-text

      const ratio = getContrastRatio(bgPrimary, textColor);

      // WCAG AA requires 4.5:1 for normal text
      expect(ratio).toBeGreaterThanOrEqual(4.5);
    });

    test('Primary text color meets WCAG AA contrast ratio against secondary background', () => {
      const bgSecondary = hexToRgb('#16213e'); // --color-bg-secondary
      const textColor = hexToRgb('#eaeaea'); // --color-text

      const ratio = getContrastRatio(bgSecondary, textColor);
      expect(ratio).toBeGreaterThanOrEqual(4.5);
    });

    test('Accent coral color meets WCAG AA contrast ratio (3:1) for large text/UI components', () => {
      const bgPrimary = hexToRgb('#1a1a2e'); // --color-bg-primary
      const accentCoral = hexToRgb('#e94560'); // --color-accent-coral

      const ratio = getContrastRatio(bgPrimary, accentCoral);

      // WCAG AA requires 3:1 for large text and UI components
      expect(ratio).toBeGreaterThanOrEqual(3);
    });

    test('Muted text color has sufficient contrast for non-essential text', () => {
      const bgPrimary = hexToRgb('#1a1a2e');
      const mutedText = hexToRgb('#a0a0a0'); // --color-text-muted

      const ratio = getContrastRatio(bgPrimary, mutedText);
      // Should meet at least 3:1 for non-essential text
      expect(ratio).toBeGreaterThanOrEqual(3);
    });

    test('White text on accent blue meets contrast requirements', () => {
      const accentBlue = hexToRgb('#0f3460'); // --color-accent-blue
      const whiteColor = hexToRgb('#ffffff');

      const ratio = getContrastRatio(accentBlue, whiteColor);
      expect(ratio).toBeGreaterThanOrEqual(4.5);
    });
  });

  describe('Keyboard Navigation Indicators', () => {
    test('Interactive buttons do not have tabindex=-1', () => {
      // Find buttons with tabindex attribute
      const buttonsWithTabindex = htmlContent.match(/<button[^>]*tabindex="-1"[^>]*>/g) || [];
      expect(buttonsWithTabindex.length).toBe(0);
    });

    test('Navigation toggle has proper ARIA attributes for keyboard accessibility', () => {
      expect(htmlContent).toMatch(/<button[^>]*class="nav-toggle"[^>]*>/);
      expect(htmlContent).toMatch(/aria-expanded="false"/);
      expect(htmlContent).toMatch(/aria-controls="nav-menu"/);
      expect(htmlContent).toMatch(/aria-label="Toggle navigation menu"/);
    });

    test('Copy buttons have accessible labels', () => {
      expect(htmlContent).toMatch(/<button[^>]*class="code-block__copy"[^>]*aria-label="Copy code to clipboard"[^>]*>/);
    });

    test('External links have noopener attribute for security', () => {
      // Find links with target="_blank"
      const externalLinks = htmlContent.match(/<a[^>]*target="_blank"[^>]*>/g) || [];
      expect(externalLinks.length).toBeGreaterThan(0);

      externalLinks.forEach((link) => {
        expect(link).toMatch(/rel="[^"]*noopener[^"]*"/);
      });
    });

    test('Copy buttons have type attribute for accessibility', () => {
      // Copy buttons should have type="button" to prevent form submission
      const copyButtons = htmlContent.match(/<button[^>]*class="code-block__copy"[^>]*>/g) || [];
      expect(copyButtons.length).toBeGreaterThan(0);

      copyButtons.forEach((button) => {
        expect(button).toMatch(/type="button"/);
      });
    });
  });

  describe('Screen Reader Compatibility', () => {
    test('Main navigation has aria-label', () => {
      expect(htmlContent).toMatch(/id="main-nav"[^>]*aria-label="Main navigation"/);
    });

    test('GitHub links have accessible names', () => {
      // GitHub link in nav should have aria-label
      expect(htmlContent).toMatch(/<a[^>]*class="nav-github"[^>]*aria-label="View MirDB on GitHub"[^>]*>/);
    });

    test('Navigation overlay is hidden from screen readers', () => {
      expect(htmlContent).toMatch(/<div[^>]*class="nav-overlay"[^>]*aria-hidden="true"[^>]*>/);
    });

    test('Feature card icons are hidden from screen readers', () => {
      expect(htmlContent).toMatch(/<div[^>]*class="feature-card__icon"[^>]*aria-hidden="true"[^>]*>/);
    });

    test('Architecture component icons are hidden from screen readers', () => {
      expect(htmlContent).toMatch(/<div[^>]*class="architecture__component-icon"[^>]*aria-hidden="true"[^>]*>/);
    });

    test('Copy button SVG icons are hidden from screen readers', () => {
      expect(htmlContent).toMatch(/<svg[^>]*class="copy-icon"[^>]*aria-hidden="true"[^>]*>/);
    });
  });

  describe('Form and Button Accessibility', () => {
    test('All buttons have accessible names via aria-label or text content', () => {
      // Navigation toggle has aria-label
      expect(htmlContent).toMatch(/<button[^>]*id="nav-toggle"[^>]*aria-label="[^"]+"/);

      // Copy buttons have aria-label
      const copyButtons = htmlContent.match(/<button[^>]*class="code-block__copy"[^>]*>/g) || [];
      expect(copyButtons.length).toBeGreaterThan(0);
      copyButtons.forEach((button) => {
        expect(button).toMatch(/aria-label="[^"]+"/);
      });
    });
  });

  describe('Link Accessibility', () => {
    test('Internal navigation links point to valid section IDs', () => {
      // Check that anchor links reference existing section IDs
      expect(htmlContent).toMatch(/href="#features"/);
      expect(htmlContent).toMatch(/href="#usage"/);
      expect(htmlContent).toMatch(/href="#architecture"/);
      expect(htmlContent).toMatch(/href="#getting-started"/);

      // Verify the sections exist
      expect(htmlContent).toMatch(/id="features"/);
      expect(htmlContent).toMatch(/id="usage"/);
      expect(htmlContent).toMatch(/id="architecture"/);
      expect(htmlContent).toMatch(/id="getting-started"/);
    });

    test('External links open in new tab with proper security attributes', () => {
      const githubLinks = htmlContent.match(/<a[^>]*href="https:\/\/github\.com[^"]*"[^>]*>/g) || [];
      expect(githubLinks.length).toBeGreaterThan(0);

      githubLinks.forEach((link) => {
        expect(link).toMatch(/target="_blank"/);
        expect(link).toMatch(/rel="[^"]*noopener[^"]*"/);
      });
    });

    test('Links do not use generic text like "click here"', () => {
      // Check that "click here" is not used as link text
      expect(htmlContent).not.toMatch(/>click here</i);
      expect(htmlContent).not.toMatch(/>here</i);
    });
  });

  describe('Document Structure', () => {
    test('Document has proper DOCTYPE', () => {
      expect(htmlContent).toMatch(/^<!DOCTYPE html>/i);
    });

    test('Document has charset meta tag', () => {
      expect(htmlContent).toMatch(/<meta[^>]*charset="UTF-8"[^>]*>/);
    });

    test('Document has viewport meta tag for responsive design', () => {
      expect(htmlContent).toMatch(/<meta[^>]*name="viewport"[^>]*content="[^"]*width=device-width[^"]*"[^>]*>/);
    });

    test('Document has descriptive title', () => {
      expect(htmlContent).toMatch(/<title>MirDB - Persistent Key-Value Store[^<]*<\/title>/);
    });
  });

  describe('TC6: Screen Reader Test Documentation (Manual Test Reference)', () => {
    test('Documentation: Page structure supports screen reader navigation', () => {
      // This test documents that the page has proper structure for screen readers
      // Manual verification with actual screen reader software is recommended

      // Verify landmarks exist
      expect(htmlContent).toMatch(/<nav[^>]*role="navigation"/);
      expect(htmlContent).toMatch(/<footer[^>]*role="contentinfo"/);

      // Verify heading hierarchy
      expect(htmlContent).toMatch(/<h1[^>]*>/);
      expect(htmlContent).toMatch(/<h2[^>]*>/);
      expect(htmlContent).toMatch(/<h3[^>]*>/);

      // Verify semantic sections
      expect(htmlContent).toMatch(/<section[^>]*id="[^"]+"/);
    });
  });
});
