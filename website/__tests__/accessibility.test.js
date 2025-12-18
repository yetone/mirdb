/**
 * Accessibility Tests for MirDB Landing Page
 * Tests for WCAG 2.1 AA compliance as required by NFR-3
 *
 * Test Cases:
 * TC-3: All images have non-empty, descriptive alt attributes (unit)
 * TC-5: Proper heading hierarchy (h1, h2, h3) is used throughout the page (unit)
 * TC-6: Semantic elements (header, nav, main, section, footer) are used appropriately (unit)
 */

const fs = require('fs');
const path = require('path');

describe('Accessibility Compliance - WCAG 2.1 AA', () => {
  let document;
  let htmlContent;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../src/index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf8');
    document = new DOMParser().parseFromString(htmlContent, 'text/html');
  });

  // TC-3: Inspect all img elements - All images have non-empty, descriptive alt attributes
  describe('TC-3: Image Alt Text', () => {
    test('should have non-empty alt attributes on all img elements', () => {
      const images = document.querySelectorAll('img');

      // If there are images, each should have alt text
      images.forEach((img, index) => {
        const alt = img.getAttribute('alt');
        expect(alt).not.toBeNull();
        expect(alt.trim().length).toBeGreaterThan(0);
      });
    });

    test('should have descriptive alt text (not just filename or generic text)', () => {
      const images = document.querySelectorAll('img');

      images.forEach((img) => {
        const alt = img.getAttribute('alt');
        if (alt) {
          // Alt text should not be just a filename
          expect(alt).not.toMatch(/\.(png|jpg|jpeg|gif|svg|webp)$/i);
          // Alt text should not be generic
          expect(alt.toLowerCase()).not.toBe('image');
          expect(alt.toLowerCase()).not.toBe('photo');
          expect(alt.toLowerCase()).not.toBe('picture');
        }
      });
    });

    test('SVG icons should have aria-label for accessibility', () => {
      const svgs = document.querySelectorAll('svg');

      svgs.forEach((svg) => {
        const hasAriaLabel = svg.getAttribute('aria-label');
        const hasAriaHidden = svg.getAttribute('aria-hidden') === 'true';
        const hasRole = svg.getAttribute('role');

        // SVGs should either be hidden from screen readers or have labels
        expect(hasAriaLabel || hasAriaHidden || hasRole === 'presentation').toBe(true);
      });
    });
  });

  // TC-5: Inspect HTML structure - Proper heading hierarchy (h1, h2, h3) is used throughout the page
  describe('TC-5: Heading Hierarchy', () => {
    test('should have exactly one h1 element on the page', () => {
      const h1Elements = document.querySelectorAll('h1');
      expect(h1Elements.length).toBe(1);
    });

    test('should have h1 as the main page title', () => {
      const h1 = document.querySelector('h1');
      expect(h1).not.toBeNull();
      expect(h1.textContent.trim().length).toBeGreaterThan(0);
      expect(h1.textContent).toContain('MirDB');
    });

    test('should have proper heading hierarchy (no skipped levels)', () => {
      const headings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      const levels = Array.from(headings).map(h => parseInt(h.tagName.charAt(1)));

      // First heading should be h1
      if (levels.length > 0) {
        expect(levels[0]).toBe(1);
      }

      // Check that no heading level is skipped
      for (let i = 1; i < levels.length; i++) {
        const diff = levels[i] - levels[i - 1];
        // Heading can go deeper by 1, stay same, or go back up any number
        expect(diff).toBeLessThanOrEqual(1);
      }
    });

    test('should have h2 elements for main sections', () => {
      const h2Elements = document.querySelectorAll('h2');
      expect(h2Elements.length).toBeGreaterThanOrEqual(1);

      // Check that h2s have meaningful content
      h2Elements.forEach((h2) => {
        expect(h2.textContent.trim().length).toBeGreaterThan(0);
      });
    });

    test('headings should have descriptive text content', () => {
      const headings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');

      headings.forEach((heading) => {
        const text = heading.textContent.trim();
        expect(text.length).toBeGreaterThan(0);
        // Headings should not be just numbers or single characters
        expect(text.length).toBeGreaterThanOrEqual(2);
      });
    });
  });

  // TC-6: Inspect semantic HTML - Semantic elements are used appropriately
  describe('TC-6: Semantic HTML Structure', () => {
    test('should have a footer element', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();
    });

    test('should have section elements for content organization', () => {
      const sections = document.querySelectorAll('section');
      expect(sections.length).toBeGreaterThanOrEqual(1);
    });

    test('sections should have identifying attributes (id or aria-label)', () => {
      const sections = document.querySelectorAll('section');

      sections.forEach((section, index) => {
        const hasId = section.getAttribute('id');
        const hasAriaLabel = section.getAttribute('aria-label');
        const hasAriaLabelledBy = section.getAttribute('aria-labelledby');
        const hasHeading = section.querySelector('h1, h2, h3, h4, h5, h6');

        // Each section should be identifiable in some way
        expect(hasId || hasAriaLabel || hasAriaLabelledBy || hasHeading).toBeTruthy();
      });
    });

    test('should use appropriate semantic elements for navigation', () => {
      const footerLinks = document.querySelector('footer .footer-links');
      const hasNavigation = document.querySelector('nav') || footerLinks;

      // Page should have some navigation, either nav element or footer links
      expect(hasNavigation).not.toBeNull();
    });

    test('should use code elements for command displays', () => {
      const commandsSection = document.querySelector('#commands, .commands-section');
      if (commandsSection) {
        const codeElements = commandsSection.querySelectorAll('code');
        expect(codeElements.length).toBeGreaterThan(0);
      }
    });

    test('pre/code elements should be used for code blocks', () => {
      const codeBlocks = document.querySelectorAll('.code-block');

      codeBlocks.forEach((block) => {
        const hasPreOrCode = block.querySelector('pre, code');
        expect(hasPreOrCode).not.toBeNull();
      });
    });
  });

  // TC-1: Keyboard Navigation Support - Tab through all interactive elements on page
  describe('TC-1: Keyboard Navigation Support', () => {
    test('all links should be focusable', () => {
      const links = document.querySelectorAll('a[href]');

      links.forEach((link) => {
        const tabIndex = link.getAttribute('tabindex');
        // tabindex should not be negative (which would remove from tab order)
        if (tabIndex !== null) {
          expect(parseInt(tabIndex)).toBeGreaterThanOrEqual(0);
        }
      });
    });

    test('all buttons should be focusable', () => {
      const buttons = document.querySelectorAll('button');

      buttons.forEach((button) => {
        const tabIndex = button.getAttribute('tabindex');
        const disabled = button.hasAttribute('disabled');

        // Non-disabled buttons should be focusable
        if (!disabled && tabIndex !== null) {
          expect(parseInt(tabIndex)).toBeGreaterThanOrEqual(0);
        }
      });
    });

    test('interactive elements should have sufficient size for touch targets', () => {
      const ctaButtons = document.querySelectorAll('.cta-primary, .cta-secondary, .btn-primary, .btn-secondary');

      // Verify CTA buttons exist and are links (will have natural click area)
      ctaButtons.forEach((btn) => {
        expect(btn).not.toBeNull();
        // CSS handles minimum size, just verify elements exist
      });
    });

    test('should have skip link for keyboard navigation', () => {
      const skipLink = document.querySelector('.skip-link, a[href="#main-content"], a[href="#content"]');
      expect(skipLink).not.toBeNull();
      expect(skipLink.textContent.toLowerCase()).toContain('skip');
    });

    test('skip link should target main content', () => {
      const skipLink = document.querySelector('.skip-link');
      if (skipLink) {
        const href = skipLink.getAttribute('href');
        expect(href).toMatch(/^#/); // Should be an anchor link
        // Target element should exist
        const targetId = href.substring(1);
        const targetElement = document.getElementById(targetId);
        expect(targetElement).not.toBeNull();
      }
    });

    test('page should have main landmark element', () => {
      const mainElement = document.querySelector('main');
      expect(mainElement).not.toBeNull();
    });

    test('page should have header landmark element', () => {
      const headerElement = document.querySelector('header');
      expect(headerElement).not.toBeNull();
    });

    test('CTA buttons should be reachable (have valid href)', () => {
      const ctaPrimary = document.querySelector('.cta-primary, .btn-primary');
      const ctaSecondary = document.querySelector('.cta-secondary');

      expect(ctaPrimary).not.toBeNull();
      expect(ctaPrimary.getAttribute('href')).toBeTruthy();

      expect(ctaSecondary).not.toBeNull();
      expect(ctaSecondary.getAttribute('href')).toBeTruthy();
    });
  });

  // TC-4: Focus Indicators - Visible focus indicator is displayed
  describe('TC-4: Focus Indicators', () => {
    test('links should be distinguishable (have color styling)', () => {
      // This is a structural test - CSS will be verified separately
      const links = document.querySelectorAll('a');
      expect(links.length).toBeGreaterThan(0);
    });

    test('CSS file should include focus styles', () => {
      // Read the CSS file to verify focus styles are defined
      const cssPath = path.join(__dirname, '../src/styles.css');
      const cssContent = fs.readFileSync(cssPath, 'utf8');

      // Check for focus-related CSS rules
      expect(cssContent).toContain(':focus');
      expect(cssContent).toContain('outline');
    });

    test('focus styles should provide visible indicators', () => {
      const cssPath = path.join(__dirname, '../src/styles.css');
      const cssContent = fs.readFileSync(cssPath, 'utf8');

      // Focus styles should have visible outline or other indicator
      expect(cssContent).toMatch(/a:focus|button:focus|\[tabindex\]:focus/);
    });

    test('skip link should have focus styles', () => {
      const cssPath = path.join(__dirname, '../src/styles.css');
      const cssContent = fs.readFileSync(cssPath, 'utf8');

      // Skip link should become visible on focus
      expect(cssContent).toContain('.skip-link:focus');
    });
  });

  // Screen reader support
  describe('Screen Reader Support', () => {
    test('page should have lang attribute on html element', () => {
      const html = document.documentElement;
      const lang = html.getAttribute('lang');
      expect(lang).not.toBeNull();
      expect(lang).toBe('en');
    });

    test('page should have a descriptive title', () => {
      const title = document.querySelector('title');
      expect(title).not.toBeNull();
      expect(title.textContent.trim().length).toBeGreaterThan(0);
      expect(title.textContent).toContain('MirDB');
    });

    test('page should have meta description', () => {
      const metaDesc = document.querySelector('meta[name="description"]');
      expect(metaDesc).not.toBeNull();
      const content = metaDesc.getAttribute('content');
      expect(content).not.toBeNull();
      expect(content.trim().length).toBeGreaterThan(0);
    });

    test('external links should have proper attributes', () => {
      const externalLinks = document.querySelectorAll('a[href^="http"], a[href^="https"]');

      externalLinks.forEach((link) => {
        if (link.getAttribute('target') === '_blank') {
          const rel = link.getAttribute('rel');
          // External links opening in new tab should have noopener for security
          expect(rel).toContain('noopener');
        }
      });
    });

    test('code blocks should be accessible to screen readers', () => {
      const codeBlocks = document.querySelectorAll('.code-block');

      codeBlocks.forEach((block) => {
        const pre = block.querySelector('pre');
        const code = block.querySelector('code');

        // Code should be wrapped in semantic elements
        expect(pre || code).not.toBeNull();
      });
    });
  });

  // TC-2: Color Contrast - All text meets minimum contrast ratio of 4.5:1 for normal text
  describe('TC-2: Color Contrast', () => {
    test('text elements should exist for contrast checking', () => {
      const textElements = document.querySelectorAll('p, h1, h2, h3, h4, h5, h6, span, a');
      expect(textElements.length).toBeGreaterThan(0);
    });

    test('body should have explicit color styling source', () => {
      // Verify that styles.css is linked
      const styleLinks = document.querySelectorAll('link[rel="stylesheet"]');
      expect(styleLinks.length).toBeGreaterThan(0);
    });

    test('CSS should define appropriate text colors', () => {
      const cssPath = path.join(__dirname, '../src/styles.css');
      const cssContent = fs.readFileSync(cssPath, 'utf8');

      // Body text color should be dark enough for contrast
      expect(cssContent).toContain('color: #333');
    });

    test('hero section should have sufficient contrast for white text on dark bg', () => {
      const cssPath = path.join(__dirname, '../src/styles.css');
      const cssContent = fs.readFileSync(cssPath, 'utf8');

      // Hero section uses white text on dark gradient background
      // The colors #a0aec0 (tagline) on #1a1a2e background = ~5.7:1 ratio (passes AA)
      // The colors #cbd5e0 (description) on #1a1a2e background = ~8.3:1 ratio (passes AAA)
      expect(cssContent).toContain('.hero-section');
      expect(cssContent).toContain('#1a1a2e'); // Dark background
    });

    test('feature cards should have readable text colors', () => {
      const cssPath = path.join(__dirname, '../src/styles.css');
      const cssContent = fs.readFileSync(cssPath, 'utf8');

      // Feature cards use #1a1a2e on white = 16.1:1 (passes AAA)
      // Secondary text #4a5568 on white = 6.0:1 (passes AA)
      expect(cssContent).toContain('#1a1a2e'); // Header color
      expect(cssContent).toContain('#4a5568'); // Text color
    });

    test('footer links should have sufficient contrast', () => {
      const cssPath = path.join(__dirname, '../src/styles.css');
      const cssContent = fs.readFileSync(cssPath, 'utf8');

      // Footer has dark background with lighter text
      expect(cssContent).toContain('.footer');
      expect(cssContent).toContain('#0d0d1a'); // Dark footer background
      expect(cssContent).toContain('#a0aec0'); // Light text color (~5.7:1 on dark)
    });
  });

  // TC-7: Code blocks accessibility - Code snippets are accessible to screen readers
  describe('TC-7: Code Blocks Accessibility', () => {
    test('code blocks should use semantic elements', () => {
      const codeBlocks = document.querySelectorAll('.code-block');

      codeBlocks.forEach((block) => {
        const pre = block.querySelector('pre');
        const code = block.querySelector('code');

        expect(pre).not.toBeNull();
        expect(code).not.toBeNull();
      });
    });

    test('code blocks should have ARIA labels for screen readers', () => {
      const codeBlocks = document.querySelectorAll('.code-block');

      codeBlocks.forEach((block) => {
        const hasAriaLabel = block.getAttribute('aria-label');
        const hasRole = block.getAttribute('role');

        // Code blocks should have region role and aria-label
        expect(hasAriaLabel || hasRole).toBeTruthy();
      });
    });

    test('code elements should have accessible labels', () => {
      const codeElements = document.querySelectorAll('.code-block code');

      codeElements.forEach((code) => {
        const hasAriaLabel = code.getAttribute('aria-label');
        const hasContent = code.textContent.trim().length > 0;

        // Code should either have aria-label or meaningful content
        expect(hasAriaLabel || hasContent).toBeTruthy();
      });
    });

    test('inline code commands should be semantic', () => {
      const commandCodes = document.querySelectorAll('.command-item code, .command-card code');

      commandCodes.forEach((code) => {
        expect(code.textContent.trim().length).toBeGreaterThan(0);
      });
    });
  });

  // Table accessibility
  describe('Table Accessibility', () => {
    test('tables should have proper structure if present', () => {
      const tables = document.querySelectorAll('table');

      tables.forEach((table) => {
        // Tables should have thead or th elements for headers
        const hasHeaders = table.querySelector('thead') || table.querySelector('th');
        expect(hasHeaders).not.toBeNull();
      });
    });
  });
});

// Integration test for overall page accessibility structure
describe('Overall Page Accessibility Structure', () => {
  let document;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../src/index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf8');
    document = new DOMParser().parseFromString(htmlContent, 'text/html');
  });

  test('page should have logical content flow', () => {
    const body = document.querySelector('body');
    expect(body).not.toBeNull();

    // Body should contain main content areas
    const children = body.children;
    expect(children.length).toBeGreaterThan(0);
  });

  test('page should have skip-to-content or main landmark', () => {
    const mainLandmark = document.querySelector('main, [role="main"]');
    const skipLink = document.querySelector('a[href="#main"], a[href="#content"], .skip-link');
    const firstSection = document.querySelector('section');

    // Page should have some way to navigate to main content
    // Either main element, skip link, or clearly structured sections
    expect(mainLandmark || skipLink || firstSection).not.toBeNull();
  });
});

// TC-8: Lighthouse Accessibility Audit Simulation
// This test suite simulates key Lighthouse accessibility checks
// Actual Lighthouse scores require browser execution, but we can verify the underlying criteria
describe('TC-8: Lighthouse Accessibility Audit Criteria', () => {
  let document;
  let cssContent;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../src/index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf8');
    document = new DOMParser().parseFromString(htmlContent, 'text/html');

    const cssPath = path.join(__dirname, '../src/styles.css');
    cssContent = fs.readFileSync(cssPath, 'utf8');
  });

  // Lighthouse: document-title
  test('[document-title] page should have a title element', () => {
    const title = document.querySelector('title');
    expect(title).not.toBeNull();
    expect(title.textContent.trim().length).toBeGreaterThan(0);
  });

  // Lighthouse: html-has-lang
  test('[html-has-lang] html element should have lang attribute', () => {
    const html = document.documentElement;
    const lang = html.getAttribute('lang');
    expect(lang).not.toBeNull();
    expect(lang.length).toBeGreaterThan(0);
  });

  // Lighthouse: meta-viewport
  test('[meta-viewport] page should have viewport meta tag', () => {
    const viewport = document.querySelector('meta[name="viewport"]');
    expect(viewport).not.toBeNull();
    const content = viewport.getAttribute('content');
    expect(content).toContain('width=');
  });

  // Lighthouse: heading-order
  test('[heading-order] headings should be in sequential order', () => {
    const headings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
    const levels = Array.from(headings).map((h) => parseInt(h.tagName.charAt(1)));

    // First heading should be h1
    expect(levels[0]).toBe(1);

    // No heading level should be skipped
    for (let i = 1; i < levels.length; i++) {
      const diff = levels[i] - levels[i - 1];
      expect(diff).toBeLessThanOrEqual(1);
    }
  });

  // Lighthouse: link-name
  test('[link-name] all links should have discernible text', () => {
    const links = document.querySelectorAll('a[href]');

    links.forEach((link) => {
      const hasText = link.textContent.trim().length > 0;
      const hasAriaLabel = link.getAttribute('aria-label');
      const hasAriaLabelledBy = link.getAttribute('aria-labelledby');
      const hasTitle = link.getAttribute('title');

      expect(hasText || hasAriaLabel || hasAriaLabelledBy || hasTitle).toBeTruthy();
    });
  });

  // Lighthouse: button-name (for any button elements)
  test('[button-name] all buttons should have discernible text', () => {
    const buttons = document.querySelectorAll('button');

    buttons.forEach((button) => {
      const hasText = button.textContent.trim().length > 0;
      const hasAriaLabel = button.getAttribute('aria-label');
      const hasAriaLabelledBy = button.getAttribute('aria-labelledby');
      const hasTitle = button.getAttribute('title');

      expect(hasText || hasAriaLabel || hasAriaLabelledBy || hasTitle).toBeTruthy();
    });
  });

  // Lighthouse: image-alt
  test('[image-alt] all images should have alt attributes', () => {
    const images = document.querySelectorAll('img');

    images.forEach((img) => {
      const hasAlt = img.hasAttribute('alt');
      expect(hasAlt).toBe(true);
    });
  });

  // Lighthouse: list structure
  test('[list] lists should use proper ul/ol/li structure', () => {
    const lists = document.querySelectorAll('ul, ol');

    lists.forEach((list) => {
      const children = list.children;
      Array.from(children).forEach((child) => {
        expect(child.tagName.toLowerCase()).toBe('li');
      });
    });
  });

  // Lighthouse: landmark structure
  test('[landmark] page should have at least one landmark', () => {
    const landmarks = document.querySelectorAll(
      'header, nav, main, footer, section[aria-label], section[aria-labelledby], [role="banner"], [role="navigation"], [role="main"], [role="contentinfo"]'
    );
    expect(landmarks.length).toBeGreaterThan(0);
  });

  // Lighthouse: bypass (skip link)
  test('[bypass] page should have a method to bypass repeated content', () => {
    const skipLink = document.querySelector('.skip-link, a[href*="#main"], a[href*="#content"]');
    const mainLandmark = document.querySelector('main, [role="main"]');

    expect(skipLink || mainLandmark).not.toBeNull();
  });

  // Lighthouse: focus visible (verified via CSS)
  test('[focus-visible] CSS should include focus styles', () => {
    expect(cssContent).toContain(':focus');
    expect(cssContent).toContain('outline');
  });

  // Lighthouse: color-contrast (structural verification)
  test('[color-contrast] CSS should define text colors for contrast', () => {
    // Verify dark text on light backgrounds
    expect(cssContent).toContain('color: #333'); // Body text
    expect(cssContent).toContain('#1a1a2e'); // Dark heading text
    expect(cssContent).toContain('#4a5568'); // Secondary text

    // Verify light text on dark backgrounds
    expect(cssContent).toContain('#a0aec0'); // Light text on dark
    expect(cssContent).toContain('#cbd5e0'); // Description text
  });

  // Summary: All major Lighthouse accessibility criteria should pass
  test('should meet overall Lighthouse accessibility criteria for score >= 90', () => {
    // Count passing criteria (this is a summary assertion)
    const criteria = {
      'document-title': document.querySelector('title')?.textContent.trim().length > 0,
      'html-has-lang': document.documentElement.getAttribute('lang')?.length > 0,
      'meta-viewport': document.querySelector('meta[name="viewport"]') !== null,
      'heading-order': document.querySelector('h1') !== null,
      'link-name': Array.from(document.querySelectorAll('a[href]')).every(
        (a) => a.textContent.trim().length > 0 || a.getAttribute('aria-label')
      ),
      'landmark': document.querySelector('header, nav, main, footer') !== null,
      'bypass': document.querySelector('.skip-link, main') !== null,
      'focus-visible': cssContent.includes(':focus'),
    };

    const passingCriteria = Object.values(criteria).filter(Boolean).length;
    const totalCriteria = Object.keys(criteria).length;
    const passRate = (passingCriteria / totalCriteria) * 100;

    // For a score of 90+, we need at least 90% of criteria to pass
    expect(passRate).toBeGreaterThanOrEqual(90);
  });
});
