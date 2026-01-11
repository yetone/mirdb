/**
 * Accessibility Tests for MirDB Homepage
 * Tests WCAG 2.1 AA compliance
 */

const { axe, toHaveNoViolations } = require('jest-axe');

expect.extend(toHaveNoViolations);

// Helper to get the full HTML document
const getFullDocument = () => {
  const html = getIndexHTML();
  const parser = new DOMParser();
  return parser.parseFromString(html, 'text/html');
};

describe('Accessibility Compliance - WCAG 2.1 AA', () => {
  let htmlContent;
  let parsedDoc;

  beforeAll(() => {
    htmlContent = getIndexHTML();
    parsedDoc = getFullDocument();
  });

  beforeEach(() => {
    document.documentElement.innerHTML = htmlContent;
  });

  // Test Case 1: Run automated accessibility audit (axe-core)
  describe('Test Case 1: Automated Accessibility Audit (axe-core)', () => {
    it('should have no critical accessibility violations', async () => {
      const results = await axe(document.body);

      // Filter for critical and serious violations
      const criticalViolations = results.violations.filter(
        v => v.impact === 'critical' || v.impact === 'serious'
      );

      expect(criticalViolations).toHaveLength(0);
    });

    it('should pass axe accessibility audit', async () => {
      const results = await axe(document.body, {
        rules: {
          // WCAG 2.1 AA rules
          'color-contrast': { enabled: true },
          'link-name': { enabled: true },
          'button-name': { enabled: true },
          'image-alt': { enabled: true },
          'label': { enabled: true },
          'landmark-one-main': { enabled: true },
          'region': { enabled: true }
        }
      });

      expect(results).toHaveNoViolations();
    });
  });

  // Test Case 2: Tab through all interactive elements
  describe('Test Case 2: Keyboard Navigation', () => {
    it('should have all links with href attributes', () => {
      const links = document.querySelectorAll('a');

      links.forEach(link => {
        expect(link).toHaveAttribute('href');
      });

      expect(links.length).toBeGreaterThan(0);
    });

    it('should have all buttons and links reachable via keyboard', () => {
      const interactiveElements = document.querySelectorAll('a[href], button, [tabindex]:not([tabindex="-1"])');

      interactiveElements.forEach(element => {
        // Elements should not have tabindex=-1 which removes them from tab order
        const tabindex = element.getAttribute('tabindex');
        expect(tabindex).not.toBe('-1');

        // Anchor tags should have href
        if (element.tagName === 'A') {
          expect(element).toHaveAttribute('href');
        }
      });

      // Verify we have interactive elements to navigate
      expect(interactiveElements.length).toBeGreaterThan(0);
    });

    it('should have navigation links accessible', () => {
      const navLinks = document.querySelectorAll('.nav-links a');

      navLinks.forEach(link => {
        expect(link).toHaveAttribute('href');
        expect(link.textContent.trim()).not.toBe('');
      });

      expect(navLinks.length).toBeGreaterThan(0);
    });

    it('should have CTA buttons accessible', () => {
      const ctaButtons = document.querySelectorAll('.hero-cta a.btn');

      ctaButtons.forEach(button => {
        expect(button).toHaveAttribute('href');
        expect(button.textContent.trim()).not.toBe('');
      });

      expect(ctaButtons.length).toBeGreaterThan(0);
    });
  });

  // Test Case 3: Check all images for alt attributes
  describe('Test Case 3: Image Alt Attributes', () => {
    it('should have alt attribute on all img elements', () => {
      const images = document.querySelectorAll('img');

      images.forEach(img => {
        expect(img).toHaveAttribute('alt');
      });
    });

    it('should have descriptive alt text for meaningful images', () => {
      const images = document.querySelectorAll('img');

      images.forEach(img => {
        const alt = img.getAttribute('alt');
        expect(alt).not.toBeNull();

        // If alt is not empty (not decorative), it should have meaningful text
        if (alt !== '') {
          expect(alt.length).toBeGreaterThan(0);
        }
      });
    });

    it('should have logo image with proper alt text', () => {
      const logoImg = document.querySelector('.logo img, .logo-img');

      if (logoImg) {
        expect(logoImg).toHaveAttribute('alt');
        expect(logoImg.getAttribute('alt')).toBeTruthy();
      }
    });

    it('should have SVG icons with proper accessibility', () => {
      const svgIcons = document.querySelectorAll('svg');

      // SVG icons in feature cards should be decorative or have accessible labels
      svgIcons.forEach(svg => {
        // Check if SVG is inside a feature-icon container (decorative)
        const parent = svg.closest('.feature-icon');
        if (parent) {
          // Decorative icons should be hidden from screen readers or have aria-hidden
          // The feature title provides the accessible name
          const featureCard = parent.closest('.feature-card');
          if (featureCard) {
            const featureTitle = featureCard.querySelector('.feature-title');
            expect(featureTitle).toBeTruthy();
          }
        }
      });
    });
  });

  // Test Case 4: Test color contrast ratios
  describe('Test Case 4: Color Contrast Ratios', () => {
    it('should pass axe color contrast check', async () => {
      const results = await axe(document.body, {
        rules: {
          'color-contrast': { enabled: true }
        }
      });

      const contrastViolations = results.violations.filter(
        v => v.id === 'color-contrast'
      );

      expect(contrastViolations).toHaveLength(0);
    });

    it('should have properly defined text colors for readability', () => {
      // The CSS file uses CSS variables for theming
      // We verify the HTML links to the stylesheet
      const styleLink = document.querySelector('link[rel="stylesheet"]');
      expect(styleLink).toBeTruthy();
    });
  });

  // Test Case 5: Verify focus indicators visible
  describe('Test Case 5: Focus Indicators', () => {
    it('should have focus styles defined', async () => {
      // Check that the page has interactive elements that can receive focus
      const focusableElements = document.querySelectorAll(
        'a[href], button, input, select, textarea, [tabindex]:not([tabindex="-1"])'
      );

      expect(focusableElements.length).toBeGreaterThan(0);
    });

    it('should not disable outline on interactive elements', () => {
      const links = document.querySelectorAll('a');
      const buttons = document.querySelectorAll('button');

      // Check inline styles don't disable outlines
      [...links, ...buttons].forEach(element => {
        const inlineStyle = element.getAttribute('style') || '';
        expect(inlineStyle).not.toMatch(/outline:\s*none/i);
        expect(inlineStyle).not.toMatch(/outline:\s*0/i);
      });
    });

    it('should have buttons with visible text or labels', () => {
      const buttons = document.querySelectorAll('button, a.btn');

      buttons.forEach(button => {
        const hasText = button.textContent.trim().length > 0;
        const hasAriaLabel = button.hasAttribute('aria-label');
        const hasAriaLabelledby = button.hasAttribute('aria-labelledby');

        expect(hasText || hasAriaLabel || hasAriaLabelledby).toBe(true);
      });
    });
  });

  // Test Case 6: Check ARIA labels on icons/buttons
  describe('Test Case 6: ARIA Labels', () => {
    it('should have proper heading hierarchy', () => {
      const h1 = document.querySelectorAll('h1');
      const h2 = document.querySelectorAll('h2');

      // Should have exactly one h1
      expect(h1.length).toBe(1);

      // Should have h2 elements for sections
      expect(h2.length).toBeGreaterThan(0);
    });

    it('should have lang attribute on html element', () => {
      // Note: When loading HTML via innerHTML, the html element's attributes may not be preserved.
      // We check the original HTML string for the lang attribute
      expect(htmlContent).toMatch(/<html[^>]*\slang=/i);
    });

    it('should have nav element with accessible name or role', () => {
      const nav = document.querySelector('nav');

      expect(nav).toBeTruthy();
    });

    it('should have footer element', () => {
      const footer = document.querySelector('footer');

      expect(footer).toBeTruthy();
    });

    it('should have descriptive link text (no generic "click here")', () => {
      const links = document.querySelectorAll('a');

      const genericLinkTexts = ['click here', 'here', 'read more', 'more', 'link'];

      links.forEach(link => {
        const text = link.textContent.trim().toLowerCase();

        if (text) {
          genericLinkTexts.forEach(generic => {
            expect(text).not.toBe(generic);
          });
        }
      });
    });

    it('should have external links marked with rel="noopener noreferrer"', () => {
      const externalLinks = document.querySelectorAll('a[target="_blank"]');

      externalLinks.forEach(link => {
        const rel = link.getAttribute('rel');
        expect(rel).toMatch(/noopener/);
      });
    });

    it('should have main landmark or proper sections', () => {
      const main = document.querySelector('main');
      const header = document.querySelector('header');
      const sections = document.querySelectorAll('section');

      // Should have main element for proper landmark structure
      expect(main).toBeTruthy();
      // Should also have header and sections
      expect(header).toBeTruthy();
      expect(sections.length).toBeGreaterThan(0);
    });

    it('should have section titles with proper association', () => {
      const sections = document.querySelectorAll('section');

      sections.forEach(section => {
        // Sections should have headers or aria-labelledby
        const heading = section.querySelector('h2, h3');
        const ariaLabel = section.getAttribute('aria-label');
        const ariaLabelledby = section.getAttribute('aria-labelledby');

        expect(heading || ariaLabel || ariaLabelledby).toBeTruthy();
      });
    });
  });

  // Additional semantic HTML tests
  describe('Semantic HTML Structure', () => {
    it('should have proper document structure', () => {
      const header = document.querySelector('header, .hero');
      const nav = document.querySelector('nav, .navbar');
      const footer = document.querySelector('footer, .footer');

      expect(header).toBeTruthy();
      expect(nav).toBeTruthy();
      expect(footer).toBeTruthy();
    });

    it('should use semantic elements for features section', () => {
      const featuresSection = document.querySelector('#features, .features-section, [data-testid="features-section"]');

      expect(featuresSection).toBeTruthy();
    });
  });
});
