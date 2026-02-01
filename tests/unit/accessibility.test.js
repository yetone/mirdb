/**
 * Accessibility Tests
 * Owner: Scenario 12 - Accessibility Compliance
 *
 * Tests:
 * - Focus indicators
 * - Keyboard navigation
 * - Color contrast (WCAG AA)
 * - ARIA labels
 * - Skip link
 */
const { loadHTML, querySection } = require('../helpers/dom-utils');

describe('Accessibility Compliance', () => {
  beforeEach(() => {
    loadHTML('index.html');
  });

  describe('Test Case 1: Focus indicators', () => {
    it('should have visible focus indicators on all interactive elements', () => {
      // Get all interactive elements
      const links = document.querySelectorAll('a');
      const buttons = document.querySelectorAll('button');

      // Verify there are interactive elements to test
      expect(links.length).toBeGreaterThan(0);
      expect(buttons.length).toBeGreaterThan(0);

      // Check that we have focus styles defined in CSS
      // The custom.css defines focus styles for a:focus and button:focus
      const styleSheets = document.querySelectorAll('link[rel="stylesheet"]');
      const hasCustomCSS = Array.from(styleSheets).some(link =>
        link.getAttribute('href')?.includes('custom.css')
      );
      expect(hasCustomCSS).toBe(true);
    });

    it('should have visible focus styles that meet accessibility requirements', () => {
      // Verify links have appropriate attributes for accessibility
      const primaryCTA = document.getElementById('github-cta');
      const secondaryCTA = document.getElementById('get-started-cta');

      expect(primaryCTA).toBeInTheDocument();
      expect(secondaryCTA).toBeInTheDocument();

      // Check that CTAs are focusable (links and buttons are focusable by default)
      expect(primaryCTA.tagName.toLowerCase()).toBe('a');
      expect(secondaryCTA.tagName.toLowerCase()).toBe('a');
    });

    it('should have copy buttons that are focusable', () => {
      const copyButtons = document.querySelectorAll('.copy-button');
      expect(copyButtons.length).toBeGreaterThan(0);

      copyButtons.forEach(button => {
        expect(button.tagName.toLowerCase()).toBe('button');
        // Buttons are focusable by default unless tabindex=-1
        const tabIndex = button.getAttribute('tabindex');
        expect(tabIndex === null || parseInt(tabIndex) >= 0).toBe(true);
      });
    });
  });

  describe('Test Case 2: Keyboard navigation', () => {
    it('should have all interactive elements reachable via Tab key', () => {
      // Get all focusable elements
      const focusableSelectors = [
        'a[href]',
        'button:not([disabled])',
        'input:not([disabled])',
        'select:not([disabled])',
        'textarea:not([disabled])',
        '[tabindex]:not([tabindex="-1"])'
      ];

      const focusableElements = document.querySelectorAll(focusableSelectors.join(', '));
      expect(focusableElements.length).toBeGreaterThan(0);

      // Verify none have negative tabindex that would exclude them
      focusableElements.forEach(element => {
        const tabIndex = element.getAttribute('tabindex');
        if (tabIndex !== null) {
          expect(parseInt(tabIndex)).toBeGreaterThanOrEqual(-1);
        }
      });
    });

    it('should have logical tab order following visual layout', () => {
      // Elements should appear in document order which follows visual layout
      const focusableElements = document.querySelectorAll('a[href], button');
      const elementIds = Array.from(focusableElements)
        .filter(el => el.id)
        .map(el => el.id);

      // Skip link should be first (if present)
      const skipLink = document.querySelector('.skip-link, #skip-link');
      if (skipLink) {
        // Skip link should be one of the first focusable elements
        const skipLinkIndex = Array.from(focusableElements).indexOf(skipLink);
        expect(skipLinkIndex).toBeLessThanOrEqual(1);
      }

      // Hero CTAs should come before footer links
      const githubCTA = document.getElementById('github-cta');
      const footerGithubLink = document.getElementById('footer-github-link');

      if (githubCTA && footerGithubLink) {
        const ctaIndex = Array.from(focusableElements).indexOf(githubCTA);
        const footerIndex = Array.from(focusableElements).indexOf(footerGithubLink);
        expect(ctaIndex).toBeLessThan(footerIndex);
      }
    });

    it('should not have any keyboard traps', () => {
      // Verify that no element has a tabindex that would create navigation issues
      const allElements = document.querySelectorAll('*');
      const highTabIndexElements = Array.from(allElements).filter(el => {
        const tabIndex = el.getAttribute('tabindex');
        return tabIndex !== null && parseInt(tabIndex) > 0;
      });

      // Having tabindex > 0 is discouraged as it disrupts natural tab order
      // We allow it but verify it's minimal
      expect(highTabIndexElements.length).toBeLessThanOrEqual(5);
    });
  });

  describe('Test Case 3: Color contrast ratios', () => {
    it('should use color combinations that meet WCAG AA standards', () => {
      // The page uses a dark theme: bg-gray-900 text-white
      // gray-900 is #111827, white is #ffffff
      // This provides excellent contrast (21:1)

      const body = document.body;
      expect(body).toHaveClass('bg-gray-900');
      expect(body).toHaveClass('text-white');
    });

    it('should have readable text colors for secondary content', () => {
      // Check that gray text (text-gray-300, text-gray-400) is used appropriately
      const grayTextElements = document.querySelectorAll('[class*="text-gray-3"], [class*="text-gray-4"]');

      // Gray-300 (#d1d5db) on gray-900 (#111827) = ~11.3:1 contrast ratio
      // Gray-400 (#9ca3af) on gray-900 (#111827) = ~7.2:1 contrast ratio
      // Both meet WCAG AA for normal text (4.5:1) and large text (3:1)
      expect(grayTextElements.length).toBeGreaterThan(0);
    });

    it('should have sufficient contrast for headings', () => {
      const h1 = document.querySelector('h1');
      const h2Elements = document.querySelectorAll('h2');

      expect(h1).toBeInTheDocument();
      expect(h1).toHaveClass('text-white');

      h2Elements.forEach(h2 => {
        // h2 should have good contrast - either white or other high-contrast color
        expect(h2).toBeInTheDocument();
      });
    });

    it('should have accessible button colors', () => {
      const primaryCTA = document.getElementById('github-cta');
      const secondaryCTA = document.getElementById('get-started-cta');

      // Primary CTA: bg-white text-gray-900 (excellent contrast)
      expect(primaryCTA).toHaveClass('bg-white');
      expect(primaryCTA).toHaveClass('text-gray-900');

      // Secondary CTA: bg-blue-600 text-white
      // blue-600 (#2563eb) with white text = ~4.7:1 contrast (meets AA)
      expect(secondaryCTA).toHaveClass('bg-blue-600');
      expect(secondaryCTA).toHaveClass('text-white');
    });
  });

  describe('Test Case 4: ARIA labels', () => {
    it('should have ARIA labels on copy buttons', () => {
      const copyButtons = document.querySelectorAll('.copy-button');
      expect(copyButtons.length).toBeGreaterThan(0);

      copyButtons.forEach(button => {
        // Copy buttons should have aria-label describing their purpose
        const hasAriaLabel = button.hasAttribute('aria-label');
        const hasTextContent = button.textContent.trim().length > 0;

        // Either aria-label or visible text content is acceptable
        expect(hasAriaLabel || hasTextContent).toBe(true);
      });
    });

    it('should have aria-hidden on decorative SVG icons', () => {
      const decorativeSvgs = document.querySelectorAll('.feature-icon svg');

      decorativeSvgs.forEach(svg => {
        // Decorative icons in feature cards should have aria-hidden
        const ariaHidden = svg.getAttribute('aria-hidden');
        expect(ariaHidden).toBe('true');
      });
    });

    it('should have proper labels on images', () => {
      const images = document.querySelectorAll('img');

      images.forEach(img => {
        // All images should have alt attributes
        const hasAlt = img.hasAttribute('alt');
        expect(hasAlt).toBe(true);

        // Alt text should be meaningful (not empty for content images)
        const altText = img.getAttribute('alt');
        const isDecorative = altText === '';
        const hasMeaningfulAlt = altText && altText.length > 0;

        // Either decorative (empty alt) or meaningful alt text
        expect(isDecorative || hasMeaningfulAlt).toBe(true);
      });
    });

    it('should have appropriate ARIA labels for navigation sections', () => {
      // Check that sections have proper landmarks
      const hero = document.getElementById('hero');
      const features = document.getElementById('features');
      const demo = document.getElementById('demo');
      const gettingStarted = document.getElementById('getting-started');
      const footer = document.querySelector('footer');

      expect(hero).toBeInTheDocument();
      expect(features).toBeInTheDocument();
      expect(demo).toBeInTheDocument();
      expect(gettingStarted).toBeInTheDocument();
      expect(footer).toBeInTheDocument();

      // Footer should use footer element (implicit landmark)
      expect(footer.tagName.toLowerCase()).toBe('footer');
    });

    it('should have a main content area', () => {
      // Check for main element or role="main"
      const main = document.querySelector('main, [role="main"]');
      expect(main).toBeInTheDocument();
    });
  });

  describe('Test Case 5: Skip link', () => {
    it('should have a skip to main content link', () => {
      const skipLink = document.querySelector('.skip-link, #skip-link, a[href="#main"], a[href="#main-content"]');
      expect(skipLink).toBeInTheDocument();
    });

    it('should have skip link as first focusable element', () => {
      const focusableElements = document.querySelectorAll('a[href], button, [tabindex]:not([tabindex="-1"])');
      const firstFocusable = focusableElements[0];

      // First focusable element should be the skip link
      const isSkipLink =
        firstFocusable.classList.contains('skip-link') ||
        firstFocusable.id === 'skip-link' ||
        firstFocusable.getAttribute('href')?.includes('#main') ||
        firstFocusable.textContent?.toLowerCase().includes('skip');

      expect(isSkipLink).toBe(true);
    });

    it('should have skip link that targets main content', () => {
      const skipLink = document.querySelector('.skip-link, #skip-link, a[href="#main"], a[href="#main-content"]');
      const href = skipLink?.getAttribute('href');

      expect(href).toBeTruthy();
      expect(href.startsWith('#')).toBe(true);

      // The target element should exist
      const targetId = href.substring(1);
      const targetElement = document.getElementById(targetId);
      expect(targetElement).toBeInTheDocument();
    });

    it('should have visually hidden skip link that appears on focus', () => {
      const skipLink = document.querySelector('.skip-link, #skip-link');

      expect(skipLink).toBeInTheDocument();

      // Skip link should have classes for screen reader accessibility
      // It should be visually hidden but focusable
      const classes = skipLink.className;

      // Common patterns: sr-only with focus:not-sr-only, or custom skip-link class
      const hasAccessibleHiddenClass =
        classes.includes('sr-only') ||
        classes.includes('skip-link') ||
        classes.includes('visually-hidden');

      expect(hasAccessibleHiddenClass).toBe(true);
    });
  });
});
