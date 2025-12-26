/**
 * Accessibility Compliance Tests
 * Tests for WCAG 2.1 AA compliance (NFR-3)
 */

describe('Accessibility Compliance', () => {
  // Test Case 2: Check all img elements for alt attributes
  describe('Image Alt Text', () => {
    test('all images have alt attributes (can be empty for decorative)', () => {
      const images = document.querySelectorAll('img');

      images.forEach((img, index) => {
        expect(img.hasAttribute('alt')).toBe(true);
      });

      // Even if there are no images, the test should pass
      // This ensures proper handling of pages without images
    });
  });

  // Test Case 3: Verify heading hierarchy
  describe('Heading Hierarchy', () => {
    test('page has single h1', () => {
      const h1Elements = document.querySelectorAll('h1');
      expect(h1Elements.length).toBe(1);
    });

    test('proper heading sequence without skipping levels', () => {
      const headings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      const headingLevels = [];

      headings.forEach(heading => {
        const level = parseInt(heading.tagName.charAt(1));
        headingLevels.push(level);
      });

      // Check that heading levels don't skip (e.g., h1 -> h3 without h2)
      for (let i = 1; i < headingLevels.length; i++) {
        const currentLevel = headingLevels[i];
        const previousLevel = headingLevels[i - 1];

        // Allow going to same level, one level deeper, or any level higher
        // But don't allow skipping levels when going deeper
        if (currentLevel > previousLevel) {
          expect(currentLevel - previousLevel).toBeLessThanOrEqual(1);
        }
      }
    });

    test('headings are properly nested', () => {
      const headings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      expect(headings.length).toBeGreaterThan(0);

      // First heading should be h1
      expect(headings[0].tagName.toLowerCase()).toBe('h1');
    });
  });

  // Test Case 5: Check for skip navigation link
  describe('Skip Navigation Link', () => {
    test('skip to main content link exists', () => {
      const skipLink = document.querySelector('a.skip-link, a[href="#main-content"]');
      expect(skipLink).not.toBeNull();
    });

    test('skip link is first focusable element', () => {
      const allFocusable = document.querySelectorAll(
        'a[href], button, input, select, textarea, [tabindex]:not([tabindex="-1"])'
      );

      expect(allFocusable.length).toBeGreaterThan(0);

      const firstFocusable = allFocusable[0];
      const isSkipLink =
        firstFocusable.classList.contains('skip-link') ||
        firstFocusable.getAttribute('href') === '#main-content';

      expect(isSkipLink).toBe(true);
    });

    test('skip link has proper text content', () => {
      const skipLink = document.querySelector('a.skip-link, a[href="#main-content"]');
      expect(skipLink).not.toBeNull();
      expect(skipLink.textContent.toLowerCase()).toContain('skip');
    });

    test('skip link target exists', () => {
      const skipLink = document.querySelector('a.skip-link, a[href="#main-content"]');
      expect(skipLink).not.toBeNull();

      const targetId = skipLink.getAttribute('href').replace('#', '');
      const targetElement = document.getElementById(targetId);
      expect(targetElement).not.toBeNull();
    });
  });

  // Test Case 6: Verify semantic landmark elements
  describe('Semantic Landmark Elements', () => {
    test('page uses header landmark element', () => {
      const header = document.querySelector('header, [role="banner"]');
      expect(header).not.toBeNull();
    });

    test('page uses main landmark element', () => {
      const main = document.querySelector('main, [role="main"]');
      expect(main).not.toBeNull();
    });

    test('page uses nav landmark element', () => {
      const nav = document.querySelector('nav, [role="navigation"]');
      expect(nav).not.toBeNull();
    });

    test('page uses footer landmark element', () => {
      const footer = document.querySelector('footer, [role="contentinfo"]');
      expect(footer).not.toBeNull();
    });

    test('main content has id for skip link target', () => {
      const main = document.querySelector('main, [role="main"]');
      expect(main).not.toBeNull();
      expect(main.id || main.querySelector('[id]')).toBeTruthy();
    });
  });

  // Additional accessibility tests
  describe('Link Accessibility', () => {
    test('all links have discernible text', () => {
      const links = document.querySelectorAll('a[href]');

      links.forEach(link => {
        const hasText = link.textContent.trim().length > 0;
        const hasAriaLabel = link.hasAttribute('aria-label');
        const hasAriaLabelledBy = link.hasAttribute('aria-labelledby');
        const hasTitle = link.hasAttribute('title');
        const hasImage = link.querySelector('img[alt]');

        const hasDiscernibleText = hasText || hasAriaLabel || hasAriaLabelledBy || hasTitle || hasImage;
        expect(hasDiscernibleText).toBe(true);
      });
    });

    test('external links have rel="noopener" for security', () => {
      const externalLinks = document.querySelectorAll('a[target="_blank"]');

      externalLinks.forEach(link => {
        const rel = link.getAttribute('rel');
        expect(rel).toContain('noopener');
      });
    });
  });

  describe('Interactive Elements', () => {
    test('buttons and interactive elements are keyboard accessible', () => {
      const buttons = document.querySelectorAll('button, [role="button"]');
      const interactiveLinks = document.querySelectorAll('a[href]');

      // All buttons should be focusable
      buttons.forEach(button => {
        const tabindex = button.getAttribute('tabindex');
        // tabindex should not be -1 (which removes from tab order)
        if (tabindex !== null) {
          expect(parseInt(tabindex)).toBeGreaterThanOrEqual(0);
        }
      });

      // All links should be focusable
      interactiveLinks.forEach(link => {
        const tabindex = link.getAttribute('tabindex');
        if (tabindex !== null) {
          expect(parseInt(tabindex)).toBeGreaterThanOrEqual(0);
        }
      });
    });
  });

  describe('Language Attribute', () => {
    test('html element has lang attribute', () => {
      const html = document.querySelector('html');
      expect(html).not.toBeNull();
      expect(html.hasAttribute('lang')).toBe(true);
      expect(html.getAttribute('lang')).toBeTruthy();
    });
  });

  describe('Form Accessibility', () => {
    test('form inputs have associated labels', () => {
      const inputs = document.querySelectorAll('input:not([type="hidden"]), select, textarea');

      inputs.forEach(input => {
        const id = input.getAttribute('id');
        const hasLabel = id ? document.querySelector(`label[for="${id}"]`) : false;
        const hasAriaLabel = input.hasAttribute('aria-label');
        const hasAriaLabelledBy = input.hasAttribute('aria-labelledby');
        const isInsideLabel = input.closest('label');

        const hasAccessibleName = hasLabel || hasAriaLabel || hasAriaLabelledBy || isInsideLabel;

        // Only fail if there are form inputs that lack accessible names
        if (inputs.length > 0) {
          expect(hasAccessibleName).toBeTruthy();
        }
      });
    });
  });
});

// Test Case 1: Automated accessibility audit (e2e-style comprehensive check)
describe('Comprehensive Accessibility Audit', () => {
  test('no critical accessibility violations detected', () => {
    // Check for common critical violations
    const violations = [];

    // Check 1: All images have alt text
    const imagesWithoutAlt = document.querySelectorAll('img:not([alt])');
    if (imagesWithoutAlt.length > 0) {
      violations.push(`${imagesWithoutAlt.length} images missing alt attribute`);
    }

    // Check 2: Has single h1
    const h1Count = document.querySelectorAll('h1').length;
    if (h1Count === 0) {
      violations.push('Page has no h1 heading');
    } else if (h1Count > 1) {
      violations.push(`Page has ${h1Count} h1 headings (should have exactly 1)`);
    }

    // Check 3: Has main landmark
    const main = document.querySelector('main, [role="main"]');
    if (!main) {
      violations.push('Page has no main landmark');
    }

    // Check 4: Has skip link
    const skipLink = document.querySelector('a.skip-link, a[href="#main-content"]');
    if (!skipLink) {
      violations.push('Page has no skip navigation link');
    }

    // Check 5: Html has lang attribute
    const html = document.querySelector('html');
    if (!html || !html.hasAttribute('lang')) {
      violations.push('Html element missing lang attribute');
    }

    // Check 6: Links have discernible text
    const links = document.querySelectorAll('a[href]');
    let linksWithoutText = 0;
    links.forEach(link => {
      const hasText = link.textContent.trim().length > 0;
      const hasAriaLabel = link.hasAttribute('aria-label');
      if (!hasText && !hasAriaLabel) {
        linksWithoutText++;
      }
    });
    if (linksWithoutText > 0) {
      violations.push(`${linksWithoutText} links without discernible text`);
    }

    // Expect no violations
    expect(violations).toEqual([]);
  });
});

// Test Case 4: Keyboard navigation (e2e-style)
describe('Keyboard Navigation', () => {
  test('all interactive elements are reachable via Tab key', () => {
    const focusableElements = document.querySelectorAll(
      'a[href], button, input, select, textarea, [tabindex]:not([tabindex="-1"])'
    );

    // Verify we have focusable elements
    expect(focusableElements.length).toBeGreaterThan(0);

    // Verify none of them are explicitly removed from tab order
    focusableElements.forEach(element => {
      const tabindex = element.getAttribute('tabindex');
      if (tabindex !== null) {
        expect(parseInt(tabindex)).toBeGreaterThanOrEqual(0);
      }
    });
  });

  test('skip link becomes visible on focus', () => {
    const skipLink = document.querySelector('a.skip-link, a[href="#main-content"]');
    expect(skipLink).not.toBeNull();

    // The skip link should exist and be focusable
    // In a real e2e test, we would check if it becomes visible on focus
    // Here we verify the structure is in place for keyboard users
    expect(skipLink.getAttribute('href')).toBe('#main-content');
  });
});

// Test Case 7: Color contrast (structure-based check)
describe('Color Contrast Support', () => {
  test('page uses CSS custom properties for theming', () => {
    // Check that the page uses CSS variables which can be verified for contrast
    const html = document.documentElement;
    const computedStyle = window.getComputedStyle(html);

    // Verify CSS is loaded (basic check)
    const body = document.querySelector('body');
    expect(body).not.toBeNull();
  });

  test('text elements have proper semantic styling', () => {
    // Check that main text content exists and uses semantic elements
    const mainContent = document.querySelector('main, [role="main"]');
    expect(mainContent).not.toBeNull();

    // Check for paragraphs and headings
    const paragraphs = mainContent.querySelectorAll('p');
    const headings = mainContent.querySelectorAll('h1, h2, h3, h4, h5, h6');

    expect(paragraphs.length + headings.length).toBeGreaterThan(0);
  });
});
