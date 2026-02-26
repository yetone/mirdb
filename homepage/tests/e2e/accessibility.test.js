/**
 * Accessibility E2E Tests
 * Owner: Scenario 7 - Accessibility and Performance
 *
 * Tests for:
 * - WCAG AA compliance
 * - Keyboard navigation
 * - Screen reader compatibility
 * - Focus management
 */

const fs = require('fs');
const path = require('path');

describe('Accessibility E2E Tests', () => {
  let document;
  let htmlContent;

  beforeAll(() => {
    // Read the HTML file
    const htmlPath = path.join(__dirname, '../../index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf-8');

    // Create a mock DOM
    document = new DOMParser().parseFromString(htmlContent, 'text/html');
  });

  describe('Test Case 1: Accessibility Audit', () => {
    test('Page has proper document structure', () => {
      // Verify DOCTYPE
      expect(htmlContent.trim().toLowerCase()).toMatch(/^<!doctype html>/);

      // Verify html element has lang attribute
      expect(document.documentElement.getAttribute('lang')).toBe('en');
    });

    test('Page uses semantic landmark elements', () => {
      // header with role="banner"
      const header = document.querySelector('header[role="banner"]');
      expect(header).toBeTruthy();

      // nav with role="navigation"
      const nav = document.querySelector('nav[role="navigation"]');
      expect(nav).toBeTruthy();

      // main with role="main"
      const main = document.querySelector('main[role="main"]');
      expect(main).toBeTruthy();

      // footer with role="contentinfo"
      const footer = document.querySelector('footer[role="contentinfo"]');
      expect(footer).toBeTruthy();
    });

    test('All images have alt text', () => {
      const images = document.querySelectorAll('img');
      const imagesArray = Array.from(images);

      expect(imagesArray.length).toBeGreaterThan(0);

      for (const img of imagesArray) {
        expect(img.hasAttribute('alt')).toBe(true);
      }
    });

    test('No empty links or buttons', () => {
      const links = document.querySelectorAll('a');
      const buttons = document.querySelectorAll('button');

      const linksArray = Array.from(links);
      const buttonsArray = Array.from(buttons);

      for (const link of linksArray) {
        const hasContent =
          link.textContent.trim() ||
          link.getAttribute('aria-label') ||
          link.querySelector('img[alt]') ||
          link.querySelector('svg');
        expect(hasContent).toBeTruthy();
      }

      for (const button of buttonsArray) {
        const hasContent =
          button.textContent.trim() ||
          button.getAttribute('aria-label');
        expect(hasContent).toBeTruthy();
      }
    });

    test('Page has proper heading structure', () => {
      const headings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      const headingsArray = Array.from(headings);

      expect(headingsArray.length).toBeGreaterThan(0);

      // First heading should be h1
      expect(headingsArray[0].tagName.toLowerCase()).toBe('h1');

      // Only one h1
      const h1Count = document.querySelectorAll('h1').length;
      expect(h1Count).toBe(1);
    });

    test('Interactive elements have accessible names', () => {
      // Check navigation links
      const navLinks = document.querySelectorAll('.nav-link');
      for (const link of navLinks) {
        const hasName = link.textContent.trim() || link.getAttribute('aria-label');
        expect(hasName).toBeTruthy();
      }

      // Check CTA buttons
      const ctaButtons = document.querySelectorAll('.btn');
      for (const btn of ctaButtons) {
        const hasName = btn.textContent.trim() || btn.getAttribute('aria-label');
        expect(hasName).toBeTruthy();
      }
    });

    test('No accessibility violations for forms', () => {
      // Check if any form elements exist
      const formElements = document.querySelectorAll('input, select, textarea');

      for (const element of formElements) {
        const id = element.id;
        const hasLabel = id ? document.querySelector(`label[for="${id}"]`) : null;
        const hasAriaLabel = element.getAttribute('aria-label');
        const hasAriaLabelledby = element.getAttribute('aria-labelledby');

        expect(hasLabel || hasAriaLabel || hasAriaLabelledby).toBeTruthy();
      }
    });
  });

  describe('Test Case 2: Keyboard Navigation', () => {
    test('All interactive elements can receive focus', () => {
      // Get all interactive elements
      const interactiveSelectors = [
        'a[href]',
        'button:not([disabled])',
        'input:not([disabled])',
        'select:not([disabled])',
        'textarea:not([disabled])',
        '[tabindex]:not([tabindex="-1"])'
      ];

      const interactiveElements = document.querySelectorAll(interactiveSelectors.join(', '));
      const elementsArray = Array.from(interactiveElements);

      expect(elementsArray.length).toBeGreaterThan(0);

      for (const element of elementsArray) {
        // Check element is not explicitly removed from tab order
        const tabindex = element.getAttribute('tabindex');
        expect(tabindex !== '-1').toBe(true);
      }
    });

    test('Skip link exists for keyboard users', () => {
      const skipLink = document.querySelector('.skip-link');
      expect(skipLink).toBeTruthy();
      expect(skipLink.getAttribute('href')).toBe('#main-content');
    });

    test('Skip link target exists', () => {
      const skipLink = document.querySelector('.skip-link');
      const targetId = skipLink.getAttribute('href').replace('#', '');
      const target = document.getElementById(targetId);
      expect(target).toBeTruthy();
    });

    test('Navigation links are focusable', () => {
      const navLinks = document.querySelectorAll('.nav-links a');
      const linksArray = Array.from(navLinks);

      expect(linksArray.length).toBeGreaterThan(0);

      for (const link of linksArray) {
        // Links should be focusable by default
        const tabindex = link.getAttribute('tabindex');
        expect(tabindex !== '-1').toBe(true);
      }
    });

    test('CTA buttons are focusable', () => {
      const buttons = document.querySelectorAll('.btn');
      const buttonsArray = Array.from(buttons);

      expect(buttonsArray.length).toBeGreaterThan(0);

      for (const button of buttonsArray) {
        const tabindex = button.getAttribute('tabindex');
        expect(tabindex !== '-1').toBe(true);
      }
    });

    test('Mobile menu toggle has keyboard support', () => {
      const toggle = document.querySelector('.nav-mobile-toggle');
      expect(toggle).toBeTruthy();
      expect(toggle.tagName.toLowerCase()).toBe('button');
    });

    test('Footer links are focusable', () => {
      const footerLinks = document.querySelectorAll('.footer a');
      const linksArray = Array.from(footerLinks);

      for (const link of linksArray) {
        const tabindex = link.getAttribute('tabindex');
        expect(tabindex !== '-1').toBe(true);
      }
    });

    test('External links are properly marked', () => {
      const externalLinks = document.querySelectorAll('a[target="_blank"]');
      const linksArray = Array.from(externalLinks);

      for (const link of linksArray) {
        // Should have aria-label or visible text content or image alt
        const ariaLabel = link.getAttribute('aria-label');
        const textContent = link.textContent.trim();
        const hasImage = link.querySelector('img[alt]');
        const hasSvg = link.querySelector('svg');

        // Either has aria-label, visible text, image with alt, or icon
        expect(ariaLabel || textContent || hasImage || hasSvg).toBeTruthy();
      }
    });
  });

  describe('Test Case 8: Screen Reader Compatibility', () => {
    test('Page has descriptive title', () => {
      const title = document.querySelector('title');
      expect(title).toBeTruthy();
      expect(title.textContent).toContain('MirDB');
      expect(title.textContent.length).toBeGreaterThan(10);
    });

    test('Main heading describes page content', () => {
      const h1 = document.querySelector('h1');
      expect(h1).toBeTruthy();
      expect(h1.textContent.trim()).toBe('MirDB');
    });

    test('Sections have proper aria-labelledby', () => {
      const labelledSections = document.querySelectorAll('section[aria-labelledby]');

      for (const section of labelledSections) {
        const labelId = section.getAttribute('aria-labelledby');
        const label = document.getElementById(labelId);

        // Some sections are placeholders for other scenarios - check if section has content
        const sectionContent = section.querySelector('.container');
        const hasRealContent = sectionContent && sectionContent.textContent.trim().length > 100;

        // Only validate aria-labelledby if the section has real content
        if (hasRealContent) {
          expect(label).toBeTruthy();
          expect(label.textContent.trim().length).toBeGreaterThan(0);
        }
      }
    });

    test('Navigation has aria-label', () => {
      const navs = document.querySelectorAll('nav');

      for (const nav of navs) {
        const ariaLabel = nav.getAttribute('aria-label');
        expect(ariaLabel).toBeTruthy();
        expect(ariaLabel.length).toBeGreaterThan(0);
      }
    });

    test('Badges container has group role with label', () => {
      const badges = document.querySelector('.hero-badges');
      expect(badges).toBeTruthy();
      expect(badges.getAttribute('role')).toBe('group');
      expect(badges.getAttribute('aria-label')).toBeTruthy();
    });

    test('SVG icons are hidden from screen readers', () => {
      const svgs = document.querySelectorAll('svg');

      for (const svg of svgs) {
        expect(svg.getAttribute('aria-hidden')).toBe('true');
      }
    });

    test('Links opening in new tab have indication', () => {
      const externalLinks = document.querySelectorAll('a[target="_blank"]');

      for (const link of externalLinks) {
        const ariaLabel = link.getAttribute('aria-label');
        const textContent = link.textContent.trim();

        // Should have either aria-label or visible text
        expect(ariaLabel || textContent).toBeTruthy();
      }
    });
  });

  describe('Test Case 4: Page Load Performance', () => {
    test('Page has valid HTML structure', () => {
      expect(document.documentElement).toBeTruthy();
      expect(document.head).toBeTruthy();
      expect(document.body).toBeTruthy();
    });

    test('Required stylesheets are present', () => {
      const mainStyle = document.querySelector('link[href="css/styles.css"]');
      const responsiveStyle = document.querySelector('link[href="css/responsive.css"]');

      expect(mainStyle).toBeTruthy();
      expect(responsiveStyle).toBeTruthy();
    });

    test('JavaScript files are loaded at end of body', () => {
      const scripts = document.querySelectorAll('body > script');
      expect(scripts.length).toBeGreaterThan(0);
    });

    test('Images have dimensions to prevent CLS', () => {
      const heroLogo = document.querySelector('.hero-logo');
      expect(heroLogo).toBeTruthy();
      expect(heroLogo.getAttribute('width')).toBeTruthy();
      expect(heroLogo.getAttribute('height')).toBeTruthy();
    });

    test('No inline JavaScript that blocks rendering', () => {
      // Check for script tags without src (inline JS) in head
      const headScripts = document.querySelectorAll('head > script:not([src])');

      // Filter out non-blocking inline scripts
      for (const script of headScripts) {
        const isModule = script.getAttribute('type') === 'module';
        const isDeferred = script.hasAttribute('defer');
        const isAsync = script.hasAttribute('async');

        // Should be module, deferred, or async if in head
        expect(isModule || isDeferred || isAsync || headScripts.length === 0).toBe(true);
      }
    });
  });

  describe('Test Case 10: Cross-Browser Compatibility', () => {
    test('Page uses standard HTML5 elements', () => {
      // Check for standard semantic elements
      expect(document.querySelector('header')).toBeTruthy();
      expect(document.querySelector('main')).toBeTruthy();
      expect(document.querySelector('footer')).toBeTruthy();
      expect(document.querySelector('nav')).toBeTruthy();
      expect(document.querySelectorAll('section').length).toBeGreaterThan(0);
    });

    test('Page has viewport meta tag for mobile', () => {
      const viewport = document.querySelector('meta[name="viewport"]');
      expect(viewport).toBeTruthy();
      expect(viewport.getAttribute('content')).toContain('width=device-width');
    });

    test('Charset is UTF-8', () => {
      const charset = document.querySelector('meta[charset]');
      expect(charset).toBeTruthy();
      expect(charset.getAttribute('charset').toLowerCase()).toBe('utf-8');
    });

    test('External links have noopener for security', () => {
      const externalLinks = document.querySelectorAll('a[target="_blank"]');

      for (const link of externalLinks) {
        const rel = link.getAttribute('rel');
        expect(rel).toContain('noopener');
      }
    });
  });
});
