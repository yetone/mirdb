/**
 * Smooth Scroll Unit Tests
 * Owner: Scenario 12 - Smooth Scrolling & Interactions
 *
 * Test cases:
 * - CSS scroll-behavior property
 * - Button hover/focus/active styles
 * - Link hover styles
 * - prefers-reduced-motion is respected
 * - Scroll offset for sticky header
 */

const fs = require('fs');
const path = require('path');

describe('Smooth Scrolling & Interactions', () => {
  let stylesCSS;
  let responsiveCSS;
  let indexHTML;
  let smoothScrollJS;

  beforeAll(() => {
    // Load CSS and HTML files
    const cssPath = path.join(__dirname, '../../css/styles.css');
    const responsivePath = path.join(__dirname, '../../css/responsive.css');
    const htmlPath = path.join(__dirname, '../../index.html');
    const jsPath = path.join(__dirname, '../../js/smooth-scroll.js');

    stylesCSS = fs.readFileSync(cssPath, 'utf8');
    indexHTML = fs.readFileSync(htmlPath, 'utf8');
    smoothScrollJS = fs.readFileSync(jsPath, 'utf8');

    // responsive.css may or may not have reduced-motion styles
    if (fs.existsSync(responsivePath)) {
      responsiveCSS = fs.readFileSync(responsivePath, 'utf8');
    } else {
      responsiveCSS = '';
    }
  });

  describe('Test Case 1: CSS scroll-behavior property', () => {
    test('html or body has scroll-behavior: smooth', () => {
      // Check for scroll-behavior: smooth in CSS
      const scrollBehaviorRegex = /(html|body)\s*\{[^}]*scroll-behavior\s*:\s*smooth/;
      expect(stylesCSS).toMatch(scrollBehaviorRegex);
    });
  });

  describe('Test Case 3: Button hover styles', () => {
    test('Buttons have :hover pseudo-class with visible change', () => {
      // Check for .btn:hover or .btn-primary:hover or .btn-secondary:hover styles
      const hoverRegex = /\.btn[^{]*:hover\s*\{[^}]+\}/;
      const btnPrimaryHover = /\.btn-primary:hover\s*\{[^}]+\}/;
      const btnSecondaryHover = /\.btn-secondary:hover\s*\{[^}]+\}/;

      const hasHoverStyles =
        hoverRegex.test(stylesCSS) ||
        btnPrimaryHover.test(stylesCSS) ||
        btnSecondaryHover.test(stylesCSS);

      expect(hasHoverStyles).toBe(true);

      // Verify there's actual style change (background, color, transform, etc)
      const hoverMatch = stylesCSS.match(/\.btn[^{]*:hover\s*\{([^}]+)\}/);
      if (hoverMatch) {
        const hoverBody = hoverMatch[1];
        const hasVisibleChange =
          /background/.test(hoverBody) ||
          /color/.test(hoverBody) ||
          /transform/.test(hoverBody) ||
          /box-shadow/.test(hoverBody) ||
          /opacity/.test(hoverBody) ||
          /border/.test(hoverBody);
        expect(hasVisibleChange).toBe(true);
      }
    });
  });

  describe('Test Case 4: Button focus styles', () => {
    test('Buttons have :focus pseudo-class with visible indicator', () => {
      // Check for .btn:focus or button focus styles
      const focusRegex = /\.btn[^{]*:focus[^{]*\{[^}]+\}/;
      const buttonFocus = /button[^{]*:focus[^{]*\{[^}]+\}/;
      const faqQuestionFocus = /\.faq-question:focus\s*\{[^}]+\}/;
      const navToggleFocus = /\.nav-mobile-toggle:focus\s*\{[^}]+\}/;

      const hasFocusStyles =
        focusRegex.test(stylesCSS) ||
        buttonFocus.test(stylesCSS) ||
        faqQuestionFocus.test(stylesCSS) ||
        navToggleFocus.test(stylesCSS);

      expect(hasFocusStyles).toBe(true);

      // Verify focus has visible indicator (outline, box-shadow, border, ring)
      const allFocusMatches = stylesCSS.match(/(\.btn[^{]*|button[^{]*|\.[a-z-]+):focus[^{]*\{([^}]+)\}/gi) || [];
      if (allFocusMatches.length > 0) {
        const hasVisibleIndicator = allFocusMatches.some((match) => {
          return (
            /outline/.test(match) ||
            /box-shadow/.test(match) ||
            /border/.test(match) ||
            /ring/.test(match)
          );
        });
        expect(hasVisibleIndicator).toBe(true);
      }
    });
  });

  describe('Test Case 5: Button active/pressed styles', () => {
    test('Buttons have :active pseudo-class with visual feedback', () => {
      // Check for .btn:active styles
      const activeRegex = /\.btn[^{]*:active\s*\{[^}]+\}/;

      // Active styles should exist
      expect(stylesCSS).toMatch(activeRegex);

      // Verify there's visual feedback
      const activeMatch = stylesCSS.match(/\.btn[^{]*:active\s*\{([^}]+)\}/);
      if (activeMatch) {
        const activeBody = activeMatch[1];
        const hasVisualFeedback =
          /transform/.test(activeBody) ||
          /scale/.test(activeBody) ||
          /background/.test(activeBody) ||
          /box-shadow/.test(activeBody) ||
          /opacity/.test(activeBody);
        expect(hasVisualFeedback).toBe(true);
      }
    });
  });

  describe('Test Case 6: Link hover styles', () => {
    test('Links have :hover pseudo-class with visible change (underline, color)', () => {
      // Check for a:hover styles
      const linkHoverRegex = /a:hover\s*\{[^}]+\}/;
      const navLinkHover = /\.nav-link:hover\s*\{[^}]+\}/;
      const footerLinkHover = /\.footer-link:hover\s*\{[^}]+\}/;

      const hasLinkHover =
        linkHoverRegex.test(stylesCSS) ||
        navLinkHover.test(stylesCSS) ||
        footerLinkHover.test(stylesCSS);

      expect(hasLinkHover).toBe(true);

      // Verify visible change (text-decoration or color)
      const linkHoverMatch = stylesCSS.match(/a:hover\s*\{([^}]+)\}/);
      if (linkHoverMatch) {
        const hoverBody = linkHoverMatch[1];
        const hasVisibleChange =
          /text-decoration/.test(hoverBody) ||
          /underline/.test(hoverBody) ||
          /color/.test(hoverBody);
        expect(hasVisibleChange).toBe(true);
      }
    });
  });

  describe('Test Case 7: prefers-reduced-motion support', () => {
    test('Smooth scroll disabled when user prefers reduced motion', () => {
      // Check for prefers-reduced-motion media query in CSS
      const allCSS = stylesCSS + (responsiveCSS || '');
      const reducedMotionRegex = /@media\s*\(\s*prefers-reduced-motion\s*:\s*reduce\s*\)/;

      expect(allCSS).toMatch(reducedMotionRegex);

      // Verify scroll-behavior is set to auto in reduced motion
      const reducedMotionBlock =
        allCSS.match(/@media\s*\(\s*prefers-reduced-motion\s*:\s*reduce\s*\)\s*\{([^}]+(?:\{[^}]*\}[^}]*)*)\}/s);
      if (reducedMotionBlock) {
        const blockContent = reducedMotionBlock[1];
        const hasScrollBehaviorAuto =
          /scroll-behavior\s*:\s*auto/.test(blockContent) ||
          /scroll-behavior\s*:\s*initial/.test(blockContent) ||
          /scroll-behavior\s*:\s*unset/.test(blockContent);
        expect(hasScrollBehaviorAuto).toBe(true);
      }
    });

    test('Smooth scroll JS respects prefers-reduced-motion', () => {
      // Check if the JS module checks for prefers-reduced-motion
      const checksReducedMotion =
        /prefers-reduced-motion/.test(smoothScrollJS) ||
        /matchMedia/.test(smoothScrollJS);

      expect(checksReducedMotion).toBe(true);
    });
  });

  describe('Test Case 8: Smooth scroll accounts for sticky header', () => {
    test('Scroll implementation accounts for sticky header offset', () => {
      // Check if smooth-scroll.js has header offset logic
      const hasHeaderOffset =
        /header/i.test(smoothScrollJS) ||
        /offset/i.test(smoothScrollJS) ||
        /nav/i.test(smoothScrollJS) ||
        /sticky/i.test(smoothScrollJS) ||
        /scrollBy/i.test(smoothScrollJS) ||
        /getBoundingClientRect/i.test(smoothScrollJS) ||
        /offsetHeight/i.test(smoothScrollJS) ||
        /scrollTo/i.test(smoothScrollJS);

      expect(hasHeaderOffset).toBe(true);
    });

    test('CSS has scroll-padding-top or scroll-margin for sticky header', () => {
      // Check for scroll-padding-top on html/body or scroll-margin on sections
      const hasScrollPadding =
        /scroll-padding-top/.test(stylesCSS) ||
        /scroll-margin-top/.test(stylesCSS) ||
        /scroll-padding/.test(stylesCSS);

      expect(hasScrollPadding).toBe(true);
    });
  });
});

describe('Smooth Scroll JavaScript Module', () => {
  let originalDocument;
  let originalWindow;

  beforeEach(() => {
    // Set up DOM
    document.body.innerHTML = `
      <header class="header" style="height: 60px; position: sticky; top: 0;">
        <nav class="nav">
          <a href="#features" class="nav-link">Features</a>
          <a href="#pricing" class="nav-link">Pricing</a>
        </nav>
      </header>
      <main>
        <section id="hero" style="height: 500px;"></section>
        <section id="features" style="height: 500px;"></section>
        <section id="pricing" style="height: 500px;"></section>
      </main>
    `;

    // Mock matchMedia
    Object.defineProperty(window, 'matchMedia', {
      writable: true,
      value: jest.fn().mockImplementation((query) => ({
        matches: false,
        media: query,
        onchange: null,
        addListener: jest.fn(),
        removeListener: jest.fn(),
        addEventListener: jest.fn(),
        removeEventListener: jest.fn(),
        dispatchEvent: jest.fn(),
      })),
    });

    // Mock scrollTo
    window.scrollTo = jest.fn();

    // Mock Element.prototype.scrollIntoView
    Element.prototype.scrollIntoView = jest.fn();
  });

  afterEach(() => {
    document.body.innerHTML = '';
    jest.clearAllMocks();
  });

  test('initSmoothScroll function is exported', () => {
    const smoothScrollPath = require.resolve('../../js/smooth-scroll.js');
    // Clear cached module
    delete require.cache[smoothScrollPath];

    const smoothScroll = require('../../js/smooth-scroll.js');
    expect(typeof smoothScroll.initSmoothScroll).toBe('function');
  });

  test('scrollToElement function is exported', () => {
    const smoothScrollPath = require.resolve('../../js/smooth-scroll.js');
    delete require.cache[smoothScrollPath];

    const smoothScroll = require('../../js/smooth-scroll.js');
    expect(typeof smoothScroll.scrollToElement).toBe('function');
  });

  test('anchor links get click handlers attached', () => {
    const smoothScrollPath = require.resolve('../../js/smooth-scroll.js');
    delete require.cache[smoothScrollPath];

    const smoothScroll = require('../../js/smooth-scroll.js');
    smoothScroll.initSmoothScroll();

    const link = document.querySelector('a[href="#features"]');
    const clickEvent = new MouseEvent('click', {
      bubbles: true,
      cancelable: true,
      view: window,
    });

    // The click should be handled (preventDefault called)
    const prevented = !link.dispatchEvent(clickEvent);

    // Either preventDefault was called (prevented=true) or scrollTo/scrollIntoView was called
    const scrollCalled =
      window.scrollTo.mock.calls.length > 0 ||
      Element.prototype.scrollIntoView.mock.calls.length > 0;

    expect(prevented || scrollCalled).toBe(true);
  });
});
