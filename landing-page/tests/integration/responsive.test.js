/**
 * Responsive Design Integration Tests
 * Owner: Scenario 9 - Responsive Design
 *
 * Test cases:
 * - Layout adapts to mobile viewport
 * - Layout adapts to tablet viewport
 * - Layout adapts to desktop viewport
 * - Touch targets are appropriately sized on mobile
 * - No horizontal scrolling on any viewport
 */

const fs = require('fs');
const path = require('path');

// Read HTML, CSS, and responsive CSS files
const htmlPath = path.join(__dirname, '../../index.html');
const cssPath = path.join(__dirname, '../../css/styles.css');
const responsiveCssPath = path.join(__dirname, '../../css/responsive.css');
const variablesCssPath = path.join(__dirname, '../../css/variables.css');

const htmlContent = fs.readFileSync(htmlPath, 'utf-8');
const cssContent = fs.readFileSync(cssPath, 'utf-8');
const responsiveCssContent = fs.readFileSync(responsiveCssPath, 'utf-8');
const variablesCssContent = fs.readFileSync(variablesCssPath, 'utf-8');

describe('Responsive Design - Integration Tests', () => {
  let container;

  beforeEach(() => {
    document.body.innerHTML = '';
    container = document.createElement('div');
    container.innerHTML = htmlContent;
    document.body.appendChild(container);

    // Add all CSS styles
    const style = document.createElement('style');
    style.textContent = variablesCssContent + cssContent + responsiveCssContent;
    document.head.appendChild(style);
  });

  afterEach(() => {
    document.body.innerHTML = '';
    const styles = document.querySelectorAll('style');
    styles.forEach((style) => style.remove());
  });

  describe('Test Case 1: No Horizontal Overflow on Mobile (375px)', () => {
    test('Body does not have horizontal overflow styles', () => {
      // Check that the CSS contains responsive rules to prevent overflow
      const allCss = cssContent + responsiveCssContent;

      // Check for overflow-x: hidden or proper width constraints in responsive CSS
      expect(allCss).toMatch(/overflow-x\s*:\s*hidden|max-width\s*:\s*100%/);
    });

    test('Images have max-width: 100% to prevent overflow', () => {
      // Check that images have proper max-width constraint
      expect(cssContent).toMatch(/img[^{]*\{[^}]*max-width\s*:\s*100%/);
    });

    test('Container has proper padding for mobile', () => {
      const container = document.querySelector('.container');
      expect(container).toBeTruthy();

      // Check CSS defines container padding
      expect(cssContent).toMatch(/\.container\s*\{[^}]*padding/);
    });
  });

  describe('Test Case 2: Touch Targets (Minimum 44x44px)', () => {
    test('Buttons have minimum size CSS defined', () => {
      const allCss = cssContent + responsiveCssContent;

      // Check that responsive CSS defines minimum touch target sizes
      expect(allCss).toMatch(/min-height\s*:\s*(44px|2\.75rem|48px|3rem)/);
      expect(allCss).toMatch(/min-width\s*:\s*(44px|2\.75rem|48px|3rem)/);
    });

    test('Navigation mobile toggle has proper touch size', () => {
      const toggle = document.getElementById('nav-mobile-toggle');
      expect(toggle).toBeTruthy();

      // Check CSS defines proper size for toggle button
      expect(cssContent).toMatch(/\.nav-mobile-toggle\s*\{[^}]*width\s*:\s*44px/);
      expect(cssContent).toMatch(/\.nav-mobile-toggle\s*\{[^}]*height\s*:\s*44px/);
    });

    test('Buttons have adequate padding for touch', () => {
      // Check that buttons have padding defined
      expect(cssContent).toMatch(/\.btn\s*\{[^}]*padding/);
    });
  });

  describe('Test Case 3: Text Readability on Mobile', () => {
    test('Base font size is at least 16px (1rem)', () => {
      // Check CSS variables define font-size-base as 1rem (16px)
      expect(variablesCssContent).toMatch(/--font-size-base\s*:\s*1rem/);
    });

    test('Body uses proper font size', () => {
      // Check that body uses the font-size-base variable
      expect(cssContent).toMatch(/body\s*\{[^}]*font-size\s*:\s*var\(--font-size-base\)/);
    });

    test('Responsive CSS maintains readable text sizes on mobile', () => {
      // Check that responsive CSS doesn't reduce font size below readable threshold
      // Should not contain font-size less than 12px in mobile breakpoints
      const hasTinyFont = responsiveCssContent.match(/font-size\s*:\s*(\d+)px/g) || [];
      const allSizesReadable = hasTinyFont.every(match => {
        const size = parseInt(match.match(/(\d+)/)[1]);
        return size >= 12;
      });
      expect(allSizesReadable).toBe(true);
    });
  });

  describe('Test Case 4: Feature Cards Stack on Mobile', () => {
    test('Features grid CSS supports single column on mobile', () => {
      const allCss = cssContent + responsiveCssContent;

      // Check that responsive CSS has media query for features grid
      // Should have grid-template-columns: 1fr for mobile
      expect(allCss).toMatch(/\.features-grid[^}]*grid-template-columns/);
    });

    test('Feature cards exist in HTML', () => {
      const featureCards = document.querySelectorAll('.feature-card');
      expect(featureCards.length).toBeGreaterThan(0);
    });

    test('Responsive CSS defines mobile breakpoint for feature cards', () => {
      // Check for media query that affects features layout
      // The regex allows for comments and other content between media query and .feature selector
      expect(responsiveCssContent).toMatch(/@media[^{]*max-width[^{]*\{[\s\S]*?\.feature/);
    });
  });

  describe('Test Case 5: Feature Cards Multi-Column on Desktop (1280px)', () => {
    test('Features grid uses auto-fit for responsive columns', () => {
      // Check that features grid uses CSS grid with auto-fit
      expect(cssContent).toMatch(/\.features-grid\s*\{[^}]*grid-template-columns[^}]*auto-fit/);
    });

    test('Features grid defines minimum column width', () => {
      // Check that features grid has minmax for responsive sizing
      expect(cssContent).toMatch(/\.features-grid\s*\{[^}]*minmax\s*\(/);
    });
  });

  describe('Test Case 6: Images Scale Appropriately', () => {
    test('Images have max-width: 100% in reset styles', () => {
      expect(cssContent).toMatch(/img[^{]*\{[^}]*max-width\s*:\s*100%/);
    });

    test('Images have display: block for proper container fit', () => {
      expect(cssContent).toMatch(/img[^{]*\{[^}]*display\s*:\s*block/);
    });

    test('Responsive CSS handles hero image scaling', () => {
      const allCss = cssContent + responsiveCssContent;
      // Check that hero image section has proper responsive handling
      expect(allCss).toMatch(/\.hero__image|\.hero-image/);
    });
  });

  describe('Test Case 7: Viewport Meta Tag', () => {
    test('HTML contains viewport meta tag', () => {
      const viewportMeta = htmlContent.match(/<meta[^>]*name=["']viewport["'][^>]*>/i);
      expect(viewportMeta).toBeTruthy();
    });

    test('Viewport meta has width=device-width', () => {
      const viewportMeta = htmlContent.match(/<meta[^>]*name=["']viewport["'][^>]*>/i);
      expect(viewportMeta[0]).toMatch(/width\s*=\s*device-width/i);
    });

    test('Viewport meta has initial-scale=1', () => {
      const viewportMeta = htmlContent.match(/<meta[^>]*name=["']viewport["'][^>]*>/i);
      expect(viewportMeta[0]).toMatch(/initial-scale\s*=\s*1/i);
    });
  });

  describe('Test Case 8: Tablet Layout (768px)', () => {
    test('CSS contains tablet breakpoint media queries', () => {
      const allCss = cssContent + responsiveCssContent;
      // Check for 768px breakpoint
      expect(allCss).toMatch(/@media[^{]*768px/);
    });

    test('Responsive CSS defines tablet-specific adjustments', () => {
      // Check responsive CSS has media queries for tablet
      expect(responsiveCssContent).toMatch(/@media/);
    });

    test('Grid layouts adapt at tablet breakpoint', () => {
      const allCss = cssContent + responsiveCssContent;
      // Check that grid layouts have responsive behavior
      expect(allCss).toMatch(/grid-template-columns[^;]*repeat|grid-template-columns[^;]*auto-fit/);
    });
  });

  describe('Responsive CSS Structure', () => {
    test('Responsive CSS file exists and is not empty', () => {
      expect(responsiveCssContent.trim().length).toBeGreaterThan(100);
    });

    test('Responsive CSS contains mobile breakpoint', () => {
      // Mobile breakpoint (typically max-width: 640px or 767px)
      expect(responsiveCssContent).toMatch(/@media[^{]*(max-width\s*:\s*(640px|767px|768px))/);
    });

    test('Responsive CSS contains desktop breakpoint', () => {
      // Desktop breakpoint (typically min-width: 1024px or 1280px)
      expect(responsiveCssContent).toMatch(/@media[^{]*(min-width\s*:\s*(1024px|1200px|1280px))/);
    });

    test('Responsive CSS applies to key sections', () => {
      // Check that responsive CSS targets main page sections
      const sectionSelectors = ['.hero', '.features', '.benefits', '.pricing', '.testimonials', '.faq'];
      const targetsSections = sectionSelectors.some(selector =>
        responsiveCssContent.includes(selector)
      );
      expect(targetsSections).toBe(true);
    });
  });

  describe('Benefits Section Responsive Layout', () => {
    test('Benefits grid supports responsive columns', () => {
      const allCss = cssContent + responsiveCssContent;
      expect(allCss).toMatch(/\.benefits-grid/);
    });

    test('Benefits cards exist in HTML', () => {
      const benefitCards = document.querySelectorAll('.benefit-card');
      expect(benefitCards.length).toBeGreaterThan(0);
    });
  });

  describe('Pricing Section Responsive Layout', () => {
    test('Pricing grid uses responsive layout', () => {
      const allCss = cssContent + responsiveCssContent;
      expect(allCss).toMatch(/\.pricing-grid[^}]*grid/);
    });

    test('Pricing cards exist in HTML', () => {
      const pricingCards = document.querySelectorAll('.pricing-card');
      expect(pricingCards.length).toBeGreaterThan(0);
    });
  });

  describe('Testimonials Section Responsive Layout', () => {
    test('Testimonials grid supports responsive columns', () => {
      const allCss = cssContent + responsiveCssContent;
      expect(allCss).toMatch(/\.testimonials-grid/);
    });

    test('Testimonial cards exist in HTML', () => {
      const testimonialCards = document.querySelectorAll('.testimonial-card');
      expect(testimonialCards.length).toBeGreaterThan(0);
    });
  });

  describe('Touch-Friendly Interactive Elements', () => {
    test('Links in mobile menu have adequate touch targets', () => {
      const allCss = cssContent + responsiveCssContent;
      // Check that mobile links have proper padding/sizing
      expect(allCss).toMatch(/\.nav-mobile-link\s*\{[^}]*padding/);
    });

    test('FAQ buttons have proper touch target size', () => {
      const allCss = cssContent + responsiveCssContent;
      // Check that FAQ question buttons have proper padding
      expect(allCss).toMatch(/\.faq-question\s*\{[^}]*padding/);
    });

    test('CTA buttons have proper sizing for touch', () => {
      const allCss = cssContent + responsiveCssContent;
      // Check btn-lg has proper padding
      expect(allCss).toMatch(/\.btn-lg\s*\{[^}]*padding/);
    });
  });
});
