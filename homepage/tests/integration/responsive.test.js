/**
 * Responsive Layout Integration Tests
 * Owner: Scenario 6 - Responsive Design
 *
 * Tests for:
 * - Mobile viewport layout (375px)
 * - Tablet viewport layout (768px)
 * - Desktop viewport layout (1280px)
 * - No horizontal scrolling
 * - Text readability (minimum 16px body text)
 * - Image responsiveness
 * - Code block overflow handling
 * - Touch target sizes (minimum 44x44px)
 */

const fs = require('fs');
const path = require('path');

describe('Responsive Design Tests', () => {
  let htmlContent;
  let cssVariables;
  let responsiveCss;
  let mainStyles;

  beforeAll(() => {
    // Load HTML content
    const htmlPath = path.join(__dirname, '../../index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf8');

    // Load CSS files
    const variablesPath = path.join(__dirname, '../../css/variables.css');
    cssVariables = fs.readFileSync(variablesPath, 'utf8');

    const responsivePath = path.join(__dirname, '../../css/responsive.css');
    responsiveCss = fs.readFileSync(responsivePath, 'utf8');

    const stylesPath = path.join(__dirname, '../../css/styles.css');
    mainStyles = fs.readFileSync(stylesPath, 'utf8');
  });

  beforeEach(() => {
    document.documentElement.innerHTML = htmlContent;

    // Inject CSS into the document
    const styleElement = document.createElement('style');
    styleElement.textContent = cssVariables + mainStyles + responsiveCss;
    document.head.appendChild(styleElement);
  });

  afterEach(() => {
    document.documentElement.innerHTML = '';
  });

  describe('Test Case 1: Mobile viewport (375px width)', () => {
    it('should have all content visible without horizontal scrolling', () => {
      // Verify the HTML has proper viewport meta tag
      const viewportMeta = htmlContent.includes('width=device-width, initial-scale=1.0');
      expect(viewportMeta).toBe(true);

      // Verify CSS prevents horizontal scroll
      expect(responsiveCss).toContain('overflow-x: hidden');
      expect(responsiveCss).toContain('max-width: 100%');
      expect(responsiveCss).toContain('max-width: 100vw');
    });

    it('should have mobile-specific styles for max-width 767px', () => {
      expect(responsiveCss).toContain('@media screen and (max-width: 767px)');
    });

    it('should have hero CTA buttons stacked vertically on mobile', () => {
      // Verify mobile styles include vertical button stacking
      expect(responsiveCss).toContain('flex-direction: column');
      expect(responsiveCss).toContain('.hero-cta');
    });

    it('should have appropriate container padding for mobile', () => {
      // Verify container has mobile padding
      expect(responsiveCss).toContain('.container');
      expect(responsiveCss).toContain('padding: 0 var(--spacing-md)');
    });
  });

  describe('Test Case 2: Tablet viewport (768px width)', () => {
    it('should have tablet-specific media query', () => {
      expect(responsiveCss).toContain('@media screen and (min-width: 768px) and (max-width: 1023px)');
    });

    it('should have appropriate hero title size for tablet', () => {
      // Verify tablet has intermediate font size
      expect(responsiveCss).toContain('font-size: var(--font-size-4xl)');
    });

    it('should have horizontal CTA buttons on tablet', () => {
      // Tablet breakpoint should have horizontal button layout
      const tabletMediaQuery = responsiveCss.match(/@media screen and \(min-width: 768px\) and \(max-width: 1023px\)[\s\S]*?(?=@media|$)/);
      expect(tabletMediaQuery).not.toBeNull();
      expect(tabletMediaQuery[0]).toContain('flex-direction: row');
    });

    it('should have 2-column links grid on tablet', () => {
      const tabletStyles = responsiveCss.match(/@media screen and \(min-width: 768px\) and \(max-width: 1023px\)[\s\S]*?(?=@media|$)/);
      expect(tabletStyles).not.toBeNull();
      expect(tabletStyles[0]).toContain('grid-template-columns: repeat(2, 1fr)');
    });
  });

  describe('Test Case 3: Desktop viewport (1280px width)', () => {
    it('should have desktop-specific media query', () => {
      expect(responsiveCss).toContain('@media screen and (min-width: 1024px)');
      expect(responsiveCss).toContain('@media screen and (min-width: 1280px)');
    });

    it('should have full-size hero title on desktop', () => {
      const desktopStyles = responsiveCss.match(/@media screen and \(min-width: 1024px\)[\s\S]*?(?=@media|$)/);
      expect(desktopStyles).not.toBeNull();
      expect(desktopStyles[0]).toContain('font-size: var(--font-size-5xl)');
    });

    it('should have container max-width set for desktop', () => {
      const desktopStyles = responsiveCss.match(/@media screen and \(min-width: 1024px\)[\s\S]*?(?=@media|$)/);
      expect(desktopStyles).not.toBeNull();
      expect(desktopStyles[0]).toContain('max-width: var(--container-xl)');
    });

    it('should have 3-column links grid on desktop', () => {
      const desktopStyles = responsiveCss.match(/@media screen and \(min-width: 1024px\)[\s\S]*?(?=@media|$)/);
      expect(desktopStyles).not.toBeNull();
      expect(desktopStyles[0]).toContain('grid-template-columns: repeat(3, 1fr)');
    });
  });

  describe('Test Case 4: Text readability on mobile', () => {
    it('should have minimum 16px font size defined for body text', () => {
      // Verify font-size-base is 1rem (16px)
      expect(cssVariables).toContain('--font-size-base: 1rem');
    });

    it('should use base font size for mobile body text', () => {
      const mobileStyles = responsiveCss.match(/@media screen and \(max-width: 767px\)[\s\S]*?(?=@media|$)/);
      expect(mobileStyles).not.toBeNull();
      expect(mobileStyles[0]).toContain('font-size: var(--font-size-base)');
    });

    it('should have 16px minimum comment in CSS variables', () => {
      expect(cssVariables).toContain('16px - minimum for mobile body text');
    });

    it('should set body font-size to base on mobile', () => {
      // Main styles should set body font size
      expect(mainStyles).toContain('font-size: var(--font-size-base)');
    });
  });

  describe('Test Case 5: No horizontal scroll', () => {
    it('should have overflow-x: hidden on html and body', () => {
      expect(responsiveCss).toContain('html, body');
      expect(responsiveCss).toContain('overflow-x: hidden');
    });

    it('should have max-width: 100vw on body', () => {
      expect(responsiveCss).toContain('max-width: 100vw');
    });

    it('should have containers with max-width: 100%', () => {
      expect(responsiveCss).toContain('.container');
      expect(responsiveCss).toContain('max-width: 100%');
    });

    it('should have hero-content with overflow-x: hidden', () => {
      expect(responsiveCss).toContain('.hero-content');
      expect(responsiveCss).toContain('overflow-x: hidden');
    });
  });

  describe('Test Case 6: Image responsiveness', () => {
    it('should have max-width: 100% for images in responsive CSS', () => {
      expect(responsiveCss).toContain('img');
      expect(responsiveCss).toContain('max-width: 100%');
    });

    it('should have height: auto for images', () => {
      expect(responsiveCss).toContain('height: auto');
    });

    it('should have responsive hero logo in main styles', () => {
      expect(mainStyles).toContain('.hero-logo');
      expect(mainStyles).toContain('max-width: 100%');
    });

    it('should have mobile-specific logo width', () => {
      const mobileStyles = responsiveCss.match(/@media screen and \(max-width: 767px\)[\s\S]*?(?=@media|$)/);
      expect(mobileStyles).not.toBeNull();
      expect(mobileStyles[0]).toContain('.hero-logo');
      expect(mobileStyles[0]).toContain('width: 80px');
    });
  });

  describe('Test Case 7: Code block responsiveness', () => {
    it('should have overflow-x: auto on code blocks', () => {
      expect(responsiveCss).toContain('.code-block');
      expect(responsiveCss).toContain('overflow-x: auto');
    });

    it('should have max-width: 100% on code blocks', () => {
      expect(responsiveCss).toContain('pre');
      expect(responsiveCss).toContain('max-width: 100%');
    });

    it('should have white-space: pre for code content', () => {
      expect(responsiveCss).toContain('white-space: pre');
    });

    it('should have webkit-overflow-scrolling for smooth mobile scroll', () => {
      expect(responsiveCss).toContain('-webkit-overflow-scrolling: touch');
    });

    it('should have code blocks in main styles with overflow handling', () => {
      expect(mainStyles).toContain('.code-block');
      expect(mainStyles).toContain('overflow-x: auto');
    });
  });

  describe('Test Case 8: Touch targets on mobile', () => {
    it('should have touch target variable defined (44px minimum)', () => {
      expect(cssVariables).toContain('--touch-target-min: 44px');
    });

    it('should apply min-height to buttons', () => {
      expect(responsiveCss).toContain('.btn');
      expect(responsiveCss).toContain('min-height: var(--touch-target-min)');
    });

    it('should apply min-width to buttons', () => {
      expect(responsiveCss).toContain('min-width: var(--touch-target-min)');
    });

    it('should apply touch target size to resource links', () => {
      expect(responsiveCss).toContain('.resource-link');
      expect(responsiveCss).toContain('min-height: var(--touch-target-min)');
    });

    it('should have comment about WCAG touch target requirements', () => {
      expect(cssVariables).toContain('WCAG minimum 44x44px');
    });
  });

  describe('CSS Variables Validation', () => {
    it('should have all required breakpoint variables', () => {
      expect(cssVariables).toContain('--breakpoint-sm: 640px');
      expect(cssVariables).toContain('--breakpoint-md: 768px');
      expect(cssVariables).toContain('--breakpoint-lg: 1024px');
      expect(cssVariables).toContain('--breakpoint-xl: 1280px');
    });

    it('should have container width variables', () => {
      expect(cssVariables).toContain('--container-sm: 640px');
      expect(cssVariables).toContain('--container-md: 768px');
      expect(cssVariables).toContain('--container-lg: 1024px');
      expect(cssVariables).toContain('--container-xl: 1200px');
    });

    it('should have complete font size scale', () => {
      expect(cssVariables).toContain('--font-size-xs: 0.75rem');
      expect(cssVariables).toContain('--font-size-sm: 0.875rem');
      expect(cssVariables).toContain('--font-size-base: 1rem');
      expect(cssVariables).toContain('--font-size-lg: 1.125rem');
      expect(cssVariables).toContain('--font-size-xl: 1.25rem');
      expect(cssVariables).toContain('--font-size-2xl: 1.5rem');
      expect(cssVariables).toContain('--font-size-3xl: 1.875rem');
      expect(cssVariables).toContain('--font-size-4xl: 2.25rem');
      expect(cssVariables).toContain('--font-size-5xl: 3rem');
    });

    it('should have complete spacing scale', () => {
      expect(cssVariables).toContain('--spacing-xs: 0.25rem');
      expect(cssVariables).toContain('--spacing-sm: 0.5rem');
      expect(cssVariables).toContain('--spacing-md: 1rem');
      expect(cssVariables).toContain('--spacing-lg: 1.5rem');
      expect(cssVariables).toContain('--spacing-xl: 2rem');
      expect(cssVariables).toContain('--spacing-2xl: 3rem');
      expect(cssVariables).toContain('--spacing-3xl: 4rem');
    });
  });

  describe('HTML Structure Validation', () => {
    it('should have viewport meta tag for responsive design', () => {
      expect(htmlContent).toContain('<meta name="viewport" content="width=device-width, initial-scale=1.0">');
    });

    it('should link to responsive.css', () => {
      expect(htmlContent).toContain('href="css/responsive.css"');
    });

    it('should link to main styles.css', () => {
      expect(htmlContent).toContain('href="css/styles.css"');
    });

    it('should have hero section with proper classes', () => {
      const heroSection = document.querySelector('.hero');
      expect(heroSection).not.toBeNull();
    });

    it('should have container classes for layout control', () => {
      const containers = document.querySelectorAll('.container');
      expect(containers.length).toBeGreaterThan(0);
    });

    it('should have code blocks with proper structure', () => {
      const codeBlocks = document.querySelectorAll('.code-block');
      expect(codeBlocks.length).toBeGreaterThan(0);
    });

    it('should have buttons with btn class', () => {
      const buttons = document.querySelectorAll('.btn');
      expect(buttons.length).toBeGreaterThan(0);
    });

    it('should have images with dimensions defined', () => {
      const logo = document.querySelector('.hero-logo');
      expect(logo).not.toBeNull();
      expect(logo.getAttribute('width')).toBeTruthy();
      expect(logo.getAttribute('height')).toBeTruthy();
    });
  });

  describe('Media Query Coverage', () => {
    it('should have mobile breakpoint (< 768px)', () => {
      expect(responsiveCss).toContain('max-width: 767px');
    });

    it('should have tablet breakpoint (768px - 1023px)', () => {
      expect(responsiveCss).toContain('min-width: 768px');
      expect(responsiveCss).toContain('max-width: 1023px');
    });

    it('should have desktop breakpoint (>= 1024px)', () => {
      expect(responsiveCss).toContain('min-width: 1024px');
    });

    it('should have large desktop breakpoint (>= 1280px)', () => {
      expect(responsiveCss).toContain('min-width: 1280px');
    });

    it('should have print styles', () => {
      expect(responsiveCss).toContain('@media print');
    });
  });
});
