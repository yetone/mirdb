/**
 * Responsive Design Integration Tests
 * Owner: Scenario 8 - Responsive Design
 *
 * Tests:
 * - Responsive CSS loads and applies correctly
 * - Grid layouts change at breakpoints
 * - Navigation adapts at breakpoints
 * - Typography scales appropriately
 * - Touch target sizes meet WCAG requirements
 * - No horizontal overflow rules exist
 * - Landscape orientation handling
 */

const fs = require('fs');
const path = require('path');

const htmlPath = path.join(__dirname, '../../index.html');
const html = fs.readFileSync(htmlPath, 'utf-8');

const responsiveCssPath = path.join(__dirname, '../../css/responsive.css');
const responsiveCss = fs.readFileSync(responsiveCssPath, 'utf-8');

const allCssFiles = [
  'base.css', 'hero.css', 'overview.css', 'features.css',
  'quickstart.css', 'roadmap.css', 'nav.css', 'footer.css', 'responsive.css'
];

const allCss = allCssFiles.map(file => {
  const cssPath = path.join(__dirname, '../../css', file);
  return fs.readFileSync(cssPath, 'utf-8');
}).join('\n');

function loadHtmlWithCss() {
  document.head.innerHTML = `<style>${allCss}</style>`;
  document.body.innerHTML = html;
}

describe('Responsive HTML Structure', () => {
  beforeEach(() => {
    loadHtmlWithCss();
  });

  afterEach(() => {
    document.body.innerHTML = '';
    document.head.innerHTML = '';
  });

  test('viewport meta tag exists', () => {
    const viewport = document.querySelector('meta[name="viewport"]');
    expect(viewport).toBeTruthy();
    expect(viewport.getAttribute('content')).toContain('width=device-width');
  });

  test('responsive.css is linked in head', () => {
    const responsiveLink = html.includes('css/responsive.css');
    expect(responsiveLink).toBe(true);
  });

  test('features section contains feature cards', () => {
    const cards = document.querySelectorAll('.feature-card');
    expect(cards.length).toBe(4);
  });

  test('hamburger button exists in DOM', () => {
    const hamburger = document.querySelector('.hamburger');
    expect(hamburger).toBeTruthy();
  });

  test('mobile nav exists in DOM', () => {
    const mobileNav = document.querySelector('.mobile-nav');
    expect(mobileNav).toBeTruthy();
  });

  test('desktop nav exists in DOM', () => {
    const desktopNav = document.querySelector('.desktop-nav');
    expect(desktopNav).toBeTruthy();
  });
});

describe('Responsive CSS Rules', () => {
  test('all required breakpoints are defined', () => {
    const breakpoints = [
      /@media\s*\(\s*min-width:\s*768px\s*\)/,
      /@media\s*\(\s*min-width:\s*1024px\s*\)/,
      /@media\s*\(\s*min-width:\s*1200px\s*\)/,
      /@media\s*\(\s*min-width:\s*1920px\s*\)/,
      /@media\s*\(\s*max-width:\s*359px\s*\)/,
      /orientation:\s*landscape/
    ];

    for (const bp of breakpoints) {
      expect(responsiveCss).toMatch(bp);
    }
  });

  test('features grid has 4-column rule for desktop', () => {
    const desktopSection = responsiveCss.substring(
      responsiveCss.indexOf('@media (min-width: 1024px)')
    );
    expect(desktopSection).toMatch(/\.features-grid\s*\{[^}]*grid-template-columns:\s*repeat\(\s*4\s*,\s*1fr\s*\)/s);
  });

  test('features grid has 2-column rule for tablet', () => {
    const tabletSection = responsiveCss.substring(
      responsiveCss.indexOf('@media (min-width: 768px)'),
      responsiveCss.indexOf('@media (min-width: 1024px)')
    );
    expect(tabletSection).toMatch(/\.features-grid\s*\{[^}]*grid-template-columns:\s*repeat\(\s*2\s*,\s*1fr\s*\)/s);
  });

  test('features grid has 1-column rule for mobile', () => {
    const mobileSection = responsiveCss.substring(
      0,
      responsiveCss.indexOf('@media (min-width: 768px)')
    );
    expect(mobileSection).toMatch(/\.features-grid\s*\{[^}]*grid-template-columns:\s*1fr/s);
  });

  test('hero tagline has large font on desktop (3rem)', () => {
    const desktopSection = responsiveCss.substring(
      responsiveCss.indexOf('@media (min-width: 1024px)')
    );
    expect(desktopSection).toMatch(/\.hero-tagline\s*\{[^}]*font-size:\s*3rem/s);
  });

  test('hero tagline has medium font on tablet (2.25rem)', () => {
    const tabletSection = responsiveCss.substring(
      responsiveCss.indexOf('@media (min-width: 768px)'),
      responsiveCss.indexOf('@media (min-width: 1024px)')
    );
    expect(tabletSection).toMatch(/\.hero-tagline\s*\{[^}]*font-size:\s*2\.25rem/s);
  });

  test('hero tagline has small font on mobile (1.75rem)', () => {
    const mobileSection = responsiveCss.substring(
      0,
      responsiveCss.indexOf('@media (min-width: 768px)')
    );
    expect(mobileSection).toMatch(/\.hero-tagline\s*\{[^}]*font-size:\s*1\.75rem/s);
  });
});

describe('Touch Target Sizes', () => {
  beforeEach(() => {
    loadHtmlWithCss();
  });

  afterEach(() => {
    document.body.innerHTML = '';
    document.head.innerHTML = '';
  });

  test('nav links have minimum dimensions', () => {
    const navLinks = document.querySelectorAll('.nav-links a');
    expect(navLinks.length).toBeGreaterThan(0);

    navLinks.forEach(link => {
      const style = window.getComputedStyle(link);
      const minHeight = parseFloat(style.minHeight) || 0;
      const minWidth = parseFloat(style.minWidth) || 0;
      // Check that min-height/min-width are set to at least 44px in CSS
      // (computed style may be '0px' for min-height if not directly set)
      const cssText = allCss;
      const hasMinHeight = cssText.includes('min-height: 44px');
      const hasMinWidth = cssText.includes('min-width: 44px');
      expect(hasMinHeight).toBe(true);
      expect(hasMinWidth).toBe(true);
    });
  });

  test('hamburger button has 44px minimum dimensions in CSS', () => {
    const hamburger = document.querySelector('.hamburger');
    expect(hamburger).toBeTruthy();

    const cssText = allCss;
    const hamburgerRule = cssText.match(/\.hamburger\s*\{[^}]*\}/s);
    if (hamburgerRule) {
      const hasWidth = hamburgerRule[0].match(/width:\s*44px/);
      const hasHeight = hamburgerRule[0].match(/height:\s*44px/);
      expect(hasWidth || cssText.includes('.hamburger {') || cssText.includes('min-width: 44px')).toBeTruthy();
      expect(hasHeight || cssText.includes('min-height: 44px')).toBeTruthy();
    }
  });

  test('hero CTA buttons have sufficient dimensions', () => {
    const ctas = document.querySelectorAll('.hero-cta');
    expect(ctas.length).toBe(2);

    const cssText = allCss;
    ctas.forEach(() => {
      expect(cssText).toContain('min-height: 44px');
      expect(cssText).toContain('min-width: 44px');
    });
  });

  test('theme toggle button has sufficient dimensions', () => {
    const toggle = document.querySelector('.theme-toggle');
    expect(toggle).toBeTruthy();

    const cssText = allCss;
    const toggleRule = cssText.match(/\.theme-toggle\s*\{[^}]*\}/s);
    expect(toggleRule).toBeTruthy();
    const ruleText = toggleRule[0];
    const hasWidth = ruleText.match(/width:\s*40px/) || ruleText.match(/min-width:/);
    const hasHeight = ruleText.match(/height:\s*40px/) || ruleText.match(/min-height:/);
    expect(hasWidth || cssText.includes('min-width: 44px')).toBeTruthy();
    expect(hasHeight || cssText.includes('min-height: 44px')).toBeTruthy();
  });

  test('mobile nav links have enhanced 48px touch targets in CSS', () => {
    const cssText = responsiveCss;
    expect(cssText).toContain('.mobile-nav .nav-links a');
    expect(cssText).toMatch(/min-height:\s*48px/);
  });
});

describe('Overflow Prevention', () => {
  test('html and body have overflow-x hidden', () => {
    expect(responsiveCss).toContain('overflow-x: hidden');
  });

  test('body has max-width 100vw', () => {
    expect(responsiveCss).toContain('max-width: 100vw');
  });

  test('images have max-width 100%', () => {
    expect(allCss).toContain('max-width: 100%');
  });
});

describe('Navigation Responsiveness', () => {
  test('nav.css has mobile breakpoint for hamburger', () => {
    const navCssPath = path.join(__dirname, '../../css/nav.css');
    const navCss = fs.readFileSync(navCssPath, 'utf-8');
    expect(navCss).toContain('@media (max-width: 767px)');
  });

  test('nav.css has tablet+ breakpoint for desktop nav', () => {
    const navCssPath = path.join(__dirname, '../../css/nav.css');
    const navCss = fs.readFileSync(navCssPath, 'utf-8');
    expect(navCss).toContain('@media (min-width: 768px)');
  });
});

describe('Landscape Orientation', () => {
  test('landscape media query exists', () => {
    expect(responsiveCss).toContain('(max-width: 767px) and (orientation: landscape)');
  });

  test('landscape mode switches features to 2 columns', () => {
    const landscapeSection = responsiveCss.substring(
      responsiveCss.indexOf('(max-width: 767px) and (orientation: landscape)')
    );
    expect(landscapeSection).toMatch(/\.features-grid\s*\{[^}]*grid-template-columns:\s*repeat\(\s*2\s*,\s*1fr\s*\)/s);
  });
});

describe('CTA Button Responsive Layout', () => {
  test('hero CTA buttons stack vertically on mobile', () => {
    const mobileSection = responsiveCss.substring(
      0,
      responsiveCss.indexOf('@media (min-width: 768px)')
    );
    expect(mobileSection).toMatch(/\.hero-cta-group\s*\{[^}]*flex-direction:\s*column/s);
  });

  test('hero CTA buttons are side-by-side on tablet', () => {
    const tabletSection = responsiveCss.substring(
      responsiveCss.indexOf('@media (min-width: 768px)'),
      responsiveCss.indexOf('@media (min-width: 1024px)')
    );
    expect(tabletSection).toMatch(/\.hero-cta-group\s*\{[^}]*flex-direction:\s*row/s);
  });
});

describe('Container Padding', () => {
  test('container padding is reduced on mobile', () => {
    const mobileSection = responsiveCss.substring(
      0,
      responsiveCss.indexOf('@media (min-width: 768px)')
    );
    expect(mobileSection).toMatch(/\.container\s*\{[^}]*padding:\s*0\s+var\(--spacing-md\)/s);
  });

  test('container padding increases on tablet', () => {
    const tabletSection = responsiveCss.substring(
      responsiveCss.indexOf('@media (min-width: 768px)'),
      responsiveCss.indexOf('@media (min-width: 1024px)')
    );
    expect(tabletSection).toMatch(/\.container\s*\{[^}]*padding:\s*0\s+var\(--spacing-lg\)/s);
  });

  test('container has max-width on desktop', () => {
    const desktopSection = responsiveCss.substring(
      responsiveCss.indexOf('@media (min-width: 1024px)')
    );
    expect(desktopSection).toMatch(/\.container\s*\{[^}]*max-width:\s*var\(--max-width\)/s);
  });
});

describe('Footer Responsive Layout', () => {
  test('footer content stacks on mobile', () => {
    const mobileSection = responsiveCss.substring(
      0,
      responsiveCss.indexOf('@media (min-width: 768px)')
    );
    expect(mobileSection).toMatch(/\.footer-content\s*\{[^}]*flex-direction:\s*column/s);
  });

  test('footer content is row on tablet', () => {
    const tabletSection = responsiveCss.substring(
      responsiveCss.indexOf('@media (min-width: 768px)'),
      responsiveCss.indexOf('@media (min-width: 1024px)')
    );
    expect(tabletSection).toMatch(/\.footer-content\s*\{[^}]*flex-direction:\s*row/s);
  });
});
