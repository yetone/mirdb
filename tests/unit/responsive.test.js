/**
 * Responsive Design Unit Tests
 * Owner: Scenario 8 - Responsive Design
 *
 * Tests:
 * - Touch target sizes for interactive elements (WCAG 2.5.5)
 * - CSS media query presence
 * - Responsive.css loads correct breakpoint variables
 * - No horizontal overflow styles
 */

const fs = require('fs');
const path = require('path');

const responsiveCssPath = path.join(__dirname, '../../css/responsive.css');
const responsiveCss = fs.readFileSync(responsiveCssPath, 'utf-8');

describe('Responsive CSS Structure', () => {
  test('responsive.css exists and is not empty', () => {
    expect(responsiveCss).toBeTruthy();
    expect(responsiveCss.length).toBeGreaterThan(100);
  });

  test('has mobile breakpoint media query', () => {
    expect(responsiveCss).toContain('@media (min-width: 768px)');
  });

  test('has desktop breakpoint media query', () => {
    expect(responsiveCss).toContain('@media (min-width: 1024px)');
  });

  test('has wide desktop breakpoint media query', () => {
    expect(responsiveCss).toContain('@media (min-width: 1200px)');
  });

  test('has ultra-wide breakpoint media query', () => {
    expect(responsiveCss).toContain('@media (min-width: 1920px)');
  });

  test('has very small screen breakpoint', () => {
    expect(responsiveCss).toContain('@media (max-width: 359px)');
  });

  test('has landscape orientation media query', () => {
    expect(responsiveCss).toContain('orientation: landscape');
  });
});

describe('Touch Target Size Rules', () => {
  test('specifies minimum touch target size of 44px', () => {
    expect(responsiveCss).toContain('min-height: 44px');
    expect(responsiveCss).toContain('min-width: 44px');
  });

  test('touch target rules apply to interactive elements', () => {
    const selectors = [
      '.nav-links a',
      '.hamburger',
      '.theme-toggle',
      '.hero-cta',
      '.copy-button',
      '.ci-badge-link',
      '.footer-link-list a',
      '.header-logo'
    ];

    for (const selector of selectors) {
      expect(responsiveCss).toContain(selector);
    }
  });

  test('mobile nav links have enhanced touch targets', () => {
    expect(responsiveCss).toContain('.mobile-nav .nav-links a');
    expect(responsiveCss).toMatch(/min-height:\s*48px/);
  });
});

describe('Overflow Prevention', () => {
  test('prevents horizontal overflow on html and body', () => {
    expect(responsiveCss).toContain('overflow-x: hidden');
  });

  test('sets max-width on body to viewport width', () => {
    expect(responsiveCss).toContain('max-width: 100vw');
  });

  test('containers have 100% width on mobile', () => {
    expect(responsiveCss).toContain('width: 100%');
  });
});

describe('Typography Scaling', () => {
  test('hero tagline scales down on mobile', () => {
    // Mobile base should have smaller font
    const mobileSection = responsiveCss.substring(
      0,
      responsiveCss.indexOf('@media (min-width: 768px)')
    );
    expect(mobileSection).toContain('.hero-tagline');
    expect(mobileSection).toContain('font-size: 1.75rem');
  });

  test('hero tagline scales up on tablet', () => {
    const tabletSection = responsiveCss.substring(
      responsiveCss.indexOf('@media (min-width: 768px)'),
      responsiveCss.indexOf('@media (min-width: 1024px)')
    );
    expect(tabletSection).toContain('.hero-tagline');
    expect(tabletSection).toContain('font-size: 2.25rem');
  });

  test('hero tagline is largest on desktop', () => {
    const desktopSection = responsiveCss.substring(
      responsiveCss.indexOf('@media (min-width: 1024px)')
    );
    expect(desktopSection).toContain('.hero-tagline');
    expect(desktopSection).toContain('font-size: 3rem');
  });
});

describe('Grid Column Adjustments', () => {
  test('features grid is single column on mobile', () => {
    const mobileSection = responsiveCss.substring(
      0,
      responsiveCss.indexOf('@media (min-width: 768px)')
    );
    expect(mobileSection).toContain('.features-grid');
    expect(mobileSection).toContain('grid-template-columns: 1fr');
  });

  test('features grid is 2 columns on tablet', () => {
    const tabletSection = responsiveCss.substring(
      responsiveCss.indexOf('@media (min-width: 768px)'),
      responsiveCss.indexOf('@media (min-width: 1024px)')
    );
    expect(tabletSection).toContain('.features-grid');
    expect(tabletSection).toContain('grid-template-columns: repeat(2, 1fr)');
  });

  test('features grid is 4 columns on desktop', () => {
    const desktopSection = responsiveCss.substring(
      responsiveCss.indexOf('@media (min-width: 1024px)')
    );
    expect(desktopSection).toContain('.features-grid');
    expect(desktopSection).toContain('grid-template-columns: repeat(4, 1fr)');
  });
});

describe('Padding Adjustments', () => {
  test('container padding is reduced on mobile', () => {
    const mobileSection = responsiveCss.substring(
      0,
      responsiveCss.indexOf('@media (min-width: 768px)')
    );
    expect(mobileSection).toContain('.container');
    expect(mobileSection).toContain('padding: 0 var(--spacing-md)');
  });

  test('container padding increases on tablet', () => {
    const tabletSection = responsiveCss.substring(
      responsiveCss.indexOf('@media (min-width: 768px)'),
      responsiveCss.indexOf('@media (min-width: 1024px)')
    );
    expect(tabletSection).toContain('.container');
    expect(tabletSection).toContain('padding: 0 var(--spacing-lg)');
  });
});

describe('CTA Button Layout', () => {
  test('hero CTA buttons stack on mobile', () => {
    const mobileSection = responsiveCss.substring(
      0,
      responsiveCss.indexOf('@media (min-width: 768px)')
    );
    expect(mobileSection).toContain('.hero-cta-group');
    expect(mobileSection).toContain('flex-direction: column');
  });

  test('hero CTA buttons are side-by-side on tablet+', () => {
    const tabletSection = responsiveCss.substring(
      responsiveCss.indexOf('@media (min-width: 768px)'),
      responsiveCss.indexOf('@media (min-width: 1024px)')
    );
    expect(tabletSection).toContain('.hero-cta-group');
    expect(tabletSection).toContain('flex-direction: row');
  });
});

describe('Landscape Orientation', () => {
  test('has landscape-specific adjustments', () => {
    expect(responsiveCss).toContain('(max-width: 767px) and (orientation: landscape)');
  });

  test('features grid goes to 2 columns in landscape on mobile', () => {
    const landscapeSection = responsiveCss.substring(
      responsiveCss.indexOf('(max-width: 767px) and (orientation: landscape)')
    );
    expect(landscapeSection).toContain('.features-grid');
    expect(landscapeSection).toContain('grid-template-columns: repeat(2, 1fr)');
  });
});
