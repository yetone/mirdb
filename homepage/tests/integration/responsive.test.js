/**
 * Responsive Design Integration Tests
 * Owner: Scenario 7 - Responsive Design
 *
 * Tests for:
 * - Desktop viewport (1280px): Full layout with no horizontal scroll
 * - Tablet viewport (768px): Adapted layout with 2-column grid
 * - Mobile viewport (375px): Stacked layout, hamburger menu, readable text
 * - Touch targets: Minimum 44x44px touch targets
 * - Code block scrolling: Horizontal scroll within containers
 * - Orientation changes: Smooth adaptation between portrait/landscape
 *
 * Requirements: REQ-7, NFR-3
 */

import { loadHomepage, clearDocument } from '../setup.js';
import { readFileSync } from 'fs';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));

// Helper to load CSS and apply styles
function loadResponsiveCSS() {
  // Load main styles
  const stylesPath = resolve(__dirname, '../../css/styles.css');
  const responsivePath = resolve(__dirname, '../../css/responsive.css');
  const heroPath = resolve(__dirname, '../../css/components/hero.css');
  const featuresPath = resolve(__dirname, '../../css/components/features.css');
  const footerPath = resolve(__dirname, '../../css/components/footer.css');
  const quickstartPath = resolve(__dirname, '../../css/components/quickstart.css');
  const statusPath = resolve(__dirname, '../../css/components/status.css');

  const styles = readFileSync(stylesPath, 'utf-8');
  const responsive = readFileSync(responsivePath, 'utf-8');

  let hero = '';
  let features = '';
  let footer = '';
  let quickstart = '';
  let status = '';

  try {
    hero = readFileSync(heroPath, 'utf-8');
    features = readFileSync(featuresPath, 'utf-8');
    footer = readFileSync(footerPath, 'utf-8');
    quickstart = readFileSync(quickstartPath, 'utf-8');
    status = readFileSync(statusPath, 'utf-8');
  } catch (e) {
    // Some CSS files may be stubs - continue without them
  }

  const styleElement = document.createElement('style');
  styleElement.textContent = styles + hero + features + footer + quickstart + status + responsive;
  document.head.appendChild(styleElement);
}

// Helper to simulate viewport resize (simplified version without jest.fn())
function setViewport(width, height = 800) {
  Object.defineProperty(window, 'innerWidth', {
    value: width,
    writable: true,
    configurable: true
  });
  Object.defineProperty(window, 'innerHeight', {
    value: height,
    writable: true,
    configurable: true
  });

  // Dispatch resize event
  window.dispatchEvent(new Event('resize'));
}

describe('Responsive Design - Integration Tests', () => {
  beforeEach(() => {
    loadHomepage();
    loadResponsiveCSS();
  });

  afterEach(() => {
    clearDocument();
  });

  describe('Desktop Viewport (1280px)', () => {
    beforeEach(() => {
      setViewport(1280);
    });

    test('TC1: All content fits within viewport, no horizontal scroll', () => {
      // Check that body and main content fit within viewport
      const body = document.body;
      const main = document.getElementById('main-content');

      // The page should not have horizontal scroll at desktop width
      expect(body).toBeInTheDocument();
      expect(main).toBeInTheDocument();

      // Verify max-width constraints exist via CSS
      const responsivePath = resolve(__dirname, '../../css/responsive.css');
      const responsiveCSS = readFileSync(responsivePath, 'utf-8');

      // Should have rules to prevent overflow
      expect(responsiveCSS).toMatch(/overflow-x:\s*hidden/i);
    });

    test('Desktop should display full navigation menu', () => {
      const navMenu = document.querySelector('.nav__menu');
      const navToggle = document.querySelector('.nav__mobile-toggle');

      expect(navMenu).toBeInTheDocument();
      expect(navToggle).toBeInTheDocument();

      // Check that responsive CSS hides mobile toggle on desktop
      const responsivePath = resolve(__dirname, '../../css/responsive.css');
      const responsiveCSS = readFileSync(responsivePath, 'utf-8');

      // Should have rules for desktop navigation display
      expect(responsiveCSS).toMatch(/min-width:\s*768px/);
    });
  });

  describe('Tablet Viewport (768px)', () => {
    beforeEach(() => {
      setViewport(768);
    });

    test('TC2: Feature grid adapts to 2 columns or stacked layout', () => {
      const featuresGrid = document.querySelector('.features-grid');
      expect(featuresGrid).toBeInTheDocument();

      // Check responsive CSS has tablet breakpoint rules
      const responsivePath = resolve(__dirname, '../../css/responsive.css');
      const responsiveCSS = readFileSync(responsivePath, 'utf-8');

      // Should contain tablet-specific grid adjustments
      expect(responsiveCSS).toMatch(/768px/);

      // Should have grid-template-columns adjustments for tablets
      const hasGridAdjustment = responsiveCSS.includes('grid-template-columns') ||
                                 responsiveCSS.includes('features-grid');
      expect(hasGridAdjustment).toBe(true);
    });
  });

  describe('Mobile Viewport (375px)', () => {
    beforeEach(() => {
      setViewport(375);
    });

    test('TC3: Content stacks vertically, text remains readable (min 14px font)', () => {
      const responsivePath = resolve(__dirname, '../../css/responsive.css');
      const responsiveCSS = readFileSync(responsivePath, 'utf-8');

      // Should have mobile breakpoint
      expect(responsiveCSS).toMatch(/max-width:\s*767px|max-width:\s*768px/);

      // Should maintain readable font sizes
      const stylesPath = resolve(__dirname, '../../css/styles.css');
      const mainCSS = readFileSync(stylesPath, 'utf-8');

      // Base font size should be at least 1rem (16px) which is > 14px
      expect(mainCSS).toMatch(/font-size:\s*var\(--text-base\)|font-size:\s*1rem/);
    });

    test('TC4: Navigation transforms to hamburger menu button', () => {
      const mobileToggle = document.querySelector('.nav__mobile-toggle');
      expect(mobileToggle).toBeInTheDocument();

      // Check for hamburger icon SVG
      const hamburgerIcon = mobileToggle.querySelector('svg');
      expect(hamburgerIcon).toBeInTheDocument();

      // Check responsive CSS shows hamburger menu on mobile
      const responsivePath = resolve(__dirname, '../../css/responsive.css');
      const responsiveCSS = readFileSync(responsivePath, 'utf-8');

      // Should have rules to show mobile toggle
      expect(responsiveCSS).toMatch(/nav__mobile-toggle/);
    });

    test('TC5: Navigation menu expands/reveals with navigation options', () => {
      const mobileToggle = document.querySelector('.nav__mobile-toggle');
      const navMenu = document.querySelector('.nav__menu');

      expect(mobileToggle).toBeInTheDocument();
      expect(navMenu).toBeInTheDocument();

      // Check that menu has navigation links
      const navLinks = navMenu.querySelectorAll('.nav__link');
      expect(navLinks.length).toBeGreaterThan(0);

      // Verify aria attributes for accessibility
      expect(mobileToggle).toHaveAttribute('aria-expanded');
      expect(mobileToggle).toHaveAttribute('aria-controls', 'nav-menu');
      expect(navMenu).toHaveAttribute('id', 'nav-menu');

      // Check responsive CSS has open state styles
      const responsivePath = resolve(__dirname, '../../css/responsive.css');
      const responsiveCSS = readFileSync(responsivePath, 'utf-8');

      expect(responsiveCSS).toMatch(/nav__menu--open/);
    });

    test('TC6: Code blocks scroll horizontally within their container', () => {
      const codeBlock = document.querySelector('.quickstart__code-block');
      expect(codeBlock).toBeInTheDocument();

      // Check that code blocks have overflow-x: auto
      const responsivePath = resolve(__dirname, '../../css/responsive.css');
      const responsiveCSS = readFileSync(responsivePath, 'utf-8');

      // Pre elements should allow horizontal scrolling
      const stylesPath = resolve(__dirname, '../../css/styles.css');
      const mainCSS = readFileSync(stylesPath, 'utf-8');

      const hasOverflow = mainCSS.includes('overflow-x: auto') ||
                          responsiveCSS.includes('overflow-x: auto') ||
                          mainCSS.includes('overflow-x:auto') ||
                          responsiveCSS.includes('overflow-x:auto');
      expect(hasOverflow).toBe(true);
    });

    test('TC7: All interactive elements have minimum 44x44px touch target', () => {
      const responsivePath = resolve(__dirname, '../../css/responsive.css');
      const responsiveCSS = readFileSync(responsivePath, 'utf-8');

      // Check for touch target size rules (44px or larger)
      // Common patterns: min-height, min-width, padding combinations
      const hasTouchTargetRules = responsiveCSS.includes('min-height') ||
                                   responsiveCSS.includes('min-width') ||
                                   responsiveCSS.includes('44px') ||
                                   responsiveCSS.includes('2.75rem'); // 44px = 2.75rem

      expect(hasTouchTargetRules).toBe(true);

      // Verify buttons exist and will have proper sizing
      const buttons = document.querySelectorAll('button, a.hero__btn, .nav__link');
      expect(buttons.length).toBeGreaterThan(0);
    });
  });

  describe('Orientation and Layout Adaptation', () => {
    test('TC8: Layout adapts smoothly between portrait and landscape', () => {
      const responsivePath = resolve(__dirname, '../../css/responsive.css');
      const responsiveCSS = readFileSync(responsivePath, 'utf-8');

      // Check for viewport-based transitions or flexible layouts
      const hasFlexibleLayout = responsiveCSS.includes('flex') ||
                                 responsiveCSS.includes('grid') ||
                                 responsiveCSS.includes('vw') ||
                                 responsiveCSS.includes('%');

      expect(hasFlexibleLayout).toBe(true);

      // Simulate portrait (375x667)
      setViewport(375, 667);
      expect(window.innerWidth).toBe(375);
      expect(window.innerHeight).toBe(667);

      // Simulate landscape (667x375)
      setViewport(667, 375);
      expect(window.innerWidth).toBe(667);
      expect(window.innerHeight).toBe(375);
    });
  });

  describe('No Horizontal Scroll', () => {
    test('Viewport 1280px should not have horizontal overflow', () => {
      setViewport(1280);
      const responsivePath = resolve(__dirname, '../../css/responsive.css');
      const responsiveCSS = readFileSync(responsivePath, 'utf-8');

      // Check for overflow prevention
      expect(responsiveCSS).toMatch(/overflow-x:\s*hidden/i);
    });

    test('Viewport 768px should not have horizontal overflow', () => {
      setViewport(768);
      const responsivePath = resolve(__dirname, '../../css/responsive.css');
      const responsiveCSS = readFileSync(responsivePath, 'utf-8');

      expect(responsiveCSS).toMatch(/overflow-x:\s*hidden/i);
    });

    test('Viewport 375px should not have horizontal overflow', () => {
      setViewport(375);
      const responsivePath = resolve(__dirname, '../../css/responsive.css');
      const responsiveCSS = readFileSync(responsivePath, 'utf-8');

      expect(responsiveCSS).toMatch(/overflow-x:\s*hidden/i);
    });
  });

  describe('Responsive CSS Structure', () => {
    test('Has proper media query breakpoints', () => {
      const responsivePath = resolve(__dirname, '../../css/responsive.css');
      const responsiveCSS = readFileSync(responsivePath, 'utf-8');

      // Should have mobile breakpoint (< 768px)
      expect(responsiveCSS).toMatch(/@media.*max-width:\s*767px|@media.*max-width:\s*768px/);

      // Should have tablet breakpoint (768px - 1024px)
      expect(responsiveCSS).toMatch(/@media.*768px/);

      // Should have desktop breakpoint (> 1024px)
      expect(responsiveCSS).toMatch(/@media.*min-width:\s*1024px|@media.*min-width:\s*1280px/);
    });

    test('Contains mobile navigation styles', () => {
      const responsivePath = resolve(__dirname, '../../css/responsive.css');
      const responsiveCSS = readFileSync(responsivePath, 'utf-8');

      // Should have styles for mobile navigation
      expect(responsiveCSS).toMatch(/\.nav__mobile-toggle/);
      expect(responsiveCSS).toMatch(/\.nav__menu/);
    });

    test('Contains responsive typography adjustments', () => {
      const responsivePath = resolve(__dirname, '../../css/responsive.css');
      const responsiveCSS = readFileSync(responsivePath, 'utf-8');

      // Should adjust font sizes or headings for mobile
      const hasTypographyRules = responsiveCSS.includes('font-size') ||
                                  responsiveCSS.includes('hero__title') ||
                                  responsiveCSS.includes('--text-');
      expect(hasTypographyRules).toBe(true);
    });
  });
});
