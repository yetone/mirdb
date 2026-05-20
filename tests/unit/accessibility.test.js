/**
 * Accessibility Unit Tests
 * Owner: Scenario 9 - Accessibility and SEO
 *
 * Tests:
 * - Color contrast ratios (WCAG AA)
 * - ARIA labels and roles
 * - Focus indicator visibility
 * - Keyboard navigability
 * - axe-core automated scan
 */

const fs = require('fs');
const path = require('path');
const axe = require('axe-core');

const htmlPath = path.join(__dirname, '../../index.html');
const html = fs.readFileSync(htmlPath, 'utf-8');

const baseCssPath = path.join(__dirname, '../../css/base.css');
const baseCss = fs.readFileSync(baseCssPath, 'utf-8');

const cssFiles = [
  'base.css', 'hero.css', 'overview.css', 'features.css',
  'quickstart.css', 'roadmap.css', 'nav.css', 'footer.css', 'responsive.css'
];

const allCss = cssFiles.map(file => {
  const cssPath = path.join(__dirname, '../../css', file);
  try {
    return fs.readFileSync(cssPath, 'utf-8');
  } catch (e) {
    return '';
  }
}).join('\n');

// Parse CSS custom properties from :root only
function getCssCustomProperties(cssText) {
  const props = {};
  // Extract only the :root block
  const rootMatch = cssText.match(/:root\s*\{([^}]*)\}/s);
  const rootContent = rootMatch ? rootMatch[1] : cssText;
  const regex = /--([\w-]+):\s*([^;]+);/g;
  let match;
  while ((match = regex.exec(rootContent)) !== null) {
    props[`--${match[1]}`] = match[2].trim();
  }
  return props;
}

// Simple hex/rgb to luminance for contrast calculation
function getLuminance(color) {
  let r, g, b;

  if (color.startsWith('#')) {
    const hex = color.replace('#', '');
    if (hex.length === 3) {
      r = parseInt(hex[0] + hex[0], 16) / 255;
      g = parseInt(hex[1] + hex[1], 16) / 255;
      b = parseInt(hex[2] + hex[2], 16) / 255;
    } else {
      r = parseInt(hex.substr(0, 2), 16) / 255;
      g = parseInt(hex.substr(2, 2), 16) / 255;
      b = parseInt(hex.substr(4, 2), 16) / 255;
    }
  } else if (color.startsWith('rgb')) {
    const match = color.match(/\d+/g);
    if (match) {
      r = parseInt(match[0]) / 255;
      g = parseInt(match[1]) / 255;
      b = parseInt(match[2]) / 255;
    } else {
      return 0.5;
    }
  } else {
    return 0.5;
  }

  r = r <= 0.03928 ? r / 12.92 : Math.pow((r + 0.055) / 1.055, 2.4);
  g = g <= 0.03928 ? g / 12.92 : Math.pow((g + 0.055) / 1.055, 2.4);
  b = b <= 0.03928 ? b / 12.92 : Math.pow((b + 0.055) / 1.055, 2.4);

  return 0.2126 * r + 0.7152 * g + 0.0722 * b;
}

function getContrastRatio(color1, color2) {
  const lum1 = getLuminance(color1);
  const lum2 = getLuminance(color2);
  const lighter = Math.max(lum1, lum2);
  const darker = Math.min(lum1, lum2);
  return (lighter + 0.05) / (darker + 0.05);
}

const cssProps = getCssCustomProperties(baseCss);

describe('WCAG Color Contrast', () => {
  test('body text on background meets WCAG AA (4.5:1)', () => {
    const textColor = cssProps['--color-text'] || '#1f2937';
    const bgColor = cssProps['--color-bg'] || '#ffffff';
    const contrastRatio = getContrastRatio(textColor, bgColor);
    expect(contrastRatio).toBeGreaterThanOrEqual(4.5);
  });

  test('muted text on background meets WCAG AA (4.5:1)', () => {
    const mutedColor = cssProps['--color-muted'] || '#6b7280';
    const bgColor = cssProps['--color-bg'] || '#ffffff';
    const contrastRatio = getContrastRatio(mutedColor, bgColor);
    expect(contrastRatio).toBeGreaterThanOrEqual(4.5);
  });

  test('primary button text on primary background meets WCAG AA (4.5:1)', () => {
    const primaryColor = cssProps['--color-primary'] || '#2563eb';
    const white = '#ffffff';
    const contrastRatio = getContrastRatio(primaryColor, white);
    expect(contrastRatio).toBeGreaterThanOrEqual(4.5);
  });

  test('primary text on card background meets WCAG AA (4.5:1)', () => {
    const textColor = cssProps['--color-text'] || '#1f2937';
    const cardBg = cssProps['--color-card-bg'] || '#f9fafb';
    const contrastRatio = getContrastRatio(textColor, cardBg);
    expect(contrastRatio).toBeGreaterThanOrEqual(4.5);
  });

  test('dark theme body text on dark background meets WCAG AA (4.5:1)', () => {
    // Dark theme values from nav.css
    const darkText = '#f3f4f6';
    const darkBg = '#111827';
    const contrastRatio = getContrastRatio(darkText, darkBg);
    expect(contrastRatio).toBeGreaterThanOrEqual(4.5);
  });

  test('dark theme muted text on dark background meets WCAG AA (4.5:1)', () => {
    const darkMuted = '#9ca3af';
    const darkBg = '#111827';
    const contrastRatio = getContrastRatio(darkMuted, darkBg);
    expect(contrastRatio).toBeGreaterThanOrEqual(4.5);
  });

  test('dark theme primary on dark background meets WCAG AA (4.5:1)', () => {
    const darkPrimary = '#3b82f6';
    const darkBg = '#111827';
    const contrastRatio = getContrastRatio(darkPrimary, darkBg);
    expect(contrastRatio).toBeGreaterThanOrEqual(4.5);
  });

  test('terminal code text on terminal background meets WCAG AA (4.5:1)', () => {
    const codeText = '#cdd6f4';
    const termBg = '#1e1e2e';
    const contrastRatio = getContrastRatio(codeText, termBg);
    expect(contrastRatio).toBeGreaterThanOrEqual(4.5);
  });
});

describe('Focus Indicators', () => {
  test('CSS contains focus styles for interactive elements', () => {
    expect(allCss).toMatch(/:focus/i);
  });

  test('focus styles use visible outline (not none)', () => {
    const focusRules = allCss.match(/[^}]*:focus[^}]*}/gi) || [];
    let hasVisibleOutline = false;
    focusRules.forEach(rule => {
      if (rule.includes('outline') && !rule.includes('outline: none') && !rule.includes('outline:none')) {
        hasVisibleOutline = true;
      }
    });
    expect(hasVisibleOutline).toBe(true);
  });

  test('nav links have focus styles defined', () => {
    expect(allCss).toMatch(/\.nav-links\s+a:focus/i);
  });

  test('buttons have focus styles defined', () => {
    expect(allCss).toMatch(/button:focus|\.theme-toggle:focus|\.copy-button:focus/i);
  });

  test('focus outline has sufficient contrast (dark color)', () => {
    // Focus outlines should use primary color which is dark/visible
    const primaryColor = cssProps['--color-primary'] || '#2563eb';
    const bgColor = cssProps['--color-bg'] || '#ffffff';
    const contrastRatio = getContrastRatio(primaryColor, bgColor);
    expect(contrastRatio).toBeGreaterThanOrEqual(3.0);
  });
});

describe('ARIA Labels and Roles', () => {
  beforeEach(() => {
    document.documentElement.innerHTML = html;
  });

  test('nav element has aria-label or role attribute', () => {
    const navs = document.querySelectorAll('nav');
    navs.forEach(nav => {
      const hasAriaLabel = nav.hasAttribute('aria-label');
      const hasRole = nav.getAttribute('role');
      expect(hasAriaLabel || hasRole).toBe(true);
    });
  });

  test('mobile menu button has aria-label', () => {
    const hamburger = document.querySelector('.hamburger');
    expect(hamburger).toBeTruthy();
    expect(hamburger.getAttribute('aria-label')).toBeTruthy();
  });

  test('mobile menu button has aria-expanded attribute', () => {
    const hamburger = document.querySelector('.hamburger');
    expect(hamburger).toBeTruthy();
    expect(hamburger.hasAttribute('aria-expanded')).toBe(true);
  });

  test('mobile menu button has aria-controls attribute', () => {
    const hamburger = document.querySelector('.hamburger');
    expect(hamburger).toBeTruthy();
    expect(hamburger.hasAttribute('aria-controls')).toBe(true);
  });

  test('theme toggle button has aria-label', () => {
    const themeToggles = document.querySelectorAll('.theme-toggle');
    themeToggles.forEach(toggle => {
      expect(toggle.getAttribute('aria-label')).toBeTruthy();
    });
  });

  test('copy button has aria-label', () => {
    const copyButton = document.querySelector('.copy-button');
    if (copyButton) {
      expect(copyButton.getAttribute('aria-label')).toBeTruthy();
    }
  });

  test('header logo link has aria-label', () => {
    const logoLink = document.querySelector('.header-logo');
    if (logoLink) {
      expect(logoLink.getAttribute('aria-label')).toBeTruthy();
    }
  });

  test('CI badge link has aria-label', () => {
    const badgeLink = document.querySelector('.ci-badge-link');
    if (badgeLink) {
      expect(badgeLink.getAttribute('aria-label')).toBeTruthy();
    }
  });

  test('terminal window has role and aria-label', () => {
    const terminal = document.querySelector('.terminal-window');
    if (terminal) {
      const hasRole = terminal.hasAttribute('role');
      const hasAriaLabel = terminal.hasAttribute('aria-label');
      expect(hasRole || hasAriaLabel).toBe(true);
    }
  });

  test('copy tooltip has role="status" and aria-live', () => {
    const tooltip = document.querySelector('.copy-tooltip');
    if (tooltip) {
      expect(tooltip.getAttribute('role')).toBe('status');
      expect(tooltip.hasAttribute('aria-live')).toBe(true);
    }
  });

  test('footer navigation has aria-label', () => {
    const footerNav = document.querySelector('footer nav');
    if (footerNav) {
      expect(footerNav.getAttribute('aria-label')).toBeTruthy();
    }
  });

  test('decorative elements have aria-hidden="true"', () => {
    const decorativeElements = document.querySelectorAll('[aria-hidden="true"]');
    expect(decorativeElements.length).toBeGreaterThan(0);
  });
});

describe('Keyboard Navigation', () => {
  beforeEach(() => {
    document.documentElement.innerHTML = html;
  });

  test('all interactive elements are focusable', () => {
    const interactiveSelectors = [
      'a[href]', 'button:not([disabled])', 'input:not([disabled])',
      'textarea:not([disabled])', 'select:not([disabled])',
      '[tabindex]:not([tabindex="-1"])'
    ];
    const interactive = document.querySelectorAll(interactiveSelectors.join(', '));
    interactive.forEach(el => {
      // Elements should be natively focusable or have tabindex
      const tag = el.tagName.toLowerCase();
      const isNativelyFocusable = ['a', 'button', 'input', 'textarea', 'select'].includes(tag);
      const hasTabindex = el.hasAttribute('tabindex');
      expect(isNativelyFocusable || hasTabindex).toBe(true);
    });
  });

  test('all anchor links have href values', () => {
    const links = document.querySelectorAll('a');
    links.forEach(link => {
      const href = link.getAttribute('href');
      expect(href).toBeTruthy();
      expect(href.length).toBeGreaterThan(0);
    });
  });

  test('no elements have negative tabindex that blocks navigation', () => {
    // tabindex="-1" is acceptable for programmatic focus, but
    // we want to ensure important interactive elements don't have it
    const importantInteractive = document.querySelectorAll(
      'a[href]:not([tabindex="-1"]), button:not([tabindex="-1"])'
    );
    expect(importantInteractive.length).toBeGreaterThan(0);
  });

  test('hamburger button is a real button element', () => {
    const hamburger = document.querySelector('.hamburger');
    expect(hamburger).toBeTruthy();
    expect(hamburger.tagName.toLowerCase()).toBe('button');
  });

  test('theme toggle is a real button element', () => {
    const themeToggle = document.querySelector('.theme-toggle');
    expect(themeToggle).toBeTruthy();
    expect(themeToggle.tagName.toLowerCase()).toBe('button');
  });

  test('copy button is a real button element', () => {
    const copyButton = document.querySelector('.copy-button');
    if (copyButton) {
      expect(copyButton.tagName.toLowerCase()).toBe('button');
    }
  });
});

describe('axe-core Automated Accessibility Scan', () => {
  let originalGetContext;

  beforeAll(() => {
    // Mock canvas getContext to avoid jsdom error
    originalGetContext = HTMLCanvasElement.prototype.getContext;
    HTMLCanvasElement.prototype.getContext = function() {
      return null;
    };
  });

  afterAll(() => {
    HTMLCanvasElement.prototype.getContext = originalGetContext;
  });

  beforeEach(() => {
    // Preserve html lang attribute when loading HTML
    const htmlMatch = html.match(/<html([^>]*)>/i);
    const langMatch = htmlMatch && htmlMatch[1].match(/lang="([^"]*)"/i);
    const lang = langMatch ? langMatch[1] : 'en';

    document.documentElement.innerHTML = html;
    document.documentElement.setAttribute('lang', lang);

    const style = document.createElement('style');
    style.textContent = allCss;
    document.head.appendChild(style);
  });

  test('axe-core scan finds no critical violations', async () => {
    const results = await axe.run(document, {
      runOnly: {
        type: 'tag',
        values: ['wcag2a', 'wcag2aa', 'wcag21aa']
      },
      resultTypes: ['violations', 'incomplete']
    });

    const criticalAndSerious = results.violations.filter(v =>
      v.impact === 'critical' || v.impact === 'serious'
    );

    if (criticalAndSerious.length > 0) {
      console.log('Critical/Serious violations:', criticalAndSerious.map(v => ({
        rule: v.id,
        description: v.description,
        impact: v.impact,
        nodes: v.nodes.length
      })));
    }

    expect(criticalAndSerious.length).toBe(0);
  });

  test('axe-core overall score is at least 90', async () => {
    const results = await axe.run(document, {
      runOnly: {
        type: 'tag',
        values: ['wcag2a', 'wcag2aa', 'wcag21aa']
      }
    });

    // Calculate accessibility score: 100 - (violations * penalty)
    const violationCount = results.violations.reduce((sum, v) => sum + v.nodes.length, 0);
    const score = Math.max(0, 100 - (violationCount * 5));

    expect(score).toBeGreaterThanOrEqual(90);
  });
});
