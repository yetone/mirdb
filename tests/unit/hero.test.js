/**
 * Hero Section Unit Tests
 * Owner: Scenario 1 - Hero Section
 *
 * Tests:
 * - Hero section DOM structure
 * - Logo image src and alt attributes
 * - Tagline text content
 * - CTA buttons presence, text, and hrefs
 * - Subtext content
 * - Background gradient/effect styling
 * - Accessibility checks
 */

const fs = require('fs');
const path = require('path');

const htmlPath = path.join(__dirname, '../../index.html');
const html = fs.readFileSync(htmlPath, 'utf-8');

const baseCssPath = path.join(__dirname, '../../css/base.css');
const heroCssPath = path.join(__dirname, '../../css/hero.css');
const baseCss = fs.readFileSync(baseCssPath, 'utf-8');
const heroCss = fs.readFileSync(heroCssPath, 'utf-8');

// Parse CSS custom properties for color values
function getCssCustomProperties(cssText) {
  const props = {};
  const regex = /--([\w-]+):\s*([^;]+);/g;
  let match;
  while ((match = regex.exec(cssText)) !== null) {
    props[`--${match[1]}`] = match[2].trim();
  }
  return props;
}

// Simple hex/rgb to luminance for contrast calculation
function getLuminance(color) {
  let r, g, b;

  if (color.startsWith('#')) {
    const hex = color.replace('#', '');
    r = parseInt(hex.substr(0, 2), 16) / 255;
    g = parseInt(hex.substr(2, 2), 16) / 255;
    b = parseInt(hex.substr(4, 2), 16) / 255;
  } else if (color.startsWith('rgb')) {
    const match = color.match(/\d+/g);
    r = parseInt(match[0]) / 255;
    g = parseInt(match[1]) / 255;
    b = parseInt(match[2]) / 255;
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

describe('Hero Section Structure', () => {
  beforeEach(() => {
    document.body.innerHTML = html;
  });

  test('hero section exists with correct id', () => {
    const hero = document.querySelector('section#hero');
    expect(hero).toBeTruthy();
  });

  test('hero section is the first section in main', () => {
    const main = document.querySelector('main');
    const firstSection = main.querySelector(':scope > section');
    expect(firstSection.id).toBe('hero');
  });

  test('hero section contains product logo image', () => {
    const hero = document.querySelector('section#hero');
    const logo = hero.querySelector('img.hero-logo');
    expect(logo).toBeTruthy();
  });

  test('logo references assets/logo.gif or equivalent path', () => {
    const hero = document.querySelector('section#hero');
    const logo = hero.querySelector('img.hero-logo');
    const src = logo.getAttribute('src');
    expect(src).toMatch(/assets\/logo\.gif|assets\/logo/i);
  });

  test('logo has alt text "MirDB Logo"', () => {
    const hero = document.querySelector('section#hero');
    const logo = hero.querySelector('img.hero-logo');
    expect(logo.getAttribute('alt')).toBe('MirDB Logo');
  });

  test('hero section contains primary tagline h1', () => {
    const hero = document.querySelector('section#hero');
    const tagline = hero.querySelector('h1.hero-tagline');
    expect(tagline).toBeTruthy();
  });

  test('tagline text is "A Persistent Key-Value Store with Memcached Protocol"', () => {
    const hero = document.querySelector('section#hero');
    const tagline = hero.querySelector('h1.hero-tagline');
    expect(tagline.textContent.trim()).toBe('A Persistent Key-Value Store with Memcached Protocol');
  });

  test('hero section contains subtext', () => {
    const hero = document.querySelector('section#hero');
    const subtext = hero.querySelector('p.hero-subtext');
    expect(subtext).toBeTruthy();
  });

  test('subtext reads "It is painless as using memcached"', () => {
    const hero = document.querySelector('section#hero');
    const subtext = hero.querySelector('p.hero-subtext');
    expect(subtext.textContent.trim()).toBe('It is painless as using memcached');
  });

  test('hero section contains CTA button group', () => {
    const hero = document.querySelector('section#hero');
    const ctaGroup = hero.querySelector('.hero-cta-group');
    expect(ctaGroup).toBeTruthy();
  });

  test('primary CTA button "View on GitHub" exists', () => {
    const hero = document.querySelector('section#hero');
    const primaryCta = hero.querySelector('.hero-cta-primary');
    expect(primaryCta).toBeTruthy();
    expect(primaryCta.textContent.trim()).toBe('View on GitHub');
  });

  test('secondary CTA button "Get Started" exists', () => {
    const hero = document.querySelector('section#hero');
    const secondaryCta = hero.querySelector('.hero-cta-secondary');
    expect(secondaryCta).toBeTruthy();
    expect(secondaryCta.textContent.trim()).toBe('Get Started');
  });

  test('primary CTA links to GitHub repository', () => {
    const hero = document.querySelector('section#hero');
    const primaryCta = hero.querySelector('.hero-cta-primary');
    expect(primaryCta.getAttribute('href')).toBe('https://github.com/yetone/mirdb');
  });

  test('primary CTA opens in new tab with security attributes', () => {
    const hero = document.querySelector('section#hero');
    const primaryCta = hero.querySelector('.hero-cta-primary');
    expect(primaryCta.getAttribute('target')).toBe('_blank');
    expect(primaryCta.getAttribute('rel')).toContain('noopener');
    expect(primaryCta.getAttribute('rel')).toContain('noreferrer');
  });

  test('secondary CTA links to quickstart section', () => {
    const hero = document.querySelector('section#hero');
    const secondaryCta = hero.querySelector('.hero-cta-secondary');
    expect(secondaryCta.getAttribute('href')).toBe('#quickstart');
  });
});

describe('Hero Section Background Styling', () => {
  beforeEach(() => {
    document.body.innerHTML = html;
    const style = document.createElement('style');
    style.textContent = baseCss + heroCss;
    document.head.appendChild(style);
  });

  test('hero section has a background element', () => {
    const hero = document.querySelector('section#hero');
    const bg = hero.querySelector('.hero-background');
    expect(bg).toBeTruthy();
  });

  test('hero CSS contains gradient background definition', () => {
    expect(heroCss).toMatch(/linear-gradient/i);
  });

  test('hero section has minimum full viewport height', () => {
    expect(heroCss).toMatch(/min-height:\s*100vh/i);
  });

  test('hero section uses flexbox for centering', () => {
    expect(heroCss).toMatch(/display:\s*flex/i);
    expect(heroCss).toMatch(/align-items:\s*center/i);
    expect(heroCss).toMatch(/justify-content:\s*center/i);
  });

  test('hero section has text-align center', () => {
    expect(heroCss).toMatch(/text-align:\s*center/i);
  });

  test('hero CSS contains radial glow decoration', () => {
    expect(heroCss).toMatch(/radial-gradient/i);
  });
});

describe('Hero Section Accessibility', () => {
  const cssProps = getCssCustomProperties(baseCss + heroCss);

  beforeEach(() => {
    document.body.innerHTML = html;
    const style = document.createElement('style');
    style.textContent = baseCss + heroCss;
    document.head.appendChild(style);
  });

  test('hero section has a single h1 element', () => {
    const hero = document.querySelector('section#hero');
    const h1s = hero.querySelectorAll('h1');
    expect(h1s.length).toBe(1);
  });

  test('hero logo has alt text', () => {
    const hero = document.querySelector('section#hero');
    const logo = hero.querySelector('img');
    expect(logo.getAttribute('alt')).toBeTruthy();
    expect(logo.getAttribute('alt').length).toBeGreaterThan(0);
  });

  test('CTA buttons have focus styles defined', () => {
    expect(heroCss).toMatch(/:focus/i);
  });

  test('primary CTA color contrast meets WCAG AA (4.5:1)', () => {
    const primaryBg = cssProps['--color-primary'] || '#2563eb';
    const white = '#ffffff';
    const contrastRatio = getContrastRatio(primaryBg, white);
    expect(contrastRatio).toBeGreaterThanOrEqual(4.5);
  });

  test('hero tagline uses high-contrast text color', () => {
    const textColor = cssProps['--color-text'] || '#1f2937';
    const bgColor = '#f0f9ff';
    const contrastRatio = getContrastRatio(textColor, bgColor);
    expect(contrastRatio).toBeGreaterThanOrEqual(4.5);
  });

  test('hero section uses semantic section element', () => {
    const hero = document.querySelector('section#hero');
    expect(hero.tagName.toLowerCase()).toBe('section');
  });
});
