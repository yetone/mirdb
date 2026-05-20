/**
 * Footer Unit Tests
 * Owner: Scenario 7 - Footer
 *
 * Tests:
 * - Footer DOM structure
 * - Copyright text content with year
 * - License information
 * - Footer links (GitHub, docs)
 * - Accessibility: semantic footer element, ARIA labels
 * - Footer styling: visual separation, consistent colors
 */

const fs = require('fs');
const path = require('path');

const htmlPath = path.join(__dirname, '../../index.html');
const html = fs.readFileSync(htmlPath, 'utf-8');

// Load CSS for computed style tests
const baseCssPath = path.join(__dirname, '../../css/base.css');
const footerCssPath = path.join(__dirname, '../../css/footer.css');
const baseCss = fs.readFileSync(baseCssPath, 'utf-8');
const footerCss = fs.readFileSync(footerCssPath, 'utf-8');

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

describe('Footer Structure', () => {
  beforeEach(() => {
    document.body.innerHTML = html;
  });

  test('footer element exists with semantic tag', () => {
    const footer = document.querySelector('footer');
    expect(footer).toBeTruthy();
  });

  test('footer has site-footer class', () => {
    const footer = document.querySelector('footer');
    expect(footer.classList.contains('site-footer')).toBe(true);
  });

  test('footer contains a container div', () => {
    const footer = document.querySelector('footer');
    const container = footer.querySelector('.container');
    expect(container).toBeTruthy();
  });

  test('footer content wrapper exists', () => {
    const footer = document.querySelector('footer');
    const content = footer.querySelector('.footer-content');
    expect(content).toBeTruthy();
  });
});

describe('Footer Copyright', () => {
  beforeEach(() => {
    document.body.innerHTML = html;
  });

  test('footer contains copyright text', () => {
    const footer = document.querySelector('footer');
    const copyright = footer.querySelector('.footer-copyright');
    expect(copyright).toBeTruthy();
  });

  test('copyright contains current year', () => {
    const footer = document.querySelector('footer');
    const yearEl = footer.querySelector('#footer-year');
    expect(yearEl).toBeTruthy();

    const yearText = yearEl.textContent.trim();
    const currentYear = new Date().getFullYear().toString();
    expect(yearText).toBe(currentYear);
  });

  test('copyright contains copyright symbol', () => {
    const footer = document.querySelector('footer');
    const copyrightSymbol = footer.querySelector('.footer-copyright-symbol');
    expect(copyrightSymbol).toBeTruthy();
  });

  test('copyright text mentions MirDB Contributors', () => {
    const footer = document.querySelector('footer');
    const copyright = footer.querySelector('.footer-copyright');
    expect(copyright.textContent).toContain('MirDB Contributors');
  });
});

describe('Footer License', () => {
  beforeEach(() => {
    document.body.innerHTML = html;
  });

  test('footer contains license information', () => {
    const footer = document.querySelector('footer');
    const license = footer.querySelector('.footer-license');
    expect(license).toBeTruthy();
  });

  test('license mentions MIT License', () => {
    const footer = document.querySelector('footer');
    const license = footer.querySelector('.footer-license');
    expect(license.textContent).toContain('MIT License');
  });

  test('license has link to LICENSE file', () => {
    const footer = document.querySelector('footer');
    const licenseLink = footer.querySelector('.footer-license a');
    expect(licenseLink).toBeTruthy();

    const href = licenseLink.getAttribute('href');
    expect(href).toContain('LICENSE');
  });
});

describe('Footer Links', () => {
  beforeEach(() => {
    document.body.innerHTML = html;
  });

  test('footer contains navigation links', () => {
    const footer = document.querySelector('footer');
    const nav = footer.querySelector('.footer-links');
    expect(nav).toBeTruthy();
  });

  test('footer navigation has aria-label', () => {
    const footer = document.querySelector('footer');
    const nav = footer.querySelector('nav[aria-label]');
    expect(nav).toBeTruthy();
    expect(nav.getAttribute('aria-label')).toBe('Footer navigation');
  });

  test('footer contains GitHub link', () => {
    const footer = document.querySelector('footer');
    const links = footer.querySelectorAll('.footer-link-list a');
    const githubLink = Array.from(links).find(a => a.textContent.includes('GitHub'));
    expect(githubLink).toBeTruthy();
  });

  test('GitHub link points to correct URL', () => {
    const footer = document.querySelector('footer');
    const links = footer.querySelectorAll('.footer-link-list a');
    const githubLink = Array.from(links).find(a => a.textContent.includes('GitHub'));
    expect(githubLink.getAttribute('href')).toBe('https://github.com/yetone/mirdb');
  });

  test('GitHub link opens in new tab', () => {
    const footer = document.querySelector('footer');
    const links = footer.querySelectorAll('.footer-link-list a');
    const githubLink = Array.from(links).find(a => a.textContent.includes('GitHub'));
    expect(githubLink.getAttribute('target')).toBe('_blank');
    expect(githubLink.getAttribute('rel')).toContain('noopener');
  });

  test('footer contains Documentation link', () => {
    const footer = document.querySelector('footer');
    const links = footer.querySelectorAll('.footer-link-list a');
    const docsLink = Array.from(links).find(a => a.textContent.includes('Documentation'));
    expect(docsLink).toBeTruthy();
  });

  test('Documentation link points to docs section', () => {
    const footer = document.querySelector('footer');
    const links = footer.querySelectorAll('.footer-link-list a');
    const docsLink = Array.from(links).find(a => a.textContent.includes('Documentation'));
    expect(docsLink.getAttribute('href')).toContain('documentation');
  });

  test('Documentation link opens in new tab', () => {
    const footer = document.querySelector('footer');
    const links = footer.querySelectorAll('.footer-link-list a');
    const docsLink = Array.from(links).find(a => a.textContent.includes('Documentation'));
    expect(docsLink.getAttribute('target')).toBe('_blank');
    expect(docsLink.getAttribute('rel')).toContain('noopener');
  });

  test('all external footer links have rel="noopener noreferrer"', () => {
    const footer = document.querySelector('footer');
    const links = footer.querySelectorAll('a[target="_blank"]');
    links.forEach(link => {
      const rel = link.getAttribute('rel') || '';
      expect(rel).toContain('noopener');
      expect(rel).toContain('noreferrer');
    });
  });
});

describe('Footer Accessibility', () => {
  beforeEach(() => {
    document.body.innerHTML = html;
  });

  test('footer is a semantic footer element', () => {
    const footer = document.querySelector('footer');
    expect(footer.tagName.toLowerCase()).toBe('footer');
  });

  test('footer links use semantic nav element', () => {
    const footer = document.querySelector('footer');
    const nav = footer.querySelector('nav');
    expect(nav).toBeTruthy();
  });

  test('footer links use ul/li list structure', () => {
    const footer = document.querySelector('footer');
    const list = footer.querySelector('.footer-link-list');
    expect(list).toBeTruthy();
    expect(list.tagName.toLowerCase()).toBe('ul');

    const items = list.querySelectorAll('li');
    expect(items.length).toBeGreaterThanOrEqual(2);
  });

  test('footer link icons have aria-hidden', () => {
    const footer = document.querySelector('footer');
    const icons = footer.querySelectorAll('.footer-link-icon');
    icons.forEach(icon => {
      expect(icon.getAttribute('aria-hidden')).toBe('true');
    });
  });
});

describe('Footer Styling', () => {
  const cssProps = getCssCustomProperties(baseCss + footerCss);

  beforeEach(() => {
    document.body.innerHTML = html;

    // Inject styles into the document
    const style = document.createElement('style');
    style.textContent = baseCss + footerCss;
    document.head.appendChild(style);
  });

  test('footer has visual separation via border-top', () => {
    const footerStyle = footerCss.match(/\.site-footer\s*\{[^}]*\}/s);
    expect(footerStyle).toBeTruthy();
    expect(footerStyle[0]).toContain('border-top');
  });

  test('footer has distinct background color', () => {
    const footerStyle = footerCss.match(/\.site-footer\s*\{[^}]*\}/s);
    expect(footerStyle).toBeTruthy();
    expect(footerStyle[0]).toContain('background-color');
  });

  test('footer background uses card-bg or similar muted color', () => {
    const footerStyle = footerCss.match(/\.site-footer\s*\{[^}]*\}/s);
    expect(footerStyle[0]).toContain('--color-card-bg');
  });

  test('footer uses consistent muted text color', () => {
    const footerCopyright = footerCss.match(/\.footer-copyright\s*p\s*\{[^}]*\}/s);
    expect(footerCopyright).toBeTruthy();
    expect(footerCopyright[0]).toContain('--color-muted');
  });

  test('footer link hover color uses primary color', () => {
    const footerLinkHover = footerCss.match(/\.footer-link-list\s*a:hover\s*\{[^}]*\}/s);
    expect(footerLinkHover).toBeTruthy();
    expect(footerLinkHover[0]).toContain('--color-primary');
  });

  test('footer copyright color contrast meets WCAG AA (4.5:1)', () => {
    const textColor = cssProps['--color-muted'] || '#6b7280';
    const bgColor = cssProps['--color-card-bg'] || '#f9fafb';
    const contrastRatio = getContrastRatio(textColor, bgColor);

    expect(contrastRatio).toBeGreaterThanOrEqual(4.5);
  });

  test('footer has responsive styles defined', () => {
    const hasMediaQuery = footerCss.includes('@media');
    expect(hasMediaQuery).toBe(true);
  });
});
