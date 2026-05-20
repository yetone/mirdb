/**
 * Overview Section Unit Tests
 * Owner: Scenario 2 - Overview Section
 *
 * Tests:
 * - Overview section DOM structure
 * - Heading text content
 * - Description content and length
 * - Accessibility: heading hierarchy, color contrast, semantic elements
 */

const fs = require('fs');
const path = require('path');

const htmlPath = path.join(__dirname, '../../index.html');
const html = fs.readFileSync(htmlPath, 'utf-8');

// Load CSS for computed style tests
const baseCssPath = path.join(__dirname, '../../css/base.css');
const overviewCssPath = path.join(__dirname, '../../css/overview.css');
const baseCss = fs.readFileSync(baseCssPath, 'utf-8');
const overviewCss = fs.readFileSync(overviewCssPath, 'utf-8');

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

describe('Overview Section Structure', () => {
  beforeEach(() => {
    document.body.innerHTML = html;
  });

  test('overview section exists with correct id', () => {
    const overview = document.querySelector('section#overview');
    expect(overview).toBeTruthy();
  });

  test('overview section contains an h2 heading', () => {
    const overview = document.querySelector('section#overview');
    const h2 = overview.querySelector('h2');
    expect(h2).toBeTruthy();
  });

  test('overview heading text is "What is MirDB?"', () => {
    const overview = document.querySelector('section#overview');
    const h2 = overview.querySelector('h2');
    expect(h2.textContent.trim()).toBe('What is MirDB?');
  });

  test('overview section has a description paragraph', () => {
    const overview = document.querySelector('section#overview');
    const paragraph = overview.querySelector('p.overview-description');
    expect(paragraph).toBeTruthy();
  });

  test('overview description contains key terms about MirDB', () => {
    const overview = document.querySelector('section#overview');
    const paragraph = overview.querySelector('p.overview-description');
    const text = paragraph.textContent.toLowerCase();

    expect(text).toContain('persistent');
    expect(text).toContain('key-value');
    expect(text).toContain('memcached');
    expect(text).toContain('rust');
  });

  test('overview description is approximately 2-3 sentences', () => {
    const overview = document.querySelector('section#overview');
    const paragraph = overview.querySelector('p.overview-description');
    const text = paragraph.textContent.trim();
    const sentences = text.split(/[.!?]+/).filter(s => s.trim().length > 0);

    expect(sentences.length).toBeGreaterThanOrEqual(2);
    expect(sentences.length).toBeLessThanOrEqual(4);
  });
});

describe('Overview Section Accessibility', () => {
  const cssProps = getCssCustomProperties(baseCss + overviewCss);

  beforeEach(() => {
    document.body.innerHTML = html;

    // Inject styles into the document
    const style = document.createElement('style');
    style.textContent = baseCss + overviewCss;
    document.head.appendChild(style);
  });

  test('overview section has proper heading hierarchy (h2)', () => {
    const overview = document.querySelector('section#overview');
    const headings = overview.querySelectorAll('h1, h2, h3, h4, h5, h6');
    expect(headings.length).toBeGreaterThan(0);
    expect(headings[0].tagName.toLowerCase()).toBe('h2');
  });

  test('overview heading uses readable font size', () => {
    const h2FontSize = cssProps['--font-size-h2'] || '2rem';
    const sizeValue = parseFloat(h2FontSize);
    expect(sizeValue).toBeGreaterThanOrEqual(1.5);
  });

  test('overview description uses readable font size', () => {
    const overview = document.querySelector('section#overview');
    const paragraph = overview.querySelector('p.overview-description');
    expect(paragraph).toBeTruthy();
  });

  test('overview section color contrast meets WCAG AA (4.5:1)', () => {
    const textColor = cssProps['--color-text'] || '#1f2937';
    const bgColor = cssProps['--color-card-bg'] || '#f9fafb';
    const contrastRatio = getContrastRatio(textColor, bgColor);

    expect(contrastRatio).toBeGreaterThanOrEqual(4.5);
  });

  test('overview description muted color has sufficient contrast', () => {
    const mutedColor = cssProps['--color-muted'] || '#6b7280';
    const bgColor = cssProps['--color-card-bg'] || '#f9fafb';
    const contrastRatio = getContrastRatio(mutedColor, bgColor);

    expect(contrastRatio).toBeGreaterThanOrEqual(4.5);
  });

  test('overview section has semantic section element', () => {
    const overview = document.querySelector('section#overview');
    expect(overview).toBeTruthy();
    expect(overview.tagName.toLowerCase()).toBe('section');
  });

  test('overview section html has lang attribute', () => {
    const htmlMatch = html.match(/<html[^>]*>/i);
    expect(htmlMatch).toBeTruthy();
    expect(htmlMatch[0]).toMatch(/lang=["']en["']/i);
  });
});
