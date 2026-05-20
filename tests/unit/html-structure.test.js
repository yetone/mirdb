/**
 * HTML Structure Unit Tests
 * Owner: Scenario 9 - Accessibility and SEO
 *
 * Tests:
 * - Heading hierarchy (single h1, no skips)
 * - Section landmarks (header, main, footer, nav)
 * - Image alt attributes
 * - Link href validity
 * - Required DOM elements exist
 */

const fs = require('fs');
const path = require('path');

const htmlPath = path.join(__dirname, '../../index.html');
const html = fs.readFileSync(htmlPath, 'utf-8');

describe('HTML Document Structure', () => {
  beforeEach(() => {
    document.documentElement.innerHTML = html;
  });

  test('document has DOCTYPE declaration', () => {
    expect(html.toLowerCase().includes('<!doctype html>')).toBe(true);
  });

  test('html element has lang attribute set to "en"', () => {
    expect(html.toLowerCase().includes('<html lang="en"')).toBe(true);
  });

  test('document has a head element', () => {
    const head = document.querySelector('head');
    expect(head).toBeTruthy();
  });

  test('document has a body element', () => {
    const body = document.querySelector('body');
    expect(body).toBeTruthy();
  });

  test('charset meta tag is present', () => {
    const charset = document.querySelector('meta[charset]');
    expect(charset).toBeTruthy();
    expect(charset.getAttribute('charset').toLowerCase()).toBe('utf-8');
  });

  test('viewport meta tag is present', () => {
    const viewport = document.querySelector('meta[name="viewport"]');
    expect(viewport).toBeTruthy();
  });
});

describe('Landmark Elements', () => {
  beforeEach(() => {
    document.documentElement.innerHTML = html;
  });

  test('header element exists', () => {
    const header = document.querySelector('header');
    expect(header).toBeTruthy();
  });

  test('main element exists', () => {
    const main = document.querySelector('main');
    expect(main).toBeTruthy();
  });

  test('footer element exists', () => {
    const footer = document.querySelector('footer');
    expect(footer).toBeTruthy();
  });

  test('at least one nav element exists', () => {
    const navs = document.querySelectorAll('nav');
    expect(navs.length).toBeGreaterThanOrEqual(1);
  });

  test('header contains a nav element', () => {
    const header = document.querySelector('header');
    const navInHeader = header.querySelector('nav');
    expect(navInHeader).toBeTruthy();
  });
});

describe('Heading Hierarchy', () => {
  beforeEach(() => {
    document.documentElement.innerHTML = html;
  });

  test('page has exactly one h1 element', () => {
    const h1s = document.querySelectorAll('h1');
    expect(h1s.length).toBe(1);
  });

  test('h1 is inside main content', () => {
    const h1 = document.querySelector('h1');
    const main = document.querySelector('main');
    expect(main.contains(h1)).toBe(true);
  });

  test('no heading level skips (h1 -> h3 without h2)', () => {
    const allHeadings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
    const levels = Array.from(allHeadings).map(h => parseInt(h.tagName[1], 10));

    for (let i = 1; i < levels.length; i++) {
      const prev = levels[i - 1];
      const curr = levels[i];
      // Current level should not be more than 1 greater than previous
      expect(curr).toBeLessThanOrEqual(prev + 1);
    }
  });

  test('headings do not skip levels going down', () => {
    // This is a more strict check: after h1, next heading must be h2
    // after h2, next must be h2 or h3, etc.
    const headings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
    const tagNames = Array.from(headings).map(h => h.tagName.toLowerCase());

    const levelMap = { h1: 1, h2: 2, h3: 3, h4: 4, h5: 5, h6: 6 };
    for (let i = 1; i < tagNames.length; i++) {
      const prevLevel = levelMap[tagNames[i - 1]];
      const currLevel = levelMap[tagNames[i]];
      // Can stay same, go down by 1, or go up any amount
      // But should NOT jump down by more than 1
      if (currLevel > prevLevel) {
        expect(currLevel).toBe(prevLevel + 1);
      }
    }
  });
});

describe('Image Alt Attributes', () => {
  beforeEach(() => {
    document.documentElement.innerHTML = html;
  });

  test('all img elements have alt attributes', () => {
    const images = document.querySelectorAll('img');
    images.forEach(img => {
      expect(img.hasAttribute('alt')).toBe(true);
    });
  });

  test('logo image has descriptive alt text', () => {
    const logoImages = document.querySelectorAll('img[src*="logo"]');
    logoImages.forEach(img => {
      const alt = img.getAttribute('alt');
      expect(alt).toBeTruthy();
      expect(alt.length).toBeGreaterThan(0);
    });
  });

  test('hero logo has alt text "MirDB Logo"', () => {
    const heroLogo = document.querySelector('section#hero img.hero-logo');
    if (heroLogo) {
      expect(heroLogo.getAttribute('alt')).toBe('MirDB Logo');
    }
  });

  test('header logo image has alt text', () => {
    const headerLogo = document.querySelector('.header-logo img');
    if (headerLogo) {
      const alt = headerLogo.getAttribute('alt');
      expect(alt).toBeTruthy();
      expect(alt.length).toBeGreaterThan(0);
    }
  });

  test('CI badge image has alt text', () => {
    const ciBadge = document.querySelector('.ci-badge');
    if (ciBadge) {
      const alt = ciBadge.getAttribute('alt');
      expect(alt).toBeTruthy();
      expect(alt.length).toBeGreaterThan(0);
    }
  });

  test('decorative SVGs inside buttons have aria-hidden or role=presentation', () => {
    const svgsInButtons = document.querySelectorAll('button svg, button [role="img"]');
    svgsInButtons.forEach(svg => {
      const ariaHidden = svg.getAttribute('aria-hidden');
      const role = svg.getAttribute('role');
      expect(ariaHidden === 'true' || role === 'presentation' || svg.hasAttribute('aria-label')).toBe(true);
    });
  });

  test('decorative SVGs inside links have aria-hidden or role=presentation', () => {
    const svgsInLinks = document.querySelectorAll('a > svg');
    svgsInLinks.forEach(svg => {
      const ariaHidden = svg.getAttribute('aria-hidden');
      const role = svg.getAttribute('role');
      expect(ariaHidden === 'true' || role === 'presentation').toBe(true);
    });
  });
});

describe('Interactive Elements', () => {
  beforeEach(() => {
    document.documentElement.innerHTML = html;
  });

  test('all anchor tags have href attributes', () => {
    const links = document.querySelectorAll('a');
    links.forEach(link => {
      expect(link.hasAttribute('href')).toBe(true);
      const href = link.getAttribute('href');
      expect(href).toBeTruthy();
    });
  });

  test('external links have rel attribute containing noopener', () => {
    const externalLinks = document.querySelectorAll('a[target="_blank"]');
    externalLinks.forEach(link => {
      const rel = link.getAttribute('rel') || '';
      expect(rel).toContain('noopener');
    });
  });

  test('all buttons have type attribute', () => {
    const buttons = document.querySelectorAll('button');
    buttons.forEach(button => {
      expect(button.hasAttribute('type')).toBe(true);
    });
  });

  test('copy button has data-copy-target attribute', () => {
    const copyButton = document.querySelector('.copy-button');
    if (copyButton) {
      expect(copyButton.hasAttribute('data-copy-target')).toBe(true);
    }
  });
});

describe('Required Sections', () => {
  beforeEach(() => {
    document.documentElement.innerHTML = html;
  });

  test('hero section exists', () => {
    expect(document.querySelector('section#hero')).toBeTruthy();
  });

  test('overview section exists', () => {
    expect(document.querySelector('section#overview')).toBeTruthy();
  });

  test('features section exists', () => {
    expect(document.querySelector('section#features')).toBeTruthy();
  });

  test('quickstart section exists', () => {
    expect(document.querySelector('section#quickstart')).toBeTruthy();
  });

  test('roadmap section exists', () => {
    expect(document.querySelector('section#roadmap')).toBeTruthy();
  });
});
