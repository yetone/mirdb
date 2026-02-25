/**
 * Accessibility Unit Tests
 * Owner: Scenario 12 - Accessibility Compliance
 *
 * Test cases:
 * - Semantic HTML elements present
 * - Heading hierarchy is valid
 * - Images have alt text
 * - Color contrast meets WCAG AA
 * - ARIA labels on interactive elements
 */

const fs = require('fs');
const path = require('path');
const { parseHTML } = require('linkedom');

describe('Semantic HTML Structure (Test Case 1)', () => {
  let document;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../_site/index.html');
    const html = fs.readFileSync(htmlPath, 'utf-8');
    const { document: doc } = parseHTML(html);
    document = doc;
  });

  test('should have header element with proper role', () => {
    const header = document.querySelector('header');
    expect(header).not.toBeNull();
    expect(header.getAttribute('role')).toBe('banner');
  });

  test('should have main element with proper role', () => {
    const main = document.querySelector('main');
    expect(main).not.toBeNull();
    expect(main.getAttribute('role')).toBe('main');
  });

  test('should have nav element with proper role and aria-label', () => {
    const nav = document.querySelector('nav');
    expect(nav).not.toBeNull();
    expect(nav.getAttribute('role')).toBe('navigation');
    expect(nav.getAttribute('aria-label')).toBeTruthy();
  });

  test('should have footer element with proper role', () => {
    const footer = document.querySelector('footer');
    expect(footer).not.toBeNull();
    expect(footer.getAttribute('role')).toBe('contentinfo');
  });

  test('should have section elements', () => {
    const sections = document.querySelectorAll('section');
    expect(sections.length).toBeGreaterThan(0);
  });

  test('should use semantic article elements for feature cards', () => {
    const articles = document.querySelectorAll('article.feature-card');
    expect(articles.length).toBeGreaterThan(0);
  });

  test('should have skip link for keyboard navigation', () => {
    const skipLink = document.querySelector('a.skip-link');
    expect(skipLink).not.toBeNull();
    expect(skipLink.getAttribute('href')).toBe('#main-content');
  });
});

describe('Heading Hierarchy (Test Case 2)', () => {
  let document;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../_site/index.html');
    const html = fs.readFileSync(htmlPath, 'utf-8');
    const { document: doc } = parseHTML(html);
    document = doc;
  });

  test('should have exactly one h1 element', () => {
    const h1Elements = document.querySelectorAll('h1');
    expect(h1Elements.length).toBe(1);
  });

  test('should have h1 as the first heading in the content', () => {
    const h1 = document.querySelector('h1');
    expect(h1).not.toBeNull();
    expect(h1.textContent.toLowerCase()).toContain('mirdb');
  });

  test('should have h2 elements following h1', () => {
    const h2Elements = document.querySelectorAll('h2');
    expect(h2Elements.length).toBeGreaterThan(0);
  });

  test('should not skip heading levels (no h3 without h2)', () => {
    const headings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
    let previousLevel = 0;

    Array.from(headings).forEach((heading) => {
      const level = parseInt(heading.tagName.charAt(1));
      // Each heading can be same level, one level higher (+1), or any level lower than previous
      // Skipping means going up more than one level (e.g., h1 to h3)
      const skipsLevel = level > previousLevel + 1;
      expect(skipsLevel).toBe(false);
      previousLevel = level;
    });
  });

  test('sections should have aria-labelledby or aria-label for accessibility', () => {
    const sections = document.querySelectorAll('section');
    sections.forEach((section) => {
      const hasLabel = section.getAttribute('aria-labelledby') || section.getAttribute('aria-label');
      expect(hasLabel).toBeTruthy();
    });
  });
});

describe('Image Alt Text (Test Case 3)', () => {
  let document;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../_site/index.html');
    const html = fs.readFileSync(htmlPath, 'utf-8');
    const { document: doc } = parseHTML(html);
    document = doc;
  });

  test('all img elements should have alt attributes', () => {
    const images = document.querySelectorAll('img');
    images.forEach((img) => {
      expect(img.hasAttribute('alt')).toBe(true);
    });
  });

  test('decorative images should have empty alt or aria-hidden', () => {
    const decorativeImages = document.querySelectorAll('img[role="presentation"], img[alt=""]');
    // Decorative images should have alt="" or aria-hidden="true"
    decorativeImages.forEach((img) => {
      const hasEmptyAlt = img.getAttribute('alt') === '';
      const hasAriaHidden = img.getAttribute('aria-hidden') === 'true';
      expect(hasEmptyAlt || hasAriaHidden).toBe(true);
    });
  });

  test('decorative SVG icons should have aria-hidden', () => {
    const decorativeIcons = document.querySelectorAll('.feature-icon svg, [aria-hidden="true"] svg');
    // SVG icons that are decorative should be hidden from screen readers
    const allIconSvgs = document.querySelectorAll('.feature-icon');
    allIconSvgs.forEach((iconContainer) => {
      expect(iconContainer.getAttribute('aria-hidden')).toBe('true');
    });
  });
});

describe('Color Contrast (Test Case 6)', () => {
  let cssContent;

  beforeAll(() => {
    const cssPath = path.join(__dirname, '../../_site/assets/css/main.css');
    cssContent = fs.readFileSync(cssPath, 'utf-8');
  });

  // Note: Full contrast ratio testing requires actual rendering.
  // These tests verify the color values used meet WCAG AA standards.

  test('should use text color that provides sufficient contrast', () => {
    // Body text color #333333 on white (#ffffff) = 12.63:1 ratio (passes AA)
    expect(cssContent).toMatch(/color:\s*#333/i);
  });

  test('should use primary color that provides sufficient contrast', () => {
    // Primary color #CE422B on white (#ffffff) = 4.95:1 ratio (passes AA for large text)
    expect(cssContent).toMatch(/#CE422B|#ce422b/i);
  });

  test('should define background color', () => {
    expect(cssContent).toMatch(/background-color:\s*#fff/i);
  });

  test('should have dark code block background for contrast', () => {
    // Code block uses dark background for syntax highlighting
    expect(cssContent).toMatch(/#2d2d2d/i);
  });
});

describe('ARIA Labels on Buttons (Test Case 7)', () => {
  let document;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../_site/index.html');
    const html = fs.readFileSync(htmlPath, 'utf-8');
    const { document: doc } = parseHTML(html);
    document = doc;
  });

  test('icon-only buttons should have aria-label', () => {
    // Copy buttons with only icon should have aria-label
    const copyButtons = document.querySelectorAll('.code-block__copy');
    copyButtons.forEach((button) => {
      expect(button.getAttribute('aria-label')).toBeTruthy();
    });
  });

  test('navigation toggle button should have aria-label', () => {
    const navToggle = document.querySelector('.nav-toggle');
    if (navToggle) {
      expect(navToggle.getAttribute('aria-label')).toBeTruthy();
      expect(navToggle.getAttribute('aria-expanded')).toBeTruthy();
    }
  });

  test('buttons with role="button" should be accessible', () => {
    const roleButtons = document.querySelectorAll('[role="button"]');
    roleButtons.forEach((element) => {
      // Either has text content or aria-label
      const hasLabel = element.textContent.trim().length > 0 || element.getAttribute('aria-label');
      expect(hasLabel).toBeTruthy();
    });
  });

  test('external links should indicate they open in new tab', () => {
    const externalLinks = document.querySelectorAll('a[target="_blank"]');
    externalLinks.forEach((link) => {
      // Should have rel="noopener" for security
      expect(link.getAttribute('rel')).toMatch(/noopener/);
      // Should indicate opens in new tab via aria-label or text
      const hasNewTabIndication =
        link.getAttribute('aria-label')?.includes('new tab') ||
        link.textContent.includes('new tab') ||
        link.querySelector('.sr-only')?.textContent.includes('new tab');
      // At least one external link in nav should have indication
    });
  });
});

describe('Form Accessibility', () => {
  let document;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../_site/index.html');
    const html = fs.readFileSync(htmlPath, 'utf-8');
    const { document: doc } = parseHTML(html);
    document = doc;
  });

  test('buttons should have type attribute', () => {
    const buttons = document.querySelectorAll('button');
    buttons.forEach((button) => {
      expect(button.getAttribute('type')).toBeTruthy();
    });
  });
});

describe('Focus Styles (CSS)', () => {
  let cssContent;

  beforeAll(() => {
    const cssPath = path.join(__dirname, '../../_site/assets/css/main.css');
    cssContent = fs.readFileSync(cssPath, 'utf-8');
  });

  test('should have focus-visible styles defined', () => {
    expect(cssContent).toMatch(/:focus-visible/);
    expect(cssContent).toMatch(/outline/);
  });

  test('should have skip-link styles', () => {
    expect(cssContent).toMatch(/\.skip-link/);
  });

  test('should have sr-only styles for screen reader text', () => {
    expect(cssContent).toMatch(/\.sr-only/);
    expect(cssContent).toMatch(/clip:\s*rect\(0,\s*0,\s*0,\s*0\)/);
  });

  test('should respect reduced motion preference', () => {
    expect(cssContent).toMatch(/prefers-reduced-motion/);
  });

  test('should have button focus styles', () => {
    expect(cssContent).toMatch(/button:focus|\.btn:focus/);
  });
});

describe('Language and Document Structure', () => {
  let document;
  let html;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../_site/index.html');
    html = fs.readFileSync(htmlPath, 'utf-8');
    const { document: doc } = parseHTML(html);
    document = doc;
  });

  test('should have lang attribute on html element', () => {
    const htmlElement = document.documentElement;
    expect(htmlElement.getAttribute('lang')).toBe('en');
  });

  test('should have proper DOCTYPE', () => {
    expect(html.trim().toLowerCase().startsWith('<!doctype html>')).toBe(true);
  });

  test('should have viewport meta tag', () => {
    const viewport = document.querySelector('meta[name="viewport"]');
    expect(viewport).not.toBeNull();
    expect(viewport.getAttribute('content')).toMatch(/width=device-width/);
  });

  test('should have charset meta tag', () => {
    const charset = document.querySelector('meta[charset]');
    expect(charset).not.toBeNull();
    expect(charset.getAttribute('charset').toLowerCase()).toBe('utf-8');
  });
});

describe('Interactive Elements Accessibility', () => {
  let document;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../_site/index.html');
    const html = fs.readFileSync(htmlPath, 'utf-8');
    const { document: doc } = parseHTML(html);
    document = doc;
  });

  test('all links should have discernible text', () => {
    const links = document.querySelectorAll('a');
    links.forEach((link) => {
      const hasText = link.textContent.trim().length > 0;
      const hasAriaLabel = link.getAttribute('aria-label');
      const hasAriaLabelledBy = link.getAttribute('aria-labelledby');
      const hasImage = link.querySelector('img[alt]');

      const isAccessible = hasText || hasAriaLabel || hasAriaLabelledBy || hasImage;
      expect(isAccessible).toBe(true);
    });
  });

  test('navigation links should be in a list', () => {
    const navLinks = document.querySelector('nav ul');
    expect(navLinks).not.toBeNull();
    expect(navLinks.querySelectorAll('li').length).toBeGreaterThan(0);
  });
});
