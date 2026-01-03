/**
 * Unit tests for SEO Semantic Structure
 * Scenario: SEO - Semantic Structure
 * Verifies the homepage uses semantic HTML for better search engine understanding
 */

const fs = require('fs');
const path = require('path');
const { JSDOM } = require('jsdom');

describe('SEO - Semantic Structure', () => {
  let document;
  let html;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../index.html');
    html = fs.readFileSync(htmlPath, 'utf-8');
    const dom = new JSDOM(html);
    document = dom.window.document;
  });

  describe('TC1: Check for semantic HTML elements - header, main, footer', () => {
    test('Page uses header element', () => {
      const header = document.querySelector('header');
      expect(header).not.toBeNull();
    });

    test('Page uses main element', () => {
      const main = document.querySelector('main');
      expect(main).not.toBeNull();
    });

    test('Page uses footer element', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();
    });

    test('Header contains the primary page title (h1)', () => {
      const header = document.querySelector('header');
      expect(header).not.toBeNull();
      const h1 = header.querySelector('h1');
      expect(h1).not.toBeNull();
      expect(h1.textContent.trim()).toBeTruthy();
    });

    test('Main element contains the primary content sections', () => {
      const main = document.querySelector('main');
      expect(main).not.toBeNull();
      const sections = main.querySelectorAll('section');
      expect(sections.length).toBeGreaterThan(0);
    });

    test('Header appears before main in document order', () => {
      const header = document.querySelector('header');
      const main = document.querySelector('main');
      expect(header).not.toBeNull();
      expect(main).not.toBeNull();

      // Use compareDocumentPosition to verify order
      const position = header.compareDocumentPosition(main);
      // DOCUMENT_POSITION_FOLLOWING means main follows header
      expect(position & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    });

    test('Main appears before footer in document order', () => {
      const main = document.querySelector('main');
      const footer = document.querySelector('footer');
      expect(main).not.toBeNull();
      expect(footer).not.toBeNull();

      const position = main.compareDocumentPosition(footer);
      expect(position & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    });
  });

  describe('TC2: Check for section elements', () => {
    test('Content sections use section elements', () => {
      const sections = document.querySelectorAll('section');
      expect(sections.length).toBeGreaterThan(0);
    });

    test('At least one article element is used for standalone content', () => {
      const articles = document.querySelectorAll('article');
      expect(articles.length).toBeGreaterThan(0);
    });

    test('Sections have identifiable purposes via id or aria attributes', () => {
      const sections = document.querySelectorAll('main section');
      expect(sections.length).toBeGreaterThan(0);

      sections.forEach((section) => {
        const hasId = section.hasAttribute('id');
        const hasAriaLabel = section.hasAttribute('aria-label');
        const hasAriaLabelledBy = section.hasAttribute('aria-labelledby');
        const hasIdentifier = hasId || hasAriaLabel || hasAriaLabelledBy;
        expect(hasIdentifier).toBe(true);
      });
    });

    test('Feature cards use article elements for semantic grouping', () => {
      const featureSection = document.querySelector('#features');
      expect(featureSection).not.toBeNull();

      const articles = featureSection.querySelectorAll('article');
      expect(articles.length).toBeGreaterThanOrEqual(4);
    });

    test('Each section has a heading for SEO structure', () => {
      const sections = document.querySelectorAll('main section');

      sections.forEach((section) => {
        const heading = section.querySelector('h2, h3, h4, h5, h6');
        expect(heading).not.toBeNull();
      });
    });
  });

  describe('TC3: Check for nav element', () => {
    test('Navigation uses nav element if navigation exists', () => {
      // Check if there are navigation links in the page
      const footerLinks = document.querySelector('.footer-links');

      if (footerLinks) {
        expect(footerLinks.tagName.toLowerCase()).toBe('nav');
      }

      // At least one nav element should exist
      const navElements = document.querySelectorAll('nav');
      expect(navElements.length).toBeGreaterThan(0);
    });

    test('Nav element contains anchor links', () => {
      const nav = document.querySelector('nav');
      expect(nav).not.toBeNull();

      const links = nav.querySelectorAll('a');
      expect(links.length).toBeGreaterThan(0);
    });

    test('Nav links have valid href attributes', () => {
      const nav = document.querySelector('nav');
      expect(nav).not.toBeNull();

      const links = nav.querySelectorAll('a');
      links.forEach((link) => {
        const href = link.getAttribute('href');
        expect(href).toBeTruthy();
        expect(href.length).toBeGreaterThan(0);
      });
    });
  });

  describe('TC4: Validate HTML structure', () => {
    test('Document has proper DOCTYPE declaration', () => {
      expect(html.trim().toLowerCase().startsWith('<!doctype html>')).toBe(true);
    });

    test('HTML element has lang attribute', () => {
      const htmlElement = document.querySelector('html');
      expect(htmlElement).not.toBeNull();
      const lang = htmlElement.getAttribute('lang');
      expect(lang).toBeTruthy();
      expect(lang).toBe('en');
    });

    test('Head contains required meta charset', () => {
      const charset = document.querySelector('meta[charset]');
      expect(charset).not.toBeNull();
    });

    test('Head contains title element', () => {
      const title = document.querySelector('title');
      expect(title).not.toBeNull();
      expect(title.textContent.trim()).toBeTruthy();
    });

    test('Body contains only block-level elements at top level', () => {
      const body = document.querySelector('body');
      expect(body).not.toBeNull();

      // Top-level children should be block elements (header, main, footer, script, etc.)
      const topLevelElements = Array.from(body.children);
      const validTopLevelTags = ['header', 'main', 'footer', 'nav', 'section', 'article', 'aside', 'div', 'script', 'noscript', 'template'];

      topLevelElements.forEach((el) => {
        expect(validTopLevelTags.includes(el.tagName.toLowerCase())).toBe(true);
      });
    });

    test('No duplicate id attributes in document', () => {
      const allElements = document.querySelectorAll('[id]');
      const ids = Array.from(allElements).map(el => el.getAttribute('id'));
      const uniqueIds = new Set(ids);
      expect(ids.length).toBe(uniqueIds.size);
    });

    test('All sections within main have unique identifiers', () => {
      const sections = document.querySelectorAll('main section[id]');
      const ids = Array.from(sections).map(s => s.getAttribute('id'));
      const uniqueIds = new Set(ids);
      expect(ids.length).toBe(uniqueIds.size);
    });

    test('Heading hierarchy is properly structured', () => {
      const allHeadings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      const headings = Array.from(allHeadings);

      expect(headings.length).toBeGreaterThan(0);

      // First heading should be h1
      expect(headings[0].tagName.toLowerCase()).toBe('h1');

      // Only one h1 should exist
      const h1Count = headings.filter(h => h.tagName.toLowerCase() === 'h1').length;
      expect(h1Count).toBe(1);

      // Check heading levels don't skip (e.g., h1 to h3 without h2)
      let maxLevelSeen = 1;
      for (const heading of headings) {
        const level = parseInt(heading.tagName.charAt(1));
        expect(level).toBeLessThanOrEqual(maxLevelSeen + 1);
        if (level > maxLevelSeen) {
          maxLevelSeen = level;
        }
      }
    });

    test('All anchor elements have valid href or are interactive', () => {
      const anchors = document.querySelectorAll('a');

      anchors.forEach((anchor) => {
        const href = anchor.getAttribute('href');
        const role = anchor.getAttribute('role');

        // Anchor should have href or be used as a button
        const isValid = (href && href.length > 0) || role === 'button';
        expect(isValid).toBe(true);
      });
    });

    test('External links have proper security attributes', () => {
      const externalLinks = document.querySelectorAll('a[target="_blank"]');

      externalLinks.forEach((link) => {
        const rel = link.getAttribute('rel');
        expect(rel).toBeTruthy();
        expect(rel).toContain('noopener');
      });
    });
  });

  describe('Semantic Structure Best Practices', () => {
    test('Content outline is SEO-friendly', () => {
      // Check that major sections exist
      const features = document.querySelector('#features');
      const quickstart = document.querySelector('#quickstart');

      expect(features).not.toBeNull();
      expect(quickstart).not.toBeNull();
    });

    test('Sections use aria-labelledby to reference their headings', () => {
      const sectionsWithAriaLabel = document.querySelectorAll('section[aria-labelledby]');

      sectionsWithAriaLabel.forEach((section) => {
        const labelledbyId = section.getAttribute('aria-labelledby');
        const referencedHeading = document.getElementById(labelledbyId);
        expect(referencedHeading).not.toBeNull();
      });
    });

    test('Definition lists are used for structured content where appropriate', () => {
      // Check if dl/dt/dd are used for command descriptions
      const definitionLists = document.querySelectorAll('dl');

      if (definitionLists.length > 0) {
        definitionLists.forEach((dl) => {
          const terms = dl.querySelectorAll('dt');
          const definitions = dl.querySelectorAll('dd');
          expect(terms.length).toBeGreaterThan(0);
          expect(definitions.length).toBeGreaterThan(0);
        });
      }
    });

    test('Tables have proper semantic structure with thead and tbody', () => {
      const tables = document.querySelectorAll('table');

      tables.forEach((table) => {
        const thead = table.querySelector('thead');
        const tbody = table.querySelector('tbody');
        const th = table.querySelectorAll('th');

        expect(thead).not.toBeNull();
        expect(tbody).not.toBeNull();
        expect(th.length).toBeGreaterThan(0);
      });
    });
  });
});
