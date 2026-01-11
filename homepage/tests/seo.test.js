// SEO Optimization Tests
// Verifies proper meta tags, structured data, and semantic HTML

const fs = require('fs');
const path = require('path');

describe('SEO Optimization', () => {
  let htmlContent;
  let parser;
  let doc;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '..', 'index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf-8');
    parser = new DOMParser();
    doc = parser.parseFromString(htmlContent, 'text/html');
  });

  describe('Meta Tags', () => {
    test('TC1: Title tag present containing "MirDB"', () => {
      const titleElement = doc.querySelector('title');
      expect(titleElement).not.toBeNull();
      expect(titleElement.textContent).toContain('MirDB');
    });

    test('TC2: Meta description tag present with meaningful content', () => {
      const metaDescription = doc.querySelector('meta[name="description"]');
      expect(metaDescription).not.toBeNull();
      const content = metaDescription.getAttribute('content');
      expect(content).toBeTruthy();
      expect(content.length).toBeGreaterThan(50); // Meaningful content should be substantial
    });

    test('TC3: Meta viewport tag with "width=device-width, initial-scale=1"', () => {
      const metaViewport = doc.querySelector('meta[name="viewport"]');
      expect(metaViewport).not.toBeNull();
      const content = metaViewport.getAttribute('content');
      expect(content).toContain('width=device-width');
      expect(content).toContain('initial-scale=1');
    });
  });

  describe('Heading Hierarchy', () => {
    test('TC4: Exactly one h1 element on the page', () => {
      const h1Elements = doc.querySelectorAll('h1');
      expect(h1Elements.length).toBe(1);
    });
  });

  describe('Open Graph Meta Tags', () => {
    test('TC5: og:title, og:description, og:image tags present', () => {
      const ogTitle = doc.querySelector('meta[property="og:title"]');
      const ogDescription = doc.querySelector('meta[property="og:description"]');
      const ogImage = doc.querySelector('meta[property="og:image"]');

      expect(ogTitle).not.toBeNull();
      expect(ogTitle.getAttribute('content')).toBeTruthy();

      expect(ogDescription).not.toBeNull();
      expect(ogDescription.getAttribute('content')).toBeTruthy();

      expect(ogImage).not.toBeNull();
      expect(ogImage.getAttribute('content')).toBeTruthy();
    });
  });

  describe('Semantic HTML Structure', () => {
    test('TC6: Page uses header, main, and footer semantic elements', () => {
      const header = doc.querySelector('header');
      const main = doc.querySelector('main');
      const footer = doc.querySelector('footer');

      expect(header).not.toBeNull();
      expect(main).not.toBeNull();
      expect(footer).not.toBeNull();
    });
  });
});
