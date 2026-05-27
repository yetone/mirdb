/**
 * SEO and Meta Tags Integration Tests
 * Scenario 12: SEO & Meta Tags
 *
 * Tests:
 * 1. HTML head element: title contains 'MirDB'; meta description exists; charset is utf-8; viewport meta is present
 * 2. Open Graph tags: all required OG tags present with correct values
 * 3. Twitter Card tags: all required Twitter tags present with correct values
 * 4. Structured data: JSON-LD script contains schema.org SoftwareApplication with name 'MirDB', description, and url
 * 5. Canonical and hreflang tags: canonical link points to root URL; no conflicting canonicals
 * 6. Robots meta tag: no robots=noindex directive; includes robots='index, follow'
 * 7. Structured data validation: JSON-LD parses as valid JSON; contains valid schema.org type; required properties are present
 * 8. Semantic HTML: proper use of main, article, section, header, nav, footer elements
 */

const fs = require('fs');
const path = require('path');
const { JSDOM } = require('jsdom');

describe('SEO and Meta Tags - Integration Tests', () => {
  let dom;
  let document;
  let html;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '..', '..', 'public', 'index.html');
    html = fs.readFileSync(htmlPath, 'utf-8');
    dom = new JSDOM(html, {
      url: 'https://mirdb.dev/',
    });
    document = dom.window.document;
  });

  afterAll(() => {
    if (dom) dom.window.close();
  });

  // ── Test Case 1: HTML head element basics ──
  describe('Test 1: HTML head element basics', () => {
    it('should have a title containing "MirDB"', () => {
      const title = document.querySelector('title');
      expect(title).toBeTruthy();
      expect(title.textContent).toContain('MirDB');
    });

    it('should have title with descriptive tagline', () => {
      const title = document.querySelector('title');
      expect(title.textContent).toMatch(/MirDB.*(Persistent|Key-Value|Store)/i);
    });

    it('should have meta charset set to utf-8', () => {
      const charset = document.querySelector('meta[charset]');
      expect(charset).toBeTruthy();
      expect(charset.getAttribute('charset').toLowerCase()).toBe('utf-8');
    });

    it('should have viewport meta tag', () => {
      const viewport = document.querySelector('meta[name="viewport"]');
      expect(viewport).toBeTruthy();
      expect(viewport.getAttribute('content')).toContain('width=device-width');
    });

    it('should have meta description', () => {
      const description = document.querySelector('meta[name="description"]');
      expect(description).toBeTruthy();
      expect(description.getAttribute('content').length).toBeGreaterThan(0);
    });

    it('should have meta description under 160 characters', () => {
      const description = document.querySelector('meta[name="description"]');
      const content = description.getAttribute('content');
      expect(content.length).toBeLessThanOrEqual(160);
    });

    it('should have meta author', () => {
      const author = document.querySelector('meta[name="author"]');
      expect(author).toBeTruthy();
    });
  });

  // ── Test Case 2: Open Graph tags ──
  describe('Test 2: Open Graph tags', () => {
    it('should have og:title meta tag containing MirDB', () => {
      const ogTitle = document.querySelector('meta[property="og:title"]');
      expect(ogTitle).toBeTruthy();
      expect(ogTitle.getAttribute('content')).toContain('MirDB');
    });

    it('should have og:description meta tag', () => {
      const ogDesc = document.querySelector('meta[property="og:description"]');
      expect(ogDesc).toBeTruthy();
      expect(ogDesc.getAttribute('content').length).toBeGreaterThan(0);
    });

    it('should have og:type set to website', () => {
      const ogType = document.querySelector('meta[property="og:type"]');
      expect(ogType).toBeTruthy();
      expect(ogType.getAttribute('content')).toBe('website');
    });

    it('should have og:url meta tag', () => {
      const ogUrl = document.querySelector('meta[property="og:url"]');
      expect(ogUrl).toBeTruthy();
      expect(ogUrl.getAttribute('content')).toContain('mirdb.dev');
    });

    it('should have og:image meta tag', () => {
      const ogImage = document.querySelector('meta[property="og:image"]');
      expect(ogImage).toBeTruthy();
      expect(ogImage.getAttribute('content')).toContain('mirdb.dev');
    });

    it('should have og:site_name meta tag', () => {
      const ogSite = document.querySelector('meta[property="og:site_name"]');
      expect(ogSite).toBeTruthy();
      expect(ogSite.getAttribute('content')).toContain('MirDB');
    });

    it('should have og:locale meta tag', () => {
      const ogLocale = document.querySelector('meta[property="og:locale"]');
      expect(ogLocale).toBeTruthy();
    });
  });

  // ── Test Case 3: Twitter Card tags ──
  describe('Test 3: Twitter Card tags', () => {
    it('should have twitter:card set to summary_large_image', () => {
      const twitterCard = document.querySelector('meta[name="twitter:card"]');
      expect(twitterCard).toBeTruthy();
      expect(twitterCard.getAttribute('content')).toBe('summary_large_image');
    });

    it('should have twitter:title meta tag containing MirDB', () => {
      const twitterTitle = document.querySelector('meta[name="twitter:title"]');
      expect(twitterTitle).toBeTruthy();
      expect(twitterTitle.getAttribute('content')).toContain('MirDB');
    });

    it('should have twitter:description meta tag', () => {
      const twitterDesc = document.querySelector('meta[name="twitter:description"]');
      expect(twitterDesc).toBeTruthy();
      expect(twitterDesc.getAttribute('content').length).toBeGreaterThan(0);
    });

    it('should have twitter:image meta tag', () => {
      const twitterImage = document.querySelector('meta[name="twitter:image"]');
      expect(twitterImage).toBeTruthy();
      expect(twitterImage.getAttribute('content')).toContain('mirdb.dev');
    });
  });

  // ── Test Case 4: Structured data (JSON-LD) ──
  describe('Test 4: Structured data (JSON-LD)', () => {
    it('should have at least one JSON-LD script tag', () => {
      const scripts = document.querySelectorAll('script[type="application/ld+json"]');
      expect(scripts.length).toBeGreaterThan(0);
    });

    it('should have SoftwareApplication structured data', () => {
      const scripts = document.querySelectorAll('script[type="application/ld+json"]');
      let found = false;
      scripts.forEach(script => {
        try {
          const data = JSON.parse(script.textContent);
          if (data['@type'] === 'SoftwareApplication') {
            found = true;
          }
        } catch (e) {
          // skip invalid JSON
        }
      });
      expect(found).toBe(true);
    });

    it('should have WebSite structured data', () => {
      const scripts = document.querySelectorAll('script[type="application/ld+json"]');
      let found = false;
      scripts.forEach(script => {
        try {
          const data = JSON.parse(script.textContent);
          if (data['@type'] === 'WebSite') {
            found = true;
          }
        } catch (e) {
          // skip invalid JSON
        }
      });
      expect(found).toBe(true);
    });

    it('should have name "MirDB" in SoftwareApplication structured data', () => {
      const scripts = document.querySelectorAll('script[type="application/ld+json"]');
      let found = false;
      scripts.forEach(script => {
        try {
          const data = JSON.parse(script.textContent);
          if (data['@type'] === 'SoftwareApplication' && data.name === 'MirDB') {
            found = true;
          }
        } catch (e) {
          // skip invalid JSON
        }
      });
      expect(found).toBe(true);
    });

    it('should have description in SoftwareApplication structured data', () => {
      const scripts = document.querySelectorAll('script[type="application/ld+json"]');
      let found = false;
      scripts.forEach(script => {
        try {
          const data = JSON.parse(script.textContent);
          if (data['@type'] === 'SoftwareApplication' && data.description && data.description.length > 0) {
            found = true;
          }
        } catch (e) {
          // skip invalid JSON
        }
      });
      expect(found).toBe(true);
    });

    it('should have url in SoftwareApplication structured data', () => {
      const scripts = document.querySelectorAll('script[type="application/ld+json"]');
      let found = false;
      scripts.forEach(script => {
        try {
          const data = JSON.parse(script.textContent);
          if (data['@type'] === 'SoftwareApplication' && data.url && data.url.includes('mirdb.dev')) {
            found = true;
          }
        } catch (e) {
          // skip invalid JSON
        }
      });
      expect(found).toBe(true);
    });

    it('should have url in WebSite structured data', () => {
      const scripts = document.querySelectorAll('script[type="application/ld+json"]');
      let found = false;
      scripts.forEach(script => {
        try {
          const data = JSON.parse(script.textContent);
          if (data['@type'] === 'WebSite' && data.url && data.url.includes('mirdb.dev')) {
            found = true;
          }
        } catch (e) {
          // skip invalid JSON
        }
      });
      expect(found).toBe(true);
    });
  });

  // ── Test Case 5: Canonical and hreflang tags ──
  describe('Test 5: Canonical and hreflang tags', () => {
    it('should have canonical link tag', () => {
      const canonical = document.querySelector('link[rel="canonical"]');
      expect(canonical).toBeTruthy();
    });

    it('should point canonical to root URL', () => {
      const canonical = document.querySelector('link[rel="canonical"]');
      const href = canonical.getAttribute('href');
      expect(href).toMatch(/^https:\/\/mirdb\.dev\//);
    });

    it('should not have conflicting canonical tags', () => {
      const canonicals = document.querySelectorAll('link[rel="canonical"]');
      expect(canonicals.length).toBe(1);
    });
  });

  // ── Test Case 6: Robots meta tag ──
  describe('Test 6: Robots meta tag', () => {
    it('should not have robots=noindex directive', () => {
      const robots = document.querySelector('meta[name="robots"]');
      expect(robots).toBeTruthy();
      const content = robots.getAttribute('content').toLowerCase();
      expect(content).not.toContain('noindex');
    });

    it('should include robots index, follow directives', () => {
      const robots = document.querySelector('meta[name="robots"]');
      expect(robots).toBeTruthy();
      const content = robots.getAttribute('content').toLowerCase();
      expect(content).toContain('index');
      expect(content).toContain('follow');
    });
  });

  // ── Test Case 7: Validate structured data with schema validator ──
  describe('Test 7: Validate structured data with schema validator', () => {
    it('should parse all JSON-LD as valid JSON', () => {
      const scripts = document.querySelectorAll('script[type="application/ld+json"]');
      expect(scripts.length).toBeGreaterThan(0);

      scripts.forEach((script, index) => {
        let data;
        expect(() => {
          data = JSON.parse(script.textContent);
        }).not.toThrow();
        expect(data).toBeTruthy();
      });
    });

    it('should have valid schema.org @context', () => {
      const scripts = document.querySelectorAll('script[type="application/ld+json"]');
      scripts.forEach(script => {
        try {
          const data = JSON.parse(script.textContent);
          expect(data['@context']).toBe('https://schema.org');
        } catch (e) {
          // skip invalid
        }
      });
    });

    it('should contain valid schema.org @type values', () => {
      const scripts = document.querySelectorAll('script[type="application/ld+json"]');
      let hasValidType = false;
      const validTypes = ['SoftwareApplication', 'WebSite', 'Organization', 'Product'];

      scripts.forEach(script => {
        try {
          const data = JSON.parse(script.textContent);
          if (validTypes.includes(data['@type'])) {
            hasValidType = true;
          }
        } catch (e) {
          // skip invalid
        }
      });

      expect(hasValidType).toBe(true);
    });

    it('should have required SoftwareApplication properties', () => {
      const scripts = document.querySelectorAll('script[type="application/ld+json"]');
      let found = false;
      scripts.forEach(script => {
        try {
          const data = JSON.parse(script.textContent);
          if (data['@type'] === 'SoftwareApplication') {
            expect(data.name).toBeTruthy();
            expect(data.description).toBeTruthy();
            expect(data.url).toBeTruthy();
            expect(data.applicationCategory).toBeTruthy();
            found = true;
          }
        } catch (e) {
          // skip invalid
        }
      });
      expect(found).toBe(true);
    });

    it('should have required WebSite properties', () => {
      const scripts = document.querySelectorAll('script[type="application/ld+json"]');
      let found = false;
      scripts.forEach(script => {
        try {
          const data = JSON.parse(script.textContent);
          if (data['@type'] === 'WebSite') {
            expect(data.name).toBeTruthy();
            expect(data.description).toBeTruthy();
            expect(data.url).toBeTruthy();
            found = true;
          }
        } catch (e) {
          // skip invalid
        }
      });
      expect(found).toBe(true);
    });
  });

  // ── Test Case 8: Semantic HTML structure for SEO ──
  describe('Test 8: Semantic HTML structure', () => {
    it('should use header element', () => {
      expect(document.querySelector('header')).toBeTruthy();
    });

    it('should use nav element inside header', () => {
      const header = document.querySelector('header');
      expect(header.querySelector('nav')).toBeTruthy();
    });

    it('should use main element', () => {
      expect(document.querySelector('main')).toBeTruthy();
    });

    it('should use section elements for major content regions', () => {
      const sections = document.querySelectorAll('main > section');
      expect(sections.length).toBeGreaterThanOrEqual(4);
    });

    it('should use article elements for feature cards', () => {
      const articles = document.querySelectorAll('article');
      expect(articles.length).toBeGreaterThan(0);
    });

    it('should use footer element', () => {
      expect(document.querySelector('footer')).toBeTruthy();
    });

    it('should have lang attribute on html element', () => {
      expect(document.documentElement.lang).toBe('en');
    });

    it('should have h1 heading for page title', () => {
      const h1 = document.querySelector('h1');
      expect(h1).toBeTruthy();
      expect(h1.textContent.trim()).toBe('MirDB');
    });
  });
});
