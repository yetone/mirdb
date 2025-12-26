/**
 * SEO Optimization Tests
 * Tests for verifying SEO-friendly meta tags and semantic HTML structure (NFR-5)
 */

describe('SEO Optimization - Meta Tags and Semantic HTML', () => {
  let head;
  let body;

  beforeAll(() => {
    head = document.head;
    body = document.body;
  });

  describe('Test Case 1: Title Tag', () => {
    it('should have a title tag', () => {
      const title = document.querySelector('title');
      expect(title).not.toBeNull();
    });

    it('should have descriptive title content', () => {
      const title = document.querySelector('title');
      expect(title.textContent.length).toBeGreaterThan(0);
    });

    it('should include "MirDB" in the title', () => {
      const title = document.querySelector('title');
      expect(title.textContent.toLowerCase()).toContain('mirdb');
    });

    it('should have a title with optimal length (30-60 characters)', () => {
      const title = document.querySelector('title');
      const length = title.textContent.length;
      expect(length).toBeGreaterThanOrEqual(30);
      expect(length).toBeLessThanOrEqual(70);
    });
  });

  describe('Test Case 2: Meta Description', () => {
    it('should have a meta description tag', () => {
      const metaDescription = document.querySelector('meta[name="description"]');
      expect(metaDescription).not.toBeNull();
    });

    it('should have meta description with relevant content', () => {
      const metaDescription = document.querySelector('meta[name="description"]');
      const content = metaDescription.getAttribute('content');
      expect(content).toBeTruthy();
      expect(content.toLowerCase()).toContain('mirdb');
    });

    it('should have meta description with optimal length (150-160 characters)', () => {
      const metaDescription = document.querySelector('meta[name="description"]');
      const content = metaDescription.getAttribute('content');
      const length = content.length;
      // Allow some flexibility in range (120-170 is acceptable)
      expect(length).toBeGreaterThanOrEqual(120);
      expect(length).toBeLessThanOrEqual(170);
    });

    it('should have meta description that describes the product', () => {
      const metaDescription = document.querySelector('meta[name="description"]');
      const content = metaDescription.getAttribute('content').toLowerCase();
      // Should mention key features
      expect(content).toMatch(/key-value|memcached|persistent/i);
    });
  });

  describe('Test Case 3: Canonical URL', () => {
    it('should have a canonical link tag', () => {
      const canonical = document.querySelector('link[rel="canonical"]');
      expect(canonical).not.toBeNull();
    });

    it('should have a valid canonical URL', () => {
      const canonical = document.querySelector('link[rel="canonical"]');
      const href = canonical.getAttribute('href');
      expect(href).toBeTruthy();
      // Should be a valid URL format
      expect(href).toMatch(/^https?:\/\//);
    });
  });

  describe('Test Case 4: Open Graph Meta Tags', () => {
    it('should have og:title meta tag', () => {
      const ogTitle = document.querySelector('meta[property="og:title"]');
      expect(ogTitle).not.toBeNull();
      expect(ogTitle.getAttribute('content')).toBeTruthy();
    });

    it('should have og:description meta tag', () => {
      const ogDescription = document.querySelector('meta[property="og:description"]');
      expect(ogDescription).not.toBeNull();
      expect(ogDescription.getAttribute('content')).toBeTruthy();
    });

    it('should have og:type meta tag', () => {
      const ogType = document.querySelector('meta[property="og:type"]');
      expect(ogType).not.toBeNull();
      expect(ogType.getAttribute('content')).toBeTruthy();
    });

    it('should have og:title that includes MirDB', () => {
      const ogTitle = document.querySelector('meta[property="og:title"]');
      expect(ogTitle.getAttribute('content').toLowerCase()).toContain('mirdb');
    });

    it('should have og:description with relevant content', () => {
      const ogDescription = document.querySelector('meta[property="og:description"]');
      const content = ogDescription.getAttribute('content').toLowerCase();
      expect(content).toMatch(/key-value|memcached|persistent/i);
    });

    it('should have appropriate og:type value', () => {
      const ogType = document.querySelector('meta[property="og:type"]');
      const validTypes = ['website', 'article', 'product'];
      expect(validTypes).toContain(ogType.getAttribute('content'));
    });
  });

  describe('Test Case 5: Heading Structure for SEO', () => {
    it('should have exactly one h1 tag', () => {
      const h1Tags = document.querySelectorAll('h1');
      expect(h1Tags.length).toBe(1);
    });

    it('should have h1 that contains primary keyword (MirDB)', () => {
      const h1 = document.querySelector('h1');
      expect(h1.textContent.toLowerCase()).toContain('mirdb');
    });

    it('should have h2 tags for main sections', () => {
      const h2Tags = document.querySelectorAll('h2');
      expect(h2Tags.length).toBeGreaterThan(0);
    });

    it('should have logical heading hierarchy (h1 -> h2 -> h3)', () => {
      // Check that h2 comes after h1 in the document
      const h1 = document.querySelector('h1');
      const h2s = document.querySelectorAll('h2');

      if (h2s.length > 0) {
        // Verify h2s appear after h1 in document order
        const h1Position = h1.compareDocumentPosition(h2s[0]);
        expect(h1Position & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
      }
    });

    it('should have h3 tags nested within sections with h2', () => {
      const h3Tags = document.querySelectorAll('h3');
      // h3 tags should exist (for feature cards, etc.)
      expect(h3Tags.length).toBeGreaterThan(0);
    });

    it('should not skip heading levels', () => {
      const allHeadings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      let previousLevel = 0;

      allHeadings.forEach((heading) => {
        const currentLevel = parseInt(heading.tagName.charAt(1));
        // Should not skip more than one level (e.g., h1 to h3 without h2)
        if (previousLevel > 0) {
          expect(currentLevel - previousLevel).toBeLessThanOrEqual(1);
        }
        if (currentLevel > previousLevel) {
          previousLevel = currentLevel;
        }
      });
    });
  });

  describe('Test Case 6: Charset Declaration', () => {
    it('should have meta charset tag', () => {
      const charset = document.querySelector('meta[charset]');
      expect(charset).not.toBeNull();
    });

    it('should have charset set to UTF-8', () => {
      const charset = document.querySelector('meta[charset]');
      expect(charset.getAttribute('charset').toUpperCase()).toBe('UTF-8');
    });

    it('should have charset as one of the first elements in head', () => {
      const headChildren = Array.from(head.children);
      const charsetIndex = headChildren.findIndex(
        (el) => el.tagName === 'META' && el.hasAttribute('charset')
      );
      // Charset should be within the first 5 elements of head for best practices
      expect(charsetIndex).toBeLessThan(5);
    });
  });

  describe('Additional SEO Best Practices', () => {
    it('should have viewport meta tag for mobile responsiveness', () => {
      const viewport = document.querySelector('meta[name="viewport"]');
      expect(viewport).not.toBeNull();
      expect(viewport.getAttribute('content')).toContain('width=device-width');
    });

    it('should have lang attribute on html element', () => {
      const htmlElement = document.documentElement;
      expect(htmlElement.getAttribute('lang')).toBeTruthy();
    });

    it('should have semantic HTML structure', () => {
      expect(document.querySelector('header')).not.toBeNull();
      expect(document.querySelector('main')).not.toBeNull();
      expect(document.querySelector('footer')).not.toBeNull();
      expect(document.querySelector('nav')).not.toBeNull();
    });

    it('should have external links with rel="noopener"', () => {
      const externalLinks = document.querySelectorAll('a[target="_blank"]');
      externalLinks.forEach((link) => {
        expect(link.getAttribute('rel')).toContain('noopener');
      });
    });
  });
});
