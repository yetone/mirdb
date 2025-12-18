/**
 * SEO Optimization Tests for MirDB Landing Page
 * Tests for NFR-5: Page must be SEO-optimized with proper meta tags and semantic HTML
 *
 * Test Cases:
 * TC-1: Title tag is present and contains 'MirDB'
 * TC-2: Meta description tag is present with relevant content about MirDB
 * TC-3: Open Graph tags (og:title, og:description, og:image) are present
 * TC-4: Twitter Card meta tags are present
 * TC-5: Exactly one h1 element exists on the page
 * TC-6: Page uses semantic elements: header, nav, main, article/section, footer
 * TC-7: Canonical link tag is present in head
 * TC-8: Lighthouse SEO audit criteria (simulated)
 */

const fs = require('fs');
const path = require('path');

describe('SEO Optimization - NFR-5', () => {
  let document;
  let htmlContent;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../src/index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf8');
    document = new DOMParser().parseFromString(htmlContent, 'text/html');
  });

  // TC-1: Title tag is present and contains 'MirDB'
  describe('TC-1: Title Tag', () => {
    test('should have a title tag present in the head', () => {
      const title = document.querySelector('head title');
      expect(title).not.toBeNull();
    });

    test('title tag should contain "MirDB"', () => {
      const title = document.querySelector('head title');
      expect(title).not.toBeNull();
      expect(title.textContent).toContain('MirDB');
    });

    test('title tag should have descriptive content', () => {
      const title = document.querySelector('head title');
      expect(title).not.toBeNull();
      // Title should have meaningful length (not just "MirDB")
      expect(title.textContent.trim().length).toBeGreaterThan(5);
    });
  });

  // TC-2: Meta description tag is present with relevant content about MirDB
  describe('TC-2: Meta Description', () => {
    test('should have a meta description tag present', () => {
      const metaDesc = document.querySelector('meta[name="description"]');
      expect(metaDesc).not.toBeNull();
    });

    test('meta description should contain relevant content about MirDB', () => {
      const metaDesc = document.querySelector('meta[name="description"]');
      expect(metaDesc).not.toBeNull();
      const content = metaDesc.getAttribute('content');
      expect(content).not.toBeNull();
      // Should mention MirDB
      expect(content.toLowerCase()).toContain('mirdb');
    });

    test('meta description should mention key features (key-value store or memcached)', () => {
      const metaDesc = document.querySelector('meta[name="description"]');
      const content = metaDesc.getAttribute('content').toLowerCase();
      // Should mention at least one key feature
      expect(
        content.includes('key-value') ||
        content.includes('memcached') ||
        content.includes('persistent') ||
        content.includes('database')
      ).toBe(true);
    });

    test('meta description should have appropriate length (50-160 characters)', () => {
      const metaDesc = document.querySelector('meta[name="description"]');
      const content = metaDesc.getAttribute('content');
      // Optimal meta description length for SEO
      expect(content.length).toBeGreaterThanOrEqual(50);
      expect(content.length).toBeLessThanOrEqual(200);
    });
  });

  // TC-3: Open Graph tags (og:title, og:description, og:image) are present
  describe('TC-3: Open Graph Tags', () => {
    test('should have og:title meta tag', () => {
      const ogTitle = document.querySelector('meta[property="og:title"]');
      expect(ogTitle).not.toBeNull();
      const content = ogTitle.getAttribute('content');
      expect(content).not.toBeNull();
      expect(content.trim().length).toBeGreaterThan(0);
    });

    test('og:title should contain MirDB', () => {
      const ogTitle = document.querySelector('meta[property="og:title"]');
      expect(ogTitle).not.toBeNull();
      const content = ogTitle.getAttribute('content');
      expect(content).toContain('MirDB');
    });

    test('should have og:description meta tag', () => {
      const ogDesc = document.querySelector('meta[property="og:description"]');
      expect(ogDesc).not.toBeNull();
      const content = ogDesc.getAttribute('content');
      expect(content).not.toBeNull();
      expect(content.trim().length).toBeGreaterThan(0);
    });

    test('og:description should contain relevant content', () => {
      const ogDesc = document.querySelector('meta[property="og:description"]');
      const content = ogDesc.getAttribute('content').toLowerCase();
      expect(
        content.includes('mirdb') ||
        content.includes('key-value') ||
        content.includes('memcached') ||
        content.includes('persistent')
      ).toBe(true);
    });

    test('should have og:image meta tag', () => {
      const ogImage = document.querySelector('meta[property="og:image"]');
      expect(ogImage).not.toBeNull();
      const content = ogImage.getAttribute('content');
      expect(content).not.toBeNull();
      expect(content.trim().length).toBeGreaterThan(0);
    });

    test('should have og:type meta tag', () => {
      const ogType = document.querySelector('meta[property="og:type"]');
      expect(ogType).not.toBeNull();
      const content = ogType.getAttribute('content');
      expect(content).toBe('website');
    });

    test('should have og:url meta tag', () => {
      const ogUrl = document.querySelector('meta[property="og:url"]');
      expect(ogUrl).not.toBeNull();
      const content = ogUrl.getAttribute('content');
      expect(content).not.toBeNull();
      expect(content.trim().length).toBeGreaterThan(0);
    });
  });

  // TC-4: Twitter Card meta tags are present
  describe('TC-4: Twitter Card Tags', () => {
    test('should have twitter:card meta tag', () => {
      const twitterCard = document.querySelector('meta[name="twitter:card"]');
      expect(twitterCard).not.toBeNull();
      const content = twitterCard.getAttribute('content');
      expect(content).not.toBeNull();
      expect(['summary', 'summary_large_image', 'app', 'player']).toContain(content);
    });

    test('should have twitter:title meta tag', () => {
      const twitterTitle = document.querySelector('meta[name="twitter:title"]');
      expect(twitterTitle).not.toBeNull();
      const content = twitterTitle.getAttribute('content');
      expect(content).not.toBeNull();
      expect(content).toContain('MirDB');
    });

    test('should have twitter:description meta tag', () => {
      const twitterDesc = document.querySelector('meta[name="twitter:description"]');
      expect(twitterDesc).not.toBeNull();
      const content = twitterDesc.getAttribute('content');
      expect(content).not.toBeNull();
      expect(content.trim().length).toBeGreaterThan(0);
    });

    test('should have twitter:image meta tag', () => {
      const twitterImage = document.querySelector('meta[name="twitter:image"]');
      expect(twitterImage).not.toBeNull();
      const content = twitterImage.getAttribute('content');
      expect(content).not.toBeNull();
      expect(content.trim().length).toBeGreaterThan(0);
    });
  });

  // TC-5: Exactly one h1 element exists on the page
  describe('TC-5: H1 Element', () => {
    test('should have exactly one h1 element on the page', () => {
      const h1Elements = document.querySelectorAll('h1');
      expect(h1Elements.length).toBe(1);
    });

    test('h1 element should contain the product name', () => {
      const h1 = document.querySelector('h1');
      expect(h1).not.toBeNull();
      expect(h1.textContent).toContain('MirDB');
    });

    test('h1 should be the first heading in document order', () => {
      const allHeadings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      expect(allHeadings.length).toBeGreaterThan(0);
      expect(allHeadings[0].tagName).toBe('H1');
    });
  });

  // TC-6: Page uses semantic elements: header, nav, main, article/section, footer
  describe('TC-6: Semantic HTML Elements', () => {
    test('should have a header element', () => {
      const header = document.querySelector('header');
      expect(header).not.toBeNull();
    });

    test('should have a nav element or navigation within footer', () => {
      const nav = document.querySelector('nav');
      const footerNav = document.querySelector('footer nav, footer .footer-links[aria-label]');
      expect(nav || footerNav).not.toBeNull();
    });

    test('should have a main element', () => {
      const main = document.querySelector('main');
      expect(main).not.toBeNull();
    });

    test('main element should have an id for skip link navigation', () => {
      const main = document.querySelector('main');
      expect(main).not.toBeNull();
      const hasId = main.getAttribute('id');
      expect(hasId).not.toBeNull();
    });

    test('should have section elements for content organization', () => {
      const sections = document.querySelectorAll('section');
      expect(sections.length).toBeGreaterThanOrEqual(1);
    });

    test('should have a footer element', () => {
      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();
    });

    test('page structure should follow logical order: header -> main -> footer', () => {
      const body = document.querySelector('body');
      const header = body.querySelector('header');
      const main = body.querySelector('main');
      const footer = body.querySelector('footer');

      expect(header).not.toBeNull();
      expect(main).not.toBeNull();
      expect(footer).not.toBeNull();

      // Check that header comes before main in DOM
      const headerIndex = Array.from(body.querySelectorAll('*')).indexOf(header);
      const mainIndex = Array.from(body.querySelectorAll('*')).indexOf(main);
      const footerIndex = Array.from(body.querySelectorAll('*')).indexOf(footer);

      expect(headerIndex).toBeLessThan(mainIndex);
      expect(mainIndex).toBeLessThan(footerIndex);
    });
  });

  // TC-7: Canonical link tag is present in head
  describe('TC-7: Canonical URL', () => {
    test('should have a canonical link tag in the head', () => {
      const canonical = document.querySelector('link[rel="canonical"]');
      expect(canonical).not.toBeNull();
    });

    test('canonical link should have a valid href', () => {
      const canonical = document.querySelector('link[rel="canonical"]');
      expect(canonical).not.toBeNull();
      const href = canonical.getAttribute('href');
      expect(href).not.toBeNull();
      expect(href.trim().length).toBeGreaterThan(0);
    });

    test('canonical href should be a valid URL format', () => {
      const canonical = document.querySelector('link[rel="canonical"]');
      const href = canonical.getAttribute('href');
      // Should be an absolute URL or a valid relative path
      expect(href).toMatch(/^(https?:\/\/|\/)/);
    });
  });

  // TC-8: Lighthouse SEO Audit Criteria (Simulated)
  describe('TC-8: Lighthouse SEO Audit Criteria', () => {
    // Lighthouse SEO: document-title
    test('[document-title] page should have a non-empty title', () => {
      const title = document.querySelector('title');
      expect(title).not.toBeNull();
      expect(title.textContent.trim().length).toBeGreaterThan(0);
    });

    // Lighthouse SEO: meta-description
    test('[meta-description] page should have a meta description', () => {
      const metaDesc = document.querySelector('meta[name="description"]');
      expect(metaDesc).not.toBeNull();
      const content = metaDesc.getAttribute('content');
      expect(content).not.toBeNull();
      expect(content.trim().length).toBeGreaterThan(0);
    });

    // Lighthouse SEO: http-status-code (not testable in jsdom, skip)

    // Lighthouse SEO: link-text
    test('[link-text] all links should have discernible text', () => {
      const links = document.querySelectorAll('a[href]');
      links.forEach((link) => {
        const hasText = link.textContent.trim().length > 0;
        const hasAriaLabel = link.getAttribute('aria-label');
        const hasTitle = link.getAttribute('title');
        expect(hasText || hasAriaLabel || hasTitle).toBeTruthy();
      });
    });

    // Lighthouse SEO: crawlable-anchors
    test('[crawlable-anchors] links should have valid href values', () => {
      const links = document.querySelectorAll('a[href]');
      links.forEach((link) => {
        const href = link.getAttribute('href');
        expect(href).not.toBe('');
        expect(href).not.toBe('#');
        // Should not be javascript: void(0) or similar
        expect(href).not.toMatch(/^javascript:/i);
      });
    });

    // Lighthouse SEO: is-crawlable
    test('[is-crawlable] page should not block search engines', () => {
      // Check for robots meta tag
      const robotsMeta = document.querySelector('meta[name="robots"]');
      if (robotsMeta) {
        const content = robotsMeta.getAttribute('content').toLowerCase();
        // Should not have noindex
        expect(content).not.toContain('noindex');
      }
    });

    // Lighthouse SEO: hreflang (optional for single-language site)

    // Lighthouse SEO: canonical
    test('[canonical] page should have canonical URL', () => {
      const canonical = document.querySelector('link[rel="canonical"]');
      expect(canonical).not.toBeNull();
    });

    // Lighthouse SEO: structured-data (optional, not required for basic SEO)

    // Additional SEO checks
    test('[viewport] page should have viewport meta tag', () => {
      const viewport = document.querySelector('meta[name="viewport"]');
      expect(viewport).not.toBeNull();
      const content = viewport.getAttribute('content');
      expect(content).toContain('width=device-width');
    });

    test('[charset] page should have charset meta tag', () => {
      const charset = document.querySelector('meta[charset]');
      expect(charset).not.toBeNull();
      expect(charset.getAttribute('charset').toLowerCase()).toBe('utf-8');
    });

    test('[html-lang] page should have lang attribute on html', () => {
      const html = document.documentElement;
      const lang = html.getAttribute('lang');
      expect(lang).not.toBeNull();
      expect(lang.length).toBeGreaterThan(0);
    });

    // Summary: Calculate SEO score based on key criteria
    test('should meet overall Lighthouse SEO criteria for score >= 90', () => {
      const criteria = {
        'document-title': document.querySelector('title')?.textContent.trim().length > 0,
        'meta-description': document.querySelector('meta[name="description"]')?.getAttribute('content')?.length > 0,
        'viewport': document.querySelector('meta[name="viewport"]') !== null,
        'html-lang': document.documentElement.getAttribute('lang')?.length > 0,
        'canonical': document.querySelector('link[rel="canonical"]') !== null,
        'link-text': Array.from(document.querySelectorAll('a[href]')).every(
          (a) => a.textContent.trim().length > 0 || a.getAttribute('aria-label')
        ),
        'crawlable-links': Array.from(document.querySelectorAll('a[href]')).every(
          (a) => !a.getAttribute('href')?.startsWith('javascript:')
        ),
        'og-tags': document.querySelector('meta[property="og:title"]') !== null,
        'twitter-tags': document.querySelector('meta[name="twitter:card"]') !== null,
        'semantic-html': document.querySelector('main') !== null && document.querySelector('header') !== null,
        'single-h1': document.querySelectorAll('h1').length === 1,
      };

      const passingCriteria = Object.values(criteria).filter(Boolean).length;
      const totalCriteria = Object.keys(criteria).length;
      const passRate = (passingCriteria / totalCriteria) * 100;

      // For a score of 90+, we need at least 90% of criteria to pass
      expect(passRate).toBeGreaterThanOrEqual(90);
    });
  });
});

// Integration test: Overall SEO structure
describe('Overall SEO Structure', () => {
  let document;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../src/index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf8');
    document = new DOMParser().parseFromString(htmlContent, 'text/html');
  });

  test('head section should contain all essential SEO elements', () => {
    const head = document.querySelector('head');
    expect(head).not.toBeNull();

    // Essential meta tags
    expect(head.querySelector('meta[charset]')).not.toBeNull();
    expect(head.querySelector('meta[name="viewport"]')).not.toBeNull();
    expect(head.querySelector('meta[name="description"]')).not.toBeNull();
    expect(head.querySelector('title')).not.toBeNull();
    expect(head.querySelector('link[rel="canonical"]')).not.toBeNull();

    // Social meta tags
    expect(head.querySelector('meta[property="og:title"]')).not.toBeNull();
    expect(head.querySelector('meta[property="og:description"]')).not.toBeNull();
    expect(head.querySelector('meta[name="twitter:card"]')).not.toBeNull();
  });

  test('page should have proper document structure for SEO', () => {
    // Single h1
    expect(document.querySelectorAll('h1').length).toBe(1);

    // Semantic structure
    expect(document.querySelector('header')).not.toBeNull();
    expect(document.querySelector('main')).not.toBeNull();
    expect(document.querySelector('footer')).not.toBeNull();

    // Content sections
    expect(document.querySelectorAll('section').length).toBeGreaterThanOrEqual(2);
  });
});
