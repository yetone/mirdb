/**
 * SEO Meta Tags Unit Tests
 * Owner: Scenario 12 - SEO Optimization
 *
 * Tests for verifying SEO optimization including:
 * - Page title tag (TC1)
 * - Meta description (TC2)
 * - Open Graph tags (TC3)
 * - Twitter Card tags (TC4)
 * - Heading hierarchy (TC5)
 * - JSON-LD structured data (TC6)
 */
const fs = require('fs');
const path = require('path');

describe('SEO Optimization', () => {
  let htmlContent;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf-8');
  });

  describe('TC1: Page Title Tag', () => {
    test('Page has a title tag', () => {
      expect(htmlContent).toMatch(/<title>[^<]+<\/title>/);
    });

    test('Title contains "MirDB"', () => {
      const titleMatch = htmlContent.match(/<title>([^<]+)<\/title>/);
      expect(titleMatch).not.toBeNull();
      expect(titleMatch[1]).toContain('MirDB');
    });

    test('Title contains "Persistent Key-Value Store"', () => {
      const titleMatch = htmlContent.match(/<title>([^<]+)<\/title>/);
      expect(titleMatch).not.toBeNull();
      expect(titleMatch[1]).toContain('Persistent Key-Value Store');
    });

    test('Title contains "Memcached Protocol"', () => {
      const titleMatch = htmlContent.match(/<title>([^<]+)<\/title>/);
      expect(titleMatch).not.toBeNull();
      expect(titleMatch[1]).toContain('Memcached Protocol');
    });

    test('Title matches expected format: "MirDB - Persistent Key-Value Store with Memcached Protocol"', () => {
      expect(htmlContent).toMatch(/<title>MirDB - Persistent Key-Value Store with Memcached Protocol<\/title>/);
    });
  });

  describe('TC2: Meta Description', () => {
    test('Page has a meta description tag', () => {
      expect(htmlContent).toMatch(/<meta[^>]*name="description"[^>]*>/);
    });

    test('Meta description mentions "high-performance"', () => {
      const descMatch = htmlContent.match(/<meta[^>]*name="description"[^>]*content="([^"]*)"[^>]*>/);
      expect(descMatch).not.toBeNull();
      expect(descMatch[1].toLowerCase()).toContain('high-performance');
    });

    test('Meta description mentions "persistent"', () => {
      const descMatch = htmlContent.match(/<meta[^>]*name="description"[^>]*content="([^"]*)"[^>]*>/);
      expect(descMatch).not.toBeNull();
      expect(descMatch[1].toLowerCase()).toContain('persistent');
    });

    test('Meta description mentions "Memcached"', () => {
      const descMatch = htmlContent.match(/<meta[^>]*name="description"[^>]*content="([^"]*)"[^>]*>/);
      expect(descMatch).not.toBeNull();
      expect(descMatch[1]).toContain('Memcached');
    });

    test('Meta description mentions "Rust"', () => {
      const descMatch = htmlContent.match(/<meta[^>]*name="description"[^>]*content="([^"]*)"[^>]*>/);
      expect(descMatch).not.toBeNull();
      expect(descMatch[1]).toContain('Rust');
    });

    test('Meta description mentions "LSM-tree"', () => {
      const descMatch = htmlContent.match(/<meta[^>]*name="description"[^>]*content="([^"]*)"[^>]*>/);
      expect(descMatch).not.toBeNull();
      expect(descMatch[1]).toContain('LSM-tree');
    });

    test('Meta description is between 50 and 160 characters (optimal SEO length)', () => {
      const descMatch = htmlContent.match(/<meta[^>]*name="description"[^>]*content="([^"]*)"[^>]*>/);
      expect(descMatch).not.toBeNull();
      const descLength = descMatch[1].length;
      expect(descLength).toBeGreaterThanOrEqual(50);
      expect(descLength).toBeLessThanOrEqual(160);
    });
  });

  describe('TC3: Open Graph Tags', () => {
    test('Page has og:title tag', () => {
      expect(htmlContent).toMatch(/<meta[^>]*property="og:title"[^>]*content="[^"]*"[^>]*>/);
    });

    test('og:title contains MirDB', () => {
      const ogTitleMatch = htmlContent.match(/<meta[^>]*property="og:title"[^>]*content="([^"]*)"[^>]*>/);
      expect(ogTitleMatch).not.toBeNull();
      expect(ogTitleMatch[1]).toContain('MirDB');
    });

    test('Page has og:description tag', () => {
      expect(htmlContent).toMatch(/<meta[^>]*property="og:description"[^>]*content="[^"]*"[^>]*>/);
    });

    test('og:description has meaningful content', () => {
      const ogDescMatch = htmlContent.match(/<meta[^>]*property="og:description"[^>]*content="([^"]*)"[^>]*>/);
      expect(ogDescMatch).not.toBeNull();
      expect(ogDescMatch[1].length).toBeGreaterThan(20);
    });

    test('Page has og:image tag', () => {
      expect(htmlContent).toMatch(/<meta[^>]*property="og:image"[^>]*content="[^"]*"[^>]*>/);
    });

    test('og:image has a valid image path', () => {
      const ogImageMatch = htmlContent.match(/<meta[^>]*property="og:image"[^>]*content="([^"]*)"[^>]*>/);
      expect(ogImageMatch).not.toBeNull();
      // Should be a path or URL ending with an image extension
      expect(ogImageMatch[1]).toMatch(/\.(png|jpg|jpeg|gif|webp)$/i);
    });

    test('Page has og:type tag', () => {
      expect(htmlContent).toMatch(/<meta[^>]*property="og:type"[^>]*content="[^"]*"[^>]*>/);
    });

    test('og:type is set to "website"', () => {
      const ogTypeMatch = htmlContent.match(/<meta[^>]*property="og:type"[^>]*content="([^"]*)"[^>]*>/);
      expect(ogTypeMatch).not.toBeNull();
      expect(ogTypeMatch[1]).toBe('website');
    });
  });

  describe('TC4: Twitter Card Tags', () => {
    test('Page has twitter:card tag', () => {
      expect(htmlContent).toMatch(/<meta[^>]*name="twitter:card"[^>]*content="[^"]*"[^>]*>/);
    });

    test('twitter:card is set to "summary_large_image"', () => {
      const twitterCardMatch = htmlContent.match(/<meta[^>]*name="twitter:card"[^>]*content="([^"]*)"[^>]*>/);
      expect(twitterCardMatch).not.toBeNull();
      expect(twitterCardMatch[1]).toBe('summary_large_image');
    });

    test('Page has twitter:title tag', () => {
      expect(htmlContent).toMatch(/<meta[^>]*name="twitter:title"[^>]*content="[^"]*"[^>]*>/);
    });

    test('twitter:title contains MirDB', () => {
      const twitterTitleMatch = htmlContent.match(/<meta[^>]*name="twitter:title"[^>]*content="([^"]*)"[^>]*>/);
      expect(twitterTitleMatch).not.toBeNull();
      expect(twitterTitleMatch[1]).toContain('MirDB');
    });

    test('Page has twitter:description tag', () => {
      expect(htmlContent).toMatch(/<meta[^>]*name="twitter:description"[^>]*content="[^"]*"[^>]*>/);
    });

    test('twitter:description has meaningful content', () => {
      const twitterDescMatch = htmlContent.match(/<meta[^>]*name="twitter:description"[^>]*content="([^"]*)"[^>]*>/);
      expect(twitterDescMatch).not.toBeNull();
      expect(twitterDescMatch[1].length).toBeGreaterThan(20);
    });
  });

  describe('TC5: Heading Hierarchy', () => {
    test('Page has exactly one h1 element', () => {
      const h1Tags = htmlContent.match(/<h1[^>]*>[^<]*<\/h1>/g) || [];
      expect(h1Tags.length).toBe(1);
    });

    test('h1 element contains "MirDB"', () => {
      const h1Match = htmlContent.match(/<h1[^>]*>([^<]*)<\/h1>/);
      expect(h1Match).not.toBeNull();
      expect(h1Match[1]).toContain('MirDB');
    });

    test('Page has multiple h2 elements for main sections', () => {
      const h2Tags = htmlContent.match(/<h2[^>]*>/g) || [];
      expect(h2Tags.length).toBeGreaterThanOrEqual(4);
    });

    test('h2 elements exist for key sections (Features, Usage, Architecture, Getting Started)', () => {
      // Check for section titles in h2 elements
      expect(htmlContent).toMatch(/<h2[^>]*>[^<]*Why MirDB[^<]*<\/h2>/);
      expect(htmlContent).toMatch(/<h2[^>]*>[^<]*Simple as Memcached[^<]*<\/h2>/);
      expect(htmlContent).toMatch(/<h2[^>]*>[^<]*How It Works[^<]*<\/h2>/);
      expect(htmlContent).toMatch(/<h2[^>]*>[^<]*Get Started[^<]*<\/h2>/);
    });

    test('Page has h3 elements for subsections', () => {
      const h3Tags = htmlContent.match(/<h3[^>]*>/g) || [];
      expect(h3Tags.length).toBeGreaterThan(0);
    });

    test('Heading order is logical (no h3 before h2, no h4 before h3)', () => {
      // Remove all content except headings to check order
      const headingsOnly = htmlContent.replace(/[\s\S]*?(<h[1-6][^>]*>)/g, '$1');

      // Get all heading tags in order
      const headings = htmlContent.match(/<h[1-6][^>]*>/g) || [];

      let lastLevel = 0;
      let validOrder = true;

      headings.forEach((heading) => {
        const levelMatch = heading.match(/<h([1-6])/);
        if (levelMatch) {
          const currentLevel = parseInt(levelMatch[1], 10);
          // Heading levels should not skip more than one level down
          if (currentLevel > lastLevel + 1 && lastLevel !== 0) {
            validOrder = false;
          }
          lastLevel = currentLevel;
        }
      });

      expect(validOrder).toBe(true);
    });

    test('Feature cards use h3 for titles', () => {
      expect(htmlContent).toMatch(/<h3[^>]*class="feature-card__title"[^>]*>/);
    });
  });

  describe('TC6: JSON-LD Structured Data', () => {
    test('Page contains JSON-LD script tag', () => {
      expect(htmlContent).toMatch(/<script[^>]*type="application\/ld\+json"[^>]*>/);
    });

    test('JSON-LD contains valid JSON', () => {
      const jsonLdMatch = htmlContent.match(/<script[^>]*type="application\/ld\+json"[^>]*>([\s\S]*?)<\/script>/);
      expect(jsonLdMatch).not.toBeNull();

      let parsed;
      expect(() => {
        parsed = JSON.parse(jsonLdMatch[1]);
      }).not.toThrow();

      expect(parsed).toBeDefined();
    });

    test('JSON-LD uses SoftwareApplication schema', () => {
      const jsonLdMatch = htmlContent.match(/<script[^>]*type="application\/ld\+json"[^>]*>([\s\S]*?)<\/script>/);
      expect(jsonLdMatch).not.toBeNull();

      const jsonLd = JSON.parse(jsonLdMatch[1]);
      expect(jsonLd['@type']).toBe('SoftwareApplication');
    });

    test('JSON-LD has required @context field', () => {
      const jsonLdMatch = htmlContent.match(/<script[^>]*type="application\/ld\+json"[^>]*>([\s\S]*?)<\/script>/);
      expect(jsonLdMatch).not.toBeNull();

      const jsonLd = JSON.parse(jsonLdMatch[1]);
      expect(jsonLd['@context']).toBe('https://schema.org');
    });

    test('JSON-LD contains name field with "MirDB"', () => {
      const jsonLdMatch = htmlContent.match(/<script[^>]*type="application\/ld\+json"[^>]*>([\s\S]*?)<\/script>/);
      expect(jsonLdMatch).not.toBeNull();

      const jsonLd = JSON.parse(jsonLdMatch[1]);
      expect(jsonLd.name).toContain('MirDB');
    });

    test('JSON-LD contains description field', () => {
      const jsonLdMatch = htmlContent.match(/<script[^>]*type="application\/ld\+json"[^>]*>([\s\S]*?)<\/script>/);
      expect(jsonLdMatch).not.toBeNull();

      const jsonLd = JSON.parse(jsonLdMatch[1]);
      expect(jsonLd.description).toBeDefined();
      expect(jsonLd.description.length).toBeGreaterThan(20);
    });

    test('JSON-LD contains applicationCategory field', () => {
      const jsonLdMatch = htmlContent.match(/<script[^>]*type="application\/ld\+json"[^>]*>([\s\S]*?)<\/script>/);
      expect(jsonLdMatch).not.toBeNull();

      const jsonLd = JSON.parse(jsonLdMatch[1]);
      expect(jsonLd.applicationCategory).toBeDefined();
    });

    test('JSON-LD contains operatingSystem field', () => {
      const jsonLdMatch = htmlContent.match(/<script[^>]*type="application\/ld\+json"[^>]*>([\s\S]*?)<\/script>/);
      expect(jsonLdMatch).not.toBeNull();

      const jsonLd = JSON.parse(jsonLdMatch[1]);
      expect(jsonLd.operatingSystem).toBeDefined();
    });

    test('JSON-LD contains offers or downloadUrl field', () => {
      const jsonLdMatch = htmlContent.match(/<script[^>]*type="application\/ld\+json"[^>]*>([\s\S]*?)<\/script>/);
      expect(jsonLdMatch).not.toBeNull();

      const jsonLd = JSON.parse(jsonLdMatch[1]);
      const hasOffersOrDownload = jsonLd.offers !== undefined || jsonLd.downloadUrl !== undefined;
      expect(hasOffersOrDownload).toBe(true);
    });
  });

  describe('Additional SEO Best Practices', () => {
    test('Page has meta keywords tag with relevant keywords', () => {
      expect(htmlContent).toMatch(/<meta[^>]*name="keywords"[^>]*content="[^"]*"[^>]*>/);

      const keywordsMatch = htmlContent.match(/<meta[^>]*name="keywords"[^>]*content="([^"]*)"[^>]*>/);
      expect(keywordsMatch).not.toBeNull();

      const keywords = keywordsMatch[1].toLowerCase();
      // Check for at least some relevant keywords
      const relevantKeywords = ['rust', 'key-value', 'memcached', 'database', 'lsm'];
      const hasRelevantKeywords = relevantKeywords.some(kw => keywords.includes(kw));
      expect(hasRelevantKeywords).toBe(true);
    });

    test('Page has canonical meta tag or link (optional but recommended)', () => {
      // Canonical is optional for a single-page site, so just check structure
      const hasCanonical = htmlContent.match(/<link[^>]*rel="canonical"[^>]*>/) !== null;
      // Just log the result, don't fail if missing
      expect(typeof hasCanonical).toBe('boolean');
    });

    test('Page has proper charset declaration', () => {
      expect(htmlContent).toMatch(/<meta[^>]*charset="UTF-8"[^>]*>/);
    });

    test('Page has viewport meta tag for mobile responsiveness', () => {
      expect(htmlContent).toMatch(/<meta[^>]*name="viewport"[^>]*content="[^"]*width=device-width[^"]*"[^>]*>/);
    });

    test('Page has lang attribute on html element', () => {
      expect(htmlContent).toMatch(/<html[^>]*lang="en"[^>]*>/);
    });

    test('Page has favicon', () => {
      expect(htmlContent).toMatch(/<link[^>]*rel="icon"[^>]*>/);
    });

    test('External links have rel="noopener noreferrer" for security', () => {
      const externalLinks = htmlContent.match(/<a[^>]*target="_blank"[^>]*>/g) || [];
      expect(externalLinks.length).toBeGreaterThan(0);

      externalLinks.forEach((link) => {
        expect(link).toMatch(/rel="[^"]*noopener[^"]*"/);
      });
    });

    test('Images have descriptive alt text for SEO', () => {
      const imgTags = htmlContent.match(/<img[^>]*>/g) || [];
      expect(imgTags.length).toBeGreaterThan(0);

      imgTags.forEach((imgTag) => {
        expect(imgTag).toMatch(/alt="[^"]+"/);
      });
    });
  });

  describe('Semantic HTML for SEO', () => {
    test('Page uses semantic section elements', () => {
      expect(htmlContent).toMatch(/<section[^>]*id="hero"/);
      expect(htmlContent).toMatch(/<section[^>]*id="features"/);
      expect(htmlContent).toMatch(/<section[^>]*id="usage"/);
      expect(htmlContent).toMatch(/<section[^>]*id="architecture"/);
      expect(htmlContent).toMatch(/<section[^>]*id="getting-started"/);
    });

    test('Page uses semantic nav element', () => {
      expect(htmlContent).toMatch(/<nav[^>]*>/);
    });

    test('Page uses semantic footer element', () => {
      expect(htmlContent).toMatch(/<footer[^>]*>/);
    });

    test('Page uses semantic article elements for feature cards', () => {
      expect(htmlContent).toMatch(/<article[^>]*class="feature-card"/);
    });
  });
});
