/**
 * SEO and Meta Tags Tests
 *
 * This test suite validates that the MirDB homepage has proper SEO meta tags
 * for discoverability, including title, meta description, Open Graph tags,
 * favicon, and structured data (JSON-LD).
 */

const fs = require('fs');
const path = require('path');
const cheerio = require('cheerio');

// Load the homepage HTML
const htmlPath = path.join(__dirname, '..', 'index.html');
const htmlContent = fs.readFileSync(htmlPath, 'utf-8');
const $ = cheerio.load(htmlContent);

describe('SEO and Meta Tags', () => {
    describe('Test Case 1: Page Title Tag', () => {
        it('should have a <title> tag', () => {
            const title = $('title');
            expect(title.length).toBe(1);
        });

        it('should have a non-empty title', () => {
            const title = $('title').text().trim();
            expect(title.length).toBeGreaterThan(0);
        });

        it("should have a title containing 'MirDB'", () => {
            const title = $('title').text().trim();
            expect(title.toLowerCase()).toContain('mirdb');
        });

        it('should have a descriptive title (more than just product name)', () => {
            const title = $('title').text().trim();
            // Title should be descriptive, not just "MirDB"
            expect(title.length).toBeGreaterThan(10);
        });
    });

    describe('Test Case 2: Meta Description', () => {
        it('should have a <meta name="description"> tag', () => {
            const metaDescription = $('meta[name="description"]');
            expect(metaDescription.length).toBe(1);
        });

        it('should have a non-empty meta description', () => {
            const metaDescription = $('meta[name="description"]').attr('content');
            expect(metaDescription).toBeDefined();
            expect(metaDescription.trim().length).toBeGreaterThan(0);
        });

        it('should have a meaningful meta description (50+ characters)', () => {
            const metaDescription = $('meta[name="description"]').attr('content');
            expect(metaDescription.trim().length).toBeGreaterThanOrEqual(50);
        });

        it("should have meta description mentioning MirDB's purpose", () => {
            const metaDescription = $('meta[name="description"]').attr('content').toLowerCase();
            // Should mention key aspects of MirDB
            const hasKeywords = metaDescription.includes('key-value') ||
                               metaDescription.includes('memcached') ||
                               metaDescription.includes('database') ||
                               metaDescription.includes('persistent');
            expect(hasKeywords).toBe(true);
        });
    });

    describe('Test Case 3: Open Graph og:title Tag', () => {
        it('should have a <meta property="og:title"> tag', () => {
            const ogTitle = $('meta[property="og:title"]');
            expect(ogTitle.length).toBe(1);
        });

        it('should have a non-empty og:title', () => {
            const ogTitle = $('meta[property="og:title"]').attr('content');
            expect(ogTitle).toBeDefined();
            expect(ogTitle.trim().length).toBeGreaterThan(0);
        });

        it("og:title should contain 'MirDB'", () => {
            const ogTitle = $('meta[property="og:title"]').attr('content');
            expect(ogTitle.toLowerCase()).toContain('mirdb');
        });
    });

    describe('Test Case 4: Open Graph og:description Tag', () => {
        it('should have a <meta property="og:description"> tag', () => {
            const ogDescription = $('meta[property="og:description"]');
            expect(ogDescription.length).toBe(1);
        });

        it('should have a non-empty og:description', () => {
            const ogDescription = $('meta[property="og:description"]').attr('content');
            expect(ogDescription).toBeDefined();
            expect(ogDescription.trim().length).toBeGreaterThan(0);
        });

        it('should have a meaningful og:description', () => {
            const ogDescription = $('meta[property="og:description"]').attr('content');
            expect(ogDescription.trim().length).toBeGreaterThanOrEqual(50);
        });
    });

    describe('Test Case 5: Favicon', () => {
        it('should have a favicon defined', () => {
            // Check for various favicon link types
            const favicon = $('link[rel="icon"], link[rel="shortcut icon"], link[rel="apple-touch-icon"]');
            expect(favicon.length).toBeGreaterThan(0);
        });

        it('favicon should have href attribute', () => {
            const favicon = $('link[rel="icon"], link[rel="shortcut icon"]').first();
            const href = favicon.attr('href');
            expect(href).toBeDefined();
            expect(href.trim().length).toBeGreaterThan(0);
        });
    });

    describe('Test Case 6: Structured Data (JSON-LD)', () => {
        it('should have a JSON-LD script tag for structured data', () => {
            const jsonLd = $('script[type="application/ld+json"]');
            expect(jsonLd.length).toBeGreaterThan(0);
        });

        it('should have valid JSON in the JSON-LD script', () => {
            const jsonLd = $('script[type="application/ld+json"]').html();
            expect(jsonLd).toBeDefined();

            let parsed;
            expect(() => {
                parsed = JSON.parse(jsonLd);
            }).not.toThrow();
        });

        it('should have @context defined in structured data', () => {
            const jsonLd = $('script[type="application/ld+json"]').html();
            const parsed = JSON.parse(jsonLd);
            expect(parsed['@context']).toBeDefined();
            expect(parsed['@context']).toContain('schema.org');
        });

        it('should have @type defined in structured data', () => {
            const jsonLd = $('script[type="application/ld+json"]').html();
            const parsed = JSON.parse(jsonLd);
            expect(parsed['@type']).toBeDefined();
        });

        it('should have name property in structured data containing MirDB', () => {
            const jsonLd = $('script[type="application/ld+json"]').html();
            const parsed = JSON.parse(jsonLd);
            expect(parsed.name).toBeDefined();
            expect(parsed.name.toLowerCase()).toContain('mirdb');
        });

        it('should have description property in structured data', () => {
            const jsonLd = $('script[type="application/ld+json"]').html();
            const parsed = JSON.parse(jsonLd);
            expect(parsed.description).toBeDefined();
            expect(parsed.description.length).toBeGreaterThan(0);
        });
    });

    describe('Additional SEO Best Practices', () => {
        it('should have og:type meta tag', () => {
            const ogType = $('meta[property="og:type"]');
            expect(ogType.length).toBe(1);
        });

        it('should have og:url meta tag', () => {
            const ogUrl = $('meta[property="og:url"]');
            expect(ogUrl.length).toBe(1);
        });

        it('should have og:image meta tag for social sharing', () => {
            const ogImage = $('meta[property="og:image"]');
            expect(ogImage.length).toBe(1);
            expect(ogImage.attr('content')).toBeDefined();
        });

        it('should have twitter:card meta tag', () => {
            const twitterCard = $('meta[name="twitter:card"]');
            expect(twitterCard.length).toBe(1);
        });

        it('should have canonical URL defined', () => {
            const canonical = $('link[rel="canonical"]');
            expect(canonical.length).toBe(1);
            expect(canonical.attr('href')).toBeDefined();
        });

        it('should have robots meta tag or default to index,follow', () => {
            const robots = $('meta[name="robots"]');
            // Either robots meta should allow indexing or not be present (defaults to index,follow)
            if (robots.length > 0) {
                const content = robots.attr('content').toLowerCase();
                expect(content).not.toContain('noindex');
            }
            // If no robots meta, page is indexable by default - this is fine
            expect(true).toBe(true);
        });
    });
});
