/**
 * SEO and Meta Tags Tests
 * Owner: Scenario 12 - SEO and Meta Tags
 *
 * Tests for:
 * - Title tag content
 * - Meta description
 * - Viewport meta tag
 * - Open Graph tags
 * - Canonical URL
 * - Twitter card tags
 */

const { loadHomepageHTML } = require('../setup/test-utils');

describe('SEO and Meta Tags', () => {
    let htmlContent;

    beforeAll(() => {
        htmlContent = loadHomepageHTML();
    });

    beforeEach(() => {
        document.documentElement.innerHTML = htmlContent;
    });

    describe('Title Tag', () => {
        it('should have a title tag', () => {
            const title = document.querySelector('title');
            expect(title).not.toBeNull();
        });

        it('should contain MirDB in the title', () => {
            const title = document.querySelector('title');
            expect(title.textContent).toMatch(/MirDB/i);
        });

        it('should contain relevant keywords in the title', () => {
            const title = document.querySelector('title');
            const titleText = title.textContent.toLowerCase();
            // Title should mention key product aspects
            expect(
                titleText.includes('key-value') ||
                titleText.includes('memcached') ||
                titleText.includes('persistent')
            ).toBe(true);
        });
    });

    describe('Meta Description', () => {
        it('should have a meta description tag', () => {
            const metaDesc = document.querySelector('meta[name="description"]');
            expect(metaDesc).not.toBeNull();
        });

        it('should have a compelling description under 160 characters', () => {
            const metaDesc = document.querySelector('meta[name="description"]');
            const content = metaDesc.getAttribute('content');
            expect(content).toBeTruthy();
            expect(content.length).toBeLessThanOrEqual(160);
            expect(content.length).toBeGreaterThan(50); // Should be meaningful
        });

        it('should contain relevant keywords in the description', () => {
            const metaDesc = document.querySelector('meta[name="description"]');
            const content = metaDesc.getAttribute('content').toLowerCase();
            expect(
                content.includes('mirdb') ||
                content.includes('key-value') ||
                content.includes('memcached')
            ).toBe(true);
        });
    });

    describe('Viewport Meta Tag', () => {
        it('should have a viewport meta tag', () => {
            const viewport = document.querySelector('meta[name="viewport"]');
            expect(viewport).not.toBeNull();
        });

        it('should set width=device-width for responsive design', () => {
            const viewport = document.querySelector('meta[name="viewport"]');
            const content = viewport.getAttribute('content');
            expect(content).toMatch(/width=device-width/);
        });

        it('should set initial-scale=1.0', () => {
            const viewport = document.querySelector('meta[name="viewport"]');
            const content = viewport.getAttribute('content');
            expect(content).toMatch(/initial-scale=1(\.0)?/);
        });
    });

    describe('Open Graph Tags', () => {
        it('should have og:title tag', () => {
            const ogTitle = document.querySelector('meta[property="og:title"]');
            expect(ogTitle).not.toBeNull();
            expect(ogTitle.getAttribute('content')).toBeTruthy();
        });

        it('should have og:description tag', () => {
            const ogDesc = document.querySelector('meta[property="og:description"]');
            expect(ogDesc).not.toBeNull();
            expect(ogDesc.getAttribute('content')).toBeTruthy();
        });

        it('should have og:image tag', () => {
            const ogImage = document.querySelector('meta[property="og:image"]');
            expect(ogImage).not.toBeNull();
            expect(ogImage.getAttribute('content')).toBeTruthy();
        });

        it('should have og:type tag set to website', () => {
            const ogType = document.querySelector('meta[property="og:type"]');
            expect(ogType).not.toBeNull();
            expect(ogType.getAttribute('content')).toBe('website');
        });

        it('should have og:title containing MirDB', () => {
            const ogTitle = document.querySelector('meta[property="og:title"]');
            expect(ogTitle.getAttribute('content')).toMatch(/MirDB/i);
        });
    });

    describe('Canonical URL', () => {
        it('should have a canonical link tag', () => {
            const canonical = document.querySelector('link[rel="canonical"]');
            expect(canonical).not.toBeNull();
        });

        it('should point to a valid URL', () => {
            const canonical = document.querySelector('link[rel="canonical"]');
            const href = canonical.getAttribute('href');
            expect(href).toBeTruthy();
            // Should be a valid URL format (starts with http or https)
            expect(href).toMatch(/^https?:\/\//);
        });
    });

    describe('Twitter Card Tags', () => {
        it('should have twitter:card tag', () => {
            const twitterCard = document.querySelector('meta[name="twitter:card"]');
            expect(twitterCard).not.toBeNull();
            expect(twitterCard.getAttribute('content')).toBeTruthy();
        });

        it('should have twitter:title tag', () => {
            const twitterTitle = document.querySelector('meta[name="twitter:title"]');
            expect(twitterTitle).not.toBeNull();
            expect(twitterTitle.getAttribute('content')).toBeTruthy();
        });

        it('should have twitter:description tag', () => {
            const twitterDesc = document.querySelector('meta[name="twitter:description"]');
            expect(twitterDesc).not.toBeNull();
            expect(twitterDesc.getAttribute('content')).toBeTruthy();
        });

        it('should have twitter:image tag for rich sharing', () => {
            const twitterImage = document.querySelector('meta[name="twitter:image"]');
            expect(twitterImage).not.toBeNull();
            expect(twitterImage.getAttribute('content')).toBeTruthy();
        });

        it('should use summary_large_image card type for rich sharing', () => {
            const twitterCard = document.querySelector('meta[name="twitter:card"]');
            expect(twitterCard.getAttribute('content')).toBe('summary_large_image');
        });

        it('should have twitter:title containing MirDB', () => {
            const twitterTitle = document.querySelector('meta[name="twitter:title"]');
            expect(twitterTitle.getAttribute('content')).toMatch(/MirDB/i);
        });
    });

    describe('Additional SEO Elements', () => {
        it('should have charset meta tag', () => {
            const charset = document.querySelector('meta[charset]');
            expect(charset).not.toBeNull();
            expect(charset.getAttribute('charset').toLowerCase()).toBe('utf-8');
        });

        it('should have html lang attribute for accessibility and SEO', () => {
            // Note: When loading HTML via innerHTML, the html element's attributes
            // need to be verified from the raw HTML content
            expect(htmlContent).toMatch(/<html[^>]*\slang=["']en["']/);
        });
    });
});
