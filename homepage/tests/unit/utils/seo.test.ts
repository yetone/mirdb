import { describe, it, expect } from 'vitest';
import { generateSEOTags, validateTitle, validateDescription } from '../../../src/utils/seo';

describe('SEO Utility', () => {
  describe('generateSEOTags', () => {
    it('generates default tags without any props', () => {
      const tags = generateSEOTags();
      expect(tags.length).toBeGreaterThan(0);

      const titleTag = tags.find((t) => t.tag === 'title');
      expect(titleTag).toBeDefined();
      expect(titleTag!.attributes.innerHTML).toContain('MirDB');
    });

    it('generates title with product name and descriptor', () => {
      const tags = generateSEOTags();
      const titleTag = tags.find((t) => t.tag === 'title');
      const title = titleTag!.attributes.innerHTML;
      expect(title).toContain('MirDB');
      expect(title).toContain('Persistent Key-Value Store');
      expect(title.length).toBeGreaterThanOrEqual(30);
      expect(title.length).toBeLessThanOrEqual(70);
    });

    it('generates meta description tag with non-empty content between 50-160 chars', () => {
      const tags = generateSEOTags();
      const descTag = tags.find(
        (t) => t.tag === 'meta' && t.attributes.name === 'description',
      );
      expect(descTag).toBeDefined();
      const content = descTag!.attributes.content;
      expect(content.length).toBeGreaterThanOrEqual(50);
      expect(content.length).toBeLessThanOrEqual(160);
    });

    it('generates viewport meta tag', () => {
      const tags = generateSEOTags();
      const viewportTag = tags.find(
        (t) => t.tag === 'meta' && t.attributes.name === 'viewport',
      );
      expect(viewportTag).toBeDefined();
      expect(viewportTag!.attributes.content).toContain('width=device-width');
      expect(viewportTag!.attributes.content).toContain('initial-scale=1');
    });

    it('generates charset meta tag', () => {
      const tags = generateSEOTags();
      const charsetTag = tags.find(
        (t) => t.tag === 'meta' && t.attributes.charset === 'UTF-8',
      );
      expect(charsetTag).toBeDefined();
    });

    it('charset declaration is the first tag', () => {
      const tags = generateSEOTags();
      expect(tags[0].tag).toBe('meta');
      expect(tags[0].attributes.charset).toBe('UTF-8');
    });

    it('generates Open Graph title tag', () => {
      const tags = generateSEOTags();
      const ogTitle = tags.find(
        (t) => t.tag === 'meta' && t.attributes.property === 'og:title',
      );
      expect(ogTitle).toBeDefined();
      expect(ogTitle!.attributes.content).toContain('MirDB');
      expect(ogTitle!.attributes.content.length).toBeGreaterThan(0);
    });

    it('generates Open Graph description tag', () => {
      const tags = generateSEOTags();
      const ogDesc = tags.find(
        (t) => t.tag === 'meta' && t.attributes.property === 'og:description',
      );
      expect(ogDesc).toBeDefined();
      expect(ogDesc!.attributes.content.length).toBeGreaterThan(0);
    });

    it('generates Open Graph image tag', () => {
      const tags = generateSEOTags();
      const ogImage = tags.find(
        (t) => t.tag === 'meta' && t.attributes.property === 'og:image',
      );
      expect(ogImage).toBeDefined();
      expect(ogImage!.attributes.content).toBe('/assets/logo.gif');
    });

    it('generates Open Graph URL tag', () => {
      const tags = generateSEOTags();
      const ogUrl = tags.find(
        (t) => t.tag === 'meta' && t.attributes.property === 'og:url',
      );
      expect(ogUrl).toBeDefined();
      expect(ogUrl!.attributes.content.length).toBeGreaterThan(0);
    });

    it('generates Open Graph type tag with default website type', () => {
      const tags = generateSEOTags();
      const ogType = tags.find(
        (t) => t.tag === 'meta' && t.attributes.property === 'og:type',
      );
      expect(ogType).toBeDefined();
      expect(ogType!.attributes.content).toBe('website');
    });

    it('generates canonical link tag', () => {
      const tags = generateSEOTags();
      const canonical = tags.find(
        (t) => t.tag === 'link' && t.attributes.rel === 'canonical',
      );
      expect(canonical).toBeDefined();
      expect(canonical!.attributes.href).toBe('https://mirdb.dev');
    });

    it('accepts custom title and description', () => {
      const tags = generateSEOTags({
        title: 'Custom MirDB Title',
        description: 'A custom description for MirDB that is long enough to pass validation checks.',
        ogImage: '/custom-image.png',
        canonical: 'https://example.com/custom',
        ogType: 'article',
      });

      const titleTag = tags.find((t) => t.tag === 'title');
      expect(titleTag!.attributes.innerHTML).toBe('Custom MirDB Title');

      const descTag = tags.find(
        (t) => t.tag === 'meta' && t.attributes.name === 'description',
      );
      expect(descTag!.attributes.content).toBe(
        'A custom description for MirDB that is long enough to pass validation checks.',
      );

      const ogImage = tags.find(
        (t) => t.tag === 'meta' && t.attributes.property === 'og:image',
      );
      expect(ogImage!.attributes.content).toBe('/custom-image.png');

      const ogType = tags.find(
        (t) => t.tag === 'meta' && t.attributes.property === 'og:type',
      );
      expect(ogType!.attributes.content).toBe('article');
    });

    it('OG image points to a valid image asset path', () => {
      const tags = generateSEOTags();
      const ogImage = tags.find(
        (t) => t.tag === 'meta' && t.attributes.property === 'og:image',
      );
      expect(ogImage!.attributes.content).toMatch(/\.(gif|png|jpg|jpeg|svg|webp)$/i);
    });

    it('all OG required tags are present with non-empty content', () => {
      const tags = generateSEOTags();
      const requiredProperties = ['og:title', 'og:description', 'og:image', 'og:url', 'og:type'];
      for (const prop of requiredProperties) {
        const tag = tags.find(
          (t) => t.tag === 'meta' && t.attributes.property === prop,
        );
        expect(tag).toBeDefined();
        expect(tag!.attributes.content.length).toBeGreaterThan(0);
      }
    });
  });

  describe('validateTitle', () => {
    it('accepts a title within 30-70 characters', () => {
      const result = validateTitle('MirDB - Persistent Key-Value Store');
      expect(result.valid).toBe(true);
      expect(result.length).toBeGreaterThanOrEqual(30);
      expect(result.length).toBeLessThanOrEqual(70);
    });

    it('rejects a title shorter than 30 characters', () => {
      const result = validateTitle('MirDB');
      expect(result.valid).toBe(false);
      expect(result.reason).toContain('short');
    });

    it('rejects a title longer than 70 characters', () => {
      const longTitle = 'MirDB - A Very Long Title That Exceeds The Recommended Maximum Length For SEO Purposes';
      const result = validateTitle(longTitle);
      expect(result.valid).toBe(false);
      expect(result.reason).toContain('long');
    });
  });

  describe('validateDescription', () => {
    it('accepts a description within 50-160 characters', () => {
      const result = validateDescription(
        'MirDB is a high-performance persistent key-value store with Memcached protocol compatibility, built in Rust.',
      );
      expect(result.valid).toBe(true);
    });

    it('rejects a description shorter than 50 characters', () => {
      const result = validateDescription('Short desc');
      expect(result.valid).toBe(false);
      expect(result.reason).toContain('short');
    });

    it('rejects a description longer than 160 characters', () => {
      const longDesc = 'MirDB is a high-performance persistent key-value store. '.repeat(10);
      const result = validateDescription(longDesc);
      expect(result.valid).toBe(false);
      expect(result.reason).toContain('long');
    });
  });
});
