/**
 * Scenario 9 - SEO Meta Tags and Document Head Integration Tests
 * Owner: Scenario 9
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { JSDOM } from 'jsdom';
import fs from 'fs';
import path from 'path';

const __dirname = path.dirname(new URL(import.meta.url).pathname);
const INDEX_PATH = path.resolve(__dirname, '../../index.html');

function loadHomepage() {
  const html = fs.readFileSync(INDEX_PATH, 'utf-8');
  return new JSDOM(html, {
    url: 'http://localhost:3000',
    runScripts: 'dangerously',
    resources: 'usable',
  });
}

describe('SEO Meta Tags and Document Head', () => {
  let dom;
  let document;

  beforeEach(() => {
    dom = loadHomepage();
    document = dom.window.document;
  });

  afterEach(() => {
    dom = null;
    document = null;
  });

  // Test Case 1: Document title mentions MirDB and describes the product
  it('should have a title containing MirDB and describing the product', () => {
    const title = document.title;
    expect(title).not.toBeNull();
    expect(title.length).toBeGreaterThan(0);
    expect(title).toContain('MirDB');
    const hasDescription = /(Memcached|key.value)/i.test(title);
    expect(hasDescription).toBe(true);
  });

  // Test Case 2: Meta description exists with non-empty content of 50-160 characters
  it('should have a meta description between 50 and 160 characters mentioning MirDB', () => {
    const metaDesc = document.querySelector('meta[name="description"]');
    expect(metaDesc).not.toBeNull();
    const content = metaDesc.getAttribute('content');
    expect(content).not.toBeNull();
    expect(content.length).toBeGreaterThanOrEqual(50);
    expect(content.length).toBeLessThanOrEqual(160);
    expect(content).toContain('MirDB');
  });

  // Test Case 3: Viewport meta tag is present with correct content
  it('should have a viewport meta tag with width=device-width and initial-scale=1', () => {
    const viewport = document.querySelector('meta[name="viewport"]');
    expect(viewport).not.toBeNull();
    const content = viewport.getAttribute('content');
    expect(content).toContain('width=device-width');
    expect(content).toContain('initial-scale=1');
  });

  // Test Case 4: At least 3 Open Graph meta tags are present
  it('should have at least 3 Open Graph meta tags (og:title, og:description, og:type)', () => {
    const ogTags = document.querySelectorAll('meta[property^="og:"]');
    expect(ogTags.length).toBeGreaterThanOrEqual(3);

    const ogTitle = document.querySelector('meta[property="og:title"]');
    expect(ogTitle).not.toBeNull();

    const ogDescription = document.querySelector('meta[property="og:description"]');
    expect(ogDescription).not.toBeNull();

    const ogType = document.querySelector('meta[property="og:type"]');
    expect(ogType).not.toBeNull();
  });

  // Test Case 5: Canonical link is present with a non-empty absolute URL
  it('should have a canonical link with a non-empty absolute URL', () => {
    const canonical = document.querySelector('link[rel="canonical"]');
    expect(canonical).not.toBeNull();
    const href = canonical.getAttribute('href');
    expect(href).not.toBeNull();
    expect(href.length).toBeGreaterThan(0);
    // Check that it's an absolute URL (starts with http:// or https://)
    expect(href).toMatch(/^https?:\/\//);
  });

  // Test Case 6: HTML lang attribute is set and charset is utf-8
  it('should have html lang attribute set and meta charset utf-8', () => {
    const htmlEl = document.documentElement;
    expect(htmlEl).not.toBeNull();
    const lang = htmlEl.getAttribute('lang');
    expect(lang).not.toBeNull();
    expect(lang.length).toBeGreaterThan(0);

    const charsetMeta = document.querySelector('meta[charset]');
    expect(charsetMeta).not.toBeNull();
    const charset = charsetMeta.getAttribute('charset');
    expect(charset.toLowerCase()).toBe('utf-8');
  });
});
