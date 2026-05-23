/**
 * Unit tests for SEO and Performance meta tags (Scenario 12).
 *
 * Tests:
 * - Title tag content and length
 * - Meta description presence and length
 * - Open Graph meta tags
 * - Twitter Card meta tags
 * - Canonical URL link
 * - JSON-LD structured data
 * - HTML lang attribute
 * - Viewport meta tag
 */

import { describe, test, expect, beforeAll } from 'vitest';
import { readFileSync } from 'fs';
import { resolve } from 'path';

describe('SEO Meta Tags', () => {
  let html;
  let parser;
  let doc;

  beforeAll(() => {
    const htmlPath = resolve(process.cwd(), 'index.html');
    html = readFileSync(htmlPath, 'utf-8');
    parser = new DOMParser();
    doc = parser.parseFromString(html, 'text/html');
  });

  test('title contains "MirDB" and descriptive text; length between 30-60 characters', () => {
    const title = doc.querySelector('title');
    expect(title).not.toBeNull();
    const titleText = title.textContent.trim();
    expect(titleText).toContain('MirDB');
    expect(titleText.toLowerCase()).toMatch(/persistent|key-value|store|memcached/);
    expect(titleText.length).toBeGreaterThanOrEqual(30);
    expect(titleText.length).toBeLessThanOrEqual(60);
  });

  test('meta description exists with content describing MirDB; length between 120-160 characters', () => {
    const metaDesc = doc.querySelector('meta[name="description"]');
    expect(metaDesc).not.toBeNull();
    const content = metaDesc.getAttribute('content');
    expect(content).not.toBeNull();
    expect(content.length).toBeGreaterThanOrEqual(120);
    expect(content.length).toBeLessThanOrEqual(160);
    const lower = content.toLowerCase();
    expect(lower).toContain('mirdb');
    expect(lower).toContain('persistent');
    expect(lower).toContain('key-value');
  });

  test('Open Graph meta tags exist with appropriate content', () => {
    const ogTitle = doc.querySelector('meta[property="og:title"]');
    expect(ogTitle).not.toBeNull();
    expect(ogTitle.getAttribute('content')).toContain('MirDB');

    const ogDesc = doc.querySelector('meta[property="og:description"]');
    expect(ogDesc).not.toBeNull();
    expect(ogDesc.getAttribute('content').length).toBeGreaterThan(0);

    const ogType = doc.querySelector('meta[property="og:type"]');
    expect(ogType).not.toBeNull();
    expect(ogType.getAttribute('content')).toBeTruthy();

    const ogUrl = doc.querySelector('meta[property="og:url"]');
    expect(ogUrl).not.toBeNull();
    const ogUrlValue = ogUrl.getAttribute('content');
    expect(ogUrlValue).toBeTruthy();
    expect(ogUrlValue.startsWith('http')).toBe(true);
  });

  test('Twitter Card meta tags exist', () => {
    const twitterCard = doc.querySelector('meta[name="twitter:card"]');
    expect(twitterCard).not.toBeNull();
    expect(twitterCard.getAttribute('content')).toBeTruthy();

    const twitterTitle = doc.querySelector('meta[name="twitter:title"]');
    expect(twitterTitle).not.toBeNull();
    expect(twitterTitle.getAttribute('content')).toContain('MirDB');

    const twitterDesc = doc.querySelector('meta[name="twitter:description"]');
    expect(twitterDesc).not.toBeNull();
    expect(twitterDesc.getAttribute('content').length).toBeGreaterThan(0);
  });

  test('canonical link exists in head with href pointing to homepage URL', () => {
    const canonical = doc.querySelector('link[rel="canonical"]');
    expect(canonical).not.toBeNull();
    const href = canonical.getAttribute('href');
    expect(href).toBeTruthy();
    expect(href.startsWith('http')).toBe(true);
  });

  test('JSON-LD structured data exists with SoftwareApplication or Organization schema', () => {
    const jsonLdScript = doc.querySelector('script[type="application/ld+json"]');
    expect(jsonLdScript).not.toBeNull();

    const jsonText = jsonLdScript.textContent.trim();
    expect(jsonText.length).toBeGreaterThan(0);

    let structuredData;
    try {
      structuredData = JSON.parse(jsonText);
    } catch (e) {
      throw new Error('JSON-LD structured data is not valid JSON: ' + e.message);
    }

    expect(structuredData['@context']).toBe('https://schema.org');
    const validTypes = ['SoftwareApplication', 'Organization', 'WebApplication', 'Product'];
    expect(validTypes).toContain(structuredData['@type']);
    expect(structuredData.name).toContain('MirDB');
  });

  test('html element has lang="en"', () => {
    const htmlEl = doc.querySelector('html');
    expect(htmlEl).not.toBeNull();
    expect(htmlEl.getAttribute('lang')).toBe('en');
  });

  test('viewport meta tag exists with correct content', () => {
    const viewport = doc.querySelector('meta[name="viewport"]');
    expect(viewport).not.toBeNull();
    expect(viewport.getAttribute('content')).toBe('width=device-width, initial-scale=1.0');
  });

  test('favicon link exists in head', () => {
    const favicon = doc.querySelector('link[rel="icon"]');
    expect(favicon).not.toBeNull();
    const href = favicon.getAttribute('href');
    expect(href).toBeTruthy();
  });
});
