/**
 * SEO and Meta Tags Unit Tests
 * Owner: Scenario 19 - SEO and Meta Tags
 *
 * Test cases:
 * - Title tag is present and includes 'MirDB'
 * - Meta description is present
 * - Open Graph tags are present (og:title, og:description, og:image)
 * - Canonical URL is set
 * - Single h1 tag exists
 */

const { test, describe, before } = require('node:test');
const assert = require('node:assert');
const fs = require('node:fs');
const path = require('node:path');

describe('SEO and Meta Tags', () => {
  let htmlContent;

  before(() => {
    const htmlPath = path.join(__dirname, '../../index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf-8');
  });

  test('Title tag is present with descriptive content including MirDB', () => {
    // Check title tag exists
    const titleMatch = htmlContent.match(/<title>([^<]+)<\/title>/i);
    assert.ok(titleMatch, 'Title tag should be present');

    const titleContent = titleMatch[1];
    assert.ok(titleContent.includes('MirDB'), 'Title should include "MirDB"');

    // Check title is descriptive (more than just "MirDB")
    assert.ok(titleContent.length > 10, 'Title should be descriptive');

    // Verify title describes the product value proposition
    assert.ok(
      titleContent.toLowerCase().includes('key-value') ||
      titleContent.toLowerCase().includes('memcached') ||
      titleContent.toLowerCase().includes('persistent'),
      'Title should describe MirDB value proposition'
    );
  });

  test('Meta description is present describing MirDB value proposition', () => {
    // Check meta description exists
    const descriptionMatch = htmlContent.match(/<meta\s+name=["']description["']\s+content=["']([^"']+)["']/i);
    assert.ok(descriptionMatch, 'Meta description should be present');

    const description = descriptionMatch[1];

    // Check description is substantial
    assert.ok(description.length >= 50, 'Meta description should be at least 50 characters');
    assert.ok(description.length <= 160, 'Meta description should be at most 160 characters for optimal SEO');

    // Check description mentions key aspects
    assert.ok(
      description.toLowerCase().includes('mirdb') ||
      description.toLowerCase().includes('key-value') ||
      description.toLowerCase().includes('memcached'),
      'Meta description should mention MirDB or its key features'
    );
  });

  test('Open Graph tags are present for social sharing (og:title, og:description, og:image)', () => {
    // Check og:title
    const ogTitleMatch = htmlContent.match(/<meta\s+property=["']og:title["']\s+content=["']([^"']+)["']/i);
    assert.ok(ogTitleMatch, 'og:title should be present');
    assert.ok(ogTitleMatch[1].includes('MirDB'), 'og:title should include MirDB');

    // Check og:description
    const ogDescriptionMatch = htmlContent.match(/<meta\s+property=["']og:description["']\s+content=["']([^"']+)["']/i);
    assert.ok(ogDescriptionMatch, 'og:description should be present');
    assert.ok(ogDescriptionMatch[1].length > 20, 'og:description should be descriptive');

    // Check og:image
    const ogImageMatch = htmlContent.match(/<meta\s+property=["']og:image["']\s+content=["']([^"']+)["']/i);
    assert.ok(ogImageMatch, 'og:image should be present for social sharing');

    const ogImage = ogImageMatch[1];
    assert.ok(
      ogImage.startsWith('http://') || ogImage.startsWith('https://'),
      'og:image should be an absolute URL'
    );
    assert.ok(
      ogImage.endsWith('.png') || ogImage.endsWith('.jpg') || ogImage.endsWith('.jpeg') || ogImage.endsWith('.webp'),
      'og:image should reference an image file'
    );
  });

  test('Canonical link element is present', () => {
    // Check canonical link
    const canonicalMatch = htmlContent.match(/<link\s+rel=["']canonical["']\s+href=["']([^"']+)["']/i);
    assert.ok(canonicalMatch, 'Canonical link should be present');

    const canonicalUrl = canonicalMatch[1];
    assert.ok(
      canonicalUrl.startsWith('http://') || canonicalUrl.startsWith('https://'),
      'Canonical URL should be an absolute URL'
    );
    assert.ok(canonicalUrl.length > 10, 'Canonical URL should be a valid URL');
  });

  test('Page has exactly one h1 tag in hero section', () => {
    // Find all h1 tags
    const h1Matches = htmlContent.match(/<h1[^>]*>[\s\S]*?<\/h1>/gi);

    assert.ok(h1Matches, 'Page should have at least one h1 tag');
    assert.strictEqual(h1Matches.length, 1, 'Page should have exactly one h1 tag for SEO');

    // Check h1 is in hero section (between hero comments or within hero section class)
    const heroSection = htmlContent.match(/<!-- BEGIN: hero -->[\s\S]*?<!-- END: hero -->/i);
    assert.ok(heroSection, 'Hero section should exist');

    const h1InHero = heroSection[0].match(/<h1[^>]*>[\s\S]*?<\/h1>/gi);
    assert.ok(h1InHero, 'The h1 tag should be within the hero section');
    assert.strictEqual(h1InHero.length, 1, 'There should be exactly one h1 in the hero section');
  });
});
