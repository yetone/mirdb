/**
 * SEO Integration Tests
 * Owner: Scenario 13 - SEO Optimization
 *
 * SEO validation tests:
 * - Meta tags (title, description)
 * - Open Graph tags
 * - Twitter Card tags
 * - Canonical URL
 * - robots.txt
 */

import { describe, it, expect, beforeAll } from 'vitest';
import * as fs from 'fs';
import * as path from 'path';
import { JSDOM } from 'jsdom';

describe('SEO Integration Tests - Scenario 13', () => {
  let document: Document;
  let htmlContent: string;

  beforeAll(() => {
    // Read the index.html file
    const indexHtmlPath = path.resolve(__dirname, '../../index.html');
    htmlContent = fs.readFileSync(indexHtmlPath, 'utf-8');
    const dom = new JSDOM(htmlContent);
    document = dom.window.document;
  });

  /**
   * Test Case 1: Check page title tag
   * Expected: Title tag contains 'MirDB' and is 50-60 characters
   */
  it('TC1: Page title contains MirDB and is 50-60 characters', () => {
    const title = document.querySelector('title');
    expect(title).not.toBeNull();

    const titleContent = title!.textContent || '';

    // Verify title contains 'MirDB'
    expect(titleContent).toContain('MirDB');

    // Verify title length is 50-60 characters
    const titleLength = titleContent.length;
    expect(titleLength).toBeGreaterThanOrEqual(50);
    expect(titleLength).toBeLessThanOrEqual(60);

    console.log(`Title: "${titleContent}" (${titleLength} characters)`);
  });

  /**
   * Test Case 2: Check meta description
   * Expected: Meta description is present and 150-160 characters
   */
  it('TC2: Meta description is present and 150-160 characters', () => {
    const metaDescription = document.querySelector('meta[name="description"]');
    expect(metaDescription).not.toBeNull();

    const content = metaDescription!.getAttribute('content');
    expect(content).toBeTruthy();

    // Verify description length is 150-160 characters
    const descriptionLength = content!.length;
    expect(descriptionLength).toBeGreaterThanOrEqual(150);
    expect(descriptionLength).toBeLessThanOrEqual(160);

    console.log(`Description: "${content}" (${descriptionLength} characters)`);
  });

  /**
   * Test Case 3: Check Open Graph tags
   * Expected: og:title, og:description, og:image tags are present
   */
  it('TC3: Open Graph tags are present (og:title, og:description, og:image)', () => {
    // Check og:title
    const ogTitle = document.querySelector('meta[property="og:title"]');
    expect(ogTitle).not.toBeNull();
    const ogTitleContent = ogTitle!.getAttribute('content');
    expect(ogTitleContent).toBeTruthy();
    expect(ogTitleContent).toContain('MirDB');

    // Check og:description
    const ogDescription = document.querySelector('meta[property="og:description"]');
    expect(ogDescription).not.toBeNull();
    const ogDescriptionContent = ogDescription!.getAttribute('content');
    expect(ogDescriptionContent).toBeTruthy();
    expect(ogDescriptionContent!.length).toBeGreaterThan(50);

    // Check og:image
    const ogImage = document.querySelector('meta[property="og:image"]');
    expect(ogImage).not.toBeNull();
    const ogImageContent = ogImage!.getAttribute('content');
    expect(ogImageContent).toBeTruthy();
    expect(ogImageContent).toMatch(/^https?:\/\/.+\.(png|jpg|jpeg|gif|webp)$/i);

    // Additional OG checks
    const ogType = document.querySelector('meta[property="og:type"]');
    expect(ogType).not.toBeNull();
    expect(ogType!.getAttribute('content')).toBe('website');

    const ogUrl = document.querySelector('meta[property="og:url"]');
    expect(ogUrl).not.toBeNull();
    expect(ogUrl!.getAttribute('content')).toMatch(/^https?:\/\//);

    console.log(`OG Title: "${ogTitleContent}"`);
    console.log(`OG Description: "${ogDescriptionContent}"`);
    console.log(`OG Image: "${ogImageContent}"`);
  });

  /**
   * Test Case 4: Check Twitter Card tags
   * Expected: Twitter card meta tags are present
   */
  it('TC4: Twitter Card meta tags are present', () => {
    // Check twitter:card
    const twitterCard = document.querySelector('meta[name="twitter:card"]');
    expect(twitterCard).not.toBeNull();
    const twitterCardContent = twitterCard!.getAttribute('content');
    expect(twitterCardContent).toBeTruthy();
    expect(['summary', 'summary_large_image', 'player', 'app']).toContain(twitterCardContent);

    // Check twitter:title
    const twitterTitle = document.querySelector('meta[name="twitter:title"]');
    expect(twitterTitle).not.toBeNull();
    const twitterTitleContent = twitterTitle!.getAttribute('content');
    expect(twitterTitleContent).toBeTruthy();
    expect(twitterTitleContent).toContain('MirDB');

    // Check twitter:description
    const twitterDescription = document.querySelector('meta[name="twitter:description"]');
    expect(twitterDescription).not.toBeNull();
    const twitterDescriptionContent = twitterDescription!.getAttribute('content');
    expect(twitterDescriptionContent).toBeTruthy();

    // Check twitter:image
    const twitterImage = document.querySelector('meta[name="twitter:image"]');
    expect(twitterImage).not.toBeNull();
    const twitterImageContent = twitterImage!.getAttribute('content');
    expect(twitterImageContent).toBeTruthy();

    console.log(`Twitter Card: "${twitterCardContent}"`);
    console.log(`Twitter Title: "${twitterTitleContent}"`);
    console.log(`Twitter Description: "${twitterDescriptionContent}"`);
    console.log(`Twitter Image: "${twitterImageContent}"`);
  });

  /**
   * Test Case 5: Check canonical URL
   * Expected: Canonical URL meta tag is present
   */
  it('TC5: Canonical URL is present', () => {
    // Check canonical link
    const canonical = document.querySelector('link[rel="canonical"]');
    expect(canonical).not.toBeNull();
    const canonicalUrl = canonical!.getAttribute('href');
    expect(canonicalUrl).toBeTruthy();
    expect(canonicalUrl).toMatch(/^https?:\/\//);

    console.log(`Canonical URL: "${canonicalUrl}"`);
  });

  /**
   * Test Case 6: Check for robots.txt
   * Expected: robots.txt file exists and is properly configured
   */
  it('TC6: robots.txt exists and is properly configured', () => {
    // Check robots.txt file exists in public directory
    const robotsTxtPath = path.resolve(__dirname, '../../public/robots.txt');
    const robotsTxtExists = fs.existsSync(robotsTxtPath);
    expect(robotsTxtExists).toBe(true);

    // Read and verify content
    const content = fs.readFileSync(robotsTxtPath, 'utf-8');

    // Verify required directives
    expect(content).toContain('User-agent:');
    expect(content).toMatch(/Allow:\s*\//);

    // Check for sitemap
    const hasSitemap = content.includes('Sitemap:');
    console.log(`robots.txt content:\n${content}`);
    console.log(`Has Sitemap directive: ${hasSitemap}`);
  });

  /**
   * Additional SEO checks
   */
  it('Page has proper document structure for SEO', () => {
    // Check html lang attribute
    const htmlLang = document.documentElement.getAttribute('lang');
    expect(htmlLang).toBe('en');

    // Check meta robots
    const metaRobots = document.querySelector('meta[name="robots"]');
    expect(metaRobots).not.toBeNull();
    const robotsContent = metaRobots!.getAttribute('content');
    expect(robotsContent).toContain('index');
    expect(robotsContent).toContain('follow');
  });

  it('Page has valid structured data (JSON-LD)', () => {
    // Check for JSON-LD script
    const jsonLdScript = document.querySelector('script[type="application/ld+json"]');
    expect(jsonLdScript).not.toBeNull();

    const scriptContent = jsonLdScript!.textContent;
    expect(scriptContent).toBeTruthy();

    // Parse and validate JSON-LD
    const jsonLd = JSON.parse(scriptContent!);

    // Verify basic Schema.org properties
    expect(jsonLd['@context']).toBe('https://schema.org');
    expect(jsonLd['@type']).toBeTruthy();
    expect(jsonLd.name).toBeTruthy();

    console.log('JSON-LD structured data:', JSON.stringify(jsonLd, null, 2));
  });

  it('Meta title tag matches OG title', () => {
    const titleTag = document.querySelector('meta[name="title"]');
    const ogTitle = document.querySelector('meta[property="og:title"]');

    expect(titleTag).not.toBeNull();
    expect(ogTitle).not.toBeNull();

    const titleContent = titleTag!.getAttribute('content');
    const ogTitleContent = ogTitle!.getAttribute('content');

    expect(titleContent).toBe(ogTitleContent);
  });

  it('All required Open Graph properties are present', () => {
    const requiredOgProperties = ['og:type', 'og:url', 'og:title', 'og:description', 'og:image', 'og:site_name'];

    for (const property of requiredOgProperties) {
      const element = document.querySelector(`meta[property="${property}"]`);
      expect(element, `Missing Open Graph property: ${property}`).not.toBeNull();
      expect(element!.getAttribute('content'), `Empty content for ${property}`).toBeTruthy();
    }
  });

  it('All required Twitter Card properties are present', () => {
    const requiredTwitterProperties = ['twitter:card', 'twitter:url', 'twitter:title', 'twitter:description', 'twitter:image'];

    for (const property of requiredTwitterProperties) {
      const element = document.querySelector(`meta[name="${property}"]`);
      expect(element, `Missing Twitter Card property: ${property}`).not.toBeNull();
      expect(element!.getAttribute('content'), `Empty content for ${property}`).toBeTruthy();
    }
  });
});
