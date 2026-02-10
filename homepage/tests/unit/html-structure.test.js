/**
 * HTML Structure Unit Tests
 * Owner: Scenario 1 - Homepage Core Structure
 *
 * Unit tests for validating the HTML document structure of the MirDB homepage.
 *
 * Requirements traced:
 * - REQ-1: Homepage displays project introduction
 * - REQ-5: Semantic HTML structure
 */

const fs = require('fs');
const path = require('path');

describe('Homepage HTML Document Structure', () => {
  let htmlContent;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '../../index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf8');
  });

  // Test Case 4: Check HTML document structure
  test('should have proper DOCTYPE declaration', () => {
    expect(htmlContent.trim().toLowerCase()).toMatch(/^<!doctype html>/);
  });

  test('should have html element with lang attribute', () => {
    expect(htmlContent).toMatch(/<html[^>]*lang\s*=\s*["']en["'][^>]*>/i);
  });

  test('should have head element', () => {
    expect(htmlContent).toMatch(/<head[^>]*>[\s\S]*<\/head>/i);
  });

  test('should have body element', () => {
    expect(htmlContent).toMatch(/<body[^>]*>[\s\S]*<\/body>/i);
  });

  test('should have charset meta tag with UTF-8', () => {
    expect(htmlContent).toMatch(/<meta[^>]*charset\s*=\s*["']UTF-8["'][^>]*>/i);
  });

  test('should have viewport meta tag', () => {
    expect(htmlContent).toMatch(/<meta[^>]*name\s*=\s*["']viewport["'][^>]*content\s*=\s*["'][^"']*width=device-width[^"']*["'][^>]*>/i);
  });

  test('should have title element', () => {
    expect(htmlContent).toMatch(/<title[^>]*>[^<]+<\/title>/i);
  });

  test('should have title containing MirDB', () => {
    const titleMatch = htmlContent.match(/<title[^>]*>([^<]+)<\/title>/i);
    expect(titleMatch).toBeTruthy();
    expect(titleMatch[1]).toContain('MirDB');
  });

  test('should link to styles.css', () => {
    expect(htmlContent).toMatch(/<link[^>]*rel\s*=\s*["']stylesheet["'][^>]*href\s*=\s*["'][^"']*styles\.css["'][^>]*>/i);
  });

  test('should have header element', () => {
    expect(htmlContent).toMatch(/<header[^>]*>[\s\S]*<\/header>/i);
  });

  test('should have hero section', () => {
    expect(htmlContent).toMatch(/id\s*=\s*["']hero["']|class\s*=\s*["'][^"']*hero[^"']*["']/i);
  });

  test('should have features section', () => {
    expect(htmlContent).toMatch(/id\s*=\s*["']features["']|class\s*=\s*["'][^"']*features[^"']*["']/i);
  });

  test('should have quickstart section', () => {
    expect(htmlContent).toMatch(/id\s*=\s*["']quickstart["']|class\s*=\s*["'][^"']*quickstart[^"']*["']/i);
  });

  test('should have resources section', () => {
    expect(htmlContent).toMatch(/id\s*=\s*["']resources["']|class\s*=\s*["'][^"']*resources[^"']*["']/i);
  });

  test('should have footer element', () => {
    expect(htmlContent).toMatch(/<footer[^>]*>[\s\S]*<\/footer>/i);
  });

  test('should have logo image referencing logo.gif', () => {
    expect(htmlContent).toMatch(/<img[^>]*src\s*=\s*["'][^"']*logo\.gif["'][^>]*>/i);
  });

  test('should have MirDB product name in hero section', () => {
    // Extract hero section content
    const heroMatch = htmlContent.match(/<section[^>]*(?:id\s*=\s*["']hero["']|class\s*=\s*["'][^"']*hero[^"']*["'])[^>]*>[\s\S]*?<\/section>/i);
    expect(heroMatch).toBeTruthy();
    expect(heroMatch[0]).toContain('MirDB');
  });

  test('should have value proposition keywords in hero section', () => {
    const heroMatch = htmlContent.match(/<section[^>]*(?:id\s*=\s*["']hero["']|class\s*=\s*["'][^"']*hero[^"']*["'])[^>]*>[\s\S]*?<\/section>/i);
    expect(heroMatch).toBeTruthy();
    const heroContent = heroMatch[0].toLowerCase();
    const hasKeyValueStore = heroContent.includes('key-value store') || heroContent.includes('key value store');
    const hasMemcached = heroContent.includes('memcached');
    expect(hasKeyValueStore || hasMemcached).toBe(true);
  });

  test('should have Get Started CTA button', () => {
    expect(htmlContent).toMatch(/Get Started/i);
  });

  test('should have GitHub CTA button', () => {
    expect(htmlContent).toMatch(/GitHub/i);
  });
});
