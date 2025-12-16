// @ts-check
const { test, expect } = require('@playwright/test');
const fs = require('fs');
const path = require('path');

/**
 * Test Scenario: SEO and Meta Tags (NFR-4)
 *
 * Verify that appropriate meta tags and semantic HTML are implemented for SEO.
 * This ensures the site is optimized for search engines and social sharing.
 */

const HOMEPAGE_DIR = path.resolve(__dirname, '..');

// Test Case 1: Verify meta title
test.describe('SEO - Meta Title', () => {
  test('should have title tag containing MirDB and descriptive text', () => {
    const htmlPath = path.join(HOMEPAGE_DIR, 'index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf-8');

    const titleMatch = htmlContent.match(/<title>([^<]+)<\/title>/i);
    expect(titleMatch).toBeTruthy();
    expect(titleMatch[1].toLowerCase()).toContain('mirdb');
    // Title should have descriptive text, not just "MirDB"
    expect(titleMatch[1].length).toBeGreaterThan(6);
  });

  test('should have proper title tag in HTML head', () => {
    const htmlPath = path.join(HOMEPAGE_DIR, 'index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf-8');

    // Check title tag exists and is in head section
    expect(htmlContent).toMatch(/<head[\s\S]*<title>[\s\S]*MirDB[\s\S]*<\/title>[\s\S]*<\/head>/i);
  });

  test('title should have appropriate length for SEO (30-60 characters)', () => {
    const htmlPath = path.join(HOMEPAGE_DIR, 'index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf-8');

    const titleMatch = htmlContent.match(/<title>([^<]+)<\/title>/i);
    expect(titleMatch).toBeTruthy();

    const titleText = titleMatch[1].trim();
    expect(titleText.length).toBeGreaterThanOrEqual(20);
    expect(titleText.length).toBeLessThanOrEqual(70);
  });
});

// Test Case 2: Verify meta description
test.describe('SEO - Meta Description', () => {
  test('should have meta description tag with relevant content about MirDB', () => {
    const htmlPath = path.join(HOMEPAGE_DIR, 'index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf-8');

    // Check meta description exists
    const descMatch = htmlContent.match(/<meta\s+name=["']description["']\s+content=["']([^"']+)["']/i);
    expect(descMatch).toBeTruthy();

    const description = descMatch[1].toLowerCase();
    expect(description).toContain('mirdb');
    // Should mention key features
    expect(description).toMatch(/key-value|memcached|rust|persistent/i);
  });

  test('should have meta description in head section', () => {
    const htmlPath = path.join(HOMEPAGE_DIR, 'index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf-8');

    expect(htmlContent).toMatch(/<head[\s\S]*<meta\s+name=["']description["'][\s\S]*<\/head>/i);
  });

  test('meta description should have appropriate length for SEO (120-160 characters)', () => {
    const htmlPath = path.join(HOMEPAGE_DIR, 'index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf-8');

    const descMatch = htmlContent.match(/<meta\s+name=["']description["']\s+content=["']([^"']+)["']/i);
    expect(descMatch).toBeTruthy();

    const description = descMatch[1];
    expect(description.length).toBeGreaterThanOrEqual(50);
    expect(description.length).toBeLessThanOrEqual(200);
  });
});

// Test Case 3: Verify semantic HTML elements
test.describe('SEO - Semantic HTML', () => {
  test('should use header element appropriately', () => {
    const htmlPath = path.join(HOMEPAGE_DIR, 'index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf-8');

    expect(htmlContent).toMatch(/<header[\s\S]*<\/header>/i);
  });

  test('should use main element for primary content', () => {
    const htmlPath = path.join(HOMEPAGE_DIR, 'index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf-8');

    expect(htmlContent).toMatch(/<main[\s\S]*<\/main>/i);
  });

  test('should use section elements for content sections', () => {
    const htmlPath = path.join(HOMEPAGE_DIR, 'index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf-8');

    // Should have multiple section elements
    const sectionMatches = htmlContent.match(/<section/gi);
    expect(sectionMatches).toBeTruthy();
    expect(sectionMatches.length).toBeGreaterThanOrEqual(4);
  });

  test('should use footer element', () => {
    const htmlPath = path.join(HOMEPAGE_DIR, 'index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf-8');

    expect(htmlContent).toMatch(/<footer[\s\S]*<\/footer>/i);
  });

  test('should use nav element for navigation', () => {
    const htmlPath = path.join(HOMEPAGE_DIR, 'index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf-8');

    expect(htmlContent).toMatch(/<nav[\s\S]*<\/nav>/i);
  });

  test('should have proper document structure with semantic elements', () => {
    const htmlPath = path.join(HOMEPAGE_DIR, 'index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf-8');

    // Check semantic elements are present
    expect(htmlContent).toMatch(/<header[\s\S]*<\/header>/i);
    expect(htmlContent).toMatch(/<main[\s\S]*<\/main>/i);
    expect(htmlContent).toMatch(/<footer[\s\S]*<\/footer>/i);
    expect(htmlContent).toMatch(/<nav[\s\S]*<\/nav>/i);

    // Section elements should be present (at least 4)
    const sectionMatches = htmlContent.match(/<section/gi);
    expect(sectionMatches).toBeTruthy();
    expect(sectionMatches.length).toBeGreaterThanOrEqual(4);
  });
});

// Test Case 4: Verify Open Graph tags
test.describe('SEO - Open Graph Tags', () => {
  test('should have og:title meta tag', () => {
    const htmlPath = path.join(HOMEPAGE_DIR, 'index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf-8');

    expect(htmlContent).toMatch(/<meta\s+property=["']og:title["']\s+content=["'][^"']+["']/i);
  });

  test('should have og:description meta tag', () => {
    const htmlPath = path.join(HOMEPAGE_DIR, 'index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf-8');

    expect(htmlContent).toMatch(/<meta\s+property=["']og:description["']\s+content=["'][^"']+["']/i);
  });

  test('should have og:type meta tag', () => {
    const htmlPath = path.join(HOMEPAGE_DIR, 'index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf-8');

    expect(htmlContent).toMatch(/<meta\s+property=["']og:type["']\s+content=["'][^"']+["']/i);
  });

  test('og:title should contain MirDB', () => {
    const htmlPath = path.join(HOMEPAGE_DIR, 'index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf-8');

    const ogTitleMatch = htmlContent.match(/<meta\s+property=["']og:title["']\s+content=["']([^"']+)["']/i);
    expect(ogTitleMatch).toBeTruthy();
    expect(ogTitleMatch[1].toLowerCase()).toContain('mirdb');
  });

  test('og:description should describe MirDB', () => {
    const htmlPath = path.join(HOMEPAGE_DIR, 'index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf-8');

    const ogDescMatch = htmlContent.match(/<meta\s+property=["']og:description["']\s+content=["']([^"']+)["']/i);
    expect(ogDescMatch).toBeTruthy();
    expect(ogDescMatch[1].toLowerCase()).toMatch(/key-value|memcached|rust|database|persistent/i);
  });

  test('og:type should be website', () => {
    const htmlPath = path.join(HOMEPAGE_DIR, 'index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf-8');

    const ogTypeMatch = htmlContent.match(/<meta\s+property=["']og:type["']\s+content=["']([^"']+)["']/i);
    expect(ogTypeMatch).toBeTruthy();
    expect(ogTypeMatch[1].toLowerCase()).toBe('website');
  });

  test('Open Graph tags should be in head section', () => {
    const htmlPath = path.join(HOMEPAGE_DIR, 'index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf-8');

    // All OG tags should be within head
    expect(htmlContent).toMatch(/<head[\s\S]*property=["']og:title["'][\s\S]*<\/head>/i);
    expect(htmlContent).toMatch(/<head[\s\S]*property=["']og:description["'][\s\S]*<\/head>/i);
    expect(htmlContent).toMatch(/<head[\s\S]*property=["']og:type["'][\s\S]*<\/head>/i);
  });
});

// Test Case 5: Verify heading hierarchy
test.describe('SEO - Heading Hierarchy', () => {
  test('should have exactly one h1 element', () => {
    const htmlPath = path.join(HOMEPAGE_DIR, 'index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf-8');

    const h1Matches = htmlContent.match(/<h1[\s>]/gi);
    expect(h1Matches).toBeTruthy();
    expect(h1Matches.length).toBe(1);
  });

  test('h1 should contain the product name MirDB', () => {
    const htmlPath = path.join(HOMEPAGE_DIR, 'index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf-8');

    expect(htmlContent).toMatch(/<h1[^>]*>[\s\S]*MirDB[\s\S]*<\/h1>/i);
  });

  test('should have logical heading hierarchy (h1 > h2 > h3)', () => {
    const htmlPath = path.join(HOMEPAGE_DIR, 'index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf-8');

    // Extract all headings in order
    const headingPattern = /<h([1-6])[^>]*>/gi;
    const headings = [];
    let match;
    while ((match = headingPattern.exec(htmlContent)) !== null) {
      headings.push(parseInt(match[1]));
    }

    // First heading should be h1
    expect(headings[0]).toBe(1);

    // No heading should skip more than one level (e.g., h1 to h3 without h2)
    for (let i = 1; i < headings.length; i++) {
      const diff = headings[i] - headings[i-1];
      // Can go down any number of levels (h2 to h1) or up by at most 1 (h1 to h2)
      expect(diff).toBeLessThanOrEqual(1);
    }
  });

  test('should have h2 elements for main sections', () => {
    const htmlPath = path.join(HOMEPAGE_DIR, 'index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf-8');

    const h2Matches = htmlContent.match(/<h2[\s>]/gi);
    expect(h2Matches).toBeTruthy();
    expect(h2Matches.length).toBeGreaterThanOrEqual(3);
  });

  test('h2 elements should have meaningful section titles', () => {
    const htmlPath = path.join(HOMEPAGE_DIR, 'index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf-8');

    // Check for expected section headings
    expect(htmlContent).toMatch(/<h2[^>]*>[\s\S]*Features[\s\S]*<\/h2>/i);
    expect(htmlContent).toMatch(/<h2[^>]*>[\s\S]*Architecture[\s\S]*<\/h2>/i);
    expect(htmlContent).toMatch(/<h2[^>]*>[\s\S]*Getting Started[\s\S]*<\/h2>/i);
  });

  test('page should have proper heading structure', () => {
    const htmlPath = path.join(HOMEPAGE_DIR, 'index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf-8');

    // Verify single h1
    const h1Matches = htmlContent.match(/<h1[\s>]/gi);
    expect(h1Matches).toBeTruthy();
    expect(h1Matches.length).toBe(1);

    // Verify h1 contains MirDB
    expect(htmlContent).toMatch(/<h1[^>]*>[\s\S]*MirDB[\s\S]*<\/h1>/i);

    // Verify multiple h2 for sections
    const h2Matches = htmlContent.match(/<h2[\s>]/gi);
    expect(h2Matches).toBeTruthy();
    expect(h2Matches.length).toBeGreaterThanOrEqual(3);
  });
});
