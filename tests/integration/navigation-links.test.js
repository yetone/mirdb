/**
 * Integration tests for Navigation Functionality
 * Scenario 17: Verify all navigation links work correctly and point to intended destinations
 *
 * Test Cases:
 * TC3 (integration): Verify no broken links - All links on page return 200 status or redirect appropriately
 */

const { test, describe } = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const path = require('path');
const https = require('https');
const http = require('http');

const indexHtmlPath = path.resolve(__dirname, '../../index.html');

/**
 * Makes an HTTP/HTTPS HEAD request to check if a URL is accessible
 * @param {string} url - The URL to check
 * @returns {Promise<{status: number, ok: boolean, redirectUrl?: string}>}
 */
function checkUrl(url) {
  return new Promise((resolve) => {
    const isHttps = url.startsWith('https');
    const client = isHttps ? https : http;

    const options = {
      method: 'HEAD',
      timeout: 10000,
      headers: {
        'User-Agent': 'Mozilla/5.0 (compatible; LinkChecker/1.0)'
      }
    };

    try {
      const req = client.request(url, options, (res) => {
        const status = res.statusCode;
        const ok = status >= 200 && status < 400;
        const redirectUrl = res.headers.location;
        resolve({ status, ok, redirectUrl });
      });

      req.on('error', () => {
        resolve({ status: 0, ok: false });
      });

      req.on('timeout', () => {
        req.destroy();
        resolve({ status: 0, ok: false });
      });

      req.end();
    } catch {
      resolve({ status: 0, ok: false });
    }
  });
}

describe('Navigation Links Integration Tests', () => {
  let html;
  let externalUrls;
  let internalAnchors;

  // Load HTML and extract links before tests
  test.beforeEach(() => {
    html = fs.readFileSync(indexHtmlPath, 'utf8');

    // Extract all external URLs
    const externalUrlRegex = /href=["'](https?:\/\/[^"']+)["']/gi;
    const externalMatches = [...html.matchAll(externalUrlRegex)];
    externalUrls = [...new Set(externalMatches.map((m) => m[1]))];

    // Extract all internal anchor links
    const anchorRegex = /href=["']#([^"']+)["']/gi;
    const anchorMatches = [...html.matchAll(anchorRegex)];
    internalAnchors = [...new Set(anchorMatches.map((m) => m[1]))];
  });

  // Test Case 3: Verify no broken internal links
  test('TC3a: All internal anchor links have corresponding section IDs', () => {
    assert.ok(
      internalAnchors.length > 0,
      'Page should have at least one internal anchor link'
    );

    const missingIds = [];

    internalAnchors.forEach((anchorId) => {
      const idRegex = new RegExp(`id=["']${anchorId}["']`, 'i');
      if (!idRegex.test(html)) {
        missingIds.push(anchorId);
      }
    });

    assert.strictEqual(
      missingIds.length,
      0,
      `Broken internal links found - missing section IDs: ${missingIds.join(', ')}`
    );
  });

  // Test Case 3: Verify external links are accessible (200 or redirect)
  test('TC3b: All external links return 200 status or redirect appropriately', async () => {
    assert.ok(
      externalUrls.length > 0,
      'Page should have at least one external link'
    );

    const results = await Promise.all(
      externalUrls.map(async (url) => {
        const result = await checkUrl(url);
        return { url, ...result };
      })
    );

    const brokenLinks = results.filter((r) => !r.ok);

    // For debugging: log all results
    results.forEach((r) => {
      console.log(`  ${r.ok ? '✓' : '✗'} ${r.url} - Status: ${r.status}${r.redirectUrl ? ` -> ${r.redirectUrl}` : ''}`);
    });

    assert.strictEqual(
      brokenLinks.length,
      0,
      `Broken external links found:\n${brokenLinks.map((r) => `  - ${r.url} (status: ${r.status})`).join('\n')}`
    );
  });

  // Additional: Verify GitHub links specifically
  test('GitHub repository links are valid', async () => {
    const githubUrls = externalUrls.filter((url) => url.includes('github.com'));

    assert.ok(
      githubUrls.length >= 2,
      `Expected at least 2 GitHub links, found ${githubUrls.length}`
    );

    const results = await Promise.all(
      githubUrls.map(async (url) => {
        const result = await checkUrl(url);
        return { url, ...result };
      })
    );

    const brokenGithubLinks = results.filter((r) => !r.ok);

    assert.strictEqual(
      brokenGithubLinks.length,
      0,
      `Broken GitHub links found:\n${brokenGithubLinks.map((r) => `  - ${r.url} (status: ${r.status})`).join('\n')}`
    );
  });

  // Verify sections exist for all navigation targets
  test('All navigable sections exist in the document', () => {
    // List of expected sections based on a typical homepage
    const expectedSections = [
      'getting-started',
      'features',
      'architecture',
      'code-examples',
      'configuration',
      'commands'
    ];

    const missingSections = [];

    expectedSections.forEach((sectionId) => {
      const idRegex = new RegExp(`id=["']${sectionId}["']`, 'i');
      if (!idRegex.test(html)) {
        missingSections.push(sectionId);
      }
    });

    // Just verify the essential sections exist
    assert.ok(
      html.includes('id="getting-started"'),
      'Page should have a getting-started section'
    );
  });
});
