/**
 * Broken Links Integration Tests
 *
 * These tests validate that external links actually respond with valid HTTP status.
 * Requires network access to run.
 *
 * @jest-environment node
 */

const fs = require('fs');
const path = require('path');
const https = require('https');

// Helper function to check if a URL returns 200
function checkUrl(url) {
  return new Promise((resolve, reject) => {
    const request = https.get(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (compatible; LinkChecker/1.0)'
      },
      timeout: 10000
    }, (response) => {
      // Follow redirects
      if (response.statusCode >= 300 && response.statusCode < 400 && response.headers.location) {
        checkUrl(response.headers.location).then(resolve).catch(reject);
        return;
      }
      resolve({
        url,
        statusCode: response.statusCode,
        success: response.statusCode >= 200 && response.statusCode < 400
      });
    });

    request.on('error', (err) => {
      resolve({
        url,
        statusCode: null,
        success: false,
        error: err.message
      });
    });

    request.on('timeout', () => {
      request.destroy();
      resolve({
        url,
        statusCode: null,
        success: false,
        error: 'Request timed out'
      });
    });
  });
}

// Extract all URLs from HTML
function extractUrls(htmlContent) {
  const urlRegex = /href=["'](https?:\/\/[^"']+)["']/gi;
  const urls = [];
  let match;

  while ((match = urlRegex.exec(htmlContent)) !== null) {
    urls.push(match[1]);
  }

  return [...new Set(urls)]; // Remove duplicates
}

describe('Integration Test: Validate GitHub link responds', () => {
  let htmlContent;
  let externalUrls;

  beforeAll(() => {
    const htmlPath = path.join(__dirname, '..', 'index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf8');
    externalUrls = extractUrls(htmlContent);
  });

  test('should extract external URLs from HTML', () => {
    expect(externalUrls.length).toBeGreaterThan(0);
  });

  test('should have GitHub repository URL in the list', () => {
    const githubRepoUrl = externalUrls.find(url => url.includes('github.com/yetone/mirdb'));
    expect(githubRepoUrl).toBeDefined();
  });

  // Test Case 3: Validate GitHub link responds - GitHub repository URL returns 200 status
  test('GitHub repository URL returns 200 status', async () => {
    const githubRepoUrl = 'https://github.com/yetone/mirdb';

    const result = await checkUrl(githubRepoUrl);

    expect(result.success).toBe(true);
    expect(result.statusCode).toBeGreaterThanOrEqual(200);
    expect(result.statusCode).toBeLessThan(400);
  }, 15000); // 15 second timeout

  test('GitHub issues URL returns 200 status', async () => {
    const githubIssuesUrl = 'https://github.com/yetone/mirdb/issues';

    const result = await checkUrl(githubIssuesUrl);

    expect(result.success).toBe(true);
    expect(result.statusCode).toBeGreaterThanOrEqual(200);
    expect(result.statusCode).toBeLessThan(400);
  }, 15000); // 15 second timeout

  test('all external links should be reachable', async () => {
    const results = await Promise.all(
      externalUrls.map(url => checkUrl(url))
    );

    const failedLinks = results.filter(r => !r.success);

    // Log failed links for debugging
    if (failedLinks.length > 0) {
      console.log('Failed links:', failedLinks);
    }

    expect(failedLinks).toEqual([]);
  }, 30000); // 30 second timeout for all links
});
