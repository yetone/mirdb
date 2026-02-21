/**
 * Integration tests for Build and Deployment
 * Owner: Scenario 13
 *
 * Test cases:
 * - Build command succeeds (exit code 0, under 30s)
 * - Build output contains required files
 * - CSS minification in production
 * - JS minification in production
 * - Static serve test (via file inspection)
 * - Internal link validation (via HTML parsing)
 */

const { test, expect } = require('@playwright/test');
const { execSync, spawnSync } = require('child_process');
const fs = require('fs');
const path = require('path');

const DOCS_DIR = path.join(__dirname, '../..');
const PUBLIC_DIR = path.join(DOCS_DIR, 'public');

// Helper function to extract IDs from HTML content
function extractIds(htmlContent) {
  const idRegex = /\bid=["']([^"']+)["']/gi;
  const ids = new Set();
  let match;
  while ((match = idRegex.exec(htmlContent)) !== null) {
    ids.add(match[1]);
  }
  return ids;
}

// Helper function to extract anchor links from HTML content
function extractAnchorLinks(htmlContent) {
  const hrefRegex = /href=["']#([^"']+)["']/gi;
  const links = [];
  let match;
  while ((match = hrefRegex.exec(htmlContent)) !== null) {
    links.push(match[1]);
  }
  return links;
}

test.describe('Build and Deployment', () => {

  // Build Process tests - run first and build the site
  test.describe('Build Process', () => {

    test('Build command completes successfully with exit code 0 in under 30 seconds', async () => {
      // Clean previous build
      if (fs.existsSync(PUBLIC_DIR)) {
        fs.rmSync(PUBLIC_DIR, { recursive: true, force: true });
      }

      const startTime = Date.now();

      // Run the build command
      const result = spawnSync('npm', ['run', 'build'], {
        cwd: DOCS_DIR,
        encoding: 'utf-8',
        timeout: 30000, // 30 second timeout
        shell: true
      });

      const buildTime = Date.now() - startTime;

      // Check exit code
      expect(result.status).toBe(0);

      // Check build completed in under 30 seconds
      expect(buildTime).toBeLessThan(30000);

      // Verify public directory was created
      expect(fs.existsSync(PUBLIC_DIR)).toBe(true);
    });

  });

  test.describe('Build Output', () => {

    test.beforeAll(async () => {
      // Ensure build is complete
      if (!fs.existsSync(PUBLIC_DIR)) {
        execSync('npm run build', { cwd: DOCS_DIR, encoding: 'utf-8' });
      }
    });

    test('Output contains index.html, CSS files, JS files, and copied assets', async () => {
      // Check index.html exists
      const indexPath = path.join(PUBLIC_DIR, 'index.html');
      expect(fs.existsSync(indexPath)).toBe(true);

      // Check CSS directory and files
      const cssDir = path.join(PUBLIC_DIR, 'css');
      expect(fs.existsSync(cssDir)).toBe(true);

      const cssFiles = fs.readdirSync(cssDir).filter(f => f.endsWith('.css'));
      expect(cssFiles.length).toBeGreaterThan(0);

      // Check JS directory and files
      const jsDir = path.join(PUBLIC_DIR, 'js');
      expect(fs.existsSync(jsDir)).toBe(true);

      const jsFiles = fs.readdirSync(jsDir).filter(f => f.endsWith('.js'));
      expect(jsFiles.length).toBeGreaterThan(0);

      // Check images (assets)
      const imagesDir = path.join(PUBLIC_DIR, 'images');
      expect(fs.existsSync(imagesDir)).toBe(true);

      // Verify logo.gif and usage.gif are present
      expect(fs.existsSync(path.join(imagesDir, 'logo.gif'))).toBe(true);
      expect(fs.existsSync(path.join(imagesDir, 'usage.gif'))).toBe(true);
    });

    test('Production CSS files are minified (no unnecessary whitespace)', async () => {
      const cssDir = path.join(PUBLIC_DIR, 'css');
      const cssFiles = fs.readdirSync(cssDir).filter(f => f.endsWith('.css'));

      expect(cssFiles.length).toBeGreaterThan(0);

      // Check style.css which is the main built CSS
      const styleCss = path.join(cssDir, 'style.css');
      if (fs.existsSync(styleCss)) {
        const content = fs.readFileSync(styleCss, 'utf-8');

        // Minified CSS should have:
        // 1. No newlines between declarations (or minimal)
        // 2. No spaces after colons in properties
        // 3. Content should be relatively compact

        // Count newlines - minified CSS typically has very few
        const newlineCount = (content.match(/\n/g) || []).length;
        const contentLength = content.length;

        // Ratio of content to newlines should be high for minified CSS
        // (at least 50 characters per newline on average)
        const ratio = contentLength / (newlineCount + 1);
        expect(ratio).toBeGreaterThan(50);

        // Check for typical minification patterns - no multiple spaces after colons
        // Minified CSS uses ":" directly without multiple spaces
        const spacesAfterColons = (content.match(/:\s{2,}/g) || []).length;
        expect(spacesAfterColons).toBe(0);
      }
    });

    test('Production JS files are minified', async () => {
      const jsDir = path.join(PUBLIC_DIR, 'js');
      const jsFiles = fs.readdirSync(jsDir);

      // Look for minified JS file (with hash in name or .min)
      const minifiedFile = jsFiles.find(f => f.includes('.min.') || f.match(/\.[a-f0-9]{8,}\.js$/));

      expect(minifiedFile).toBeTruthy();

      // Read the minified file
      const minifiedPath = path.join(jsDir, minifiedFile);
      const content = fs.readFileSync(minifiedPath, 'utf-8');

      // Minified JS characteristics:
      // 1. Very few newlines relative to content length
      // 2. No multi-line comments
      // 3. Compact format

      const newlineCount = (content.match(/\n/g) || []).length;
      const contentLength = content.length;

      // Minified JS should have high character-to-newline ratio
      if (contentLength > 100) {
        const ratio = contentLength / (newlineCount + 1);
        expect(ratio).toBeGreaterThan(50);
      }

      // Should not contain multi-line comment blocks
      // Single license comment is acceptable, but not multiple
      const commentCount = (content.match(/\/\*/g) || []).length;
      expect(commentCount).toBeLessThanOrEqual(1);
    });

  });

  // File-based validation tests (no browser required)
  test.describe('Static Server Validation', () => {

    test.beforeAll(async () => {
      // Ensure build is complete
      if (!fs.existsSync(PUBLIC_DIR)) {
        execSync('npm run build', { cwd: DOCS_DIR, encoding: 'utf-8' });
      }
    });

    test('Static files can be served and homepage loads correctly', async () => {
      const indexPath = path.join(PUBLIC_DIR, 'index.html');
      expect(fs.existsSync(indexPath)).toBe(true);

      const htmlContent = fs.readFileSync(indexPath, 'utf-8');

      // Check title contains MirDB
      expect(htmlContent).toMatch(/<title[^>]*>.*MirDB.*<\/title>/is);

      // Check main content element exists (handle minified HTML which may omit quotes)
      expect(htmlContent).toMatch(/id=["']?main-content["']?/i);

      // Check h1 heading is present
      expect(htmlContent).toMatch(/<h1[^>]*>/i);

      // Verify the page has meaningful content (more than 1000 characters)
      expect(htmlContent.length).toBeGreaterThan(1000);

      // Check that the HTML is valid (has html, head, body)
      expect(htmlContent).toMatch(/<html[^>]*>/i);
      expect(htmlContent).toMatch(/<head[^>]*>/i);
      expect(htmlContent).toMatch(/<body[^>]*>/i);
    });

  });

  test.describe('Internal Links Validation', () => {

    test.beforeAll(async () => {
      // Ensure build is complete
      if (!fs.existsSync(PUBLIC_DIR)) {
        execSync('npm run build', { cwd: DOCS_DIR, encoding: 'utf-8' });
      }
    });

    test('All internal anchor links resolve to existing elements', async () => {
      const indexPath = path.join(PUBLIC_DIR, 'index.html');
      const htmlContent = fs.readFileSync(indexPath, 'utf-8');

      // Extract all IDs from the HTML
      const ids = extractIds(htmlContent);

      // Extract all anchor links
      const anchorLinks = extractAnchorLinks(htmlContent);

      // Check that all anchor links point to existing IDs
      const brokenLinks = [];
      for (const link of anchorLinks) {
        if (!ids.has(link)) {
          brokenLinks.push(`#${link}`);
        }
      }

      // Assert no broken links
      expect(brokenLinks).toEqual([]);
    });

  });

});
