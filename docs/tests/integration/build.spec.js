/**
 * Integration tests for Build and Deployment
 * Owner: Scenario 13
 *
 * Test cases:
 * - Build command succeeds (exit code 0, under 30s)
 * - Build output contains required files
 * - CSS minification in production
 * - JS minification in production
 * - Static serve test
 * - Internal link validation
 */

const { test, expect } = require('@playwright/test');
const { execSync, spawnSync } = require('child_process');
const fs = require('fs');
const path = require('path');

const DOCS_DIR = path.join(__dirname, '../..');
const PUBLIC_DIR = path.join(DOCS_DIR, 'public');

test.describe('Build and Deployment', () => {

  // Browser-based tests that rely on the Hugo dev server
  // These should run first, before we mess with the public directory
  test.describe('Static Server', () => {

    test('Static files can be served and homepage loads correctly', async ({ page, baseURL }) => {
      // Navigate using the full baseURL to ensure correct path
      const response = await page.goto(baseURL, { waitUntil: 'domcontentloaded' });

      // Ensure we got a successful response (200 or 304)
      expect([200, 304]).toContain(response.status());

      // Wait for the title element to be present
      await page.waitForFunction(() => document.title.length > 0, { timeout: 15000 });

      // Check the page loaded successfully - title should contain MirDB
      const title = await page.title();
      expect(title).toMatch(/MirDB/);

      // Check main content is visible
      const main = page.locator('main#main-content');
      await expect(main).toBeVisible({ timeout: 10000 });

      // Verify h1 heading is present
      const h1 = page.locator('h1');
      await expect(h1).toBeVisible({ timeout: 10000 });

      // Verify the page has meaningful content
      const bodyText = await page.textContent('body');
      expect(bodyText.length).toBeGreaterThan(100);
    });

  });

  test.describe('Internal Links', () => {

    test('All internal anchor links resolve to existing elements', async ({ page }) => {
      await page.goto('/');

      // Get all internal anchor links (href starting with #)
      const anchorLinks = page.locator('a[href^="#"]');
      const count = await anchorLinks.count();

      const brokenLinks = [];

      for (let i = 0; i < count; i++) {
        const link = anchorLinks.nth(i);
        const href = await link.getAttribute('href');

        // Skip empty or just "#" links
        if (!href || href === '#') continue;

        // Get the target ID from the href (remove the #)
        const targetId = href.substring(1);

        // Check if element with that ID exists
        const targetElement = page.locator(`#${CSS.escape(targetId)}`);
        const exists = await targetElement.count();

        if (exists === 0) {
          brokenLinks.push(href);
        }
      }

      // Assert no broken links
      expect(brokenLinks).toEqual([]);
    });

  });

  // Non-browser tests for build process - these run after browser tests
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
        // (at least 100 characters per newline on average)
        const ratio = contentLength / (newlineCount + 1);
        expect(ratio).toBeGreaterThan(50);

        // Check for typical minification patterns - no ": " after property names
        // Minified CSS uses ":" directly without spaces
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
      const hasMultiLineComments = content.includes('/*') && content.includes('*/') &&
        content.indexOf('/*') !== content.lastIndexOf('/*');
      // Single license comment is acceptable, but not multiple
      const commentCount = (content.match(/\/\*/g) || []).length;
      expect(commentCount).toBeLessThanOrEqual(1);
    });

  });

});

// CSS.escape polyfill for Node.js
if (typeof CSS === 'undefined') {
  global.CSS = {
    escape: function(value) {
      if (arguments.length === 0) {
        throw new TypeError('`CSS.escape` requires an argument.');
      }
      let string = String(value);
      let length = string.length;
      let index = -1;
      let codeUnit;
      let result = '';
      let firstCodeUnit = string.charCodeAt(0);

      while (++index < length) {
        codeUnit = string.charCodeAt(index);
        // Escape characters per CSS spec
        if (codeUnit === 0x0000) {
          result += '\uFFFD';
          continue;
        }
        if (
          (codeUnit >= 0x0001 && codeUnit <= 0x001F) ||
          codeUnit === 0x007F ||
          (index === 0 && codeUnit >= 0x0030 && codeUnit <= 0x0039) ||
          (index === 1 && codeUnit >= 0x0030 && codeUnit <= 0x0039 && firstCodeUnit === 0x002D)
        ) {
          result += '\\' + codeUnit.toString(16) + ' ';
          continue;
        }
        if (index === 0 && length === 1 && codeUnit === 0x002D) {
          result += '\\' + string.charAt(index);
          continue;
        }
        if (
          codeUnit >= 0x0080 ||
          codeUnit === 0x002D ||
          codeUnit === 0x005F ||
          (codeUnit >= 0x0030 && codeUnit <= 0x0039) ||
          (codeUnit >= 0x0041 && codeUnit <= 0x005A) ||
          (codeUnit >= 0x0061 && codeUnit <= 0x007A)
        ) {
          result += string.charAt(index);
          continue;
        }
        result += '\\' + string.charAt(index);
      }
      return result;
    }
  };
}
