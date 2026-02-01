/**
 * Static Site Structure Tests
 * Owner: Scenario 16 - Static Site Requirements
 *
 * Tests:
 * - index.html exists
 * - Works with static file server
 * - No server-side dependencies
 */
const fs = require('fs');
const path = require('path');
const { loadHTML } = require('../helpers/dom-utils');

describe('Static Site Requirements', () => {
  const projectRoot = path.resolve(__dirname, '../../');

  describe('Test Case 1: index.html exists as entry point', () => {
    test('index.html file exists in project root', () => {
      const indexPath = path.join(projectRoot, 'index.html');
      expect(fs.existsSync(indexPath)).toBe(true);
    });

    test('index.html is a valid HTML file', () => {
      const indexPath = path.join(projectRoot, 'index.html');
      const content = fs.readFileSync(indexPath, 'utf8');

      // Check for DOCTYPE declaration
      expect(content).toMatch(/<!DOCTYPE html>/i);

      // Check for html tag with lang attribute
      expect(content).toMatch(/<html[^>]+lang=/i);
    });

    test('index.html contains required head elements', () => {
      loadHTML('index.html');

      // Check for charset meta tag
      const charsetMeta = document.querySelector('meta[charset]');
      expect(charsetMeta).not.toBeNull();

      // Check for viewport meta tag
      const viewportMeta = document.querySelector('meta[name="viewport"]');
      expect(viewportMeta).not.toBeNull();

      // Check for title element
      const title = document.querySelector('title');
      expect(title).not.toBeNull();
      expect(title.textContent.length).toBeGreaterThan(0);
    });

    test('index.html contains main content sections', () => {
      loadHTML('index.html');

      // Check for body element
      const body = document.querySelector('body');
      expect(body).not.toBeNull();

      // Check for main content area
      const mainContent = document.querySelector('main') || document.querySelector('#main-content');
      expect(mainContent).not.toBeNull();
    });
  });

  describe('Test Case 2: Static file server compatibility', () => {
    test('all asset paths are relative (no absolute server paths)', () => {
      const indexPath = path.join(projectRoot, 'index.html');
      const content = fs.readFileSync(indexPath, 'utf8');

      // Check that local asset references are relative, not absolute
      // Should not have paths like /assets/ or http://localhost
      const localAssetPattern = /(src|href)=["'](?!https?:\/\/|\/\/|#)/g;
      const assetMatches = content.match(localAssetPattern) || [];

      // Verify no absolute paths to local resources
      expect(content).not.toMatch(/(?:src|href)=["']\/assets\//);
      expect(content).not.toMatch(/(?:src|href)=["']http:\/\/localhost/);
    });

    test('CSS files are referenced correctly', () => {
      loadHTML('index.html');

      const styleLinks = document.querySelectorAll('link[rel="stylesheet"]');
      expect(styleLinks.length).toBeGreaterThan(0);

      styleLinks.forEach(link => {
        const href = link.getAttribute('href');
        // Either CDN (external) or relative path
        expect(
          href.startsWith('http') ||
          href.startsWith('//') ||
          href.startsWith('assets/')
        ).toBe(true);
      });
    });

    test('JavaScript files are referenced correctly', () => {
      loadHTML('index.html');

      const scripts = document.querySelectorAll('script[src]');
      expect(scripts.length).toBeGreaterThan(0);

      scripts.forEach(script => {
        const src = script.getAttribute('src');
        // Either CDN (external) or relative path
        expect(
          src.startsWith('http') ||
          src.startsWith('//') ||
          src.startsWith('assets/')
        ).toBe(true);
      });
    });

    test('image assets exist and are referenced relatively', () => {
      loadHTML('index.html');

      const images = document.querySelectorAll('img[src]');

      images.forEach(img => {
        const src = img.getAttribute('src');
        // Images should be either CDN/external or relative paths
        const isExternal = src.startsWith('http') || src.startsWith('//');
        const isRelative = src.startsWith('assets/');

        expect(isExternal || isRelative).toBe(true);

        // If relative, verify the file exists
        if (isRelative) {
          const imagePath = path.join(projectRoot, src);
          expect(fs.existsSync(imagePath)).toBe(true);
        }
      });
    });

    test('page can be parsed as valid HTML', () => {
      const indexPath = path.join(projectRoot, 'index.html');
      const content = fs.readFileSync(indexPath, 'utf8');

      // Load HTML and verify document structure
      loadHTML('index.html');

      // Should have html, head, and body
      expect(document.documentElement).not.toBeNull();
      expect(document.head).not.toBeNull();
      expect(document.body).not.toBeNull();
    });
  });

  describe('Test Case 3: No server-side dependencies', () => {
    test('no PHP files in the project structure', () => {
      const phpFiles = findFiles(projectRoot, '.php');
      expect(phpFiles).toHaveLength(0);
    });

    test('no server-side JavaScript markers in index.html', () => {
      const indexPath = path.join(projectRoot, 'index.html');
      const content = fs.readFileSync(indexPath, 'utf8');

      // No server-side templating syntax
      expect(content).not.toMatch(/<%.*%>/); // EJS, ASP
      expect(content).not.toMatch(/<\?php/i); // PHP
      expect(content).not.toMatch(/{{.*}}/); // Handlebars, Mustache (unless it's for display only)
      expect(content).not.toMatch(/{%.*%}/); // Jinja, Liquid
    });

    test('no server.js or app.js in project root', () => {
      // These typically indicate Node.js server requirements
      const serverJs = path.join(projectRoot, 'server.js');
      const appJs = path.join(projectRoot, 'app.js');

      expect(fs.existsSync(serverJs)).toBe(false);
      expect(fs.existsSync(appJs)).toBe(false);
    });

    test('package.json does not have server dependencies', () => {
      const packagePath = path.join(projectRoot, 'package.json');

      if (fs.existsSync(packagePath)) {
        const packageJson = JSON.parse(fs.readFileSync(packagePath, 'utf8'));
        const allDeps = {
          ...(packageJson.dependencies || {}),
          ...(packageJson.devDependencies || {})
        };

        // Check for common server frameworks
        const serverDeps = ['express', 'koa', 'fastify', 'hapi', 'next', 'nuxt', 'gatsby'];
        serverDeps.forEach(dep => {
          expect(allDeps).not.toHaveProperty(dep);
        });
      }
    });

    test('no build output required for serving', () => {
      // Check that index.html can be served directly (no dist/ or build/ folder requirement)
      const indexPath = path.join(projectRoot, 'index.html');
      expect(fs.existsSync(indexPath)).toBe(true);

      // index.html should be in root, not requiring a build step
      const content = fs.readFileSync(indexPath, 'utf8');
      expect(content.length).toBeGreaterThan(100);
    });

    test('CSS files are either CDN-hosted or static files', () => {
      loadHTML('index.html');

      const styleLinks = document.querySelectorAll('link[rel="stylesheet"]');

      styleLinks.forEach(link => {
        const href = link.getAttribute('href');
        // Should be either CDN URL or static file (not compiled/processed paths)
        const isCDN = href.startsWith('http') || href.startsWith('//');
        const isStaticCSS = href.endsWith('.css');

        expect(isCDN || isStaticCSS).toBe(true);
      });
    });

    test('all internal links are to static files or anchors', () => {
      loadHTML('index.html');

      const links = document.querySelectorAll('a[href]');

      links.forEach(link => {
        const href = link.getAttribute('href');

        // Skip external links
        if (href.startsWith('http') || href.startsWith('//')) {
          return;
        }

        // Internal links should be anchors or static files
        const isAnchor = href.startsWith('#');
        const isStaticFile = href.endsWith('.html') || href.endsWith('.htm');
        const isEmpty = href === '';

        expect(isAnchor || isStaticFile || isEmpty).toBe(true);
      });
    });
  });
});

/**
 * Helper function to find files with a specific extension
 * @param {string} dir - Directory to search
 * @param {string} extension - File extension to search for
 * @returns {string[]} - Array of file paths
 */
function findFiles(dir, extension) {
  const results = [];

  // Skip node_modules, .git, and hidden directories
  const skipDirs = ['node_modules', '.git', '.something', 'target'];

  try {
    const items = fs.readdirSync(dir);

    for (const item of items) {
      if (skipDirs.includes(item) || item.startsWith('.')) {
        continue;
      }

      const fullPath = path.join(dir, item);
      const stat = fs.statSync(fullPath);

      if (stat.isDirectory()) {
        results.push(...findFiles(fullPath, extension));
      } else if (item.endsWith(extension)) {
        results.push(fullPath);
      }
    }
  } catch (err) {
    // Ignore permission errors
  }

  return results;
}
