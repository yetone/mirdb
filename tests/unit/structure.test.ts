/**
 * File Structure Unit Tests
 * Owner: Scenario 14 - Static Site Deployment Compatibility
 *
 * Test coverage:
 * - Required files exist (index.html, 404.html)
 * - Correct file locations
 * - Valid file types (no server-side dependencies)
 * - Relative asset paths for deployment flexibility
 */

import { describe, it, expect } from 'vitest';
import * as fs from 'fs';
import * as path from 'path';

const HOMEPAGE_DIR = path.resolve(__dirname, '../../homepage');

describe('Static Site Deployment Compatibility', () => {
  describe('Test Case 1: index.html exists at root', () => {
    it('index.html file exists as entry point', () => {
      const indexPath = path.join(HOMEPAGE_DIR, 'index.html');
      const indexExists = fs.existsSync(indexPath);
      expect(indexExists).toBe(true);
    });

    it('index.html is at the root of homepage directory', () => {
      const indexPath = path.join(HOMEPAGE_DIR, 'index.html');
      const stats = fs.statSync(indexPath);
      expect(stats.isFile()).toBe(true);
    });

    it('index.html contains valid HTML5 doctype', () => {
      const indexPath = path.join(HOMEPAGE_DIR, 'index.html');
      const content = fs.readFileSync(indexPath, 'utf-8');
      expect(content.trim().toLowerCase().startsWith('<!doctype html>')).toBe(true);
    });
  });

  describe('Test Case 2: No server-side dependencies', () => {
    it('no PHP files exist', () => {
      const phpFiles = findFiles(HOMEPAGE_DIR, '.php');
      expect(phpFiles).toHaveLength(0);
    });

    it('no server-side template files exist (.erb, .ejs, .pug, .hbs)', () => {
      const serverTemplates = [
        ...findFiles(HOMEPAGE_DIR, '.erb'),
        ...findFiles(HOMEPAGE_DIR, '.ejs'),
        ...findFiles(HOMEPAGE_DIR, '.pug'),
        ...findFiles(HOMEPAGE_DIR, '.hbs'),
        ...findFiles(HOMEPAGE_DIR, '.jade'),
      ];
      expect(serverTemplates).toHaveLength(0);
    });

    it('no Python server files exist (.py)', () => {
      const pyFiles = findFiles(HOMEPAGE_DIR, '.py');
      expect(pyFiles).toHaveLength(0);
    });

    it('no server configuration files that require processing', () => {
      const serverConfigs = [
        path.join(HOMEPAGE_DIR, '.htaccess'),
        path.join(HOMEPAGE_DIR, 'web.config'),
        path.join(HOMEPAGE_DIR, 'nginx.conf'),
      ];
      // These files are optional for redirects but should not be required
      // We just check they don't contain dynamic processing directives if they exist
      for (const configPath of serverConfigs) {
        if (fs.existsSync(configPath)) {
          const content = fs.readFileSync(configPath, 'utf-8');
          // Should not contain PHP handler or other dynamic processing
          expect(content).not.toContain('AddHandler');
          expect(content).not.toContain('mod_php');
        }
      }
    });

    it('only static file types exist (HTML, CSS, JS, images)', () => {
      const allFiles = getAllFiles(HOMEPAGE_DIR);
      const staticExtensions = [
        '.html', '.css', '.js',
        '.gif', '.png', '.jpg', '.jpeg', '.svg', '.webp', '.ico',
        '.woff', '.woff2', '.ttf', '.eot',
        '.json', '.txt', '.xml', '.map'
      ];

      for (const file of allFiles) {
        const ext = path.extname(file).toLowerCase();
        expect(staticExtensions).toContain(ext);
      }
    });
  });

  describe('Test Case 3: Asset paths are relative', () => {
    it('CSS links use relative paths', () => {
      const indexPath = path.join(HOMEPAGE_DIR, 'index.html');
      const content = fs.readFileSync(indexPath, 'utf-8');

      // Find all CSS link tags
      const cssLinks = content.match(/href="[^"]*\.css"/g) || [];
      expect(cssLinks.length).toBeGreaterThan(0);

      for (const link of cssLinks) {
        // Should not start with absolute path (/)
        // But relative path like "css/styles.css" is fine
        expect(link).not.toMatch(/href="\/[^/]/);
        expect(link).not.toMatch(/href="https?:\/\//);
      }
    });

    it('JavaScript references use relative paths', () => {
      const indexPath = path.join(HOMEPAGE_DIR, 'index.html');
      const content = fs.readFileSync(indexPath, 'utf-8');

      // Find all local JS script tags (excluding external CDNs which are OK)
      const jsScripts = content.match(/src="[^"]*\.js"/g) || [];

      for (const script of jsScripts) {
        // Should not use absolute paths for local files
        expect(script).not.toMatch(/src="\/[a-z]/i);
      }
    });

    it('image sources use relative paths', () => {
      const indexPath = path.join(HOMEPAGE_DIR, 'index.html');
      const content = fs.readFileSync(indexPath, 'utf-8');

      // Find all img src and srcset attributes for local images
      const imgSources = content.match(/src="[^"]*\.(gif|png|jpg|jpeg|svg|webp)"/gi) || [];
      const srcSets = content.match(/srcset="[^"]*\.(gif|png|jpg|jpeg|svg|webp)"/gi) || [];

      for (const src of [...imgSources, ...srcSets]) {
        // Local images should use relative paths (assets/images/...)
        // External images (like og:image meta tags) are OK to be absolute
        if (!src.includes('og:image') && !src.includes('http')) {
          expect(src).not.toMatch(/src="\/[a-z]/i);
        }
      }
    });

    it('referenced assets actually exist', () => {
      const indexPath = path.join(HOMEPAGE_DIR, 'index.html');
      const content = fs.readFileSync(indexPath, 'utf-8');

      // Check CSS file exists
      const cssMatch = content.match(/href="([^"]*\.css)"/);
      if (cssMatch) {
        const cssPath = path.join(HOMEPAGE_DIR, cssMatch[1]);
        expect(fs.existsSync(cssPath)).toBe(true);
      }

      // Check JS file exists
      const jsMatch = content.match(/src="(js\/[^"]*\.js)"/);
      if (jsMatch) {
        const jsPath = path.join(HOMEPAGE_DIR, jsMatch[1]);
        expect(fs.existsSync(jsPath)).toBe(true);
      }
    });

    it('404.html also uses relative paths', () => {
      const notFoundPath = path.join(HOMEPAGE_DIR, '404.html');
      if (fs.existsSync(notFoundPath)) {
        const content = fs.readFileSync(notFoundPath, 'utf-8');

        // CSS link should be relative
        const cssLinks = content.match(/href="[^"]*\.css"/g) || [];
        for (const link of cssLinks) {
          expect(link).not.toMatch(/href="\/[^/]/);
          expect(link).not.toMatch(/href="https?:\/\//);
        }
      }
    });
  });

  describe('Test Case 4: 404 handling', () => {
    it('custom 404.html exists', () => {
      const notFoundPath = path.join(HOMEPAGE_DIR, '404.html');
      const exists = fs.existsSync(notFoundPath);
      expect(exists).toBe(true);
    });

    it('404.html contains valid HTML structure', () => {
      const notFoundPath = path.join(HOMEPAGE_DIR, '404.html');
      const content = fs.readFileSync(notFoundPath, 'utf-8');

      expect(content.toLowerCase()).toContain('<!doctype html>');
      expect(content).toContain('<html');
      expect(content).toContain('<head>');
      expect(content).toContain('<body>');
      expect(content).toContain('</html>');
    });

    it('404.html indicates page not found', () => {
      const notFoundPath = path.join(HOMEPAGE_DIR, '404.html');
      const content = fs.readFileSync(notFoundPath, 'utf-8').toLowerCase();

      // Should contain some indication of 404 or "not found"
      const has404Indicator =
        content.includes('404') ||
        content.includes('not found') ||
        content.includes('page not found');
      expect(has404Indicator).toBe(true);
    });

    it('404.html has a link back to homepage', () => {
      const notFoundPath = path.join(HOMEPAGE_DIR, '404.html');
      const content = fs.readFileSync(notFoundPath, 'utf-8');

      // Should have a link to home
      const hasHomeLink =
        content.includes('href="/"') ||
        content.includes('href="index.html"') ||
        content.includes('href="./"');
      expect(hasHomeLink).toBe(true);
    });

    it('404.html uses consistent styling with main site', () => {
      const notFoundPath = path.join(HOMEPAGE_DIR, '404.html');
      const content = fs.readFileSync(notFoundPath, 'utf-8');

      // Should link to the same stylesheet
      expect(content).toContain('css/styles.css');
    });
  });

  describe('Deployment platform compatibility', () => {
    it('directory structure is flat enough for GitHub Pages', () => {
      // GitHub Pages can serve from root or /docs folder
      // The structure should not be too deeply nested
      const allFiles = getAllFiles(HOMEPAGE_DIR);
      const maxDepth = 5;

      for (const file of allFiles) {
        const relativePath = path.relative(HOMEPAGE_DIR, file);
        const depth = relativePath.split(path.sep).length;
        expect(depth).toBeLessThanOrEqual(maxDepth);
      }
    });

    it('no special build output required', () => {
      // Site should be ready to deploy as-is
      // No dist/, build/, or _site/ folder needed
      const buildDirs = ['dist', 'build', '_site', 'out', 'public'];

      for (const dir of buildDirs) {
        const buildPath = path.join(HOMEPAGE_DIR, dir);
        // These directories should not be required for deployment
        // The homepage directory itself should be deployable
        if (fs.existsSync(buildPath)) {
          // If they exist, they should just be additional, not required
          const indexInBuild = path.join(buildPath, 'index.html');
          // Main index.html should be in homepage root, not just in build folder
          expect(fs.existsSync(path.join(HOMEPAGE_DIR, 'index.html'))).toBe(true);
        }
      }
    });

    it('file names are safe for all platforms', () => {
      const allFiles = getAllFiles(HOMEPAGE_DIR);
      const unsafeChars = /[<>:"|?*\\]/;

      for (const file of allFiles) {
        const fileName = path.basename(file);
        expect(fileName).not.toMatch(unsafeChars);
        // No spaces in file names (best practice for URLs)
        expect(fileName).not.toContain(' ');
      }
    });
  });
});

/**
 * Helper function to find files with a specific extension
 */
function findFiles(dir: string, extension: string): string[] {
  const files: string[] = [];

  if (!fs.existsSync(dir)) {
    return files;
  }

  const items = fs.readdirSync(dir);

  for (const item of items) {
    const fullPath = path.join(dir, item);
    const stat = fs.statSync(fullPath);

    if (stat.isDirectory()) {
      files.push(...findFiles(fullPath, extension));
    } else if (item.endsWith(extension)) {
      files.push(fullPath);
    }
  }

  return files;
}

/**
 * Helper function to get all files recursively
 */
function getAllFiles(dir: string): string[] {
  const files: string[] = [];

  if (!fs.existsSync(dir)) {
    return files;
  }

  const items = fs.readdirSync(dir);

  for (const item of items) {
    const fullPath = path.join(dir, item);
    const stat = fs.statSync(fullPath);

    if (stat.isDirectory()) {
      files.push(...getAllFiles(fullPath));
    } else {
      files.push(fullPath);
    }
  }

  return files;
}
