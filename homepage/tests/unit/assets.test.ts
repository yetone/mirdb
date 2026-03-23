/**
 * Static Deployment Unit Tests
 * Owner: Scenario 13 - Static Deployment Verification
 *
 * Test cases:
 * - No server-side dependencies
 * - All assets properly referenced
 * - No required external API calls
 * - Static file server compatibility
 */

import { describe, it, expect, beforeAll } from 'vitest';
import { JSDOM } from 'jsdom';
import * as fs from 'fs';
import * as path from 'path';

const homepageDir = path.resolve(__dirname, '../..');
let document: Document;
let htmlContent: string;
let jsContent: string;

beforeAll(() => {
  const htmlPath = path.resolve(homepageDir, 'index.html');
  htmlContent = fs.readFileSync(htmlPath, 'utf-8');
  const dom = new JSDOM(htmlContent);
  document = dom.window.document;

  const jsPath = path.resolve(homepageDir, 'js/main.js');
  jsContent = fs.readFileSync(jsPath, 'utf-8');
});

describe('Static Deployment Verification Tests', () => {
  describe('Test Case 1: No Server-Side Dependencies', () => {
    it('should not contain server-side script files in homepage directory', () => {
      const serverSideExtensions = ['.php', '.py', '.rb', '.jsp', '.asp', '.aspx'];
      const files = getAllFiles(homepageDir);

      const serverSideFiles = files.filter(file => {
        const ext = path.extname(file).toLowerCase();
        return serverSideExtensions.includes(ext);
      });

      expect(serverSideFiles).toEqual([]);
    });

    it('should not have server framework dependencies in package.json', () => {
      const packageJsonPath = path.resolve(homepageDir, 'package.json');
      const packageJson = JSON.parse(fs.readFileSync(packageJsonPath, 'utf-8'));

      const serverFrameworks = [
        'express', 'koa', 'hapi', 'fastify', 'next', 'nuxt',
        'gatsby', 'nest', 'sails', 'meteor', 'loopback'
      ];

      const allDeps = {
        ...packageJson.dependencies,
        ...packageJson.devDependencies
      };

      const foundServerDeps = Object.keys(allDeps || {}).filter(dep =>
        serverFrameworks.some(framework => dep.includes(framework))
      );

      expect(foundServerDeps).toEqual([]);
    });

    it('should not have a server entry point file', () => {
      const serverEntryFiles = [
        'server.js', 'server.ts', 'app.js', 'app.ts',
        'index.js', 'index.ts', 'start.js', 'start.ts'
      ];

      const existingServerFiles = serverEntryFiles.filter(file =>
        fs.existsSync(path.resolve(homepageDir, file))
      );

      expect(existingServerFiles).toEqual([]);
    });

    it('should only contain static file types', () => {
      const allowedExtensions = [
        '.html', '.css', '.js', '.ts', '.json', '.md',
        '.png', '.jpg', '.jpeg', '.gif', '.svg', '.ico', '.webp',
        '.woff', '.woff2', '.ttf', '.eot', '.otf'
      ];

      const files = getAllFiles(homepageDir).filter(file => {
        // Exclude test files and node_modules
        return !file.includes('node_modules') &&
               !file.includes('tests/') &&
               !file.includes('.lock');
      });

      const invalidFiles = files.filter(file => {
        const ext = path.extname(file).toLowerCase();
        return ext && !allowedExtensions.includes(ext);
      });

      expect(invalidFiles).toEqual([]);
    });
  });

  describe('Test Case 3: No Required External API Calls', () => {
    it('should not make required fetch calls in main JavaScript', () => {
      // Check for fetch API calls that are required for functionality
      const fetchPatterns = [
        /fetch\s*\(/g,
        /\.json\(\)/g,
        /XMLHttpRequest/g,
        /\.ajax\(/g,
        /axios\./g
      ];

      const hasFetchCalls = fetchPatterns.some(pattern => pattern.test(jsContent));
      expect(hasFetchCalls).toBe(false);
    });

    it('should not have API endpoint configuration', () => {
      // Check for API URL patterns in JavaScript
      const apiPatterns = [
        /api\s*[=:]\s*['"`]/i,
        /endpoint\s*[=:]\s*['"`]/i,
        /baseUrl\s*[=:]\s*['"`]/i,
        /apiUrl\s*[=:]\s*['"`]/i
      ];

      const hasApiConfig = apiPatterns.some(pattern => pattern.test(jsContent));
      expect(hasApiConfig).toBe(false);
    });

    it('should not require websocket connections', () => {
      const websocketPatterns = [
        /new\s+WebSocket\s*\(/g,
        /socket\.io/gi,
        /\.connect\(\s*['"`]ws/g
      ];

      const hasWebsockets = websocketPatterns.some(pattern => pattern.test(jsContent));
      expect(hasWebsockets).toBe(false);
    });

    it('should not have async data loading requirements', () => {
      // Content should be statically included in HTML
      const dataLoadingPatterns = [
        /loadData\s*\(/g,
        /getData\s*\(/g,
        /fetchData\s*\(/g,
        /\.then\s*\(\s*response\s*=>/g
      ];

      const hasDataLoading = dataLoadingPatterns.some(pattern => pattern.test(jsContent));
      expect(hasDataLoading).toBe(false);
    });
  });

  describe('Test Case 4: All Assets Are Properly Bundled', () => {
    it('should have all referenced CSS files present', () => {
      const linkElements = document.querySelectorAll('link[rel="stylesheet"]');
      const cssFiles: string[] = [];

      linkElements.forEach(link => {
        const href = link.getAttribute('href');
        if (href && !href.startsWith('http')) {
          cssFiles.push(href);
        }
      });

      cssFiles.forEach(cssFile => {
        const cssPath = path.resolve(homepageDir, cssFile);
        expect(fs.existsSync(cssPath)).toBe(true);
      });

      // Verify we found expected CSS files
      expect(cssFiles.length).toBeGreaterThan(0);
    });

    it('should have all referenced JavaScript files present', () => {
      const scriptElements = document.querySelectorAll('script[src]');
      const jsFiles: string[] = [];

      scriptElements.forEach(script => {
        const src = script.getAttribute('src');
        if (src && !src.startsWith('http')) {
          jsFiles.push(src);
        }
      });

      jsFiles.forEach(jsFile => {
        const jsPath = path.resolve(homepageDir, jsFile);
        expect(fs.existsSync(jsPath)).toBe(true);
      });

      // Verify we found expected JS files
      expect(jsFiles.length).toBeGreaterThan(0);
    });

    it('should have CSS imports resolved locally', () => {
      const cssPath = path.resolve(homepageDir, 'css/styles.css');
      const cssContent = fs.readFileSync(cssPath, 'utf-8');

      // Check for @import statements
      const importMatches = cssContent.match(/@import\s+url\s*\(\s*['"]?([^'")]+)['"]?\s*\)/g) || [];

      importMatches.forEach(importStmt => {
        const urlMatch = importStmt.match(/url\s*\(\s*['"]?([^'")]+)['"]?\s*\)/);
        if (urlMatch && urlMatch[1]) {
          const importPath = urlMatch[1];
          // Skip external URLs
          if (!importPath.startsWith('http')) {
            const resolvedPath = path.resolve(homepageDir, 'css', importPath);
            expect(fs.existsSync(resolvedPath)).toBe(true);
          }
        }
      });
    });

    it('should not have broken image references', () => {
      const imgElements = document.querySelectorAll('img[src]');

      imgElements.forEach(img => {
        const src = img.getAttribute('src');
        if (src && !src.startsWith('http') && !src.startsWith('data:')) {
          const imgPath = path.resolve(homepageDir, src);
          // Only check if image is referenced and expected to exist
          if (!src.startsWith('images/placeholder')) {
            expect(fs.existsSync(imgPath)).toBe(true);
          }
        }
      });
    });

    it('should have index.html as the entry point', () => {
      const indexPath = path.resolve(homepageDir, 'index.html');
      expect(fs.existsSync(indexPath)).toBe(true);

      // Verify it's a valid HTML document
      expect(htmlContent).toContain('<!DOCTYPE html>');
      expect(htmlContent).toContain('<html');
      expect(htmlContent).toContain('</html>');
    });

    it('should have all required meta tags for static hosting', () => {
      const charset = document.querySelector('meta[charset]');
      const viewport = document.querySelector('meta[name="viewport"]');

      expect(charset).not.toBeNull();
      expect(viewport).not.toBeNull();
    });
  });

  describe('Test Case 2: Static File Server Compatibility', () => {
    it('should use relative paths for local assets', () => {
      const links = document.querySelectorAll('link[href]');
      const scripts = document.querySelectorAll('script[src]');

      links.forEach(link => {
        const href = link.getAttribute('href');
        if (href && !href.startsWith('http')) {
          // Should not start with absolute path
          expect(href.startsWith('/')).toBe(false);
        }
      });

      scripts.forEach(script => {
        const src = script.getAttribute('src');
        if (src && !src.startsWith('http')) {
          // Should not start with absolute path
          expect(src.startsWith('/')).toBe(false);
        }
      });
    });

    it('should have proper MIME-type compatible file extensions', () => {
      const cssFiles = getAllFiles(path.resolve(homepageDir, 'css')).filter(f =>
        !f.includes('node_modules')
      );
      const jsFiles = getAllFiles(path.resolve(homepageDir, 'js')).filter(f =>
        !f.includes('node_modules')
      );

      cssFiles.forEach(file => {
        expect(file.endsWith('.css')).toBe(true);
      });

      jsFiles.forEach(file => {
        expect(file.endsWith('.js')).toBe(true);
      });
    });

    it('should not require URL rewriting or routing', () => {
      // Check that internal links use anchor tags properly
      const internalLinks = document.querySelectorAll('a[href^="#"]');
      expect(internalLinks.length).toBeGreaterThan(0);

      // All navigation should work without server-side routing
      internalLinks.forEach(link => {
        const href = link.getAttribute('href');
        if (href && href !== '#') {
          const targetId = href.substring(1);
          const targetElement = document.getElementById(targetId);
          expect(targetElement).not.toBeNull();
        }
      });
    });

    it('should have a flat directory structure suitable for static hosting', () => {
      // Key directories should exist at expected locations
      expect(fs.existsSync(path.resolve(homepageDir, 'css'))).toBe(true);
      expect(fs.existsSync(path.resolve(homepageDir, 'js'))).toBe(true);
      expect(fs.existsSync(path.resolve(homepageDir, 'index.html'))).toBe(true);
    });

    it('should work without server-side redirects', () => {
      // External links should have complete URLs
      const externalLinks = document.querySelectorAll('a[target="_blank"]');

      externalLinks.forEach(link => {
        const href = link.getAttribute('href');
        expect(href).toMatch(/^https?:\/\//);
      });
    });
  });
});

/**
 * Recursively get all files in a directory
 */
function getAllFiles(dirPath: string, arrayOfFiles: string[] = []): string[] {
  if (!fs.existsSync(dirPath)) {
    return arrayOfFiles;
  }

  const files = fs.readdirSync(dirPath);

  files.forEach(file => {
    const filePath = path.join(dirPath, file);
    if (fs.statSync(filePath).isDirectory()) {
      if (!file.includes('node_modules')) {
        getAllFiles(filePath, arrayOfFiles);
      }
    } else {
      arrayOfFiles.push(filePath);
    }
  });

  return arrayOfFiles;
}
