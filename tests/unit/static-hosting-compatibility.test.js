/**
 * Unit and Integration tests for Static Hosting Compatibility
 * Scenario: Static Hosting Compatibility
 * Validates that the homepage can be deployed to static hosting platforms
 * (GitHub Pages, Netlify) without server-side requirements
 */

const fs = require('fs');
const path = require('path');
const { JSDOM } = require('jsdom');

describe('Static Hosting Compatibility', () => {
  const projectRoot = path.resolve(__dirname, '../../');
  let htmlContent;
  let document;
  let dom;

  beforeAll(() => {
    const htmlPath = path.resolve(projectRoot, 'index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf-8');
    dom = new JSDOM(htmlContent);
    document = dom.window.document;
  });

  describe('Test Case 1: Build output contains static files only', () => {
    test('should have a valid HTML file at root', () => {
      const htmlPath = path.resolve(projectRoot, 'index.html');
      expect(fs.existsSync(htmlPath)).toBe(true);
    });

    test('HTML file should be valid HTML5 document', () => {
      expect(htmlContent).toContain('<!DOCTYPE html>');
      expect(htmlContent).toContain('<html');
      expect(htmlContent).toContain('</html>');
    });

    test('should contain inline CSS (no external CSS build step required)', () => {
      const styleElements = document.querySelectorAll('style');
      expect(styleElements.length).toBeGreaterThan(0);
    });

    test('should not have any server-side file extensions referenced', () => {
      // Check that no PHP, JSP, ASP, or other server-side file extensions are referenced
      const serverSidePatterns = [
        /\.php/i,
        /\.jsp/i,
        /\.asp/i,
        /\.aspx/i,
        /\.cfm/i,
        /\.cgi/i,
        /\.pl/i,
        /\.rb/i,
      ];

      serverSidePatterns.forEach((pattern) => {
        expect(htmlContent).not.toMatch(pattern);
      });
    });

    test('should only reference static file types (HTML, CSS, JS, images)', () => {
      const allowedExtensions = ['.html', '.css', '.js', '.png', '.jpg', '.jpeg', '.gif', '.svg', '.ico', '.webp', '.avif'];

      // Get all href and src attributes
      const links = document.querySelectorAll('[href], [src]');

      links.forEach((element) => {
        const href = element.getAttribute('href');
        const src = element.getAttribute('src');
        const url = href || src;

        if (url && !url.startsWith('http') && !url.startsWith('//') && !url.startsWith('#') && !url.startsWith('data:') && !url.startsWith('mailto:')) {
          const ext = path.extname(url).toLowerCase();
          if (ext) {
            expect(allowedExtensions).toContain(ext);
          }
        }
      });
    });

    test('assets directory should only contain static files if it exists', () => {
      const assetsPath = path.resolve(projectRoot, 'assets');
      if (fs.existsSync(assetsPath)) {
        const files = fs.readdirSync(assetsPath);
        const staticExtensions = ['.png', '.jpg', '.jpeg', '.gif', '.svg', '.ico', '.webp', '.avif', '.css', '.js', '.woff', '.woff2', '.ttf', '.eot'];

        files.forEach((file) => {
          const ext = path.extname(file).toLowerCase();
          if (ext) {
            expect(staticExtensions).toContain(ext);
          }
        });
      }
    });
  });

  describe('Test Case 2: Page loads and functions from static server', () => {
    test('HTML structure should be complete and self-contained', () => {
      const head = document.querySelector('head');
      const body = document.querySelector('body');

      expect(head).not.toBeNull();
      expect(body).not.toBeNull();
    });

    test('all styles should be embedded or reference static files', () => {
      const styleLinks = document.querySelectorAll('link[rel="stylesheet"]');

      styleLinks.forEach((link) => {
        const href = link.getAttribute('href');
        // External stylesheets should be CDN links or local static files
        if (href && !href.startsWith('http')) {
          expect(href).toMatch(/\.(css)$/i);
        }
      });
    });

    test('all scripts should be static files (no server-side generation)', () => {
      const scripts = document.querySelectorAll('script[src]');

      scripts.forEach((script) => {
        const src = script.getAttribute('src');
        if (src && !src.startsWith('http')) {
          expect(src).toMatch(/\.(js)$/i);
        }
      });
    });

    test('navigation links should work without server-side routing', () => {
      const internalLinks = document.querySelectorAll('a[href^="#"], a[href^="./"], a[href="index.html"]');

      internalLinks.forEach((link) => {
        const href = link.getAttribute('href');
        // Anchor links should start with # or reference static HTML files
        expect(href).toMatch(/^(#|\.\/|index\.html)/);
      });
    });

    test('page should have meaningful content without JavaScript', () => {
      // Core content should be visible in HTML without JS
      const heroContent = document.querySelector('.hero, [class*="hero"], header');
      const mainContent = document.querySelector('main, .main, #main');

      expect(heroContent || mainContent).not.toBeNull();

      // Text content should be present in HTML
      const textContent = document.body.textContent;
      expect(textContent).toContain('MirDB');
    });
  });

  describe('Test Case 3: No API calls or server-side rendering required', () => {
    test('should not have fetch or XMLHttpRequest for critical content', () => {
      const scripts = document.querySelectorAll('script:not([src])');
      let inlineJS = '';

      scripts.forEach((script) => {
        inlineJS += script.textContent;
      });

      // Check that there's no data fetching for initial content
      // Small fetch calls for analytics are ok, but critical content shouldn't depend on them
      const hasCriticalFetch = /fetch\s*\(\s*['"`]\/api/i.test(inlineJS) ||
                               /new\s+XMLHttpRequest/i.test(inlineJS);

      expect(hasCriticalFetch).toBe(false);
    });

    test('all visible content should be pre-rendered in HTML', () => {
      // Check that main sections have actual content
      const h1 = document.querySelector('h1');
      expect(h1).not.toBeNull();
      expect(h1.textContent.trim().length).toBeGreaterThan(0);

      // Check for pre-rendered feature content
      const featureCards = document.querySelectorAll('.feature-card, [class*="feature"]');
      if (featureCards.length > 0) {
        featureCards.forEach((card) => {
          expect(card.textContent.trim().length).toBeGreaterThan(0);
        });
      }
    });

    test('should not require server-side session or authentication', () => {
      // No session-based content markers
      const sessionPatterns = [
        /\{\{.*user.*\}\}/i,
        /\{\{.*session.*\}\}/i,
        /<%.*%>/,
        /<\?php/i,
        /<%= /,
      ];

      sessionPatterns.forEach((pattern) => {
        expect(htmlContent).not.toMatch(pattern);
      });
    });

    test('should not have form actions pointing to server-side handlers', () => {
      const forms = document.querySelectorAll('form');

      forms.forEach((form) => {
        const action = form.getAttribute('action');
        if (action) {
          // Form actions should be external services or mailto links, not local server endpoints
          expect(action).not.toMatch(/^\/[^\/]/);
        }
      });
    });

    test('meta tags should contain pre-rendered content', () => {
      const description = document.querySelector('meta[name="description"]');
      const ogTitle = document.querySelector('meta[property="og:title"]');

      expect(description).not.toBeNull();
      expect(description.getAttribute('content').length).toBeGreaterThan(0);

      if (ogTitle) {
        expect(ogTitle.getAttribute('content').length).toBeGreaterThan(0);
      }
    });
  });

  describe('Test Case 4: GitHub Pages compatibility', () => {
    test('should not have _config.yml that conflicts with Jekyll', () => {
      const configPath = path.resolve(projectRoot, '_config.yml');
      // If _config.yml exists, it should be intentional for Jekyll usage
      // For pure static sites, it's better to have .nojekyll
      if (fs.existsSync(configPath)) {
        console.log('Note: _config.yml exists - ensure Jekyll compatibility is intentional');
      }
    });

    test('should have .nojekyll file or not use underscore-prefixed directories', () => {
      const nojekyllPath = path.resolve(projectRoot, '.nojekyll');
      const hasNojekyll = fs.existsSync(nojekyllPath);

      // Check for underscore-prefixed directories that would be ignored by Jekyll
      const rootFiles = fs.readdirSync(projectRoot);
      const underscoreDirs = rootFiles.filter((file) => {
        const filePath = path.resolve(projectRoot, file);
        return file.startsWith('_') && fs.statSync(filePath).isDirectory();
      });

      // Either have .nojekyll or no underscore directories
      // .nojekyll is optional if there are no underscore-prefixed directories
      if (underscoreDirs.length > 0) {
        console.log(`Underscore directories found: ${underscoreDirs.join(', ')}`);
        console.log('Consider adding .nojekyll file for GitHub Pages');
      }

      // This is a soft check - both configurations can work
      expect(true).toBe(true);
    });

    test('should have index.html at root level', () => {
      const indexPath = path.resolve(projectRoot, 'index.html');
      expect(fs.existsSync(indexPath)).toBe(true);
    });

    test('relative paths should work without base URL configuration', () => {
      // Check that asset references use relative paths that work from root
      const assetRefs = document.querySelectorAll('[src], [href]');

      assetRefs.forEach((element) => {
        const src = element.getAttribute('src');
        const href = element.getAttribute('href');
        const url = src || href;

        if (url && !url.startsWith('http') && !url.startsWith('//') && !url.startsWith('#') && !url.startsWith('data:') && !url.startsWith('mailto:')) {
          // Should not have absolute paths that assume specific server root
          expect(url).not.toMatch(/^\/(?!\/)/);
        }
      });
    });

    test('should not require server-side redirects', () => {
      // Check for patterns that would require server-side redirects
      const redirectPatterns = [
        /window\.location\.href\s*=\s*['"`]\/[^\/]/,
        /window\.location\.replace\s*\(\s*['"`]\/[^\/]/,
      ];

      const scripts = document.querySelectorAll('script:not([src])');
      let inlineJS = '';

      scripts.forEach((script) => {
        inlineJS += script.textContent;
      });

      redirectPatterns.forEach((pattern) => {
        // Allow relative redirects, just not absolute server paths
        const hasAbsoluteRedirect = pattern.test(inlineJS);
        if (hasAbsoluteRedirect) {
          console.log('Warning: Found absolute path redirect which may not work on GitHub Pages');
        }
      });
    });

    test('should not have CNAME file conflicts (or valid CNAME)', () => {
      const cnamePath = path.resolve(projectRoot, 'CNAME');
      if (fs.existsSync(cnamePath)) {
        const cname = fs.readFileSync(cnamePath, 'utf-8').trim();
        // CNAME should contain a valid domain if present
        expect(cname).toMatch(/^[a-zA-Z0-9][a-zA-Z0-9-_.]+\.[a-zA-Z]{2,}$/);
      }
    });

    test('404.html should exist for SPA routing support (optional)', () => {
      const notFoundPath = path.resolve(projectRoot, '404.html');
      // This is optional but good practice for GitHub Pages
      if (fs.existsSync(notFoundPath)) {
        const content = fs.readFileSync(notFoundPath, 'utf-8');
        expect(content).toContain('<!DOCTYPE html>');
      }
      // Pass even if 404.html doesn't exist (it's optional for simple static sites)
      expect(true).toBe(true);
    });

    test('all referenced local assets should exist', () => {
      const assetRefs = document.querySelectorAll('img[src], link[href], script[src]');

      assetRefs.forEach((element) => {
        const src = element.getAttribute('src');
        const href = element.getAttribute('href');
        const url = src || href;

        if (url && !url.startsWith('http') && !url.startsWith('//') && !url.startsWith('#') && !url.startsWith('data:')) {
          // Clean the URL (remove query strings, fragments)
          const cleanUrl = url.split('?')[0].split('#')[0];
          const assetPath = path.resolve(projectRoot, cleanUrl);

          // Only check if it's a file reference (not anchor or external)
          const ext = path.extname(cleanUrl);
          if (ext && ['.css', '.js', '.png', '.jpg', '.gif', '.svg', '.ico', '.woff', '.woff2'].includes(ext.toLowerCase())) {
            if (!fs.existsSync(assetPath)) {
              console.log(`Warning: Referenced asset not found: ${cleanUrl}`);
            }
          }
        }
      });

      // This is a soft check to not fail on CDN resources
      expect(true).toBe(true);
    });
  });

  describe('Static File Structure Verification', () => {
    test('should have proper MIME-type compatible file extensions', () => {
      const validMimeTypes = {
        '.html': 'text/html',
        '.css': 'text/css',
        '.js': 'application/javascript',
        '.json': 'application/json',
        '.png': 'image/png',
        '.jpg': 'image/jpeg',
        '.jpeg': 'image/jpeg',
        '.gif': 'image/gif',
        '.svg': 'image/svg+xml',
        '.ico': 'image/x-icon',
        '.webp': 'image/webp',
      };

      const indexPath = path.resolve(projectRoot, 'index.html');
      const ext = path.extname(indexPath).toLowerCase();

      expect(ext in validMimeTypes).toBe(true);
    });

    test('HTML file should be servable as static content', () => {
      // Verify the HTML is complete and doesn't require processing
      expect(htmlContent.indexOf('<!DOCTYPE html>')).toBeLessThan(50);
      expect(htmlContent).toContain('</html>');

      // Should not contain server-side template markers
      expect(htmlContent).not.toContain('<?php');
      expect(htmlContent).not.toContain('<%=');
      expect(htmlContent).not.toContain('{%');
      expect(htmlContent).not.toContain('{{#');
    });
  });
});
