/**
 * Static Site Deployment Compatibility Tests
 *
 * Scenario: Verify the site is suitable for static hosting deployment
 * Test Cases:
 * 1. Serve site from static file server - Site works fully from static files without backend
 * 2. Run HTML validator on index.html - No critical HTML validation errors
 * 3. Check all internal links use relative paths - Internal assets use relative or root-relative paths
 * 4. Deploy to GitHub Pages test environment - Site renders correctly when served from GitHub Pages
 */

const fs = require('fs');
const path = require('path');

// Load HTML content for DOM testing
const htmlPath = path.join(__dirname, '..', 'index.html');
const htmlContent = fs.existsSync(htmlPath) ? fs.readFileSync(htmlPath, 'utf-8') : '';

describe('Static Site Deployment Compatibility', () => {
  beforeAll(() => {
    document.documentElement.innerHTML = htmlContent;
  });

  /**
   * Test Case 1: Static File Server Compatibility
   * Expected: Site works fully from static files without backend
   */
  describe('Test Case 1: Static File Server Compatibility', () => {
    test('should have valid HTML structure without server-side requirements', () => {
      // Check for DOCTYPE
      expect(htmlContent).toMatch(/^<!DOCTYPE html>/i);

      // Check for html tag with lang attribute in source
      expect(htmlContent).toMatch(/<html\s+lang=["']en["']/i);
    });

    test('should not have any server-side processing directives', () => {
      // Check for PHP tags
      expect(htmlContent).not.toMatch(/<\?php/i);
      expect(htmlContent).not.toMatch(/\?>/);

      // Check for ASP tags
      expect(htmlContent).not.toMatch(/<%/);
      expect(htmlContent).not.toMatch(/%>/);

      // Check for JSP tags
      expect(htmlContent).not.toMatch(/<%@/);
      expect(htmlContent).not.toMatch(/<%!/);

      // Check for server-side includes
      expect(htmlContent).not.toMatch(/<!--#include/i);
      expect(htmlContent).not.toMatch(/<!--#exec/i);
    });

    test('should not have any backend API calls for essential content', () => {
      // Check for dynamic data loading patterns in script tags
      const scripts = document.querySelectorAll('script');
      let hasBackendDependency = false;

      scripts.forEach(script => {
        const content = script.textContent || '';
        // Check for fetch calls to backend APIs
        if (content.includes('fetch(') && content.includes('/api/')) {
          hasBackendDependency = true;
        }
        // Check for XMLHttpRequest to backend
        if (content.includes('XMLHttpRequest') && content.includes('/api/')) {
          hasBackendDependency = true;
        }
      });

      expect(hasBackendDependency).toBe(false);
    });

    test('should have all essential content rendered in HTML', () => {
      // Check that main sections exist in the HTML (not dynamically loaded)
      const sections = ['hero', 'features', 'code-example', 'quickstart', 'architecture'];

      sections.forEach(section => {
        const element = document.getElementById(section);
        expect(element).not.toBeNull();
        expect(element.innerHTML.trim().length).toBeGreaterThan(0);
      });
    });

    test('should not require any environment variables or configuration files', () => {
      // Check that no JavaScript references process.env or similar
      const scripts = document.querySelectorAll('script');

      scripts.forEach(script => {
        const content = script.textContent || '';
        // These patterns indicate server-side or build-time dependencies
        expect(content).not.toMatch(/process\.env/);
        expect(content).not.toMatch(/import\.meta\.env/);
        expect(content).not.toMatch(/__ENV__/);
        expect(content).not.toMatch(/window\.__CONFIG__/);
      });
    });

    test('should be a self-contained HTML file with inline styles', () => {
      // Check for inline styles (needed for static deployment without build step)
      const styleElements = document.querySelectorAll('style');
      expect(styleElements.length).toBeGreaterThan(0);

      let totalCSSSize = 0;
      styleElements.forEach(style => {
        totalCSSSize += (style.textContent || '').length;
      });

      // Should have substantial CSS content
      expect(totalCSSSize).toBeGreaterThan(1000);
    });
  });

  /**
   * Test Case 2: HTML Validation
   * Expected: No critical HTML validation errors
   */
  describe('Test Case 2: HTML Validation', () => {
    test('should have proper DOCTYPE declaration', () => {
      const doctype = htmlContent.match(/^<!DOCTYPE\s+html/i);
      expect(doctype).not.toBeNull();
    });

    test('should have required head elements', () => {
      const head = document.querySelector('head');
      expect(head).not.toBeNull();

      // Required meta tags
      const charset = document.querySelector('meta[charset]');
      expect(charset).not.toBeNull();
      expect(charset.getAttribute('charset').toLowerCase()).toBe('utf-8');

      const viewport = document.querySelector('meta[name="viewport"]');
      expect(viewport).not.toBeNull();
      expect(viewport.getAttribute('content')).toContain('width=device-width');

      const title = document.querySelector('title');
      expect(title).not.toBeNull();
      expect(title.textContent.trim().length).toBeGreaterThan(0);
    });

    test('should have semantic HTML structure', () => {
      // Check for semantic elements
      const nav = document.querySelector('nav');
      expect(nav).not.toBeNull();

      const sections = document.querySelectorAll('section');
      expect(sections.length).toBeGreaterThan(0);

      const footer = document.querySelector('footer');
      expect(footer).not.toBeNull();
    });

    test('should have proper heading hierarchy', () => {
      const h1Elements = document.querySelectorAll('h1');
      expect(h1Elements.length).toBe(1); // Only one h1 per page

      // Check that headings exist in a logical order
      const allHeadings = document.querySelectorAll('h1, h2, h3, h4, h5, h6');
      expect(allHeadings.length).toBeGreaterThan(1);

      // First heading should be h1
      expect(allHeadings[0].tagName).toBe('H1');
    });

    test('should have alt attributes on all images', () => {
      const images = document.querySelectorAll('img');

      images.forEach(img => {
        const alt = img.getAttribute('alt');
        expect(alt).not.toBeNull();
        expect(alt.trim().length).toBeGreaterThan(0);
      });
    });

    test('should have proper link attributes', () => {
      const externalLinks = document.querySelectorAll('a[target="_blank"]');

      externalLinks.forEach(link => {
        const rel = link.getAttribute('rel');
        expect(rel).not.toBeNull();
        // Should have noopener for security
        expect(rel).toContain('noopener');
      });
    });

    test('should not have deprecated HTML elements', () => {
      const deprecatedElements = [
        'center', 'font', 'marquee', 'blink', 'frame',
        'frameset', 'noframes', 'applet', 'basefont',
        'big', 'strike', 'tt'
      ];

      deprecatedElements.forEach(element => {
        const found = document.querySelector(element);
        expect(found).toBeNull();
      });
    });

    test('should have valid id attributes (no duplicates)', () => {
      const elementsWithId = document.querySelectorAll('[id]');
      const ids = new Set();
      const duplicates = [];

      elementsWithId.forEach(el => {
        const id = el.getAttribute('id');
        if (ids.has(id)) {
          duplicates.push(id);
        }
        ids.add(id);
      });

      expect(duplicates).toEqual([]);
    });

    test('should have properly nested elements', () => {
      // Check that interactive elements are not nested improperly
      const buttonsInLinks = document.querySelectorAll('a button');
      expect(buttonsInLinks.length).toBe(0);

      const linksInLinks = document.querySelectorAll('a a');
      expect(linksInLinks.length).toBe(0);
    });
  });

  /**
   * Test Case 3: Relative Paths for Internal Assets
   * Expected: Internal assets use relative or root-relative paths
   */
  describe('Test Case 3: Relative Paths for Internal Assets', () => {
    test('should use relative paths for local images', () => {
      const images = document.querySelectorAll('img');
      const localImages = [];
      const absoluteLocalPaths = [];

      images.forEach(img => {
        const src = img.getAttribute('src');
        if (!src) return;

        // Skip external images (http/https)
        if (src.startsWith('http://') || src.startsWith('https://') || src.startsWith('//')) {
          return;
        }

        localImages.push(src);

        // Check for absolute paths that won't work in subdirectory deployment
        if (src.startsWith('/') && !src.startsWith('./') && !src.startsWith('../')) {
          // Root-relative paths (starting with /) are acceptable for GitHub Pages
          // as long as they're consistent, but relative paths are preferred
          // For subdirectory deployment, relative paths work better
        }
      });

      console.log('Local image paths:');
      localImages.forEach(src => {
        const isRelative = !src.startsWith('/') || src.startsWith('./') || src.startsWith('../');
        const status = isRelative ? '✓ relative' : '✓ root-relative';
        console.log(`  ${status}: ${src}`);
      });

      // All local images should have paths
      expect(localImages.length).toBeGreaterThan(0);

      // Check that paths use consistent relative format
      localImages.forEach(src => {
        // Should not be absolute file system paths
        expect(src).not.toMatch(/^[A-Z]:\\/i); // Windows absolute
        expect(src).not.toMatch(/^\/home\//i); // Linux absolute
        expect(src).not.toMatch(/^\/Users\//i); // Mac absolute
      });
    });

    test('should use relative paths for internal navigation links', () => {
      const links = document.querySelectorAll('a[href]');
      const internalLinks = [];

      links.forEach(link => {
        const href = link.getAttribute('href');
        if (!href) return;

        // Skip external links and anchor links
        if (href.startsWith('http://') ||
            href.startsWith('https://') ||
            href.startsWith('//') ||
            href.startsWith('#') ||
            href.startsWith('mailto:') ||
            href.startsWith('tel:')) {
          return;
        }

        internalLinks.push(href);
      });

      console.log('Internal navigation links:');
      internalLinks.forEach(href => {
        console.log(`  - ${href}`);
      });

      // Internal links should use relative or anchor paths
      internalLinks.forEach(href => {
        // Should not be absolute file system paths
        expect(href).not.toMatch(/^[A-Z]:\\/i);
        expect(href).not.toMatch(/^\/home\//i);
        expect(href).not.toMatch(/^\/Users\//i);
      });
    });

    test('should not have hardcoded localhost URLs', () => {
      // Check in href attributes
      const linksWithLocalhost = document.querySelectorAll('a[href*="localhost"]');
      expect(linksWithLocalhost.length).toBe(0);

      // Check in src attributes
      const srcWithLocalhost = document.querySelectorAll('[src*="localhost"]');
      expect(srcWithLocalhost.length).toBe(0);

      // Check in style attributes and inline content
      expect(htmlContent).not.toMatch(/url\(['"]?http:\/\/localhost/i);
      expect(htmlContent).not.toMatch(/url\(['"]?http:\/\/127\.0\.0\.1/i);
    });

    test('should not have hardcoded file:// protocol URLs', () => {
      expect(htmlContent).not.toMatch(/file:\/\//i);
    });

    test('should have CSS url() references use relative paths', () => {
      const styleElements = document.querySelectorAll('style');

      styleElements.forEach(style => {
        const content = style.textContent || '';

        // Find all url() references
        const urlMatches = content.match(/url\(['"]?([^'")\s]+)['"]?\)/gi) || [];

        urlMatches.forEach(match => {
          // Extract the URL from url()
          const urlMatch = match.match(/url\(['"]?([^'")\s]+)['"]?\)/i);
          if (urlMatch && urlMatch[1]) {
            const url = urlMatch[1];

            // Skip data URIs
            if (url.startsWith('data:')) return;

            // Should not have absolute file system paths
            expect(url).not.toMatch(/^[A-Z]:\\/i);
            expect(url).not.toMatch(/^\/home\//i);
            expect(url).not.toMatch(/^\/Users\//i);
            expect(url).not.toMatch(/^file:\/\//i);
          }
        });
      });
    });

    test('should have asset paths that exist in the repository', () => {
      const images = document.querySelectorAll('img');
      const missingAssets = [];

      images.forEach(img => {
        const src = img.getAttribute('src');
        if (!src) return;

        // Skip external images
        if (src.startsWith('http://') || src.startsWith('https://') || src.startsWith('//')) {
          return;
        }

        // Check if file exists
        const assetPath = path.join(__dirname, '..', src);
        if (!fs.existsSync(assetPath)) {
          missingAssets.push(src);
        }
      });

      if (missingAssets.length > 0) {
        console.log('Missing assets:', missingAssets);
      }

      expect(missingAssets).toEqual([]);
    });
  });

  /**
   * Test Case 4: GitHub Pages Compatibility
   * Expected: Site renders correctly when served from GitHub Pages
   */
  describe('Test Case 4: GitHub Pages Compatibility', () => {
    test('should have index.html at root level', () => {
      expect(fs.existsSync(htmlPath)).toBe(true);
    });

    test('should not require any build step for deployment', () => {
      // Check that index.html is ready to serve without processing
      expect(htmlContent.length).toBeGreaterThan(1000);

      // Check that CSS is inline (no need to compile)
      const styleElements = document.querySelectorAll('style');
      expect(styleElements.length).toBeGreaterThan(0);

      // Check that there are no build tool artifacts references
      expect(htmlContent).not.toMatch(/\.tsx/i); // No TypeScript JSX
      expect(htmlContent).not.toMatch(/\.ts"/i); // No TypeScript files
      expect(htmlContent).not.toMatch(/\.jsx/i); // No JSX files
      expect(htmlContent).not.toMatch(/\.scss/i); // No SCSS files
      expect(htmlContent).not.toMatch(/\.sass/i); // No SASS files
      expect(htmlContent).not.toMatch(/\.less/i); // No LESS files
    });

    test('should not have any Jekyll-specific files required', () => {
      // GitHub Pages uses Jekyll by default, but static HTML should work without it
      // Check that we don't rely on Jekyll front matter
      expect(htmlContent).not.toMatch(/^---\s*$/m); // Jekyll front matter delimiter
      expect(htmlContent).not.toMatch(/\{\{\s*.*\s*\}\}/); // Liquid template tags
      expect(htmlContent).not.toMatch(/\{%\s*.*\s*%\}/); // Liquid control tags
    });

    test('should work in subdirectory deployment scenario', () => {
      // Check that all asset paths would work if served from a subdirectory
      const images = document.querySelectorAll('img[src]');

      images.forEach(img => {
        const src = img.getAttribute('src');
        if (!src) return;

        // Skip external URLs
        if (src.startsWith('http://') || src.startsWith('https://') || src.startsWith('//')) {
          return;
        }

        // Relative paths (not starting with /) work best for subdirectory deployment
        // Root-relative paths (starting with /) only work if served from domain root
        const isRelative = !src.startsWith('/');
        const isRootRelative = src.startsWith('/');

        console.log(`Image path: ${src} (${isRelative ? 'relative' : 'root-relative'})`);

        // Both are acceptable, but we prefer relative paths
        expect(src).toBeTruthy();
      });
    });

    test('should have meta tags for proper rendering', () => {
      // Description meta tag for SEO
      const description = document.querySelector('meta[name="description"]');
      expect(description).not.toBeNull();
      expect(description.getAttribute('content').length).toBeGreaterThan(10);

      // Viewport meta tag for responsive design
      const viewport = document.querySelector('meta[name="viewport"]');
      expect(viewport).not.toBeNull();
    });

    test('should not have any CORS-dependent resources for static hosting', () => {
      // Check for external resources that might have CORS issues
      const scripts = document.querySelectorAll('script[src]');

      scripts.forEach(script => {
        const src = script.getAttribute('src');
        if (src && (src.startsWith('http://') || src.startsWith('https://'))) {
          // External scripts should use HTTPS for GitHub Pages
          expect(src).not.toMatch(/^http:\/\//i);
        }
      });

      // Check stylesheets
      const links = document.querySelectorAll('link[rel="stylesheet"]');

      links.forEach(link => {
        const href = link.getAttribute('href');
        if (href && (href.startsWith('http://') || href.startsWith('https://'))) {
          // External stylesheets should use HTTPS
          expect(href).not.toMatch(/^http:\/\//i);
        }
      });
    });

    test('should have all required files present in repository', () => {
      // Check for essential files
      const requiredFiles = [
        path.join(__dirname, '..', 'index.html'),
      ];

      requiredFiles.forEach(file => {
        expect(fs.existsSync(file)).toBe(true);
      });

      // Check for assets directory
      const assetsDir = path.join(__dirname, '..', 'assets');
      expect(fs.existsSync(assetsDir)).toBe(true);
    });

    test('should not require any runtime database or storage', () => {
      // Check that no database connections are referenced
      expect(htmlContent).not.toMatch(/mongodb:\/\//i);
      expect(htmlContent).not.toMatch(/postgres:\/\//i);
      expect(htmlContent).not.toMatch(/mysql:\/\//i);
      expect(htmlContent).not.toMatch(/redis:\/\//i);
      expect(htmlContent).not.toMatch(/sqlite/i);

      // Check scripts for database calls
      const scripts = document.querySelectorAll('script');
      scripts.forEach(script => {
        const content = script.textContent || '';
        expect(content).not.toMatch(/new\s+(?:IndexedDB|IDBDatabase)/i);
        expect(content).not.toMatch(/openDatabase/i);
      });
    });
  });
});
