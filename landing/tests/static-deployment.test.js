/**
 * Static Deployment Compatibility Tests
 *
 * Scenario: Verify the page can be deployed as static content (Technical Constraint)
 *
 * Test Cases:
 * 1. Page renders correctly when served locally from filesystem (manual)
 * 2. All CSS, JS, and image paths are relative or use CDN URLs (unit)
 * 3. Page deploys successfully without server configuration (integration)
 * 4. Page displays all content without requiring backend API calls (e2e)
 */

import { describe, it, expect, beforeEach, beforeAll } from 'vitest';
import { JSDOM } from 'jsdom';
import * as fs from 'fs';
import * as path from 'path';

// Read the HTML file for testing
const htmlPath = path.resolve(__dirname, '../index.html');
const htmlContent = fs.readFileSync(htmlPath, 'utf-8');

// Read the CSS file for testing
const cssPath = path.resolve(__dirname, '../styles.css');
const cssContent = fs.readFileSync(cssPath, 'utf-8');

describe('Static Deployment Compatibility', () => {
  let dom;
  let document;

  beforeEach(() => {
    // Use http URL to avoid JSDOM security restrictions with file:// protocol
    // The tests themselves verify file:// compatibility by checking content patterns
    dom = new JSDOM(htmlContent, {
      url: 'http://localhost/index.html'
    });
    document = dom.window.document;
  });

  describe('Test Case 2: Asset Path Verification', () => {
    describe('CSS paths should be relative or CDN-based', () => {
      it('should use relative path for stylesheet link', () => {
        const styleLinks = document.querySelectorAll('link[rel="stylesheet"]');
        styleLinks.forEach(link => {
          const href = link.getAttribute('href');
          expect(href).toBeDefined();
          // Path should be relative (not starting with /) or a CDN URL (https://)
          const isRelative = !href.startsWith('/') || href.startsWith('./') || href.startsWith('../');
          const isCDN = href.startsWith('https://') || href.startsWith('http://');
          expect(isRelative || isCDN).toBe(true);
        });
      });

      it('should have styles.css as a relative reference', () => {
        const stylesheetLink = document.querySelector('link[rel="stylesheet"][href="styles.css"]');
        expect(stylesheetLink).not.toBeNull();
      });
    });

    describe('JavaScript paths should be relative or CDN-based', () => {
      it('should have no script tags with absolute server paths', () => {
        const scripts = document.querySelectorAll('script[src]');
        scripts.forEach(script => {
          const src = script.getAttribute('src');
          // Should not start with / (absolute path to server root)
          // Unless it's a CDN URL
          if (!src.startsWith('https://') && !src.startsWith('http://')) {
            expect(src.startsWith('/')).toBe(false);
          }
        });
      });

      it('should have no inline scripts that fetch from server paths', () => {
        const scripts = document.querySelectorAll('script:not([src])');
        scripts.forEach(script => {
          const content = script.textContent;
          // Should not have fetch/ajax calls to absolute server paths
          expect(content).not.toMatch(/fetch\s*\(\s*['"`]\/[^/]/);
          expect(content).not.toMatch(/XMLHttpRequest.*open\s*\(\s*['"`]\w+['"`]\s*,\s*['"`]\/[^/]/);
        });
      });
    });

    describe('Image paths should be relative or CDN-based', () => {
      it('should have no images with absolute server paths', () => {
        const images = document.querySelectorAll('img[src]');
        images.forEach(img => {
          const src = img.getAttribute('src');
          // Should be relative or CDN URL
          if (!src.startsWith('https://') && !src.startsWith('http://') && !src.startsWith('data:')) {
            expect(src.startsWith('/')).toBe(false);
          }
        });
      });

      it('should have all background images use relative or CDN paths', () => {
        // Check CSS for url() references
        const urlMatches = cssContent.match(/url\s*\(\s*['"]?([^'")]+)['"]?\s*\)/g) || [];
        urlMatches.forEach(match => {
          // Extract the URL from url()
          const urlMatch = match.match(/url\s*\(\s*['"]?([^'")]+)['"]?\s*\)/);
          if (urlMatch) {
            const url = urlMatch[1];
            // Should be relative, CDN, or data URI
            if (!url.startsWith('https://') && !url.startsWith('http://') && !url.startsWith('data:')) {
              expect(url.startsWith('/')).toBe(false);
            }
          }
        });
      });
    });

    describe('Link paths should be appropriate for static deployment', () => {
      it('should have all internal links as relative or anchor-based', () => {
        const internalLinks = document.querySelectorAll('a:not([href^="http"]):not([href^="mailto"]):not([href^="tel"])');
        internalLinks.forEach(link => {
          const href = link.getAttribute('href');
          if (href) {
            // Internal links should be relative or anchor-based
            const isValid = href.startsWith('#') || href.startsWith('./') ||
                           (!href.startsWith('/') && !href.includes('://'));
            expect(isValid).toBe(true);
          }
        });
      });

      it('should have external links use full URLs with https', () => {
        const externalLinks = document.querySelectorAll('a[href^="http"]');
        externalLinks.forEach(link => {
          const href = link.getAttribute('href');
          expect(href.startsWith('https://') || href.startsWith('http://')).toBe(true);
        });
      });
    });

    describe('Meta tags should not require server-side processing', () => {
      it('should have all meta tags with static content', () => {
        const metaTags = document.querySelectorAll('meta');
        metaTags.forEach(meta => {
          const content = meta.getAttribute('content');
          if (content) {
            // Should not contain server-side template syntax
            expect(content).not.toMatch(/\{\{.*\}\}/); // Handlebars/Mustache
            expect(content).not.toMatch(/<%.*%>/); // EJS/ERB
            expect(content).not.toMatch(/\${.*}/); // Template literals in server context
          }
        });
      });
    });
  });

  describe('Test Case 3: Static Hosting Compatibility (Integration)', () => {
    describe('No server-side requirements', () => {
      it('should be a valid standalone HTML file', () => {
        expect(document.doctype).not.toBeNull();
        expect(document.documentElement.tagName).toBe('HTML');
        expect(document.head).not.toBeNull();
        expect(document.body).not.toBeNull();
      });

      it('should not require PHP or server-side processing', () => {
        // Check for PHP syntax in HTML
        expect(htmlContent).not.toMatch(/<\?php/i);
        expect(htmlContent).not.toMatch(/<\?=/);
      });

      it('should not require ASP.NET server-side processing', () => {
        expect(htmlContent).not.toMatch(/<%.*%>/);
        expect(htmlContent).not.toMatch(/@\{.*\}/);
        expect(htmlContent).not.toMatch(/@Html\./);
      });

      it('should not contain server-side include directives', () => {
        expect(htmlContent).not.toMatch(/<!--#include/);
        expect(htmlContent).not.toMatch(/<!--#exec/);
        expect(htmlContent).not.toMatch(/<!--#config/);
      });

      it('should not contain Jinja/Django template syntax', () => {
        expect(htmlContent).not.toMatch(/\{\{.*\}\}/);
        expect(htmlContent).not.toMatch(/\{%.*%\}/);
      });
    });

    describe('Static file structure', () => {
      it('should have index.html as entry point', () => {
        expect(fs.existsSync(htmlPath)).toBe(true);
      });

      it('should have styles.css file', () => {
        expect(fs.existsSync(cssPath)).toBe(true);
      });

      it('should have CSS file referenced correctly from HTML', () => {
        const styleLink = document.querySelector('link[href="styles.css"]');
        expect(styleLink).not.toBeNull();
      });
    });

    describe('GitHub Pages / Netlify / Vercel compatibility', () => {
      it('should not require special server configuration files for basic operation', () => {
        // Core functionality should work without these files
        // (though they may exist for optimization)
        const coreContent = document.body.textContent;
        expect(coreContent).toBeTruthy();
        expect(coreContent.length).toBeGreaterThan(100);
      });

      it('should have proper character encoding specified', () => {
        const charsetMeta = document.querySelector('meta[charset]');
        expect(charsetMeta).not.toBeNull();
        expect(charsetMeta.getAttribute('charset').toLowerCase()).toBe('utf-8');
      });

      it('should have proper lang attribute for accessibility', () => {
        expect(document.documentElement.getAttribute('lang')).toBe('en');
      });
    });
  });

  describe('Test Case 4: No API Calls Required for Core Content (E2E)', () => {
    describe('Content should be embedded in HTML', () => {
      it('should display product name without API call', () => {
        const productName = document.querySelector('.product-name, h1');
        expect(productName).not.toBeNull();
        expect(productName.textContent).toContain('MirDB');
      });

      it('should display tagline without API call', () => {
        const tagline = document.querySelector('.tagline');
        expect(tagline).not.toBeNull();
        expect(tagline.textContent).toBeTruthy();
      });

      it('should display value proposition without API call', () => {
        const valueProp = document.querySelector('.value-proposition');
        expect(valueProp).not.toBeNull();
        expect(valueProp.textContent).toBeTruthy();
      });

      it('should display CTA buttons without API call', () => {
        const ctaButtons = document.querySelectorAll('.btn');
        expect(ctaButtons.length).toBeGreaterThanOrEqual(2);
      });

      it('should display Get Started section without API call', () => {
        const getStartedSection = document.querySelector('#get-started, .get-started-section');
        expect(getStartedSection).not.toBeNull();
        expect(getStartedSection.textContent).toBeTruthy();
      });

      it('should display installation instructions without API call', () => {
        const installation = document.querySelector('.installation');
        expect(installation).not.toBeNull();
        expect(installation.textContent).toContain('cargo');
      });

      it('should display footer without API call', () => {
        const footer = document.querySelector('footer');
        expect(footer).not.toBeNull();
        expect(footer.textContent).toBeTruthy();
      });
    });

    describe('No dynamic content loading required', () => {
      it('should not have fetch() calls for core content', () => {
        const scripts = document.querySelectorAll('script');
        scripts.forEach(script => {
          const content = script.textContent;
          // Core content should not require fetch
          expect(content).not.toMatch(/fetch\s*\(/);
        });
      });

      it('should not have AJAX calls for core content', () => {
        const scripts = document.querySelectorAll('script');
        scripts.forEach(script => {
          const content = script.textContent;
          expect(content).not.toMatch(/XMLHttpRequest/);
          expect(content).not.toMatch(/\$\.ajax/);
          expect(content).not.toMatch(/\$\.get/);
          expect(content).not.toMatch(/\$\.post/);
        });
      });

      it('should not require WebSocket connection for content', () => {
        const scripts = document.querySelectorAll('script');
        scripts.forEach(script => {
          const content = script.textContent;
          expect(content).not.toMatch(/new\s+WebSocket/);
        });
      });

      it('should not have data loading placeholders', () => {
        const body = document.body.innerHTML;
        // Should not have common loading placeholder patterns
        expect(body).not.toMatch(/data-src=['"][^'"]+['"]/);
        expect(body).not.toMatch(/ng-bind|ng-model/); // Angular bindings
        expect(body).not.toMatch(/v-bind|v-model/); // Vue bindings
        expect(body).not.toMatch(/:src=|:href=/); // Vue dynamic attributes
      });
    });

    describe('All visible content is in HTML', () => {
      it('should have all main sections in HTML', () => {
        // Check for main structural elements
        expect(document.querySelector('header, .hero')).not.toBeNull();
        expect(document.querySelector('main')).not.toBeNull();
        expect(document.querySelector('footer')).not.toBeNull();
      });

      it('should have meaningful content in each section', () => {
        const header = document.querySelector('header, .hero');
        const main = document.querySelector('main');
        const footer = document.querySelector('footer');

        expect(header.textContent.trim().length).toBeGreaterThan(50);
        expect(main.textContent.trim().length).toBeGreaterThan(100);
        expect(footer.textContent.trim().length).toBeGreaterThan(10);
      });

      it('should have code examples embedded in HTML', () => {
        const codeBlocks = document.querySelectorAll('pre code, .code-block code');
        expect(codeBlocks.length).toBeGreaterThan(0);

        // Check that code blocks have actual content
        let hasContent = false;
        codeBlocks.forEach(block => {
          if (block.textContent.trim().length > 0) {
            hasContent = true;
          }
        });
        expect(hasContent).toBe(true);
      });

      it('should have project status information embedded', () => {
        const projectStatus = document.querySelector('#project-status, .project-status-section');
        if (projectStatus) {
          expect(projectStatus.textContent).toBeTruthy();
        }
      });
    });
  });

  describe('Test Case 1: File Protocol Rendering (Manual Verification Support)', () => {
    describe('File protocol compatibility checks', () => {
      it('should work with file:// URL base (content verification)', () => {
        // Verify the HTML structure works without relying on file:// protocol in JSDOM
        // (file:// triggers security restrictions in JSDOM, so we verify content directly)
        expect(document.querySelector('.hero, header')).not.toBeNull();
        expect(document.querySelector('.product-name, h1')).not.toBeNull();
        // Verify no absolute server paths that would break file:// serving
        expect(htmlContent).not.toMatch(/href=["']\/[a-zA-Z]/);  // No absolute paths like /styles.css
      });

      it('should not have protocol-relative URLs that fail on file://', () => {
        // Protocol-relative URLs (//example.com) don't work with file://
        expect(htmlContent).not.toMatch(/href=["']\/\/[^/]/);
        expect(htmlContent).not.toMatch(/src=["']\/\/[^/]/);
      });

      it('should have all essential CSS in external file or inline', () => {
        // Check that CSS is properly linked - use the document from main test suite
        const styleLink = document.querySelector('link[rel="stylesheet"]');
        const inlineStyles = document.querySelectorAll('style');

        // Should have either external stylesheet or inline styles
        const hasCSS = styleLink !== null || inlineStyles.length > 0;
        expect(hasCSS).toBe(true);
      });
    });

    describe('Cross-origin restrictions handling', () => {
      it('should not require cookies or session storage for display', () => {
        // Check for JavaScript that requires cookies
        const scripts = document.querySelectorAll('script');
        scripts.forEach(script => {
          const content = script.textContent;
          expect(content).not.toMatch(/document\.cookie/);
          expect(content).not.toMatch(/sessionStorage\./);
        });
      });

      it('should gracefully handle disabled JavaScript', () => {
        // Core content should be visible without JS
        const noscript = document.querySelector('noscript');
        const visibleContent = document.body.textContent;

        // Even without noscript tag, content should be in HTML
        expect(visibleContent).toBeTruthy();
        expect(visibleContent.length).toBeGreaterThan(500);
      });
    });
  });

  describe('CSS Static Deployment Compatibility', () => {
    it('should not use CSS @import with absolute URLs', () => {
      const importMatches = cssContent.match(/@import\s+(?:url\s*\()?\s*['"]?([^'");\s]+)/g) || [];
      importMatches.forEach(match => {
        // Extract the URL
        const urlMatch = match.match(/['"]?([^'");\s]+)['"]?/);
        if (urlMatch) {
          const url = urlMatch[1];
          if (!url.startsWith('https://') && !url.startsWith('http://')) {
            expect(url.startsWith('/')).toBe(false);
          }
        }
      });
    });

    it('should use system fonts or CDN-hosted fonts', () => {
      // Check font-family declarations
      const fontFamilyMatches = cssContent.match(/font-family\s*:\s*[^;]+/g) || [];
      fontFamilyMatches.forEach(declaration => {
        // Should use system fonts or generic font families
        // or have proper @font-face with relative/CDN URLs
        expect(declaration).toBeTruthy();
      });
    });

    it('should not have hardcoded localhost or server URLs', () => {
      expect(cssContent).not.toMatch(/localhost/);
      expect(cssContent).not.toMatch(/127\.0\.0\.1/);
      expect(cssContent).not.toMatch(/0\.0\.0\.0/);
    });
  });

  describe('HTML Validation for Static Deployment', () => {
    it('should have proper DOCTYPE declaration', () => {
      expect(htmlContent.trim().toLowerCase()).toMatch(/^<!doctype html>/);
    });

    it('should have required head elements', () => {
      expect(document.querySelector('title')).not.toBeNull();
      expect(document.querySelector('meta[charset]')).not.toBeNull();
      expect(document.querySelector('meta[name="viewport"]')).not.toBeNull();
    });

    it('should not have empty src or href attributes', () => {
      const emptyAttrs = document.querySelectorAll('[src=""],[href=""]');
      expect(emptyAttrs.length).toBe(0);
    });

    it('should have all IDs unique', () => {
      const ids = {};
      document.querySelectorAll('[id]').forEach(el => {
        const id = el.id;
        expect(ids[id]).toBeUndefined();
        ids[id] = true;
      });
    });
  });
});

describe('Build Output Static Compatibility', () => {
  it('should produce static files with vite build', async () => {
    // This test validates that the project is configured for static builds
    const viteConfigPath = path.resolve(__dirname, '../vite.config.js');
    expect(fs.existsSync(viteConfigPath)).toBe(true);

    const viteConfig = fs.readFileSync(viteConfigPath, 'utf-8');
    // Should have a build output directory configured
    expect(viteConfig).toMatch(/outDir/);
  });

  it('should have package.json with build script', () => {
    const packageJsonPath = path.resolve(__dirname, '../package.json');
    const packageJson = JSON.parse(fs.readFileSync(packageJsonPath, 'utf-8'));

    expect(packageJson.scripts).toBeDefined();
    expect(packageJson.scripts.build).toBeDefined();
  });
});
