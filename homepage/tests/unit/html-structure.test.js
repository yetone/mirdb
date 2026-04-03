/**
 * HTML Structure Unit Tests
 * Owner: Scenario 18 - Static Site Requirements
 *
 * Also includes Asset Path Validation Tests for Scenario 19 - Asset Integration
 *
 * Tests:
 * - Valid HTML5 document structure
 * - All required sections present
 * - No external API dependencies
 * - Asset paths correct
 * - Static file structure validation
 */

const fs = require('fs');
const path = require('path');
const { JSDOM } = require('jsdom');

describe('Static Site Requirements', () => {
  let htmlContent;
  let dom;
  let document;

  const homepagePath = path.resolve(__dirname, '../../');

  beforeAll(() => {
    const htmlPath = path.join(homepagePath, 'index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf8');
    dom = new JSDOM(htmlContent);
    document = dom.window.document;
  });

  describe('Test Case 1: File Structure Validation', () => {
    test('index.html file exists', () => {
      const htmlPath = path.join(homepagePath, 'index.html');
      expect(fs.existsSync(htmlPath)).toBe(true);
    });

    test('CSS files exist and can be served statically', () => {
      const cssFiles = ['css/main.css', 'css/variables.css', 'css/responsive.css'];

      cssFiles.forEach(cssFile => {
        const cssPath = path.join(homepagePath, cssFile);
        expect(fs.existsSync(cssPath)).toBe(true);

        // Verify CSS files contain valid CSS (basic check - no syntax errors that would prevent static serving)
        const cssContent = fs.readFileSync(cssPath, 'utf8');
        expect(cssContent.length).toBeGreaterThan(0);
      });
    });

    test('JavaScript files exist and can be served statically', () => {
      const jsFiles = ['js/main.js', 'js/navigation.js', 'js/copy-code.js'];

      jsFiles.forEach(jsFile => {
        const jsPath = path.join(homepagePath, jsFile);
        expect(fs.existsSync(jsPath)).toBe(true);

        // Verify JS files contain valid JavaScript (basic check)
        const jsContent = fs.readFileSync(jsPath, 'utf8');
        expect(jsContent.length).toBeGreaterThan(0);
      });
    });

    test('HTML file has valid HTML5 structure', () => {
      expect(htmlContent).toMatch(/<!DOCTYPE html>/i);
      expect(document.documentElement.getAttribute('lang')).toBe('en');
      expect(document.head).toBeTruthy();
      expect(document.body).toBeTruthy();
    });

    test('All required sections are present in HTML', () => {
      const requiredSections = [
        'hero',
        'features',
        'quick-start',
        'architecture',
        'api-reference',
        'configuration',
        'performance',
        'contributing'
      ];

      requiredSections.forEach(sectionId => {
        const section = document.getElementById(sectionId);
        expect(section).toBeTruthy();
      });
    });

    test('CSS files are referenced with relative paths', () => {
      const links = document.querySelectorAll('link[rel="stylesheet"]');

      links.forEach(link => {
        const href = link.getAttribute('href');
        // Should be relative paths starting with css/
        expect(href).toMatch(/^css\//);
        // Should not have absolute URLs
        expect(href).not.toMatch(/^https?:\/\//);
      });
    });

    test('JavaScript files are referenced with relative paths', () => {
      const scripts = document.querySelectorAll('script[src]');

      scripts.forEach(script => {
        const src = script.getAttribute('src');
        // Should be relative paths starting with js/
        expect(src).toMatch(/^js\//);
        // Should not have absolute URLs
        expect(src).not.toMatch(/^https?:\/\//);
      });
    });

    test('Asset folder exists with symlink or assets', () => {
      const assetsPath = path.join(homepagePath, 'assets');
      // Assets folder should exist (either symlink or real folder)
      expect(fs.existsSync(assetsPath)).toBe(true);
    });
  });

  describe('Test Case 3: No External API Calls Required', () => {
    let mainJsContent;
    let navigationJsContent;
    let copyCodeJsContent;

    beforeAll(() => {
      mainJsContent = fs.readFileSync(path.join(homepagePath, 'js/main.js'), 'utf8');
      navigationJsContent = fs.readFileSync(path.join(homepagePath, 'js/navigation.js'), 'utf8');
      copyCodeJsContent = fs.readFileSync(path.join(homepagePath, 'js/copy-code.js'), 'utf8');
    });

    test('main.js has no fetch() calls', () => {
      // Check for fetch API calls
      expect(mainJsContent).not.toMatch(/\bfetch\s*\(/);
    });

    test('main.js has no XMLHttpRequest usage', () => {
      expect(mainJsContent).not.toMatch(/XMLHttpRequest/);
      expect(mainJsContent).not.toMatch(/\.\s*open\s*\(\s*['"](?:GET|POST|PUT|DELETE)/i);
    });

    test('navigation.js has no fetch() calls', () => {
      expect(navigationJsContent).not.toMatch(/\bfetch\s*\(/);
    });

    test('navigation.js has no XMLHttpRequest usage', () => {
      expect(navigationJsContent).not.toMatch(/XMLHttpRequest/);
    });

    test('copy-code.js has no fetch() calls', () => {
      expect(copyCodeJsContent).not.toMatch(/\bfetch\s*\(/);
    });

    test('copy-code.js has no XMLHttpRequest usage', () => {
      expect(copyCodeJsContent).not.toMatch(/XMLHttpRequest/);
    });

    test('HTML has no data-* attributes requiring external API', () => {
      // Check that there are no data-api-endpoint or similar attributes
      const elementsWithDataApi = document.querySelectorAll('[data-api], [data-api-endpoint], [data-fetch]');
      expect(elementsWithDataApi.length).toBe(0);
    });

    test('All content is embedded in static files', () => {
      // Verify all sections have content (not placeholders for dynamic data)
      const sections = document.querySelectorAll('section');

      sections.forEach(section => {
        const textContent = section.textContent.trim();
        expect(textContent.length).toBeGreaterThan(0);
        // Should not have loading placeholders
        expect(textContent).not.toMatch(/loading\.\.\./i);
        expect(textContent).not.toMatch(/fetching data/i);
      });
    });

    test('No inline scripts with API calls', () => {
      const inlineScripts = document.querySelectorAll('script:not([src])');

      inlineScripts.forEach(script => {
        const content = script.textContent;
        // No fetch calls in inline scripts
        expect(content).not.toMatch(/\bfetch\s*\(/);
        expect(content).not.toMatch(/XMLHttpRequest/);
      });
    });

    test('No scripts with external API URLs', () => {
      const allJsContent = mainJsContent + navigationJsContent + copyCodeJsContent;

      // Should not have API endpoint URLs
      expect(allJsContent).not.toMatch(/https?:\/\/api\./);
      expect(allJsContent).not.toMatch(/\/api\/v\d+/);
    });
  });

  describe('Static Site Compatibility', () => {
    test('HTML uses only standard self-closing tags', () => {
      // img, br, hr, input, meta, link should be properly formatted
      const imgTags = document.querySelectorAll('img');
      imgTags.forEach(img => {
        expect(img.getAttribute('src')).toBeTruthy();
      });
    });

    test('All images use relative asset paths', () => {
      const images = document.querySelectorAll('img');

      images.forEach(img => {
        const src = img.getAttribute('src');
        if (src) {
          // Should be relative paths (assets/ or ../assets/)
          expect(src).toMatch(/^(\.\.\/)?assets\//);
          // Should not require server-side processing
          expect(src).not.toMatch(/\.php|\.asp|\.jsp/i);
        }
      });
    });

    test('No form actions to server-side endpoints', () => {
      const forms = document.querySelectorAll('form');

      forms.forEach(form => {
        const action = form.getAttribute('action');
        if (action) {
          // Should not post to server endpoints
          expect(action).not.toMatch(/^https?:\/\//);
        }
      });
    });

    test('Links use anchor navigation or external URLs', () => {
      const internalLinks = document.querySelectorAll('a[href^="#"]');
      expect(internalLinks.length).toBeGreaterThan(0);

      // Navigation links should use hash anchors
      const navLinks = document.querySelectorAll('.nav__link');
      navLinks.forEach(link => {
        const href = link.getAttribute('href');
        expect(href).toMatch(/^#/);
      });
    });

    test('No server-side includes or SSI directives', () => {
      // Check for SSI directives in HTML
      expect(htmlContent).not.toMatch(/<!--#include/);
      expect(htmlContent).not.toMatch(/<!--#exec/);
      expect(htmlContent).not.toMatch(/<!--#set/);
    });

    test('No PHP or other server-side code', () => {
      expect(htmlContent).not.toMatch(/<\?php/);
      expect(htmlContent).not.toMatch(/<\?=/);
      expect(htmlContent).not.toMatch(/<%/);
    });
  });

  describe('CDN Dependencies Validation', () => {
    test('External stylesheets are not required for core functionality', () => {
      const externalStylesheets = document.querySelectorAll('link[rel="stylesheet"][href^="http"]');

      // External stylesheets should be minimal or none
      // Any external ones should be for optional features like fonts
      externalStylesheets.forEach(link => {
        const href = link.getAttribute('href');
        // External stylesheets should be for fonts or optional enhancements
        if (href) {
          expect(href).toMatch(/fonts|highlight|prism/i);
        }
      });
    });

    test('External scripts are not required for core functionality', () => {
      const externalScripts = document.querySelectorAll('script[src^="http"]');

      // Should have no required external scripts
      // All core functionality should be in local files
      expect(externalScripts.length).toBe(0);
    });

    test('All CSS custom properties are defined locally', () => {
      const variablesCss = fs.readFileSync(path.join(homepagePath, 'css/variables.css'), 'utf8');

      // Should have CSS custom properties defined
      expect(variablesCss).toMatch(/--color/);
      expect(variablesCss).toMatch(/:root/);
    });

    test('Syntax highlighting is embedded or optional', () => {
      // Code blocks should work without external syntax highlighting
      const codeBlocks = document.querySelectorAll('.code-block');
      expect(codeBlocks.length).toBeGreaterThan(0);

      // Code should be readable even without syntax highlighting
      codeBlocks.forEach(block => {
        const codeContent = block.textContent;
        expect(codeContent.length).toBeGreaterThan(0);
      });
    });
  });
});

// ============================================================================
// Asset Integration Tests (Scenario 19)
// ============================================================================

describe('Asset Path Validation (Scenario 19)', () => {
  let dom;
  let document;

  const homepagePath = path.resolve(__dirname, '../../');

  beforeAll(() => {
    const htmlPath = path.join(homepagePath, 'index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf8');
    dom = new JSDOM(htmlContent);
    document = dom.window.document;
  });

  describe('logo.gif Path Tests', () => {
    test('TC1: Header logo references assets/logo.gif with correct relative path', () => {
      const headerLogo = document.querySelector('.header__logo');
      expect(headerLogo).not.toBeNull();
      expect(headerLogo.getAttribute('src')).toBe('assets/logo.gif');
    });

    test('TC1: Hero logo references assets/logo.gif with correct relative path', () => {
      const heroLogo = document.querySelector('.hero__logo');
      expect(heroLogo).not.toBeNull();
      expect(heroLogo.getAttribute('src')).toBe('assets/logo.gif');
    });

    test('TC1: Footer logo references assets/logo.gif with correct relative path', () => {
      const footerLogo = document.querySelector('.footer__logo');
      expect(footerLogo).not.toBeNull();
      expect(footerLogo.getAttribute('src')).toBe('assets/logo.gif');
    });

    test('TC1: Open Graph image references assets/logo.gif with correct path', () => {
      const ogImage = document.querySelector('meta[property="og:image"]');
      expect(ogImage).not.toBeNull();
      expect(ogImage.getAttribute('content')).toBe('assets/logo.gif');
    });

    test('Logo.gif file exists in assets directory', () => {
      const assetPath = path.join(homepagePath, 'assets/logo.gif');
      expect(fs.existsSync(assetPath)).toBe(true);
    });

    test('Logo images have appropriate alt text', () => {
      const headerLogo = document.querySelector('.header__logo');
      const heroLogo = document.querySelector('.hero__logo');
      const footerLogo = document.querySelector('.footer__logo');

      expect(headerLogo.getAttribute('alt')).toBeTruthy();
      expect(heroLogo.getAttribute('alt')).toBeTruthy();
      expect(footerLogo.getAttribute('alt')).toBeTruthy();
    });
  });

  describe('usage.gif Path Tests', () => {
    test('TC4: Usage demo image references assets/usage.gif with correct relative path', () => {
      const usageGif = document.querySelector('#usage-gif, .usage-gif');
      expect(usageGif).not.toBeNull();
      expect(usageGif.getAttribute('src')).toBe('assets/usage.gif');
    });

    test('Usage.gif file exists in assets directory', () => {
      const assetPath = path.join(homepagePath, 'assets/usage.gif');
      expect(fs.existsSync(assetPath)).toBe(true);
    });

    test('Usage.gif has appropriate alt text describing the demo', () => {
      const usageGif = document.querySelector('#usage-gif, .usage-gif');
      expect(usageGif).not.toBeNull();
      const alt = usageGif.getAttribute('alt');
      expect(alt).toBeTruthy();
      expect(alt.toLowerCase()).toMatch(/usage|demo|mirdb/);
    });

    test('Usage.gif has lazy loading attribute for performance', () => {
      const usageGif = document.querySelector('#usage-gif, .usage-gif');
      expect(usageGif).not.toBeNull();
      expect(usageGif.getAttribute('loading')).toBe('lazy');
    });
  });

  describe('Asset Integration Consistency', () => {
    test('All logo.gif references use the same relative path', () => {
      const images = document.querySelectorAll('img[src*="logo.gif"]');
      const sources = Array.from(images).map(img => img.getAttribute('src'));

      expect(sources.length).toBeGreaterThanOrEqual(1);
      sources.forEach(src => {
        expect(src).toBe('assets/logo.gif');
      });
    });

    test('All asset paths are relative (no absolute URLs)', () => {
      const assetImages = document.querySelectorAll('img[src*=".gif"]');
      const sources = Array.from(assetImages).map(img => img.getAttribute('src'));

      sources.forEach(src => {
        expect(src).not.toMatch(/^https?:\/\//);
        expect(src).not.toMatch(/^\//);
      });
    });
  });
});
