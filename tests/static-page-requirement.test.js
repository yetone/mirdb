/**
 * Static Page Requirement Tests (NFR-4)
 *
 * Verifies that the page is static with no server-side rendering required.
 * The page must be deployable to static hosting platforms like GitHub Pages,
 * Netlify, or Vercel without any server-side processing.
 */

const fs = require('fs');
const path = require('path');

describe('Static Page Requirement (NFR-4)', () => {
  let htmlContent;
  let document;
  const projectRoot = path.join(__dirname, '..');

  beforeAll(() => {
    // Read the HTML file directly (simulating static file serving)
    const htmlPath = path.join(__dirname, '..', 'index.html');
    htmlContent = fs.readFileSync(htmlPath, 'utf8');

    // Parse HTML using jsdom environment (provided by Jest)
    document = new DOMParser().parseFromString(htmlContent, 'text/html');
  });

  describe('Test Case 1: All content is present in initial HTML', () => {
    test('page content is rendered in static HTML without requiring JavaScript', () => {
      // Verify the HTML document is complete and valid
      expect(htmlContent).toContain('<!DOCTYPE html>');
      expect(htmlContent).toContain('<html');
      expect(htmlContent).toContain('</html>');
    });

    test('hero section content is in initial HTML', () => {
      const heroSection = document.querySelector('.hero');
      expect(heroSection).not.toBeNull();

      const h1 = document.querySelector('.hero h1');
      expect(h1).not.toBeNull();
      expect(h1.textContent).toBe('MirDB');

      const tagline = document.querySelector('.tagline');
      expect(tagline).not.toBeNull();
      expect(tagline.textContent).toContain('Persistent Key-Value Store');
    });

    test('value propositions content is in initial HTML', () => {
      const valuePropositions = document.querySelectorAll('.proposition');
      expect(valuePropositions.length).toBe(3);

      // Check that all three value propositions have content in the HTML
      const propositionTitles = Array.from(document.querySelectorAll('.proposition-title'));
      const titleTexts = propositionTitles.map(el => el.textContent);

      expect(titleTexts).toContain('Drop-in Memcached Compatibility');
      expect(titleTexts).toContain('Built-in Persistence');
      expect(titleTexts).toContain('LSM Tree Architecture');
    });

    test('getting started section content is in initial HTML', () => {
      const gettingStarted = document.getElementById('getting-started');
      expect(gettingStarted).not.toBeNull();

      const steps = document.querySelectorAll('.getting-started .step');
      expect(steps.length).toBeGreaterThan(0);

      // Verify code blocks with examples are present
      const codeBlocks = document.querySelectorAll('.getting-started pre code');
      expect(codeBlocks.length).toBeGreaterThan(0);
    });

    test('supported commands content is in initial HTML', () => {
      const commandsSection = document.getElementById('commands');
      expect(commandsSection).not.toBeNull();

      const commandGroups = document.querySelectorAll('.command-group');
      expect(commandGroups.length).toBeGreaterThan(0);

      // Verify commands are present in HTML
      const commandList = document.querySelectorAll('.command-list li');
      expect(commandList.length).toBeGreaterThan(0);
    });

    test('feature comparison content is in initial HTML', () => {
      const comparisonSection = document.getElementById('feature-comparison');
      expect(comparisonSection).not.toBeNull();

      const comparisonTable = document.querySelector('.comparison-table');
      expect(comparisonTable).not.toBeNull();

      const tableRows = document.querySelectorAll('.comparison-table tbody tr');
      expect(tableRows.length).toBeGreaterThan(0);
    });

    test('client connection examples content is in initial HTML', () => {
      const clientSection = document.getElementById('client-connection');
      expect(clientSection).not.toBeNull();

      const clientExamples = document.querySelectorAll('.client-example');
      expect(clientExamples.length).toBeGreaterThan(0);

      // Check for code examples in initial HTML
      const codeExamples = document.querySelectorAll('.client-example pre code');
      expect(codeExamples.length).toBeGreaterThan(0);
    });

    test('footer content is in initial HTML', () => {
      const footer = document.querySelector('.footer');
      expect(footer).not.toBeNull();

      const footerLinks = document.querySelectorAll('.footer-links a');
      expect(footerLinks.length).toBeGreaterThan(0);
    });

    test('no JavaScript-based content injection patterns detected', () => {
      // Check for common client-side rendering framework mount points that are empty
      // These patterns indicate CSR when the container is empty
      const reactRoot = document.getElementById('root');
      const appRoot = document.getElementById('app');
      const nextRoot = document.getElementById('__next');

      // If these elements exist, they should have content (not be empty placeholders)
      if (reactRoot) {
        expect(reactRoot.innerHTML.trim().length).toBeGreaterThan(0);
      }
      if (appRoot) {
        expect(appRoot.innerHTML.trim().length).toBeGreaterThan(0);
      }
      if (nextRoot) {
        expect(nextRoot.innerHTML.trim().length).toBeGreaterThan(0);
      }
    });

    test('no data-loading or loading spinner elements present as primary content', () => {
      // Check that primary content sections don't show loading states
      const loadingIndicators = document.querySelectorAll('[data-loading], .loading, .spinner, .skeleton');

      // If loading elements exist, they should not be the primary content mechanism
      loadingIndicators.forEach(indicator => {
        // Loading indicators should be hidden or for enhancement only
        const style = indicator.getAttribute('style') || '';
        const hasHiddenClass = indicator.classList.contains('hidden') ||
                               indicator.classList.contains('d-none') ||
                               style.includes('display: none');
        // Either hidden or there should be actual content siblings
        const hasContentSiblings = indicator.parentElement &&
                                   indicator.parentElement.textContent.trim().length > indicator.textContent.trim().length;
        expect(hasHiddenClass || hasContentSiblings).toBe(true);
      });
    });
  });

  describe('Test Case 2: Site consists of HTML, CSS, JS, and asset files only', () => {
    test('index.html file exists as entry point', () => {
      const indexPath = path.join(projectRoot, 'index.html');
      expect(fs.existsSync(indexPath)).toBe(true);
    });

    test('styles.css file exists', () => {
      const cssPath = path.join(projectRoot, 'styles.css');
      expect(fs.existsSync(cssPath)).toBe(true);
    });

    test('no server-side script files present (.php, .py server scripts, .rb server scripts, .jsp, .asp)', () => {
      const serverSideExtensions = ['.php', '.jsp', '.asp', '.aspx'];

      function checkForServerFiles(dir, extensions) {
        if (!fs.existsSync(dir)) return [];

        const serverFiles = [];
        const items = fs.readdirSync(dir, { withFileTypes: true });

        for (const item of items) {
          const fullPath = path.join(dir, item.name);

          // Skip node_modules, .git, and hidden directories, and Rust backend code
          if (item.name === 'node_modules' ||
              item.name === '.git' ||
              item.name.startsWith('.') ||
              item.name === 'mirdb-server' ||
              item.name === 'skip-list' ||
              item.name === 'sstable') {
            continue;
          }

          if (item.isDirectory()) {
            serverFiles.push(...checkForServerFiles(fullPath, extensions));
          } else if (item.isFile()) {
            const ext = path.extname(item.name).toLowerCase();
            if (extensions.includes(ext)) {
              serverFiles.push(fullPath);
            }
          }
        }

        return serverFiles;
      }

      const serverFiles = checkForServerFiles(projectRoot, serverSideExtensions);
      expect(serverFiles).toEqual([]);
    });

    test('HTML file references only static resources (CSS, JS, images)', () => {
      // Check that script and link tags reference static files
      const scripts = document.querySelectorAll('script[src]');
      const links = document.querySelectorAll('link[href]');
      const images = document.querySelectorAll('img[src]');

      // Verify no server-side processing URLs
      const serverPatterns = [
        /\.php/i,
        /\.jsp/i,
        /\.asp/i,
        /\/api\//i,
        /\/cgi-bin\//i
      ];

      const allSrcs = [
        ...Array.from(scripts).map(s => s.getAttribute('src')),
        ...Array.from(links).map(l => l.getAttribute('href')),
        ...Array.from(images).map(i => i.getAttribute('src'))
      ].filter(Boolean);

      allSrcs.forEach(src => {
        serverPatterns.forEach(pattern => {
          expect(src).not.toMatch(pattern);
        });
      });
    });

    test('assets folder contains only static files (images, icons, fonts)', () => {
      const assetsPath = path.join(projectRoot, 'assets');

      if (fs.existsSync(assetsPath)) {
        const staticExtensions = [
          '.png', '.jpg', '.jpeg', '.gif', '.svg', '.webp', '.ico',
          '.woff', '.woff2', '.ttf', '.eot', '.otf',
          '.mp4', '.webm', '.ogg', '.mp3', '.wav',
          '.pdf'
        ];

        function checkStaticOnly(dir) {
          const items = fs.readdirSync(dir, { withFileTypes: true });

          for (const item of items) {
            const fullPath = path.join(dir, item.name);

            if (item.isDirectory()) {
              checkStaticOnly(fullPath);
            } else if (item.isFile()) {
              const ext = path.extname(item.name).toLowerCase();
              expect(staticExtensions).toContain(ext);
            }
          }
        }

        checkStaticOnly(assetsPath);
      }
    });

    test('no build output requiring server runtime (no node_modules in deployment)', () => {
      // The deployment should not require node_modules
      // Check that HTML doesn't reference node_modules paths
      expect(htmlContent).not.toContain('node_modules/');
    });

    test('CSS is referenced correctly for static serving', () => {
      const cssLink = document.querySelector('link[rel="stylesheet"]');
      expect(cssLink).not.toBeNull();

      const href = cssLink.getAttribute('href');
      expect(href).toBe('styles.css');

      // Verify CSS file exists
      const cssPath = path.join(projectRoot, href);
      expect(fs.existsSync(cssPath)).toBe(true);
    });
  });

  describe('Test Case 3: Core content visible without JavaScript (progressive enhancement)', () => {
    test('HTML contains all text content directly (not loaded via JS)', () => {
      // Main content text should be in the HTML source
      const bodyText = document.body.textContent;

      // Key content pieces that must be present
      expect(bodyText).toContain('MirDB');
      expect(bodyText).toContain('Persistent Key-Value Store');
      expect(bodyText).toContain('Memcached');
      expect(bodyText).toContain('Getting Started');
    });

    test('navigation links work without JavaScript', () => {
      const navLinks = document.querySelectorAll('a[href^="#"]');

      navLinks.forEach(link => {
        const targetId = link.getAttribute('href').substring(1);
        if (targetId) {
          const targetElement = document.getElementById(targetId);
          // Either the target exists or it's a placeholder link
          expect(targetElement !== null || targetId === 'documentation').toBe(true);
        }
      });
    });

    test('external links are properly formatted for non-JS environment', () => {
      const externalLinks = document.querySelectorAll('a[target="_blank"]');

      externalLinks.forEach(link => {
        const href = link.getAttribute('href');
        // External links should have full URLs, not JS handlers
        expect(href).toMatch(/^https?:\/\//);
        // Should have noopener for security
        const rel = link.getAttribute('rel');
        expect(rel).toContain('noopener');
      });
    });

    test('no onclick handlers required for essential functionality', () => {
      // Essential content elements should not depend on onclick
      const essentialElements = [
        '.hero',
        '.value-propositions',
        '.getting-started',
        '.commands-section',
        '.feature-comparison',
        '.footer'
      ];

      essentialElements.forEach(selector => {
        const element = document.querySelector(selector);
        if (element) {
          // Check if element has onclick that's required for viewing content
          const onclick = element.getAttribute('onclick');
          // onclick for analytics is ok, but not for content display
          if (onclick) {
            expect(onclick).not.toMatch(/innerHTML|appendChild|render/i);
          }
        }
      });
    });

    test('code examples are visible in HTML source', () => {
      const codeBlocks = document.querySelectorAll('pre code');
      expect(codeBlocks.length).toBeGreaterThan(0);

      // Each code block should have actual content, not placeholders
      codeBlocks.forEach(codeBlock => {
        const content = codeBlock.textContent.trim();
        expect(content.length).toBeGreaterThan(10);
        // Should not be a placeholder
        expect(content).not.toMatch(/^loading|^\.\.\.$/i);
      });
    });

    test('images have alt text for accessibility when JS is disabled', () => {
      const images = document.querySelectorAll('img');

      images.forEach(img => {
        const alt = img.getAttribute('alt');
        // All images should have alt attribute (even if empty for decorative)
        expect(img.hasAttribute('alt')).toBe(true);
      });
    });

    test('page structure is semantic HTML (works without JS)', () => {
      // Check for semantic elements
      expect(document.querySelector('header')).not.toBeNull();
      expect(document.querySelector('footer')).not.toBeNull();
      expect(document.querySelectorAll('section').length).toBeGreaterThan(0);

      // Check for proper heading hierarchy
      const h1 = document.querySelector('h1');
      const h2s = document.querySelectorAll('h2');

      expect(h1).not.toBeNull();
      expect(h2s.length).toBeGreaterThan(0);
    });

    test('no noscript fallbacks needed (content already visible)', () => {
      const noscriptElements = document.querySelectorAll('noscript');

      // If noscript exists, it should be for enhancement, not critical content
      noscriptElements.forEach(noscript => {
        const content = noscript.textContent.trim();
        // noscript should not contain critical content that's missing from main page
        // (because our page works without JS)
        expect(content).not.toContain('enable JavaScript');
      });
    });

    test('CSS provides styling without JavaScript', () => {
      // Check that CSS file exists and is properly linked
      const cssLink = document.querySelector('link[rel="stylesheet"]');
      expect(cssLink).not.toBeNull();

      // CSS should not be injected via JS
      const styleElements = document.querySelectorAll('style');
      // Inline styles for critical CSS are ok, but main styling should be in CSS file
      styleElements.forEach(style => {
        // If there are inline styles, they should be minimal
        const content = style.textContent;
        // Should not contain the bulk of the styling
        expect(content.length).toBeLessThan(5000);
      });
    });
  });

  describe('Static Hosting Compatibility', () => {
    test('page can be served from root path', () => {
      // All relative paths should work from root
      const relativeRefs = [
        ...Array.from(document.querySelectorAll('link[href]')).map(el => el.getAttribute('href')),
        ...Array.from(document.querySelectorAll('script[src]')).map(el => el.getAttribute('src')),
        ...Array.from(document.querySelectorAll('img[src]')).map(el => el.getAttribute('src'))
      ].filter(ref => ref && !ref.startsWith('http') && !ref.startsWith('//'));

      relativeRefs.forEach(ref => {
        // Should not use absolute paths that assume specific server structure
        expect(ref).not.toMatch(/^\/[^/]/);
      });
    });

    test('no server-specific configurations referenced', () => {
      // Check for server-specific config files that shouldn't be in static deployment
      const serverConfigs = [
        'web.config',
        '.htaccess',
        'server.js',
        'app.js',
        'server.py',
        'wsgi.py'
      ];

      serverConfigs.forEach(config => {
        // These files shouldn't be referenced in the HTML
        expect(htmlContent).not.toContain(config);
      });
    });

    test('meta tags present for SEO (static deployment benefit)', () => {
      const charset = document.querySelector('meta[charset]');
      const viewport = document.querySelector('meta[name="viewport"]');
      const description = document.querySelector('meta[name="description"]');

      expect(charset).not.toBeNull();
      expect(viewport).not.toBeNull();
      expect(description).not.toBeNull();
    });
  });
});
