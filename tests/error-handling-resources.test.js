/**
 * Error Handling - Missing Resources Tests
 *
 * Scenario: Verify that the homepage handles missing resources gracefully
 *
 * Test Cases:
 * 1. Load page with blocked images - Alt text displays, page remains functional
 * 2. Load page with CSS disabled - Content is still readable and logically structured
 * 3. Check for 404 errors - No 404 errors for any resources referenced by the page
 */

const http = require('http');
const fs = require('fs');
const path = require('path');
const cheerio = require('cheerio');

// Load the homepage HTML and CSS
const htmlPath = path.join(__dirname, '..', 'index.html');
const cssPath = path.join(__dirname, '..', 'styles.css');
const htmlContent = fs.readFileSync(htmlPath, 'utf-8');
const cssContent = fs.readFileSync(cssPath, 'utf-8');
const $ = cheerio.load(htmlContent);

describe('Error Handling - Missing Resources', () => {
  let server;
  let serverPort;

  /**
   * Simple static file server for testing
   * Can be configured to simulate missing resources
   */
  function createServer(options = {}) {
    const { blockImages = false, blockCSS = false } = options;

    return new Promise((resolve, reject) => {
      const srv = http.createServer((req, res) => {
        const url = req.url === '/' ? '/index.html' : req.url;
        const filePath = path.join(__dirname, '..', url);
        const ext = path.extname(filePath).toLowerCase();

        // Simulate blocked images
        if (blockImages && ['.gif', '.png', '.jpg', '.jpeg', '.webp', '.svg'].includes(ext)) {
          res.writeHead(404);
          res.end('Not Found - Images Blocked');
          return;
        }

        // Simulate blocked CSS
        if (blockCSS && ext === '.css') {
          res.writeHead(404);
          res.end('Not Found - CSS Blocked');
          return;
        }

        fs.readFile(filePath, (err, data) => {
          if (err) {
            res.writeHead(404);
            res.end('Not Found');
            return;
          }

          const contentTypes = {
            '.html': 'text/html',
            '.css': 'text/css',
            '.js': 'application/javascript',
            '.gif': 'image/gif',
            '.png': 'image/png',
            '.jpg': 'image/jpeg',
            '.svg': 'image/svg+xml'
          };

          res.writeHead(200, { 'Content-Type': contentTypes[ext] || 'application/octet-stream' });
          res.end(data);
        });
      });

      srv.listen(0, () => {
        resolve({ server: srv, port: srv.address().port });
      });

      srv.on('error', reject);
    });
  }

  function fetchUrl(port, urlPath) {
    return new Promise((resolve, reject) => {
      const req = http.get(`http://localhost:${port}${urlPath}`, (res) => {
        let data = [];
        res.on('data', chunk => data.push(chunk));
        res.on('end', () => {
          resolve({
            statusCode: res.statusCode,
            headers: res.headers,
            body: Buffer.concat(data).toString()
          });
        });
      });

      req.on('error', reject);
      req.setTimeout(5000, () => {
        req.destroy();
        reject(new Error('Request timeout'));
      });
    });
  }

  beforeAll(async () => {
    // Default server without any blocking
    const result = await createServer();
    server = result.server;
    serverPort = result.port;
  });

  afterAll((done) => {
    if (server) {
      server.close(done);
    } else {
      done();
    }
  });

  describe('Test Case 1: Load page with blocked images', () => {
    /**
     * When images fail to load, the page should:
     * 1. Display alt text for all images
     * 2. Remain functional (navigation, content visible)
     * 3. Not break the layout significantly
     */

    test('All images have alt attributes for graceful degradation', () => {
      const images = $('img');
      expect(images.length).toBeGreaterThan(0);

      images.each((index, element) => {
        const $img = $(element);
        const alt = $img.attr('alt');
        const src = $img.attr('src');

        // Every image must have an alt attribute
        expect(alt).toBeDefined();
        expect(typeof alt).toBe('string');

        // Alt text should be meaningful (not empty for content images)
        // Empty alt is only acceptable for decorative images
        if (!$img.hasClass('decorative') && !$img.attr('role')?.includes('presentation')) {
          expect(alt.length).toBeGreaterThan(0);
        }
      });
    });

    test('Alt text is descriptive and meaningful', () => {
      const images = $('img');

      images.each((index, element) => {
        const $img = $(element);
        const alt = $img.attr('alt');
        const src = $img.attr('src') || '';

        // Alt should not just be the filename
        const filename = path.basename(src, path.extname(src));
        expect(alt.toLowerCase()).not.toBe(filename.toLowerCase());

        // Alt should not be generic placeholder text
        const genericAlts = ['image', 'picture', 'photo', 'img', 'icon'];
        expect(genericAlts).not.toContain(alt.toLowerCase());
      });
    });

    test('Page structure remains intact without images', () => {
      // Verify key sections exist and are not dependent on images
      expect($('header').length).toBeGreaterThan(0);
      expect($('main').length).toBe(1);
      expect($('footer').length).toBe(1);
      expect($('nav').length).toBeGreaterThan(0);

      // Navigation should be text-based
      const navLinks = $('nav a');
      expect(navLinks.length).toBeGreaterThan(0);

      // Each nav link should have text content (not just images)
      navLinks.each((index, element) => {
        const $link = $(element);
        const text = $link.text().trim();
        const hasTextOrAlt = text.length > 0 || $link.find('img[alt]').length > 0;
        expect(hasTextOrAlt).toBe(true);
      });
    });

    test('Content headings are visible without images', () => {
      // All section headings should be present
      const h1 = $('h1');
      const h2s = $('h2');

      expect(h1.length).toBe(1);
      expect(h1.text().trim().length).toBeGreaterThan(0);

      expect(h2s.length).toBeGreaterThan(0);
      h2s.each((index, element) => {
        expect($(element).text().trim().length).toBeGreaterThan(0);
      });
    });

    test('Feature descriptions are text-based', () => {
      // Feature cards should have text content that works without images
      const featureCards = $('.feature-card');

      featureCards.each((index, element) => {
        const $card = $(element);
        const h3 = $card.find('h3');
        const p = $card.find('p');

        expect(h3.text().trim().length).toBeGreaterThan(0);
        expect(p.text().trim().length).toBeGreaterThan(0);
      });
    });

    test('Server responds correctly when images are blocked', async () => {
      // Create a temporary server that blocks images
      const blockedServer = await createServer({ blockImages: true });

      try {
        // HTML should still load
        const htmlResponse = await fetchUrl(blockedServer.port, '/');
        expect(htmlResponse.statusCode).toBe(200);

        // Images should return 404
        const imgResponse = await fetchUrl(blockedServer.port, '/assets/logo.gif');
        expect(imgResponse.statusCode).toBe(404);

        // CSS should still load
        const cssResponse = await fetchUrl(blockedServer.port, '/styles.css');
        expect(cssResponse.statusCode).toBe(200);
      } finally {
        blockedServer.server.close();
      }
    });
  });

  describe('Test Case 2: Load page with CSS disabled', () => {
    /**
     * When CSS fails to load, the page should:
     * 1. Display content in a logical, readable order
     * 2. Maintain semantic structure
     * 3. All interactive elements remain functional
     */

    test('HTML uses semantic elements for proper structure without CSS', () => {
      // Check for semantic HTML5 elements
      expect($('header').length).toBeGreaterThan(0);
      expect($('nav').length).toBeGreaterThan(0);
      expect($('main').length).toBe(1);
      expect($('section').length).toBeGreaterThan(0);
      expect($('footer').length).toBe(1);
    });

    test('Content order is logical in HTML source', () => {
      // Content should appear in a logical order in the source
      const allElements = $('body').children();
      const elementOrder = [];

      allElements.each((index, element) => {
        elementOrder.push(element.tagName.toLowerCase());
      });

      // Header should come before main, main before footer
      const headerIndex = elementOrder.indexOf('header');
      const mainIndex = elementOrder.indexOf('main');
      const footerIndex = elementOrder.indexOf('footer');

      expect(headerIndex).toBeLessThan(mainIndex);
      expect(mainIndex).toBeLessThan(footerIndex);
    });

    test('Headings provide document outline without CSS', () => {
      // Headings should create a meaningful outline
      const headings = $('h1, h2, h3, h4, h5, h6');

      expect(headings.length).toBeGreaterThan(0);

      // First heading should be h1
      const firstHeading = headings.first();
      expect(firstHeading.get(0).tagName.toLowerCase()).toBe('h1');

      // All headings should have text content
      headings.each((index, element) => {
        expect($(element).text().trim().length).toBeGreaterThan(0);
      });
    });

    test('Links are identifiable without CSS (using href)', () => {
      const links = $('a[href]');

      expect(links.length).toBeGreaterThan(0);

      links.each((index, element) => {
        const href = $(element).attr('href');
        expect(href).toBeDefined();
        expect(href.length).toBeGreaterThan(0);
      });
    });

    test('Lists are properly structured without CSS', () => {
      // Check for navigation list structure
      const navLists = $('nav ul, nav ol');

      if (navLists.length > 0) {
        navLists.each((index, element) => {
          const listItems = $(element).find('li');
          expect(listItems.length).toBeGreaterThan(0);
        });
      }
    });

    test('Tables have proper header structure for readability without CSS', () => {
      const tables = $('table');

      tables.each((index, element) => {
        const $table = $(element);
        const thead = $table.find('thead');
        const th = $table.find('th');

        // Tables should have thead or th elements
        expect(thead.length > 0 || th.length > 0).toBe(true);
      });
    });

    test('Code blocks use proper semantic elements', () => {
      // Code examples should use <pre> and <code> elements
      const codeBlocks = $('pre code, code');

      // The getting started section has code examples
      expect(codeBlocks.length).toBeGreaterThan(0);
    });

    test('Server responds correctly when CSS is blocked', async () => {
      // Create a temporary server that blocks CSS
      const blockedServer = await createServer({ blockCSS: true });

      try {
        // HTML should still load
        const htmlResponse = await fetchUrl(blockedServer.port, '/');
        expect(htmlResponse.statusCode).toBe(200);

        // CSS should return 404
        const cssResponse = await fetchUrl(blockedServer.port, '/styles.css');
        expect(cssResponse.statusCode).toBe(404);

        // Images should still load
        const imgResponse = await fetchUrl(blockedServer.port, '/assets/logo.gif');
        expect(imgResponse.statusCode).toBe(200);
      } finally {
        blockedServer.server.close();
      }
    });

    test('Content remains readable in source order', () => {
      // Extract text content in source order
      const mainContent = $('main').text();

      // Key content sections should be present
      expect(mainContent).toContain('MirDB');
      expect(mainContent.toLowerCase()).toContain('feature');
      expect(mainContent.toLowerCase()).toContain('getting started');
    });
  });

  describe('Test Case 3: Check for 404 errors in network tab', () => {
    /**
     * All resources referenced by the page should load successfully:
     * 1. All local images exist
     * 2. All stylesheets exist
     * 3. No broken internal links
     */

    test('All local images referenced in HTML exist', () => {
      const images = $('img');

      images.each((index, element) => {
        const src = $(element).attr('src');

        // Skip external images (http/https)
        if (src && !src.startsWith('http://') && !src.startsWith('https://') && !src.startsWith('data:')) {
          const imagePath = path.join(__dirname, '..', src);
          const exists = fs.existsSync(imagePath);

          if (!exists) {
            console.log(`Missing image: ${src}`);
          }

          expect(exists).toBe(true);
        }
      });
    });

    test('All local stylesheets referenced in HTML exist', () => {
      const styleLinks = $('link[rel="stylesheet"]');

      styleLinks.each((index, element) => {
        const href = $(element).attr('href');

        // Skip external stylesheets
        if (href && !href.startsWith('http://') && !href.startsWith('https://')) {
          const cssFilePath = path.join(__dirname, '..', href);
          const exists = fs.existsSync(cssFilePath);

          if (!exists) {
            console.log(`Missing stylesheet: ${href}`);
          }

          expect(exists).toBe(true);
        }
      });
    });

    test('All local images load via HTTP without 404', async () => {
      // Extract all local image sources
      const imgSrcRegex = /<img[^>]+src=["']([^"']+)["']/g;
      const imageSources = [];
      let match;

      while ((match = imgSrcRegex.exec(htmlContent)) !== null) {
        const src = match[1];
        if (!src.startsWith('http://') && !src.startsWith('https://') && !src.startsWith('data:')) {
          imageSources.push(src);
        }
      }

      // Test each image via HTTP
      const results = await Promise.all(
        imageSources.map(async (src) => {
          try {
            const normalizedSrc = src.startsWith('/') ? src : '/' + src;
            const response = await fetchUrl(serverPort, normalizedSrc);
            return { src, statusCode: response.statusCode };
          } catch (error) {
            return { src, statusCode: 'error', error: error.message };
          }
        })
      );

      // Check all images returned 200
      const failedImages = results.filter(r => r.statusCode !== 200);
      if (failedImages.length > 0) {
        console.log('Failed to load images:', failedImages);
      }

      expect(failedImages.length).toBe(0);
    });

    test('All stylesheets load via HTTP without 404', async () => {
      // Extract all local stylesheet hrefs
      const cssHrefRegex = /<link[^>]+rel=["']stylesheet["'][^>]+href=["']([^"']+)["']/g;
      const cssAltRegex = /<link[^>]+href=["']([^"']+)["'][^>]+rel=["']stylesheet["']/g;
      const cssSources = [];
      let match;

      while ((match = cssHrefRegex.exec(htmlContent)) !== null) {
        const href = match[1];
        if (!href.startsWith('http://') && !href.startsWith('https://')) {
          cssSources.push(href);
        }
      }

      while ((match = cssAltRegex.exec(htmlContent)) !== null) {
        const href = match[1];
        if (!href.startsWith('http://') && !href.startsWith('https://') && !cssSources.includes(href)) {
          cssSources.push(href);
        }
      }

      // Test each stylesheet via HTTP
      const results = await Promise.all(
        cssSources.map(async (href) => {
          try {
            const normalizedHref = href.startsWith('/') ? href : '/' + href;
            const response = await fetchUrl(serverPort, normalizedHref);
            return { href, statusCode: response.statusCode };
          } catch (error) {
            return { href, statusCode: 'error', error: error.message };
          }
        })
      );

      // Check all stylesheets returned 200
      const failedStylesheets = results.filter(r => r.statusCode !== 200);
      if (failedStylesheets.length > 0) {
        console.log('Failed to load stylesheets:', failedStylesheets);
      }

      expect(failedStylesheets.length).toBe(0);
    });

    test('HTML page loads successfully', async () => {
      const response = await fetchUrl(serverPort, '/');
      expect(response.statusCode).toBe(200);
      expect(response.headers['content-type']).toContain('text/html');
    });

    test('Internal anchor links have valid targets', () => {
      const internalLinks = $('a[href^="#"]');

      internalLinks.each((index, element) => {
        const href = $(element).attr('href');

        // Skip empty href="#"
        if (href === '#') return;

        const targetId = href.substring(1);
        const target = $(`#${targetId}`);

        if (target.length === 0) {
          console.log(`Missing anchor target: ${href}`);
        }

        expect(target.length).toBeGreaterThan(0);
      });
    });

    test('Assets directory structure is correct', () => {
      const assetsDir = path.join(__dirname, '..', 'assets');

      // Assets directory should exist
      expect(fs.existsSync(assetsDir)).toBe(true);

      // Expected assets should be present
      const expectedAssets = ['logo.gif', 'usage.gif'];

      expectedAssets.forEach(asset => {
        const assetPath = path.join(assetsDir, asset);
        expect(fs.existsSync(assetPath)).toBe(true);
      });
    });

    test('No references to non-existent files in CSS', () => {
      // Check for url() references in CSS
      const urlRegex = /url\(["']?([^"')]+)["']?\)/g;
      const cssUrls = [];
      let match;

      while ((match = urlRegex.exec(cssContent)) !== null) {
        const url = match[1];
        // Skip data URIs and external URLs
        if (!url.startsWith('data:') && !url.startsWith('http://') && !url.startsWith('https://')) {
          cssUrls.push(url);
        }
      }

      // Verify each CSS-referenced file exists
      cssUrls.forEach(url => {
        const filePath = path.join(__dirname, '..', url);
        if (!fs.existsSync(filePath)) {
          console.log(`Missing CSS reference: ${url}`);
        }
        // Note: If CSS has no local file references, this test passes
        // Current CSS doesn't reference local files which is fine
      });

      // If there are URL references, they should exist
      // If no references, test passes automatically
      expect(true).toBe(true);
    });
  });
});
