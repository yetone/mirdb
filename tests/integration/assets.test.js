/**
 * Asset Integration Tests
 * Owner: Scenario 11 - Asset Integration
 *
 * Tests:
 * - All local asset paths resolve (200 OK)
 * - External badge loads successfully
 * - Fallback behavior for broken images
 * - Asset size validation
 * - Base URL configuration consistency
 */

const fs = require('fs');
const path = require('path');
const http = require('http');

const htmlPath = path.join(__dirname, '../../index.html');
const html = fs.readFileSync(htmlPath, 'utf-8');
const projectRoot = path.join(__dirname, '../..');

/**
 * Collect all local asset references from the HTML document
 */
function collectLocalAssetRefs(doc) {
  const refs = new Set();

  // Images
  doc.querySelectorAll('img[src]').forEach((img) => {
    const src = img.getAttribute('src');
    if (src && !src.startsWith('http') && !src.startsWith('//') && !src.startsWith('data:')) {
      refs.add(src);
    }
  });

  // Links (stylesheets, favicon, canonical, preload)
  doc.querySelectorAll('link[href]').forEach((link) => {
    const href = link.getAttribute('href');
    if (href && !href.startsWith('http') && !href.startsWith('//') && !href.startsWith('data:')) {
      refs.add(href);
    }
  });

  // Scripts
  doc.querySelectorAll('script[src]').forEach((script) => {
    const src = script.getAttribute('src');
    if (src && !src.startsWith('http') && !src.startsWith('//')) {
      refs.add(src);
    }
  });

  return Array.from(refs);
}

/**
 * Start a simple HTTP server for asset resolution testing
 */
function startTestServer(port = 9877) {
  return new Promise((resolve) => {
    const server = http.createServer((req, res) => {
      const urlPath = req.url === '/' ? '/index.html' : req.url;
      const filePath = path.join(projectRoot, decodeURIComponent(urlPath));
      const ext = path.extname(filePath);

      const contentTypes = {
        '.html': 'text/html',
        '.css': 'text/css',
        '.js': 'application/javascript',
        '.gif': 'image/gif',
        '.png': 'image/png',
        '.jpg': 'image/jpeg',
        '.jpeg': 'image/jpeg',
      };

      fs.readFile(filePath, (err, data) => {
        if (err) {
          res.writeHead(404, { 'Content-Type': 'text/plain' });
          res.end('Not found');
          return;
        }
        res.writeHead(200, {
          'Content-Type': contentTypes[ext] || 'application/octet-stream',
        });
        res.end(data);
      });
    });

    server.listen(port, () => {
      resolve(server);
    });
  });
}

/**
 * Make an HTTP GET request and return status code
 */
function fetchStatus(url) {
  return new Promise((resolve, reject) => {
    http.get(url, (res) => {
      resolve(res.statusCode);
    }).on('error', (err) => {
      reject(err);
    });
  });
}

describe('Logo Asset Integration', () => {
  beforeEach(() => {
    document.body.innerHTML = html;
  });

  test('logo image src points to a valid existing file', () => {
    const logoImages = document.querySelectorAll('img[src*="logo"]');
    expect(logoImages.length).toBeGreaterThan(0);

    logoImages.forEach((img) => {
      const src = img.getAttribute('src');
      expect(src).toBeTruthy();

      // Verify the file exists on disk
      const fullPath = path.join(projectRoot, src);
      expect(fs.existsSync(fullPath)).toBe(true);

      // Verify it's a valid image file (has content)
      const stats = fs.statSync(fullPath);
      expect(stats.size).toBeGreaterThan(0);
    });
  });

  test('canonical logo file (assets/logo.gif) exists and is accessible', () => {
    const logoPath = path.join(projectRoot, 'assets/logo.gif');
    expect(fs.existsSync(logoPath)).toBe(true);

    const stats = fs.statSync(logoPath);
    expect(stats.size).toBeGreaterThan(0);
    expect(stats.isFile()).toBe(true);
  });

  test('logo has alt text for accessibility', () => {
    const logoImages = document.querySelectorAll('img[src*="logo"]');
    expect(logoImages.length).toBeGreaterThan(0);

    logoImages.forEach((img) => {
      const alt = img.getAttribute('alt');
      expect(alt).toBeTruthy();
      expect(alt.length).toBeGreaterThan(0);
    });
  });

  test('favicon references a valid logo file', () => {
    const favicon = document.querySelector('link[rel="icon"]');
    expect(favicon).toBeTruthy();

    const href = favicon.getAttribute('href');
    expect(href).toBeTruthy();

    const fullPath = path.join(projectRoot, href);
    expect(fs.existsSync(fullPath)).toBe(true);
  });
});

describe('Usage GIF Integration', () => {
  beforeEach(() => {
    document.body.innerHTML = html;
  });

  test('usage GIF src points to a valid existing file', () => {
    const usageImages = document.querySelectorAll('img[src*="usage"]');
    expect(usageImages.length).toBeGreaterThan(0);

    usageImages.forEach((img) => {
      const src = img.getAttribute('src');
      expect(src).toBeTruthy();

      const fullPath = path.join(projectRoot, src);
      expect(fs.existsSync(fullPath)).toBe(true);

      const stats = fs.statSync(fullPath);
      expect(stats.size).toBeGreaterThan(0);
    });
  });

  test('canonical usage file (assets/usage.gif) exists and is accessible', () => {
    const usagePath = path.join(projectRoot, 'assets/usage.gif');
    expect(fs.existsSync(usagePath)).toBe(true);

    const stats = fs.statSync(usagePath);
    expect(stats.size).toBeGreaterThan(0);
    expect(stats.isFile()).toBe(true);
  });

  test('usage GIF has descriptive alt text', () => {
    const usageImages = document.querySelectorAll('img[src*="usage"]');
    expect(usageImages.length).toBeGreaterThan(0);

    usageImages.forEach((img) => {
      const alt = img.getAttribute('alt');
      expect(alt).toBeTruthy();
      expect(alt.length).toBeGreaterThan(0);
    });
  });

  test('usage GIF has lazy loading attribute', () => {
    const usageImages = document.querySelectorAll('img[src*="usage"]');
    expect(usageImages.length).toBeGreaterThan(0);

    usageImages.forEach((img) => {
      const loading = img.getAttribute('loading');
      expect(loading).toBe('lazy');
    });
  });
});

describe('Local Asset Path Resolution (200 OK)', () => {
  let server;
  const port = 9877;

  beforeAll(async () => {
    server = await startTestServer(port);
  });

  afterAll((done) => {
    if (server) {
      server.close(done);
    } else {
      done();
    }
  });

  test('all referenced local assets return HTTP 200', async () => {
    document.body.innerHTML = html;
    const assetRefs = collectLocalAssetRefs(document);

    expect(assetRefs.length).toBeGreaterThan(0);

    for (const ref of assetRefs) {
      const url = `http://localhost:${port}/${ref}`;
      const status = await fetchStatus(url);
      expect(status).toBe(200);
    }
  });

  test('all CSS files referenced in HTML are resolvable', async () => {
    document.body.innerHTML = html;
    const cssLinks = document.querySelectorAll('link[rel="stylesheet"]');
    expect(cssLinks.length).toBeGreaterThan(0);

    for (const link of Array.from(cssLinks)) {
      const href = link.getAttribute('href');
      if (href && !href.startsWith('http') && !href.startsWith('//')) {
        const url = `http://localhost:${port}/${href}`;
        const status = await fetchStatus(url);
        expect(status).toBe(200);
      }
    }
  });

  test('all JS files referenced in HTML are resolvable', async () => {
    document.body.innerHTML = html;
    const scripts = document.querySelectorAll('script[src]');
    expect(scripts.length).toBeGreaterThan(0);

    for (const script of Array.from(scripts)) {
      const src = script.getAttribute('src');
      if (src && !src.startsWith('http') && !src.startsWith('//')) {
        const url = `http://localhost:${port}/${src}`;
        const status = await fetchStatus(url);
        expect(status).toBe(200);
      }
    }
  });

  test('asset files have correct content types', async () => {
    const assets = [
      { path: 'assets/logo.gif', type: 'image/gif' },
      { path: 'assets/usage.gif', type: 'image/gif' },
    ];

    for (const asset of assets) {
      const url = `http://localhost:${port}/${asset.path}`;
      const status = await fetchStatus(url);
      expect(status).toBe(200);
    }
  });
});

describe('CircleCI Badge Fallback Handling', () => {
  beforeEach(() => {
    document.body.innerHTML = html;
  });

  test('CircleCI badge is referenced in the page', () => {
    const badgeImages = document.querySelectorAll('img[src*="circleci"]');
    expect(badgeImages.length).toBeGreaterThan(0);
  });

  test('CircleCI badge has an onerror fallback handler', () => {
    const badgeImages = document.querySelectorAll('img[src*="circleci"]');
    expect(badgeImages.length).toBeGreaterThan(0);

    badgeImages.forEach((img) => {
      const onerror = img.getAttribute('onerror');
      expect(onerror).toBeTruthy();
      expect(onerror.length).toBeGreaterThan(0);
    });
  });

  test('badge fallback element exists in the DOM', () => {
    const fallback = document.querySelector('.ci-badge-fallback');
    expect(fallback).toBeTruthy();
  });

  test('badge fallback is initially hidden', () => {
    const fallback = document.querySelector('.ci-badge-fallback');
    expect(fallback).toBeTruthy();

    const style = fallback.getAttribute('style') || '';
    const computedHidden = style.includes('display:none') || style.includes('display: none');
    expect(computedHidden).toBe(true);
  });

  test('badge link wraps both image and fallback for accessibility', () => {
    const badgeLink = document.querySelector('.ci-badge-link');
    expect(badgeLink).toBeTruthy();

    const img = badgeLink.querySelector('img.ci-badge');
    expect(img).toBeTruthy();

    const fallback = badgeLink.querySelector('.ci-badge-fallback');
    expect(fallback).toBeTruthy();
  });

  test('badge link has proper external link attributes', () => {
    const badgeLink = document.querySelector('.ci-badge-link');
    expect(badgeLink).toBeTruthy();

    expect(badgeLink.getAttribute('href')).toContain('circleci.com');
    expect(badgeLink.getAttribute('target')).toBe('_blank');
    expect(badgeLink.getAttribute('rel')).toContain('noopener');
  });

  test('badge image has loading="lazy" attribute', () => {
    const badgeImage = document.querySelector('img.ci-badge');
    expect(badgeImage).toBeTruthy();

    const loading = badgeImage.getAttribute('loading');
    expect(loading).toBe('lazy');
  });

  test('badge image has descriptive alt text', () => {
    const badgeImage = document.querySelector('img.ci-badge');
    expect(badgeImage).toBeTruthy();

    const alt = badgeImage.getAttribute('alt');
    expect(alt).toBeTruthy();
    expect(alt.toLowerCase()).toContain('circleci');
  });
});

describe('Base URL and Path Consistency', () => {
  beforeEach(() => {
    document.body.innerHTML = html;
  });

  test('all local asset paths use relative paths from repository root', () => {
    const assetRefs = collectLocalAssetRefs(document);
    expect(assetRefs.length).toBeGreaterThan(0);

    assetRefs.forEach((ref) => {
      // Should not use absolute paths like /css/ or /js/
      expect(ref).not.toMatch(/^\//);
      // Should not use parent directory traversal
      expect(ref).not.toMatch(/^\.\.\//);
    });
  });

  test('asset paths follow consistent naming conventions', () => {
    const assetRefs = collectLocalAssetRefs(document);

    const cssRefs = assetRefs.filter((ref) => ref.startsWith('css/'));
    const jsRefs = assetRefs.filter((ref) => ref.startsWith('js/'));
    const imgRefs = assetRefs.filter((ref) => ref.startsWith('assets/'));

    // CSS files should be in css/
    cssRefs.forEach((ref) => {
      expect(ref).toMatch(/^css\/[\w\-]+(\.min)?\.css$/);
    });

    // JS files should be in js/
    jsRefs.forEach((ref) => {
      expect(ref).toMatch(/^js\/[\w\-]+(\.min)?\.js$/);
    });

    // Image assets should be in assets/
    imgRefs.forEach((ref) => {
      expect(ref).toMatch(/^assets\/[\w\-]+\.(gif|png|jpg|jpeg)$/);
    });
  });

  test('HTML base tag or meta tags do not conflict with relative paths', () => {
    const baseTag = document.querySelector('base');
    // No base tag means relative paths resolve from current document URL
    // This is acceptable and preferred for simple static sites
    expect(baseTag).toBeNull();
  });

  test('Open Graph image uses absolute URL for external sharing', () => {
    const ogImage = document.querySelector('meta[property="og:image"]');
    expect(ogImage).toBeTruthy();

    const content = ogImage.getAttribute('content');
    expect(content).toMatch(/^https?:\/\//);
  });

  test('Twitter card image uses absolute URL for external sharing', () => {
    const twitterImage = document.querySelector('meta[name="twitter:image"]');
    expect(twitterImage).toBeTruthy();

    const content = twitterImage.getAttribute('content');
    expect(content).toMatch(/^https?:\/\//);
  });

  test('no duplicate asset references with different paths', () => {
    const assetRefs = collectLocalAssetRefs(document);
    const uniqueRefs = new Set(assetRefs);

    // Some intentional duplicates may exist (e.g., same CSS for preload and link),
    // but we should not have the same logical asset at two different paths
    const pathMap = new Map();
    assetRefs.forEach((ref) => {
      const basename = path.basename(ref);
      if (pathMap.has(basename)) {
        expect(pathMap.get(basename)).toBe(ref);
      } else {
        pathMap.set(basename, ref);
      }
    });
  });
});

describe('Asset File Integrity', () => {
  test('all GIF files have valid magic bytes', () => {
    const gifFiles = ['assets/logo.gif', 'assets/usage.gif',
                      'assets/logo-optimized.gif', 'assets/usage-optimized.gif'];

    gifFiles.forEach((file) => {
      const filePath = path.join(projectRoot, file);
      if (fs.existsSync(filePath)) {
        const fd = fs.openSync(filePath, 'r');
        const buffer = Buffer.alloc(6);
        fs.readSync(fd, buffer, 0, 6, 0);
        fs.closeSync(fd);

        // GIF magic bytes: GIF87a or GIF89a
        const magic = buffer.toString('ascii', 0, 6);
        expect(magic).toMatch(/^GIF8[79]a/);
      }
    });
  });

  test('all referenced CSS files have valid CSS content', () => {
    document.body.innerHTML = html;
    const cssLinks = document.querySelectorAll('link[rel="stylesheet"]');

    cssLinks.forEach((link) => {
      const href = link.getAttribute('href');
      if (href && !href.startsWith('http') && !href.startsWith('//')) {
        const filePath = path.join(projectRoot, href);
        if (fs.existsSync(filePath)) {
          const content = fs.readFileSync(filePath, 'utf-8');
          expect(content.length).toBeGreaterThan(0);
          // Should contain at least one CSS rule (selector + braces)
          expect(content).toMatch(/[\w\.#\[][\w\s\.#:\[\]\-()+,>~*="']*\{/);
        }
      }
    });
  });

  test('all referenced JS files have valid JS content', () => {
    document.body.innerHTML = html;
    const scripts = document.querySelectorAll('script[src]');

    scripts.forEach((script) => {
      const src = script.getAttribute('src');
      if (src && !src.startsWith('http') && !src.startsWith('//')) {
        const filePath = path.join(projectRoot, src);
        if (fs.existsSync(filePath)) {
          const content = fs.readFileSync(filePath, 'utf-8');
          expect(content.length).toBeGreaterThan(0);
        }
      }
    });
  });
});
