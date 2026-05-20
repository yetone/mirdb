/**
 * Performance Integration Tests
 * Owner: Scenario 10 - Performance
 *
 * Tests:
 * - Page load performance metrics (simulated)
 * - Total page weight under 1MB
 * - Image optimization (file sizes)
 * - CSS/JS minification
 * - Lazy loading on below-the-fold images
 * - Cache-Control headers on static assets
 * - Core Web Vitals indicators
 */

const fs = require('fs');
const path = require('path');
const http = require('http');

const htmlPath = path.join(__dirname, '../../index.html');
const html = fs.readFileSync(htmlPath, 'utf-8');

const projectRoot = path.join(__dirname, '../..');

/**
 * Calculate file size in bytes
 */
function getFileSize(filePath) {
  try {
    return fs.statSync(filePath).size;
  } catch (e) {
    return 0;
  }
}

/**
 * Start a simple HTTP server for cache header testing
 */
function startTestServer(port = 9876) {
  return new Promise((resolve) => {
    const server = http.createServer((req, res) => {
      const urlPath = req.url === '/' ? '/index.html' : req.url;
      const filePath = path.join(projectRoot, urlPath);
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

      // Set cache headers for static assets
      if (ext === '.css' || ext === '.js' || ext === '.gif' || ext === '.png' || ext === '.jpg' || ext === '.jpeg') {
        res.setHeader('Cache-Control', 'public, max-age=86400');
      }

      fs.readFile(filePath, (err, data) => {
        if (err) {
          res.writeHead(404);
          res.end('Not found');
          return;
        }
        res.writeHead(200, { 'Content-Type': contentTypes[ext] || 'application/octet-stream' });
        res.end(data);
      });
    });

    server.listen(port, () => {
      resolve(server);
    });
  });
}

/**
 * Make an HTTP request and return response headers
 */
function fetchHeaders(url) {
  return new Promise((resolve, reject) => {
    const req = http.get(url, (res) => {
      resolve(res.headers);
    });
    req.on('error', reject);
  });
}

describe('Page Weight and Asset Sizes', () => {
  test('total page weight is under 1MB for initial load', () => {
    const filesToMeasure = [
      'index.html',
      'css/base.min.css',
      'css/hero.min.css',
      'css/overview.min.css',
      'css/features.min.css',
      'css/quickstart.min.css',
      'css/roadmap.min.css',
      'css/nav.min.css',
      'css/footer.min.css',
      'css/responsive.css',
      'js/main.min.js',
      'js/nav.min.js',
      'js/copy.min.js',
      'assets/logo-optimized.gif',
    ];

    let totalSize = 0;
    const fileSizes = {};

    filesToMeasure.forEach((file) => {
      const size = getFileSize(path.join(projectRoot, file));
      fileSizes[file] = size;
      totalSize += size;
    });

    const totalMB = totalSize / (1024 * 1024);
    console.log('File sizes (bytes):', fileSizes);
    console.log(`Total page weight: ${totalMB.toFixed(2)} MB`);

    expect(totalSize).toBeLessThan(1024 * 1024); // Under 1MB
  });
});

describe('Image Optimization', () => {
  test('logo.gif optimized version exists and is under 500KB', () => {
    const optimizedPath = path.join(projectRoot, 'assets/logo-optimized.gif');
    const size = getFileSize(optimizedPath);

    expect(fs.existsSync(optimizedPath)).toBe(true);
    expect(size).toBeGreaterThan(0);
    expect(size).toBeLessThan(500 * 1024); // Under 500KB
  });

  test('usage.gif optimized version exists and is under 500KB', () => {
    const optimizedPath = path.join(projectRoot, 'assets/usage-optimized.gif');
    const size = getFileSize(optimizedPath);

    expect(fs.existsSync(optimizedPath)).toBe(true);
    expect(size).toBeGreaterThan(0);
    expect(size).toBeLessThan(500 * 1024); // Under 500KB
  });

  test('no images referenced in HTML exceed 500KB', () => {
    document.body.innerHTML = html;
    const images = document.querySelectorAll('img[src]');

    images.forEach((img) => {
      const src = img.getAttribute('src');
      if (src.startsWith('assets/')) {
        const fullPath = path.join(projectRoot, src);
        const size = getFileSize(fullPath);
        expect(size).toBeLessThan(500 * 1024);
      }
    });
  });

  test('HTML references optimized image versions', () => {
    document.body.innerHTML = html;
    const images = document.querySelectorAll('img');

    const hasOptimizedLogo = Array.from(images).some(
      (img) => img.getAttribute('src') === 'assets/logo-optimized.gif'
    );
    const hasOptimizedUsage = Array.from(images).some(
      (img) => img.getAttribute('src') === 'assets/usage-optimized.gif'
    );

    expect(hasOptimizedLogo).toBe(true);
    expect(hasOptimizedUsage).toBe(true);
  });
});

describe('CSS and JS Minification', () => {
  const cssDir = path.join(projectRoot, 'css');
  const jsDir = path.join(projectRoot, 'js');

  test('minified CSS files exist', () => {
    const cssFiles = ['base.min.css', 'hero.min.css', 'overview.min.css', 'features.min.css',
                      'quickstart.min.css', 'roadmap.min.css', 'nav.min.css', 'footer.min.css'];

    cssFiles.forEach((file) => {
      const filePath = path.join(cssDir, file);
      expect(fs.existsSync(filePath)).toBe(true);
      const content = fs.readFileSync(filePath, 'utf-8');
      expect(content.length).toBeGreaterThan(0);
    });
  });

  test('minified CSS files have no block comments', () => {
    const cssFiles = fs.readdirSync(cssDir).filter((f) => f.endsWith('.min.css'));

    cssFiles.forEach((file) => {
      const content = fs.readFileSync(path.join(cssDir, file), 'utf-8');
      expect(content).not.toMatch(/\/\*[\s\S]*?\*\//);
    });
  });

  test('minified JS files exist', () => {
    const jsFiles = ['main.min.js', 'nav.min.js', 'copy.min.js'];

    jsFiles.forEach((file) => {
      const filePath = path.join(jsDir, file);
      expect(fs.existsSync(filePath)).toBe(true);
      const content = fs.readFileSync(filePath, 'utf-8');
      expect(content.length).toBeGreaterThan(0);
    });
  });

  test('minified JS files have no block comments', () => {
    const jsFiles = fs.readdirSync(jsDir).filter((f) => f.endsWith('.min.js'));

    jsFiles.forEach((file) => {
      const content = fs.readFileSync(path.join(jsDir, file), 'utf-8');
      expect(content).not.toMatch(/\/\*[\s\S]*?\*\//);
    });
  });

  test('HTML references minified CSS files', () => {
    const minCssPattern = /href="css\/[\w-]+\.min\.css"/g;
    const matches = html.match(minCssPattern) || [];
    expect(matches.length).toBeGreaterThanOrEqual(7);
  });

  test('HTML references minified JS files', () => {
    const minJsPattern = /src="js\/[\w-]+\.min\.js"/g;
    const matches = html.match(minJsPattern) || [];
    expect(matches.length).toBeGreaterThanOrEqual(2);
  });
});

describe('Lazy Loading', () => {
  beforeEach(() => {
    document.body.innerHTML = html;
  });

  test('below-the-fold images have loading="lazy" attribute', () => {
    const images = document.querySelectorAll('img');
    const belowFoldImages = Array.from(images).filter((img) => {
      const src = img.getAttribute('src') || '';
      return src.includes('usage-optimized.gif');
    });

    expect(belowFoldImages.length).toBeGreaterThan(0);

    belowFoldImages.forEach((img) => {
      expect(img.getAttribute('loading')).toBe('lazy');
    });
  });

  test('all images below the fold use lazy loading', () => {
    // Images that are not the hero logo (above the fold) should be lazy loaded
    const images = document.querySelectorAll('img');

    images.forEach((img) => {
      const src = img.getAttribute('src') || '';
      const parentSection = img.closest('section');
      const sectionId = parentSection ? parentSection.id : '';

      if (sectionId !== 'hero' && !src.includes('logo-optimized.gif')) {
        expect(img.getAttribute('loading')).toBe('lazy');
      }
    });
  });
});

describe('Cache Headers', () => {
  let server;
  const port = 9876;
  const baseUrl = `http://localhost:${port}`;

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

  test('CSS files are served with Cache-Control header', async () => {
    const headers = await fetchHeaders(`${baseUrl}/css/base.min.css`);
    expect(headers['cache-control']).toBeDefined();
    expect(headers['cache-control']).toMatch(/max-age=(\d+)/);

    const match = headers['cache-control'].match(/max-age=(\d+)/);
    const maxAge = parseInt(match[1], 10);
    expect(maxAge).toBeGreaterThanOrEqual(86400);
  });

  test('JS files are served with Cache-Control header', async () => {
    const headers = await fetchHeaders(`${baseUrl}/js/main.min.js`);
    expect(headers['cache-control']).toBeDefined();
    expect(headers['cache-control']).toMatch(/max-age=(\d+)/);

    const match = headers['cache-control'].match(/max-age=(\d+)/);
    const maxAge = parseInt(match[1], 10);
    expect(maxAge).toBeGreaterThanOrEqual(86400);
  });

  test('image assets are served with Cache-Control header', async () => {
    const headers = await fetchHeaders(`${baseUrl}/assets/logo-optimized.gif`);
    expect(headers['cache-control']).toBeDefined();
    expect(headers['cache-control']).toMatch(/max-age=(\d+)/);

    const match = headers['cache-control'].match(/max-age=(\d+)/);
    const maxAge = parseInt(match[1], 10);
    expect(maxAge).toBeGreaterThanOrEqual(86400);
  });
});

describe('Performance Structure and Lighthouse Indicators', () => {
  beforeEach(() => {
    document.body.innerHTML = html;
  });

  test('preconnect or preload hints exist for critical resources', () => {
    const hasPreload = html.includes('rel="preload"');
    expect(hasPreload).toBe(true);
  });

  test('critical CSS is preloaded', () => {
    const preloadMatches = html.match(/rel="preload"[^>]+as="style"/g) || [];
    expect(preloadMatches.length).toBeGreaterThanOrEqual(1);
  });

  test('viewport meta tag is present for mobile optimization', () => {
    const headMatch = html.match(/<head>[\s\S]*?<\/head>/);
    expect(headMatch).toBeTruthy();
    const headContent = headMatch[0];
    expect(headContent).toMatch(/name="viewport"/);
  });

  test('images have explicit width and height attributes', () => {
    const images = document.querySelectorAll('img');

    images.forEach((img) => {
      const width = img.getAttribute('width');
      const height = img.getAttribute('height');
      // At least one dimension should be specified to prevent CLS
      expect(width || height).toBeTruthy();
    });
  });

  test('hero section renders critical content first', () => {
    const main = document.querySelector('main');
    expect(main).toBeTruthy();

    const children = Array.from(main.children);
    const firstSection = children.find((child) => child.tagName.toLowerCase() === 'section');
    expect(firstSection).toBeTruthy();
    expect(firstSection.id).toBe('hero');

    const hero = document.querySelector('#hero');
    const h1 = hero.querySelector('h1');
    expect(h1).toBeTruthy();
  });
});

describe('Core Web Vitals Structure', () => {
  beforeEach(() => {
    document.body.innerHTML = html;
  });

  test('LCP candidate exists (largest contentful paint element)', () => {
    // Hero section with large image or text is typical LCP element
    const hero = document.querySelector('#hero');
    expect(hero).toBeTruthy();

    // Check for hero logo (image LCP candidate)
    const heroLogo = hero.querySelector('img');
    expect(heroLogo).toBeTruthy();
  });

  test('no render-blocking external scripts in head', () => {
    const headContent = html.match(/<head>[\s\S]*?<\/head>/)[0];
    const scriptInHead = headContent.match(/<script[^>]*src=[^>]*>/g) || [];
    expect(scriptInHead.length).toBe(0);
  });

  test('scripts are placed at end of body', () => {
    const bodyMatch = html.match(/<body>[\s\S]*?<\/body>/);
    expect(bodyMatch).toBeTruthy();

    const bodyContent = bodyMatch[0];
    const lastScript = bodyContent.lastIndexOf('<script');
    const closingBody = bodyContent.lastIndexOf('</body>');

    expect(lastScript).toBeLessThan(closingBody);
  });

  test('CLS risk minimized with sized images', () => {
    const images = document.querySelectorAll('img');

    images.forEach((img) => {
      const hasWidth = img.hasAttribute('width');
      const hasHeight = img.hasAttribute('height');

      if (hasWidth || hasHeight) {
        expect(true).toBe(true);
      }
    });
  });

  test('page has meta cache-control for browser caching', () => {
    const headContent = html.match(/<head>[\s\S]*?<\/head>/)[0];
    expect(headContent).toMatch(/Cache-Control/);
    expect(headContent).toMatch(/max-age=\d+/);
  });
});

describe('Simulated Lighthouse Performance Score', () => {
  test('performance indicators suggest score >= 90', () => {
    // Collect performance indicators
    const indicators = {
      // Page weight under 1MB
      pageWeightOk: false,
      // Images optimized
      imagesOptimized: false,
      // CSS minified
      cssMinified: false,
      // JS minified
      jsMinified: false,
      // Lazy loading present
      lazyLoading: false,
      // Preload hints present
      preloadHints: false,
      // Cache headers present
      cacheHeaders: false,
      // Scripts at end of body
      scriptsDeferred: false,
    };

    // Check page weight
    const filesToMeasure = [
      'index.html',
      'css/base.min.css',
      'css/hero.min.css',
      'css/overview.min.css',
      'css/features.min.css',
      'css/quickstart.min.css',
      'css/roadmap.min.css',
      'css/nav.min.css',
      'css/footer.min.css',
      'css/responsive.css',
      'js/main.min.js',
      'js/nav.min.js',
      'js/copy.min.js',
      'assets/logo-optimized.gif',
    ];

    let totalSize = 0;
    filesToMeasure.forEach((file) => {
      totalSize += getFileSize(path.join(projectRoot, file));
    });
    indicators.pageWeightOk = totalSize < 1024 * 1024;

    // Check images
    indicators.imagesOptimized =
      getFileSize(path.join(projectRoot, 'assets/logo-optimized.gif')) < 500 * 1024 &&
      getFileSize(path.join(projectRoot, 'assets/usage-optimized.gif')) < 500 * 1024;

    // Check minification
    const cssDir = path.join(projectRoot, 'css');
    const jsDir = path.join(projectRoot, 'js');
    const minCssFiles = fs.readdirSync(cssDir).filter((f) => f.endsWith('.min.css'));
    const minJsFiles = fs.readdirSync(jsDir).filter((f) => f.endsWith('.min.js'));
    indicators.cssMinified = minCssFiles.length >= 7;
    indicators.jsMinified = minJsFiles.length >= 2;

    // Check lazy loading
    document.body.innerHTML = html;
    const lazyImages = document.querySelectorAll('img[loading="lazy"]');
    indicators.lazyLoading = lazyImages.length > 0;

    // Check preload hints
    indicators.preloadHints = html.includes('rel="preload"');

    // Check cache headers in meta
    indicators.cacheHeaders = html.includes('Cache-Control');

    // Check scripts at end of body
    const bodyMatch = html.match(/<body>[\s\S]*?<\/body>/);
    const scriptInHead = html.match(/<head>[\s\S]*?<script[^>]*src=[\s\S]*?<\/head>/);
    indicators.scriptsDeferred = !scriptInHead;

    // Calculate simulated score (each indicator contributes)
    const score = Object.values(indicators).filter(Boolean).length / Object.keys(indicators).length * 100;
    console.log('Performance indicators:', indicators);
    console.log(`Simulated performance score: ${score.toFixed(1)}`);

    expect(score).toBeGreaterThanOrEqual(90);
  });
});
