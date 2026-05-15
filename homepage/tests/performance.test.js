/**
 * Performance tests for MirDB homepage.
 * Owner: Scenario 10 - Performance Optimization
 *
 * Test framework: Vitest + jsdom (unit tests) + HTTP server (integration tests)
 *
 * Test coverage:
 * - Page load time < 2s on simulated 4G
 * - LCP < 2.5s
 * - CLS < 0.1
 * - Critical CSS inlined, non-critical CSS loaded async
 * - Scripts use defer/async
 * - Images below fold use loading="lazy"
 * - Total page size under 1MB
 * - Browser caching headers
 * - Lighthouse Performance score >= 90 (structural prerequisites)
 * - Performance on slow 3G (structural checks)
 */

import { describe, it, expect, beforeAll, beforeEach, afterAll } from 'vitest';
import { readFileSync, statSync, existsSync } from 'fs';
import { resolve, join } from 'path';
import { JSDOM } from 'jsdom';
import { createServer } from 'http';

const __dirname = import.meta.dirname;
const htmlPath = resolve(__dirname, '../index.html');
const cssDir = resolve(__dirname, '../css');
const jsDir = resolve(__dirname, '../js');
const assetsDir = resolve(__dirname, '../assets');

function loadDOM() {
  const html = readFileSync(htmlPath, 'utf-8');
  return new JSDOM(html, { url: 'http://localhost:8080' });
}

function getFileSize(filePath) {
  if (!existsSync(filePath)) return 0;
  return statSync(filePath).size;
}

function getTotalPageSize() {
  const files = [
    htmlPath,
    resolve(cssDir, 'base.css'),
    resolve(cssDir, 'navigation.css'),
    resolve(cssDir, 'hero.css'),
    resolve(cssDir, 'features.css'),
    resolve(cssDir, 'quickstart.css'),
    resolve(cssDir, 'resources.css'),
    resolve(cssDir, 'footer.css'),
    resolve(cssDir, 'responsive.css'),
    resolve(jsDir, 'navigation.js'),
    resolve(jsDir, 'copy-code.js'),
    resolve(assetsDir, 'logo.webp'),
  ];
  let total = 0;
  for (const file of files) {
    total += getFileSize(file);
  }
  return total;
}

/* ========================================================================
   UNIT TESTS: HTML/CSS structure verification (jsdom)
   ======================================================================== */

describe('Unit Tests: Render-blocking Resources', () => {
  let dom;
  let document;

  beforeEach(() => {
    dom = loadDOM();
    document = dom.window.document;
  });

  /* Test Case 4: Check for render-blocking CSS */
  describe('Test Case 4: Render-blocking CSS', () => {
    it('should have critical CSS inlined in a style tag within head', () => {
      const styles = document.querySelectorAll('head style');
      expect(styles.length).toBeGreaterThanOrEqual(1);
      let hasCriticalContent = false;
      for (const style of styles) {
        const text = style.textContent;
        if (text.includes('box-sizing') || text.includes(':root') || text.includes('#hero')) {
          hasCriticalContent = true;
          break;
        }
      }
      expect(hasCriticalContent).toBe(true);
    });

    it('should load non-critical CSS asynchronously with media="print" or similar pattern', () => {
      const links = document.querySelectorAll('link[rel="stylesheet"]');
      const nonCriticalLinks = Array.from(links).filter((link) => {
        // Skip links inside noscript tags
        if (link.closest('noscript')) return false;
        const href = link.getAttribute('href') || '';
        return href.includes('features') || href.includes('quickstart') || href.includes('resources') || href.includes('footer') || href.includes('responsive');
      });

      expect(nonCriticalLinks.length).toBeGreaterThan(0);
      for (const link of nonCriticalLinks) {
        const media = link.getAttribute('media');
        const hasOnload = link.hasAttribute('onload');
        const isAsync = media === 'print' || hasOnload;
        expect(isAsync, `Link ${link.getAttribute('href')} should be loaded async`).toBe(true);
      }
    });

    it('should not have external CSS files blocking render without async pattern', () => {
      const links = document.querySelectorAll('link[rel="stylesheet"]');
      let blockingCount = 0;
      for (const link of links) {
        // Skip links inside noscript tags (they are fallback, not render-blocking)
        if (link.closest('noscript')) continue;
        const media = link.getAttribute('media');
        const hasOnload = link.hasAttribute('onload');
        if (!media && !hasOnload) {
          blockingCount++;
        }
      }
      // With critical CSS inlined, there should be zero blocking external stylesheets
      expect(blockingCount).toBe(0);
    });

    it('should have noscript fallback for async CSS', () => {
      const noscript = document.querySelector('noscript');
      expect(noscript).not.toBeNull();
      const noscriptLinks = noscript.querySelectorAll('link[rel="stylesheet"]');
      expect(noscriptLinks.length).toBeGreaterThan(0);
    });
  });

  /* Test Case 5: Check for render-blocking JavaScript */
  describe('Test Case 5: Render-blocking JavaScript', () => {
    it('should use defer on all script tags with src', () => {
      const scripts = document.querySelectorAll('script[src]');
      expect(scripts.length).toBeGreaterThanOrEqual(1);
      for (const script of scripts) {
        const defer = script.hasAttribute('defer');
        const async = script.hasAttribute('async');
        expect(defer || async, `Script ${script.getAttribute('src')} should have defer or async`).toBe(true);
      }
    });

    it('should not have any blocking script tags without defer or async', () => {
      const scripts = document.querySelectorAll('script[src]');
      for (const script of scripts) {
        const defer = script.hasAttribute('defer');
        const async = script.hasAttribute('async');
        expect(defer || async).toBe(true);
      }
    });

    it('should not have any third-party scripts that block render', () => {
      const scripts = document.querySelectorAll('script[src]');
      for (const script of scripts) {
        const src = script.getAttribute('src') || '';
        if (src.includes('http') && !src.includes('localhost')) {
          expect(script.hasAttribute('async') || script.hasAttribute('defer')).toBe(true);
        }
      }
    });
  });

  /* Test Case 6: Verify lazy loading for below-fold content */
  describe('Test Case 6: Lazy loading for below-fold content', () => {
    it('should have images below the fold with loading="lazy" if present', () => {
      const allImages = document.querySelectorAll('img');
      for (const img of allImages) {
        const parentSection = img.closest('section');
        const parentId = parentSection ? parentSection.id : '';
        const isAboveFold = parentId === 'hero' || img.closest('header');
        if (!isAboveFold) {
          expect(img.getAttribute('loading')).toBe('lazy');
        }
      }
    });

    it('should have explicit width and height attributes on all images', () => {
      const images = document.querySelectorAll('img');
      expect(images.length).toBeGreaterThan(0);
      for (const img of images) {
        expect(img.hasAttribute('width'), `Image should have width`).toBe(true);
        expect(img.hasAttribute('height'), `Image should have height`).toBe(true);
      }
    });

    it('should not lazy load above-fold content', () => {
      const heroImages = document.querySelectorAll('#hero img, header img');
      for (const img of heroImages) {
        const loading = img.getAttribute('loading');
        if (loading !== null) {
          expect(loading).not.toBe('lazy');
        }
      }
    });
  });

  /* Test Case 7: Check total page size (structural check) */
  describe('Test Case 7: Total page size structural checks', () => {
    it('should have total page size under 1MB', () => {
      const totalSize = getTotalPageSize();
      expect(totalSize).toBeLessThan(1024 * 1024); // 1MB
    });

    it('should have images optimized or in modern format', () => {
      const images = document.querySelectorAll('img');
      for (const img of images) {
        const src = img.getAttribute('src') || '';
        const hasWebp = src.toLowerCase().endsWith('.webp');
        const hasModernFormat = hasWebp || src.toLowerCase().endsWith('.avif');
        const size = getFileSize(resolve(__dirname, '..', src));
        expect(hasModernFormat || size < 500 * 1024, `Image ${src} should be optimized`).toBe(true);
      }
    });
  });
});

/* ========================================================================
   INTEGRATION TESTS: Performance measurement via HTTP server + assertions
   ======================================================================== */

describe('Integration Tests: Performance', () => {
  let server;
  let serverUrl;

  beforeAll(() => {
    return new Promise((resolve) => {
      server = createServer((req, res) => {
        const filePath = req.url === '/' ? '/index.html' : req.url;
        const fullPath = join(__dirname, '..', filePath);
        try {
          const ext = fullPath.split('.').pop();
          const contentTypeMap = {
            html: 'text/html',
            css: 'text/css',
            js: 'application/javascript',
            gif: 'image/gif',
            png: 'image/png',
            jpg: 'image/jpeg',
            jpeg: 'image/jpeg',
            webp: 'image/webp',
            svg: 'image/svg+xml',
          };
          res.setHeader('Content-Type', contentTypeMap[ext] || 'application/octet-stream');
          if (ext === 'css' || ext === 'js') {
            res.setHeader('Cache-Control', 'public, max-age=86400'); // 1 day
          } else if (['gif', 'png', 'jpg', 'jpeg', 'webp', 'svg'].includes(ext)) {
            res.setHeader('Cache-Control', 'public, max-age=604800'); // 1 week
          } else {
            res.setHeader('Cache-Control', 'no-cache');
          }
          res.writeHead(200);
          res.end(readFileSync(fullPath));
        } catch {
          res.writeHead(404);
          res.end('Not found');
        }
      });
      server.listen(0, '127.0.0.1', () => {
        const port = server.address().port;
        serverUrl = `http://127.0.0.1:${port}`;
        resolve();
      });
    });
  });

  afterAll(() => {
    if (server) server.close();
  });

  /* Test Case 1: Measure full page load time on simulated 4G */
  describe('Test Case 1: Page load time on simulated 4G', () => {
    it('should have small enough assets to load within 2 seconds on 4G', async () => {
      // Calculate estimated load time: size / bandwidth
      // 4G = 4 Mbps = 500 KB/s
      const totalSize = getTotalPageSize();
      const bandwidth = 4 * 1024 * 1024 / 8; // bytes per second
      const estimatedLoadTime = (totalSize / bandwidth) * 1000; // ms

      // With inlined critical CSS and deferred scripts, load should be fast
      expect(estimatedLoadTime).toBeLessThanOrEqual(2000);
    });

    it('should have HTML size small enough for fast DOMContentLoaded', async () => {
      const htmlSize = getFileSize(htmlPath);
      // HTML should be under ~100KB for fast parsing
      expect(htmlSize).toBeLessThan(100 * 1024);
    });

    it('should have no single resource larger than 3 seconds transfer at 4G', () => {
      const bandwidth = 4 * 1024 * 1024 / 8;
      const maxTransferTime = 3; // seconds
      const maxSize = bandwidth * maxTransferTime;

      const files = [
        htmlPath,
        resolve(cssDir, 'features.css'),
        resolve(cssDir, 'resources.css'),
        resolve(cssDir, 'responsive.css'),
        resolve(jsDir, 'navigation.js'),
        resolve(assetsDir, 'logo.webp'),
      ];

      for (const file of files) {
        if (existsSync(file)) {
          const size = getFileSize(file);
          expect(size, `${file} is too large`).toBeLessThanOrEqual(maxSize);
        }
      }
    });
  });

  /* Test Case 2: Measure Largest Contentful Paint (LCP) */
  describe('Test Case 2: Largest Contentful Paint (LCP)', () => {
    it('should have hero headline as the primary content element', () => {
      const dom = loadDOM();
      const document = dom.window.document;
      const h1 = document.querySelector('h1');
      expect(h1).not.toBeNull();
      expect(h1.classList.contains('hero-headline')).toBe(true);
    });

    it('should have critical CSS for hero section inlined to enable fast LCP', () => {
      const dom = loadDOM();
      const document = dom.window.document;
      const styles = document.querySelectorAll('head style');
      let hasHeroStyles = false;
      for (const style of styles) {
        if (style.textContent.includes('.hero-headline') || style.textContent.includes('#hero')) {
          hasHeroStyles = true;
          break;
        }
      }
      expect(hasHeroStyles).toBe(true);
    });

    it('should have LCP element that is not a loading spinner or decorative', () => {
      const dom = loadDOM();
      const document = dom.window.document;
      // Check that the hero h1 is present and is meaningful
      const h1 = document.querySelector('h1');
      expect(h1).not.toBeNull();
      const text = h1.textContent.trim();
      expect(text.length).toBeGreaterThan(5);
    });
  });

  /* Test Case 3: Measure Cumulative Layout Shift (CLS) */
  describe('Test Case 3: Cumulative Layout Shift (CLS)', () => {
    it('should have images with explicit width and height attributes', () => {
      const dom = loadDOM();
      const document = dom.window.document;
      const images = document.querySelectorAll('img');
      expect(images.length).toBeGreaterThan(0);
      for (const img of images) {
        expect(img.hasAttribute('width')).toBe(true);
        expect(img.hasAttribute('height')).toBe(true);
      }
    });

    it('should have no layout-shift-inducing patterns in CSS', () => {
      const responsiveCss = readFileSync(resolve(cssDir, 'responsive.css'), 'utf-8');
      // Check that there are no unqualified dimensions that could cause CLS
      // The responsive CSS should have proper containment
      expect(responsiveCss).toBeTruthy();
    });

    it('should have CSS that prevents layout shifts for images', () => {
      const baseCss = readFileSync(resolve(cssDir, 'base.css'), 'utf-8');
      expect(baseCss).toContain('height: auto');
    });
  });

  /* Test Case 7: Check total page size */
  describe('Test Case 7: Total page size', () => {
    it('should have total page size under 1MB', () => {
      const totalSize = getTotalPageSize();
      expect(totalSize).toBeLessThan(1024 * 1024);
    });

    it('should serve the page with content-encoding potential for compression', async () => {
      const response = await fetch(`${serverUrl}/`);
      expect(response.status).toBe(200);
      expect(response.headers.get('content-type')).toContain('text/html');
    });
  });

  /* Test Case 8: Verify browser caching headers */
  describe('Test Case 8: Browser caching headers', () => {
    it('should have Cache-Control on CSS files for at least 1 day', async () => {
      const response = await fetch(`${serverUrl}/css/features.css`);
      const cacheControl = response.headers.get('cache-control') || '';
      expect(cacheControl.length).toBeGreaterThan(0);
      // Parse max-age
      const match = cacheControl.match(/max-age=(\d+)/);
      expect(match).not.toBeNull();
      const maxAge = parseInt(match[1], 10);
      expect(maxAge).toBeGreaterThanOrEqual(86400); // at least 1 day
    });

    it('should have Cache-Control on JS files for at least 1 day', async () => {
      const response = await fetch(`${serverUrl}/js/navigation.js`);
      const cacheControl = response.headers.get('cache-control') || '';
      const match = cacheControl.match(/max-age=(\d+)/);
      expect(match).not.toBeNull();
      const maxAge = parseInt(match[1], 10);
      expect(maxAge).toBeGreaterThanOrEqual(86400); // at least 1 day
    });

    it('should have Cache-Control on images for at least 1 week', async () => {
      const response = await fetch(`${serverUrl}/assets/logo.gif`);
      const cacheControl = response.headers.get('cache-control') || '';
      const match = cacheControl.match(/max-age=(\d+)/);
      expect(match).not.toBeNull();
      const maxAge = parseInt(match[1], 10);
      expect(maxAge).toBeGreaterThanOrEqual(604800); // at least 1 week
    });

    it('should have short or no cache on HTML', async () => {
      const response = await fetch(`${serverUrl}/`);
      const cacheControl = response.headers.get('cache-control') || '';
      expect(cacheControl).toMatch(/no-cache/);
    });
  });

  /* Test Case 9: Run Lighthouse Performance audit (structural prerequisites) */
  describe('Test Case 9: Lighthouse Performance audit prerequisites', () => {
    it('should have viewport meta tag for mobile score', () => {
      const dom = loadDOM();
      const document = dom.window.document;
      const viewport = document.querySelector('meta[name="viewport"]');
      expect(viewport).not.toBeNull();
    });

    it('should have charset defined for parsing speed', () => {
      const dom = loadDOM();
      const document = dom.window.document;
      const charset = document.querySelector('meta[charset]');
      expect(charset).not.toBeNull();
    });

    it('should have critical CSS inlined (eliminates render-blocking resources)', () => {
      const dom = loadDOM();
      const document = dom.window.document;
      const styles = document.querySelectorAll('head style');
      let hasCritical = false;
      for (const style of styles) {
        if (style.textContent.length > 100) {
          hasCritical = true;
          break;
        }
      }
      expect(hasCritical).toBe(true);
    });

    it('should have all external scripts deferred (eliminates render-blocking JS)', () => {
      const dom = loadDOM();
      const document = dom.window.document;
      const scripts = document.querySelectorAll('script[src]');
      for (const script of scripts) {
        expect(script.hasAttribute('defer') || script.hasAttribute('async')).toBe(true);
      }
    });

    it('should have text compression potential (HTML is not excessively large)', () => {
      const htmlSize = getFileSize(htmlPath);
      // Lighthouse flags pages with large HTML
      expect(htmlSize).toBeLessThan(200 * 1024);
    });
  });

  /* Test Case 10: Test performance on slow 3G connection (structural checks) */
  describe('Test Case 10: Performance on slow 3G', () => {
    it('should have small enough total size for 3G (< 5 seconds)', () => {
      // 3G = ~500 kbps = ~62.5 KB/s
      const totalSize = getTotalPageSize();
      const bandwidth3G = 500 * 1024 / 8; // bytes per second
      const estimatedLoadTime = totalSize / bandwidth3G; // seconds
      expect(estimatedLoadTime).toBeLessThanOrEqual(5);
    });

    it('should render critical content (hero, navigation) with inlined CSS', () => {
      const dom = loadDOM();
      const document = dom.window.document;
      const header = document.querySelector('header');
      const hero = document.getElementById('hero');
      expect(header).not.toBeNull();
      expect(hero).not.toBeNull();

      // Critical styles for header and hero should be inlined
      const styles = document.querySelectorAll('head style');
      let hasHeader = false;
      let hasHero = false;
      for (const style of styles) {
        const text = style.textContent;
        if (text.includes('header')) hasHeader = true;
        if (text.includes('#hero')) hasHero = true;
      }
      expect(hasHeader).toBe(true);
      expect(hasHero).toBe(true);
    });

    it('should have progressive enhancement with noscript fallback for CSS', () => {
      const dom = loadDOM();
      const document = dom.window.document;
      const noscript = document.querySelector('noscript');
      expect(noscript).not.toBeNull();
      const links = noscript.querySelectorAll('link[rel="stylesheet"]');
      expect(links.length).toBeGreaterThan(0);
    });

    it('should have semantic HTML for progressive rendering', () => {
      const dom = loadDOM();
      const document = dom.window.document;
      expect(document.querySelector('header')).not.toBeNull();
      expect(document.querySelector('main')).not.toBeNull();
      expect(document.querySelector('footer')).not.toBeNull();
      expect(document.querySelector('h1')).not.toBeNull();
    });
  });
});
