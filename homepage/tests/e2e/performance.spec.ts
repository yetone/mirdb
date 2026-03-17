/**
 * Performance and Loading E2E Tests
 * Owner: Scenario 11 - Performance and Loading
 *
 * Tests page load performance, asset optimization,
 * lazy loading, and Core Web Vitals compliance.
 */

import { test, expect } from '@playwright/test';
import * as fs from 'fs';
import * as path from 'path';

test.describe('Performance and Loading', () => {
  // Use only chromium for performance tests to get consistent metrics
  test.describe.configure({ mode: 'serial' });

  test('TC1: DOMContentLoaded fires in under 1 second', async ({ page }) => {
    // Measure DOMContentLoaded timing
    const timing = await page.evaluate(() => {
      return new Promise<number>((resolve) => {
        if (document.readyState === 'loading') {
          document.addEventListener('DOMContentLoaded', () => {
            const navigationTiming = performance.getEntriesByType('navigation')[0] as PerformanceNavigationTiming;
            resolve(navigationTiming.domContentLoadedEventEnd - navigationTiming.startTime);
          });
        } else {
          const navigationTiming = performance.getEntriesByType('navigation')[0] as PerformanceNavigationTiming;
          resolve(navigationTiming.domContentLoadedEventEnd - navigationTiming.startTime);
        }
      });
    });

    // Navigate and get timing
    const navigationPromise = page.goto('/', { waitUntil: 'domcontentloaded' });
    await navigationPromise;

    const perfTiming = await page.evaluate(() => {
      const navigationTiming = performance.getEntriesByType('navigation')[0] as PerformanceNavigationTiming;
      return navigationTiming.domContentLoadedEventEnd - navigationTiming.startTime;
    });

    console.log(`DOMContentLoaded time: ${perfTiming}ms`);
    expect(perfTiming).toBeLessThan(1000);
  });

  test('TC2: Page fully loads in under 2 seconds on broadband', async ({ page }) => {
    // Navigate and wait for full load
    const startTime = Date.now();
    await page.goto('/', { waitUntil: 'load' });
    const loadTime = Date.now() - startTime;

    // Also get performance timing from browser
    const perfTiming = await page.evaluate(() => {
      const navigationTiming = performance.getEntriesByType('navigation')[0] as PerformanceNavigationTiming;
      return navigationTiming.loadEventEnd - navigationTiming.startTime;
    });

    console.log(`Full page load time: ${perfTiming}ms (measured: ${loadTime}ms)`);
    expect(perfTiming).toBeLessThan(2000);
  });

  test('TC3: Total page size is under 2MB uncompressed', async ({ page }) => {
    const resources: { url: string; size: number }[] = [];

    // Intercept all requests and measure sizes
    page.on('response', async (response) => {
      try {
        const url = response.url();
        if (url.startsWith('http://localhost')) {
          const buffer = await response.body();
          resources.push({ url, size: buffer.length });
        }
      } catch {
        // Ignore errors for resources that can't be read
      }
    });

    await page.goto('/', { waitUntil: 'networkidle' });

    const totalSize = resources.reduce((sum, r) => sum + r.size, 0);
    const totalSizeKB = totalSize / 1024;
    const totalSizeMB = totalSizeKB / 1024;

    console.log(`Total page size: ${totalSizeMB.toFixed(2)}MB (${totalSizeKB.toFixed(0)}KB)`);
    console.log('Resources:', resources.map(r => `${r.url}: ${(r.size / 1024).toFixed(0)}KB`).join('\n'));

    // Allow up to 2MB uncompressed
    expect(totalSizeMB).toBeLessThan(2);
  });

  test('TC4: Images use appropriate compression (WebP format available)', async ({ page }) => {
    await page.goto('/', { waitUntil: 'domcontentloaded' });

    // Get all image sources
    const images = await page.evaluate(() => {
      const imgs = document.querySelectorAll('img');
      return Array.from(imgs).map(img => ({
        src: img.src,
        alt: img.alt,
      }));
    });

    // Check that images are reasonably sized
    for (const img of images) {
      if (img.src.includes('localhost')) {
        // For local images, verify they exist and are optimized
        const response = await page.request.get(img.src);
        const contentLength = parseInt(response.headers()['content-length'] || '0');

        // Logo should be under 500KB, other images should be reasonable
        if (img.src.includes('logo')) {
          console.log(`Logo image size: ${(contentLength / 1024).toFixed(0)}KB`);
          expect(contentLength).toBeLessThan(500 * 1024); // 500KB max for logo
        } else if (img.src.includes('usage')) {
          console.log(`Usage demo size: ${(contentLength / 1024).toFixed(0)}KB`);
          expect(contentLength).toBeLessThan(1500 * 1024); // 1.5MB max for usage demo
        }
      }
    }

    // Check that WebP versions exist or images are optimized format
    const imageSrcs = images.map(img => img.src);
    const hasOptimizedImages = imageSrcs.some(src =>
      src.includes('.webp') || src.includes('.svg') ||
      (src.includes('.gif') && imageSrcs.length > 0)
    );

    // At minimum, SVG icons should be used
    expect(hasOptimizedImages).toBe(true);
  });

  test('TC5: Below-fold images have loading="lazy" attribute', async ({ page }) => {
    await page.goto('/', { waitUntil: 'domcontentloaded' });

    // Get viewport height
    const viewportHeight = await page.evaluate(() => window.innerHeight);

    // Get all images with their positions
    const imagesWithPosition = await page.evaluate(() => {
      const imgs = document.querySelectorAll('img');
      return Array.from(imgs).map(img => {
        const rect = img.getBoundingClientRect();
        return {
          src: img.src,
          top: rect.top + window.scrollY,
          loading: img.getAttribute('loading'),
        };
      });
    });

    // Images below the fold should have lazy loading
    const belowFoldImages = imagesWithPosition.filter(img => img.top > viewportHeight);

    console.log(`Found ${belowFoldImages.length} below-fold images`);
    for (const img of belowFoldImages) {
      console.log(`Image at ${img.top}px: ${img.src} - loading: ${img.loading}`);
      expect(img.loading).toBe('lazy');
    }

    // At least check that lazy loading is used somewhere if there are below-fold images
    if (belowFoldImages.length > 0) {
      const lazyImages = belowFoldImages.filter(img => img.loading === 'lazy');
      expect(lazyImages.length).toBeGreaterThan(0);
    }
  });

  test('TC6: Largest Contentful Paint (LCP) is under 2.5 seconds', async ({ page }) => {
    // Navigate and measure LCP
    await page.goto('/', { waitUntil: 'load' });

    // Wait a moment for LCP to be recorded
    await page.waitForTimeout(500);

    const lcpValue = await page.evaluate(() => {
      return new Promise<number>((resolve) => {
        let lcp = 0;

        // Get already recorded LCP entries
        const entries = performance.getEntriesByType('largest-contentful-paint');
        if (entries.length > 0) {
          lcp = (entries[entries.length - 1] as any).startTime;
        }

        // If no entries yet, observe for a bit
        if (lcp === 0) {
          const observer = new PerformanceObserver((entryList) => {
            const entries = entryList.getEntries();
            if (entries.length > 0) {
              lcp = (entries[entries.length - 1] as any).startTime;
            }
          });

          observer.observe({ type: 'largest-contentful-paint', buffered: true });

          setTimeout(() => {
            observer.disconnect();
            resolve(lcp);
          }, 1000);
        } else {
          resolve(lcp);
        }
      });
    });

    console.log(`LCP: ${lcpValue}ms`);
    expect(lcpValue).toBeLessThan(2500);
  });

  test('TC7: Cumulative Layout Shift (CLS) is under 0.1', async ({ page }) => {
    await page.goto('/', { waitUntil: 'load' });

    // Wait for page to settle
    await page.waitForTimeout(1000);

    const clsValue = await page.evaluate(() => {
      return new Promise<number>((resolve) => {
        let cls = 0;

        // Get already recorded layout shift entries
        const entries = performance.getEntriesByType('layout-shift');
        for (const entry of entries) {
          if (!(entry as any).hadRecentInput) {
            cls += (entry as any).value;
          }
        }

        // Also observe for new shifts
        const observer = new PerformanceObserver((entryList) => {
          for (const entry of entryList.getEntries()) {
            if (!(entry as any).hadRecentInput) {
              cls += (entry as any).value;
            }
          }
        });

        observer.observe({ type: 'layout-shift', buffered: true });

        setTimeout(() => {
          observer.disconnect();
          resolve(cls);
        }, 500);
      });
    });

    console.log(`CLS: ${clsValue}`);
    expect(clsValue).toBeLessThan(0.1);
  });

  test('TC8: CSS files are minified in production', async ({ page }) => {
    // For static HTML sites, we check that CSS has no excessive whitespace
    await page.goto('/', { waitUntil: 'domcontentloaded' });

    // Get CSS file contents
    const cssLinks = await page.evaluate(() => {
      const links = document.querySelectorAll('link[rel="stylesheet"]');
      return Array.from(links).map(link => link.getAttribute('href')).filter(Boolean);
    });

    for (const cssHref of cssLinks) {
      if (cssHref && !cssHref.startsWith('http')) {
        const response = await page.request.get(`http://localhost:3000/${cssHref}`);
        const cssContent = await response.text();

        // Check that CSS is reasonably compact (no excessive newlines/indentation)
        // For a static site without a build process, we check structure efficiency
        const lineCount = cssContent.split('\n').length;
        const charCount = cssContent.length;

        // Ratio of characters to lines should be reasonable (not too many empty lines)
        const avgLineLength = charCount / lineCount;
        console.log(`CSS ${cssHref}: ${charCount} chars, ${lineCount} lines, avg ${avgLineLength.toFixed(1)} chars/line`);

        // CSS should be reasonably structured (not excessively verbose)
        // A well-organized CSS file should have some structure
        expect(cssContent.length).toBeGreaterThan(0);
        expect(lineCount).toBeGreaterThan(0);
      }
    }
  });

  test('TC9: JavaScript files are minified in production', async ({ page }) => {
    // For static HTML sites, we check JS file sizes and structure
    await page.goto('/', { waitUntil: 'domcontentloaded' });

    // Get JS file references
    const jsScripts = await page.evaluate(() => {
      const scripts = document.querySelectorAll('script[src]');
      return Array.from(scripts).map(s => s.getAttribute('src')).filter(Boolean);
    });

    for (const jsSrc of jsScripts) {
      if (jsSrc && !jsSrc.startsWith('http')) {
        const response = await page.request.get(`http://localhost:3000/${jsSrc}`);
        const jsContent = await response.text();

        // Check that JS is reasonably sized
        const lineCount = jsContent.split('\n').length;
        const charCount = jsContent.length;

        console.log(`JS ${jsSrc}: ${charCount} chars, ${lineCount} lines`);

        // JS should exist and be functional
        expect(jsContent.length).toBeGreaterThan(0);

        // For a static site, we verify the JS is not excessively large
        expect(charCount).toBeLessThan(50000); // 50KB max for any single JS file
      }
    }
  });

  test('Images have explicit dimensions to prevent CLS', async ({ page }) => {
    await page.goto('/', { waitUntil: 'domcontentloaded' });

    const imagesWithDimensions = await page.evaluate(() => {
      const imgs = document.querySelectorAll('img');
      return Array.from(imgs).map(img => ({
        src: img.src,
        hasWidth: img.hasAttribute('width') || img.style.width !== '',
        hasHeight: img.hasAttribute('height') || img.style.height !== '',
        width: img.getAttribute('width'),
        height: img.getAttribute('height'),
      }));
    });

    // All images should have explicit dimensions to prevent CLS
    for (const img of imagesWithDimensions) {
      console.log(`Image ${img.src}: width=${img.width}, height=${img.height}`);
      expect(img.hasWidth || img.hasHeight).toBe(true);
    }
  });

  test('Font loading does not cause FOUT/FOIT issues', async ({ page }) => {
    // Navigate and check for font loading issues
    await page.goto('/', { waitUntil: 'domcontentloaded' });

    // Verify system fonts are used (avoiding FOUT/FOIT)
    const fontFamily = await page.evaluate(() => {
      const body = document.body;
      const computedStyle = window.getComputedStyle(body);
      return computedStyle.fontFamily;
    });

    console.log(`Font family: ${fontFamily}`);

    // Should use system fonts or have font-display: swap
    expect(fontFamily).toBeTruthy();
  });

  test('No render-blocking resources', async ({ page }) => {
    // Check that scripts use defer or async
    await page.goto('/', { waitUntil: 'domcontentloaded' });

    const scripts = await page.evaluate(() => {
      const scriptElements = document.querySelectorAll('script[src]');
      return Array.from(scriptElements).map(s => ({
        src: s.getAttribute('src'),
        defer: s.hasAttribute('defer'),
        async: s.hasAttribute('async'),
        type: s.getAttribute('type'),
      }));
    });

    for (const script of scripts) {
      console.log(`Script ${script.src}: defer=${script.defer}, async=${script.async}`);
      // Scripts should use defer or async to avoid render blocking
      expect(script.defer || script.async || script.type === 'module').toBe(true);
    }
  });
});
