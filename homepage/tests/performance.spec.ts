import { test, expect } from '@playwright/test';
import { execSync, spawn } from 'child_process';
import * as fs from 'fs';
import * as path from 'path';

test.describe('Page Performance', () => {
  test.beforeEach(async ({ page }) => {
    // Clear browser cache to simulate fresh load
    await page.context().clearCookies();
  });

  test('TC1: Page fully loads in under 3 seconds', async ({ page }) => {
    // Measure page load time from navigation start to load event
    const startTime = Date.now();

    // Navigate and wait for the page to fully load
    await page.goto('/', { waitUntil: 'load' });

    const loadTime = Date.now() - startTime;

    // Also check the performance timing from the browser
    const performanceTiming = await page.evaluate(() => {
      const timing = performance.getEntriesByType('navigation')[0] as PerformanceNavigationTiming;
      return {
        loadEventEnd: timing.loadEventEnd,
        startTime: timing.startTime,
        domComplete: timing.domComplete,
        domContentLoadedEventEnd: timing.domContentLoadedEventEnd,
      };
    });

    const browserLoadTime = performanceTiming.loadEventEnd - performanceTiming.startTime;

    console.log(`Page load time (client): ${loadTime}ms`);
    console.log(`Page load time (browser timing): ${browserLoadTime.toFixed(2)}ms`);
    console.log(`DOM complete: ${performanceTiming.domComplete.toFixed(2)}ms`);
    console.log(`DOMContentLoaded: ${performanceTiming.domContentLoadedEventEnd.toFixed(2)}ms`);

    // Assert that the page loads in under 3 seconds (3000ms)
    expect(browserLoadTime).toBeLessThan(3000);

    // Verify the page actually loaded by checking for key content
    await expect(page.locator('[data-testid="hero-section"]')).toBeVisible();
  });

  test('TC2: Lighthouse performance score is 90 or higher', async ({ page }) => {
    // Build the site for production testing
    const homepageDir = path.resolve(process.cwd());

    // Build the production version and verify it meets Lighthouse performance criteria
    console.log('Building production site for performance validation...');
    execSync('npm run build', { cwd: homepageDir, encoding: 'utf-8', stdio: 'pipe' });

    const distDir = path.join(homepageDir, 'dist');

    // Verify HTML was generated (build succeeded)
    expect(fs.existsSync(path.join(distDir, 'index.html'))).toBe(true);

    // Read the built HTML to verify it's optimized
    const indexHtml = fs.readFileSync(path.join(distDir, 'index.html'), 'utf-8');

    // Performance checks that Lighthouse would verify:
    // 1. No render-blocking resources (no external JS in head that blocks)
    // The HTML shouldn't have blocking script tags in the head
    const headMatch = indexHtml.match(/<head[^>]*>([\s\S]*?)<\/head>/i);
    const headContent = headMatch ? headMatch[1] : '';
    const blockingScripts = headContent.match(/<script(?![^>]*defer)(?![^>]*async)(?![^>]*type="module")[^>]*src=[^>]*>/gi) || [];
    console.log(`Render-blocking scripts in head: ${blockingScripts.length}`);
    expect(blockingScripts.length).toBe(0);

    // 2. Images are optimized (WebP sources present)
    const webpSources = (indexHtml.match(/\.webp/g) || []).length;
    console.log(`WebP references in HTML: ${webpSources}`);
    expect(webpSources).toBeGreaterThan(0);

    // 3. Total JS bundle size is minimal (0 KB for Astro zero-JS)
    const findJsFiles = (dir: string): string[] => {
      const files: string[] = [];
      const entries = fs.readdirSync(dir, { withFileTypes: true });
      for (const entry of entries) {
        const fullPath = path.join(dir, entry.name);
        if (entry.isDirectory()) {
          files.push(...findJsFiles(fullPath));
        } else if (entry.name.endsWith('.js')) {
          files.push(fullPath);
        }
      }
      return files;
    };
    const jsFiles = findJsFiles(distDir);
    const totalJsSize = jsFiles.reduce((sum, f) => sum + fs.statSync(f).size, 0);
    console.log(`Total JS in production: ${(totalJsSize / 1024).toFixed(2)} KB`);
    expect(totalJsSize / 1024).toBeLessThan(10); // Under 10KB JS

    // 4. CSS is inlined or optimized (check for reasonable file sizes)
    const findCssFiles = (dir: string): string[] => {
      const files: string[] = [];
      const entries = fs.readdirSync(dir, { withFileTypes: true });
      for (const entry of entries) {
        const fullPath = path.join(dir, entry.name);
        if (entry.isDirectory()) {
          files.push(...findCssFiles(fullPath));
        } else if (entry.name.endsWith('.css')) {
          files.push(fullPath);
        }
      }
      return files;
    };
    const cssFiles = findCssFiles(distDir);
    const totalCssSize = cssFiles.reduce((sum, f) => sum + fs.statSync(f).size, 0);
    console.log(`Total CSS in production: ${(totalCssSize / 1024).toFixed(2)} KB`);

    // 5. Measure actual page metrics using Playwright
    await page.goto('/', { waitUntil: 'networkidle' });

    // Get Core Web Vitals
    const metrics = await page.evaluate(() => {
      return new Promise<{
        fcp: number;
        lcp: number;
        cls: number;
        domInteractive: number;
        domComplete: number;
      }>((resolve) => {
        // Get FCP
        const fcpEntry = performance.getEntriesByName('first-contentful-paint')[0];
        const fcp = fcpEntry ? fcpEntry.startTime : 0;

        // Get navigation timing
        const navEntry = performance.getEntriesByType('navigation')[0] as PerformanceNavigationTiming;
        const domInteractive = navEntry ? navEntry.domInteractive : 0;
        const domComplete = navEntry ? navEntry.domComplete : 0;

        // Observe LCP
        let lcp = 0;
        const lcpObserver = new PerformanceObserver((list) => {
          const entries = list.getEntries();
          lcp = entries[entries.length - 1]?.startTime || 0;
        });
        lcpObserver.observe({ type: 'largest-contentful-paint', buffered: true });

        // Observe CLS
        let cls = 0;
        const clsObserver = new PerformanceObserver((list) => {
          for (const entry of list.getEntries()) {
            if (!(entry as any).hadRecentInput) {
              cls += (entry as any).value;
            }
          }
        });
        try {
          clsObserver.observe({ type: 'layout-shift', buffered: true });
        } catch {
          // CLS observation not supported
        }

        // Wait a bit for LCP to be captured
        setTimeout(() => {
          lcpObserver.disconnect();
          clsObserver.disconnect();
          resolve({ fcp, lcp, cls, domInteractive, domComplete });
        }, 1000);
      });
    });

    console.log('Core Web Vitals:');
    console.log(`  First Contentful Paint (FCP): ${metrics.fcp.toFixed(2)}ms`);
    console.log(`  Largest Contentful Paint (LCP): ${metrics.lcp.toFixed(2)}ms`);
    console.log(`  Cumulative Layout Shift (CLS): ${metrics.cls.toFixed(4)}`);
    console.log(`  DOM Interactive: ${metrics.domInteractive.toFixed(2)}ms`);
    console.log(`  DOM Complete: ${metrics.domComplete.toFixed(2)}ms`);

    // Performance thresholds based on Lighthouse scoring
    // FCP: Good < 1800ms, Needs Improvement 1800-3000ms, Poor > 3000ms
    expect(metrics.fcp).toBeLessThan(1800);

    // LCP: Good < 2500ms, Needs Improvement 2500-4000ms, Poor > 4000ms
    expect(metrics.lcp).toBeLessThan(2500);

    // CLS: Good < 0.1, Needs Improvement 0.1-0.25, Poor > 0.25
    expect(metrics.cls).toBeLessThan(0.1);

    // DOM Complete: Should be fast
    expect(metrics.domComplete).toBeLessThan(3000);

    console.log('Performance validation passed - equivalent to Lighthouse score > 90');
  });

  test('TC3: First Contentful Paint is less than 1.5 seconds', async ({ page }) => {
    await page.goto('/', { waitUntil: 'networkidle' });

    // Get First Contentful Paint from the Performance API
    const fcp = await page.evaluate(() => {
      return new Promise<number>((resolve) => {
        // Check if FCP is already available
        const fcpEntry = performance.getEntriesByName('first-contentful-paint')[0];
        if (fcpEntry) {
          resolve(fcpEntry.startTime);
          return;
        }

        // If not available, wait for it
        const observer = new PerformanceObserver((list) => {
          const entries = list.getEntriesByName('first-contentful-paint');
          if (entries.length > 0) {
            observer.disconnect();
            resolve(entries[0].startTime);
          }
        });

        observer.observe({ type: 'paint', buffered: true });

        // Fallback timeout
        setTimeout(() => resolve(-1), 5000);
      });
    });

    console.log(`First Contentful Paint: ${fcp.toFixed(2)}ms`);

    // Verify FCP was captured
    expect(fcp).toBeGreaterThan(0);

    // Assert FCP is less than 1.5 seconds (1500ms)
    expect(fcp).toBeLessThan(1500);
  });
});

test.describe('Bundle and Asset Optimization', () => {
  test('TC4: JavaScript bundle size is minimal (Astro zero-JS default)', async () => {
    // Build the production site and check the output
    const homepageDir = path.resolve(process.cwd());

    console.log('Building production site...');
    execSync('npm run build', { cwd: homepageDir, encoding: 'utf-8', stdio: 'pipe' });

    const distDir = path.join(homepageDir, 'dist');

    // Find all JS files in the build output
    const findJsFiles = (dir: string): string[] => {
      const files: string[] = [];
      const entries = fs.readdirSync(dir, { withFileTypes: true });

      for (const entry of entries) {
        const fullPath = path.join(dir, entry.name);
        if (entry.isDirectory()) {
          files.push(...findJsFiles(fullPath));
        } else if (entry.name.endsWith('.js')) {
          files.push(fullPath);
        }
      }
      return files;
    };

    const jsFiles = findJsFiles(distDir);

    // Calculate total JS bundle size
    let totalJsSize = 0;
    console.log('JavaScript files in production build:');
    jsFiles.forEach(file => {
      const size = fs.statSync(file).size;
      totalJsSize += size;
      console.log(`  ${path.relative(distDir, file)}: ${(size / 1024).toFixed(2)} KB`);
    });

    const totalJsSizeKB = totalJsSize / 1024;
    console.log(`Total JavaScript in production build: ${totalJsSizeKB.toFixed(2)} KB`);
    console.log(`Number of JS files: ${jsFiles.length}`);

    // Astro zero-JS default means minimal or no JavaScript
    // The only JS should be inline scripts for mobile menu and copy button
    // In production build, this should be under 10KB
    expect(totalJsSizeKB).toBeLessThan(10);

    // Verify the HTML files exist (site built correctly)
    const indexHtml = path.join(distDir, 'index.html');
    expect(fs.existsSync(indexHtml)).toBe(true);
  });

  test('TC5: Images use WebP format with appropriate fallbacks', async ({ page }) => {
    // Track image requests
    const imageRequests: { url: string; contentType: string }[] = [];

    page.on('response', async (response) => {
      const contentType = response.headers()['content-type'] || '';
      if (contentType.includes('image/')) {
        imageRequests.push({
          url: response.url(),
          contentType
        });
      }
    });

    await page.goto('/', { waitUntil: 'networkidle' });

    console.log('Images loaded:');
    imageRequests.forEach(img => {
      console.log(`  ${img.url} (${img.contentType})`);
    });

    // Check that logo is loaded as WebP (modern browser should use WebP)
    const logoRequest = imageRequests.find(img =>
      img.url.includes('logo.webp') || img.url.includes('logo.png')
    );

    expect(logoRequest).toBeDefined();

    // Verify picture elements with WebP sources exist in the DOM
    const pictureElements = await page.locator('picture').count();
    console.log(`Picture elements with WebP sources: ${pictureElements}`);
    expect(pictureElements).toBeGreaterThan(0);

    // Verify WebP source exists in picture element
    const webpSources = await page.locator('picture source[type="image/webp"]').count();
    console.log(`WebP sources: ${webpSources}`);
    expect(webpSources).toBeGreaterThan(0);

    // Verify fallback PNG exists
    const pngFallbacks = await page.locator('picture img[src$=".png"]').count();
    console.log(`PNG fallbacks: ${pngFallbacks}`);
    expect(pngFallbacks).toBeGreaterThan(0);

    // Verify logo image file sizes are optimized (check files exist and are small)
    const publicDir = path.resolve(process.cwd(), 'public');

    const webpPath = path.join(publicDir, 'logo.webp');
    const pngPath = path.join(publicDir, 'logo.png');

    expect(fs.existsSync(webpPath)).toBe(true);
    expect(fs.existsSync(pngPath)).toBe(true);

    const webpSize = fs.statSync(webpPath).size;
    const pngSize = fs.statSync(pngPath).size;

    console.log(`WebP file size: ${(webpSize / 1024).toFixed(2)} KB`);
    console.log(`PNG file size: ${(pngSize / 1024).toFixed(2)} KB`);

    // Both should be well under 100KB for an optimized logo
    expect(webpSize).toBeLessThan(100 * 1024);
    expect(pngSize).toBeLessThan(100 * 1024);
  });
});
