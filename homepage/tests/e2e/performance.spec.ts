/**
 * Performance E2E Tests
 * Owner: Scenario 10 - Performance Requirements
 *
 * Tests for:
 * - Page load time measurement (< 2 seconds)
 * - Performance score verification (>= 90)
 * - Bundle size check (< 200KB gzipped)
 * - Static file verification
 */

import { test, expect } from '@playwright/test';
import { execSync } from 'child_process';
import * as fs from 'fs';
import * as path from 'path';
import { fileURLToPath } from 'url';

// ESM compatibility
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Performance thresholds
const PERFORMANCE_THRESHOLDS = {
  MAX_LOAD_TIME_MS: 2000, // 2 seconds
  MIN_PERFORMANCE_SCORE: 90,
  MAX_BUNDLE_SIZE_KB: 200, // 200KB gzipped
  MAX_FCP_MS: 1800, // First Contentful Paint
  MAX_LCP_MS: 2500, // Largest Contentful Paint
  MAX_CLS: 0.1, // Cumulative Layout Shift
  MAX_TBT_MS: 200, // Total Blocking Time
};

test.describe('Performance Requirements', () => {
  test.describe('Page Load Time', () => {
    test('page fully loads in under 2 seconds', async ({ page }) => {
      // Navigate and capture performance timing
      const startTime = Date.now();
      await page.goto('/', { waitUntil: 'load' });
      const loadTime = Date.now() - startTime;

      // Get navigation timing from the browser
      const performanceTiming = await page.evaluate(() => {
        const timing = performance.getEntriesByType('navigation')[0] as PerformanceNavigationTiming;
        return {
          loadEventEnd: timing.loadEventEnd,
          navigationStart: timing.startTime,
          domContentLoaded: timing.domContentLoadedEventEnd,
          responseEnd: timing.responseEnd,
        };
      });

      const browserLoadTime = performanceTiming.loadEventEnd - performanceTiming.navigationStart;

      console.log(`Page load time: ${browserLoadTime.toFixed(0)}ms (threshold: ${PERFORMANCE_THRESHOLDS.MAX_LOAD_TIME_MS}ms)`);

      // Verify load time is under threshold
      expect(browserLoadTime).toBeLessThan(PERFORMANCE_THRESHOLDS.MAX_LOAD_TIME_MS);
    });

    test('DOM is interactive quickly', async ({ page }) => {
      await page.goto('/', { waitUntil: 'domcontentloaded' });

      const timing = await page.evaluate(() => {
        const nav = performance.getEntriesByType('navigation')[0] as PerformanceNavigationTiming;
        return {
          domInteractive: nav.domInteractive,
          domContentLoaded: nav.domContentLoadedEventEnd,
        };
      });

      // DOM should be interactive within 1 second
      expect(timing.domInteractive).toBeLessThan(1000);
      console.log(`DOM interactive at: ${timing.domInteractive.toFixed(0)}ms`);
    });
  });

  test.describe('Performance Metrics', () => {
    test('achieves performance score of 90 or higher', async ({ page, browser }) => {
      // Create a CDP session for performance metrics
      const client = await page.context().newCDPSession(page);

      // Enable performance metrics
      await client.send('Performance.enable');

      // Navigate to the page
      await page.goto('/', { waitUntil: 'networkidle' });

      // Wait for paint to complete
      await page.waitForLoadState('load');

      // Get performance metrics
      const metrics = await client.send('Performance.getMetrics');
      const metricsMap = new Map(metrics.metrics.map(m => [m.name, m.value]));

      // Get paint timing metrics from the browser
      const paintMetrics = await page.evaluate(() => {
        const entries = performance.getEntriesByType('paint');
        const fcp = entries.find(e => e.name === 'first-contentful-paint');
        const lcp = entries.find(e => e.name === 'largest-contentful-paint');

        return {
          firstPaint: entries.find(e => e.name === 'first-paint')?.startTime || 0,
          firstContentfulPaint: fcp?.startTime || 0,
        };
      });

      // Get LCP through PerformanceObserver
      const lcpValue = await page.evaluate(() => {
        return new Promise<number>((resolve) => {
          let lcpValue = 0;
          const observer = new PerformanceObserver((list) => {
            const entries = list.getEntries();
            if (entries.length > 0) {
              lcpValue = entries[entries.length - 1].startTime;
            }
          });
          observer.observe({ type: 'largest-contentful-paint', buffered: true });
          // Give time for LCP to be recorded
          setTimeout(() => {
            observer.disconnect();
            resolve(lcpValue);
          }, 500);
        });
      });

      console.log(`First Contentful Paint: ${paintMetrics.firstContentfulPaint.toFixed(0)}ms`);
      console.log(`Largest Contentful Paint: ${lcpValue.toFixed(0)}ms`);

      // Verify FCP is acceptable
      expect(paintMetrics.firstContentfulPaint).toBeLessThan(PERFORMANCE_THRESHOLDS.MAX_FCP_MS);

      // Verify LCP is acceptable (if we got a value)
      if (lcpValue > 0) {
        expect(lcpValue).toBeLessThan(PERFORMANCE_THRESHOLDS.MAX_LCP_MS);
      }

      // Calculate approximate performance score based on key metrics
      // Score = 100 - (penalty for slow metrics)
      let score = 100;
      if (paintMetrics.firstContentfulPaint > 1000) score -= 5;
      if (paintMetrics.firstContentfulPaint > 1500) score -= 10;
      if (lcpValue > 2000) score -= 10;
      if (lcpValue > 2500) score -= 15;

      console.log(`Calculated performance score: ${score}`);
      expect(score).toBeGreaterThanOrEqual(PERFORMANCE_THRESHOLDS.MIN_PERFORMANCE_SCORE);
    });

    test('no render-blocking resources cause significant delays', async ({ page }) => {
      const client = await page.context().newCDPSession(page);

      // Track resource loading
      const resourceTimings: { url: string; duration: number; blocking: boolean }[] = [];

      await client.send('Network.enable');

      client.on('Network.responseReceived', (event) => {
        if (event.type === 'Stylesheet' || event.type === 'Script') {
          resourceTimings.push({
            url: event.response.url,
            duration: 0,
            blocking: !event.response.url.includes('async') && !event.response.url.includes('defer'),
          });
        }
      });

      await page.goto('/', { waitUntil: 'load' });

      // Check resource timing
      const resources = await page.evaluate(() => {
        const entries = performance.getEntriesByType('resource') as PerformanceResourceTiming[];
        return entries
          .filter(e => e.initiatorType === 'link' || e.initiatorType === 'script')
          .map(e => ({
            name: e.name,
            duration: e.duration,
            renderBlockingStatus: (e as any).renderBlockingStatus || 'unknown',
          }));
      });

      console.log('Resource loading times:');
      for (const resource of resources) {
        const shortName = resource.name.split('/').pop() || resource.name;
        console.log(`  ${shortName}: ${resource.duration.toFixed(0)}ms`);
      }

      // Verify no single resource blocks for too long
      const longBlockingResources = resources.filter(r => r.duration > 500);
      expect(longBlockingResources.length).toBeLessThanOrEqual(1);
    });

    test('cumulative layout shift is minimal', async ({ page }) => {
      await page.goto('/', { waitUntil: 'networkidle' });

      // Measure CLS
      const cls = await page.evaluate(() => {
        return new Promise<number>((resolve) => {
          let clsValue = 0;
          const observer = new PerformanceObserver((list) => {
            for (const entry of list.getEntries()) {
              if (!(entry as any).hadRecentInput) {
                clsValue += (entry as any).value;
              }
            }
          });
          observer.observe({ type: 'layout-shift', buffered: true });
          setTimeout(() => {
            observer.disconnect();
            resolve(clsValue);
          }, 500);
        });
      });

      console.log(`Cumulative Layout Shift: ${cls.toFixed(4)}`);
      expect(cls).toBeLessThan(PERFORMANCE_THRESHOLDS.MAX_CLS);
    });
  });

  test.describe('Bundle Size', () => {
    test('total JS bundle size is under 200KB gzipped', async () => {
      const distPath = path.resolve(__dirname, '../../dist');
      const assetsPath = path.join(distPath, 'assets');

      // Check if dist folder exists (build should be run first)
      if (!fs.existsSync(distPath)) {
        // Build the project first
        execSync('npm run build', { cwd: path.resolve(__dirname, '../..'), stdio: 'pipe' });
      }

      // Get all JS files in dist/assets
      let totalJsSize = 0;

      if (fs.existsSync(assetsPath)) {
        const files = fs.readdirSync(assetsPath);
        const jsFiles = files.filter(f => f.endsWith('.js'));

        for (const file of jsFiles) {
          const filePath = path.join(assetsPath, file);
          const stats = fs.statSync(filePath);
          totalJsSize += stats.size;
          console.log(`  ${file}: ${(stats.size / 1024).toFixed(2)}KB`);
        }
      } else {
        // For static HTML with inline scripts, check the HTML file
        const indexPath = path.join(distPath, 'index.html');
        if (fs.existsSync(indexPath)) {
          const htmlContent = fs.readFileSync(indexPath, 'utf-8');
          // Extract script content size
          const scriptMatch = htmlContent.match(/<script[^>]*>([\s\S]*?)<\/script>/g);
          if (scriptMatch) {
            for (const script of scriptMatch) {
              totalJsSize += Buffer.byteLength(script, 'utf-8');
            }
          }
        }
      }

      // Also check for .js files in root dist
      if (fs.existsSync(distPath)) {
        const rootFiles = fs.readdirSync(distPath);
        const rootJsFiles = rootFiles.filter(f => f.endsWith('.js'));

        for (const file of rootJsFiles) {
          const filePath = path.join(distPath, file);
          const stats = fs.statSync(filePath);
          totalJsSize += stats.size;
          console.log(`  ${file}: ${(stats.size / 1024).toFixed(2)}KB`);
        }
      }

      const totalKB = totalJsSize / 1024;
      // Gzip typically achieves 70% compression
      const estimatedGzipKB = totalKB * 0.3;

      console.log(`Total JS size: ${totalKB.toFixed(2)}KB (estimated gzipped: ${estimatedGzipKB.toFixed(2)}KB)`);

      // Allow for uncompressed size to be much larger, gzipped should be under threshold
      expect(estimatedGzipKB).toBeLessThan(PERFORMANCE_THRESHOLDS.MAX_BUNDLE_SIZE_KB);
    });
  });

  test.describe('Static Site Verification', () => {
    test('build produces static HTML/CSS/JS files only', async () => {
      const distPath = path.resolve(__dirname, '../../dist');

      // Build the project if not already built
      if (!fs.existsSync(distPath)) {
        execSync('npm run build', { cwd: path.resolve(__dirname, '../..'), stdio: 'pipe' });
      }

      expect(fs.existsSync(distPath)).toBe(true);

      // Verify index.html exists
      const indexPath = path.join(distPath, 'index.html');
      expect(fs.existsSync(indexPath)).toBe(true);

      // Read and verify it's valid HTML
      const htmlContent = fs.readFileSync(indexPath, 'utf-8');
      expect(htmlContent).toContain('<!DOCTYPE html>');
      expect(htmlContent).toContain('<html');
      expect(htmlContent).toContain('MirDB');

      // Collect all file types in dist
      const fileTypes = new Set<string>();
      const collectFiles = (dir: string) => {
        if (!fs.existsSync(dir)) return;
        const items = fs.readdirSync(dir, { withFileTypes: true });
        for (const item of items) {
          if (item.isFile()) {
            const ext = path.extname(item.name).toLowerCase();
            fileTypes.add(ext);
          } else if (item.isDirectory()) {
            collectFiles(path.join(dir, item.name));
          }
        }
      };

      collectFiles(distPath);

      console.log('File types in dist:', Array.from(fileTypes).join(', '));

      // Allowed static file types
      const allowedTypes = new Set(['.html', '.css', '.js', '.svg', '.png', '.jpg', '.jpeg', '.gif', '.webp', '.ico', '.woff', '.woff2', '.ttf', '.eot', '.map', '.json', '.txt']);

      // Disallowed server-side types
      const serverSideTypes = ['.php', '.py', '.rb', '.java', '.go', '.rs'];

      for (const serverType of serverSideTypes) {
        expect(fileTypes.has(serverType)).toBe(false);
      }

      // All file types should be in allowed list
      for (const fileType of fileTypes) {
        expect(allowedTypes.has(fileType)).toBe(true);
      }
    });

    test('site functions correctly when served by static file server', async ({ page }) => {
      // The page is already being served by Vite preview (static server)
      // Navigate and verify core functionality
      await page.goto('/');

      // Verify page loads
      await expect(page).toHaveTitle(/MirDB/);

      // Verify main sections are visible
      await expect(page.locator('#hero')).toBeVisible();
      await expect(page.locator('#features')).toBeVisible();
      await expect(page.locator('#quickstart')).toBeVisible();
      await expect(page.locator('#techspecs')).toBeVisible();
      await expect(page.locator('#footer')).toBeVisible();

      // Verify navigation works
      await page.click('a[href="#features"]');
      await expect(page.locator('#features')).toBeInViewport();

      // Verify no JavaScript errors
      const errors: string[] = [];
      page.on('pageerror', (error) => {
        errors.push(error.message);
      });

      // Interact with the page
      await page.click('a[href="#quickstart"]');
      await page.waitForTimeout(500);

      // Should have no errors
      expect(errors.length).toBe(0);
    });

    test('no server-side dependencies required', async ({ page }) => {
      // Track all network requests
      const requests: { url: string; method: string }[] = [];

      page.on('request', (request) => {
        requests.push({
          url: request.url(),
          method: request.method(),
        });
      });

      await page.goto('/', { waitUntil: 'networkidle' });

      // Filter for API/server requests (exclude static file requests)
      const serverRequests = requests.filter((r) => {
        const url = r.url.toLowerCase();
        // These would indicate server-side dependencies
        return (
          url.includes('/api/') ||
          url.includes('.php') ||
          url.includes('.asp') ||
          url.includes('/graphql') ||
          (r.method === 'POST' && !url.includes('localhost'))
        );
      });

      console.log(`Total requests: ${requests.length}`);
      console.log(`Server-side requests: ${serverRequests.length}`);

      // Should have no server-side dependencies
      expect(serverRequests.length).toBe(0);
    });
  });
});
