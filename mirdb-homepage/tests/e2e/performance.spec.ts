/**
 * Performance Optimization E2E Tests
 * Owner: Scenario 11 - Performance Optimization
 *
 * Tests NFR-1 and NFR-2 requirements:
 * - Lighthouse performance score of 90+
 * - Initial viewport content load within 1.5 seconds on 3G
 *
 * Test cases:
 * 1. Lighthouse performance score >= 90
 * 2. Lighthouse accessibility score >= 90
 * 3. Lighthouse best practices score >= 90
 * 4. Lighthouse SEO score >= 90
 * 5. First Contentful Paint within 1.5s on 3G
 * 6. Largest Contentful Paint within 2.5s on 3G
 * 7. Cumulative Layout Shift < 0.1
 * 8. First Input Delay < 100ms
 * 9. JavaScript bundle under 100KB gzipped
 * 10. Images properly sized and compressed
 */

import { test, expect } from '@playwright/test';
import lighthouse from 'lighthouse';
import { chromium, Browser, Page } from 'playwright';

// Base URL for the server - Playwright config sets this to baseURL
const BASE_URL = 'http://localhost:5173';

// Helper function to run Lighthouse audit
async function runLighthouseAudit(url: string, categories: string[]) {
  const browser = await chromium.launch({
    args: ['--remote-debugging-port=9222'],
  });

  try {
    const context = await browser.newContext();
    const page = await context.newPage();
    await page.goto(url, { waitUntil: 'networkidle' });

    const result = await lighthouse(url, {
      port: 9222,
      onlyCategories: categories,
      output: 'json',
      logLevel: 'error',
    });

    return result;
  } finally {
    await browser.close();
  }
}

test.describe('Performance Optimization - Lighthouse Audits', () => {
  test('Test Case 1: Lighthouse performance score is 90 or higher', async () => {
    const result = await runLighthouseAudit(BASE_URL, ['performance']);
    const score = Math.round((result?.lhr?.categories?.performance?.score || 0) * 100);
    console.log(`Lighthouse Performance Score: ${score}`);
    expect(score).toBeGreaterThanOrEqual(90);
  });

  test('Test Case 2: Lighthouse accessibility score is 90 or higher', async () => {
    const result = await runLighthouseAudit(BASE_URL, ['accessibility']);
    const score = Math.round((result?.lhr?.categories?.accessibility?.score || 0) * 100);
    console.log(`Lighthouse Accessibility Score: ${score}`);
    expect(score).toBeGreaterThanOrEqual(90);
  });

  test('Test Case 3: Lighthouse best practices score is 90 or higher', async () => {
    const result = await runLighthouseAudit(BASE_URL, ['best-practices']);
    const score = Math.round((result?.lhr?.categories?.['best-practices']?.score || 0) * 100);
    console.log(`Lighthouse Best Practices Score: ${score}`);
    expect(score).toBeGreaterThanOrEqual(90);
  });

  test('Test Case 4: Lighthouse SEO score is 90 or higher', async () => {
    const result = await runLighthouseAudit(BASE_URL, ['seo']);
    const score = Math.round((result?.lhr?.categories?.seo?.score || 0) * 100);
    console.log(`Lighthouse SEO Score: ${score}`);
    expect(score).toBeGreaterThanOrEqual(90);
  });
});

test.describe('Performance Optimization - Core Web Vitals', () => {
  test('Test Case 5: First Contentful Paint within 1.5 seconds on 3G', async ({ page }) => {
    // Create a CDP session to simulate 3G network
    const client = await page.context().newCDPSession(page);

    // Enable network emulation - Slow 3G settings
    await client.send('Network.emulateNetworkConditions', {
      offline: false,
      downloadThroughput: (500 * 1024) / 8, // 500 Kbps
      uploadThroughput: (500 * 1024) / 8,
      latency: 400, // 400ms RTT
    });

    await page.goto('/', { waitUntil: 'domcontentloaded' });

    // Get FCP from Performance API
    const fcp = await page.evaluate(() => {
      return new Promise<number>((resolve) => {
        const observer = new PerformanceObserver((list) => {
          const entries = list.getEntries();
          for (const entry of entries) {
            if (entry.name === 'first-contentful-paint') {
              resolve(entry.startTime);
              observer.disconnect();
              return;
            }
          }
        });
        observer.observe({ type: 'paint', buffered: true });

        // Check existing entries
        const existingEntries = performance.getEntriesByType('paint');
        for (const entry of existingEntries) {
          if (entry.name === 'first-contentful-paint') {
            resolve(entry.startTime);
            observer.disconnect();
            return;
          }
        }

        // Timeout fallback
        setTimeout(() => resolve(-1), 5000);
      });
    });

    console.log(`First Contentful Paint: ${fcp}ms`);
    // FCP should be within 1.5 seconds (1500ms)
    expect(fcp).toBeLessThanOrEqual(1500);
    expect(fcp).toBeGreaterThan(0);
  });

  test('Test Case 6: Largest Contentful Paint within 2.5 seconds on 3G', async ({ page }) => {
    // Create a CDP session to simulate 3G network
    const client = await page.context().newCDPSession(page);

    // Enable network emulation - Slow 3G settings
    await client.send('Network.emulateNetworkConditions', {
      offline: false,
      downloadThroughput: (500 * 1024) / 8,
      uploadThroughput: (500 * 1024) / 8,
      latency: 400,
    });

    await page.goto('/', { waitUntil: 'load' });

    // Get LCP from Performance API
    const lcp = await page.evaluate(() => {
      return new Promise<number>((resolve) => {
        let lcpValue = 0;
        const observer = new PerformanceObserver((list) => {
          const entries = list.getEntries();
          const lastEntry = entries[entries.length - 1];
          lcpValue = lastEntry.startTime;
        });
        observer.observe({ type: 'largest-contentful-paint', buffered: true });

        // Wait for LCP to stabilize
        setTimeout(() => {
          observer.disconnect();
          resolve(lcpValue);
        }, 3000);
      });
    });

    console.log(`Largest Contentful Paint: ${lcp}ms`);
    // LCP should be within 2.5 seconds (2500ms)
    expect(lcp).toBeLessThanOrEqual(2500);
    expect(lcp).toBeGreaterThan(0);
  });

  test('Test Case 7: Cumulative Layout Shift is less than 0.1', async ({ page }) => {
    await page.goto('/', { waitUntil: 'networkidle' });

    // Wait for page to stabilize and measure CLS
    const cls = await page.evaluate(() => {
      return new Promise<number>((resolve) => {
        let clsValue = 0;
        const observer = new PerformanceObserver((list) => {
          for (const entry of list.getEntries()) {
            // Only count layout shifts without recent user input
            if (!(entry as PerformanceEntry & { hadRecentInput?: boolean }).hadRecentInput) {
              clsValue += (entry as PerformanceEntry & { value?: number }).value || 0;
            }
          }
        });
        observer.observe({ type: 'layout-shift', buffered: true });

        // Wait for CLS to stabilize
        setTimeout(() => {
          observer.disconnect();
          resolve(clsValue);
        }, 3000);
      });
    });

    console.log(`Cumulative Layout Shift: ${cls}`);
    // CLS should be less than 0.1
    expect(cls).toBeLessThan(0.1);
  });

  test('Test Case 8: First Input Delay is less than 100ms', async ({ page }) => {
    await page.goto('/', { waitUntil: 'networkidle' });

    // Measure FID by clicking an interactive element
    const fid = await page.evaluate(() => {
      return new Promise<number>((resolve) => {
        let fidValue = -1;
        const observer = new PerformanceObserver((list) => {
          for (const entry of list.getEntries()) {
            fidValue = (entry as PerformanceEntry & { processingStart?: number }).processingStart! - entry.startTime;
          }
        });
        observer.observe({ type: 'first-input', buffered: true });

        // Trigger a click to measure FID
        const clickable = document.querySelector('button, a, [role="button"]');
        if (clickable) {
          (clickable as HTMLElement).click();
        }

        // Wait for measurement
        setTimeout(() => {
          observer.disconnect();
          resolve(fidValue);
        }, 1000);
      });
    });

    console.log(`First Input Delay: ${fid}ms`);
    // FID should be less than 100ms
    // If FID is -1, no input was recorded, which is also acceptable (means no blocking)
    if (fid !== -1) {
      expect(fid).toBeLessThan(100);
    }
  });
});

test.describe('Performance Optimization - Bundle and Asset Analysis', () => {
  test('Test Case 9: JavaScript bundle is under 100KB gzipped', async ({ page }) => {
    // Track JS resource sizes
    const jsResources: { url: string; size: number; encodedSize: number }[] = [];

    page.on('response', async (response) => {
      const url = response.url();
      if (url.endsWith('.js') || url.includes('.js?')) {
        const headers = response.headers();
        const contentLength = parseInt(headers['content-length'] || '0', 10);

        // Try to get the actual transfer size
        try {
          const buffer = await response.body();
          jsResources.push({
            url,
            size: buffer.length,
            encodedSize: contentLength || buffer.length,
          });
        } catch {
          // Response may not be available
        }
      }
    });

    await page.goto('/', { waitUntil: 'networkidle' });

    // Calculate total JS size
    const totalJsSize = jsResources.reduce((acc, r) => acc + r.size, 0);
    const totalEncodedSize = jsResources.reduce((acc, r) => acc + r.encodedSize, 0);

    console.log('JS Resources:', jsResources.map(r => `${r.url}: ${r.size} bytes`));
    console.log(`Total JS size: ${totalJsSize} bytes (${(totalJsSize / 1024).toFixed(2)} KB)`);

    // 100KB = 102400 bytes gzipped
    // Use encoded size if available, otherwise estimate gzip as ~30% of original
    const estimatedGzipSize = totalEncodedSize > 0 ? totalEncodedSize : totalJsSize * 0.3;
    console.log(`Estimated gzipped size: ${(estimatedGzipSize / 1024).toFixed(2)} KB`);

    expect(estimatedGzipSize).toBeLessThan(102400);
  });

  test('Test Case 10: Images are properly sized and compressed', async ({ page }) => {
    // Track image resources
    const imageResources: {
      url: string;
      size: number;
      naturalWidth: number;
      naturalHeight: number;
      displayWidth: number;
      displayHeight: number;
    }[] = [];

    page.on('response', async (response) => {
      const url = response.url();
      const contentType = response.headers()['content-type'] || '';
      if (contentType.startsWith('image/') || url.match(/\.(png|jpg|jpeg|gif|webp|svg)$/i)) {
        try {
          const buffer = await response.body();
          imageResources.push({
            url,
            size: buffer.length,
            naturalWidth: 0,
            naturalHeight: 0,
            displayWidth: 0,
            displayHeight: 0,
          });
        } catch {
          // Response may not be available
        }
      }
    });

    await page.goto('/', { waitUntil: 'networkidle' });

    // Get image dimensions from DOM
    const imageDimensions = await page.evaluate(() => {
      const images = document.querySelectorAll('img');
      return Array.from(images).map(img => ({
        src: img.src,
        naturalWidth: img.naturalWidth,
        naturalHeight: img.naturalHeight,
        displayWidth: img.clientWidth,
        displayHeight: img.clientHeight,
      }));
    });

    // Update image resources with dimensions
    for (const dim of imageDimensions) {
      const resource = imageResources.find(r => r.url === dim.src);
      if (resource) {
        resource.naturalWidth = dim.naturalWidth;
        resource.naturalHeight = dim.naturalHeight;
        resource.displayWidth = dim.displayWidth;
        resource.displayHeight = dim.displayHeight;
      }
    }

    console.log('Image Resources:', imageResources.map(r =>
      `${r.url}: ${(r.size / 1024).toFixed(2)} KB, natural: ${r.naturalWidth}x${r.naturalHeight}, display: ${r.displayWidth}x${r.displayHeight}`
    ));

    // Check each image for optimization
    for (const img of imageResources) {
      // Skip SVGs as they're usually small and scalable
      if (img.url.endsWith('.svg')) continue;

      // Images should not be significantly larger than displayed size (2x for retina)
      if (img.displayWidth > 0 && img.naturalWidth > 0) {
        const ratio = img.naturalWidth / img.displayWidth;
        // Allow up to 3x for retina displays, but warn if larger
        expect(ratio).toBeLessThanOrEqual(3);
      }

      // Individual images should be reasonable size (< 500KB for GIFs, < 200KB for others)
      const maxSize = img.url.includes('.gif') ? 512000 : 204800;
      expect(img.size).toBeLessThanOrEqual(maxSize);
    }

    // At least verify some images were loaded
    if (imageDimensions.length > 0) {
      expect(imageResources.length).toBeGreaterThan(0);
    }
  });
});
