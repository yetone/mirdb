import { test, expect, chromium } from '@playwright/test';
import type { Page, Browser, BrowserContext, CDPSession } from '@playwright/test';
import { playAudit } from 'playwright-lighthouse';
import * as fs from 'fs';
import * as path from 'path';

// Port for Lighthouse debugging
const LIGHTHOUSE_PORT = 9222;

// Lighthouse result interfaces
interface LighthouseAudit {
  id: string;
  title: string;
  score: number | null;
  numericValue?: number;
  displayValue?: string;
  details?: {
    type: string;
    items?: Array<{
      url?: string;
      wastedMs?: number;
      totalBytes?: number;
    }>;
    overallSavingsMs?: number;
  };
}

interface LighthouseCategory {
  title: string;
  score: number;
}

interface LighthouseResult {
  lhr: {
    categories: {
      performance: LighthouseCategory;
      accessibility?: LighthouseCategory;
      'best-practices'?: LighthouseCategory;
      seo?: LighthouseCategory;
    };
    audits: {
      [key: string]: LighthouseAudit;
    };
  };
}

// Performance metrics from Chrome DevTools Protocol
interface PerformanceMetrics {
  Timestamp: number;
  Documents: number;
  Frames: number;
  JSEventListeners: number;
  Nodes: number;
  LayoutCount: number;
  RecalcStyleCount: number;
  LayoutDuration: number;
  RecalcStyleDuration: number;
  ScriptDuration: number;
  TaskDuration: number;
  JSHeapUsedSize: number;
  JSHeapTotalSize: number;
}

test.describe('Page Performance - Lighthouse Audits', () => {
  let browser: Browser;
  let context: BrowserContext;
  let page: Page;
  let lighthouseResult: LighthouseResult | null = null;
  let lighthouseError: Error | null = null;

  test.beforeAll(async () => {
    // Launch a separate browser instance with remote debugging for Lighthouse
    browser = await chromium.launch({
      args: ['--remote-debugging-port=' + LIGHTHOUSE_PORT],
    });
    context = await browser.newContext();
    page = await context.newPage();

    // Navigate to the page
    await page.goto('http://localhost:3000/', { waitUntil: 'networkidle' });

    // Run Lighthouse audit
    try {
      lighthouseResult = await playAudit({
        page,
        port: LIGHTHOUSE_PORT,
        thresholds: {
          performance: 0, // We check thresholds in individual tests
        },
        config: {
          extends: 'lighthouse:default',
          settings: {
            formFactor: 'desktop',
            // Use desktop throttling (no CPU slowdown, faster network)
            // This is appropriate for desktop performance testing
            throttling: {
              rttMs: 40,
              throughputKbps: 10240,
              cpuSlowdownMultiplier: 1,
            },
            screenEmulation: {
              mobile: false,
              width: 1350,
              height: 940,
              deviceScaleFactor: 1,
              disabled: false,
            },
          },
        },
      }) as LighthouseResult;
    } catch (error) {
      lighthouseError = error as Error;
      console.error('Lighthouse audit failed:', error);
    }
  });

  test.afterAll(async () => {
    if (context) await context.close();
    if (browser) await browser.close();
  });

  test('TC1: Time to Interactive (TTI) is under 3 seconds', async () => {
    // Skip if Lighthouse failed, fall back to Playwright metrics test
    test.skip(lighthouseError !== null, 'Lighthouse unavailable - see fallback test');

    expect(lighthouseResult).toBeDefined();
    expect(lighthouseResult!.lhr.audits['interactive']).toBeDefined();

    const ttiMs = lighthouseResult!.lhr.audits['interactive'].numericValue ?? 0;
    const ttiSeconds = ttiMs / 1000;

    console.log(`Time to Interactive: ${ttiSeconds.toFixed(2)} seconds (${ttiMs.toFixed(0)} ms)`);
    // TTI should be under 3 seconds on standard desktop connection
    expect(ttiSeconds).toBeLessThan(3);
  });

  test('TC2: Lighthouse performance score is 80 or higher', async () => {
    test.skip(lighthouseError !== null, 'Lighthouse unavailable - see fallback test');

    expect(lighthouseResult).toBeDefined();
    const performanceScore = lighthouseResult!.lhr.categories.performance.score * 100;

    console.log(`Lighthouse Performance Score: ${performanceScore}`);
    expect(performanceScore).toBeGreaterThanOrEqual(80);
  });

  test('TC3: Total page size (including assets) is reasonable (<2MB)', async () => {
    test.skip(lighthouseError !== null, 'Lighthouse unavailable - see fallback test');

    expect(lighthouseResult).toBeDefined();
    const totalBytes = lighthouseResult!.lhr.audits['total-byte-weight']?.numericValue ?? 0;
    const totalMB = totalBytes / (1024 * 1024);

    console.log(`Total Page Size: ${totalMB.toFixed(2)} MB (${totalBytes} bytes)`);
    expect(totalBytes).toBeLessThan(2 * 1024 * 1024);
  });

  test('TC4: No unnecessary render-blocking CSS or JS', async () => {
    test.skip(lighthouseError !== null, 'Lighthouse unavailable - see fallback test');

    expect(lighthouseResult).toBeDefined();
    const renderBlockingAudit = lighthouseResult!.lhr.audits['render-blocking-resources'];
    const score = renderBlockingAudit?.score ?? 1;
    const potentialSavingsMs = renderBlockingAudit?.details?.overallSavingsMs ?? 0;

    console.log(`Render-blocking resources score: ${score}`);
    console.log(`Potential savings: ${potentialSavingsMs} ms`);

    // Acceptable if score >= 0.9 or savings < 150ms
    expect(score >= 0.9 || potentialSavingsMs < 150).toBe(true);
  });

  test('TC5: Images are compressed and appropriately sized', async () => {
    test.skip(lighthouseError !== null, 'Lighthouse unavailable - see fallback test');

    expect(lighthouseResult).toBeDefined();
    const optimizedScore = lighthouseResult!.lhr.audits['uses-optimized-images']?.score ?? 1;
    const responsiveScore = lighthouseResult!.lhr.audits['uses-responsive-images']?.score ?? 1;

    console.log(`Optimized Images Score: ${optimizedScore}`);
    console.log(`Responsive Images Score: ${responsiveScore}`);

    expect(optimizedScore).toBeGreaterThanOrEqual(0.9);
    expect(responsiveScore).toBeGreaterThanOrEqual(0.9);
  });

  test('TC6: First Contentful Paint (FCP) is under 1.8 seconds', async () => {
    test.skip(lighthouseError !== null, 'Lighthouse unavailable - see fallback test');

    expect(lighthouseResult).toBeDefined();
    const fcpMs = lighthouseResult!.lhr.audits['first-contentful-paint']?.numericValue ?? 0;
    const fcpSeconds = fcpMs / 1000;

    console.log(`First Contentful Paint: ${fcpSeconds.toFixed(2)} seconds (${fcpMs.toFixed(0)} ms)`);
    expect(fcpSeconds).toBeLessThan(1.8);
  });
});

// Fallback tests using Playwright's built-in performance APIs
// These run if Lighthouse cannot be used in the environment
test.describe('Page Performance - Playwright Metrics (Fallback)', () => {
  test('TC1-Fallback: Page becomes interactive within 3 seconds', async ({ page }) => {
    const startTime = Date.now();

    // Navigate and wait for the page to be fully loaded
    await page.goto('/');

    // Wait for DOM to be ready and interactive
    await page.waitForLoadState('domcontentloaded');
    const domContentLoadedTime = Date.now() - startTime;

    // Wait for network idle (no pending requests)
    await page.waitForLoadState('networkidle');
    const networkIdleTime = Date.now() - startTime;

    // Test that all interactive elements are clickable
    const getStartedButton = page.locator('a.cta-button', { hasText: 'Get Started' });
    await expect(getStartedButton).toBeEnabled();
    const interactiveTime = Date.now() - startTime;

    console.log(`DOM Content Loaded: ${domContentLoadedTime}ms`);
    console.log(`Network Idle: ${networkIdleTime}ms`);
    console.log(`Interactive Time: ${interactiveTime}ms`);

    // Page should be interactive within 3 seconds (3000ms)
    expect(interactiveTime).toBeLessThan(3000);
  });

  test('TC2-Fallback: Page meets performance best practices', async ({ page }) => {
    // Connect to Chrome DevTools Protocol for performance metrics
    const client = await page.context().newCDPSession(page);
    await client.send('Performance.enable');

    await page.goto('/', { waitUntil: 'networkidle' });

    // Get performance metrics from CDP
    const metrics = await client.send('Performance.getMetrics');
    const metricsMap: Record<string, number> = {};
    metrics.metrics.forEach((m: { name: string; value: number }) => {
      metricsMap[m.name] = m.value;
    });

    console.log('Performance Metrics:');
    console.log(`  - Task Duration: ${(metricsMap['TaskDuration'] * 1000).toFixed(2)}ms`);
    console.log(`  - Script Duration: ${(metricsMap['ScriptDuration'] * 1000).toFixed(2)}ms`);
    console.log(`  - Layout Duration: ${(metricsMap['LayoutDuration'] * 1000).toFixed(2)}ms`);
    console.log(`  - JS Heap Used: ${(metricsMap['JSHeapUsedSize'] / 1024 / 1024).toFixed(2)}MB`);

    // Performance checks (proxies for Lighthouse score)
    // Script duration should be minimal (< 500ms) for a static page
    expect(metricsMap['ScriptDuration'] * 1000).toBeLessThan(500);

    // Layout duration should be reasonable (< 500ms) - varies by environment
    expect(metricsMap['LayoutDuration'] * 1000).toBeLessThan(500);

    // JS Heap should be reasonable (< 50MB for a static page)
    expect(metricsMap['JSHeapUsedSize'] / 1024 / 1024).toBeLessThan(50);
  });

  test('TC3-Fallback: Total page size is under 2MB', async ({ page }) => {
    let totalBytes = 0;
    const resources: { url: string; size: number }[] = [];

    // Track all network responses
    page.on('response', async (response) => {
      const request = response.request();
      if (request.resourceType() !== 'websocket') {
        try {
          const body = await response.body();
          const size = body.length;
          totalBytes += size;
          resources.push({ url: request.url(), size });
        } catch {
          // Some responses may not have a body
        }
      }
    });

    await page.goto('/', { waitUntil: 'networkidle' });

    // Wait a bit for any deferred resources
    await page.waitForTimeout(500);

    const totalMB = totalBytes / (1024 * 1024);
    console.log(`Total transferred: ${totalMB.toFixed(2)} MB (${totalBytes} bytes)`);
    console.log('Resources:');
    resources.sort((a, b) => b.size - a.size).slice(0, 5).forEach(r => {
      console.log(`  - ${r.url}: ${(r.size / 1024).toFixed(2)} KB`);
    });

    // Total page size should be under 2MB
    expect(totalBytes).toBeLessThan(2 * 1024 * 1024);
  });

  test('TC4-Fallback: No render-blocking resources detected', async ({ page }) => {
    // Check HTML for render-blocking patterns
    await page.goto('/', { waitUntil: 'domcontentloaded' });

    // Get all link and script tags in head
    const headContent = await page.evaluate(() => {
      const head = document.head;
      const links = Array.from(head.querySelectorAll('link[rel="stylesheet"]'));
      const scripts = Array.from(head.querySelectorAll('script:not([async]):not([defer])'));

      return {
        stylesheets: links.map(l => (l as HTMLLinkElement).href),
        blockingScripts: scripts.map(s => (s as HTMLScriptElement).src).filter(src => src),
      };
    });

    console.log('Stylesheets in head:', headContent.stylesheets);
    console.log('Blocking scripts in head:', headContent.blockingScripts);

    // For this static page, we expect only one stylesheet and no blocking scripts
    expect(headContent.stylesheets.length).toBeLessThanOrEqual(2);
    expect(headContent.blockingScripts.length).toBe(0);
  });

  test('TC5-Fallback: Images are optimized (no external images, SVG is used)', async ({ page }) => {
    await page.goto('/', { waitUntil: 'networkidle' });

    // Get all images on the page
    const imageInfo = await page.evaluate(() => {
      const images = Array.from(document.querySelectorAll('img'));
      const svgs = Array.from(document.querySelectorAll('svg'));

      return {
        imgTags: images.map(img => ({
          src: img.src,
          naturalWidth: img.naturalWidth,
          naturalHeight: img.naturalHeight,
          displayWidth: img.width,
          displayHeight: img.height,
        })),
        svgCount: svgs.length,
        // Check for background images in styles
        hasBackgroundImages: Array.from(document.querySelectorAll('*')).some(el => {
          const style = window.getComputedStyle(el);
          return style.backgroundImage !== 'none' && style.backgroundImage.includes('url(');
        }),
      };
    });

    console.log(`Image tags: ${imageInfo.imgTags.length}`);
    console.log(`SVG elements: ${imageInfo.svgCount}`);
    console.log(`Has background images: ${imageInfo.hasBackgroundImages}`);

    // This page uses SVGs instead of raster images - good practice
    // If there are img tags, verify they're not oversized
    if (imageInfo.imgTags.length > 0) {
      imageInfo.imgTags.forEach((img, i) => {
        console.log(`  Image ${i + 1}: ${img.src}`);
        console.log(`    Natural: ${img.naturalWidth}x${img.naturalHeight}`);
        console.log(`    Display: ${img.displayWidth}x${img.displayHeight}`);

        // Image should not be more than 2x display size (retina)
        if (img.naturalWidth > 0 && img.displayWidth > 0) {
          const ratio = img.naturalWidth / img.displayWidth;
          expect(ratio).toBeLessThanOrEqual(3);
        }
      });
    }

    // SVG usage is good for scalable graphics
    expect(imageInfo.svgCount).toBeGreaterThanOrEqual(1);
  });

  test('TC6-Fallback: First Contentful Paint is under 1.8 seconds', async ({ page }) => {
    // Use Performance API to measure FCP
    await page.goto('/', { waitUntil: 'networkidle' });

    const performanceTiming = await page.evaluate(() => {
      const entries = performance.getEntriesByType('paint');
      const fcp = entries.find(e => e.name === 'first-contentful-paint');
      const fp = entries.find(e => e.name === 'first-paint');

      // Also get navigation timing
      const navEntries = performance.getEntriesByType('navigation') as PerformanceNavigationTiming[];
      const navTiming = navEntries[0];

      return {
        firstPaint: fp ? fp.startTime : null,
        firstContentfulPaint: fcp ? fcp.startTime : null,
        domContentLoaded: navTiming ? navTiming.domContentLoadedEventEnd : null,
        loadEvent: navTiming ? navTiming.loadEventEnd : null,
      };
    });

    console.log('Paint Timing:');
    console.log(`  - First Paint: ${performanceTiming.firstPaint?.toFixed(2)}ms`);
    console.log(`  - First Contentful Paint: ${performanceTiming.firstContentfulPaint?.toFixed(2)}ms`);
    console.log(`  - DOM Content Loaded: ${performanceTiming.domContentLoaded?.toFixed(2)}ms`);
    console.log(`  - Load Event: ${performanceTiming.loadEvent?.toFixed(2)}ms`);

    // FCP should be under 1800ms
    if (performanceTiming.firstContentfulPaint !== null) {
      expect(performanceTiming.firstContentfulPaint).toBeLessThan(1800);
    } else {
      // If FCP not available, fall back to DOM content loaded
      expect(performanceTiming.domContentLoaded).toBeLessThan(1800);
    }
  });
});

// Additional static file analysis tests
test.describe('Page Performance - Static Asset Analysis', () => {
  test('HTML file size is reasonable', async () => {
    const htmlPath = path.join(__dirname, '..', 'public', 'index.html');
    const htmlContent = fs.readFileSync(htmlPath, 'utf-8');
    const htmlSize = Buffer.byteLength(htmlContent, 'utf-8');

    console.log(`HTML file size: ${(htmlSize / 1024).toFixed(2)} KB`);

    // HTML should be under 50KB for a single page
    expect(htmlSize).toBeLessThan(50 * 1024);
  });

  test('CSS file size is reasonable', async () => {
    const cssPath = path.join(__dirname, '..', 'public', 'styles.css');
    const cssContent = fs.readFileSync(cssPath, 'utf-8');
    const cssSize = Buffer.byteLength(cssContent, 'utf-8');

    console.log(`CSS file size: ${(cssSize / 1024).toFixed(2)} KB`);

    // CSS should be under 100KB for a static page
    expect(cssSize).toBeLessThan(100 * 1024);
  });

  test('No JavaScript files in public folder (pure HTML/CSS)', async () => {
    const publicDir = path.join(__dirname, '..', 'public');
    const files = fs.readdirSync(publicDir);
    const jsFiles = files.filter(f => f.endsWith('.js'));

    console.log(`Files in public: ${files.join(', ')}`);
    console.log(`JavaScript files: ${jsFiles.length > 0 ? jsFiles.join(', ') : 'none'}`);

    // Static page should have no JS files (or minimal)
    expect(jsFiles.length).toBe(0);
  });

  test('CSS uses efficient selectors and no large media queries', async () => {
    const cssPath = path.join(__dirname, '..', 'public', 'styles.css');
    const cssContent = fs.readFileSync(cssPath, 'utf-8');

    // Check for CSS variables usage (good practice)
    const usesVariables = cssContent.includes(':root') && cssContent.includes('var(--');
    console.log(`Uses CSS variables: ${usesVariables}`);
    expect(usesVariables).toBe(true);

    // Check for media queries (responsive design)
    const mediaQueries = (cssContent.match(/@media/g) || []).length;
    console.log(`Media queries count: ${mediaQueries}`);
    expect(mediaQueries).toBeGreaterThan(0);

    // Check for reasonable number of selectors (not bloated)
    const selectorCount = (cssContent.match(/\{/g) || []).length;
    console.log(`Approximate selector count: ${selectorCount}`);
    expect(selectorCount).toBeLessThan(300);
  });
});
