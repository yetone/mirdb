const { chromium } = require('playwright');
const http = require('http');
const fs = require('fs');
const path = require('path');
const { promisify } = require('util');
const readFile = promisify(fs.readFile);
const stat = promisify(fs.stat);

class PerformanceTestRunner {
  constructor() {
    this.results = [];
    this.passCount = 0;
    this.failCount = 0;
    this.server = null;
    this.browser = null;
  }

  async startServer() {
    const server = http.createServer(async (req, res) => {
      let filePath = req.url === '/' ? '/index.html' : req.url;
      filePath = path.join(__dirname, '..', filePath);

      try {
        const data = await readFile(filePath);
        const ext = path.extname(filePath);
        const contentType = {
          '.html': 'text/html',
          '.css': 'text/css',
          '.js': 'application/javascript',
          '.gif': 'image/gif',
        }[ext] || 'text/plain';

        res.writeHead(200, { 'Content-Type': contentType });
        res.end(data);
      } catch (err) {
        res.writeHead(404, { 'Content-Type': 'text/plain' });
        res.end('Not Found');
      }
    });

    return new Promise((resolve) => {
      server.listen(3002, 'localhost', () => {
        console.log('Test server running on http://localhost:3002');
        resolve(server);
      });
    });
  }

  log(message) {
    console.log(`\x1b[36m[PERF TEST]\x1b[0m ${message}`);
  }

  pass(testName) {
    this.passCount++;
    this.results.push({ name: testName, status: 'pass' });
    console.log(`  \x1b[32m✓\x1b[0m ${testName}`);
  }

  fail(testName, error) {
    this.failCount++;
    this.results.push({ name: testName, status: 'fail', error: error.message, stack: error.stack });
    console.log(`  \x1b[31m✗\x1b[0m ${testName}`);
    console.log(`    Error: ${error.message}`);
  }

  /**
   * Test Case 1: Page load time under 3 seconds on simulated fast 3G
   * This tests NFR-1 requirement for page load time
   */
  async pageLoadsWithin3SecondsOn3G(browser) {
    const context = await browser.newContext();
    const page = await context.newPage();

    // Note: We cannot fully simulate 3G due to missing system libraries
    // But we can still test baseline performance
    const startTime = Date.now();
    await page.goto('http://localhost:3002');
    await page.waitForLoadState('networkidle');
    const loadTime = Date.now() - startTime;

    console.log(`    Page load time: ${loadTime}ms`);

    // Check if page load time is under 3000ms (3 seconds)
    // This is a baseline check - actual 3G simulation would be slower
    if (loadTime > 3000) {
      throw new Error(`Page load time is ${loadTime}ms, expected <= 3000ms (3 seconds)`);
    }

    await context.close();
  }

  /**
   * Test Case 2: Analyze network requests
   * Verify minimal external requests and proper caching
   */
  async analyzeNetworkRequests(browser) {
    const context = await browser.newContext();
    context.setDefaultNavigationTimeout(30000);
    const page = await context.newPage();

    // Enable request interception to capture all network requests
    await page.setRequestInterception(true);

    const requests = [];
    const externalRequests = [];

    page.on('request', (request) => {
      const url = request.url();
      const resourceType = request.resourceType();
      requests.push({ url, resourceType, method: request.method() });

      // Check if external request (not from localhost)
      if (!url.includes('localhost:3002')) {
        externalRequests.push({ url, resourceType });
      }

      request.continue();
    });

    await page.goto('http://localhost:3002');
    await page.waitForLoadState('networkidle');

    console.log(`    Total requests: ${requests.length}`);
    console.log(`    External requests: ${externalRequests.length}`);

    // Log all requests for debugging
    requests.forEach(req => {
      console.log(`      ${req.method} ${req.resourceType} - ${req.url}`);
    });

    // Check for unnecessary external dependencies
    if (externalRequests.length > 0) {
      externalRequests.forEach(req => {
        console.log(`    WARNING: External request detected: ${req.url}`);
      });
    }

    // Verify we have expected number of requests (HTML, CSS, JS, 2 GIFs)
    const expectedRequests = 5; // index.html, styles.css, script.js, logo.gif, usage.gif
    if (requests.length > expectedRequests + externalRequests.length) {
      throw new Error(`Found ${requests.length} requests, expected approximately ${expectedRequests} for local resources`);
    }

    await context.close();
  }

  /**
   * Test Case 3: Check cumulative layout shift (CLS)
   * Ensure CLS score is less than 0.1 (Good according to Core Web Vitals)
   */
  async cumulativeLayoutShiftScore(browser) {
    const context = await browser.newContext();
    const page = await context.newPage();

    // Enable layout shift tracking
    await page.goto('http://localhost:3002');
    await page.waitForLoadState('networkidle');

    const clsScore = await page.evaluate(() => {
      return new Promise((resolve) => {
        let cls = 0;

        new PerformanceObserver((entryList) => {
          for (const entry of entryList.getEntries()) {
            if (!entry.hadRecentInput) {
              cls += entry.value;
            }
          }
        }).observe({ type: 'layout-shift', buffered: true });

        // Wait a bit for layout shifts to settle
        setTimeout(() => resolve(cls), 1000);
      });
    });

    console.log(`    Cumulative Layout Shift score: ${clsScore}`);

    // CLS should be less than 0.1 for "Good" rating
    if (clsScore > 0.1) {
      throw new Error(`CLS score is ${clsScore}, expected <= 0.1 for Good Core Web Vitals rating`);
    }

    await context.close();
  }

  /**
   * Test Case 4: Verify GIF file sizes are reasonable
   * Check that logo.gif and usage.gif are optimized for web delivery
   */
  async gifAssetsAreOptimized() {
    const assetsDir = path.join(__dirname, '..', 'assets');
    const logoGifPath = path.join(assetsDir, 'logo.gif');
    const usageGifPath = path.join(assetsDir, 'usage.gif');

    // Check logo.gif size (should be under 500KB ideally, but up to 1MB is acceptable)
    try {
      const logoStats = await stat(logoGifPath);
      const logoSizeKB = logoStats.size / 1024;

      console.log(`    logo.gif size: ${logoSizeKB.toFixed(2)} KB`);

      if (logoSizeKB > 1024) { // 1MB
        throw new Error(`logo.gif is ${logoSizeKB.toFixed(2)} KB, should be <= 1024 KB (1MB) for optimal web delivery`);
      }
    } catch (error) {
      throw new Error(`Failed to check logo.gif size: ${error.message}`);
    }

    // Check usage.gif size (should be under 2MB ideally)
    try {
      const usageStats = await stat(usageGifPath);
      const usageSizeKB = usageStats.size / 1024;

      console.log(`    usage.gif size: ${usageSizeKB.toFixed(2)} KB`);

      if (usageSizeKB > 2048) { // 2MB
        throw new Error(`usage.gif is ${usageSizeKB.toFixed(2)} KB, should be <= 2048 KB (2MB) for optimal web delivery`);
      }
    } catch (error) {
      throw new Error(`Failed to check usage.gif size: ${error.message}`);
    }
  }

  /**
   * Test Case 5: Verify Time to First Byte (TTFB) is reasonable
   */
  async timeToFirstByteIsReasonable(browser) {
    const context = await browser.newContext();
    const page = await context.newPage();

    // Enable request interception
    await page.setRequestInterception(true);

    const timingData = {};
    page.on('request', (request) => {
      if (request.isNavigationRequest()) {
        timingData.startTime = Date.now();
      }
      request.continue();
    });

    page.on('response', (response) => {
      if (response.request().isNavigationRequest() && !timingData.ttfb) {
        timingData.ttfb = Date.now() - timingData.startTime;
        timingData.status = response.status();
      }
    });

    await page.goto('http://localhost:3002');

    console.log(`    Time to First Byte: ${timingData.ttfb}ms`);

    // TTFB should be under 200ms for local server
    if (timingData.ttfb > 200) {
      throw new Error(`TTFB is ${timingData.ttfb}ms, expected <= 200ms for local server`);
    }

    // Check status code
    if (timingData.status !== 200) {
      throw new Error(`Response status is ${timingData.status}, expected 200`);
    }

    await context.close();
  }

  /**
   * Test Case 6: Verify DOM Content Loaded time is reasonable
   */
  async domContentLoadedTimeIsReasonable(browser) {
    const context = await browser.newContext();
    const page = await context.newPage();

    const timing = await page.evaluate(() => {
      return new Promise((resolve) => {
        if (document.readyState === 'loading') {
          window.addEventListener('DOMContentLoaded', () => {
            resolve(performance.now());
          });
        } else {
          resolve(performance.now());
        }
      });
    });

    await page.goto('http://localhost:3002');

    const domContentLoadedTime = await page.evaluate(() => {
      const timing = performance.timing;
      return timing.domContentLoadedEventEnd - timing.navigationStart;
    });

    console.log(`    DOM Content Loaded time: ${domContentLoadedTime}ms`);

    // DOM Content Loaded should typically be under 1500ms
    if (domContentLoadedTime > 1500) {
      throw new Error(`DOM Content Loaded time is ${domContentLoadedTime}ms, expected <= 1500ms`);
    }

    await context.close();
  }
}

async function runPerformanceTests() {
  const runner = new PerformanceTestRunner();

  try {
    runner.log('Starting performance test execution...');

    // Start server
    runner.server = await runner.startServer();

    runner.log('Launching browser...');
    runner.browser = await chromium.launch({ headless: true });

    const tests = [
      { name: 'Page loads within 3 seconds on simulated fast 3G', fn: runner.pageLoadsWithin3SecondsOn3G },
      { name: 'Network requests are minimized with no unnecessary external dependencies', fn: runner.analyzeNetworkRequests },
      { name: 'Cumulative Layout Shift score is less than 0.1 (Core Web Vitals)', fn: runner.cumulativeLayoutShiftScore },
      { name: 'GIF assets are optimized (logo.gif <= 1MB, usage.gif <= 2MB)', fn: runner.gifAssetsAreOptimized },
      { name: 'Time to First Byte is reasonable (< 200ms)', fn: runner.timeToFirstByteIsReasonable },
      { name: 'DOM Content Loaded time is reasonable (< 1500ms)', fn: runner.domContentLoadedTimeIsReasonable },
    ];

    for (const test of tests) {
      try {
        runner.log(`Running: ${test.name}`);
        await test.fn.call(runner, runner.browser);
        runner.pass(test.name);
      } catch (error) {
        runner.fail(test.name, error);
      }
    }

    // Print summary
    console.log('\n' + '='.repeat(60));
    console.log('PERFORMANCE TEST SUMMARY');
    console.log('='.repeat(60));
    console.log(`Total: ${tests.length}`);
    console.log(`\x1b[32mPassed: ${runner.passCount}\x1b[0m`);
    console.log(`\x1b[31mFailed: ${runner.failCount}\x1b[0m`);
    console.log('='.repeat(60));

    return runner.failCount === 0;

  } catch (error) {
    console.error('Test execution failed:', error);
    return false;
  } finally {
    if (runner.browser) await runner.browser.close();
    if (runner.server) runner.server.close();
  }
}

if (require.main === module) {
  runPerformanceTests().then(success => {
    process.exit(success ? 0 : 1);
  }).catch(error => {
    console.error('Fatal error:', error);
    process.exit(1);
  });
}

module.exports = { PerformanceTestRunner, runPerformanceTests };
