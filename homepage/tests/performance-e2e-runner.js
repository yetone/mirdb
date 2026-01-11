/**
 * Performance E2E Tests - Page Load Time and Lighthouse Audit
 *
 * This test suite verifies:
 * - Page loads within 3 seconds on 3G connection
 * - Lighthouse performance score is greater than 80
 *
 * Note: These tests require a Node.js test environment (not jsdom)
 * Run with: node --test tests/performance-e2e.test.js
 * Or with Jest using testEnvironment: 'node'
 */

const path = require('path');
const http = require('http');
const fs = require('fs');

// Test configuration
const PORT = 8766;
const BASE_URL = `http://localhost:${PORT}`;
const homepageDir = path.join(__dirname, '..');
const assetsDir = path.join(__dirname, '..', '..', 'assets');

// Simple static file server
function createServer() {
  return http.createServer((req, res) => {
    let filePath;
    let contentType = 'text/html';

    if (req.url === '/' || req.url === '/index.html') {
      filePath = path.join(homepageDir, 'index.html');
    } else if (req.url === '/styles.css') {
      filePath = path.join(homepageDir, 'styles.css');
      contentType = 'text/css';
    } else if (req.url.startsWith('/assets/') || req.url.includes('/assets/')) {
      const assetName = req.url.split('/assets/')[1];
      filePath = path.join(assetsDir, assetName);
      if (assetName && assetName.endsWith('.gif')) contentType = 'image/gif';
      else if (assetName && assetName.endsWith('.png')) contentType = 'image/png';
      else if (assetName && (assetName.endsWith('.jpg') || assetName.endsWith('.jpeg'))) contentType = 'image/jpeg';
      else if (assetName && assetName.endsWith('.webp')) contentType = 'image/webp';
    }

    if (filePath && fs.existsSync(filePath)) {
      const content = fs.readFileSync(filePath);
      res.writeHead(200, { 'Content-Type': contentType });
      res.end(content);
    } else {
      res.writeHead(404);
      res.end('Not found');
    }
  });
}

async function runTests() {
  const results = {
    tests: [],
    passed: 0,
    failed: 0
  };

  // Start server
  const server = createServer();
  await new Promise((resolve) => server.listen(PORT, resolve));
  console.log(`Test server running at ${BASE_URL}`);

  try {
    // Import playwright dynamically
    const { chromium } = require('playwright');

    // Launch browser
    const browser = await chromium.launch({
      headless: true,
      args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage', '--disable-gpu']
    });

    try {
      // Test Case 1: 3G Connection Load Time
      console.log('\n=== Test Case 1: 3G Connection Load Time ===');
      const context1 = await browser.newContext();
      const page1 = await context1.newPage();

      try {
        const client1 = await context1.newCDPSession(page1);
        await client1.send('Network.emulateNetworkConditions', {
          offline: false,
          downloadThroughput: (750 * 1024) / 8,
          uploadThroughput: (250 * 1024) / 8,
          latency: 100
        });

        const startTime = Date.now();
        await page1.goto(BASE_URL, { waitUntil: 'domcontentloaded', timeout: 30000 });
        const loadTime = Date.now() - startTime;
        const loadTimeSeconds = loadTime / 1000;

        console.log(`DOM Content Loaded in: ${loadTimeSeconds.toFixed(2)} seconds (3G throttled)`);

        if (loadTimeSeconds < 3) {
          console.log('✓ PASS: Page interactive within 3 seconds');
          results.tests.push({ name: '3G Load Time', status: 'pass', time: loadTimeSeconds });
          results.passed++;
        } else {
          console.log('✗ FAIL: Page took longer than 3 seconds');
          results.tests.push({ name: '3G Load Time', status: 'fail', time: loadTimeSeconds, error: `Load time ${loadTimeSeconds}s exceeds 3s` });
          results.failed++;
        }
      } finally {
        await page1.close();
        await context1.close();
      }

      // Test Case 2: First Contentful Paint
      console.log('\n=== Test Case 2: First Contentful Paint ===');
      const context2 = await browser.newContext();
      const page2 = await context2.newPage();

      try {
        const client2 = await context2.newCDPSession(page2);
        await client2.send('Network.emulateNetworkConditions', {
          offline: false,
          downloadThroughput: (750 * 1024) / 8,
          uploadThroughput: (250 * 1024) / 8,
          latency: 100
        });

        await page2.goto(BASE_URL, { waitUntil: 'networkidle', timeout: 60000 });

        const performanceMetrics = await page2.evaluate(() => {
          const perfEntries = performance.getEntriesByType('paint');
          const fcp = perfEntries.find(entry => entry.name === 'first-contentful-paint');
          return { fcp: fcp ? fcp.startTime : null };
        });

        if (performanceMetrics.fcp) {
          const fcpSeconds = performanceMetrics.fcp / 1000;
          console.log(`First Contentful Paint: ${fcpSeconds.toFixed(2)} seconds`);

          if (performanceMetrics.fcp < 3000) {
            console.log('✓ PASS: FCP within 3 seconds');
            results.tests.push({ name: 'FCP on 3G', status: 'pass', time: fcpSeconds });
            results.passed++;
          } else {
            console.log('✗ FAIL: FCP exceeds 3 seconds');
            results.tests.push({ name: 'FCP on 3G', status: 'fail', time: fcpSeconds, error: `FCP ${fcpSeconds}s exceeds 3s` });
            results.failed++;
          }
        } else {
          console.log('⚠ SKIP: FCP metric not available');
          results.tests.push({ name: 'FCP on 3G', status: 'skipped', error: 'FCP metric not available' });
        }
      } finally {
        await page2.close();
        await context2.close();
      }

      // Test Case 3: Lighthouse Performance Score
      console.log('\n=== Test Case 3: Lighthouse Performance Score ===');
      const { exec } = require('child_process');
      const { promisify } = require('util');
      const execAsync = promisify(exec);

      const lighthouseBin = path.join(__dirname, '..', 'node_modules', '.bin', 'lighthouse');
      const outputPath = path.join(__dirname, 'lighthouse-report.json');

      // Get Chrome path from Playwright
      const chromePath = chromium.executablePath();

      try {
        try {
          await execAsync(
            `"${lighthouseBin}" "${BASE_URL}" --output=json --output-path="${outputPath}" --only-categories=performance --chrome-flags="--headless --no-sandbox --disable-gpu" --preset=desktop`,
            {
              timeout: 120000,
              env: { ...process.env, CHROME_PATH: chromePath }
            }
          );
        } catch (execErr) {
          // Lighthouse may report an error but still generate a valid report
          // Check if the report exists before failing
          if (!fs.existsSync(outputPath)) {
            throw execErr;
          }
          console.log('Lighthouse completed with warnings (report generated)');
        }

        const reportContent = fs.readFileSync(outputPath, 'utf-8');
        const report = JSON.parse(reportContent);
        const performanceScore = report.categories.performance.score * 100;

        console.log(`Lighthouse Performance Score: ${performanceScore.toFixed(0)}`);
        console.log('Performance Metrics:');
        const audits = report.audits;
        console.log(`  - First Contentful Paint: ${audits['first-contentful-paint']?.displayValue || 'N/A'}`);
        console.log(`  - Largest Contentful Paint: ${audits['largest-contentful-paint']?.displayValue || 'N/A'}`);
        console.log(`  - Total Blocking Time: ${audits['total-blocking-time']?.displayValue || 'N/A'}`);
        console.log(`  - Cumulative Layout Shift: ${audits['cumulative-layout-shift']?.displayValue || 'N/A'}`);
        console.log(`  - Speed Index: ${audits['speed-index']?.displayValue || 'N/A'}`);

        if (performanceScore > 80) {
          console.log('✓ PASS: Performance score > 80');
          results.tests.push({ name: 'Lighthouse Score', status: 'pass', score: performanceScore });
          results.passed++;
        } else {
          console.log('✗ FAIL: Performance score <= 80');
          results.tests.push({ name: 'Lighthouse Score', status: 'fail', score: performanceScore, error: `Score ${performanceScore} <= 80` });
          results.failed++;
        }
      } catch (err) {
        console.log(`✗ FAIL: Lighthouse failed - ${err.message}`);
        results.tests.push({ name: 'Lighthouse Score', status: 'fail', error: err.message });
        results.failed++;
      } finally {
        if (fs.existsSync(outputPath)) {
          fs.unlinkSync(outputPath);
        }
      }

    } finally {
      await browser.close();
    }

  } finally {
    server.close();
  }

  // Summary
  console.log('\n=== Test Summary ===');
  console.log(`Passed: ${results.passed}, Failed: ${results.failed}`);

  // Write results to file for Jest to pick up
  fs.writeFileSync(
    path.join(__dirname, 'performance-e2e-results.json'),
    JSON.stringify(results, null, 2)
  );

  return results;
}

// Export for Jest or run directly
if (require.main === module) {
  runTests()
    .then(results => {
      process.exit(results.failed > 0 ? 1 : 0);
    })
    .catch(err => {
      console.error('Test runner failed:', err);
      process.exit(1);
    });
}

module.exports = { runTests, createServer, BASE_URL, PORT };
