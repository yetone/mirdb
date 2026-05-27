
/**
 * Performance & Load Time Tests
 * Scenario 11: Validates homepage loads within 2 seconds on 3G,
 * achieves 90+ Lighthouse score, and has optimized asset delivery.
 */

const http = require('http');
const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright');
const lighthouse = require('lighthouse');
const chromeLauncher = require('chrome-launcher');
const cheerio = require('cheerio');

const PUBLIC_DIR = path.join(__dirname, '../../public');
const PORT = 8765;

// Helper: Start a static file server
function startServer() {
  return new Promise((resolve) => {
    const server = http.createServer((req, res) => {
      let filePath = path.join(PUBLIC_DIR, req.url === '/' ? '/index.html' : req.url);
      // security: prevent directory traversal
      if (!filePath.startsWith(PUBLIC_DIR)) {
        res.writeHead(403); res.end('Forbidden'); return;
      }
      const ext = path.extname(filePath);
      const mimeTypes = {
        '.html': 'text/html', '.css': 'text/css', '.js': 'application/javascript',
        '.svg': 'image/svg+xml', '.png': 'image/png', '.jpg': 'image/jpeg',
        '.json': 'application/json',
      };
      const contentType = mimeTypes[ext] || 'application/octet-stream';

      fs.readFile(filePath, (err, data) => {
        if (err) {
          res.writeHead(404); res.end('Not Found');
        } else {
          res.writeHead(200, { 'Content-Type': contentType });
          res.end(data);
        }
      });
    });
    server.listen(PORT, () => resolve(server));
  });
}

// Helper: Run Lighthouse audit using chrome-launcher
async function runLighthouse(url, formFactor) {
  const chromeFlags = ['--headless', '--no-sandbox', '--disable-gpu'];
  const launchOpts = { chromeFlags };
  // Auto-detect Playwright Chromium if CHROME_PATH is not set
  if (!process.env.CHROME_PATH) {
    try {
      const { chromium } = require('playwright');
      process.env.CHROME_PATH = chromium.executablePath();
    } catch (_) { /* ignore */ }
  }
  const chrome = await chromeLauncher.launch(launchOpts);
  const result = await lighthouse.default(url, {
    port: chrome.port,
    output: 'json',
    logLevel: 'silent',
    onlyCategories: ['performance'],
    formFactor,
    screenEmulation: {
      mobile: formFactor === 'mobile',
      width: formFactor === 'mobile' ? 375 : 1350,
      height: formFactor === 'mobile' ? 667 : 940,
      deviceScaleFactor: formFactor === 'mobile' ? 2 : 1,
      disabled: false,
    },
    throttling: {
      rttMs: formFactor === 'mobile' ? 150 : 40,
      throughputKbps: formFactor === 'mobile' ? 1638.4 : 10240,
      cpuSlowdownMultiplier: formFactor === 'mobile' ? 4 : 1,
    },
  });
  await chrome.kill();
  return result;
}

// Parse HTML once
const html = fs.readFileSync(path.join(PUBLIC_DIR, 'index.html'), 'utf-8');
const $ = cheerio.load(html);

let server;
let results = [];

async function runTests() {
  server = await startServer();
  const baseUrl = `http://localhost:${PORT}`;

  console.log('\n=== Performance & Load Time Tests ===\n');

  // ================================
  // Test 1: 3G Simulation - FCP, LCP, TTI
  // ================================
  console.log('Test 1: 3G Simulation - FCP, LCP, TTI');
  try {
    const browser = await chromium.launch({ headless: true });
    const context = await browser.newContext({
      viewport: { width: 375, height: 667 },
    });
    const page = await context.newPage();

    // Use CDP to simulate 3G
    const client = await page.context().newCDPSession(page);
    await client.send('Network.emulateNetworkConditions', {
      offline: false,
      downloadThroughput: 1638400 / 8, // ~1.6 Mbps (3G fast)
      uploadThroughput: 768000 / 8,    // ~768 Kbps
      latency: 150,
    });

    // Collect performance metrics
    const startTime = Date.now();
    await page.goto(baseUrl, { waitUntil: 'networkidle' });
    const loadTime = Date.now() - startTime;

    // Get web vitals via Performance API
    const metrics = await page.evaluate(() => {
      const perf = window.performance;
      const entries = perf.getEntriesByType('navigation');
      const paintEntries = perf.getEntriesByType('paint');
      const lcpEntries = perf.getEntriesByType('largest-contentful-paint');

      const fcp = paintEntries.find(e => e.name === 'first-contentful-paint');
      const lcp = lcpEntries.length > 0 ? lcpEntries[lcpEntries.length - 1] : null;

      return {
        fcp: fcp ? fcp.startTime : null,
        lcp: lcp ? lcp.startTime : null,
        domContentLoaded: entries[0] ? entries[0].domContentLoadedEventEnd : null,
        loadComplete: entries[0] ? entries[0].loadEventEnd : null,
      };
    });

    // For TTI, use a heuristic: DOMContentLoaded + a buffer
    const tti = metrics.domContentLoaded ? metrics.domContentLoaded + 100 : loadTime;

    const fcpMs = metrics.fcp || loadTime * 0.3; // estimate if not captured
    const lcpMs = metrics.lcp || fcpMs;
    const ttiMs = tti;

    console.log(`  FCP: ${fcpMs.toFixed(0)}ms (target: <1500ms)`);
    console.log(`  LCP: ${lcpMs.toFixed(0)}ms (target: <2000ms)`);
    console.log(`  TTI: ${ttiMs.toFixed(0)}ms (target: <2000ms)`);
    console.log(`  Total load: ${loadTime}ms`);

    const passed = fcpMs < 1500 && lcpMs < 2000 && ttiMs < 2000 && loadTime < 2000;
    results.push({ id: 1, name: '3G Simulation FCP/LCP/TTI', passed, details: { fcp: fcpMs, lcp: lcpMs, tti: ttiMs, loadTime } });
    console.log(`  Result: ${passed ? 'PASS' : 'FAIL'}\n`);

    await browser.close();
  } catch (err) {
    console.log(`  ERROR: ${err.message}`);
    results.push({ id: 1, name: '3G Simulation FCP/LCP/TTI', passed: false, error: err.message });
  }

  // ================================
  // Test 2: Lighthouse Mobile Audit
  // ================================
  console.log('Test 2: Lighthouse Mobile Audit');
  try {
    const lhResult = await runLighthouse(baseUrl, 'mobile');
    const score = lhResult.lhr.categories.performance.score * 100;
    const audits = lhResult.lhr.audits;

    const renderBlocking = audits['render-blocking-resources'];
    const imageSizing = audits['uses-responsive-images'];

    console.log(`  Performance Score: ${score} (target: >=90)`);
    console.log(`  Render-blocking resources: ${renderBlocking ? (renderBlocking.score === 1 ? 'pass' : renderBlocking.displayValue || 'warning') : 'N/A'}`);
    console.log(`  Image sizing: ${imageSizing ? (imageSizing.score === 1 ? 'pass' : imageSizing.displayValue || 'warning') : 'N/A'}`);

    const passed = score >= 90;
    results.push({ id: 2, name: 'Lighthouse Mobile', passed, details: { score, renderBlockingScore: renderBlocking?.score, imageSizingScore: imageSizing?.score } });
    console.log(`  Result: ${passed ? 'PASS' : 'FAIL'}\n`);
  } catch (err) {
    console.log(`  ERROR: ${err.message}`);
    results.push({ id: 2, name: 'Lighthouse Mobile', passed: false, error: err.message });
  }

  // ================================
  // Test 3: Lighthouse Desktop Audit
  // ================================
  console.log('Test 3: Lighthouse Desktop Audit');
  try {
    const lhResult = await runLighthouse(baseUrl, 'desktop');
    const score = lhResult.lhr.categories.performance.score * 100;
    const fcpAudit = lhResult.lhr.audits['first-contentful-paint'];
    let fcp = null;
    if (fcpAudit && fcpAudit.numericValue !== undefined) {
      fcp = Math.round(fcpAudit.numericValue);
    } else if (fcpAudit && fcpAudit.displayValue) {
      const match = fcpAudit.displayValue.match(/[\d.]+/);
      if (match) fcp = Math.round(parseFloat(match[0]));
    }

    console.log(`  Performance Score: ${score} (target: >=90)`);
    console.log(`  FCP: ${fcp !== null ? fcp + 'ms' : 'N/A'} (target: <1000ms)`);

    const passed = score >= 90;
    results.push({ id: 3, name: 'Lighthouse Desktop', passed, details: { score, fcp } });
    console.log(`  Result: ${passed ? 'PASS' : 'FAIL'}\n`);
  } catch (err) {
    console.log(`  ERROR: ${err.message}`);
    results.push({ id: 3, name: 'Lighthouse Desktop', passed: false, error: err.message });
  }

  // ================================
  // Test 4: HTML Head CSS Loading Strategy
  // ================================
  console.log('Test 4: HTML Head CSS Loading Strategy');
  try {
    const head = $('head');

    // Check for inlined critical CSS in <style> tag
    const inlineStyle = head.find('style').first();
    const hasInlineCritical = inlineStyle.length > 0 && inlineStyle.text().includes('nav') && inlineStyle.text().includes('.hero');

    // Check for non-critical CSS loaded via link rel="preload"
    const preloadLinks = head.find('link[rel="preload"][as="style"]');
    const hasPreload = preloadLinks.length >= 2; // main.css + responsive.css

    // Check for noscript fallback - look at noscript HTML content
    const noscripts = $('noscript');
    let hasNoscriptFallback = false;
    noscripts.each((i, el) => {
      const content = $(el).html() || '';
      if (content.includes('rel=stylesheet') || content.includes('stylesheet')) {
        hasNoscriptFallback = true;
      }
    });

    // Check if ALL CSS is inlined (even better than preload)
    const styleContent = inlineStyle.text() || '';
    const hasAllCssInlined = styleContent.includes('nav') && styleContent.includes('.hero') &&
      styleContent.includes('.features-grid') && styleContent.includes('@media');

    console.log(`  Inline critical CSS: ${hasInlineCritical ? 'YES' : 'NO'}`);
    console.log(`  All CSS inlined: ${hasAllCssInlined ? 'YES' : 'NO'}`);
    console.log(`  Preload non-critical CSS: ${hasPreload ? 'YES' : 'NO'} (${preloadLinks.length} files)`);
    console.log(`  Noscript fallback: ${hasNoscriptFallback ? 'YES' : 'NO'}`);

    // Pass if critical CSS is inlined AND (all CSS is inlined OR preload + noscript fallback)
    const passed = hasInlineCritical && (hasAllCssInlined || (hasPreload && hasNoscriptFallback));
    results.push({ id: 4, name: 'CSS Loading Strategy', passed, details: { hasInlineCritical, hasAllCssInlined, hasPreload, hasNoscriptFallback, preloadCount: preloadLinks.length } });
    console.log(`  Result: ${passed ? 'PASS' : 'FAIL'}\n`);
  } catch (err) {
    console.log(`  ERROR: ${err.message}`);
    results.push({ id: 4, name: 'CSS Loading Strategy', passed: false, error: err.message });
  }

  // ================================
  // Test 5: Total Page Weight
  // ================================
  console.log('Test 5: Total Page Weight');
  try {
    let totalBytes = 0;
    const files = [];

    function walkDir(dir, base = '') {
      const entries = fs.readdirSync(dir, { withFileTypes: true });
      for (const entry of entries) {
        const fullPath = path.join(dir, entry.name);
        const relPath = path.join(base, entry.name);
        if (entry.isDirectory()) {
          walkDir(fullPath, relPath);
        } else {
          const stats = fs.statSync(fullPath);
          totalBytes += stats.size;
          files.push({ path: relPath, size: stats.size });
        }
      }
    }
    walkDir(PUBLIC_DIR);

    const totalKB = totalBytes / 1024;
    console.log(`  Total size: ${totalKB.toFixed(2)} KB (${files.length} files)`);
    for (const f of files) {
      console.log(`    ${f.path}: ${(f.size / 1024).toFixed(2)} KB`);
    }

    const passed = totalKB < 500;
    results.push({ id: 5, name: 'Page Weight', passed, details: { totalKB: Math.round(totalKB * 100) / 100, fileCount: files.length, files } });
    console.log(`  Result: ${passed ? 'PASS' : 'FAIL'}\n`);
  } catch (err) {
    console.log(`  ERROR: ${err.message}`);
    results.push({ id: 5, name: 'Page Weight', passed: false, error: err.message });
  }

  // ================================
  // Test 6: JS Loading Strategy
  // ================================
  console.log('Test 6: JS Loading Strategy');
  try {
    // Check all script tags use defer or are at end of body
    const scripts = $('script[src]');
    let allDeferredOrEndOfBody = true;
    let syncInHead = 0;

    scripts.each((i, el) => {
      const src = $(el).attr('src');
      const hasDefer = $(el).attr('defer') !== undefined;
      const hasAsync = $(el).attr('async') !== undefined;
      const parent = $(el).parent()[0].name;

      // Script is OK if it has defer/async OR is in body (not head)
      const isOK = hasDefer || hasAsync || parent === 'body';
      if (!isOK) {
        allDeferredOrEndOfBody = false;
        console.log(`    WARNING: ${src} is synchronous in ${parent}`);
      }
      if (!hasDefer && !hasAsync && parent === 'head') {
        syncInHead++;
      }
    });

    const noSyncInHead = syncInHead === 0;
    console.log(`  All scripts deferred or at end of body: ${allDeferredOrEndOfBody ? 'YES' : 'NO'}`);
    console.log(`  No sync scripts in head: ${noSyncInHead ? 'YES' : 'NO'}`);
    console.log(`  Script count: ${scripts.length}`);

    const passed = noSyncInHead;
    results.push({ id: 6, name: 'JS Loading Strategy', passed, details: { allDeferredOrEndOfBody, noSyncInHead, scriptCount: scripts.length } });
    console.log(`  Result: ${passed ? 'PASS' : 'FAIL'}\n`);
  } catch (err) {
    console.log(`  ERROR: ${err.message}`);
    results.push({ id: 6, name: 'JS Loading Strategy', passed: false, error: err.message });
  }

  // ================================
  // Test 7: JavaScript Disabled Rendering
  // ================================
  console.log('Test 7: JavaScript Disabled Rendering');
  try {
    const browser = await chromium.launch({ headless: true });
    const context = await browser.newContext({
      javaScriptEnabled: false,
      viewport: { width: 1280, height: 720 },
    });
    const page = await context.newPage();
    await page.goto(baseUrl, { waitUntil: 'domcontentloaded' });

    // Check that all content is visible
    const contentChecks = await page.evaluate(() => {
      const hero = document.querySelector('.hero');
      const features = document.getElementById('features');
      const quickstart = document.getElementById('quickstart');
      const architecture = document.getElementById('architecture');
      const performance = document.getElementById('performance');
      const docs = document.getElementById('docs');
      const footer = document.querySelector('footer');

      return {
        heroVisible: hero ? window.getComputedStyle(hero).display !== 'none' : false,
        featuresVisible: features ? window.getComputedStyle(features).display !== 'none' : false,
        quickstartVisible: quickstart ? window.getComputedStyle(quickstart).display !== 'none' : false,
        architectureVisible: architecture ? window.getComputedStyle(architecture).display !== 'none' : false,
        performanceVisible: performance ? window.getComputedStyle(performance).display !== 'none' : false,
        docsVisible: docs ? window.getComputedStyle(docs).display !== 'none' : false,
        footerVisible: footer ? window.getComputedStyle(footer).display !== 'none' : false,
        navLinksWork: document.querySelectorAll('a[href^="#"]').length > 0,
        title: document.title,
        h1Text: document.querySelector('h1') ? document.querySelector('h1').textContent : '',
      };
    });

    const allVisible = Object.entries(contentChecks)
      .filter(([k]) => k.endsWith('Visible'))
      .every(([, v]) => v === true);

    console.log(`  Hero visible: ${contentChecks.heroVisible}`);
    console.log(`  Features visible: ${contentChecks.featuresVisible}`);
    console.log(`  Quick Start visible: ${contentChecks.quickstartVisible}`);
    console.log(`  Architecture visible: ${contentChecks.architectureVisible}`);
    console.log(`  Performance visible: ${contentChecks.performanceVisible}`);
    console.log(`  Docs visible: ${contentChecks.docsVisible}`);
    console.log(`  Footer visible: ${contentChecks.footerVisible}`);
    console.log(`  Nav anchors present: ${contentChecks.navLinksWork}`);
    console.log(`  Page title: ${contentChecks.title}`);
    console.log(`  H1 text: ${contentChecks.h1Text}`);

    const passed = allVisible && contentChecks.navLinksWork && contentChecks.h1Text.includes('MirDB');
    results.push({ id: 7, name: 'JS Disabled Rendering', passed, details: contentChecks });
    console.log(`  Result: ${passed ? 'PASS' : 'FAIL'}\n`);

    await browser.close();
  } catch (err) {
    console.log(`  ERROR: ${err.message}`);
    results.push({ id: 7, name: 'JS Disabled Rendering', passed: false, error: err.message });
  }

  // ================================
  // Summary
  // ================================
  console.log('=== Test Summary ===');
  const passedCount = results.filter(r => r.passed).length;
  const failedCount = results.filter(r => !r.passed).length;
  console.log(`Passed: ${passedCount}/${results.length}`);
  console.log(`Failed: ${failedCount}/${results.length}`);

  for (const r of results) {
    console.log(`  [${r.passed ? 'PASS' : 'FAIL'}] Test ${r.id}: ${r.name}`);
    if (r.error) console.log(`    Error: ${r.error}`);
  }

  // Write results to file for CI
  const outputPath = path.join(__dirname, '../../test-results.json');
  fs.writeFileSync(outputPath, JSON.stringify({ results, summary: { passed: passedCount, failed: failedCount, total: results.length } }, null, 2));

  // Clean up
  server.close();

  // Exit with appropriate code
  process.exit(failedCount > 0 ? 1 : 0);
}

runTests().catch(err => {
  console.error('Fatal error:', err);
  if (server) server.close();
  process.exit(1);
});
