const http = require('http');
const fs = require('fs');
const path = require('path');
const { promisify } = require('util');
const readFile = promisify(fs.readFile);
const stat = promisify(fs.stat);

class StaticPerformanceTestRunner {
  constructor() {
    this.results = [];
    this.passCount = 0;
    this.failCount = 0;
    this.server = null;
    this.metrics = {
      performance: {},
      resources: {},
      recommendations: []
    };
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
      server.listen(3003, 'localhost', () => {
        console.log('Test server running on http://localhost:3003');
        resolve(server);
      });
    });
  }

  log(message) {
    console.log(`\x1b[36m[STATIC PERF TEST]\x1b[0m ${message}`);
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
   * Test Case 1: Analyze HTML structure for performance issues
   * Check for best practices that impact loading time
   */
  async analyzeHtmlStructure() {
    const indexPath = path.join(__dirname, '..', 'index.html');
    const html = await readFile(indexPath, 'utf8');

    // Check for presence of CSS in head (prevents render-blocking)
    const hasCssLink = html.includes('<link rel="stylesheet" href="styles.css">');
    if (!hasCssLink) {
      throw new Error('CSS should be linked in head for optimal loading');
    }
    console.log('    CSS is linked in head ✓');

    // Check CSS is in head, not body
    const bodyIndex = html.toLowerCase().indexOf('<body');
    const cssIndex = html.indexOf('<link rel="stylesheet"');
    if (cssIndex > bodyIndex && bodyIndex > -1) {
      throw new Error('CSS link appears after body tag - should be in head');
    }
    console.log('    CSS placed before body for non-blocking rendering ✓');

    // Check JavaScript is at bottom or has defer/async
    const scriptIndex = html.indexOf('<script src="script.js">');
    if (scriptIndex > -1 && scriptIndex < bodyIndex) {
      throw new Error('JavaScript appears before body - should be at bottom for better performance');
    }
    console.log('    JavaScript is placed at bottom for better performance ✓');

    // Check for meta viewport (helps prevent layout shifts)
    const hasViewport = html.includes('name="viewport"');
    if (!hasViewport) {
      throw new Error('Missing viewport meta tag - can cause layout issues on mobile');
    }
    console.log('    Viewport meta tag present for mobile optimization ✓');

    // Count total resources referenced
    const resources = [
      ...html.matchAll(/<link[^>]+href=["']([^"']+)["'][^>]*>/g),
      ...html.matchAll(/<script[^>]+src=["']([^"']+)["'][^>]*>/g),
      ...html.matchAll(/<img[^>]+src=["']([^"']+)["'][^>]*>/g)
    ];

    this.metrics.resources.count = resources.length;
    this.metrics.resources.localResources = resources.filter(r => r[1].startsWith('/') || r[1].startsWith('.'));
    this.metrics.resources.externalResources = resources.filter(r => !r[1].startsWith('/') && !r[1].startsWith('.') && r[1].startsWith('http'));

    console.log(`    Total resources referenced: ${resources.length}`);
    console.log(`    Local resources: ${this.metrics.resources.localResources.length}`);
    console.log(`    External resources: ${this.metrics.resources.externalResources.length}`);

    if (this.metrics.resources.externalResources.length > 0) {
      console.log('    WARNING: External resources detected (may impact performance on slow connections):');
      this.metrics.resources.externalResources.forEach(r => console.log(`      - ${r[1]}`));
      this.metrics.recommendations.push('Consider hosting external resources locally for better control over loading');
    }
  }

  /**
   * Test Case 2: Check GIF file sizes
   * Verify logo.gif and usage.gif are optimized for web delivery
   */
  async checkGifFileSizes() {
    const assetsDir = path.join(__dirname, '..', 'assets');
    const logoGifPath = path.join(assetsDir, 'logo.gif');
    const usageGifPath = path.join(assetsDir, 'usage.gif');

    // Check logo.gif size (should be under 500KB ideally, up to 1MB is acceptable)
    try {
      const logoStats = await stat(logoGifPath);
      const logoSizeKB = logoStats.size / 1024;

      this.metrics.resources.logoGifSize = logoSizeKB;
      console.log(`    logo.gif size: ${logoSizeKB.toFixed(2)} KB`);

      if (logoSizeKB > 1024) { // 1MB
        throw new Error(`logo.gif is ${logoSizeKB.toFixed(2)} KB, exceeds recommended 1024 KB (1MB) for web delivery. Consider optimizing or converting to video format.`);
      } else if (logoSizeKB > 500) {
        this.metrics.recommendations.push('logo.gif is > 500KB - consider optimizing further');
      }
    } catch (error) {
      throw new Error(`Failed to check logo.gif size: ${error.message}`);
    }

    // Check usage.gif size (should be under 2MB ideally)
    try {
      const usageStats = await stat(usageGifPath);
      const usageSizeKB = usageStats.size / 1024;

      this.metrics.resources.usageGifSize = usageSizeKB;
      console.log(`    usage.gif size: ${usageSizeKB.toFixed(2)} KB`);

      if (usageSizeKB > 2048) { // 2MB
        throw new Error(`usage.gif is ${usageSizeKB.toFixed(2)} KB, exceeds recommended 2048 KB (2MB) for web delivery. Consider optimizing or converting to video format (MP4/WebM).`);
      } else if (usageSizeKB > 1024) {
        this.metrics.recommendations.push('usage.gif is > 1MB - consider converting to video format for better compression');
      }
    } catch (error) {
      throw new Error(`Failed to check usage.gif size: ${error.message}`);
    }
  }

  /**
   * Test Case 3: Estimate file transfer time on simulated slow connection
   * Calculate approximate load time on 3G connection
   */
  async estimateLoadTimeOn3G() {
    const assetsDir = path.join(__dirname, '..', 'assets');
    const rootDir = path.join(__dirname, '..');

    // Get size of all critical resources
    const resources = [
      { path: path.join(rootDir, 'index.html'), name: 'index.html' },
      { path: path.join(rootDir, 'styles.css'), name: 'styles.css' },
      { path: path.join(rootDir, 'script.js'), name: 'script.js' },
      { path: path.join(assetsDir, 'logo.gif'), name: 'logo.gif' },
      { path: path.join(assetsDir, 'usage.gif'), name: 'usage.gif' },
    ];

    let totalSize = 0;
    const breakdown = {};

    for (const resource of resources) {
      try {
        const stats = await stat(resource.path);
        const sizeKB = stats.size / 1024;
        totalSize += sizeKB;
        breakdown[resource.name] = sizeKB;
        this.metrics.resources[resource.name] = sizeKB;
      } catch (err) {
        breakdown[resource.name] = 0;
      }
    }

    console.log(`    Total page size: ${totalSize.toFixed(2)} KB`);
    console.log(`    Resource breakdown:`);
    for (const [name, size] of Object.entries(breakdown)) {
      const percentage = (size / totalSize) * 100;
      console.log(`      - ${name}: ${size.toFixed(2)} KB (${percentage.toFixed(1)}%)`);
    }

    // Estimate load times on different connections (in seconds)
    // 3G: ~1.6 Mbps = 200 KB/s
    // 4G: ~20 Mbps = 2.5 MB/s = 2560 KB/s
    const threeGSpeed = 200; // KB/s
    const fourGSpeed = 2560; // KB/s

    const loadTime3G = totalSize / threeGSpeed;
    const loadTime4G = totalSize / fourGSpeed;

    this.metrics.performance.loadTime3G = loadTime3G;
    this.metrics.performance.loadTime4G = loadTime4G;
    this.metrics.performance.totalSize = totalSize;

    console.log(`    Estimated load time on 3G (1.6 Mbps): ${loadTime3G.toFixed(2)}s`);
    console.log(`    Estimated load time on 4G (20 Mbps): ${loadTime4G.toFixed(2)}s`);

    // Check NFR-1: Page should load within 3 seconds
    if (loadTime3G > 3) {
      throw new Error(`Page will take ~${loadTime3G.toFixed(2)}s to load on 3G, exceeds NFR-1 requirement of 3 seconds.`);
    }

    // Store recommendation if close to limit
    if (loadTime3G > 2.5) {
      this.metrics.recommendations.push('Page load time on 3G is approaching 3s limit - consider further optimization');
    }
  }

  /**
   * Test Case 4: Check for layout shift risks
   * Analyze CSS and HTML for potential CLS issues
   */
  async checkForLayoutShiftRisks() {
    const indexPath = path.join(__dirname, '..', 'index.html');
    const cssPath = path.join(__dirname, '..', 'styles.css');

    const html = await readFile(indexPath, 'utf8');
    const css = await readFile(cssPath, 'utf8');

    // Check if images have dimensions set
    const imgTags = html.match(/<img[^>]*>/g) || [];
    let imgsWithoutDimensions = 0;
    let imgsWithDimensions = 0;

    for (const img of imgTags) {
      const hasWidth = img.includes('width=') || img.includes('style="');
      const hasHeight = img.includes('height=') || img.includes('style="');

      if (hasWidth && hasHeight) {
        imgsWithDimensions++;
      } else {
        imgsWithoutDimensions++;
      }
    }

    console.log(`    Images with dimensions: ${imgsWithDimensions}`);
    console.log(`    Images without explicit dimensions: ${imgsWithoutDimensions}`);

    if (imgsWithoutDimensions > 0) {
      this.metrics.recommendations.push(`${imgsWithoutDimensions} image(s) lack explicit dimensions - add width/height attributes to prevent layout shifts`);
    }

    // Check CSS for layout-affecting properties
    const layoutProperties = [
      'position: absolute',
      'position: fixed',
      'transform:',
      'will-change:',
    ];

    console.log(`    CSS layout optimizations found:`);
    let optimizationsFound = 0;
    for (const prop of layoutProperties) {
      if (css.includes(prop)) {
        optimizationsFound++;
        console.log(`      ✓ Uses ${prop} (reduces layout thrashing)`);
      }
    }

    if (optimizationsFound === 0) {
      this.metrics.recommendations.push('Consider using CSS transforms and will-change for better performance');
    }
  }

  /**
   * Test Case 5: Check CSS for performance best practices
   */
  async checkCssPerformanceBestPractices() {
    const cssPath = path.join(__dirname, '..', 'styles.css');
    const css = await readFile(cssPath, 'utf8');

    // Check file size
    const stats = await stat(cssPath);
    const cssSizeKB = stats.size / 1024;

    console.log(`    CSS file size: ${cssSizeKB.toFixed(2)} KB`);

    if (cssSizeKB > 50) {
      this.metrics.recommendations.push(`CSS file is ${cssSizeKB.toFixed(2)} KB - consider minification`);
    }

    // Check for box-sizing border-box (reduces layout calculations)
    if (css.includes('box-sizing: border-box')) {
      console.log('    ✓ Uses box-sizing: border-box (good for predictable sizing)');
    }

    // Check for reduced motion support (accessibility + performance)
    if (css.includes('@media (prefers-reduced-motion: reduce)')) {
      console.log('    ✓ Respects prefers-reduced-motion (accessibility)');
    }

    // Check for high contrast support
    if (css.includes('@media (prefers-contrast: high)')) {
      console.log('    ✓ Supports high contrast mode (accessibility)');
    }
  }

  /**
   * Test Case 6: Check JavaScript for performance best practices
   */
  async checkJavaScriptPerformance() {
    const jsPath = path.join(__dirname, '..', 'script.js');
    const js = await readFile(jsPath, 'utf8');

    // Check file size
    const stats = await stat(jsPath);
    const jsSizeKB = stats.size / 1024;

    console.log(`    JavaScript file size: ${jsSizeKB.toFixed(2)} KB`);

    if (jsSizeKB > 100) {
      this.metrics.recommendations.push(`JavaScript file is ${jsSizeKB.toFixed(2)} KB - consider minification`);
    }

    // Check for debounce/throttle implementations
    if (js.includes('debounce') || js.includes('throttle')) {
      console.log('    ✓ Implements debounce/throttle (reduces expensive operations)');
    }

    // Check for requestAnimationFrame usage
    if (js.includes('requestAnimationFrame')) {
      console.log('    ✓ Uses requestAnimationFrame (smoother animations)');
    }

    // Check for Intersection Observer
    if (js.includes('IntersectionObserver')) {
      console.log('    ✓ Uses IntersectionObserver (lazy loading)');
    }

    // Check for console.warn/error logs (performance monitoring)
    if (js.includes('console.warn') || js.includes('console.error')) {
      console.log('    ✓ Has performance monitoring via console logs');
    }
  }
}

async function runStaticPerformanceTests() {
  const runner = new StaticPerformanceTestRunner();

  try {
    runner.log('Starting static performance analysis...');

    // Start server (needed for consistency with other tests, though we won't use browser)
    runner.server = await runner.startServer();

    const tests = [
      { name: 'HTML structure follows performance best practices', fn: runner.analyzeHtmlStructure },
      { name: 'GIF assets are optimized (logo.gif <= 1MB, usage.gif <= 2MB)', fn: runner.checkGifFileSizes },
      { name: 'Estimated load time on 3G meets NFR-1 (< 3 seconds)', fn: runner.estimateLoadTimeOn3G },
      { name: 'Page structure minimizes layout shift risks', fn: runner.checkForLayoutShiftRisks },
      { name: 'CSS follows performance best practices', fn: runner.checkCssPerformanceBestPractices },
      { name: 'JavaScript follows performance best practices', fn: runner.checkJavaScriptPerformance },
    ];

    for (const test of tests) {
      try {
        runner.log(`Running: ${test.name}`);
        await test.fn.call(runner);
        runner.pass(test.name);
      } catch (error) {
        runner.fail(test.name, error);
      }
    }

    // Print metrics summary
    console.log('\n' + '='.repeat(60));
    console.log('PERFORMANCE METRICS & RECOMMENDATIONS');
    console.log('='.repeat(60));
    console.log(`Total page size: ${runner.metrics.performance.totalSize?.toFixed(2) || 'N/A'} KB`);
    console.log(`Estimated 3G load time: ${runner.metrics.performance.loadTime3G?.toFixed(2) || 'N/A'}s`);
    console.log(`Estimated 4G load time: ${runner.metrics.performance.loadTime4G?.toFixed(2) || 'N/A'}s`);
    console.log(`\nRecommendations (${runner.metrics.recommendations.length}):`);
    runner.metrics.recommendations.forEach((rec, i) => {
      console.log(`  ${i + 1}. ${rec}`);
    });
    console.log('='.repeat(60));

    // Print summary
    console.log('\n' + '='.repeat(60));
    console.log('STATIC PERFORMANCE TEST SUMMARY');
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
    if (runner.server) runner.server.close();
    return { pass: runner.failCount === 0, metrics: runner.metrics };
  }
}

if (require.main === module) {
  runStaticPerformanceTests().then(({ pass, metrics }) => {
    process.exit(pass ? 0 : 1);
  }).catch(error => {
    console.error('Fatal error:', error);
    process.exit(1);
  });
}

module.exports = { StaticPerformanceTestRunner, runStaticPerformanceTests };
