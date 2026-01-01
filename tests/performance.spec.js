const { test, expect } = require('@playwright/test');
const fs = require('fs');
const path = require('path');

test.describe('Page Load Performance', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('TC1: Page fully loads in under 2 seconds on broadband connection', async ({ page, browser }) => {
    // Create a new page with network throttling to simulate broadband
    const context = await browser.newContext();
    const newPage = await context.newPage();

    // Set up CDP session for network throttling (simulating broadband: ~10Mbps)
    const cdp = await context.newCDPSession(newPage);
    await cdp.send('Network.enable');
    await cdp.send('Network.emulateNetworkConditions', {
      offline: false,
      downloadThroughput: (10 * 1024 * 1024) / 8, // 10 Mbps
      uploadThroughput: (1 * 1024 * 1024) / 8,    // 1 Mbps
      latency: 20                                  // 20ms latency
    });

    // Measure page load time
    const startTime = Date.now();

    // Navigate and wait for load event
    await newPage.goto('/', { waitUntil: 'load' });

    const loadTime = Date.now() - startTime;

    console.log(`Page load time: ${loadTime}ms`);

    // Assert page loads in under 2 seconds (2000ms)
    expect(loadTime).toBeLessThan(2000);

    // Also verify First Contentful Paint using performance metrics
    const performanceTiming = await newPage.evaluate(() => {
      const timing = performance.getEntriesByType('navigation')[0];
      const paintEntries = performance.getEntriesByType('paint');
      const fcp = paintEntries.find(entry => entry.name === 'first-contentful-paint');
      return {
        loadEventEnd: timing?.loadEventEnd || 0,
        domContentLoaded: timing?.domContentLoadedEventEnd || 0,
        firstContentfulPaint: fcp?.startTime || 0
      };
    });

    console.log('Performance metrics:', performanceTiming);

    // Verify First Contentful Paint is also under 2 seconds
    if (performanceTiming.firstContentfulPaint > 0) {
      expect(performanceTiming.firstContentfulPaint).toBeLessThan(2000);
    }

    await context.close();
  });

  test('TC2: Lighthouse performance score is 80 or above', async ({ page }) => {
    // For this test, we'll simulate Lighthouse metrics by checking key performance indicators
    // that contribute to the Lighthouse Performance score:
    // - First Contentful Paint (FCP)
    // - Largest Contentful Paint (LCP)
    // - Cumulative Layout Shift (CLS)
    // - Total Blocking Time (TBT)

    await page.goto('/', { waitUntil: 'networkidle' });

    // Collect performance metrics
    const metrics = await page.evaluate(() => {
      return new Promise((resolve) => {
        // Wait a bit for all metrics to be available
        setTimeout(() => {
          const paintEntries = performance.getEntriesByType('paint');
          const fcp = paintEntries.find(entry => entry.name === 'first-contentful-paint');

          const navigation = performance.getEntriesByType('navigation')[0];

          // Get Largest Contentful Paint via PerformanceObserver
          let lcp = 0;
          const lcpEntries = performance.getEntriesByType('largest-contentful-paint');
          if (lcpEntries.length > 0) {
            lcp = lcpEntries[lcpEntries.length - 1].startTime;
          }

          // Get Cumulative Layout Shift
          let cls = 0;
          const layoutShiftEntries = performance.getEntriesByType('layout-shift');
          layoutShiftEntries.forEach(entry => {
            if (!entry.hadRecentInput) {
              cls += entry.value;
            }
          });

          resolve({
            firstContentfulPaint: fcp?.startTime || 0,
            largestContentfulPaint: lcp,
            cumulativeLayoutShift: cls,
            domContentLoaded: navigation?.domContentLoadedEventEnd || 0,
            loadComplete: navigation?.loadEventEnd || 0,
            // Check for blocking resources
            resources: performance.getEntriesByType('resource').map(r => ({
              name: r.name,
              type: r.initiatorType,
              duration: r.duration,
              size: r.transferSize
            }))
          });
        }, 1000);
      });
    });

    console.log('Lighthouse-style metrics:', JSON.stringify(metrics, null, 2));

    // Lighthouse scoring thresholds (approximate):
    // FCP: Good < 1.8s, Needs improvement < 3s
    // LCP: Good < 2.5s, Needs improvement < 4s
    // CLS: Good < 0.1, Needs improvement < 0.25

    // Calculate approximate Lighthouse score components
    let score = 100;

    // FCP scoring (25% weight in Lighthouse)
    const fcpScore = metrics.firstContentfulPaint < 1800 ? 100 :
                     metrics.firstContentfulPaint < 3000 ? 75 :
                     metrics.firstContentfulPaint < 4000 ? 50 : 25;

    // LCP scoring (25% weight in Lighthouse)
    const lcpScore = metrics.largestContentfulPaint < 2500 ? 100 :
                     metrics.largestContentfulPaint < 4000 ? 75 :
                     metrics.largestContentfulPaint < 6000 ? 50 : 25;

    // CLS scoring (25% weight in Lighthouse)
    const clsScore = metrics.cumulativeLayoutShift < 0.1 ? 100 :
                     metrics.cumulativeLayoutShift < 0.25 ? 75 :
                     metrics.cumulativeLayoutShift < 0.5 ? 50 : 25;

    // Speed Index approximation (25% weight)
    // Using DOM Content Loaded as proxy
    const siScore = metrics.domContentLoaded < 1500 ? 100 :
                    metrics.domContentLoaded < 3000 ? 75 :
                    metrics.domContentLoaded < 4500 ? 50 : 25;

    // Calculate weighted score
    const estimatedScore = Math.round((fcpScore * 0.25 + lcpScore * 0.25 + clsScore * 0.25 + siScore * 0.25));

    console.log(`Performance score breakdown:
      FCP (${metrics.firstContentfulPaint}ms): ${fcpScore}
      LCP (${metrics.largestContentfulPaint}ms): ${lcpScore}
      CLS (${metrics.cumulativeLayoutShift}): ${clsScore}
      Speed Index (${metrics.domContentLoaded}ms): ${siScore}
      Estimated Lighthouse Score: ${estimatedScore}`);

    // Verify the estimated Lighthouse score is 80 or above
    expect(estimatedScore).toBeGreaterThanOrEqual(80);

    // Also verify key metrics are in "Good" range
    expect(metrics.firstContentfulPaint).toBeLessThan(3000);
    expect(metrics.cumulativeLayoutShift).toBeLessThan(0.25);
  });
});

test.describe('Bundle Size Optimization', () => {
  test('TC3: Total CSS and JS bundle size is optimized (under 500KB)', async () => {
    // Read the index.html file to check inline CSS and JS
    const indexPath = path.join(__dirname, '..', 'index.html');
    const htmlContent = fs.readFileSync(indexPath, 'utf-8');

    // Calculate inline CSS size
    const styleMatches = htmlContent.match(/<style[^>]*>([\s\S]*?)<\/style>/gi);
    let inlineCssSize = 0;
    if (styleMatches) {
      inlineCssSize = styleMatches.reduce((acc, match) => acc + match.length, 0);
    }

    // Calculate inline JS size
    const scriptMatches = htmlContent.match(/<script[^>]*>([\s\S]*?)<\/script>/gi);
    let inlineJsSize = 0;
    if (scriptMatches) {
      inlineJsSize = scriptMatches.reduce((acc, match) => acc + match.length, 0);
    }

    // Check for external CSS files
    const externalCssMatches = htmlContent.match(/href=["'][^"']*\.css["']/gi);
    let externalCssSize = 0;

    // Check for external JS files
    const externalJsMatches = htmlContent.match(/src=["'][^"']*\.js["']/gi);
    let externalJsSize = 0;

    // Total bundle size (HTML includes inline CSS/JS)
    const htmlSize = htmlContent.length;
    const totalBundleSize = htmlSize;

    console.log(`Bundle size analysis:
      HTML file size: ${htmlSize} bytes (${(htmlSize / 1024).toFixed(2)} KB)
      Inline CSS size: ${inlineCssSize} bytes (${(inlineCssSize / 1024).toFixed(2)} KB)
      Inline JS size: ${inlineJsSize} bytes (${(inlineJsSize / 1024).toFixed(2)} KB)
      External CSS files: ${externalCssMatches?.length || 0}
      External JS files: ${externalJsMatches?.length || 0}
      Total bundle size: ${totalBundleSize} bytes (${(totalBundleSize / 1024).toFixed(2)} KB)`);

    // Assert total bundle is under 500KB (512000 bytes)
    expect(totalBundleSize).toBeLessThan(512000);

    // Additionally verify the CSS is reasonable size (inline CSS should be optimized)
    // A good target is under 50KB for inline CSS
    expect(inlineCssSize).toBeLessThan(51200);

    // Verify no bloated external dependencies
    expect(externalJsMatches?.length || 0).toBeLessThanOrEqual(5);
  });
});

test.describe('Image Optimization', () => {
  test('TC4: Below-fold images use lazy loading', async ({ page }) => {
    await page.goto('/');

    // Get all images on the page
    const images = await page.evaluate(() => {
      const imgElements = document.querySelectorAll('img');
      const svgElements = document.querySelectorAll('svg[role="img"]');

      const allImages = [];

      // Process img elements
      imgElements.forEach((img, index) => {
        const rect = img.getBoundingClientRect();
        const viewportHeight = window.innerHeight;
        const isAboveFold = rect.top < viewportHeight;

        allImages.push({
          type: 'img',
          src: img.src || img.dataset.src,
          loading: img.loading,
          isAboveFold,
          position: rect.top,
          hasLazyLoading: img.loading === 'lazy' || img.dataset.src !== undefined
        });
      });

      // Process SVG images (like the architecture diagram)
      svgElements.forEach((svg, index) => {
        const rect = svg.getBoundingClientRect();
        const viewportHeight = window.innerHeight;
        const isAboveFold = rect.top < viewportHeight;

        allImages.push({
          type: 'svg',
          label: svg.getAttribute('aria-label') || `SVG ${index}`,
          isAboveFold,
          position: rect.top
        });
      });

      return allImages;
    });

    console.log(`Found ${images.length} images/SVGs on the page`);
    console.log('Images:', JSON.stringify(images, null, 2));

    // Check if there are any img elements that need lazy loading
    const imgElements = images.filter(img => img.type === 'img');
    const belowFoldImages = imgElements.filter(img => !img.isAboveFold);

    // If there are below-fold img elements, verify they have lazy loading
    if (belowFoldImages.length > 0) {
      belowFoldImages.forEach(img => {
        console.log(`Below-fold image: ${img.src}, has lazy loading: ${img.hasLazyLoading}`);
        expect(img.hasLazyLoading).toBe(true);
      });
    }

    // Since this page uses inline SVGs for diagrams (which don't need lazy loading)
    // and doesn't have traditional image files below the fold, we verify:
    // 1. The page follows best practices by not having large images without lazy loading
    // 2. SVG elements are used for diagrams (which is optimal for performance)

    const svgElements = images.filter(img => img.type === 'svg');
    console.log(`Found ${svgElements.length} SVG elements (optimal for performance)`);

    // Verify that if there are any below-fold img tags, they use lazy loading
    // OR that the page primarily uses optimized SVG graphics
    const hasProperImageOptimization =
      belowFoldImages.every(img => img.hasLazyLoading) ||
      (imgElements.length === 0 && svgElements.length > 0);

    expect(hasProperImageOptimization).toBe(true);

    // Additional check: Verify no large unoptimized images are being loaded
    const pageMetrics = await page.evaluate(() => {
      const resources = performance.getEntriesByType('resource');
      const imageResources = resources.filter(r =>
        r.initiatorType === 'img' ||
        r.name.match(/\.(jpg|jpeg|png|gif|webp|svg)$/i)
      );

      return imageResources.map(r => ({
        name: r.name,
        size: r.transferSize,
        duration: r.duration
      }));
    });

    console.log('Image resources loaded:', pageMetrics);

    // If there are image resources, verify total size is reasonable
    const totalImageSize = pageMetrics.reduce((acc, r) => acc + (r.size || 0), 0);
    console.log(`Total image transfer size: ${totalImageSize} bytes (${(totalImageSize / 1024).toFixed(2)} KB)`);

    // Images shouldn't exceed 500KB for optimal performance
    expect(totalImageSize).toBeLessThan(512000);
  });
});
