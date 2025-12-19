import { test, expect, Page } from '@playwright/test'

/**
 * Performance tests for the MirDB homepage
 * Verifies NFR-2: page loads within 3 seconds on 3G and achieves 90+ performance score
 */

test.describe('Page Performance', () => {
  /**
   * TC1: Run Lighthouse-like performance audit
   * Uses Navigation Timing API to measure performance metrics
   * Performance score is calculated based on key metrics
   */
  test('TC1: performance score is 90 or higher', async ({ page }) => {
    // Navigate to the page and collect performance metrics
    await page.goto('/', { waitUntil: 'networkidle' })

    // Get performance metrics using Navigation Timing API
    const metrics = await page.evaluate(() => {
      const nav = performance.getEntriesByType('navigation')[0] as PerformanceNavigationTiming
      const paint = performance.getEntriesByType('paint')

      const fcp = paint.find(entry => entry.name === 'first-contentful-paint')?.startTime || 0

      return {
        // Time to First Byte
        ttfb: nav.responseStart - nav.requestStart,
        // First Contentful Paint
        fcp: fcp,
        // DOM Content Loaded
        dcl: nav.domContentLoadedEventEnd - nav.domContentLoadedEventStart,
        // Load complete time
        loadTime: nav.loadEventEnd - nav.startTime,
        // DOM Interactive
        domInteractive: nav.domInteractive - nav.startTime,
        // Total transfer size
        transferSize: nav.transferSize,
      }
    })

    // Calculate a simplified performance score based on key metrics
    // This simulates Lighthouse scoring methodology
    // FCP < 1.8s = 100 points, FCP > 3s = 0 points (linear interpolation)
    const fcpScore = Math.max(0, Math.min(100, 100 - ((metrics.fcp - 1000) / 20)))

    // TTI (using domInteractive as proxy) < 3.8s = 100 points
    const ttiScore = Math.max(0, Math.min(100, 100 - ((metrics.domInteractive - 2000) / 20)))

    // Load time < 3s = 100 points
    const loadScore = Math.max(0, Math.min(100, 100 - ((metrics.loadTime - 1500) / 20)))

    // Weighted average (similar to Lighthouse weights)
    const performanceScore = Math.round(
      (fcpScore * 0.3) + (ttiScore * 0.4) + (loadScore * 0.3)
    )

    console.log('Performance Metrics:', {
      fcp: `${metrics.fcp.toFixed(2)}ms`,
      domInteractive: `${metrics.domInteractive.toFixed(2)}ms`,
      loadTime: `${metrics.loadTime.toFixed(2)}ms`,
      calculatedScore: performanceScore,
    })

    // Verify performance score is 90+
    // Using a more lenient threshold for test environment
    expect(performanceScore).toBeGreaterThanOrEqual(90)
  })

  /**
   * TC2: Measure page load time on simulated 3G
   * Uses Chrome DevTools Protocol for network throttling
   */
  test('TC2: page fully loads within 3 seconds on 3G', async ({ page, context }) => {
    // Get the CDP session for network throttling
    const client = await context.newCDPSession(page)

    // Simulate 3G network conditions
    // 3G typically has: 1.6 Mbps down, 750 Kbps up, 150ms RTT
    await client.send('Network.emulateNetworkConditions', {
      offline: false,
      downloadThroughput: (1.6 * 1024 * 1024) / 8, // 1.6 Mbps in bytes/s
      uploadThroughput: (750 * 1024) / 8, // 750 Kbps in bytes/s
      latency: 150, // 150ms RTT
    })

    const startTime = Date.now()

    // Navigate and wait for network idle (all resources loaded)
    await page.goto('/', { waitUntil: 'networkidle' })

    const loadTime = Date.now() - startTime

    console.log(`3G Page Load Time: ${loadTime}ms`)

    // Page should load within 3000ms (3 seconds) on 3G
    expect(loadTime).toBeLessThanOrEqual(3000)

    // Verify page actually rendered
    await expect(page.locator('h1')).toBeVisible()
    await expect(page.locator('.hero')).toBeVisible()
  })

  /**
   * TC3: Check First Contentful Paint metric
   * FCP should be under 1.8 seconds
   */
  test('TC3: FCP is under 1.8 seconds', async ({ page }) => {
    await page.goto('/', { waitUntil: 'networkidle' })

    // Get FCP from Paint Timing API
    const fcp = await page.evaluate(() => {
      const paintEntries = performance.getEntriesByType('paint')
      const fcpEntry = paintEntries.find(entry => entry.name === 'first-contentful-paint')
      return fcpEntry?.startTime || 0
    })

    console.log(`First Contentful Paint: ${fcp.toFixed(2)}ms`)

    // FCP should be under 1800ms (1.8 seconds)
    expect(fcp).toBeLessThan(1800)
  })

  /**
   * TC4: Check total page weight
   * Total page size (transfer size / compressed) should be under 100KB per design spec
   * This uses the Resource Timing API to measure actual bytes transferred over the network
   * which accounts for compression (gzip/brotli) as served by the web server
   */
  test('TC4: total page size is under 100KB', async ({ page }) => {
    await page.goto('/', { waitUntil: 'networkidle' })

    // Get transfer sizes from Resource Timing API
    // transferSize represents the actual bytes transferred (after compression)
    // encodedBodySize is the compressed body size
    const resourceMetrics = await page.evaluate(() => {
      const resources = performance.getEntriesByType('resource') as PerformanceResourceTiming[]
      const navigation = performance.getEntriesByType('navigation')[0] as PerformanceNavigationTiming

      const resourceDetails = resources.map(r => ({
        name: r.name.replace(/^https?:\/\/[^\/]+/, ''),
        transferSize: r.transferSize,
        encodedBodySize: r.encodedBodySize,
        decodedBodySize: r.decodedBodySize,
        initiatorType: r.initiatorType,
      }))

      // Include the main document
      const documentSize = navigation.transferSize || navigation.encodedBodySize || 0

      return {
        resources: resourceDetails,
        documentTransferSize: documentSize,
        documentEncodedSize: navigation.encodedBodySize,
        documentDecodedSize: navigation.decodedBodySize,
      }
    })

    // Calculate total transfer size (what actually goes over the network)
    const resourceTransferSize = resourceMetrics.resources.reduce(
      (sum, r) => sum + (r.transferSize || r.encodedBodySize || 0),
      0
    )
    const totalTransferSize = resourceTransferSize + resourceMetrics.documentTransferSize
    const totalTransferSizeKB = totalTransferSize / 1024

    // Calculate total uncompressed size for reference
    const resourceDecodedSize = resourceMetrics.resources.reduce(
      (sum, r) => sum + (r.decodedBodySize || 0),
      0
    )
    const totalDecodedSize = resourceDecodedSize + resourceMetrics.documentDecodedSize
    const totalDecodedSizeKB = totalDecodedSize / 1024

    console.log('Page Resources (Transfer Size):')
    console.log(`  Document: ${(resourceMetrics.documentTransferSize / 1024).toFixed(2)}KB`)
    resourceMetrics.resources.forEach(r => {
      const size = r.transferSize || r.encodedBodySize || 0
      console.log(`  ${r.initiatorType}: ${r.name} - ${(size / 1024).toFixed(2)}KB`)
    })
    console.log(`Total Transfer Size: ${totalTransferSizeKB.toFixed(2)}KB (compressed)`)
    console.log(`Total Decoded Size: ${totalDecodedSizeKB.toFixed(2)}KB (uncompressed)`)

    // The design spec states "total bundle should be under 100KB"
    // For modern web serving with gzip/brotli compression, we measure transfer size
    // The vite build shows gzip sizes: ~0.36KB + ~1.87KB + ~64.50KB = ~66.73KB
    expect(totalTransferSizeKB).toBeLessThan(100)
  })
})

/**
 * Helper to measure various performance metrics
 */
async function collectPerformanceMetrics(page: Page) {
  return await page.evaluate(() => {
    const nav = performance.getEntriesByType('navigation')[0] as PerformanceNavigationTiming
    const paint = performance.getEntriesByType('paint')

    return {
      // Navigation timing
      dns: nav.domainLookupEnd - nav.domainLookupStart,
      tcp: nav.connectEnd - nav.connectStart,
      ttfb: nav.responseStart - nav.requestStart,
      download: nav.responseEnd - nav.responseStart,
      domInteractive: nav.domInteractive - nav.startTime,
      domComplete: nav.domComplete - nav.startTime,
      loadComplete: nav.loadEventEnd - nav.startTime,

      // Paint timing
      firstPaint: paint.find(e => e.name === 'first-paint')?.startTime || 0,
      fcp: paint.find(e => e.name === 'first-contentful-paint')?.startTime || 0,

      // Resource timing
      resourceCount: performance.getEntriesByType('resource').length,
      totalTransferSize: (performance.getEntriesByType('resource') as PerformanceResourceTiming[])
        .reduce((sum, r) => sum + (r.transferSize || 0), 0),
    }
  })
}
