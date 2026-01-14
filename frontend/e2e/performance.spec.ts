import { test, expect, type Page, type BrowserContext } from '@playwright/test'
import { playAudit } from 'playwright-lighthouse'
import type { OutputMode, Flags } from 'lighthouse'

/**
 * Performance tests for page load time verification
 * Validates NFR-1: Page load time under 2 seconds on standard connections
 *
 * Test cases cover:
 * 1. Total page load time (< 2s)
 * 2. Lighthouse performance score (>= 85)
 * 3. First Contentful Paint (< 1.8s)
 * 4. Largest Contentful Paint (< 2.5s)
 */

// Thresholds based on PRD NFR-1 requirements
const PERFORMANCE_THRESHOLDS = {
  pageLoadTime: 2000, // 2 seconds in milliseconds
  lighthouseScore: 85, // Minimum performance score
  fcp: 1800, // First Contentful Paint in milliseconds
  lcp: 2500, // Largest Contentful Paint in milliseconds
}

/**
 * Helper function to collect Web Vitals metrics using Performance API
 */
async function collectPerformanceMetrics(page: Page): Promise<{
  fcp: number | null
  lcp: number | null
  domContentLoaded: number
  load: number
}> {
  return await page.evaluate(() => {
    return new Promise<{
      fcp: number | null
      lcp: number | null
      domContentLoaded: number
      load: number
    }>((resolve) => {
      // Use PerformanceObserver for LCP
      let lcp: number | null = null

      const lcpObserver = new PerformanceObserver((list) => {
        const entries = list.getEntries()
        const lastEntry = entries[entries.length - 1]
        lcp = lastEntry.startTime
      })

      try {
        lcpObserver.observe({ type: 'largest-contentful-paint', buffered: true })
      } catch {
        // LCP observer not supported
      }

      // Give some time for metrics to be collected
      setTimeout(() => {
        lcpObserver.disconnect()

        // Get FCP from paint timing
        const paintEntries = performance.getEntriesByType('paint')
        const fcpEntry = paintEntries.find(
          (entry) => entry.name === 'first-contentful-paint'
        )
        const fcp = fcpEntry ? fcpEntry.startTime : null

        // Get navigation timing
        const navEntry = performance.getEntriesByType(
          'navigation'
        )[0] as PerformanceNavigationTiming
        const domContentLoaded = navEntry
          ? navEntry.domContentLoadedEventEnd - navEntry.startTime
          : 0
        const load = navEntry ? navEntry.loadEventEnd - navEntry.startTime : 0

        resolve({ fcp, lcp, domContentLoaded, load })
      }, 1000)
    })
  })
}

test.describe('Performance - Page Load Time (NFR-1)', () => {
  /**
   * Test Case 1: Load homepage on standard connection (4G)
   * Expected: Page load completes in under 2 seconds
   */
  test('page load completes in under 2 seconds on standard connection', async ({
    page,
    context,
  }) => {
    // Simulate 4G network conditions
    const cdpSession = await context.newCDPSession(page)
    await cdpSession.send('Network.emulateNetworkConditions', {
      offline: false,
      downloadThroughput: (4 * 1024 * 1024) / 8, // 4 Mbps
      uploadThroughput: (3 * 1024 * 1024) / 8, // 3 Mbps
      latency: 50, // 50ms RTT
    })

    // Clear cache for clean test
    await context.clearCookies()

    // Measure page load time
    const startTime = Date.now()

    // Navigate to homepage and wait for load event
    await page.goto('/', { waitUntil: 'load' })

    const loadTime = Date.now() - startTime

    // Also collect performance metrics from the browser
    const metrics = await collectPerformanceMetrics(page)

    console.log(`Page load time: ${loadTime}ms`)
    console.log(`DOM Content Loaded: ${metrics.domContentLoaded}ms`)
    console.log(`Full Load: ${metrics.load}ms`)

    // Verify page actually loaded
    await expect(page.getByTestId('hero-section')).toBeVisible()

    // Assert page load is under threshold
    // Use the larger of measured time or browser's load event
    const effectiveLoadTime = Math.max(loadTime, metrics.load)
    expect(effectiveLoadTime).toBeLessThan(PERFORMANCE_THRESHOLDS.pageLoadTime)
  })

  /**
   * Test Case 2: Run Lighthouse performance audit
   * Expected: Performance score is 85 or higher
   */
  test('Lighthouse performance score is 85 or higher', async ({
    playwright,
  }) => {
    // Launch a fresh browser with remote debugging for Lighthouse
    const browser = await playwright.chromium.launch({
      args: ['--remote-debugging-port=9222'],
    })

    const page = await browser.newPage()
    await page.goto('http://localhost:5173/', { waitUntil: 'networkidle' })

    // Run Lighthouse audit
    const lighthouseConfig: { port: number; thresholds: { performance: number }; config?: { extends?: string; settings?: Flags } } = {
      port: 9222,
      thresholds: {
        performance: PERFORMANCE_THRESHOLDS.lighthouseScore,
      },
      config: {
        extends: 'lighthouse:default',
        settings: {
          formFactor: 'desktop' as const,
          throttling: {
            rttMs: 40,
            throughputKbps: 10 * 1024,
            cpuSlowdownMultiplier: 1,
            requestLatencyMs: 0,
            downloadThroughputKbps: 10 * 1024,
            uploadThroughputKbps: 10 * 1024,
          },
          screenEmulation: {
            mobile: false,
            width: 1350,
            height: 940,
            deviceScaleFactor: 1,
            disabled: false,
          },
          emulatedUserAgent:
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
          onlyCategories: ['performance'] as OutputMode[],
        },
      },
    }

    try {
      const result = await playAudit({
        page,
        ...lighthouseConfig,
      })

      console.log(`Lighthouse Performance Score: ${result.lhr.categories.performance.score! * 100}`)

      // The playAudit will throw if thresholds aren't met
      // If we get here, the test passed
      expect(result.lhr.categories.performance.score! * 100).toBeGreaterThanOrEqual(
        PERFORMANCE_THRESHOLDS.lighthouseScore
      )
    } finally {
      await browser.close()
    }
  })

  /**
   * Test Case 3: Measure First Contentful Paint
   * Expected: FCP is under 1.8 seconds
   */
  test('First Contentful Paint is under 1.8 seconds', async ({
    page,
    context,
  }) => {
    // Clear cache for clean test
    await context.clearCookies()

    // Navigate to homepage
    await page.goto('/', { waitUntil: 'domcontentloaded' })

    // Wait for content to be visible
    await expect(page.getByTestId('hero-section')).toBeVisible()

    // Collect FCP metric
    const metrics = await collectPerformanceMetrics(page)

    console.log(`First Contentful Paint: ${metrics.fcp}ms`)

    // Assert FCP is under threshold
    expect(metrics.fcp).not.toBeNull()
    expect(metrics.fcp!).toBeLessThan(PERFORMANCE_THRESHOLDS.fcp)
  })

  /**
   * Test Case 4: Measure Largest Contentful Paint
   * Expected: LCP is under 2.5 seconds
   */
  test('Largest Contentful Paint is under 2.5 seconds', async ({
    page,
    context,
  }) => {
    // Clear cache for clean test
    await context.clearCookies()

    // Navigate to homepage
    await page.goto('/', { waitUntil: 'load' })

    // Wait for all content to be visible
    await expect(page.getByTestId('hero-section')).toBeVisible()
    await expect(page.getByTestId('features-section')).toBeVisible()

    // Collect LCP metric
    const metrics = await collectPerformanceMetrics(page)

    console.log(`Largest Contentful Paint: ${metrics.lcp}ms`)

    // Assert LCP is under threshold
    expect(metrics.lcp).not.toBeNull()
    expect(metrics.lcp!).toBeLessThan(PERFORMANCE_THRESHOLDS.lcp)
  })
})

/**
 * Additional performance tests for robustness
 */
test.describe('Performance - Additional Metrics', () => {
  test('DOM Content Loaded event fires quickly', async ({ page }) => {
    const metrics: { domContentLoaded: number } = { domContentLoaded: 0 }

    // Capture DOMContentLoaded timing
    page.on('domcontentloaded', () => {
      metrics.domContentLoaded = Date.now()
    })

    const startTime = Date.now()
    await page.goto('/', { waitUntil: 'domcontentloaded' })
    const domLoadTime = metrics.domContentLoaded - startTime

    console.log(`DOM Content Loaded in: ${domLoadTime}ms`)

    // DOM should load faster than full page
    expect(domLoadTime).toBeLessThan(PERFORMANCE_THRESHOLDS.pageLoadTime)
  })

  test('page has no significant render-blocking resources', async ({
    page,
  }) => {
    // Navigate and capture performance entries
    await page.goto('/', { waitUntil: 'networkidle' })

    const blockingResources = await page.evaluate(() => {
      const resources = performance.getEntriesByType(
        'resource'
      ) as PerformanceResourceTiming[]
      return resources
        .filter((r) => {
          // Check for render-blocking CSS/JS
          const isCSS = r.name.endsWith('.css')
          const isJS = r.name.endsWith('.js')
          // Resources that took longer than 500ms could be blocking
          return (isCSS || isJS) && r.duration > 500
        })
        .map((r) => ({ name: r.name, duration: r.duration }))
    })

    console.log('Potentially blocking resources:', blockingResources)

    // Should have minimal blocking resources
    expect(blockingResources.length).toBeLessThan(3)
  })
})
