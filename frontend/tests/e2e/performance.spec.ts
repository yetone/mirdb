import { test, expect } from '@playwright/test'

/**
 * Performance E2E Tests for Homepage (REQ-10)
 *
 * These tests verify that the homepage meets Core Web Vitals thresholds:
 * - FCP (First Contentful Paint): < 1.8s
 * - LCP (Largest Contentful Paint): < 2.5s
 * - TTI (Time to Interactive): < 3s
 */
test.describe('Page Load Performance - REQ-10', () => {
  test.describe.configure({ mode: 'serial' })

  /**
   * Test Case 1: Measure First Contentful Paint (FCP)
   * Expected: FCP < 1.8 seconds (Core Web Vitals threshold)
   */
  test('FCP should be less than 1.8 seconds', async ({ page }) => {
    // Clear cache and cookies to simulate cold load
    await page.context().clearCookies()

    // Start performance measurement
    await page.goto('/', { waitUntil: 'networkidle' })

    // Get performance metrics using the Performance API
    const performanceMetrics = await page.evaluate(() => {
      return new Promise<{ fcp: number | null }>((resolve) => {
        // Try to get FCP from Performance Observer entries
        const perfEntries = performance.getEntriesByType('paint')
        const fcpEntry = perfEntries.find(entry => entry.name === 'first-contentful-paint')

        if (fcpEntry) {
          resolve({ fcp: fcpEntry.startTime })
        } else {
          // Fallback: use PerformanceObserver
          const observer = new PerformanceObserver((list) => {
            for (const entry of list.getEntries()) {
              if (entry.name === 'first-contentful-paint') {
                observer.disconnect()
                resolve({ fcp: entry.startTime })
                return
              }
            }
          })

          try {
            observer.observe({ type: 'paint', buffered: true })
          } catch {
            // If observer fails, fall back to timing
            const timing = performance.timing
            const fcp = timing.domContentLoadedEventStart - timing.navigationStart
            resolve({ fcp: fcp > 0 ? fcp : null })
          }

          // Timeout fallback
          setTimeout(() => {
            observer.disconnect()
            resolve({ fcp: null })
          }, 5000)
        }
      })
    })

    // Verify FCP metric exists and is within threshold
    expect(performanceMetrics.fcp).not.toBeNull()
    if (performanceMetrics.fcp !== null) {
      const fcpInSeconds = performanceMetrics.fcp / 1000
      console.log(`FCP: ${fcpInSeconds.toFixed(3)}s`)
      expect(fcpInSeconds).toBeLessThan(1.8)
    }
  })

  /**
   * Test Case 2: Measure Largest Contentful Paint (LCP)
   * Expected: LCP < 2.5 seconds (Core Web Vitals threshold)
   */
  test('LCP should be less than 2.5 seconds', async ({ page }) => {
    // Clear cache to simulate cold load
    await page.context().clearCookies()

    // Navigate and wait for load
    await page.goto('/', { waitUntil: 'networkidle' })

    // Get LCP metric
    const lcpMetric = await page.evaluate(() => {
      return new Promise<number | null>((resolve) => {
        // Check for existing LCP entries
        const existingEntries = performance.getEntriesByType('largest-contentful-paint')
        if (existingEntries.length > 0) {
          const lastEntry = existingEntries[existingEntries.length - 1] as PerformanceEntry & { startTime: number }
          resolve(lastEntry.startTime)
          return
        }

        // Set up observer for LCP
        let lcpValue: number | null = null
        const observer = new PerformanceObserver((list) => {
          const entries = list.getEntries()
          if (entries.length > 0) {
            const lastEntry = entries[entries.length - 1] as PerformanceEntry & { startTime: number }
            lcpValue = lastEntry.startTime
          }
        })

        try {
          observer.observe({ type: 'largest-contentful-paint', buffered: true })
        } catch {
          // Fallback for browsers without LCP support
          resolve(null)
          return
        }

        // Wait for LCP to settle (usually happens within 2.5s of load)
        setTimeout(() => {
          observer.disconnect()
          resolve(lcpValue)
        }, 3000)
      })
    })

    // Verify LCP metric exists and is within threshold
    expect(lcpMetric).not.toBeNull()
    if (lcpMetric !== null) {
      const lcpInSeconds = lcpMetric / 1000
      console.log(`LCP: ${lcpInSeconds.toFixed(3)}s`)
      expect(lcpInSeconds).toBeLessThan(2.5)
    }
  })

  /**
   * Test Case 3: Measure Time to Interactive (TTI)
   * Expected: TTI < 3 seconds
   *
   * Note: True TTI requires Long Tasks API which may not be available.
   * We approximate TTI using domInteractive + additional checks.
   */
  test('TTI should be less than 3 seconds', async ({ page }) => {
    // Clear cache to simulate cold load
    await page.context().clearCookies()

    // Start timing
    const startTime = Date.now()

    // Navigate to the page
    await page.goto('/', { waitUntil: 'domcontentloaded' })

    // Wait for the page to be interactive (all main elements loaded)
    await page.waitForSelector('[data-testid="home-page"]', { state: 'visible' })
    await page.waitForSelector('[data-testid="hero-section"]', { state: 'visible' })

    // Ensure JavaScript has finished execution by checking for interactive elements
    const primaryButton = page.getByTestId('hero-cta-primary')
    await expect(primaryButton).toBeVisible()
    await expect(primaryButton).toBeEnabled()

    // Verify navigation is interactive
    const navHeader = page.locator('header nav')
    await expect(navHeader).toBeVisible()

    const endTime = Date.now()
    const ttiApprox = (endTime - startTime) / 1000

    // Also get the browser's timing metrics
    const browserTTI = await page.evaluate(() => {
      const timing = performance.timing
      // Calculate approximate TTI using domInteractive
      const tti = timing.domInteractive - timing.navigationStart
      return tti > 0 ? tti : null
    })

    console.log(`Approximate TTI (navigation): ${ttiApprox.toFixed(3)}s`)
    if (browserTTI !== null) {
      console.log(`Browser domInteractive: ${(browserTTI / 1000).toFixed(3)}s`)
    }

    // Use the more accurate of the two measurements
    const measuredTTI = browserTTI !== null ? Math.max(ttiApprox, browserTTI / 1000) : ttiApprox
    expect(measuredTTI).toBeLessThan(3)
  })

  /**
   * Test Case 4: Verify overall page load is under 3 seconds
   * This is a holistic test that combines multiple metrics
   */
  test('Homepage should load completely within 3 seconds', async ({ page }) => {
    // Clear cache to simulate cold load
    await page.context().clearCookies()

    const startTime = Date.now()

    // Navigate and wait for full load
    await page.goto('/', { waitUntil: 'networkidle' })

    // Verify all main sections are loaded
    await expect(page.getByTestId('home-page')).toBeVisible()
    await expect(page.getByTestId('hero-section')).toBeVisible()
    await expect(page.getByTestId('features-section')).toBeVisible()
    await expect(page.getByTestId('how-it-works-section')).toBeVisible()

    const loadTime = (Date.now() - startTime) / 1000
    console.log(`Total page load time: ${loadTime.toFixed(3)}s`)

    // Verify load time is under 3 seconds as per REQ-10
    expect(loadTime).toBeLessThan(3)
  })

  /**
   * Test Case 5: Verify performance under simulated network throttling
   * Tests Fast 3G conditions (supplementary test - not required by REQ-10)
   *
   * Note: REQ-10 specifies "standard connections" for the 3 second requirement.
   * This test verifies the page remains functional on slower connections.
   */
  test('Homepage loads on Fast 3G connection (supplementary)', async ({ page, context }) => {
    // Configure CDP session for network throttling
    const client = await context.newCDPSession(page)

    // Simulate Fast 3G: 1.6 Mbps download, 750 Kbps upload, 150ms latency
    await client.send('Network.emulateNetworkConditions', {
      offline: false,
      downloadThroughput: (1.6 * 1024 * 1024) / 8, // 1.6 Mbps in bytes/sec
      uploadThroughput: (750 * 1024) / 8, // 750 Kbps in bytes/sec
      latency: 150, // 150ms
    })

    const startTime = Date.now()

    await page.goto('/', { waitUntil: 'domcontentloaded' })

    // Wait for critical content to be visible
    await expect(page.getByTestId('hero-section')).toBeVisible()
    await expect(page.getByTestId('hero-headline')).toBeVisible()

    const loadTime = (Date.now() - startTime) / 1000
    console.log(`Page load time on Fast 3G: ${loadTime.toFixed(3)}s`)

    // Under throttled conditions, the page should still be usable within 30 seconds
    // This is a supplementary test - the 3 second requirement applies to standard connections only
    expect(loadTime).toBeLessThan(30)

    // Verify the page is interactive even on slow connections
    const primaryCta = page.getByTestId('hero-cta-primary')
    await expect(primaryCta).toBeVisible()
  })
})
