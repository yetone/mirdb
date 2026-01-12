import { test, expect, type Page, type CDPSession } from '@playwright/test'

/**
 * Performance test suite for page load metrics.
 * Tests Core Web Vitals: FCP, LCP, CLS, and total page load time.
 *
 * Requirements from NFR-1: Page load time under 3 seconds on standard broadband connection
 *
 * @see https://web.dev/vitals/
 */

interface PerformanceMetrics {
  fcp: number | null
  lcp: number | null
  cls: number
  loadTime: number
}

/**
 * Collect Core Web Vitals metrics using CDP (Chrome DevTools Protocol)
 */
async function collectPerformanceMetrics(page: Page): Promise<PerformanceMetrics> {
  const cdpSession: CDPSession = await page.context().newCDPSession(page)

  // Enable performance domain
  await cdpSession.send('Performance.enable')

  // Navigate and wait for load
  const startTime = Date.now()

  await page.goto('/', { waitUntil: 'networkidle' })

  const loadTime = Date.now() - startTime

  // Get paint timing metrics (FCP)
  const paintMetrics = await page.evaluate(() => {
    const entries = performance.getEntriesByType('paint')
    const fcpEntry = entries.find((entry) => entry.name === 'first-contentful-paint')
    return {
      fcp: fcpEntry ? fcpEntry.startTime : null,
    }
  })

  // Get LCP using PerformanceObserver
  const lcpValue = await page.evaluate(() => {
    return new Promise<number | null>((resolve) => {
      let lcpValue: number | null = null

      // Check if LCP entries already exist
      const lcpEntries = performance.getEntriesByType('largest-contentful-paint')
      if (lcpEntries.length > 0) {
        const lastEntry = lcpEntries[lcpEntries.length - 1] as PerformanceEntry & { startTime: number }
        lcpValue = lastEntry.startTime
      }

      // Also set up observer for any new entries
      const observer = new PerformanceObserver((list) => {
        const entries = list.getEntries()
        for (const entry of entries) {
          lcpValue = entry.startTime
        }
      })

      try {
        observer.observe({ type: 'largest-contentful-paint', buffered: true })
      } catch {
        // LCP not supported
      }

      // Give time for LCP to be reported
      setTimeout(() => {
        observer.disconnect()
        resolve(lcpValue)
      }, 500)
    })
  })

  // Get CLS using layout-shift entries
  const clsValue = await page.evaluate(() => {
    return new Promise<number>((resolve) => {
      let clsValue = 0
      let sessionValue = 0
      let sessionEntries: PerformanceEntry[] = []

      // Check if layout-shift entries already exist
      const layoutShiftEntries = performance.getEntriesByType('layout-shift') as Array<
        PerformanceEntry & { hadRecentInput: boolean; value: number }
      >

      for (const entry of layoutShiftEntries) {
        if (!entry.hadRecentInput) {
          const firstSessionEntry = sessionEntries[0] as PerformanceEntry | undefined
          if (firstSessionEntry && entry.startTime - firstSessionEntry.startTime < 5000) {
            sessionValue += entry.value
            sessionEntries.push(entry)
          } else {
            sessionValue = entry.value
            sessionEntries = [entry]
          }
          if (sessionValue > clsValue) {
            clsValue = sessionValue
          }
        }
      }

      // Also observe for new shifts
      const observer = new PerformanceObserver((list) => {
        for (const entry of list.getEntries() as Array<
          PerformanceEntry & { hadRecentInput: boolean; value: number }
        >) {
          if (!entry.hadRecentInput) {
            const firstSessionEntry = sessionEntries[0] as PerformanceEntry | undefined
            if (firstSessionEntry && entry.startTime - firstSessionEntry.startTime < 5000) {
              sessionValue += entry.value
              sessionEntries.push(entry)
            } else {
              sessionValue = entry.value
              sessionEntries = [entry]
            }
            if (sessionValue > clsValue) {
              clsValue = sessionValue
            }
          }
        }
      })

      try {
        observer.observe({ type: 'layout-shift', buffered: true })
      } catch {
        // layout-shift not supported
      }

      setTimeout(() => {
        observer.disconnect()
        resolve(clsValue)
      }, 500)
    })
  })

  await cdpSession.detach()

  return {
    fcp: paintMetrics.fcp,
    lcp: lcpValue,
    cls: clsValue,
    loadTime,
  }
}

test.describe('Performance - Page Load', () => {
  test.describe.configure({ mode: 'serial' })

  let metrics: PerformanceMetrics

  test.beforeAll(async ({ browser }) => {
    // Create a fresh context with clean cache
    const context = await browser.newContext({
      bypassCSP: true,
    })
    const page = await context.newPage()

    metrics = await collectPerformanceMetrics(page)

    // Log metrics for debugging
    console.log('Performance Metrics:', {
      'FCP (ms)': metrics.fcp,
      'LCP (ms)': metrics.lcp,
      'CLS': metrics.cls,
      'Load Time (ms)': metrics.loadTime,
    })

    await context.close()
  })

  test('First Contentful Paint (FCP) occurs within 1.8 seconds', async () => {
    expect(metrics.fcp).not.toBeNull()
    expect(metrics.fcp).toBeLessThanOrEqual(1800)
  })

  test('Largest Contentful Paint (LCP) occurs within 2.5 seconds', async () => {
    expect(metrics.lcp).not.toBeNull()
    expect(metrics.lcp).toBeLessThanOrEqual(2500)
  })

  test('Cumulative Layout Shift (CLS) score is less than 0.1', async () => {
    expect(metrics.cls).toBeLessThan(0.1)
  })

  test('Page fully loads within 3 seconds on broadband', async () => {
    expect(metrics.loadTime).toBeLessThanOrEqual(3000)
  })
})
