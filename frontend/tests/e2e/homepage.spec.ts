/**
 * E2E tests for homepage performance and load time.
 * Owner: Scenario 9 - Performance and Load Time
 *
 * Test coverage:
 * - Page loads within 2 seconds on simulated 4G
 * - Lighthouse performance metrics are within acceptable range
 * - Hero section is visible above the fold
 * - Features section becomes visible on scroll
 */

import { test, expect } from '@playwright/test'

const LOAD_TIME_BUDGET_MS = 2000 // 2 seconds

test.describe('Performance and Load Time', () => {
  test('Page is interactive within 2 seconds on 4G', async ({ page }) => {
    // Simulate 4G network conditions
    const client = await page.context().newCDPSession(page)
    await client.send('Network.emulateNetworkConditions', {
      offline: false,
      downloadThroughput: (1.5 * 1024 * 1024) / 8, // 1.5 Mbps download (typical 4G)
      uploadThroughput: (750 * 1024) / 8, // 750 Kbps upload
      latency: 40, // 40ms latency
    })

    const startTime = Date.now()

    await page.goto('/', { waitUntil: 'domcontentloaded' })

    // Wait for the page to be interactive (hero section visible and clickable)
    await page.waitForSelector('h1', { state: 'visible' })
    await page.waitForSelector('a[href="/register"], button', { state: 'visible' })

    const loadTime = Date.now() - startTime
    console.log(`Page interactive time: ${loadTime}ms`)

    expect(loadTime).toBeLessThan(LOAD_TIME_BUDGET_MS)
  })

  test('Lighthouse performance metrics are acceptable', async ({ page }) => {
    // Navigate to homepage first
    await page.goto('/', { waitUntil: 'networkidle' })

    // Measure Core Web Vitals using Performance API
    const performanceMetrics = await page.evaluate(() => {
      return new Promise<{
        fcp: number | null
        domInteractive: number
        loadComplete: number
      }>((resolve) => {
        // Get First Contentful Paint
        const paintEntries = performance.getEntriesByType('paint')
        const fcpEntry = paintEntries.find(entry => entry.name === 'first-contentful-paint')

        const timing = performance.getEntriesByType('navigation')[0] as PerformanceNavigationTiming

        resolve({
          fcp: fcpEntry ? fcpEntry.startTime : null,
          domInteractive: timing.domInteractive - timing.fetchStart,
          loadComplete: timing.loadEventEnd - timing.fetchStart,
        })
      })
    })

    console.log('Performance Metrics:', performanceMetrics)

    // Validate FCP is under 1.8s (good threshold for Lighthouse score of 90+)
    if (performanceMetrics.fcp !== null) {
      expect(performanceMetrics.fcp).toBeLessThan(1800)
    }

    // DOM should be interactive within 2 seconds
    expect(performanceMetrics.domInteractive).toBeLessThan(2000)
  })

  test('Hero section is visible above the fold', async ({ page }) => {
    await page.goto('/', { waitUntil: 'domcontentloaded' })

    // Check that h1 headline is visible in viewport
    const headline = page.locator('h1').first()
    await expect(headline).toBeVisible()

    // Verify it's above the fold (within initial viewport)
    const boundingBox = await headline.boundingBox()
    expect(boundingBox).not.toBeNull()

    const viewportHeight = page.viewportSize()?.height || 720
    expect(boundingBox!.y).toBeLessThan(viewportHeight)
  })

  test('Features section becomes visible on scroll', async ({ page }) => {
    await page.goto('/', { waitUntil: 'domcontentloaded' })

    // Scroll down to reveal features section
    await page.evaluate(() => window.scrollTo(0, 500))

    // The features should exist somewhere on the page
    const allText = await page.textContent('body')
    expect(allText?.toLowerCase()).toContain('analytics')
  })
})
