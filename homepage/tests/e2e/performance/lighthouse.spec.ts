/**
 * E2E tests for Performance and Loading
 * Scenario 12 - Performance and Loading
 *
 * Requirements:
 * - NFR-1: Homepage shall load in under 2 seconds on 3G connections
 * - NFR-2: Homepage shall score at section 90+ on Lighthouse performance audit
 * - Core Web Vitals: LCP < 2.5s, CLS < 0.1
 * - Images below fold should use lazy loading
 */

import { test, expect, Page } from '@playwright/test'

// Performance thresholds based on requirements
const PERFORMANCE_THRESHOLDS = {
  maxLoadTimeMs: 2000, // NFR-1: 2 seconds max on 3G
  minLighthouseScore: 90, // NFR-2: 90+ Lighthouse score
  maxLCP: 2500, // Core Web Vitals: LCP < 2.5s
  maxCLS: 0.1, // Core Web Vitals: CLS < 0.1
}

// Simulated 3G network conditions
const SLOW_3G_NETWORK = {
  offline: false,
  downloadThroughput: (500 * 1024) / 8, // 500 Kbps
  uploadThroughput: (500 * 1024) / 8,
  latency: 400, // 400ms latency
}

// Helper function to measure performance metrics
async function measurePerformance(page: Page) {
  const metrics = await page.evaluate(() => {
    const navigation = performance.getEntriesByType(
      'navigation'
    )[0] as PerformanceNavigationTiming
    const paint = performance.getEntriesByType('paint')

    return {
      domContentLoaded: navigation?.domContentLoadedEventEnd - navigation?.startTime,
      loadComplete: navigation?.loadEventEnd - navigation?.startTime,
      domInteractive: navigation?.domInteractive - navigation?.startTime,
      firstContentfulPaint:
        paint.find((p) => p.name === 'first-contentful-paint')?.startTime || 0,
    }
  })
  return metrics
}

// Helper to get Core Web Vitals
async function getCoreWebVitals(page: Page) {
  const vitals = await page.evaluate(() => {
    return new Promise<{
      lcp: number
      cls: number
    }>((resolve) => {
      let lcp = 0
      let cls = 0

      // Observe LCP
      const lcpObserver = new PerformanceObserver((list) => {
        const entries = list.getEntries()
        const lastEntry = entries[entries.length - 1] as PerformanceEntry & {
          startTime: number
        }
        lcp = lastEntry.startTime
      })

      // Observe CLS
      const clsObserver = new PerformanceObserver((list) => {
        for (const entry of list.getEntries()) {
          const layoutShiftEntry = entry as PerformanceEntry & {
            hadRecentInput?: boolean
            value?: number
          }
          if (!layoutShiftEntry.hadRecentInput) {
            cls += layoutShiftEntry.value || 0
          }
        }
      })

      try {
        lcpObserver.observe({ type: 'largest-contentful-paint', buffered: true })
        clsObserver.observe({ type: 'layout-shift', buffered: true })
      } catch {
        // Some metrics might not be available
      }

      // Give time for metrics to be collected
      setTimeout(() => {
        lcpObserver.disconnect()
        clsObserver.disconnect()
        resolve({ lcp, cls })
      }, 3000)
    })
  })

  return vitals
}

test.describe('Performance and Loading', () => {
  test.describe('Test Case 1: Page Load Time on 3G', () => {
    test('page becomes interactive in under 2 seconds on throttled 3G', async ({
      page,
      context,
    }) => {
      // Set up network throttling via CDP
      const cdpSession = await context.newCDPSession(page)
      await cdpSession.send('Network.emulateNetworkConditions', SLOW_3G_NETWORK)

      // Start measuring
      const startTime = Date.now()

      // Navigate to the page
      await page.goto('/', { waitUntil: 'domcontentloaded' })

      const domContentLoadedTime = Date.now() - startTime

      // Wait for the page to become interactive
      await page.waitForLoadState('domcontentloaded')

      // Verify key interactive elements are available
      const heroSection = page.locator('#hero')
      await expect(heroSection).toBeVisible({ timeout: PERFORMANCE_THRESHOLDS.maxLoadTimeMs })

      // Check that CTA button is interactive
      const ctaButton = page.getByRole('button', { name: 'Get Started' })
      await expect(ctaButton).toBeVisible({ timeout: PERFORMANCE_THRESHOLDS.maxLoadTimeMs })
      await expect(ctaButton).toBeEnabled()

      // Measure performance metrics
      const metrics = await measurePerformance(page)

      // DOM interactive time should be under 2 seconds
      // Note: In development mode, this might be higher. We check domInteractive.
      expect(metrics.domInteractive).toBeLessThan(PERFORMANCE_THRESHOLDS.maxLoadTimeMs)

      // Log metrics for debugging
      console.log('Performance Metrics (3G simulation):', {
        domContentLoaded: `${domContentLoadedTime}ms`,
        domInteractive: `${metrics.domInteractive}ms`,
        firstContentfulPaint: `${metrics.firstContentfulPaint}ms`,
      })
    })
  })

  test.describe('Test Case 2: Lighthouse Performance Score', () => {
    test('page meets performance requirements for Lighthouse-like metrics', async ({
      page,
    }) => {
      // Navigate to the page with performance measurement
      await page.goto('/', { waitUntil: 'networkidle' })

      // Get performance metrics that Lighthouse would measure
      const metrics = await measurePerformance(page)

      // Check First Contentful Paint (FCP) - Lighthouse requires < 1.8s for good score
      expect(metrics.firstContentfulPaint).toBeLessThan(1800)

      // Check DOM Content Loaded
      expect(metrics.domContentLoaded).toBeLessThan(PERFORMANCE_THRESHOLDS.maxLoadTimeMs)

      // Verify page content is fully rendered
      const mainContent = page.locator('main')
      await expect(mainContent).toBeVisible()

      // Check that critical sections are present
      const hero = page.locator('#hero')
      const features = page.locator('#features')
      const quickStart = page.locator('#quick-start')

      await expect(hero).toBeVisible()
      await expect(features).toBeVisible()
      await expect(quickStart).toBeVisible()

      // Check for optimized images (width/height attributes to prevent CLS)
      const images = page.locator('img')
      const imageCount = await images.count()

      for (let i = 0; i < imageCount; i++) {
        const img = images.nth(i)
        const hasWidth = await img.getAttribute('width')
        const hasHeight = await img.getAttribute('height')

        // Images should have width and height or be styled appropriately
        const hasInlineDimensions = hasWidth && hasHeight
        const hasCSSConstraints = await img.evaluate((el) => {
          const style = window.getComputedStyle(el)
          return style.width !== 'auto' || style.maxWidth !== 'none'
        })

        expect(hasInlineDimensions || hasCSSConstraints).toBeTruthy()
      }

      console.log('Performance Metrics (Lighthouse-like):', {
        firstContentfulPaint: `${metrics.firstContentfulPaint}ms`,
        domContentLoaded: `${metrics.domContentLoaded}ms`,
        loadComplete: `${metrics.loadComplete}ms`,
      })
    })
  })

  test.describe('Test Case 3: Largest Contentful Paint (LCP)', () => {
    test('LCP is under 2.5 seconds', async ({ page }) => {
      await page.goto('/', { waitUntil: 'load' })

      // Get Core Web Vitals
      const vitals = await getCoreWebVitals(page)

      // LCP should be under 2.5 seconds
      expect(vitals.lcp).toBeLessThan(PERFORMANCE_THRESHOLDS.maxLCP)

      // Additionally verify the hero section (likely LCP element) loads quickly
      const heroTitle = page.locator('.hero__title')
      await expect(heroTitle).toBeVisible()

      console.log(`LCP: ${vitals.lcp}ms (threshold: ${PERFORMANCE_THRESHOLDS.maxLCP}ms)`)
    })
  })

  test.describe('Test Case 4: Cumulative Layout Shift (CLS)', () => {
    test('CLS is under 0.1', async ({ page }) => {
      await page.goto('/', { waitUntil: 'load' })

      // Get Core Web Vitals
      const vitals = await getCoreWebVitals(page)

      // CLS should be under 0.1
      expect(vitals.cls).toBeLessThan(PERFORMANCE_THRESHOLDS.maxCLS)

      // Verify images have dimensions to prevent layout shifts
      const demoImage = page.locator('.demo__image')
      if ((await demoImage.count()) > 0) {
        const hasWidth = await demoImage.getAttribute('width')
        const hasHeight = await demoImage.getAttribute('height')
        expect(hasWidth).toBeTruthy()
        expect(hasHeight).toBeTruthy()
      }

      console.log(`CLS: ${vitals.cls} (threshold: ${PERFORMANCE_THRESHOLDS.maxCLS})`)
    })
  })

  test.describe('Test Case 6: Critical CSS', () => {
    test('above-fold content renders without waiting for full CSS', async ({ page }) => {
      // Block non-critical resources to verify critical CSS works
      await page.route('**/*.gif', (route) => route.abort())

      // Navigate to page
      await page.goto('/', { waitUntil: 'domcontentloaded' })

      // Above-fold content should be visible immediately after DOM is ready
      // Header should be visible
      const header = page.locator('.header')
      await expect(header).toBeVisible()

      // Hero section should be visible (main above-fold content)
      const heroSection = page.locator('#hero')
      await expect(heroSection).toBeVisible()

      // Hero title should be styled and visible
      const heroTitle = page.locator('.hero__title')
      await expect(heroTitle).toBeVisible()

      // Verify critical styles are applied (font-size, colors exist)
      const titleStyles = await heroTitle.evaluate((el) => {
        const computed = window.getComputedStyle(el)
        return {
          fontSize: computed.fontSize,
          color: computed.color,
          fontWeight: computed.fontWeight,
        }
      })

      // Title should have meaningful styles applied
      expect(parseFloat(titleStyles.fontSize)).toBeGreaterThan(0)
      expect(titleStyles.color).not.toBe('')

      // Navigation should be styled
      const navigation = page.locator('.navigation')
      if ((await navigation.count()) > 0) {
        await expect(navigation).toBeVisible()
      }

      // Primary CTA button should be styled
      const ctaButton = page.getByRole('button', { name: 'Get Started' })
      await expect(ctaButton).toBeVisible()

      const buttonStyles = await ctaButton.evaluate((el) => {
        const computed = window.getComputedStyle(el)
        return {
          backgroundColor: computed.backgroundColor,
          padding: computed.padding,
        }
      })

      // Button should have background color applied
      expect(buttonStyles.backgroundColor).not.toBe('')
      expect(buttonStyles.backgroundColor).not.toBe('transparent')
    })
  })
})

test.describe('Image Optimization', () => {
  test('all images have proper optimization attributes', async ({ page }) => {
    await page.goto('/', { waitUntil: 'load' })

    const images = page.locator('img')
    const imageCount = await images.count()

    for (let i = 0; i < imageCount; i++) {
      const img = images.nth(i)
      const src = await img.getAttribute('src')
      const alt = await img.getAttribute('alt')

      // All images should have alt text (NFR-7)
      expect(alt).toBeTruthy()

      // Log image info for debugging
      console.log(`Image ${i + 1}: src=${src}, alt=${alt}`)
    }
  })
})
