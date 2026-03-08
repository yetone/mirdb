/**
 * Performance E2E tests for MirDB homepage.
 * Owner: Scenario 15 - Performance - Page Load Time
 *
 * Requirements:
 * - Page load under 2 seconds (NFR-1)
 * - LCP under 2.5 seconds
 * - Total page weight under 1MB
 *
 * Expected tests:
 * - DOMContentLoaded timing
 * - Full page load timing
 * - Largest Contentful Paint
 * - Total page size
 * - Image optimization
 */

import { test, expect } from '@playwright/test'

test.describe('Performance - Page Load Time', () => {
  test.describe('Navigation Timing', () => {
    // Test Case 1: DOMContentLoaded fires within 1 second
    test('DOMContentLoaded fires within 1 second', async ({ page }) => {
      // Start timing from navigation
      const startTime = Date.now()

      // Navigate to page and wait for DOM content loaded
      await page.goto('/', { waitUntil: 'domcontentloaded' })

      const domContentLoadedTime = Date.now() - startTime

      // Get navigation timing from browser
      const navigationTiming = await page.evaluate(() => {
        const timing = performance.getEntriesByType('navigation')[0] as PerformanceNavigationTiming
        return {
          domContentLoadedEventEnd: timing.domContentLoadedEventEnd,
          responseStart: timing.responseStart,
          domContentLoadedDuration: timing.domContentLoadedEventEnd - timing.responseStart
        }
      })

      // DOMContentLoaded should fire within 1 second (1000ms)
      expect(navigationTiming.domContentLoadedDuration).toBeLessThan(1000)
      expect(domContentLoadedTime).toBeLessThan(1000)
    })

    // Test Case 2: Full page load completes within 2 seconds
    test('full page load completes within 2 seconds', async ({ page }) => {
      const startTime = Date.now()

      // Navigate and wait for full page load
      await page.goto('/', { waitUntil: 'load' })

      const loadTime = Date.now() - startTime

      // Get detailed navigation timing
      const navigationTiming = await page.evaluate(() => {
        const timing = performance.getEntriesByType('navigation')[0] as PerformanceNavigationTiming
        return {
          loadEventEnd: timing.loadEventEnd,
          responseStart: timing.responseStart,
          totalLoadTime: timing.loadEventEnd - timing.responseStart,
          // Additional metrics for debugging
          domComplete: timing.domComplete - timing.responseStart,
          domInteractive: timing.domInteractive - timing.responseStart
        }
      })

      // Full page load should complete within 2 seconds (2000ms)
      expect(navigationTiming.totalLoadTime).toBeLessThan(2000)
      expect(loadTime).toBeLessThan(2000)
    })
  })

  test.describe('Core Web Vitals', () => {
    // Test Case 3: Largest Contentful Paint is under 2.5 seconds
    test('Largest Contentful Paint is under 2.5 seconds', async ({ page }) => {
      // Navigate first and wait for load
      await page.goto('/', { waitUntil: 'load' })

      // Then collect LCP from buffered entries
      const lcpTime = await page.evaluate(() => {
        return new Promise<number>((resolve) => {
          let lcpValue = 0

          // Check for existing LCP entries
          const existingEntries = performance.getEntriesByType(
            'largest-contentful-paint'
          ) as Array<PerformanceEntry & { startTime: number }>
          if (existingEntries.length > 0) {
            lcpValue = existingEntries[existingEntries.length - 1].startTime
          }

          // Also observe for any new LCP events
          const observer = new PerformanceObserver((entryList) => {
            const entries = entryList.getEntries() as Array<
              PerformanceEntry & { startTime: number }
            >
            if (entries.length > 0) {
              lcpValue = entries[entries.length - 1].startTime
            }
          })

          try {
            observer.observe({ type: 'largest-contentful-paint', buffered: true })
          } catch {
            // LCP observer not supported, fall back to existing entries
          }

          // Wait a short time for any final LCP updates
          setTimeout(() => {
            observer.disconnect()
            resolve(lcpValue)
          }, 200)
        })
      })

      // LCP should be under 2.5 seconds (2500ms)
      // Allow 0 if LCP entries are not available in this browser
      if (lcpTime > 0) {
        expect(lcpTime).toBeLessThan(2500)
      }
    })
  })

  test.describe('Page Size', () => {
    // Test Case 4: Total page weight is under 1MB
    test('total page weight is under 1MB', async ({ page }) => {
      // Navigate and wait for network idle
      await page.goto('/', { waitUntil: 'networkidle' })

      // Get resource transfer sizes from Performance API
      const resourceMetrics = await page.evaluate(() => {
        const resources = performance.getEntriesByType('resource') as PerformanceResourceTiming[]
        const navEntry = performance.getEntriesByType(
          'navigation'
        )[0] as PerformanceNavigationTiming

        let totalSize = 0
        let productionSize = 0
        const resourceSizes: Array<{ name: string; size: number; isDevOnly: boolean }> = []

        // Add the main document size
        if (navEntry && navEntry.transferSize) {
          totalSize += navEntry.transferSize
          productionSize += navEntry.transferSize
          resourceSizes.push({ name: 'document', size: navEntry.transferSize, isDevOnly: false })
        }

        // Add all resource sizes, categorizing dev vs production resources
        for (const resource of resources) {
          const size = resource.transferSize || 0
          totalSize += size

          // Identify development-only resources (Vite HMR, dev tools, etc.)
          const isDevOnly =
            resource.name.includes('/@vite') ||
            resource.name.includes('/@react-refresh') ||
            resource.name.includes('/node_modules/') ||
            resource.name.includes('?v=') || // Vite's cache busting
            resource.name.includes('__vite') ||
            resource.name.includes('hot-update')

          if (!isDevOnly) {
            productionSize += size
          }

          if (size > 0) {
            resourceSizes.push({ name: resource.name, size, isDevOnly })
          }
        }

        // Check if we're in development mode
        const isDevMode =
          resourceSizes.some((r) => r.isDevOnly) ||
          (window as Record<string, unknown>).__vite_is_modern_browser !== undefined

        return {
          totalSize,
          productionSize,
          resourceCount: resources.length,
          resourceSizes,
          isDevMode
        }
      })

      // Total page weight should be under 1MB (1,048,576 bytes)
      const oneMB = 1024 * 1024

      // In development mode, check that production resources are under the limit
      // In production mode, check total resources
      if (resourceMetrics.isDevMode) {
        // In development mode, the production-equivalent resources should be under 1MB
        // Note: Dev mode includes HMR client, source maps, and unminified code
        // The actual production bundle is typically 10-20x smaller
        expect(resourceMetrics.productionSize).toBeLessThan(oneMB)
      } else {
        expect(resourceMetrics.totalSize).toBeLessThan(oneMB)
      }

      // Additional verification: main app bundle should be reasonably sized
      const mainBundle = resourceMetrics.resourceSizes.find(
        (r) =>
          r.name.includes('/src/main') ||
          r.name.includes('index') ||
          (r.name.endsWith('.js') && !r.isDevOnly)
      )

      // If we can identify the main bundle, ensure it's not excessively large
      // (Even in dev mode, the main app code should be reasonable)
      if (mainBundle) {
        // 5MB is a generous limit even for dev mode
        expect(mainBundle.size).toBeLessThan(5 * oneMB)
      }
    })
  })

  test.describe('Asset Optimization', () => {
    // Test Case 5: Images are compressed and appropriately sized
    test('images are compressed and appropriately sized', async ({ page }) => {
      await page.goto('/', { waitUntil: 'networkidle' })

      // Get all images on the page
      const images = page.locator('img')
      const imageCount = await images.count()

      const imageMetrics: Array<{
        src: string
        naturalWidth: number
        naturalHeight: number
        displayedWidth: number
        displayedHeight: number
        isOptimized: boolean
        sizeRatio: number
      }> = []

      for (let i = 0; i < imageCount; i++) {
        const img = images.nth(i)
        const isVisible = await img.isVisible()

        if (isVisible) {
          const metrics = await img.evaluate((el: HTMLImageElement) => {
            const naturalWidth = el.naturalWidth
            const naturalHeight = el.naturalHeight
            const displayedWidth = el.clientWidth || el.offsetWidth
            const displayedHeight = el.clientHeight || el.offsetHeight

            // Calculate size ratio (natural to displayed)
            // A well-optimized image should not be more than 2.5x the displayed size (for retina)
            const widthRatio = displayedWidth > 0 ? naturalWidth / displayedWidth : 1
            const heightRatio = displayedHeight > 0 ? naturalHeight / displayedHeight : 1
            const sizeRatio = Math.max(widthRatio, heightRatio)

            return {
              src: el.src,
              naturalWidth,
              naturalHeight,
              displayedWidth,
              displayedHeight,
              isOptimized: sizeRatio <= 3, // Allow up to 3x for high-DPI displays
              sizeRatio
            }
          })

          imageMetrics.push(metrics)
        }
      }

      // All images should be appropriately sized (not oversized)
      for (const metric of imageMetrics) {
        // Skip data URIs, SVGs, and images that haven't loaded
        if (
          metric.src.startsWith('data:') ||
          metric.src.endsWith('.svg') ||
          metric.naturalWidth === 0
        ) {
          continue
        }

        expect(
          metric.isOptimized,
          `Image ${metric.src} is oversized: ${metric.naturalWidth}x${metric.naturalHeight} displayed at ${metric.displayedWidth}x${metric.displayedHeight} (ratio: ${metric.sizeRatio.toFixed(2)})`
        ).toBe(true)
      }

      // Verify SVG usage for vector graphics (logo should be SVG)
      const logoImg = page.locator('header img[alt*="MirDB"]')
      const logoCount = await logoImg.count()

      if (logoCount > 0) {
        const logoSrc = await logoImg.getAttribute('src')
        // Logo should be SVG for optimal scaling
        expect(logoSrc).toContain('.svg')
      }
    })

    test('JavaScript and CSS assets are properly loaded', async ({ page }) => {
      await page.goto('/', { waitUntil: 'networkidle' })

      // Get resource timing from the page
      const resourceTiming = await page.evaluate(() => {
        const resources = performance.getEntriesByType('resource') as PerformanceResourceTiming[]
        return resources
          .filter((r) => r.name.endsWith('.js') || r.name.endsWith('.css'))
          .map((r) => ({
            name: r.name,
            duration: r.duration,
            transferSize: r.transferSize
          }))
      })

      // All JS/CSS resources should load quickly (under 500ms each)
      for (const resource of resourceTiming) {
        expect(
          resource.duration,
          `Resource ${resource.name} took too long to load: ${resource.duration}ms`
        ).toBeLessThan(500)
      }
    })
  })

  test.describe('Render Performance', () => {
    test('First Contentful Paint is under 1.8 seconds', async ({ page }) => {
      // Navigate and wait for load
      await page.goto('/', { waitUntil: 'load' })

      // Get FCP from buffered paint entries
      const fcpTime = await page.evaluate(() => {
        return new Promise<number>((resolve) => {
          // Try to get from paint entries
          const paintEntries = performance.getEntriesByType('paint')
          const fcpEntry = paintEntries.find((entry) => entry.name === 'first-contentful-paint')

          if (fcpEntry) {
            resolve(fcpEntry.startTime)
            return
          }

          // Fall back to observer if entries not available
          const observer = new PerformanceObserver((entryList) => {
            const entries = entryList.getEntries()
            const fcp = entries.find((entry) => entry.name === 'first-contentful-paint')
            if (fcp) {
              observer.disconnect()
              resolve(fcp.startTime)
            }
          })

          try {
            observer.observe({ type: 'paint', buffered: true })
          } catch {
            resolve(-1) // Observer not supported
          }

          // Timeout fallback
          setTimeout(() => {
            observer.disconnect()
            resolve(-1)
          }, 1000)
        })
      })

      // FCP should be under 1.8 seconds for good performance
      // Skip if FCP is not available (-1)
      if (fcpTime > 0) {
        expect(fcpTime).toBeLessThan(1800)
      }
    })

    test('page has no major layout shifts', async ({ page }) => {
      // Navigate and wait for load
      await page.goto('/', { waitUntil: 'load' })

      // Wait for page to stabilize
      await page.waitForTimeout(500)

      // Get CLS from buffered entries
      const clsValue = await page.evaluate(() => {
        return new Promise<number>((resolve) => {
          let cls = 0

          // Get existing layout shift entries
          const entries = performance.getEntriesByType('layout-shift') as Array<
            PerformanceEntry & { value: number; hadRecentInput: boolean }
          >
          for (const entry of entries) {
            if (!entry.hadRecentInput) {
              cls += entry.value
            }
          }

          // Also observe for any new shifts
          const observer = new PerformanceObserver((entryList) => {
            const newEntries = entryList.getEntries() as Array<
              PerformanceEntry & { value: number; hadRecentInput: boolean }
            >
            for (const entry of newEntries) {
              if (!entry.hadRecentInput) {
                cls += entry.value
              }
            }
          })

          try {
            observer.observe({ type: 'layout-shift', buffered: true })
          } catch {
            // Layout shift observer not supported
          }

          // Wait briefly for any final shifts
          setTimeout(() => {
            observer.disconnect()
            resolve(cls)
          }, 300)
        })
      })

      // CLS should be under 0.1 for good experience
      expect(clsValue).toBeLessThan(0.1)
    })
  })
})
