/**
 * Performance Requirements E2E Tests
 * Owner: Scenario 10 - Performance Requirements
 *
 * Verifies the homepage loads within 2 seconds and has minimal bundle impact.
 *
 * Test coverage:
 * - TC1: Load homepage on broadband - interactive within 2 seconds
 * - TC2: No unnecessary large dependencies imported
 * - TC3: No render-blocking resources delay initial paint
 * - TC4: LCP occurs within 2.5 seconds
 * - TC5: CLS is less than 0.1
 */

import { test, expect, type Page, type CDPSession } from '@playwright/test'

/**
 * Helper to get performance metrics using Performance API
 */
async function getPerformanceMetrics(page: Page) {
  return page.evaluate(() => {
    const performance = window.performance
    const timing = performance.timing
    const navigationEntry = performance.getEntriesByType('navigation')[0] as PerformanceNavigationTiming

    return {
      // Time to Interactive approximation (domInteractive)
      domInteractive: navigationEntry?.domInteractive || timing.domInteractive - timing.navigationStart,
      // DOM Content Loaded
      domContentLoaded: navigationEntry?.domContentLoadedEventEnd || timing.domContentLoadedEventEnd - timing.navigationStart,
      // Load complete
      loadComplete: navigationEntry?.loadEventEnd || timing.loadEventEnd - timing.navigationStart,
      // First Paint
      firstPaint: performance.getEntriesByType('paint').find(e => e.name === 'first-paint')?.startTime || 0,
      // First Contentful Paint
      firstContentfulPaint: performance.getEntriesByType('paint').find(e => e.name === 'first-contentful-paint')?.startTime || 0,
    }
  })
}

/**
 * Helper to measure LCP using PerformanceObserver
 */
async function measureLCP(page: Page): Promise<number> {
  return page.evaluate(() => {
    return new Promise<number>((resolve) => {
      let lcpValue = 0

      // Create observer for LCP
      const observer = new PerformanceObserver((entryList) => {
        const entries = entryList.getEntries()
        const lastEntry = entries[entries.length - 1] as PerformanceEntry & { startTime: number }
        if (lastEntry) {
          lcpValue = lastEntry.startTime
        }
      })

      observer.observe({ type: 'largest-contentful-paint', buffered: true })

      // Wait a bit for LCP to be recorded, then resolve
      setTimeout(() => {
        observer.disconnect()
        resolve(lcpValue)
      }, 3000)
    })
  })
}

/**
 * Helper to measure CLS using PerformanceObserver
 */
async function measureCLS(page: Page): Promise<number> {
  return page.evaluate(() => {
    return new Promise<number>((resolve) => {
      let clsValue = 0

      const observer = new PerformanceObserver((entryList) => {
        for (const entry of entryList.getEntries()) {
          const layoutShiftEntry = entry as PerformanceEntry & {
            hadRecentInput: boolean;
            value: number
          }
          if (!layoutShiftEntry.hadRecentInput) {
            clsValue += layoutShiftEntry.value
          }
        }
      })

      observer.observe({ type: 'layout-shift', buffered: true })

      // Wait for layout shifts to be recorded
      setTimeout(() => {
        observer.disconnect()
        resolve(clsValue)
      }, 3000)
    })
  })
}

/**
 * Helper to check for render-blocking resources
 */
async function getRenderBlockingResources(page: Page) {
  return page.evaluate(() => {
    const resources = performance.getEntriesByType('resource') as PerformanceResourceTiming[]
    const renderBlockingResources: string[] = []

    resources.forEach((resource) => {
      // Check for synchronous scripts in head that could block rendering
      if (resource.initiatorType === 'script' && resource.renderBlockingStatus === 'blocking') {
        renderBlockingResources.push(resource.name)
      }
      // Check for synchronous stylesheets that could block rendering
      if (resource.initiatorType === 'link' && resource.renderBlockingStatus === 'blocking') {
        renderBlockingResources.push(resource.name)
      }
    })

    return renderBlockingResources
  })
}

test.describe('Performance Requirements - Load Time', () => {
  test.beforeEach(async ({ page }) => {
    // Clear cache to get fresh load metrics
    const context = page.context()
    await context.clearCookies()
  })

  test('TC1: Page is interactive within 2 seconds on broadband', async ({ page }) => {
    // Navigate to homepage
    const startTime = Date.now()
    await page.goto('/', { waitUntil: 'domcontentloaded' })

    // Wait for the home page element to be visible (indicating interactivity)
    await page.waitForSelector('[data-testid="home-page"]', { state: 'visible' })
    const interactiveTime = Date.now() - startTime

    // Also get performance API metrics
    const metrics = await getPerformanceMetrics(page)

    console.log('Performance Metrics:')
    console.log(`  - Wall clock time to interactive: ${interactiveTime}ms`)
    console.log(`  - DOM Interactive: ${metrics.domInteractive}ms`)
    console.log(`  - DOM Content Loaded: ${metrics.domContentLoaded}ms`)
    console.log(`  - First Contentful Paint: ${metrics.firstContentfulPaint}ms`)

    // Assert page is interactive within 2 seconds (2000ms)
    // Using DOM Interactive as the primary metric for interactivity
    expect(metrics.domInteractive).toBeLessThan(2000)

    // Wall clock verification
    expect(interactiveTime).toBeLessThan(2000)
  })

  test('TC3: No render-blocking resources delay initial paint', async ({ page }) => {
    await page.goto('/', { waitUntil: 'load' })
    await page.waitForSelector('[data-testid="home-page"]')

    const metrics = await getPerformanceMetrics(page)

    // First Contentful Paint should occur quickly (within 1.8 seconds is considered "good")
    // This indicates no significant render blocking
    console.log(`First Contentful Paint: ${metrics.firstContentfulPaint}ms`)

    // FCP should be fast, indicating no render blocking
    expect(metrics.firstContentfulPaint).toBeLessThan(2000)

    // Check for render-blocking resources
    const blockingResources = await getRenderBlockingResources(page)
    console.log('Render-blocking resources:', blockingResources)

    // Ideally should have no render-blocking resources
    // Allow some flexibility for critical CSS
    expect(blockingResources.length).toBeLessThanOrEqual(2)

    // Verify the page content is actually rendered (not blocked)
    const heroSection = page.locator('[data-testid="hero-section"]')
    await expect(heroSection).toBeVisible()

    const featuresSection = page.locator('[data-testid="features-section"]')
    await expect(featuresSection).toBeVisible()
  })
})

test.describe('Performance Requirements - Web Vitals', () => {
  test('TC4: LCP occurs within 2.5 seconds', async ({ page }) => {
    // Navigate and wait for page to load
    await page.goto('/', { waitUntil: 'load' })
    await page.waitForSelector('[data-testid="home-page"]')

    // Measure LCP
    const lcpValue = await measureLCP(page)
    console.log(`Largest Contentful Paint: ${lcpValue}ms`)

    // LCP should be less than 2.5 seconds (2500ms) for "good" performance
    expect(lcpValue).toBeLessThan(2500)

    // Verify the largest content element is visible
    const heroSection = page.locator('[data-testid="hero-section"]')
    await expect(heroSection).toBeVisible()
  })

  test('TC5: CLS is less than 0.1', async ({ page }) => {
    // Navigate to homepage
    await page.goto('/', { waitUntil: 'load' })
    await page.waitForSelector('[data-testid="home-page"]')

    // Wait for any animations or lazy-loaded content to settle
    await page.waitForTimeout(1000)

    // Scroll down to trigger any lazy-loaded content
    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight))
    await page.waitForTimeout(500)

    // Scroll back up
    await page.evaluate(() => window.scrollTo(0, 0))
    await page.waitForTimeout(500)

    // Measure CLS
    const clsValue = await measureCLS(page)
    console.log(`Cumulative Layout Shift: ${clsValue}`)

    // CLS should be less than 0.1 for "good" performance
    expect(clsValue).toBeLessThan(0.1)

    // Verify content is stable
    const homePage = page.locator('[data-testid="home-page"]')
    await expect(homePage).toBeVisible()
  })
})

test.describe('Performance Requirements - Bundle Analysis', () => {
  test('TC2: No unnecessary large dependencies imported', async ({ page }) => {
    // Navigate to homepage
    await page.goto('/', { waitUntil: 'load' })
    await page.waitForSelector('[data-testid="home-page"]')

    // Get all loaded JavaScript resources
    const jsResources = await page.evaluate(() => {
      const resources = performance.getEntriesByType('resource') as PerformanceResourceTiming[]
      return resources
        .filter((r) => r.initiatorType === 'script' || r.name.endsWith('.js'))
        .map((r) => ({
          name: r.name,
          size: r.transferSize,
          decodedSize: r.decodedBodySize,
          duration: r.duration,
        }))
    })

    console.log('JavaScript bundles loaded:')
    let totalJsSize = 0
    jsResources.forEach((resource) => {
      console.log(`  - ${resource.name.split('/').pop()}: ${Math.round(resource.size / 1024)}KB`)
      totalJsSize += resource.size
    })
    console.log(`Total JS size: ${Math.round(totalJsSize / 1024)}KB`)

    // Note: In development mode, bundles are not minified/optimized.
    // Vite serves modules individually, so sizes are much larger than production.
    // Production builds should be under 500KB. Development can be ~10x larger.
    // We use a more lenient limit for development mode testing.
    const isDevelopmentMode = jsResources.some(r => r.name.includes('@react-refresh') || r.name.includes('vite'))
    const sizeLimit = isDevelopmentMode ? 5 * 1024 * 1024 : 500 * 1024 // 5MB dev, 500KB prod

    console.log(`Mode: ${isDevelopmentMode ? 'Development' : 'Production'}`)
    console.log(`Size limit: ${Math.round(sizeLimit / 1024)}KB`)

    expect(totalJsSize).toBeLessThan(sizeLimit)

    // Verify the homepage components are loaded correctly
    const homePage = page.locator('[data-testid="home-page"]')
    await expect(homePage).toBeVisible()

    // Check specific sections are rendered (confirming components loaded successfully)
    const heroSection = page.locator('[data-testid="hero-section"]')
    await expect(heroSection).toBeVisible()

    const featuresSection = page.locator('[data-testid="features-section"]')
    await expect(featuresSection).toBeVisible()

    // Verify no UNNECESSARY heavy libraries are loaded
    // These are commonly bloated dependencies that shouldn't be in a modern React app
    const unnecessaryHeavyLibPatterns = [
      'moment.min',           // Use date-fns or dayjs instead
      'moment-with-locales',  // Even heavier moment bundle
      'lodash.min.js',        // Full lodash bundle (should use lodash-es or individual imports)
      'jquery',               // jQuery is not needed in React apps
      'd3.min.js',            // Full D3 bundle (should only import needed parts)
      'chart.bundle.min',     // Heavy charting library
      'bootstrap.bundle',     // Full Bootstrap JS
      'three.min',            // Three.js (heavy 3D library)
    ]

    const loadedUnnecessaryLibs = jsResources.filter((r) =>
      unnecessaryHeavyLibPatterns.some((pattern) =>
        r.name.toLowerCase().includes(pattern.toLowerCase())
      )
    )

    if (loadedUnnecessaryLibs.length > 0) {
      console.warn('Unnecessary heavy libraries detected:', loadedUnnecessaryLibs.map(l => l.name))
    }
    expect(loadedUnnecessaryLibs.length).toBe(0)

    // Verify expected libraries are being used efficiently
    // lucide-react is acceptable as it's the icon library used by the project
    // react-router-dom is necessary for routing
    // These are expected dependencies and not "unnecessary"

    console.log('Bundle analysis passed: No unnecessary heavy dependencies detected')
  })
})

test.describe('Performance Requirements - Overall Performance Score', () => {
  test('Overall homepage performance meets requirements', async ({ page }) => {
    const startTime = Date.now()

    // Navigate to homepage
    await page.goto('/', { waitUntil: 'networkidle' })
    await page.waitForSelector('[data-testid="home-page"]')

    const loadTime = Date.now() - startTime
    console.log(`Total page load time (network idle): ${loadTime}ms`)

    // Get all performance metrics
    const metrics = await getPerformanceMetrics(page)

    // Summary of all metrics
    console.log('\n=== Performance Summary ===')
    console.log(`DOM Interactive: ${metrics.domInteractive}ms`)
    console.log(`First Contentful Paint: ${metrics.firstContentfulPaint}ms`)
    console.log(`DOM Content Loaded: ${metrics.domContentLoaded}ms`)
    console.log(`Load Complete: ${metrics.loadComplete}ms`)
    console.log(`Total Load Time: ${loadTime}ms`)

    // All critical metrics should pass
    expect(metrics.domInteractive).toBeLessThan(2000)
    expect(metrics.firstContentfulPaint).toBeLessThan(2000)

    // Verify page is fully functional
    const heroSection = page.locator('[data-testid="hero-section"]')
    await expect(heroSection).toBeVisible()

    const ctaButtons = page.locator('[data-testid="hero-section"] a, [data-testid="hero-section"] button')
    const buttonCount = await ctaButtons.count()
    expect(buttonCount).toBeGreaterThanOrEqual(2)
  })
})
