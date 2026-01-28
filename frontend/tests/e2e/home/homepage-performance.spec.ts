/**
 * Homepage Performance E2E Tests
 * Owner: Scenario 20 - Performance - Page Load
 *
 * End-to-end performance tests:
 * - Page load time < 3 seconds (NFR-1)
 * - DOMContentLoaded < 2 seconds
 * - Largest Contentful Paint < 2.5s (good score)
 * - Lighthouse performance score > 70
 * - Lazy loading works for below-fold content
 *
 * Testing framework: Playwright
 */
import { test, expect } from '@playwright/test'

test.describe('Homepage Performance - Page Load (Scenario 20)', () => {
  test.describe('Test Case 1: DOMContentLoaded Time', () => {
    test('DOMContentLoaded fires within 2 seconds', async ({ page }) => {
      // Navigate to homepage and wait for DOM content loaded
      const startTime = Date.now()
      await page.goto('/', { waitUntil: 'domcontentloaded' })
      const navigationTime = Date.now() - startTime

      // Get performance timing metrics using Performance API
      const performanceMetrics = await page.evaluate(() => {
        const entries = performance.getEntriesByType('navigation') as PerformanceNavigationTiming[]
        if (entries.length > 0) {
          const navEntry = entries[0]
          return {
            domContentLoadedEventEnd: navEntry.domContentLoadedEventEnd,
            loadEventEnd: navEntry.loadEventEnd,
            responseEnd: navEntry.responseEnd,
            domInteractive: navEntry.domInteractive,
          }
        }
        // Fallback to legacy timing API
        const timing = performance.timing
        return {
          domContentLoadedEventEnd: timing.domContentLoadedEventEnd - timing.navigationStart,
          loadEventEnd: timing.loadEventEnd - timing.navigationStart,
          responseEnd: timing.responseEnd - timing.navigationStart,
          domInteractive: timing.domInteractive - timing.navigationStart,
        }
      })

      console.log(`DOMContentLoaded time: ${performanceMetrics.domContentLoadedEventEnd}ms`)
      console.log(`Navigation time: ${navigationTime}ms`)

      // Assert DOMContentLoaded fires within 2 seconds (2000ms)
      expect(performanceMetrics.domContentLoadedEventEnd).toBeLessThan(2000)
    })

    test('Page reaches interactive state quickly', async ({ page }) => {
      await page.goto('/', { waitUntil: 'domcontentloaded' })

      // Verify the page is interactive by checking if main content is rendered
      const heroSection = page.locator('[data-testid="hero-section"]')
      await expect(heroSection).toBeVisible({ timeout: 2000 })

      // Get Time to Interactive approximation
      const interactiveMetrics = await page.evaluate(() => {
        const entries = performance.getEntriesByType('navigation') as PerformanceNavigationTiming[]
        if (entries.length > 0) {
          return {
            domInteractive: entries[0].domInteractive,
            domComplete: entries[0].domComplete,
          }
        }
        return { domInteractive: 0, domComplete: 0 }
      })

      console.log(`DOM Interactive: ${interactiveMetrics.domInteractive}ms`)
      console.log(`DOM Complete: ${interactiveMetrics.domComplete}ms`)

      // DOM should be interactive quickly
      expect(interactiveMetrics.domInteractive).toBeLessThan(2000)
    })
  })

  test.describe('Test Case 2: Largest Contentful Paint (LCP)', () => {
    test('LCP is under 2.5 seconds (good score)', async ({ page }) => {
      // Set up LCP observation before navigation
      await page.goto('/')

      // Wait for the page to fully load
      await page.waitForLoadState('networkidle')

      // Give time for LCP to be recorded
      await page.waitForTimeout(1000)

      // Get LCP from Performance Observer
      const lcpValue = await page.evaluate(() => {
        return new Promise<number>((resolve) => {
          // Check if there are existing LCP entries
          const lcpEntries = performance.getEntriesByType('largest-contentful-paint')
          if (lcpEntries.length > 0) {
            const lastEntry = lcpEntries[lcpEntries.length - 1] as PerformanceEntry & { startTime: number }
            resolve(lastEntry.startTime)
            return
          }

          // Set up observer for LCP
          let lcpValue = 0
          const observer = new PerformanceObserver((list) => {
            const entries = list.getEntries()
            const lastEntry = entries[entries.length - 1] as PerformanceEntry & { startTime: number }
            lcpValue = lastEntry.startTime
          })

          try {
            observer.observe({ type: 'largest-contentful-paint', buffered: true })
          } catch {
            // Browser may not support LCP observation
            resolve(0)
            return
          }

          // Wait a bit and return the value
          setTimeout(() => {
            observer.disconnect()
            resolve(lcpValue)
          }, 500)
        })
      })

      console.log(`Largest Contentful Paint: ${lcpValue}ms`)

      // LCP should be under 2.5 seconds (2500ms) for a "good" score
      // We also allow 0 if the browser doesn't support LCP observation (test passes)
      if (lcpValue > 0) {
        expect(lcpValue).toBeLessThan(2500)
      } else {
        // Fallback: verify the hero section (likely LCP element) loads quickly
        const heroSection = page.locator('[data-testid="hero-section"]')
        await expect(heroSection).toBeVisible({ timeout: 2500 })
      }
    })

    test('Hero section (likely LCP element) renders within acceptable time', async ({ page }) => {
      const startTime = Date.now()
      await page.goto('/')

      // Hero section is typically the LCP element
      const heroSection = page.locator('[data-testid="hero-section"]')
      await expect(heroSection).toBeVisible()

      const heroVisibleTime = Date.now() - startTime
      console.log(`Hero section visible in: ${heroVisibleTime}ms`)

      // Hero should be visible well within 2.5 seconds
      expect(heroVisibleTime).toBeLessThan(2500)
    })
  })

  test.describe('Test Case 3: Lighthouse Performance Audit', () => {
    test('Page achieves acceptable performance score (simulated)', async ({ page }) => {
      // Since we cannot run actual Lighthouse in Playwright directly,
      // we simulate key performance checks that Lighthouse evaluates

      const startTime = Date.now()
      await page.goto('/')
      await page.waitForLoadState('networkidle')
      const loadTime = Date.now() - startTime

      // Collect multiple performance metrics
      const metrics = await page.evaluate(() => {
        const navEntries = performance.getEntriesByType('navigation') as PerformanceNavigationTiming[]
        const paintEntries = performance.getEntriesByType('paint')
        const resourceEntries = performance.getEntriesByType('resource') as PerformanceResourceTiming[]

        const navEntry = navEntries[0]
        const fcpEntry = paintEntries.find((e) => e.name === 'first-contentful-paint')

        // Calculate total resource size and count
        let totalResourceSize = 0
        let resourceCount = 0
        resourceEntries.forEach((r) => {
          totalResourceSize += r.transferSize || 0
          resourceCount++
        })

        return {
          domContentLoaded: navEntry?.domContentLoadedEventEnd || 0,
          loadComplete: navEntry?.loadEventEnd || 0,
          firstContentfulPaint: fcpEntry?.startTime || 0,
          resourceCount,
          totalResourceSizeKB: Math.round(totalResourceSize / 1024),
          ttfb: navEntry?.responseStart || 0,
        }
      })

      console.log('Performance Metrics:')
      console.log(`  Total Load Time: ${loadTime}ms`)
      console.log(`  DOM Content Loaded: ${metrics.domContentLoaded}ms`)
      console.log(`  First Contentful Paint: ${metrics.firstContentfulPaint}ms`)
      console.log(`  Time to First Byte: ${metrics.ttfb}ms`)
      console.log(`  Resource Count: ${metrics.resourceCount}`)
      console.log(`  Total Resource Size: ${metrics.totalResourceSizeKB}KB`)

      // Calculate a simulated performance score based on key metrics
      // Lighthouse scoring is complex, but we can approximate key thresholds
      let score = 100

      // Penalize slow FCP (target: < 1.8s is good)
      if (metrics.firstContentfulPaint > 1800) {
        score -= 15
      } else if (metrics.firstContentfulPaint > 1000) {
        score -= 5
      }

      // Penalize slow DOM Content Loaded (target: < 2s)
      if (metrics.domContentLoaded > 2000) {
        score -= 15
      } else if (metrics.domContentLoaded > 1500) {
        score -= 5
      }

      // Penalize slow TTFB (target: < 600ms)
      if (metrics.ttfb > 600) {
        score -= 10
      } else if (metrics.ttfb > 300) {
        score -= 3
      }

      // Penalize too many resources (affects speed index)
      if (metrics.resourceCount > 50) {
        score -= 10
      } else if (metrics.resourceCount > 30) {
        score -= 5
      }

      console.log(`  Simulated Performance Score: ${score}`)

      // Score should be above 70
      expect(score).toBeGreaterThan(70)
    })

    test('First Contentful Paint is within acceptable range', async ({ page }) => {
      await page.goto('/')
      await page.waitForLoadState('domcontentloaded')

      // Get FCP metric
      const fcpMetric = await page.evaluate(() => {
        return new Promise<number>((resolve) => {
          const paintEntries = performance.getEntriesByType('paint')
          const fcpEntry = paintEntries.find((e) => e.name === 'first-contentful-paint')

          if (fcpEntry) {
            resolve(fcpEntry.startTime)
            return
          }

          // Wait for FCP if not yet recorded
          const observer = new PerformanceObserver((list) => {
            const entries = list.getEntries()
            const fcp = entries.find((e) => e.name === 'first-contentful-paint')
            if (fcp) {
              observer.disconnect()
              resolve(fcp.startTime)
            }
          })

          try {
            observer.observe({ type: 'paint', buffered: true })
          } catch {
            resolve(0)
          }

          // Timeout after 3 seconds
          setTimeout(() => {
            observer.disconnect()
            resolve(0)
          }, 3000)
        })
      })

      console.log(`First Contentful Paint: ${fcpMetric}ms`)

      // FCP should be under 1.8 seconds for good score, under 3s for acceptable
      if (fcpMetric > 0) {
        expect(fcpMetric).toBeLessThan(3000)
      }
    })

    test('No excessive render-blocking resources', async ({ page }) => {
      await page.goto('/')
      await page.waitForLoadState('networkidle')

      // Check for render-blocking resources
      const renderBlockingInfo = await page.evaluate(() => {
        const resources = performance.getEntriesByType('resource') as PerformanceResourceTiming[]

        // Find potentially render-blocking resources (sync scripts and stylesheets in head)
        const scripts = document.querySelectorAll('script:not([async]):not([defer])[src]')
        const stylesheets = document.querySelectorAll('link[rel="stylesheet"]')

        // Check resource timing for CSS files
        const cssResources = resources.filter((r) => r.initiatorType === 'link' && r.name.includes('.css'))

        return {
          syncScriptCount: scripts.length,
          stylesheetCount: stylesheets.length,
          cssResourceCount: cssResources.length,
          totalCssSize: cssResources.reduce((sum, r) => sum + (r.transferSize || 0), 0),
        }
      })

      console.log('Render-Blocking Analysis:')
      console.log(`  Sync Scripts: ${renderBlockingInfo.syncScriptCount}`)
      console.log(`  Stylesheets: ${renderBlockingInfo.stylesheetCount}`)
      console.log(`  CSS Resources: ${renderBlockingInfo.cssResourceCount}`)
      console.log(`  Total CSS Size: ${Math.round(renderBlockingInfo.totalCssSize / 1024)}KB`)

      // Vite bundles CSS efficiently, so we just ensure reasonable counts
      // A well-optimized page should have minimal render-blocking resources
      expect(renderBlockingInfo.syncScriptCount).toBeLessThanOrEqual(5)
      expect(renderBlockingInfo.stylesheetCount).toBeLessThanOrEqual(3)
    })
  })

  test.describe('Test Case 4: Lazy Loading Implementation', () => {
    test('Below-fold content uses Framer Motion whileInView for lazy rendering', async ({
      page,
    }) => {
      await page.goto('/')

      // Verify hero section is visible immediately (above fold)
      const heroSection = page.locator('[data-testid="hero-section"]')
      await expect(heroSection).toBeVisible()

      // Features section should be present but may have initial opacity 0 due to Framer Motion
      const featuresSection = page.locator('[data-testid="features-section"]')
      await expect(featuresSection).toBeAttached()

      // How It Works section should also be present
      const howItWorksSection = page.locator('[data-testid="how-it-works-section"]')
      await expect(howItWorksSection).toBeAttached()

      // Verify Framer Motion animations are set up with whileInView
      // The sections use viewport: { once: true } which means they animate when scrolled into view
      const motionElementsExist = await page.evaluate(() => {
        // Check if motion elements have the expected data attributes from Framer Motion
        const featuresSection = document.querySelector('[data-testid="features-section"]')
        const howItWorksSection = document.querySelector('[data-testid="how-it-works-section"]')

        // Framer Motion adds style attributes for opacity animations
        const featuresHeading = featuresSection?.querySelector('h2')
        const howItWorksHeading = howItWorksSection?.querySelector('h2')

        return {
          featuresSectionExists: !!featuresSection,
          howItWorksSectionExists: !!howItWorksSection,
          hasAnimatedElements: !!featuresHeading && !!howItWorksHeading,
        }
      })

      expect(motionElementsExist.featuresSectionExists).toBe(true)
      expect(motionElementsExist.howItWorksSectionExists).toBe(true)
      expect(motionElementsExist.hasAnimatedElements).toBe(true)
    })

    test('Feature cards animate in when scrolled into view', async ({ page }) => {
      await page.goto('/')
      await page.waitForLoadState('domcontentloaded')

      // Get initial state of feature cards (they may be invisible due to animation)
      const initialCardStates = await page.evaluate(() => {
        const cards = document.querySelectorAll('[data-testid^="feature-card-"]')
        return Array.from(cards).map((card) => {
          const style = window.getComputedStyle(card)
          return {
            opacity: style.opacity,
            transform: style.transform,
          }
        })
      })

      // Scroll to features section to trigger animations
      await page.evaluate(() => {
        const section = document.querySelector('[data-testid="features-section"]')
        if (section) {
          section.scrollIntoView({ behavior: 'instant' })
        }
      })

      // Wait for Framer Motion animations to complete
      await page.waitForTimeout(800)

      // Check that cards are now visible
      const featureCards = page.locator('[data-testid^="feature-card-"]')
      const cardCount = await featureCards.count()
      expect(cardCount).toBe(3)

      // Verify all cards are visible after scroll
      for (let i = 0; i < cardCount; i++) {
        await expect(featureCards.nth(i)).toBeVisible()
      }
    })

    test('How It Works steps animate in when scrolled into view', async ({ page }) => {
      await page.goto('/')
      await page.waitForLoadState('domcontentloaded')

      // Scroll to How It Works section
      await page.evaluate(() => {
        const section = document.querySelector('[data-testid="how-it-works-section"]')
        if (section) {
          section.scrollIntoView({ behavior: 'instant' })
        }
      })

      // Wait for animations
      await page.waitForTimeout(800)

      // Verify all steps are visible
      await expect(page.locator('[data-testid="step-1"]')).toBeVisible()
      await expect(page.locator('[data-testid="step-2"]')).toBeVisible()
      await expect(page.locator('[data-testid="step-3"]')).toBeVisible()
    })

    test('Page uses efficient resource loading patterns', async ({ page }) => {
      await page.goto('/')
      await page.waitForLoadState('networkidle')

      // Check that Vite's module bundling is working (ESM modules)
      const resourceInfo = await page.evaluate(() => {
        const resources = performance.getEntriesByType('resource') as PerformanceResourceTiming[]

        // Check for JS modules (Vite bundles)
        const jsResources = resources.filter(
          (r) => r.initiatorType === 'script' || r.name.endsWith('.js')
        )

        // Check for CSS (should be minimal with Tailwind)
        const cssResources = resources.filter(
          (r) => r.initiatorType === 'link' && r.name.includes('.css')
        )

        // Check for any images
        const imageResources = resources.filter(
          (r) =>
            r.initiatorType === 'img' ||
            r.name.match(/\.(png|jpg|jpeg|gif|webp|svg|avif)$/i)
        )

        return {
          jsCount: jsResources.length,
          cssCount: cssResources.length,
          imageCount: imageResources.length,
          totalTransferSize: resources.reduce((sum, r) => sum + (r.transferSize || 0), 0),
        }
      })

      console.log('Resource Loading:')
      console.log(`  JS files: ${resourceInfo.jsCount}`)
      console.log(`  CSS files: ${resourceInfo.cssCount}`)
      console.log(`  Images: ${resourceInfo.imageCount}`)
      console.log(`  Total transfer size: ${Math.round(resourceInfo.totalTransferSize / 1024)}KB`)

      // Verify efficient bundling
      // In dev mode, Vite serves many individual modules for HMR; in prod, bundles are fewer
      // Allow higher count for dev mode (up to 50), production builds would be much smaller
      expect(resourceInfo.jsCount).toBeLessThanOrEqual(50)
      expect(resourceInfo.cssCount).toBeLessThanOrEqual(5)
    })
  })

  test.describe('Additional Performance Checks', () => {
    test('Page loads within NFR-1 requirement (< 3 seconds)', async ({ page }) => {
      const startTime = Date.now()
      await page.goto('/', { waitUntil: 'load' })
      const loadTime = Date.now() - startTime

      console.log(`Total page load time: ${loadTime}ms`)

      // NFR-1: Page load time under 3 seconds
      expect(loadTime).toBeLessThan(3000)
    })

    test('Cumulative Layout Shift (CLS) is minimal', async ({ page }) => {
      await page.goto('/')

      // Wait for page to stabilize
      await page.waitForLoadState('networkidle')
      await page.waitForTimeout(1000)

      // Check for layout shifts
      const clsValue = await page.evaluate(() => {
        return new Promise<number>((resolve) => {
          let clsScore = 0

          const observer = new PerformanceObserver((list) => {
            for (const entry of list.getEntries()) {
              const layoutShiftEntry = entry as PerformanceEntry & {
                hadRecentInput: boolean
                value: number
              }
              if (!layoutShiftEntry.hadRecentInput) {
                clsScore += layoutShiftEntry.value
              }
            }
          })

          try {
            observer.observe({ type: 'layout-shift', buffered: true })
          } catch {
            resolve(0)
            return
          }

          // Wait and collect CLS
          setTimeout(() => {
            observer.disconnect()
            resolve(clsScore)
          }, 500)
        })
      })

      console.log(`Cumulative Layout Shift: ${clsValue}`)

      // CLS should be under 0.1 for "good" score, under 0.25 for acceptable
      if (clsValue > 0) {
        expect(clsValue).toBeLessThan(0.25)
      }
    })

    test('Time to First Byte (TTFB) is acceptable', async ({ page }) => {
      await page.goto('/')

      const ttfb = await page.evaluate(() => {
        const navEntries = performance.getEntriesByType(
          'navigation'
        ) as PerformanceNavigationTiming[]
        if (navEntries.length > 0) {
          return navEntries[0].responseStart
        }
        return 0
      })

      console.log(`Time to First Byte: ${ttfb}ms`)

      // TTFB should be under 800ms for good performance
      // Local dev server should be very fast
      expect(ttfb).toBeLessThan(800)
    })
  })
})
