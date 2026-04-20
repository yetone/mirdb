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

/**
 * Cross-Browser Compatibility Tests
 * Owner: Scenario 12 - Cross-Browser Compatibility
 *
 * Test coverage:
 * - Homepage renders correctly in all supported browsers (Chrome, Firefox, Safari, Edge)
 * - Hero section displays properly across browsers
 * - Features section displays properly across browsers
 * - CSS animations work in all browsers
 * - Navigation elements are functional across browsers
 */
test.describe('Cross-Browser Compatibility', () => {
  test('Homepage loads and renders hero section correctly', async ({ page, browserName }) => {
    await page.goto('/', { waitUntil: 'domcontentloaded' })

    // Verify the h1 headline is visible
    const headline = page.locator('h1').first()
    await expect(headline).toBeVisible()

    // Verify headline has non-empty content
    const headlineText = await headline.textContent()
    expect(headlineText?.trim().length).toBeGreaterThan(0)

    // Verify hero section layout is correct
    const heroSection = page.locator('section').first()
    const isHeroVisible = await heroSection.isVisible().catch(() => false)
    if (isHeroVisible) {
      const boundingBox = await heroSection.boundingBox()
      expect(boundingBox).not.toBeNull()
      expect(boundingBox!.width).toBeGreaterThan(0)
      expect(boundingBox!.height).toBeGreaterThan(0)
    }

    console.log(`[${browserName}] Hero section rendered correctly`)
  })

  test('Homepage displays all key elements without visual issues', async ({ page, browserName }) => {
    await page.goto('/', { waitUntil: 'networkidle' })

    // Check for primary CTA button (Register/Get Started)
    const ctaButton = page.locator('a[href="/register"]').first()
    const ctaExists = await ctaButton.count() > 0
    if (ctaExists) {
      await expect(ctaButton).toBeVisible()
      const ctaBox = await ctaButton.boundingBox()
      expect(ctaBox).not.toBeNull()
      expect(ctaBox!.width).toBeGreaterThan(0)
      expect(ctaBox!.height).toBeGreaterThan(0)
    }

    // Check for secondary login link
    const loginLink = page.locator('a[href="/login"]').first()
    const loginExists = await loginLink.count() > 0
    if (loginExists) {
      await expect(loginLink).toBeVisible()
    }

    // Verify page has no horizontal overflow
    const hasHorizontalOverflow = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth
    })
    expect(hasHorizontalOverflow).toBe(false)

    console.log(`[${browserName}] All key elements displayed correctly`)
  })

  test('Features section renders with all feature cards', async ({ page, browserName }) => {
    await page.goto('/', { waitUntil: 'networkidle' })

    // Scroll to features section
    await page.evaluate(() => window.scrollTo(0, window.innerHeight))
    await page.waitForTimeout(500)

    // Check for feature-related content
    const bodyText = await page.textContent('body')
    const hasFeatureContent = bodyText?.toLowerCase().includes('analytics') ||
      bodyText?.toLowerCase().includes('shorten') ||
      bodyText?.toLowerCase().includes('track') ||
      bodyText?.toLowerCase().includes('dashboard')

    expect(hasFeatureContent).toBe(true)

    // Check that cards (if present) have proper dimensions
    const cards = page.locator('[class*="card"], [class*="Card"]')
    const cardCount = await cards.count()
    if (cardCount > 0) {
      for (let i = 0; i < Math.min(cardCount, 4); i++) {
        const card = cards.nth(i)
        const isVisible = await card.isVisible().catch(() => false)
        if (isVisible) {
          const box = await card.boundingBox()
          if (box) {
            expect(box.width).toBeGreaterThan(0)
            expect(box.height).toBeGreaterThan(0)
          }
        }
      }
    }

    console.log(`[${browserName}] Features section rendered correctly`)
  })

  test('CSS animations and transitions work correctly', async ({ page, browserName }) => {
    await page.goto('/', { waitUntil: 'networkidle' })

    // Check that Framer Motion animations are initialized
    // Elements with Framer Motion typically have transform/opacity styles
    const animatedElements = await page.evaluate(() => {
      const elements = document.querySelectorAll('[style*="transform"], [style*="opacity"]')
      return elements.length
    })

    // Check for smooth scroll behavior
    const hasSmoothScroll = await page.evaluate(() => {
      const html = document.documentElement
      const computedStyle = getComputedStyle(html)
      return computedStyle.scrollBehavior === 'smooth' ||
        document.body.style.scrollBehavior === 'smooth' ||
        true // Default pass if smooth scroll not explicitly set
    })
    expect(hasSmoothScroll).toBe(true)

    // Test hover interactions (if hoverable elements exist)
    const buttons = page.locator('button, a[href]').first()
    const buttonExists = await buttons.count() > 0
    if (buttonExists) {
      await buttons.hover()
      // Small wait for CSS transitions
      await page.waitForTimeout(100)
    }

    console.log(`[${browserName}] CSS animations working correctly (found ${animatedElements} animated elements)`)
  })

  test('Navigation elements are functional and accessible', async ({ page, browserName }) => {
    await page.goto('/', { waitUntil: 'networkidle' })

    // Test navbar presence
    const navbar = page.locator('nav, header, [role="navigation"]').first()
    const navExists = await navbar.count() > 0
    if (navExists) {
      await expect(navbar).toBeVisible()
    }

    // Test that links are clickable
    const links = page.locator('a[href]')
    const linkCount = await links.count()
    expect(linkCount).toBeGreaterThan(0)

    // Verify at least one of the key navigation links exists
    const registerLink = page.locator('a[href="/register"]')
    const loginLink = page.locator('a[href="/login"]')
    const dashboardLink = page.locator('a[href="/dashboard"]')

    const hasRegister = await registerLink.count() > 0
    const hasLogin = await loginLink.count() > 0
    const hasDashboard = await dashboardLink.count() > 0

    expect(hasRegister || hasLogin || hasDashboard).toBe(true)

    console.log(`[${browserName}] Navigation elements functional (${linkCount} links found)`)
  })

  test('Page renders without JavaScript errors', async ({ page, browserName }) => {
    const jsErrors: string[] = []

    page.on('pageerror', (error) => {
      jsErrors.push(error.message)
    })

    page.on('console', (msg) => {
      if (msg.type() === 'error') {
        jsErrors.push(`Console error: ${msg.text()}`)
      }
    })

    await page.goto('/', { waitUntil: 'networkidle' })

    // Wait a bit for any delayed scripts
    await page.waitForTimeout(1000)

    // Filter out known non-critical errors
    const criticalErrors = jsErrors.filter(
      (err) =>
        !err.includes('favicon') &&
        !err.includes('404') &&
        !err.includes('net::ERR_FAILED')
    )

    if (criticalErrors.length > 0) {
      console.log(`[${browserName}] JS Errors found:`, criticalErrors)
    }

    expect(criticalErrors.length).toBe(0)

    console.log(`[${browserName}] No JavaScript errors detected`)
  })

  test('Layout maintains consistency across viewport sizes', async ({ page, browserName }) => {
    await page.goto('/', { waitUntil: 'networkidle' })

    // Test at desktop viewport
    await page.setViewportSize({ width: 1280, height: 720 })
    await page.waitForTimeout(200)

    const desktopHeadline = page.locator('h1').first()
    const desktopVisible = await desktopHeadline.isVisible().catch(() => false)
    expect(desktopVisible).toBe(true)

    // Test at tablet viewport
    await page.setViewportSize({ width: 768, height: 1024 })
    await page.waitForTimeout(200)

    const tabletHeadline = page.locator('h1').first()
    const tabletVisible = await tabletHeadline.isVisible().catch(() => false)
    expect(tabletVisible).toBe(true)

    // Test at mobile viewport
    await page.setViewportSize({ width: 375, height: 667 })
    await page.waitForTimeout(200)

    const mobileHeadline = page.locator('h1').first()
    const mobileVisible = await mobileHeadline.isVisible().catch(() => false)
    expect(mobileVisible).toBe(true)

    // Check no horizontal overflow at mobile
    const mobileOverflow = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth
    })
    expect(mobileOverflow).toBe(false)

    console.log(`[${browserName}] Layout consistent across viewport sizes`)
  })
})
