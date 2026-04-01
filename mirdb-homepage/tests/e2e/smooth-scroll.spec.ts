/**
 * Smooth Scroll Navigation E2E Tests.
 * Owner: Scenario 14 - Smooth Scroll Navigation
 *
 * Tests:
 * - Test Case 2: Click navigation link to features section scrolls smoothly
 * - Verify smooth scroll animation occurs (not instant jump)
 * - Verify target section becomes visible after scroll
 */

import { test, expect } from '@playwright/test'

test.describe('Smooth Scroll Navigation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
  })

  test('Test Case 2: clicking Features navigation link scrolls to features section smoothly', async ({ page }) => {
    // Verify we start at the top of the page
    const initialScrollPosition = await page.evaluate(() => window.scrollY)
    expect(initialScrollPosition).toBe(0)

    // Get the features section element and its position
    const featuresSection = page.locator('#features')
    await expect(featuresSection).toBeVisible()

    // Get initial position of features section (should be below viewport)
    const initialFeaturesBounds = await featuresSection.boundingBox()
    expect(initialFeaturesBounds).not.toBeNull()
    expect(initialFeaturesBounds!.y).toBeGreaterThan(100) // Features should be below initial viewport

    // Click the Features navigation link
    await page.click('nav a[href="#features"]')

    // Wait for scroll animation to complete
    // Use a more reliable approach: wait for scroll position to stabilize
    await page.waitForFunction(
      () => {
        return new Promise<boolean>((resolve) => {
          let lastScrollY = window.scrollY
          let stableCount = 0
          const checkInterval = setInterval(() => {
            if (window.scrollY === lastScrollY) {
              stableCount++
              if (stableCount >= 3) {
                clearInterval(checkInterval)
                resolve(true)
              }
            } else {
              stableCount = 0
              lastScrollY = window.scrollY
            }
          }, 100)
          // Timeout after 3 seconds
          setTimeout(() => {
            clearInterval(checkInterval)
            resolve(true)
          }, 3000)
        })
      },
      { timeout: 5000 }
    )

    // Verify scroll occurred
    const finalScrollPosition = await page.evaluate(() => window.scrollY)
    expect(finalScrollPosition).toBeGreaterThan(0)

    // Verify features section is now in view
    await expect(featuresSection).toBeInViewport()
  })

  test('clicking Usage navigation link scrolls to usage section', async ({ page }) => {
    // Click the Usage navigation link
    await page.click('nav a[href="#usage"]')

    // Wait for scroll to complete
    await page.waitForTimeout(1000)

    // Verify usage section is visible
    const usageSection = page.locator('#usage')
    await expect(usageSection).toBeInViewport()
  })

  test('clicking Roadmap navigation link scrolls to roadmap section', async ({ page }) => {
    // Click the Roadmap navigation link
    await page.click('nav a[href="#roadmap"]')

    // Wait for scroll to complete
    await page.waitForTimeout(1000)

    // Verify roadmap section is visible
    const roadmapSection = page.locator('#roadmap')
    await expect(roadmapSection).toBeInViewport()
  })

  test('navigation uses smooth scroll behavior (not instant jump)', async ({ page }) => {
    // Record scroll positions during animation
    const scrollPositions: number[] = []

    // Set up scroll position tracking
    await page.evaluate(() => {
      (window as unknown as Record<string, number[]>).__scrollPositions = []
      const originalScrollTo = window.scrollTo.bind(window)
      window.scrollTo = function (...args: Parameters<typeof window.scrollTo>) {
        if (args.length === 1 && typeof args[0] === 'object' && 'behavior' in args[0]) {
          // Check that smooth behavior is used
          (window as unknown as Record<string, boolean>).__usedSmoothScroll = args[0].behavior === 'smooth'
        }
        return originalScrollTo(...args)
      }

      // Also intercept scrollIntoView
      Element.prototype.scrollIntoView = new Proxy(Element.prototype.scrollIntoView, {
        apply(target, thisArg, args) {
          if (args[0] && typeof args[0] === 'object' && 'behavior' in args[0]) {
            (window as unknown as Record<string, boolean>).__usedSmoothScroll = args[0].behavior === 'smooth'
          }
          return Reflect.apply(target, thisArg, args)
        },
      })
    })

    // Click navigation link
    await page.click('nav a[href="#features"]')

    // Check that smooth scroll was used
    const usedSmoothScroll = await page.evaluate(
      () => (window as unknown as Record<string, boolean>).__usedSmoothScroll
    )

    expect(usedSmoothScroll).toBe(true)
  })

  test('each major section has an id attribute for anchor linking', async ({ page }) => {
    // Verify features section has correct id
    const features = page.locator('section#features')
    await expect(features).toBeVisible()

    // Verify usage section has correct id
    const usage = page.locator('section#usage')
    await expect(usage).toBeVisible()

    // Verify roadmap section has correct id
    const roadmap = page.locator('section#roadmap')
    await expect(roadmap).toBeVisible()
  })

  test('smooth scroll CSS property is applied to html element', async ({ page }) => {
    // Check CSS scroll-behavior property on html element
    const scrollBehavior = await page.evaluate(() => {
      const html = document.documentElement
      return window.getComputedStyle(html).scrollBehavior
    })

    expect(scrollBehavior).toBe('smooth')
  })
})
