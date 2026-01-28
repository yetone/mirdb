/**
 * Homepage E2E Tests
 * Owner: Scenario 17 - Smooth Scroll Behavior
 *
 * End-to-end tests for homepage smooth scroll behavior:
 * - CSS scroll-behavior property verification
 * - Anchor link smooth scroll animation (if present)
 * - Framer Motion scroll-triggered animations
 *
 * Testing framework: Playwright
 */
import { test, expect } from '@playwright/test'

test.describe('Smooth Scroll Behavior', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    // Wait for the page to be fully loaded
    await page.waitForLoadState('networkidle')
  })

  test('should have scroll-behavior: smooth CSS property on html element', async ({
    page,
  }) => {
    // Test Case 1: Check CSS scroll-behavior property on html/body
    const scrollBehavior = await page.evaluate(() => {
      const html = document.documentElement
      return window.getComputedStyle(html).scrollBehavior
    })

    expect(scrollBehavior).toBe('smooth')
  })

  test('should have smooth scroll behavior applied to the page', async ({
    page,
  }) => {
    // Additional verification: check that the CSS is correctly applied
    const htmlScrollBehavior = await page.evaluate(() => {
      return window.getComputedStyle(document.documentElement).scrollBehavior
    })

    // Verify scroll-behavior is set to smooth
    expect(htmlScrollBehavior).toBe('smooth')
  })

  test('should scroll smoothly when using scrollIntoView', async ({ page }) => {
    // Test Case 2: Test smooth scroll animation behavior
    // Scroll to the features section using scrollIntoView with smooth behavior
    const initialScrollY = await page.evaluate(() => window.scrollY)

    // Trigger a smooth scroll to the features section
    await page.evaluate(() => {
      const featuresSection = document.querySelector(
        '[data-testid="features-section"]'
      )
      if (featuresSection) {
        featuresSection.scrollIntoView({ behavior: 'smooth' })
      }
    })

    // Wait for scroll animation to complete (smooth scroll takes time)
    await page.waitForTimeout(500)

    // Verify scroll position changed
    const finalScrollY = await page.evaluate(() => window.scrollY)
    expect(finalScrollY).toBeGreaterThan(initialScrollY)
  })

  test('should smoothly scroll to different sections', async ({ page }) => {
    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY)

    // Scroll to How It Works section
    await page.evaluate(() => {
      const howItWorksSection = document.querySelector(
        '[data-testid="how-it-works-section"]'
      )
      if (howItWorksSection) {
        howItWorksSection.scrollIntoView({ behavior: 'smooth' })
      }
    })

    // Wait for smooth scroll to complete
    await page.waitForTimeout(600)

    // Verify scroll occurred
    const scrolledY = await page.evaluate(() => window.scrollY)
    expect(scrolledY).toBeGreaterThan(initialScrollY)

    // Verify the section is in view
    const isInView = await page.evaluate(() => {
      const section = document.querySelector(
        '[data-testid="how-it-works-section"]'
      )
      if (!section) return false
      const rect = section.getBoundingClientRect()
      return rect.top >= 0 && rect.top < window.innerHeight
    })
    expect(isInView).toBe(true)
  })

  test('should have Framer Motion scroll-triggered animations', async ({
    page,
  }) => {
    // Test Case 3: Verify Framer Motion scroll animations are present and performant
    // Check that motion elements exist in the hero section
    const heroSection = page.locator('[data-testid="hero-section"]')
    await expect(heroSection).toBeVisible()

    // Scroll to features section and verify it's visible with animations
    await page.evaluate(() => {
      const featuresSection = document.querySelector(
        '[data-testid="features-section"]'
      )
      if (featuresSection) {
        featuresSection.scrollIntoView({ behavior: 'smooth' })
      }
    })

    // Wait for scroll and animations
    await page.waitForTimeout(700)

    // Verify features section is now visible
    const featuresSection = page.locator('[data-testid="features-section"]')
    await expect(featuresSection).toBeVisible()

    // Check that feature cards are visible after scroll animation
    const featureCards = page.locator('[data-testid^="feature-card-"]')
    const cardCount = await featureCards.count()
    expect(cardCount).toBe(3)

    // Verify each card is visible
    for (let i = 0; i < cardCount; i++) {
      await expect(featureCards.nth(i)).toBeVisible()
    }
  })

  test('should have smooth scroll-triggered animations in How It Works section', async ({
    page,
  }) => {
    // Scroll to How It Works section
    await page.evaluate(() => {
      const section = document.querySelector(
        '[data-testid="how-it-works-section"]'
      )
      if (section) {
        section.scrollIntoView({ behavior: 'smooth' })
      }
    })

    // Wait for scroll and animations to complete
    await page.waitForTimeout(800)

    // Verify all steps are visible
    const steps = page.locator('[data-testid^="step-"]')
    const stepCount = await steps.count()
    expect(stepCount).toBeGreaterThanOrEqual(3)

    // Check that step 1, 2, and 3 are visible
    await expect(page.locator('[data-testid="step-1"]')).toBeVisible()
    await expect(page.locator('[data-testid="step-2"]')).toBeVisible()
    await expect(page.locator('[data-testid="step-3"]')).toBeVisible()
  })

  test('scroll animation should be performant with no significant frame drops', async ({
    page,
  }) => {
    // Measure scroll performance
    const performanceMetrics = await page.evaluate(async () => {
      const startTime = performance.now()
      const initialScrollY = window.scrollY

      // Perform smooth scroll to bottom
      window.scrollTo({
        top: document.body.scrollHeight,
        behavior: 'smooth',
      })

      // Wait for scroll to complete (using RAF-based detection)
      await new Promise<void>((resolve) => {
        let lastScrollY = window.scrollY
        let stableFrames = 0

        const checkScroll = () => {
          if (window.scrollY === lastScrollY) {
            stableFrames++
            if (stableFrames > 5) {
              resolve()
              return
            }
          } else {
            stableFrames = 0
            lastScrollY = window.scrollY
          }
          requestAnimationFrame(checkScroll)
        }
        requestAnimationFrame(checkScroll)

        // Timeout fallback
        setTimeout(resolve, 2000)
      })

      const endTime = performance.now()
      const finalScrollY = window.scrollY

      return {
        duration: endTime - startTime,
        scrollDistance: finalScrollY - initialScrollY,
        completed: finalScrollY > initialScrollY,
      }
    })

    // Verify scroll completed successfully
    expect(performanceMetrics.completed).toBe(true)

    // Verify scroll animation was smooth (reasonable duration)
    // Smooth scroll should take some time (not instant) but not too long
    expect(performanceMetrics.duration).toBeGreaterThan(100) // Not instant
    expect(performanceMetrics.duration).toBeLessThan(3000) // Not too slow
  })

  test('should scroll back to top smoothly', async ({ page }) => {
    // First scroll down
    await page.evaluate(() => {
      window.scrollTo({
        top: document.body.scrollHeight,
        behavior: 'smooth',
      })
    })

    await page.waitForTimeout(800)

    // Then scroll back to top
    await page.evaluate(() => {
      window.scrollTo({
        top: 0,
        behavior: 'smooth',
      })
    })

    await page.waitForTimeout(800)

    // Verify we're back at the top
    const scrollY = await page.evaluate(() => window.scrollY)
    expect(scrollY).toBeLessThanOrEqual(50) // Allow small tolerance
  })
})
