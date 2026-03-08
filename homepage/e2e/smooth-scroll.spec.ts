/**
 * Smooth Scroll Navigation E2E tests for MirDB homepage.
 * Owner: Scenario 18 - Smooth Scroll Navigation
 *
 * Requirements:
 * - Verify smooth scroll behavior for internal navigation links
 * - Test hero CTA button scrolling to Getting Started section
 * - Test Features navigation link scrolling to Features section
 * - Verify scroll-behavior CSS property on html element
 * - Test keyboard activation of scroll links
 */

import { test, expect } from '@playwright/test'

test.describe('Smooth Scroll Navigation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    await page.waitForLoadState('networkidle')
  })

  // Test Case 1: Click hero CTA button -> Page smoothly scrolls to Getting Started section
  test('hero CTA button scrolls smoothly to Getting Started section', async ({ page }) => {
    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY)
    expect(initialScrollY).toBe(0)

    // Find and click the "Get Started" CTA button in hero section
    const heroCtaButton = page.locator('section#hero a[href="#getting-started"]')
    await expect(heroCtaButton).toBeVisible()
    await heroCtaButton.click()

    // Wait for scroll animation to complete
    await page.waitForTimeout(1000)

    // Verify we scrolled to the Getting Started section
    const gettingStartedSection = page.locator('#getting-started')
    await expect(gettingStartedSection).toBeVisible()

    // Verify the section is at or near the top of the viewport
    const sectionRect = await gettingStartedSection.boundingBox()
    expect(sectionRect).not.toBeNull()
    // Section should be visible in the viewport (top within reasonable range)
    expect(sectionRect!.y).toBeLessThan(200)

    // Verify scroll position changed
    const finalScrollY = await page.evaluate(() => window.scrollY)
    expect(finalScrollY).toBeGreaterThan(initialScrollY)
  })

  // Test Case 2: Click Features navigation link -> Page smoothly scrolls to Features section
  test('Features navigation link scrolls smoothly to Features section', async ({ page }) => {
    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY)
    expect(initialScrollY).toBe(0)

    // Find and click the Features navigation link in the header
    const featuresNavLink = page.locator('nav a[href="#features"]')
    await expect(featuresNavLink).toBeVisible()
    await featuresNavLink.click()

    // Wait for scroll animation to complete
    await page.waitForTimeout(1000)

    // Verify we scrolled to the Features section
    const featuresSection = page.locator('#features')
    await expect(featuresSection).toBeVisible()

    // Verify the section header is at or near the top of the viewport
    const sectionRect = await featuresSection.boundingBox()
    expect(sectionRect).not.toBeNull()
    // Section should be visible near the top of viewport
    expect(sectionRect!.y).toBeLessThan(200)

    // Verify scroll position changed
    const finalScrollY = await page.evaluate(() => window.scrollY)
    expect(finalScrollY).toBeGreaterThan(initialScrollY)
  })

  // Test Case 3: Verify scroll-behavior CSS property -> html element has scroll-behavior: smooth
  test('html element has scroll-behavior: smooth CSS property', async ({ page }) => {
    // Get the computed scroll-behavior style of the html element
    const scrollBehavior = await page.evaluate(() => {
      return window.getComputedStyle(document.documentElement).scrollBehavior
    })

    // Verify scroll-behavior is set to smooth
    expect(scrollBehavior).toBe('smooth')
  })

  // Test Case 4: Test keyboard activation of scroll links -> Enter key on focused link triggers smooth scroll
  test('Enter key on focused link triggers smooth scroll', async ({ page }) => {
    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY)
    expect(initialScrollY).toBe(0)

    // Tab to navigate to the Features link (skip links and other elements first)
    // Use Tab to navigate through the page
    await page.keyboard.press('Tab') // Skip link
    await page.keyboard.press('Tab') // Skip link action
    await page.keyboard.press('Tab') // Logo/brand
    await page.keyboard.press('Tab') // Features link (first nav link)

    // Verify the Features link is focused
    const featuresLink = page.locator('nav a[href="#features"]')
    const isFocused = await featuresLink.evaluate((el) => document.activeElement === el)

    // If not focused on features link, keep tabbing until we find it
    if (!isFocused) {
      // Navigate through the page using Tab to find the Features link
      for (let i = 0; i < 10; i++) {
        await page.keyboard.press('Tab')
        const currentlyFocused = await featuresLink.evaluate((el) => document.activeElement === el)
        if (currentlyFocused) break
      }
    }

    // Press Enter to activate the link
    await page.keyboard.press('Enter')

    // Wait for scroll animation to complete
    await page.waitForTimeout(1000)

    // Verify we scrolled to the Features section
    const featuresSection = page.locator('#features')
    await expect(featuresSection).toBeVisible()

    // Verify the section is at or near the top of the viewport
    const sectionRect = await featuresSection.boundingBox()
    expect(sectionRect).not.toBeNull()
    expect(sectionRect!.y).toBeLessThan(200)

    // Verify scroll position changed
    const finalScrollY = await page.evaluate(() => window.scrollY)
    expect(finalScrollY).toBeGreaterThan(initialScrollY)
  })

  // Additional test: Verify smooth scroll works from Getting Started link in header
  test('Getting Started navigation link scrolls smoothly', async ({ page }) => {
    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY)
    expect(initialScrollY).toBe(0)

    // Find and click the Getting Started navigation link in the header
    const gettingStartedNavLink = page.locator('nav a[href="#getting-started"]')
    await expect(gettingStartedNavLink).toBeVisible()
    await gettingStartedNavLink.click()

    // Wait for scroll animation to complete
    await page.waitForTimeout(1000)

    // Verify we scrolled to the Getting Started section
    const gettingStartedSection = page.locator('#getting-started')
    await expect(gettingStartedSection).toBeVisible()

    // Verify the section is at or near the top of the viewport
    const sectionRect = await gettingStartedSection.boundingBox()
    expect(sectionRect).not.toBeNull()
    expect(sectionRect!.y).toBeLessThan(200)

    // Verify scroll position changed
    const finalScrollY = await page.evaluate(() => window.scrollY)
    expect(finalScrollY).toBeGreaterThan(initialScrollY)
  })

  // Additional test: Verify smooth scroll animation is not instant
  test('scroll animation is smooth and not instant', async ({ page }) => {
    // Record scroll positions over time
    const scrollPositions: number[] = []

    // Start recording scroll position
    const recordingPromise = page.evaluate(() => {
      return new Promise<number[]>((resolve) => {
        const positions: number[] = []
        const startTime = Date.now()

        const recordPosition = () => {
          positions.push(window.scrollY)
          if (Date.now() - startTime < 800) {
            requestAnimationFrame(recordPosition)
          } else {
            resolve(positions)
          }
        }

        requestAnimationFrame(recordPosition)
      })
    })

    // Click the Features link to trigger scroll
    const featuresNavLink = page.locator('nav a[href="#features"]')
    await featuresNavLink.click()

    // Collect scroll positions
    const positions = await recordingPromise

    // Verify we captured multiple intermediate positions (smooth scroll)
    // An instant jump would have very few intermediate positions
    const uniquePositions = [...new Set(positions)]

    // Should have multiple different scroll positions captured during animation
    // Smooth scroll typically has 10+ unique positions captured at 60fps
    expect(uniquePositions.length).toBeGreaterThan(3)
  })
})
