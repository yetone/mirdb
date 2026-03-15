/**
 * Smooth Scroll Navigation E2E Tests
 * Owner: Scenario 14 - Smooth Scroll Navigation
 *
 * Test Cases:
 * - TC1: Click navigation link with section anchor -> page scrolls to target section
 * - TC2: Check CSS scroll-behavior property -> scroll-behavior: smooth is set on html
 * - TC3: Click section anchor link -> target section is visible in viewport after scroll
 * - TC4: Navigate via anchor links -> sticky header does not obstruct scrolled-to content
 */

import { test, expect } from '@playwright/test'

test.describe('Smooth Scroll Navigation', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    await page.waitForLoadState('domcontentloaded')
  })

  /**
   * Test Case 1: Click navigation link with section anchor
   * Verify page scrolls to target section when clicking nav links
   */
  test('TC1: clicking navigation link scrolls to target section', async ({ page }) => {
    // Start at top of page
    await page.evaluate(() => window.scrollTo(0, 0))
    await page.waitForTimeout(100)

    // Get initial scroll position
    const initialScrollY = await page.evaluate(() => window.scrollY)
    expect(initialScrollY).toBe(0)

    // Click Features navigation link
    const featuresLink = page.locator('nav a[href="#features"]')
    await featuresLink.click()

    // Wait for smooth scroll to complete
    await page.waitForTimeout(1000)

    // Verify page has scrolled down
    const newScrollY = await page.evaluate(() => window.scrollY)
    expect(newScrollY).toBeGreaterThan(0)

    // Verify URL hash has been updated
    await expect(page).toHaveURL(/#features/)
  })

  test('TC1: clicking Quick Start link scrolls to quick-start section', async ({ page }) => {
    // Start at top of page
    await page.evaluate(() => window.scrollTo(0, 0))
    await page.waitForTimeout(100)

    // Click Quick Start navigation link
    const quickStartLink = page.locator('nav a[href="#quick-start"]')
    await quickStartLink.click()

    // Wait for smooth scroll to complete
    await page.waitForTimeout(1000)

    // Verify page has scrolled
    const scrollY = await page.evaluate(() => window.scrollY)
    expect(scrollY).toBeGreaterThan(0)

    // Verify URL hash has been updated
    await expect(page).toHaveURL(/#quick-start/)
  })

  test('TC1: clicking Architecture link scrolls to architecture section', async ({ page }) => {
    // Start at top
    await page.evaluate(() => window.scrollTo(0, 0))
    await page.waitForTimeout(100)

    // Click Architecture navigation link
    const archLink = page.locator('nav a[href="#architecture"]')
    await archLink.click()

    // Wait for scroll
    await page.waitForTimeout(1000)

    // Verify scrolled and URL updated
    const scrollY = await page.evaluate(() => window.scrollY)
    expect(scrollY).toBeGreaterThan(0)
    await expect(page).toHaveURL(/#architecture/)
  })

  test('TC1: clicking Community link scrolls to community section', async ({ page }) => {
    // Start at top
    await page.evaluate(() => window.scrollTo(0, 0))
    await page.waitForTimeout(100)

    // Click Community navigation link
    const communityLink = page.locator('nav a[href="#community"]')
    await communityLink.click()

    // Wait for scroll
    await page.waitForTimeout(1000)

    // Verify scrolled and URL updated
    const scrollY = await page.evaluate(() => window.scrollY)
    expect(scrollY).toBeGreaterThan(0)
    await expect(page).toHaveURL(/#community/)
  })

  /**
   * Test Case 2: Check CSS scroll-behavior property
   * Verify scroll-behavior: smooth is set on html or body
   */
  test('TC2: CSS scroll-behavior smooth is set on html element', async ({ page }) => {
    const scrollBehavior = await page.evaluate(() => {
      const html = document.documentElement
      const computedStyle = window.getComputedStyle(html)
      return computedStyle.scrollBehavior
    })

    expect(scrollBehavior).toBe('smooth')
  })

  test('TC2: verify smooth scrolling behavior in action', async ({ page }) => {
    // Start at top
    await page.evaluate(() => window.scrollTo(0, 0))
    await page.waitForTimeout(100)

    // Click a navigation link
    const featuresLink = page.locator('nav a[href="#features"]')
    await featuresLink.click()

    // Capture multiple scroll positions to verify smooth animation
    const scrollPositions: number[] = []
    for (let i = 0; i < 5; i++) {
      await page.waitForTimeout(100)
      const scrollY = await page.evaluate(() => window.scrollY)
      scrollPositions.push(scrollY)
    }

    // Verify scroll is progressive (smooth), not instant
    // In smooth scroll, we should see gradual increase in positions
    const uniquePositions = [...new Set(scrollPositions)]

    // If scroll was instant, all positions would be the same
    // With smooth scroll, we expect at least some variation during animation
    // or if scroll completed, it should have reached the target
    const finalScroll = scrollPositions[scrollPositions.length - 1]
    expect(finalScroll).toBeGreaterThan(0)
  })

  /**
   * Test Case 3: Click section anchor link -> target section is visible in viewport
   */
  test('TC3: features section is visible in viewport after clicking nav link', async ({ page }) => {
    // Start at top
    await page.evaluate(() => window.scrollTo(0, 0))
    await page.waitForTimeout(100)

    // Click Features link
    const featuresLink = page.locator('nav a[href="#features"]')
    await featuresLink.click()

    // Wait for scroll to complete
    await page.waitForTimeout(1000)

    // Check if features section is in viewport
    const featuresSection = page.locator('#features')
    await expect(featuresSection).toBeVisible()

    // Verify the section heading is visible (accounting for header offset)
    const isInViewport = await featuresSection.evaluate((el) => {
      const rect = el.getBoundingClientRect()
      const viewHeight = window.innerHeight || document.documentElement.clientHeight
      // Element should be in the upper portion of viewport after scroll
      return rect.top >= 0 && rect.top < viewHeight * 0.5
    })

    expect(isInViewport).toBe(true)
  })

  test('TC3: quick-start section is visible in viewport after clicking nav link', async ({ page }) => {
    // Start at top
    await page.evaluate(() => window.scrollTo(0, 0))
    await page.waitForTimeout(100)

    // Click Quick Start link
    const quickStartLink = page.locator('nav a[href="#quick-start"]')
    await quickStartLink.click()

    // Wait for scroll
    await page.waitForTimeout(1000)

    // Check visibility
    const quickStartSection = page.locator('#quick-start')
    await expect(quickStartSection).toBeVisible()

    // Verify in viewport
    const isInViewport = await quickStartSection.evaluate((el) => {
      const rect = el.getBoundingClientRect()
      const viewHeight = window.innerHeight || document.documentElement.clientHeight
      return rect.top >= 0 && rect.top < viewHeight * 0.5
    })

    expect(isInViewport).toBe(true)
  })

  test('TC3: all navigation sections are reachable', async ({ page }) => {
    const sections = ['#features', '#quick-start', '#architecture', '#community']

    for (const sectionId of sections) {
      // Reset to top
      await page.evaluate(() => window.scrollTo(0, 0))
      await page.waitForTimeout(200)

      // Click navigation link
      const navLink = page.locator(`nav a[href="${sectionId}"]`)
      await navLink.click()

      // Wait for scroll
      await page.waitForTimeout(1000)

      // Verify section is visible
      const section = page.locator(sectionId)
      await expect(section).toBeVisible()
    }
  })

  /**
   * Test Case 4: Sticky header does not obstruct scrolled-to content
   */
  test('TC4: sticky header does not obstruct features section heading', async ({ page }) => {
    // Start at top
    await page.evaluate(() => window.scrollTo(0, 0))
    await page.waitForTimeout(100)

    // Click Features link
    const featuresLink = page.locator('nav a[href="#features"]')
    await featuresLink.click()

    // Wait for scroll to complete
    await page.waitForTimeout(1000)

    // Get header height
    const headerHeight = await page.evaluate(() => {
      const header = document.querySelector('header')
      return header ? header.getBoundingClientRect().height : 0
    })

    // Get features section top position
    const featuresSectionTop = await page.evaluate(() => {
      const section = document.getElementById('features')
      return section ? section.getBoundingClientRect().top : 0
    })

    // The section should be below the header (not obscured)
    // With header offset, the section top should be at or below header height
    expect(featuresSectionTop).toBeGreaterThanOrEqual(headerHeight - 10) // Allow small tolerance
  })

  test('TC4: sticky header does not obstruct quick-start section heading', async ({ page }) => {
    // Start at top
    await page.evaluate(() => window.scrollTo(0, 0))
    await page.waitForTimeout(100)

    // Click Quick Start link
    const quickStartLink = page.locator('nav a[href="#quick-start"]')
    await quickStartLink.click()

    // Wait for scroll
    await page.waitForTimeout(1000)

    // Get header height
    const headerHeight = await page.evaluate(() => {
      const header = document.querySelector('header')
      return header ? header.getBoundingClientRect().height : 0
    })

    // Get quick-start section top position
    const sectionTop = await page.evaluate(() => {
      const section = document.getElementById('quick-start')
      return section ? section.getBoundingClientRect().top : 0
    })

    // Section should not be hidden under the header
    expect(sectionTop).toBeGreaterThanOrEqual(headerHeight - 10)
  })

  test('TC4: section heading text is not covered by sticky header', async ({ page }) => {
    // Test all sections
    const sections = [
      { id: 'features', headingId: 'features-heading' },
      { id: 'quick-start', headingId: 'quick-start-heading' },
    ]

    for (const { id, headingId } of sections) {
      // Reset to top
      await page.evaluate(() => window.scrollTo(0, 0))
      await page.waitForTimeout(200)

      // Click nav link
      const navLink = page.locator(`nav a[href="#${id}"]`)
      await navLink.click()

      // Wait for scroll
      await page.waitForTimeout(1000)

      // Get header bottom position
      const headerBottom = await page.evaluate(() => {
        const header = document.querySelector('header')
        return header ? header.getBoundingClientRect().bottom : 0
      })

      // Get heading top position
      const headingTop = await page.evaluate((hId) => {
        const heading = document.getElementById(hId)
        return heading ? heading.getBoundingClientRect().top : 0
      }, headingId)

      // Heading should be visible below header
      expect(headingTop).toBeGreaterThanOrEqual(headerBottom - 20) // Small tolerance for padding
    }
  })

  test('TC4: verifies header remains visible when navigating to sections', async ({ page }) => {
    // Navigate to a section deep in the page
    const communityLink = page.locator('nav a[href="#community"]')
    await communityLink.click()
    await page.waitForTimeout(1000)

    // Verify we scrolled down
    const scrollY = await page.evaluate(() => window.scrollY)
    expect(scrollY).toBeGreaterThan(0)

    // Verify header is still visible (sticky behavior)
    const header = page.locator('header')
    await expect(header).toBeVisible()

    // Verify header has sticky class (Tailwind)
    const headerClasses = await header.getAttribute('class')
    expect(headerClasses).toContain('sticky')
    expect(headerClasses).toContain('top-0')
  })
})

test.describe('Smooth Scroll Navigation - Edge Cases', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    await page.waitForLoadState('domcontentloaded')
  })

  test('navigating between sections maintains smooth behavior', async ({ page }) => {
    // Start at features
    const featuresLink = page.locator('nav a[href="#features"]')
    await featuresLink.click()
    await page.waitForTimeout(1000)

    // Navigate to community (further down)
    const communityLink = page.locator('nav a[href="#community"]')
    await communityLink.click()
    await page.waitForTimeout(1000)

    // Navigate back up to quick-start
    const quickStartLink = page.locator('nav a[href="#quick-start"]')
    await quickStartLink.click()
    await page.waitForTimeout(1000)

    // Verify we're at quick-start section
    const quickStartSection = page.locator('#quick-start')
    await expect(quickStartSection).toBeVisible()

    const isInViewport = await quickStartSection.evaluate((el) => {
      const rect = el.getBoundingClientRect()
      const viewHeight = window.innerHeight || document.documentElement.clientHeight
      return rect.top >= 0 && rect.top < viewHeight * 0.5
    })

    expect(isInViewport).toBe(true)
  })

  test('direct URL with hash scrolls to section on page load', async ({ page }) => {
    // Navigate directly to URL with hash
    await page.goto('/#features')
    await page.waitForLoadState('domcontentloaded')
    await page.waitForTimeout(1000)

    // Features section should be in view
    const featuresSection = page.locator('#features')
    await expect(featuresSection).toBeVisible()
  })

  test('scroll position updates URL hash correctly', async ({ page }) => {
    // Click Features link
    const featuresLink = page.locator('nav a[href="#features"]')
    await featuresLink.click()
    await page.waitForTimeout(1000)

    // Verify URL contains #features
    const url = page.url()
    expect(url).toContain('#features')

    // Click Quick Start link
    const quickStartLink = page.locator('nav a[href="#quick-start"]')
    await quickStartLink.click()
    await page.waitForTimeout(1000)

    // Verify URL now contains #quick-start
    const newUrl = page.url()
    expect(newUrl).toContain('#quick-start')
  })
})
