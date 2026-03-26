/**
 * Responsive Design E2E Tests
 * Owner: Scenarios 7, 8, 9 - Responsive Design
 *
 * Tests for multiple viewports:
 * - Mobile (<768px): Single column, touch targets, no horizontal scroll
 * - Tablet (768-1023px): 2-column features grid
 * - Desktop (>=1024px): 3-column features grid, full navigation
 *
 * Requirements: REQ-8, US-5
 */

import { test, expect, Page } from '@playwright/test'

/**
 * Mobile viewport widths to test
 * Common mobile device widths: iPhone SE (375px), iPhone 12 (390px), iPhone 14 Pro Max (430px), Pixel 7 (412px)
 */
const MOBILE_VIEWPORTS = [
  { width: 375, height: 667, name: 'iPhone SE' },
  { width: 414, height: 896, name: 'iPhone XR/11' },
]

/**
 * Minimum touch target size per WCAG 2.1 guidelines
 */
const MIN_TOUCH_TARGET_SIZE = 44

/**
 * Helper function to check if an element has proper touch target size
 */
async function checkTouchTargetSize(page: Page, selector: string): Promise<{ width: number; height: number }> {
  const element = page.locator(selector).first()
  const box = await element.boundingBox()
  if (!box) {
    throw new Error(`Element ${selector} not found or not visible`)
  }
  return { width: box.width, height: box.height }
}

/**
 * Helper function to get computed font size
 */
async function getComputedFontSize(page: Page, selector: string): Promise<number> {
  const element = page.locator(selector).first()
  const fontSize = await element.evaluate((el) => {
    return parseFloat(window.getComputedStyle(el).fontSize)
  })
  return fontSize
}

test.describe('Mobile Responsive Design (<768px)', () => {
  for (const viewport of MOBILE_VIEWPORTS) {
    test.describe(`Viewport: ${viewport.name} (${viewport.width}px)`, () => {
      test.beforeEach(async ({ page }) => {
        await page.setViewportSize({ width: viewport.width, height: viewport.height })
        await page.goto('/')
        // Wait for page to be fully loaded
        await page.waitForLoadState('networkidle')
      })

      test('all text is readable without zooming (font size >= 12px)', async ({ page }) => {
        // Check hero headline font size
        const headlineFontSize = await getComputedFontSize(page, 'h1')
        expect(headlineFontSize).toBeGreaterThanOrEqual(12)

        // Check hero description font size
        const descFontSize = await getComputedFontSize(page, '[data-testid="hero-description"]')
        expect(descFontSize).toBeGreaterThanOrEqual(12)

        // Check that body text is at least 16px (standard readable size)
        const bodyFontSize = await getComputedFontSize(page, 'body')
        expect(bodyFontSize).toBeGreaterThanOrEqual(16)

        // Check feature card titles
        const featureCards = page.locator('[data-testid="feature-card"]')
        const cardCount = await featureCards.count()
        if (cardCount > 0) {
          const firstCardTitle = featureCards.first().locator('h3')
          if (await firstCardTitle.count() > 0) {
            const titleFontSize = await firstCardTitle.evaluate((el) => {
              return parseFloat(window.getComputedStyle(el).fontSize)
            })
            expect(titleFontSize).toBeGreaterThanOrEqual(12)
          }
        }
      })

      test('no horizontal scrollbar appears', async ({ page }) => {
        // Get document and viewport dimensions
        const scrollWidth = await page.evaluate(() => document.documentElement.scrollWidth)
        const clientWidth = await page.evaluate(() => document.documentElement.clientWidth)

        // Scroll width should not exceed client width (no horizontal overflow)
        expect(scrollWidth).toBeLessThanOrEqual(clientWidth)

        // Additional check: body should not have horizontal overflow
        const bodyOverflowX = await page.evaluate(() => {
          return window.getComputedStyle(document.body).overflowX
        })
        // Body should not have visible horizontal scroll
        expect(bodyOverflowX).not.toBe('scroll')
      })

      test('all CTA buttons have minimum 44x44px touch target', async ({ page }) => {
        // Check primary CTA button in hero section
        const primaryCta = await checkTouchTargetSize(page, '[data-testid="primary-cta"]')
        expect(primaryCta.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET_SIZE)
        expect(primaryCta.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET_SIZE)

        // Check secondary CTA button in hero section
        const secondaryCta = await checkTouchTargetSize(page, '[data-testid="secondary-cta"]')
        expect(secondaryCta.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET_SIZE)
        expect(secondaryCta.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET_SIZE)

        // Check shorten button in inline shortener
        const shortenButton = await checkTouchTargetSize(page, '[data-testid="shorten-button"]')
        expect(shortenButton.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET_SIZE)
        expect(shortenButton.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET_SIZE)

        // Check navbar buttons
        const loginButton = await checkTouchTargetSize(page, '[data-testid="navbar-login-button"]')
        expect(loginButton.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET_SIZE)
        expect(loginButton.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET_SIZE)

        const signupButton = await checkTouchTargetSize(page, '[data-testid="navbar-signup-button"]')
        expect(signupButton.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET_SIZE)
        expect(signupButton.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET_SIZE)
      })

      test('features grid stacks in single column layout', async ({ page }) => {
        // Scroll to features section to ensure it's loaded
        await page.locator('[data-testid="features-section"]').scrollIntoViewIfNeeded()
        await page.waitForTimeout(500) // Allow for any layout adjustments

        const featuresGrid = page.locator('[data-testid="features-grid"]')
        await expect(featuresGrid).toBeVisible()

        // Get feature cards
        const featureCards = page.locator('[data-testid="feature-card"]')
        const cardCount = await featureCards.count()

        if (cardCount >= 2) {
          // Get bounding boxes of first two cards
          const firstCard = await featureCards.nth(0).boundingBox()
          const secondCard = await featureCards.nth(1).boundingBox()

          expect(firstCard).not.toBeNull()
          expect(secondCard).not.toBeNull()

          // In single column layout, cards should be vertically stacked
          // Second card should be below first card (y position greater)
          expect(secondCard!.y).toBeGreaterThan(firstCard!.y)

          // Cards should have similar x positions (aligned in single column)
          // Allow some tolerance for padding differences
          expect(Math.abs(secondCard!.x - firstCard!.x)).toBeLessThan(20)
        }

        // Check that grid has single column layout via CSS
        const gridColumns = await featuresGrid.evaluate((el) => {
          const style = window.getComputedStyle(el)
          return style.gridTemplateColumns
        })
        // Single column should have only one value (not multiple)
        const columnCount = gridColumns.split(' ').filter((col) => col.trim()).length
        expect(columnCount).toBe(1)
      })

      test('URL shortener input and button layout fits properly on mobile', async ({ page }) => {
        const shortener = page.locator('[data-testid="inline-shortener"]')
        await expect(shortener).toBeVisible()

        const urlInput = page.locator('[data-testid="url-input"]')
        const shortenButton = page.locator('[data-testid="shorten-button"]')

        await expect(urlInput).toBeVisible()
        await expect(shortenButton).toBeVisible()

        // Get bounding boxes
        const inputBox = await urlInput.boundingBox()
        const buttonBox = await shortenButton.boundingBox()

        expect(inputBox).not.toBeNull()
        expect(buttonBox).not.toBeNull()

        // Check that elements don't overflow the viewport
        expect(inputBox!.x).toBeGreaterThanOrEqual(0)
        expect(inputBox!.x + inputBox!.width).toBeLessThanOrEqual(viewport.width)

        expect(buttonBox!.x).toBeGreaterThanOrEqual(0)
        expect(buttonBox!.x + buttonBox!.width).toBeLessThanOrEqual(viewport.width)

        // On mobile, elements should either:
        // 1. Stack vertically (button below input)
        // 2. Or fit side by side within viewport
        const isStacked = buttonBox!.y > inputBox!.y + inputBox!.height - 10
        const fitsInline =
          inputBox!.x + inputBox!.width + buttonBox!.width <= viewport.width + 20

        expect(isStacked || fitsInline).toBe(true)
      })

      test('navigation remains accessible on mobile', async ({ page }) => {
        const navbar = page.locator('[data-testid="navbar"]')
        await expect(navbar).toBeVisible()

        // Logo should be visible
        const logo = page.locator('[data-testid="navbar-logo"]')
        await expect(logo).toBeVisible()

        // Navigation should be present (either full menu or hamburger)
        // Check if login and signup buttons are accessible
        const loginButton = page.locator('[data-testid="navbar-login-button"]')
        const signupButton = page.locator('[data-testid="navbar-signup-button"]')

        // At least the main auth buttons should be visible or accessible via menu
        const loginVisible = await loginButton.isVisible()
        const signupVisible = await signupButton.isVisible()

        // Navigation should be usable - at minimum signup should be visible as it's primary CTA
        expect(loginVisible || signupVisible).toBe(true)
      })

      test('hero section adapts properly to mobile viewport', async ({ page }) => {
        const heroSection = page.locator('[data-testid="hero-section"]')
        await expect(heroSection).toBeVisible()

        const heroBox = await heroSection.boundingBox()
        expect(heroBox).not.toBeNull()

        // Hero should span full width
        expect(heroBox!.width).toBeGreaterThanOrEqual(viewport.width - 20)

        // Headline should be visible and centered
        const headline = page.locator('h1')
        await expect(headline).toBeVisible()

        // CTAs should stack vertically on small mobile screens
        const primaryCta = page.locator('[data-testid="primary-cta"]')
        const secondaryCta = page.locator('[data-testid="secondary-cta"]')

        const primaryBox = await primaryCta.boundingBox()
        const secondaryBox = await secondaryCta.boundingBox()

        expect(primaryBox).not.toBeNull()
        expect(secondaryBox).not.toBeNull()

        // On very small screens, buttons may stack (secondary below primary)
        // Or they may be side by side if they fit
        const buttonsStack = secondaryBox!.y > primaryBox!.y
        const buttonsFitInline =
          primaryBox!.x + primaryBox!.width + secondaryBox!.width <= viewport.width

        expect(buttonsStack || buttonsFitInline).toBe(true)
      })

      test('content remains within viewport bounds', async ({ page }) => {
        // Check main sections don't overflow
        const sections = [
          '[data-testid="hero-section"]',
          '[data-testid="inline-shortener"]',
          '[data-testid="features-section"]',
        ]

        for (const selector of sections) {
          const section = page.locator(selector)
          if ((await section.count()) > 0) {
            const box = await section.boundingBox()
            if (box) {
              // Section should start within viewport
              expect(box.x).toBeGreaterThanOrEqual(-5) // Small tolerance for borders

              // Section width shouldn't exceed viewport
              expect(box.width).toBeLessThanOrEqual(viewport.width + 10)
            }
          }
        }
      })
    })
  }
})
