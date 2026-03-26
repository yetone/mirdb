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

/**
 * Desktop viewport widths to test
 * Common desktop widths: 1280px (standard), 1440px (common laptop), 1920px (full HD)
 */
const DESKTOP_VIEWPORTS = [
  { width: 1280, height: 800, name: 'Standard Desktop' },
  { width: 1440, height: 900, name: 'Large Laptop' },
  { width: 1920, height: 1080, name: 'Full HD' },
]

test.describe('Desktop Responsive Design (≥1024px)', () => {
  for (const viewport of DESKTOP_VIEWPORTS) {
    test.describe(`Viewport: ${viewport.name} (${viewport.width}px)`, () => {
      test.beforeEach(async ({ page }) => {
        await page.setViewportSize({ width: viewport.width, height: viewport.height })
        await page.goto('/')
        // Wait for page to be fully loaded
        await page.waitForLoadState('networkidle')
      })

      test('full desktop layout displays correctly', async ({ page }) => {
        // Hero section should be visible and prominent
        const heroSection = page.locator('[data-testid="hero-section"]')
        await expect(heroSection).toBeVisible()

        const heroBox = await heroSection.boundingBox()
        expect(heroBox).not.toBeNull()
        // Hero should span full width on desktop
        expect(heroBox!.width).toBeGreaterThanOrEqual(viewport.width - 100)

        // Headline should be visible with large font
        const headline = page.locator('h1')
        await expect(headline).toBeVisible()
        const headlineFontSize = await headline.evaluate((el) => {
          return parseFloat(window.getComputedStyle(el).fontSize)
        })
        // Desktop headline should be larger than mobile (at least 32px)
        expect(headlineFontSize).toBeGreaterThanOrEqual(32)

        // Inline shortener should be visible
        const shortener = page.locator('[data-testid="inline-shortener"]')
        await expect(shortener).toBeVisible()

        // Features section should be visible
        const featuresSection = page.locator('[data-testid="features-section"]')
        await expect(featuresSection).toBeVisible()

        // All main sections should be present and properly laid out
        const sections = [
          '[data-testid="hero-section"]',
          '[data-testid="inline-shortener"]',
          '[data-testid="features-section"]',
        ]

        for (const selector of sections) {
          const section = page.locator(selector)
          await expect(section).toBeVisible()
          const box = await section.boundingBox()
          expect(box).not.toBeNull()
          // Section should be centered with reasonable max-width or full-width
          expect(box!.x).toBeGreaterThanOrEqual(0)
        }
      })

      test('features display in 3-column grid layout', async ({ page }) => {
        // Scroll to features section to ensure it's loaded
        await page.locator('[data-testid="features-section"]').scrollIntoViewIfNeeded()
        await page.waitForTimeout(500) // Allow for any layout adjustments

        const featuresGrid = page.locator('[data-testid="features-grid"]')
        await expect(featuresGrid).toBeVisible()

        // Get feature cards
        const featureCards = page.locator('[data-testid="feature-card"]')
        const cardCount = await featureCards.count()

        // Should have multiple feature cards
        expect(cardCount).toBeGreaterThanOrEqual(3)

        if (cardCount >= 3) {
          // Get bounding boxes of first three cards to verify 3-column layout
          const firstCard = await featureCards.nth(0).boundingBox()
          const secondCard = await featureCards.nth(1).boundingBox()
          const thirdCard = await featureCards.nth(2).boundingBox()

          expect(firstCard).not.toBeNull()
          expect(secondCard).not.toBeNull()
          expect(thirdCard).not.toBeNull()

          // In 3-column layout, first three cards should be on the same row
          // (similar Y positions, different X positions)
          const yTolerance = 20 // Allow some tolerance for alignment

          // Cards 1, 2, 3 should have similar Y positions (same row)
          expect(Math.abs(secondCard!.y - firstCard!.y)).toBeLessThan(yTolerance)
          expect(Math.abs(thirdCard!.y - firstCard!.y)).toBeLessThan(yTolerance)

          // Cards should have different X positions (side by side)
          expect(secondCard!.x).toBeGreaterThan(firstCard!.x)
          expect(thirdCard!.x).toBeGreaterThan(secondCard!.x)
        }

        // Check that grid has 3 columns via CSS
        const gridColumns = await featuresGrid.evaluate((el) => {
          const style = window.getComputedStyle(el)
          return style.gridTemplateColumns
        })
        // Should have 3 column values (e.g., "200px 200px 200px" or "1fr 1fr 1fr")
        const columnCount = gridColumns.split(' ').filter((col) => col.trim()).length
        expect(columnCount).toBe(3)
      })

      test('full navigation menu visible (no hamburger icon)', async ({ page }) => {
        const navbar = page.locator('[data-testid="navbar"]')
        await expect(navbar).toBeVisible()

        // Logo should be visible and left-aligned
        const logo = page.locator('[data-testid="navbar-logo"]')
        await expect(logo).toBeVisible()
        const logoBox = await logo.boundingBox()
        expect(logoBox).not.toBeNull()
        // Logo should be on the left side
        expect(logoBox!.x).toBeLessThan(viewport.width / 2)

        // Features link should be visible in navigation
        const featuresLink = page.locator('[data-testid="navbar-features-link"]')
        await expect(featuresLink).toBeVisible()

        // Login button should be visible (not hidden in hamburger menu)
        const loginButton = page.locator('[data-testid="navbar-login-button"]')
        await expect(loginButton).toBeVisible()

        // Sign Up button should be visible
        const signupButton = page.locator('[data-testid="navbar-signup-button"]')
        await expect(signupButton).toBeVisible()

        // Navigation items should be horizontally aligned (same row)
        const loginBox = await loginButton.boundingBox()
        const signupBox = await signupButton.boundingBox()
        const featuresBox = await featuresLink.boundingBox()

        expect(loginBox).not.toBeNull()
        expect(signupBox).not.toBeNull()
        expect(featuresBox).not.toBeNull()

        // All navigation items should have similar Y positions (same row)
        const yTolerance = 30
        expect(Math.abs(loginBox!.y - signupBox!.y)).toBeLessThan(yTolerance)
        expect(Math.abs(loginBox!.y - featuresBox!.y)).toBeLessThan(yTolerance)

        // Navigation items should be on the right side of the viewport
        expect(featuresBox!.x).toBeGreaterThan(viewport.width / 3)
        expect(loginBox!.x).toBeGreaterThan(viewport.width / 3)
        expect(signupBox!.x).toBeGreaterThan(viewport.width / 3)

        // Hamburger menu should NOT be visible on desktop
        const hamburgerMenu = page.locator('[data-testid="navbar-hamburger"]')
        const hamburgerVisible = await hamburgerMenu.isVisible().catch(() => false)
        expect(hamburgerVisible).toBe(false)

        // Mobile menu button should NOT be visible on desktop
        const mobileMenuButton = page.locator('[data-testid="mobile-menu-button"]')
        const mobileMenuVisible = await mobileMenuButton.isVisible().catch(() => false)
        expect(mobileMenuVisible).toBe(false)
      })

      test('hero section CTAs are inline on desktop', async ({ page }) => {
        const primaryCta = page.locator('[data-testid="primary-cta"]')
        const secondaryCta = page.locator('[data-testid="secondary-cta"]')

        await expect(primaryCta).toBeVisible()
        await expect(secondaryCta).toBeVisible()

        const primaryBox = await primaryCta.boundingBox()
        const secondaryBox = await secondaryCta.boundingBox()

        expect(primaryBox).not.toBeNull()
        expect(secondaryBox).not.toBeNull()

        // On desktop, buttons should be side by side (similar Y, different X)
        const yTolerance = 20
        expect(Math.abs(secondaryBox!.y - primaryBox!.y)).toBeLessThan(yTolerance)
        // Secondary should be to the right of primary
        expect(secondaryBox!.x).toBeGreaterThan(primaryBox!.x)
      })

      test('URL shortener has inline layout on desktop', async ({ page }) => {
        const urlInput = page.locator('[data-testid="url-input"]')
        const shortenButton = page.locator('[data-testid="shorten-button"]')

        await expect(urlInput).toBeVisible()
        await expect(shortenButton).toBeVisible()

        const inputBox = await urlInput.boundingBox()
        const buttonBox = await shortenButton.boundingBox()

        expect(inputBox).not.toBeNull()
        expect(buttonBox).not.toBeNull()

        // On desktop, input and button should be on the same row
        const yTolerance = 30
        expect(Math.abs(buttonBox!.y - inputBox!.y)).toBeLessThan(yTolerance)

        // Input should be wider on desktop (take more space)
        expect(inputBox!.width).toBeGreaterThan(200)
      })

      test('content is properly centered with max-width container', async ({ page }) => {
        // Check that content sections use max-width container for readability
        const heroSection = page.locator('[data-testid="hero-section"]')
        const heroBox = await heroSection.boundingBox()

        expect(heroBox).not.toBeNull()

        // On very wide desktops, content should be centered with margins
        if (viewport.width >= 1440) {
          // Hero should not stretch to full viewport width on very wide screens
          // (should use max-width container or centered layout)
          const heroContent = heroSection.locator('.container, .max-w-7xl, .max-w-6xl, .max-w-5xl').first()
          if (await heroContent.count() > 0) {
            const contentBox = await heroContent.boundingBox()
            if (contentBox) {
              // Content should have margins on both sides
              expect(contentBox.x).toBeGreaterThan(0)
            }
          }
        }
      })

      test('typography scales appropriately for desktop', async ({ page }) => {
        // Headline should have larger font on desktop
        const headline = page.locator('h1')
        const headlineFontSize = await headline.evaluate((el) => {
          return parseFloat(window.getComputedStyle(el).fontSize)
        })
        // Desktop headline should be prominent (at least 32px, preferably larger)
        expect(headlineFontSize).toBeGreaterThanOrEqual(32)

        // Feature section heading should also be appropriately sized
        const featuresHeading = page.locator('[data-testid="features-heading"]')
        if (await featuresHeading.count() > 0) {
          await featuresHeading.scrollIntoViewIfNeeded()
          const featuresFontSize = await featuresHeading.evaluate((el) => {
            return parseFloat(window.getComputedStyle(el).fontSize)
          })
          // Section headings should be at least 24px on desktop
          expect(featuresFontSize).toBeGreaterThanOrEqual(24)
        }

        // Body text should be readable
        const bodyFontSize = await getComputedFontSize(page, 'body')
        expect(bodyFontSize).toBeGreaterThanOrEqual(16)
      })
    })
  }
})
