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

/**
 * Scenario 7: Responsive Design - Mobile
 * Tests homepage displays correctly on mobile viewports (<768px)
 */
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
 * Scenario 8: Responsive Design - Tablet
 * Tests homepage displays correctly on tablet viewports (768px-1023px)
 */
test.describe('Responsive Design - Tablet', () => {
  // Test at 768px (minimum tablet width)
  test.describe('768px viewport', () => {
    test.use({ viewport: { width: 768, height: 1024 } })

    test.beforeEach(async ({ page }) => {
      await page.goto('/')
    })

    // Test Case 1: Render homepage at 768px viewport width
    test('layout adapts to tablet breakpoint correctly at 768px', async ({ page }) => {
      // Verify homepage loads
      const homePage = page.getByTestId('home-page')
      await expect(homePage).toBeVisible()

      // Verify hero section is visible
      const heroSection = page.getByTestId('hero-section')
      await expect(heroSection).toBeVisible()

      // Verify there's no horizontal scrolling
      const scrollWidth = await page.evaluate(() => document.documentElement.scrollWidth)
      const clientWidth = await page.evaluate(() => document.documentElement.clientWidth)
      expect(scrollWidth).toBeLessThanOrEqual(clientWidth)

      // Verify navigation is visible
      const navbar = page.getByTestId('navbar')
      await expect(navbar).toBeVisible()

      // Verify features section is visible
      const featuresSection = page.getByTestId('features-section')
      await expect(featuresSection).toBeAttached()
    })

    // Test Case 2: Render features grid at tablet viewport
    test('features display in 2-column grid layout at 768px', async ({ page }) => {
      // Navigate to features section
      const featuresSection = page.getByTestId('features-section')
      await featuresSection.scrollIntoViewIfNeeded()

      // Get the features grid
      const featuresGrid = page.getByTestId('features-grid')
      await expect(featuresGrid).toBeVisible()

      // Verify grid has 2 columns at tablet breakpoint
      // Tailwind md:grid-cols-2 applies at 768px+
      const gridStyle = await featuresGrid.evaluate((el) => {
        return window.getComputedStyle(el).gridTemplateColumns
      })

      // Should have 2 columns (two fr values or two fixed widths)
      const columnCount = gridStyle.split(' ').length
      expect(columnCount).toBe(2)

      // Verify feature cards are visible
      const featureCards = page.locator('[data-testid="feature-card"]')
      const cardCount = await featureCards.count()
      expect(cardCount).toBeGreaterThan(0)

      // Verify all cards are visible without horizontal scroll
      for (let i = 0; i < Math.min(cardCount, 3); i++) {
        await expect(featureCards.nth(i)).toBeVisible()
      }
    })

    // Test Case 3: Test URL shortener at tablet viewport
    test('URL input and button are properly sized and functional at 768px', async ({ page }) => {
      // Get inline shortener
      const shortener = page.getByTestId('inline-shortener')
      await expect(shortener).toBeVisible()

      // Get input and button
      const urlInput = page.getByTestId('url-input')
      const shortenButton = page.getByTestId('shorten-button')

      await expect(urlInput).toBeVisible()
      await expect(shortenButton).toBeVisible()

      // Verify input is properly sized (not too small for tablet)
      const inputBox = await urlInput.boundingBox()
      expect(inputBox).not.toBeNull()
      expect(inputBox!.width).toBeGreaterThan(200) // Minimum usable width

      // Verify button has adequate touch target (min 44px per accessibility)
      const buttonBox = await shortenButton.boundingBox()
      expect(buttonBox).not.toBeNull()
      expect(buttonBox!.height).toBeGreaterThanOrEqual(44)

      // Verify input is focusable and accepts text
      await urlInput.fill('https://example.com/test')
      await expect(urlInput).toHaveValue('https://example.com/test')

      // Verify button is clickable
      await expect(shortenButton).toBeEnabled()
    })
  })

  // Test at 834px (common iPad width)
  test.describe('834px viewport (iPad)', () => {
    test.use({ viewport: { width: 834, height: 1112 } })

    test.beforeEach(async ({ page }) => {
      await page.goto('/')
    })

    test('homepage adapts correctly at iPad viewport', async ({ page }) => {
      // Verify homepage loads
      const homePage = page.getByTestId('home-page')
      await expect(homePage).toBeVisible()

      // Verify hero section
      const heroSection = page.getByTestId('hero-section')
      await expect(heroSection).toBeVisible()

      // Verify hero headline is visible
      const headline = page.getByRole('heading', { level: 1 })
      await expect(headline).toBeVisible()

      // Verify CTAs are visible and properly arranged
      const primaryCta = page.getByTestId('primary-cta')
      const secondaryCta = page.getByTestId('secondary-cta')
      await expect(primaryCta).toBeVisible()
      await expect(secondaryCta).toBeVisible()

      // Verify CTAs are arranged horizontally (sm:flex-row applies)
      const primaryBox = await primaryCta.boundingBox()
      const secondaryBox = await secondaryCta.boundingBox()
      expect(primaryBox).not.toBeNull()
      expect(secondaryBox).not.toBeNull()
      // Same Y position means horizontal arrangement
      expect(Math.abs(primaryBox!.y - secondaryBox!.y)).toBeLessThan(10)
    })

    test('features grid shows 2 columns at iPad viewport', async ({ page }) => {
      const featuresGrid = page.getByTestId('features-grid')
      await featuresGrid.scrollIntoViewIfNeeded()
      await expect(featuresGrid).toBeVisible()

      // Verify 2-column layout
      const gridStyle = await featuresGrid.evaluate((el) => {
        return window.getComputedStyle(el).gridTemplateColumns
      })
      const columnCount = gridStyle.split(' ').length
      expect(columnCount).toBe(2)
    })
  })

  // Test at 1023px (upper bound of tablet viewport)
  test.describe('1023px viewport (max tablet)', () => {
    test.use({ viewport: { width: 1023, height: 768 } })

    test.beforeEach(async ({ page }) => {
      await page.goto('/')
    })

    test('features grid still shows 2 columns at 1023px (just below desktop)', async ({ page }) => {
      const featuresGrid = page.getByTestId('features-grid')
      await featuresGrid.scrollIntoViewIfNeeded()
      await expect(featuresGrid).toBeVisible()

      // At 1023px, should still be 2 columns (lg breakpoint is 1024px)
      const gridStyle = await featuresGrid.evaluate((el) => {
        return window.getComputedStyle(el).gridTemplateColumns
      })
      const columnCount = gridStyle.split(' ').length
      expect(columnCount).toBe(2)
    })

    test('URL shortener is fully functional at max tablet width', async ({ page }) => {
      const urlInput = page.getByTestId('url-input')
      const shortenButton = page.getByTestId('shorten-button')

      // Test interaction
      await urlInput.fill('https://example.com/a-long-url-to-shorten')
      await expect(urlInput).toHaveValue('https://example.com/a-long-url-to-shorten')

      // Verify button is enabled and properly sized
      await expect(shortenButton).toBeEnabled()

      const buttonBox = await shortenButton.boundingBox()
      expect(buttonBox).not.toBeNull()
      expect(buttonBox!.height).toBeGreaterThanOrEqual(44)
    })

    test('navigation elements remain visible and accessible', async ({ page }) => {
      const navbar = page.getByTestId('navbar')
      await expect(navbar).toBeVisible()

      const logo = page.getByTestId('navbar-logo')
      await expect(logo).toBeVisible()

      const loginButton = page.getByTestId('navbar-login-button')
      await expect(loginButton).toBeVisible()

      const signUpButton = page.getByTestId('navbar-signup-button')
      await expect(signUpButton).toBeVisible()
    })
  })

  // Test all interactive elements work at tablet viewport
  test.describe('Interactive elements at tablet viewport', () => {
    test.use({ viewport: { width: 768, height: 1024 } })

    test.beforeEach(async ({ page }) => {
      await page.goto('/')
    })

    test('all CTAs are clickable with adequate touch targets', async ({ page }) => {
      // Check primary CTA
      const primaryCta = page.getByTestId('primary-cta')
      const primaryBox = await primaryCta.boundingBox()
      expect(primaryBox).not.toBeNull()
      expect(primaryBox!.height).toBeGreaterThanOrEqual(44)
      expect(primaryBox!.width).toBeGreaterThanOrEqual(44)

      // Check secondary CTA
      const secondaryCta = page.getByTestId('secondary-cta')
      const secondaryBox = await secondaryCta.boundingBox()
      expect(secondaryBox).not.toBeNull()
      expect(secondaryBox!.height).toBeGreaterThanOrEqual(44)
      expect(secondaryBox!.width).toBeGreaterThanOrEqual(44)

      // Check shorten button
      const shortenButton = page.getByTestId('shorten-button')
      const shortenBox = await shortenButton.boundingBox()
      expect(shortenBox).not.toBeNull()
      expect(shortenBox!.height).toBeGreaterThanOrEqual(44)
    })

    test('Features link scroll works at tablet viewport', async ({ page }) => {
      const featuresLink = page.getByTestId('navbar-features-link')
      await expect(featuresLink).toBeVisible()

      // Get initial scroll position
      const initialScrollY = await page.evaluate(() => window.scrollY)

      // Click features link
      await featuresLink.click()

      // Wait for scroll
      await page.waitForTimeout(600)

      // Verify scroll occurred
      const finalScrollY = await page.evaluate(() => window.scrollY)
      expect(finalScrollY).toBeGreaterThan(initialScrollY)

      // Verify features section is in viewport
      const featuresSection = page.getByTestId('features-section')
      await expect(featuresSection).toBeInViewport()
    })
  })
})
