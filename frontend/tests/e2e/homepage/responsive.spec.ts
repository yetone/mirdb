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

import { test, expect } from '@playwright/test'

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
