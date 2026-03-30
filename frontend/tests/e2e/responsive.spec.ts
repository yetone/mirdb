/**
 * Responsive design tests for homepage.
 * Owner: Scenario 8 (Mobile) and Scenario 9 (Tablet/Desktop)
 *
 * This file tests responsive behavior across different viewports:
 * - Scenario 8: Mobile viewport tests (< 768px)
 * - Scenario 9: Tablet (768px) and Desktop (1024px+) viewport tests
 */

import { test, expect } from '@playwright/test'

// =============================================================================
// SCENARIO 9: Tablet and Desktop Responsive Tests
// =============================================================================

test.describe('Responsive Design - Tablet and Desktop', () => {
  test.describe('Tablet Viewport (768px)', () => {
    test.use({ viewport: { width: 768, height: 1024 } })

    test('navigation links are visible at tablet width', async ({ page }) => {
      await page.goto('/')

      // Wait for page to load
      await expect(page.getByTestId('homepage')).toBeVisible()

      // At 768px, navigation links should be visible (not hidden in hamburger menu)
      const navbar = page.locator('nav.navbar')
      await expect(navbar).toBeVisible()

      // Check Login and Sign Up links are visible
      const loginLink = page.getByRole('link', { name: /login/i })
      const signUpLink = page.getByRole('link', { name: /sign up/i })

      await expect(loginLink).toBeVisible()
      await expect(signUpLink).toBeVisible()

      // Verify links are not hidden (have non-zero dimensions)
      const loginBox = await loginLink.boundingBox()
      const signUpBox = await signUpLink.boundingBox()

      expect(loginBox).not.toBeNull()
      expect(signUpBox).not.toBeNull()
      expect(loginBox!.width).toBeGreaterThan(0)
      expect(signUpBox!.width).toBeGreaterThan(0)
    })

    test('features grid displays in 2 columns at tablet width', async ({ page }) => {
      await page.goto('/')

      // Wait for features section
      const featuresGrid = page.getByTestId('features-grid')
      await expect(featuresGrid).toBeVisible()

      // Get all feature cards
      const featureCards = page.locator('[data-testid^="feature-card-"]')
      const count = await featureCards.count()
      expect(count).toBe(4)

      // At 768px (md breakpoint), grid should be 2 columns
      // Check that first two cards are roughly on the same row (similar y position)
      const firstCard = featureCards.nth(0)
      const secondCard = featureCards.nth(1)
      const thirdCard = featureCards.nth(2)

      const firstBox = await firstCard.boundingBox()
      const secondBox = await secondCard.boundingBox()
      const thirdBox = await thirdCard.boundingBox()

      expect(firstBox).not.toBeNull()
      expect(secondBox).not.toBeNull()
      expect(thirdBox).not.toBeNull()

      // First and second card should be on the same row (similar Y)
      expect(Math.abs(firstBox!.y - secondBox!.y)).toBeLessThan(10)

      // Third card should be on a different row (lower Y)
      expect(thirdBox!.y).toBeGreaterThan(firstBox!.y + firstBox!.height / 2)
    })
  })

  test.describe('Desktop Viewport (1024px)', () => {
    test.use({ viewport: { width: 1024, height: 768 } })

    test('feature cards display in multi-column grid (4 columns) at desktop width', async ({ page }) => {
      await page.goto('/')

      // Wait for features section
      const featuresGrid = page.getByTestId('features-grid')
      await expect(featuresGrid).toBeVisible()

      // Get all feature cards
      const featureCards = page.locator('[data-testid^="feature-card-"]')
      const count = await featureCards.count()
      expect(count).toBe(4)

      // At 1024px (lg breakpoint), grid should be 4 columns
      // All 4 cards should be on the same row
      const boxes = await Promise.all([
        featureCards.nth(0).boundingBox(),
        featureCards.nth(1).boundingBox(),
        featureCards.nth(2).boundingBox(),
        featureCards.nth(3).boundingBox(),
      ])

      // All boxes should exist
      boxes.forEach((box, i) => {
        expect(box, `Card ${i} should have bounding box`).not.toBeNull()
      })

      // All cards should have similar Y position (same row)
      const baseY = boxes[0]!.y
      boxes.forEach((box, i) => {
        expect(
          Math.abs(box!.y - baseY),
          `Card ${i} should be on same row as first card`
        ).toBeLessThan(10)
      })

      // Cards should be arranged horizontally (increasing X)
      for (let i = 1; i < boxes.length; i++) {
        expect(
          boxes[i]!.x,
          `Card ${i} should be to the right of card ${i - 1}`
        ).toBeGreaterThan(boxes[i - 1]!.x)
      }
    })

    test('full navigation is visible at desktop width', async ({ page }) => {
      await page.goto('/')

      // Wait for page to load
      await expect(page.getByTestId('homepage')).toBeVisible()

      const navbar = page.locator('nav.navbar')
      await expect(navbar).toBeVisible()

      // Logo/brand link should be visible
      const brandLink = page.getByRole('link', { name: /url shortener/i })
      await expect(brandLink).toBeVisible()

      // Navigation links should be visible
      const loginLink = page.getByRole('link', { name: /login/i })
      const signUpLink = page.getByRole('link', { name: /sign up/i })

      await expect(loginLink).toBeVisible()
      await expect(signUpLink).toBeVisible()

      // Theme toggle should be visible
      const themeToggle = page.locator('label.swap')
      await expect(themeToggle).toBeVisible()
    })
  })

  test.describe('Large Desktop Viewport (1440px)', () => {
    test.use({ viewport: { width: 1440, height: 900 } })

    test('content is centered with appropriate max-width constraints', async ({ page }) => {
      await page.goto('/')

      // Wait for features section
      const featuresSection = page.getByTestId('features-section')
      await expect(featuresSection).toBeVisible()

      // Get the features container (has max-w-6xl mx-auto)
      const featuresContainer = page.locator('[data-testid="features-section"] > div.max-w-6xl')
      await expect(featuresContainer).toBeVisible()

      const containerBox = await featuresContainer.boundingBox()
      expect(containerBox).not.toBeNull()

      // max-w-6xl is 72rem = 1152px at default font size
      // Container should be less than or equal to this width
      expect(containerBox!.width).toBeLessThanOrEqual(1152 + 32) // Allow for padding

      // Container should be centered (margins should be roughly equal)
      const viewportWidth = 1440
      const leftMargin = containerBox!.x
      const rightMargin = viewportWidth - (containerBox!.x + containerBox!.width)

      // Left and right margins should be approximately equal (within 50px)
      expect(Math.abs(leftMargin - rightMargin)).toBeLessThan(50)

      // There should be visible margins on both sides
      expect(leftMargin).toBeGreaterThan(10)
      expect(rightMargin).toBeGreaterThan(10)
    })

    test('homepage layout is not stretched at large viewport', async ({ page }) => {
      await page.goto('/')

      // Hero section should have appropriate constraints
      const heroSection = page.getByTestId('hero-section')
      await expect(heroSection).toBeVisible()

      const heroBox = await heroSection.boundingBox()
      expect(heroBox).not.toBeNull()

      // Hero should span full width but content should be constrained
      // The hero section itself might be full-width but the content inside should be constrained
      const heroContent = page.locator('[data-testid="hero-section"] .max-w-4xl, [data-testid="hero-section"] .max-w-2xl, [data-testid="hero-section"] .max-w-3xl').first()

      if (await heroContent.count() > 0) {
        const contentBox = await heroContent.boundingBox()
        if (contentBox) {
          // Content should be constrained and not stretch to full viewport width
          expect(contentBox.width).toBeLessThan(1200)
        }
      }

      // Features grid should maintain 4-column layout
      const featureCards = page.locator('[data-testid^="feature-card-"]')
      const count = await featureCards.count()
      expect(count).toBe(4)

      const boxes = await Promise.all([
        featureCards.nth(0).boundingBox(),
        featureCards.nth(1).boundingBox(),
        featureCards.nth(2).boundingBox(),
        featureCards.nth(3).boundingBox(),
      ])

      // All cards should be on the same row
      const baseY = boxes[0]!.y
      boxes.forEach((box) => {
        expect(Math.abs(box!.y - baseY)).toBeLessThan(10)
      })
    })

    test('all 4 feature cards remain in single row at 1440px', async ({ page }) => {
      await page.goto('/')

      const featuresGrid = page.getByTestId('features-grid')
      await expect(featuresGrid).toBeVisible()

      // Verify grid layout
      const gridStyles = await featuresGrid.evaluate((el) => {
        const styles = window.getComputedStyle(el)
        return {
          display: styles.display,
          gridTemplateColumns: styles.gridTemplateColumns,
        }
      })

      expect(gridStyles.display).toBe('grid')
      // At lg breakpoint (1024px+), should have 4 columns
      // gridTemplateColumns will show actual column widths

      // Get all feature cards and verify they're in one row
      const featureCards = page.locator('[data-testid^="feature-card-"]')
      const boxes = await Promise.all([
        featureCards.nth(0).boundingBox(),
        featureCards.nth(1).boundingBox(),
        featureCards.nth(2).boundingBox(),
        featureCards.nth(3).boundingBox(),
      ])

      // All should be on the same row
      const baseY = boxes[0]!.y
      for (let i = 1; i < boxes.length; i++) {
        expect(Math.abs(boxes[i]!.y - baseY)).toBeLessThan(10)
      }
    })
  })
})

// =============================================================================
// SCENARIO 8: Mobile Responsive Tests
// =============================================================================

// Mobile viewport configuration (iPhone/Pixel 5 width)
const MOBILE_VIEWPORT = { width: 375, height: 667 }

// Minimum touch target size per WCAG guidelines
const MIN_TOUCH_TARGET_SIZE = 44

test.describe('Mobile Layout - Responsive Design', () => {
  test.beforeEach(async ({ page }) => {
    // Set mobile viewport before navigating
    await page.setViewportSize(MOBILE_VIEWPORT)
    await page.goto('/')
    // Wait for page to be fully loaded
    await page.waitForLoadState('domcontentloaded')
  })

  test('should display hamburger menu icon and hide desktop nav links on mobile viewport (375px)', async ({ page }) => {
    // Test Case 1: Render Homepage at viewport width 375px
    // Expected: Hamburger menu icon is visible, desktop nav links are hidden

    // Verify hamburger menu is visible
    const hamburgerMenu = page.locator('[data-testid="hamburger-menu"]')
    await expect(hamburgerMenu).toBeVisible()

    // Verify desktop nav links are hidden
    const desktopNav = page.locator('[data-testid="desktop-nav"]')
    await expect(desktopNav).toBeHidden()

    // Verify desktop login/register buttons are hidden
    const desktopLogin = page.locator('[data-testid="desktop-login"]')
    const desktopRegister = page.locator('[data-testid="desktop-register"]')
    await expect(desktopLogin).toBeHidden()
    await expect(desktopRegister).toBeHidden()
  })

  test('should open mobile navigation menu with Login and Register links when hamburger is clicked', async ({ page }) => {
    // Test Case 2: Click hamburger menu on mobile viewport
    // Expected: Mobile navigation menu opens with Login and Register links

    // Click the hamburger menu
    const hamburgerMenu = page.locator('[data-testid="hamburger-menu"]')
    await hamburgerMenu.click()

    // Verify mobile menu is visible
    const mobileMenu = page.locator('[data-testid="mobile-menu"]')
    await expect(mobileMenu).toBeVisible()

    // Verify Login link is visible in mobile menu
    const mobileLogin = page.locator('[data-testid="mobile-login"]')
    await expect(mobileLogin).toBeVisible()
    await expect(mobileLogin).toHaveText('Login')

    // Verify Register link is visible in mobile menu
    const mobileRegister = page.locator('[data-testid="mobile-register"]')
    await expect(mobileRegister).toBeVisible()
    await expect(mobileRegister).toHaveText('Register')
  })

  test('should have minimum 44x44px touch targets for all interactive elements on mobile', async ({ page }) => {
    // Test Case 3: Measure button and link touch targets at mobile viewport
    // Expected: All interactive elements have minimum 44x44px touch target

    // Test hamburger menu touch target
    const hamburgerMenu = page.locator('[data-testid="hamburger-menu"]')
    const hamburgerBox = await hamburgerMenu.boundingBox()
    expect(hamburgerBox).not.toBeNull()
    expect(hamburgerBox!.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET_SIZE)
    expect(hamburgerBox!.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET_SIZE)

    // Test Shorten URL button touch target
    const shortenButton = page.locator('[data-testid="shorten-button"]')
    await expect(shortenButton).toBeVisible()
    const shortenBox = await shortenButton.boundingBox()
    expect(shortenBox).not.toBeNull()
    expect(shortenBox!.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET_SIZE)
    expect(shortenBox!.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET_SIZE)

    // Test URL input touch target
    const urlInput = page.locator('[data-testid="url-input"]')
    await expect(urlInput).toBeVisible()
    const inputBox = await urlInput.boundingBox()
    expect(inputBox).not.toBeNull()
    expect(inputBox!.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET_SIZE)

    // Open mobile menu and test links
    await hamburgerMenu.click()
    const mobileMenu = page.locator('[data-testid="mobile-menu"]')
    await expect(mobileMenu).toBeVisible()

    // Test mobile Login link touch target
    const mobileLogin = page.locator('[data-testid="mobile-login"]')
    const loginBox = await mobileLogin.boundingBox()
    expect(loginBox).not.toBeNull()
    expect(loginBox!.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET_SIZE)
    expect(loginBox!.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET_SIZE)

    // Test mobile Register link touch target
    const mobileRegister = page.locator('[data-testid="mobile-register"]')
    const registerBox = await mobileRegister.boundingBox()
    expect(registerBox).not.toBeNull()
    expect(registerBox!.width).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET_SIZE)
    expect(registerBox!.height).toBeGreaterThanOrEqual(MIN_TOUCH_TARGET_SIZE)
  })

  test('should stack feature cards vertically in single column on mobile viewport', async ({ page }) => {
    // Test Case 4: Render features section at mobile viewport
    // Expected: Feature cards stack vertically in single column

    // Scroll to features section
    await page.locator('[data-testid="features-section"]').scrollIntoViewIfNeeded()

    // Get the features grid
    const featuresGrid = page.locator('[data-testid="features-grid"]')
    await expect(featuresGrid).toBeVisible()

    // Get all feature cards
    const featureCards = page.locator('[data-testid^="feature-card-"]')
    const count = await featureCards.count()
    expect(count).toBeGreaterThanOrEqual(4)

    // Get bounding boxes of first two feature cards
    const firstCard = page.locator('[data-testid="feature-card-url-shortening"]')
    const secondCard = page.locator('[data-testid="feature-card-analytics-dashboard"]')

    await expect(firstCard).toBeVisible()
    await expect(secondCard).toBeVisible()

    const firstBox = await firstCard.boundingBox()
    const secondBox = await secondCard.boundingBox()

    expect(firstBox).not.toBeNull()
    expect(secondBox).not.toBeNull()

    // Verify vertical stacking: second card should be below first card
    // (second card's top position should be greater than first card's bottom position)
    const firstCardBottom = firstBox!.y + firstBox!.height
    expect(secondBox!.y).toBeGreaterThanOrEqual(firstCardBottom - 10) // Allow small margin for spacing

    // Verify single column: cards should have similar left positions (within viewport padding)
    expect(Math.abs(firstBox!.x - secondBox!.x)).toBeLessThan(50)

    // Verify cards span nearly full viewport width (accounting for padding)
    const viewportWidth = MOBILE_VIEWPORT.width
    expect(firstBox!.width).toBeGreaterThan(viewportWidth * 0.7) // At least 70% of viewport width
    expect(secondBox!.width).toBeGreaterThan(viewportWidth * 0.7)
  })

  test('should close mobile menu when clicking a navigation link', async ({ page }) => {
    // Additional test for better UX verification
    const hamburgerMenu = page.locator('[data-testid="hamburger-menu"]')
    await hamburgerMenu.click()

    const mobileMenu = page.locator('[data-testid="mobile-menu"]')
    await expect(mobileMenu).toBeVisible()

    // Click login link
    const mobileLogin = page.locator('[data-testid="mobile-login"]')
    await mobileLogin.click()

    // Should navigate to login page
    await expect(page).toHaveURL(/\/login/)
  })

  test('should toggle hamburger menu icon between open and close states', async ({ page }) => {
    const hamburgerMenu = page.locator('[data-testid="hamburger-menu"]')

    // Initially should have "Open menu" label
    await expect(hamburgerMenu).toHaveAttribute('aria-label', 'Open menu')
    await expect(hamburgerMenu).toHaveAttribute('aria-expanded', 'false')

    // Click to open
    await hamburgerMenu.click()

    // Should now have "Close menu" label
    await expect(hamburgerMenu).toHaveAttribute('aria-label', 'Close menu')
    await expect(hamburgerMenu).toHaveAttribute('aria-expanded', 'true')

    // Click to close
    await hamburgerMenu.click()

    // Should be back to "Open menu" label
    await expect(hamburgerMenu).toHaveAttribute('aria-label', 'Open menu')
    await expect(hamburgerMenu).toHaveAttribute('aria-expanded', 'false')
  })
})
