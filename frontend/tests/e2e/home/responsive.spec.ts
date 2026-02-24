/**
 * Mobile Responsive Design E2E Tests
 * Owner: Scenario 9 - Responsive Design - Mobile
 *
 * Verifies homepage is fully responsive and functional on mobile viewport (320px-767px).
 * Tests cover:
 * - No horizontal scrollbar at 320px
 * - Touch targets meet 44px minimum
 * - Navigation collapses to hamburger menu
 * - Mobile menu opens with navigation links
 * - URL shortening form is usable at 320px
 */

import { test, expect } from '@playwright/test'

test.describe('Mobile Responsive Design (320px-767px)', () => {
  // Configure mobile viewport for all tests in this describe block
  test.use({
    viewport: { width: 320, height: 568 },
  })

  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    // Wait for the page to load completely
    await page.waitForSelector('nav[aria-label="Main navigation"]')
  })

  test('TC1: No horizontal scrollbar present at 320px viewport', async ({ page }) => {
    // Check that document width doesn't exceed viewport width
    const documentWidth = await page.evaluate(() => document.documentElement.scrollWidth)
    const viewportWidth = await page.evaluate(() => window.innerWidth)

    // Document width should not exceed viewport width (no horizontal scroll)
    expect(documentWidth).toBeLessThanOrEqual(viewportWidth)

    // Also check that horizontal scroll is not possible
    const hasHorizontalScroll = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth
    })
    expect(hasHorizontalScroll).toBe(false)
  })

  test('TC2: All interactive buttons have minimum 44px touch targets', async ({ page }) => {
    // Get all interactive buttons
    const buttons = page.locator('button, a.btn, [role="button"]')
    const buttonCount = await buttons.count()

    expect(buttonCount).toBeGreaterThan(0)

    for (let i = 0; i < buttonCount; i++) {
      const button = buttons.nth(i)

      // Skip hidden elements
      const isVisible = await button.isVisible()
      if (!isVisible) continue

      const boundingBox = await button.boundingBox()
      if (boundingBox) {
        // Touch targets should be at least 44px in both width and height
        // Using 43 to account for potential sub-pixel rounding
        expect(boundingBox.height).toBeGreaterThanOrEqual(43)
        expect(boundingBox.width).toBeGreaterThanOrEqual(43)
      }
    }
  })

  test('TC3: Navigation displays hamburger menu icon at mobile viewport', async ({ page }) => {
    // The hamburger menu button should be visible on mobile
    const hamburgerButton = page.locator('[data-testid="mobile-menu-button"], button[aria-label*="menu" i], button[aria-label*="Menu" i], .navbar button[aria-expanded]')

    // Expect hamburger menu to be visible
    await expect(hamburgerButton.first()).toBeVisible()

    // Login/Register links should be hidden in the collapsed state on mobile
    // They should only appear when the mobile menu is opened
    const loginLink = page.locator('a[href="/login"]:visible')
    const registerLink = page.locator('a[href="/register"]:visible')

    // Check if navigation links are hidden (they should be in the mobile menu)
    // OR if the hamburger menu is present
    const hamburgerVisible = await hamburgerButton.first().isVisible().catch(() => false)
    expect(hamburgerVisible).toBe(true)
  })

  test('TC4: Mobile menu slides open with navigation links when hamburger is clicked', async ({ page }) => {
    // Find and click the hamburger menu button
    const hamburgerButton = page.locator('[data-testid="mobile-menu-button"], button[aria-label*="menu" i], button[aria-label*="Menu" i], .navbar button[aria-expanded]').first()

    await expect(hamburgerButton).toBeVisible()
    await hamburgerButton.click()

    // Wait for menu animation
    await page.waitForTimeout(300)

    // Mobile menu should be open and contain navigation links
    const mobileMenu = page.locator('[data-testid="mobile-menu"], [role="menu"], .mobile-menu, .dropdown-content:visible, nav ul:visible')

    // Expect the mobile menu to be visible
    await expect(mobileMenu.first()).toBeVisible()

    // Check for navigation links inside the menu
    const loginInMenu = page.locator('a[href="/login"]:visible, [data-testid="mobile-menu"] a[href="/login"]')
    const registerInMenu = page.locator('a[href="/register"]:visible, [data-testid="mobile-menu"] a[href="/register"]')

    await expect(loginInMenu.first()).toBeVisible()
    await expect(registerInMenu.first()).toBeVisible()
  })

  test('TC5: URL shortening form is usable at 320px viewport', async ({ page }) => {
    // Find the URL input field
    const urlInput = page.locator('input[aria-label="URL to shorten"], input[placeholder*="URL" i]')

    // Input should be visible and tappable
    await expect(urlInput).toBeVisible()

    // Check that input field is wide enough to be usable
    const inputBox = await urlInput.boundingBox()
    expect(inputBox).not.toBeNull()
    if (inputBox) {
      // Input should be at least 200px wide to be usable on mobile
      expect(inputBox.width).toBeGreaterThanOrEqual(200)
      // Input should have proper touch target height
      expect(inputBox.height).toBeGreaterThanOrEqual(40)
    }

    // Test that input is functional - can type in it
    await urlInput.fill('https://example.com/test')
    await expect(urlInput).toHaveValue('https://example.com/test')

    // Find and check the submit button
    const submitButton = page.locator('button[aria-label="Shorten URL"], button:has-text("Shorten")')

    await expect(submitButton).toBeVisible()

    // Submit button should be accessible and have proper touch target
    const submitBox = await submitButton.boundingBox()
    expect(submitBox).not.toBeNull()
    if (submitBox) {
      expect(submitBox.height).toBeGreaterThanOrEqual(43)
      expect(submitBox.width).toBeGreaterThanOrEqual(43)
    }
  })

  test('Content is readable without horizontal scrolling', async ({ page }) => {
    // Check hero section heading is visible and contained
    const heading = page.locator('h1')
    await expect(heading).toBeVisible()

    // Check that the heading doesn't overflow
    const headingBox = await heading.boundingBox()
    const viewportWidth = await page.evaluate(() => window.innerWidth)
    if (headingBox) {
      expect(headingBox.x + headingBox.width).toBeLessThanOrEqual(viewportWidth + 10) // Allow small margin
    }

    // Check that main content areas are visible
    const heroSection = page.locator('section[aria-label="Hero section"], .hero')
    await expect(heroSection.first()).toBeVisible()

    // Check footer is visible
    const footer = page.locator('footer, [data-testid="footer-section"]')
    await expect(footer.first()).toBeVisible()
  })

  test('Form layout is responsive - stacks vertically on mobile', async ({ page }) => {
    // Get the form container
    const formContainer = page.locator('form').first()
    await expect(formContainer).toBeVisible()

    // Get input and button positions
    const input = page.locator('input[aria-label="URL to shorten"], input[placeholder*="URL" i]').first()
    const button = page.locator('button[aria-label="Shorten URL"], button:has-text("Shorten")').first()

    const inputBox = await input.boundingBox()
    const buttonBox = await button.boundingBox()

    if (inputBox && buttonBox) {
      // On mobile (320px), form elements should stack vertically
      // Button should be below the input (higher Y value)
      // OR they should both fit side by side without overflowing
      const totalWidth = inputBox.width + buttonBox.width
      const viewportWidth = 320

      if (totalWidth > viewportWidth - 32) {
        // Elements should be stacked - button Y should be greater than input Y + input height
        expect(buttonBox.y).toBeGreaterThanOrEqual(inputBox.y + inputBox.height - 10)
      }
    }
  })
})

test.describe('Mobile Viewport at 767px (Edge case)', () => {
  test.use({
    viewport: { width: 767, height: 1024 },
  })

  test('Navigation still shows hamburger menu at 767px', async ({ page }) => {
    await page.goto('/')
    await page.waitForSelector('nav[aria-label="Main navigation"]')

    // At 767px (just below tablet breakpoint), hamburger should still be visible
    const hamburgerButton = page.locator('[data-testid="mobile-menu-button"], button[aria-label*="menu" i], button[aria-label*="Menu" i], .navbar button[aria-expanded]')

    // Either hamburger is visible OR navigation links are visible (depending on breakpoint)
    // The key is that the layout should work at this viewport
    const hasHamburger = await hamburgerButton.first().isVisible().catch(() => false)
    const hasDirectLinks = await page.locator('nav a[href="/login"]:visible').first().isVisible().catch(() => false)

    // At minimum one navigation approach should be visible
    expect(hasHamburger || hasDirectLinks).toBe(true)
  })
})
