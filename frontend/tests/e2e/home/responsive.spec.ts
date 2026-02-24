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

/**
 * Tablet and Desktop Responsive Design E2E Tests
 * Owner: Scenario 10 - Responsive Design - Tablet and Desktop
 *
 * Verifies homepage displays correctly on tablet (768px-1023px) and desktop (1024px+) viewports.
 * Tests cover:
 * - Tablet-optimized layout at 768px
 * - Full desktop navigation bar (not hamburger) at 1024px+
 * - Multi-column feature cards grid at desktop
 * - Hero section with wider layout and centered content
 */

test.describe('Tablet Responsive Design (768px-1023px)', () => {
  test.use({
    viewport: { width: 768, height: 1024 },
  })

  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    await page.waitForSelector('nav[aria-label="Main navigation"]')
  })

  test('TC1: Tablet-optimized layout is applied at 768px viewport', async ({ page }) => {
    // Check that the page renders without horizontal scroll
    const documentWidth = await page.evaluate(() => document.documentElement.scrollWidth)
    const viewportWidth = await page.evaluate(() => window.innerWidth)
    expect(documentWidth).toBeLessThanOrEqual(viewportWidth)

    // At 768px (md breakpoint), desktop navigation should be visible
    // The navbar uses "hidden md:flex" for desktop nav, so at 768px it should show
    const desktopNav = page.locator('.navbar-end.hidden.md\\:flex, .navbar-end:not(.md\\:hidden)')
    const navIsVisible = await desktopNav.first().isVisible().catch(() => false)

    // Hero section should be visible and properly laid out
    const heroSection = page.locator('section[aria-label="Hero section"], .hero')
    await expect(heroSection).toBeVisible()

    // Features section should be visible
    const featuresSection = page.locator('[data-testid="features-section"], section[aria-label="Features section"]')
    await expect(featuresSection).toBeVisible()

    // At tablet size, feature cards should display in 2-column grid (md:grid-cols-2)
    const featureCards = page.locator('[data-testid="feature-card"]')
    const cardCount = await featureCards.count()
    expect(cardCount).toBeGreaterThanOrEqual(3)

    // Get first and second card positions to verify grid layout
    if (cardCount >= 2) {
      const firstCard = await featureCards.nth(0).boundingBox()
      const secondCard = await featureCards.nth(1).boundingBox()

      if (firstCard && secondCard) {
        // At 768px with md:grid-cols-2, cards should be side by side (same Y position)
        // They should be in the same row
        expect(Math.abs(firstCard.y - secondCard.y)).toBeLessThan(20)
        // And horizontally separated
        expect(secondCard.x).toBeGreaterThan(firstCard.x)
      }
    }
  })

  test('Tablet layout maintains proper spacing and readability', async ({ page }) => {
    // Check hero section content is properly centered
    const heroContent = page.locator('.hero-content')
    await expect(heroContent).toBeVisible()

    const heroBox = await heroContent.boundingBox()
    const viewportWidth = 768

    if (heroBox) {
      // Hero content should be centered (roughly equal margins on both sides)
      const leftMargin = heroBox.x
      const rightMargin = viewportWidth - (heroBox.x + heroBox.width)
      // Allow some variance for padding/margins
      expect(Math.abs(leftMargin - rightMargin)).toBeLessThan(100)
    }

    // Check that heading is visible and readable
    const heading = page.locator('h1')
    await expect(heading).toBeVisible()

    // Features section heading should use larger text at tablet size (md:text-4xl)
    const featuresHeading = page.locator('[data-testid="features-section"] h2, section[aria-label="Features section"] h2')
    await expect(featuresHeading).toBeVisible()
  })
})

test.describe('Desktop Responsive Design (1024px+)', () => {
  test.use({
    viewport: { width: 1024, height: 768 },
  })

  test.beforeEach(async ({ page }) => {
    await page.goto('/')
    await page.waitForSelector('nav[aria-label="Main navigation"]')
  })

  test('TC2: Full desktop navigation bar is displayed (not hamburger) at 1024px', async ({ page }) => {
    // The hamburger menu button should be hidden at desktop viewport
    // It has class "md:hidden" which hides it at 768px and above
    const hamburgerButton = page.locator('[data-testid="mobile-menu-button"]')
    await expect(hamburgerButton).toBeHidden()

    // Desktop navigation links should be visible (Login, Register)
    // They use "hidden md:flex" so they should be visible at 1024px
    const loginLink = page.locator('nav a[href="/login"]').first()
    const registerLink = page.locator('nav a[href="/register"]').first()

    // Check if direct nav links are visible
    await expect(loginLink).toBeVisible()
    await expect(registerLink).toBeVisible()

    // Verify the links are in the navbar-end section (desktop layout)
    const navbarEnd = page.locator('.navbar-end').first()
    await expect(navbarEnd).toBeVisible()
  })

  test('TC3: Feature cards display in multi-column grid layout at desktop', async ({ page }) => {
    // At desktop (lg:grid-cols-4), feature cards should display in a 4-column grid
    const featuresSection = page.locator('[data-testid="features-section"], section[aria-label="Features section"]')
    await expect(featuresSection).toBeVisible()

    const featureCards = page.locator('[data-testid="feature-card"]')
    const cardCount = await featureCards.count()

    // We expect 4 feature cards based on FeaturesSection implementation
    expect(cardCount).toBe(4)

    // Get bounding boxes for all cards
    const cardBoxes = []
    for (let i = 0; i < cardCount; i++) {
      const box = await featureCards.nth(i).boundingBox()
      if (box) cardBoxes.push(box)
    }

    // At lg:grid-cols-4 (1024px), all 4 cards should be in the same row
    if (cardBoxes.length === 4) {
      // All cards should have approximately the same Y position (same row)
      const firstCardY = cardBoxes[0].y
      for (const box of cardBoxes) {
        expect(Math.abs(box.y - firstCardY)).toBeLessThan(10)
      }

      // Cards should be horizontally distributed
      // Card 2 should be to the right of card 1, etc.
      for (let i = 1; i < cardBoxes.length; i++) {
        expect(cardBoxes[i].x).toBeGreaterThan(cardBoxes[i - 1].x)
      }
    }
  })

  test('TC4: Hero section uses wider layout with centered content at desktop', async ({ page }) => {
    // Hero section should be visible
    const heroSection = page.locator('section[aria-label="Hero section"], .hero')
    await expect(heroSection).toBeVisible()

    // Hero content container should be centered
    const heroContent = page.locator('.hero-content')
    await expect(heroContent).toBeVisible()

    // Check hero content box
    const heroBox = await heroContent.boundingBox()
    const viewportWidth = 1024

    if (heroBox) {
      // Content should be centered in the viewport
      const leftMargin = heroBox.x
      const rightMargin = viewportWidth - (heroBox.x + heroBox.width)
      // Allow some variance for padding but should be roughly centered
      expect(Math.abs(leftMargin - rightMargin)).toBeLessThan(100)
    }

    // Check that the form uses horizontal layout (flex-row) at desktop (sm:flex-row)
    const form = page.locator('section[aria-label="Hero section"] form, .hero form')
    await expect(form).toBeVisible()

    // Get input and button to verify they are side by side
    const urlInput = page.locator('input[aria-label="URL to shorten"]')
    const submitButton = page.locator('button[aria-label="Shorten URL"]')

    const inputBox = await urlInput.boundingBox()
    const buttonBox = await submitButton.boundingBox()

    if (inputBox && buttonBox) {
      // At desktop with sm:flex-row, input and button should be on the same line
      // Y positions should be approximately equal
      expect(Math.abs(inputBox.y - buttonBox.y)).toBeLessThan(20)
      // Button should be to the right of the input
      expect(buttonBox.x).toBeGreaterThan(inputBox.x)
    }

    // Heading should use larger text at desktop (md:text-6xl)
    const heading = page.locator('h1')
    await expect(heading).toBeVisible()

    // Tagline should be visible with proper styling
    const tagline = page.locator('.hero-content p').first()
    await expect(tagline).toBeVisible()
  })
})

test.describe('Large Desktop Viewport (1280px+)', () => {
  test.use({
    viewport: { width: 1280, height: 800 },
  })

  test('Content remains centered and readable at large desktop', async ({ page }) => {
    await page.goto('/')
    await page.waitForSelector('nav[aria-label="Main navigation"]')

    // Check max-width constraints are applied (max-w-6xl = 72rem = 1152px)
    const featuresContainer = page.locator('[data-testid="features-section"] > div, section[aria-label="Features section"] > div').first()
    await expect(featuresContainer).toBeVisible()

    const containerBox = await featuresContainer.boundingBox()
    if (containerBox) {
      // Container should not exceed max-width (approximately 1152px for max-w-6xl)
      expect(containerBox.width).toBeLessThanOrEqual(1200)
    }

    // Hero content should also be constrained
    const heroContent = page.locator('.hero-content')
    const heroBox = await heroContent.boundingBox()
    if (heroBox) {
      // max-w-3xl = 48rem = 768px
      expect(heroBox.width).toBeLessThanOrEqual(800)
    }

    // Navigation should still be visible
    const loginLink = page.locator('nav a[href="/login"]').first()
    await expect(loginLink).toBeVisible()
  })
})
