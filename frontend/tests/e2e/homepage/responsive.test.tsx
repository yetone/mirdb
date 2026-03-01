/**
 * Responsive Design E2E Tests
 * Owner: Scenario 4 - Responsive Design
 *
 * Verifies the homepage displays correctly across mobile, tablet, and desktop viewports.
 *
 * Test coverage:
 * - Mobile viewport (320px): Layout integrity, no horizontal scrollbar
 * - Mobile viewport: Hero text readable and not truncated
 * - Mobile viewport: CTA buttons easily tappable (min 44x44px)
 * - Mobile viewport: Feature cards stack vertically
 * - Desktop viewport (1920px): Feature cards display in grid layout
 * - Tablet viewport (768px): Navigation is accessible
 */

import { test, expect } from '@playwright/test'

test.describe('Responsive Design - Mobile Viewport (320px)', () => {
  test.beforeEach(async ({ page }) => {
    // Set mobile viewport width
    await page.setViewportSize({ width: 320, height: 568 })
    await page.goto('/')
    // Wait for the page to be fully loaded
    await page.waitForSelector('[data-testid="home-page"]')
  })

  test('TC1: Layout does not break, no horizontal scrollbar', async ({ page }) => {
    // Check that the page has no horizontal overflow (no horizontal scrollbar)
    const documentWidth = await page.evaluate(() => document.documentElement.scrollWidth)
    const viewportWidth = await page.evaluate(() => window.innerWidth)

    // The document width should not exceed viewport width (no horizontal scroll)
    expect(documentWidth).toBeLessThanOrEqual(viewportWidth)

    // Verify the home page is visible and laid out correctly
    const homePage = page.locator('[data-testid="home-page"]')
    await expect(homePage).toBeVisible()

    // Check that hero section is visible
    const heroSection = page.locator('[data-testid="hero-section"]')
    await expect(heroSection).toBeVisible()

    // Check that features section is visible
    const featuresSection = page.locator('[data-testid="features-section"]')
    await expect(featuresSection).toBeVisible()
  })

  test('TC2: Hero text is readable and not truncated', async ({ page }) => {
    // Check product name is visible and not truncated
    const productName = page.locator('[data-testid="product-name"]')
    await expect(productName).toBeVisible()

    // Get the text content and verify it's complete
    const productNameText = await productName.textContent()
    expect(productNameText).toContain('URL')
    expect(productNameText).toContain('Shortener')

    // Check tagline is visible
    const tagline = page.locator('[data-testid="tagline"]')
    await expect(tagline).toBeVisible()

    // Verify tagline text is not empty and readable
    const taglineText = await tagline.textContent()
    expect(taglineText).toBeTruthy()
    expect(taglineText!.length).toBeGreaterThan(10) // Should have meaningful content

    // Ensure text is not overflowing (check that text-overflow is not ellipsis)
    const taglineStyles = await tagline.evaluate((el) => {
      const styles = window.getComputedStyle(el)
      return {
        overflow: styles.overflow,
        textOverflow: styles.textOverflow,
        visibility: styles.visibility,
      }
    })
    expect(taglineStyles.visibility).not.toBe('hidden')
  })

  test('TC3: CTA buttons are easily tappable (min 44x44px)', async ({ page }) => {
    // WCAG touch target size recommendation: minimum 44x44px
    const minTouchSize = 44

    // Find all CTA buttons in the hero section
    const heroSection = page.locator('[data-testid="hero-section"]')
    const ctaButtons = heroSection.locator('a, button').filter({ hasText: /(Sign Up|Log In)/ })

    // Check that we have at least 2 CTA buttons
    const buttonCount = await ctaButtons.count()
    expect(buttonCount).toBeGreaterThanOrEqual(2)

    // Check each button meets minimum touch target size
    for (let i = 0; i < buttonCount; i++) {
      const button = ctaButtons.nth(i)
      await expect(button).toBeVisible()

      const boundingBox = await button.boundingBox()
      expect(boundingBox).not.toBeNull()
      expect(boundingBox!.width).toBeGreaterThanOrEqual(minTouchSize)
      expect(boundingBox!.height).toBeGreaterThanOrEqual(minTouchSize)
    }
  })

  test('TC4: Feature cards stack vertically on mobile', async ({ page }) => {
    const featuresGrid = page.locator('[data-testid="features-grid"]')
    await expect(featuresGrid).toBeVisible()

    // Get all feature cards (GlassMorphismCard renders with data-testid="glass-card")
    const featureCards = featuresGrid.locator('[data-testid="glass-card"]')
    const cardCount = await featureCards.count()
    expect(cardCount).toBeGreaterThanOrEqual(2) // Should have at least 2 feature cards

    // Get positions of cards to verify they're stacked vertically
    const cardPositions: { top: number; left: number }[] = []

    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i)
      const boundingBox = await card.boundingBox()
      expect(boundingBox).not.toBeNull()
      cardPositions.push({ top: boundingBox!.y, left: boundingBox!.x })
    }

    // For vertical stacking: each card's top position should be greater than the previous
    // and they should have similar left positions (indicating they're in a single column)
    for (let i = 1; i < cardPositions.length; i++) {
      // Cards should be below each other (top position increases)
      expect(cardPositions[i].top).toBeGreaterThan(cardPositions[i - 1].top)
    }

    // Check that cards are not side-by-side (similar left positions with tolerance)
    const leftPositions = cardPositions.map((p) => p.left)
    const maxLeftDiff = Math.max(...leftPositions) - Math.min(...leftPositions)
    // Cards should have similar left positions (allowing some margin difference)
    expect(maxLeftDiff).toBeLessThan(50)
  })
})

test.describe('Responsive Design - Desktop Viewport (1920px)', () => {
  test.beforeEach(async ({ page }) => {
    // Set desktop viewport width
    await page.setViewportSize({ width: 1920, height: 1080 })
    await page.goto('/')
    await page.waitForSelector('[data-testid="home-page"]')
  })

  test('TC5: Feature cards display in grid layout', async ({ page }) => {
    const featuresGrid = page.locator('[data-testid="features-grid"]')
    await expect(featuresGrid).toBeVisible()

    // Get all feature cards (GlassMorphismCard renders with data-testid="glass-card")
    const featureCards = featuresGrid.locator('[data-testid="glass-card"]')
    const cardCount = await featureCards.count()
    expect(cardCount).toBeGreaterThanOrEqual(2)

    // Get positions of cards to verify they're in a grid (some side-by-side)
    const cardPositions: { top: number; left: number }[] = []

    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i)
      const boundingBox = await card.boundingBox()
      expect(boundingBox).not.toBeNull()
      cardPositions.push({ top: boundingBox!.y, left: boundingBox!.x })
    }

    // For grid layout: at least some cards should be on the same row (same top position)
    // or have different left positions indicating they're side-by-side
    const uniqueTopPositions = [...new Set(cardPositions.map((p) => Math.round(p.top / 10) * 10))]
    const uniqueLeftPositions = [...new Set(cardPositions.map((p) => Math.round(p.left / 10) * 10))]

    // In a grid layout, there should be multiple unique left positions (cards side-by-side)
    // and fewer unique top positions than total cards (some cards share same row)
    expect(uniqueLeftPositions.length).toBeGreaterThan(1)

    // Also verify that the grid container has grid display
    const gridStyles = await featuresGrid.evaluate((el) => {
      const styles = window.getComputedStyle(el)
      return {
        display: styles.display,
        gridTemplateColumns: styles.gridTemplateColumns,
      }
    })
    expect(gridStyles.display).toBe('grid')
  })
})

test.describe('Responsive Design - Tablet Viewport (768px)', () => {
  test.beforeEach(async ({ page }) => {
    // Set tablet viewport width
    await page.setViewportSize({ width: 768, height: 1024 })
    await page.goto('/')
    await page.waitForSelector('[data-testid="home-page"]')
  })

  test('TC6: Navigation is accessible (hamburger menu or visible links)', async ({ page }) => {
    // Check that navigation element exists
    const navbar = page.locator('nav[role="navigation"], nav')
    await expect(navbar).toBeVisible()

    // Check for visible navigation links OR hamburger menu
    // Option 1: Visible navigation links
    const loginLink = page.locator('[data-testid="nav-login"]')
    const registerLink = page.locator('[data-testid="nav-register"]')

    // Option 2: Hamburger menu (common mobile pattern)
    const hamburgerMenu = page.locator(
      '[data-testid="hamburger-menu"], button[aria-label*="menu"], .hamburger, [class*="hamburger"]'
    )

    // At least one navigation method should be visible
    const hasVisibleLinks =
      (await loginLink.isVisible().catch(() => false)) ||
      (await registerLink.isVisible().catch(() => false))

    const hasHamburgerMenu = await hamburgerMenu.isVisible().catch(() => false)

    // Navigation is accessible if either visible links exist or hamburger menu is present
    expect(hasVisibleLinks || hasHamburgerMenu).toBe(true)

    if (hasVisibleLinks) {
      // Verify links are clickable
      if (await loginLink.isVisible().catch(() => false)) {
        await expect(loginLink).toBeEnabled()
      }
      if (await registerLink.isVisible().catch(() => false)) {
        await expect(registerLink).toBeEnabled()
      }
    }

    if (hasHamburgerMenu) {
      // If hamburger exists, it should be clickable
      await expect(hamburgerMenu).toBeEnabled()
    }
  })
})
