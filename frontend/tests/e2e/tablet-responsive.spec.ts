import { test, expect } from '@playwright/test'

// Tablet viewport size (768px - 1023px range)
const TABLET_VIEWPORT = { width: 768, height: 1024 }

test.describe('Responsive Design - Tablet (768px - 1023px)', () => {
  test.beforeEach(async ({ page }) => {
    // Set viewport to tablet size
    await page.setViewportSize(TABLET_VIEWPORT)
  })

  // Test Case 1: Render homepage at 768px viewport width
  test('homepage displays with tablet-appropriate layout at 768px', async ({ page }) => {
    await page.goto('/')
    await page.waitForLoadState('networkidle')

    // Verify homepage is rendered
    const homePage = page.getByTestId('home-page')
    await expect(homePage).toBeVisible()

    // Verify hero section is visible and properly laid out
    const heroSection = page.getByTestId('hero-section')
    await expect(heroSection).toBeVisible()

    // Check that hero headline is visible and readable
    const headline = page.getByTestId('hero-headline')
    await expect(headline).toBeVisible()

    // Check that hero subheadline is visible
    const subheadline = page.getByTestId('hero-subheadline')
    await expect(subheadline).toBeVisible()

    // Check that CTA buttons are visible
    const primaryCta = page.getByTestId('hero-cta-primary')
    await expect(primaryCta).toBeVisible()

    const secondaryCta = page.getByTestId('hero-cta-secondary')
    await expect(secondaryCta).toBeVisible()

    // Verify page width matches viewport
    const pageBox = await homePage.boundingBox()
    expect(pageBox).toBeTruthy()
    expect(pageBox!.width).toBeLessThanOrEqual(TABLET_VIEWPORT.width)
  })

  // Test Case 2: Check navigation at tablet viewport
  test('navigation adapts to tablet size', async ({ page }) => {
    await page.goto('/')
    await page.waitForLoadState('networkidle')

    // Check navigation header is visible
    const navHeader = page.getByTestId('navigation-header')
    await expect(navHeader).toBeVisible()

    // Check that logo is visible
    const logo = page.getByTestId('nav-logo')
    await expect(logo).toBeVisible()

    // Navigation should show either:
    // - Full navigation with condensed buttons
    // - Hamburger menu for mobile-like tablet experience
    // Check for either full nav links or hamburger menu
    const featuresNav = page.getByTestId('nav-features')
    const howItWorksNav = page.getByTestId('nav-how-it-works')
    const loginNav = page.getByTestId('nav-login')
    const signupNav = page.getByTestId('nav-signup')

    // At 768px (tablet), navigation items should still be visible
    // Check if they exist and are visible, or if a hamburger menu exists
    const hamburgerMenu = page.locator('[data-testid="nav-hamburger"], [aria-label="Toggle menu"], .drawer-toggle, .menu-toggle')
    const hamburgerExists = await hamburgerMenu.count() > 0

    if (hamburgerExists) {
      // If hamburger menu exists, it should be visible
      await expect(hamburgerMenu.first()).toBeVisible()
    } else {
      // Otherwise, nav items should be visible (condensed navigation)
      // At tablet size, login and signup should always be visible
      await expect(loginNav).toBeVisible()
      await expect(signupNav).toBeVisible()
    }

    // Theme toggle should be accessible
    const themeToggle = page.locator('[data-testid="theme-toggle"], [aria-label*="theme"]')
    if (await themeToggle.count() > 0) {
      await expect(themeToggle.first()).toBeVisible()
    }
  })

  // Test Case 3: Check feature cards layout at tablet viewport
  test('feature cards display in 2-column grid at tablet viewport', async ({ page }) => {
    await page.goto('/')
    await page.waitForLoadState('networkidle')

    // Scroll to features section
    const featuresSection = page.locator('#features')
    await featuresSection.scrollIntoViewIfNeeded()
    await expect(featuresSection).toBeVisible()

    // Get the features grid
    const featuresGrid = page.getByTestId('features-grid')
    await expect(featuresGrid).toBeVisible()

    // Get all feature cards
    const featureCards = page.locator('[data-testid^="feature-card-"]')
    const cardCount = await featureCards.count()
    expect(cardCount).toBeGreaterThanOrEqual(3)

    // Verify cards are visible
    for (let i = 0; i < cardCount; i++) {
      await expect(featureCards.nth(i)).toBeVisible()
    }

    // At tablet viewport (768px), with md:grid-cols-2, we expect 2-column layout
    // Get positions of first two cards to verify they're side by side
    if (cardCount >= 2) {
      const firstCard = featureCards.first()
      const secondCard = featureCards.nth(1)

      const firstBox = await firstCard.boundingBox()
      const secondBox = await secondCard.boundingBox()

      expect(firstBox).toBeTruthy()
      expect(secondBox).toBeTruthy()

      // In a 2-column grid, the first two cards should have similar Y positions
      // (they should be on the same row)
      const yDifference = Math.abs(firstBox!.y - secondBox!.y)

      // Allow some tolerance for minor positioning differences
      // If cards are in same row, Y difference should be small
      // If cards are stacked, Y difference would be >= card height
      expect(yDifference).toBeLessThan(firstBox!.height)
    }
  })

  // Test Case 4: Check button tap targets at tablet viewport
  test('all buttons and interactive elements are at least 44x44px', async ({ page }) => {
    await page.goto('/')
    await page.waitForLoadState('networkidle')

    const MINIMUM_TAP_TARGET = 44

    // Check hero CTA buttons
    const primaryCta = page.getByTestId('hero-cta-primary')
    const primaryBox = await primaryCta.boundingBox()
    expect(primaryBox).toBeTruthy()
    expect(primaryBox!.width).toBeGreaterThanOrEqual(MINIMUM_TAP_TARGET)
    expect(primaryBox!.height).toBeGreaterThanOrEqual(MINIMUM_TAP_TARGET)

    const secondaryCta = page.getByTestId('hero-cta-secondary')
    const secondaryBox = await secondaryCta.boundingBox()
    expect(secondaryBox).toBeTruthy()
    expect(secondaryBox!.width).toBeGreaterThanOrEqual(MINIMUM_TAP_TARGET)
    expect(secondaryBox!.height).toBeGreaterThanOrEqual(MINIMUM_TAP_TARGET)

    // Check navigation buttons
    const loginNav = page.getByTestId('nav-login')
    const loginBox = await loginNav.boundingBox()
    expect(loginBox).toBeTruthy()
    expect(loginBox!.width).toBeGreaterThanOrEqual(MINIMUM_TAP_TARGET)
    expect(loginBox!.height).toBeGreaterThanOrEqual(MINIMUM_TAP_TARGET)

    const signupNav = page.getByTestId('nav-signup')
    const signupBox = await signupNav.boundingBox()
    expect(signupBox).toBeTruthy()
    expect(signupBox!.width).toBeGreaterThanOrEqual(MINIMUM_TAP_TARGET)
    expect(signupBox!.height).toBeGreaterThanOrEqual(MINIMUM_TAP_TARGET)

    // Check navigation link buttons
    const featuresNav = page.getByTestId('nav-features')
    if (await featuresNav.isVisible()) {
      const featuresBox = await featuresNav.boundingBox()
      expect(featuresBox).toBeTruthy()
      expect(featuresBox!.width).toBeGreaterThanOrEqual(MINIMUM_TAP_TARGET)
      expect(featuresBox!.height).toBeGreaterThanOrEqual(MINIMUM_TAP_TARGET)
    }

    const howItWorksNav = page.getByTestId('nav-how-it-works')
    if (await howItWorksNav.isVisible()) {
      const howItWorksBox = await howItWorksNav.boundingBox()
      expect(howItWorksBox).toBeTruthy()
      expect(howItWorksBox!.width).toBeGreaterThanOrEqual(MINIMUM_TAP_TARGET)
      expect(howItWorksBox!.height).toBeGreaterThanOrEqual(MINIMUM_TAP_TARGET)
    }
  })

  // Additional test: Verify content readability at tablet viewport
  test('all text content is readable at tablet viewport', async ({ page }) => {
    await page.goto('/')
    await page.waitForLoadState('networkidle')

    // Check hero headline font size - should be readable
    const headline = page.getByTestId('hero-headline')
    await expect(headline).toBeVisible()

    // Verify headline has text content
    const headlineText = await headline.textContent()
    expect(headlineText).toBeTruthy()
    expect(headlineText!.length).toBeGreaterThan(0)

    // Check subheadline
    const subheadline = page.getByTestId('hero-subheadline')
    await expect(subheadline).toBeVisible()
    const subheadlineText = await subheadline.textContent()
    expect(subheadlineText).toBeTruthy()
    expect(subheadlineText!.length).toBeGreaterThan(0)

    // Scroll to features and verify text is readable
    const featuresSection = page.locator('#features')
    await featuresSection.scrollIntoViewIfNeeded()

    const featuresHeading = page.locator('#features-heading')
    await expect(featuresHeading).toBeVisible()
    await expect(featuresHeading).toHaveText('Powerful Features')
  })

  // Test at upper tablet range (1023px)
  test('homepage displays correctly at upper tablet range (1023px)', async ({ page }) => {
    await page.setViewportSize({ width: 1023, height: 768 })
    await page.goto('/')
    await page.waitForLoadState('networkidle')

    // Verify homepage renders
    const homePage = page.getByTestId('home-page')
    await expect(homePage).toBeVisible()

    // Verify hero section
    const heroSection = page.getByTestId('hero-section')
    await expect(heroSection).toBeVisible()

    // Verify features section
    const featuresSection = page.locator('#features')
    await featuresSection.scrollIntoViewIfNeeded()
    await expect(featuresSection).toBeVisible()

    // Feature cards should be in 2-column layout (md:grid-cols-2)
    const featuresGrid = page.getByTestId('features-grid')
    await expect(featuresGrid).toBeVisible()
  })

  // Test horizontal scrolling doesn't occur
  test('no horizontal scrolling occurs at tablet viewport', async ({ page }) => {
    await page.goto('/')
    await page.waitForLoadState('networkidle')

    // Check that page doesn't cause horizontal scroll
    const scrollWidth = await page.evaluate(() => document.documentElement.scrollWidth)
    const clientWidth = await page.evaluate(() => document.documentElement.clientWidth)

    // There should be no horizontal overflow
    expect(scrollWidth).toBeLessThanOrEqual(clientWidth + 1) // Allow 1px tolerance
  })
})
