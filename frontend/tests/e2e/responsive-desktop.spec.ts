import { test, expect } from '@playwright/test'

/**
 * Responsive Design - Desktop E2E Tests
 * Tests for verifying homepage displays correctly on desktop devices (>= 1024px width)
 * as specified in REQ-9
 */
test.describe('Responsive Design - Desktop', () => {
  // Configure desktop viewport for all tests in this suite
  test.use({
    viewport: { width: 1280, height: 800 },
  })

  // Test Case 1: Render homepage at 1280px viewport width
  test('homepage displays with desktop layout, full navigation visible, feature cards in row', async ({
    page,
  }) => {
    // Navigate to homepage
    await page.goto('/')
    await page.waitForLoadState('networkidle')

    // Verify homepage is rendered
    const homePage = page.getByTestId('home-page')
    await expect(homePage).toBeVisible()

    // Verify navigation header is visible
    const navHeader = page.getByTestId('navigation-header')
    await expect(navHeader).toBeVisible()

    // Verify all navigation items are visible (no hamburger menu on desktop)
    const navLogo = page.getByTestId('nav-logo')
    await expect(navLogo).toBeVisible()

    const navFeatures = page.getByTestId('nav-features')
    await expect(navFeatures).toBeVisible()

    const navHowItWorks = page.getByTestId('nav-how-it-works')
    await expect(navHowItWorks).toBeVisible()

    const navLogin = page.getByTestId('nav-login')
    await expect(navLogin).toBeVisible()

    const navSignup = page.getByTestId('nav-signup')
    await expect(navSignup).toBeVisible()

    // Verify theme toggle is visible (use first() as there may be desktop and mobile toggles)
    const themeToggle = page.getByTestId('theme-toggle').first()
    await expect(themeToggle).toBeVisible()

    // Scroll to features section and verify grid layout
    const featuresGrid = page.getByTestId('features-grid')
    await featuresGrid.scrollIntoViewIfNeeded()
    await expect(featuresGrid).toBeVisible()

    // Verify feature cards are displayed (should have 4 cards for 4-column desktop layout)
    const featureCards = page.locator('[data-testid^="feature-card-"]')
    const cardCount = await featureCards.count()
    expect(cardCount).toBe(4)

    // Verify the grid has the correct CSS classes for desktop layout
    await expect(featuresGrid).toHaveClass(/grid/)
    await expect(featuresGrid).toHaveClass(/lg:grid-cols-4/)
  })

  // Test Case 2: Check navigation at desktop viewport
  test('full navigation links visible in header (no hamburger menu)', async ({ page }) => {
    await page.goto('/')
    await page.waitForLoadState('networkidle')

    // Verify navigation header is present
    const navHeader = page.getByTestId('navigation-header')
    await expect(navHeader).toBeVisible()

    // Verify all navigation elements are visible and properly displayed
    const navigationElements = [
      { testId: 'nav-logo', expectedText: 'URL Shortener' },
      { testId: 'nav-features', expectedText: 'Features' },
      { testId: 'nav-how-it-works', expectedText: 'How It Works' },
      { testId: 'nav-login', expectedText: 'Log In' },
      { testId: 'nav-signup', expectedText: 'Sign Up' },
    ]

    for (const navElement of navigationElements) {
      const element = page.getByTestId(navElement.testId)
      await expect(element).toBeVisible()
      await expect(element).toHaveText(navElement.expectedText)
    }

    // Verify theme toggle button is visible (use first() as there may be desktop and mobile toggles)
    const themeToggle = page.getByTestId('theme-toggle').first()
    await expect(themeToggle).toBeVisible()

    // Verify no hamburger menu is present on desktop
    // Hamburger menus typically use btn-square btn-ghost with menu icon or have specific identifiers
    const hamburgerMenu = page.locator('[data-testid="hamburger-menu"]')
    await expect(hamburgerMenu).not.toBeVisible()

    // Verify navigation items are displayed inline (not hidden in dropdown)
    // Check that navigation buttons are in a horizontal layout
    const navFeaturesBox = await page.getByTestId('nav-features').boundingBox()
    const navHowItWorksBox = await page.getByTestId('nav-how-it-works').boundingBox()
    const navLoginBox = await page.getByTestId('nav-login').boundingBox()

    // All navigation items should be on the same horizontal line (similar Y position)
    if (navFeaturesBox && navHowItWorksBox && navLoginBox) {
      // Items should be horizontally aligned (within 20px Y tolerance)
      expect(Math.abs(navFeaturesBox.y - navHowItWorksBox.y)).toBeLessThan(20)
      expect(Math.abs(navHowItWorksBox.y - navLoginBox.y)).toBeLessThan(20)

      // Items should be horizontally spaced (increasing X positions)
      expect(navHowItWorksBox.x).toBeGreaterThan(navFeaturesBox.x)
      expect(navLoginBox.x).toBeGreaterThan(navHowItWorksBox.x)
    }
  })

  // Test Case 3: Check feature cards layout at desktop viewport
  test('feature cards display in a horizontal row (3-4 columns)', async ({ page }) => {
    await page.goto('/')
    await page.waitForLoadState('networkidle')

    // Scroll to features section
    const featuresSection = page.locator('#features')
    await featuresSection.scrollIntoViewIfNeeded()
    await expect(featuresSection).toBeVisible()

    // Verify features grid has correct structure
    const featuresGrid = page.getByTestId('features-grid')
    await expect(featuresGrid).toBeVisible()

    // Verify the grid class for 4-column layout on large screens
    await expect(featuresGrid).toHaveClass(/lg:grid-cols-4/)

    // Get all feature cards
    const featureCards = page.locator('[data-testid^="feature-card-"]')
    const cardCount = await featureCards.count()
    expect(cardCount).toBe(4)

    // Verify the expected feature cards exist
    const expectedFeatureIds = ['url-shortening', 'analytics', 'link-management', 'security']
    for (const featureId of expectedFeatureIds) {
      const card = page.getByTestId(`feature-card-${featureId}`)
      await expect(card).toBeVisible()
    }

    // Verify cards are displayed horizontally (in a row) by checking their positions
    const cardPositions: { x: number; y: number }[] = []
    for (const featureId of expectedFeatureIds) {
      const card = page.getByTestId(`feature-card-${featureId}`)
      const box = await card.boundingBox()
      if (box) {
        cardPositions.push({ x: box.x, y: box.y })
      }
    }

    // All cards should be at approximately the same Y position (horizontal row)
    if (cardPositions.length === 4) {
      const firstCardY = cardPositions[0].y
      for (const pos of cardPositions) {
        // Allow 10px tolerance for alignment variations
        expect(Math.abs(pos.y - firstCardY)).toBeLessThan(10)
      }

      // Cards should have increasing X positions (left to right order)
      expect(cardPositions[1].x).toBeGreaterThan(cardPositions[0].x)
      expect(cardPositions[2].x).toBeGreaterThan(cardPositions[1].x)
      expect(cardPositions[3].x).toBeGreaterThan(cardPositions[2].x)
    }
  })

  // Test Case 4: Check hero section at desktop viewport
  test('hero section displays with optimal spacing and sizing for desktop', async ({ page }) => {
    await page.goto('/')
    await page.waitForLoadState('networkidle')

    // Verify hero section is visible
    const heroSection = page.getByTestId('hero-section')
    await expect(heroSection).toBeVisible()

    // Verify hero headline is visible and properly sized
    const heroHeadline = page.getByTestId('hero-headline')
    await expect(heroHeadline).toBeVisible()
    await expect(heroHeadline).toContainText('Shorten. Share. Track.')

    // Verify the headline has desktop-specific styling (md:text-6xl class)
    await expect(heroHeadline).toHaveClass(/md:text-6xl/)

    // Verify subheadline is visible
    const heroSubheadline = page.getByTestId('hero-subheadline')
    await expect(heroSubheadline).toBeVisible()
    await expect(heroSubheadline).toHaveClass(/md:text-xl/)

    // Verify CTA buttons are visible
    const primaryCta = page.getByTestId('hero-cta-primary')
    await expect(primaryCta).toBeVisible()
    await expect(primaryCta).toHaveText('Get Started Free')

    const secondaryCta = page.getByTestId('hero-cta-secondary')
    await expect(secondaryCta).toBeVisible()
    await expect(secondaryCta).toHaveText('Log In')

    // Verify hero section takes significant vertical space
    const heroBox = await heroSection.boundingBox()
    if (heroBox) {
      // Hero should take at least 60% of viewport height (min-h-[80vh] in component)
      const viewportHeight = 800
      expect(heroBox.height).toBeGreaterThan(viewportHeight * 0.6)
    }

    // Verify CTA buttons are displayed horizontally on desktop (sm:flex-row)
    const primaryCtaBox = await primaryCta.boundingBox()
    const secondaryCtaBox = await secondaryCta.boundingBox()

    if (primaryCtaBox && secondaryCtaBox) {
      // On desktop (1280px > sm breakpoint), buttons should be side by side
      // They should have similar Y positions (horizontal layout)
      expect(Math.abs(primaryCtaBox.y - secondaryCtaBox.y)).toBeLessThan(20)

      // Secondary CTA should be to the right of primary CTA
      expect(secondaryCtaBox.x).toBeGreaterThan(primaryCtaBox.x)
    }
  })

  // Additional test: Verify desktop-specific breakpoint behavior
  test('desktop layout is applied at 1024px and above', async ({ page }) => {
    // Test at exactly 1024px (minimum desktop width per scenario)
    await page.setViewportSize({ width: 1024, height: 768 })
    await page.goto('/')
    await page.waitForLoadState('networkidle')

    // Navigation should still be fully visible at 1024px
    const navFeatures = page.getByTestId('nav-features')
    await expect(navFeatures).toBeVisible()

    const navHowItWorks = page.getByTestId('nav-how-it-works')
    await expect(navHowItWorks).toBeVisible()

    const navLogin = page.getByTestId('nav-login')
    await expect(navLogin).toBeVisible()

    // Feature cards should still be in grid layout
    const featuresGrid = page.getByTestId('features-grid')
    await featuresGrid.scrollIntoViewIfNeeded()
    await expect(featuresGrid).toHaveClass(/lg:grid-cols-4/)
  })

  // Additional test: Verify larger desktop viewport (1920px)
  test('homepage scales properly on large desktop screens', async ({ page }) => {
    await page.setViewportSize({ width: 1920, height: 1080 })
    await page.goto('/')
    await page.waitForLoadState('networkidle')

    // Verify homepage is rendered correctly
    const homePage = page.getByTestId('home-page')
    await expect(homePage).toBeVisible()

    // Verify all main sections are present
    const heroSection = page.getByTestId('hero-section')
    await expect(heroSection).toBeVisible()

    const featuresSection = page.locator('#features')
    await featuresSection.scrollIntoViewIfNeeded()
    await expect(featuresSection).toBeVisible()

    // Content should be centered with max-width constraint
    const featuresGrid = page.getByTestId('features-grid')
    const gridBox = await featuresGrid.boundingBox()
    if (gridBox) {
      // Grid should be centered (not stretching to full viewport width)
      // max-w-7xl = 1280px, so grid width should be <= 1280px with some padding
      expect(gridBox.width).toBeLessThan(1300)
    }
  })
})
