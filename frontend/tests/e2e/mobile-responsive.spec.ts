import { test, expect } from '@playwright/test'

// Mobile viewport size (< 768px as specified in REQ-9 and US-7)
const MOBILE_VIEWPORT = { width: 375, height: 667 }
const MINIMUM_TAP_TARGET = 44

test.describe('Responsive Design - Mobile (< 768px)', () => {
  test.beforeEach(async ({ page }) => {
    // Set viewport to mobile size (iPhone SE/7/8 size)
    await page.setViewportSize(MOBILE_VIEWPORT)
  })

  // Test Case 1: Render homepage at 375px viewport width
  test('homepage displays with mobile layout, hamburger menu visible', async ({ page }) => {
    await page.goto('/')
    await page.waitForLoadState('networkidle')

    // Verify homepage is rendered
    const homePage = page.getByTestId('home-page')
    await expect(homePage).toBeVisible()

    // Verify navigation header is visible
    const navHeader = page.getByTestId('navigation-header')
    await expect(navHeader).toBeVisible()

    // Verify hamburger menu is visible on mobile
    const hamburgerMenu = page.getByTestId('hamburger-menu')
    await expect(hamburgerMenu).toBeVisible()

    // Verify logo is visible
    const navLogo = page.getByTestId('nav-logo')
    await expect(navLogo).toBeVisible()

    // Hero section should be visible
    const heroSection = page.getByTestId('hero-section')
    await expect(heroSection).toBeVisible()
  })

  // Test Case 2: Check navigation at mobile viewport
  test('hamburger menu icon is visible instead of full navigation', async ({ page }) => {
    await page.goto('/')
    await page.waitForLoadState('networkidle')

    // Verify hamburger menu is visible
    const hamburgerMenu = page.getByTestId('hamburger-menu')
    await expect(hamburgerMenu).toBeVisible()

    // Verify desktop navigation items are hidden on mobile
    const desktopNav = page.getByTestId('desktop-nav')
    await expect(desktopNav).toBeHidden()

    // Navigation items should not be visible directly
    const navFeatures = page.getByTestId('nav-features')
    await expect(navFeatures).toBeHidden()

    const navHowItWorks = page.getByTestId('nav-how-it-works')
    await expect(navHowItWorks).toBeHidden()

    // Logo should still be visible
    const navLogo = page.getByTestId('nav-logo')
    await expect(navLogo).toBeVisible()
  })

  // Test Case 3: Click hamburger menu icon on mobile
  test('mobile navigation menu opens with all navigation options', async ({ page }) => {
    await page.goto('/')
    await page.waitForLoadState('networkidle')

    // Click hamburger menu
    const hamburgerMenu = page.getByTestId('hamburger-menu')
    await expect(hamburgerMenu).toBeVisible()
    await hamburgerMenu.click()

    // Wait for mobile menu to open
    const mobileMenu = page.getByTestId('mobile-menu')
    await expect(mobileMenu).toBeVisible()

    // Verify all navigation options are visible in mobile menu
    const mobileNavFeatures = page.getByTestId('mobile-nav-features')
    await expect(mobileNavFeatures).toBeVisible()

    const mobileNavHowItWorks = page.getByTestId('mobile-nav-how-it-works')
    await expect(mobileNavHowItWorks).toBeVisible()

    const mobileNavLogin = page.getByTestId('mobile-nav-login')
    await expect(mobileNavLogin).toBeVisible()

    const mobileNavSignup = page.getByTestId('mobile-nav-signup')
    await expect(mobileNavSignup).toBeVisible()
  })

  // Test Case 4: Check feature cards layout at mobile viewport
  test('feature cards stack vertically (single column)', async ({ page }) => {
    await page.goto('/')
    await page.waitForLoadState('networkidle')

    // Scroll to features section
    const featuresSection = page.locator('#features')
    await featuresSection.scrollIntoViewIfNeeded()
    await expect(featuresSection).toBeVisible()

    // Get feature cards
    const featureCards = page.locator('[data-testid^="feature-card-"]')
    const cardCount = await featureCards.count()
    expect(cardCount).toBe(4)

    // Get positions of all cards
    const cardPositions: { x: number; y: number; width: number }[] = []
    for (let i = 0; i < cardCount; i++) {
      const card = featureCards.nth(i)
      const box = await card.boundingBox()
      if (box) {
        cardPositions.push({ x: box.x, y: box.y, width: box.width })
      }
    }

    // Cards should be stacked vertically (increasing Y positions)
    // All cards should have similar X position (left aligned)
    if (cardPositions.length === 4) {
      // Cards should have similar X positions (single column)
      const firstCardX = cardPositions[0].x
      for (const pos of cardPositions) {
        expect(Math.abs(pos.x - firstCardX)).toBeLessThan(20)
      }

      // Cards should have increasing Y positions (stacked vertically)
      expect(cardPositions[1].y).toBeGreaterThan(cardPositions[0].y)
      expect(cardPositions[2].y).toBeGreaterThan(cardPositions[1].y)
      expect(cardPositions[3].y).toBeGreaterThan(cardPositions[2].y)
    }
  })

  // Test Case 5: Check button sizes at mobile viewport
  test('all interactive elements have minimum 44x44px touch targets', async ({ page }) => {
    await page.goto('/')
    await page.waitForLoadState('networkidle')

    // Check hamburger menu button
    const hamburgerMenu = page.getByTestId('hamburger-menu')
    const hamburgerBox = await hamburgerMenu.boundingBox()
    expect(hamburgerBox).toBeTruthy()
    expect(hamburgerBox!.width).toBeGreaterThanOrEqual(MINIMUM_TAP_TARGET)
    expect(hamburgerBox!.height).toBeGreaterThanOrEqual(MINIMUM_TAP_TARGET)

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

    // Open mobile menu and check its buttons
    await hamburgerMenu.click()
    const mobileMenu = page.getByTestId('mobile-menu')
    await expect(mobileMenu).toBeVisible()

    // Check mobile menu buttons
    const mobileNavLogin = page.getByTestId('mobile-nav-login')
    const loginBox = await mobileNavLogin.boundingBox()
    expect(loginBox).toBeTruthy()
    expect(loginBox!.width).toBeGreaterThanOrEqual(MINIMUM_TAP_TARGET)
    expect(loginBox!.height).toBeGreaterThanOrEqual(MINIMUM_TAP_TARGET)

    const mobileNavSignup = page.getByTestId('mobile-nav-signup')
    const signupBox = await mobileNavSignup.boundingBox()
    expect(signupBox).toBeTruthy()
    expect(signupBox!.width).toBeGreaterThanOrEqual(MINIMUM_TAP_TARGET)
    expect(signupBox!.height).toBeGreaterThanOrEqual(MINIMUM_TAP_TARGET)
  })

  // Test Case 6: Check text readability at mobile viewport
  test('all text is readable without horizontal scrolling', async ({ page }) => {
    await page.goto('/')
    await page.waitForLoadState('networkidle')

    // Check that no horizontal scrolling is needed
    const scrollWidth = await page.evaluate(() => document.documentElement.scrollWidth)
    const clientWidth = await page.evaluate(() => document.documentElement.clientWidth)
    expect(scrollWidth).toBeLessThanOrEqual(clientWidth + 1) // Allow 1px tolerance

    // Verify hero text is visible and readable
    const headline = page.getByTestId('hero-headline')
    await expect(headline).toBeVisible()
    const headlineText = await headline.textContent()
    expect(headlineText).toBeTruthy()
    expect(headlineText!.length).toBeGreaterThan(0)

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

    // Verify content is within viewport width
    const featureCard = page.getByTestId('feature-card-url-shortening')
    const cardBox = await featureCard.boundingBox()
    expect(cardBox).toBeTruthy()
    expect(cardBox!.width).toBeLessThanOrEqual(MOBILE_VIEWPORT.width)
  })

  // Test Case 7: Scroll through entire homepage on mobile
  test('all sections are accessible and display correctly', async ({ page }) => {
    await page.goto('/')
    await page.waitForLoadState('networkidle')

    // Verify hero section
    const heroSection = page.getByTestId('hero-section')
    await expect(heroSection).toBeVisible()

    // Scroll to features section
    const featuresSection = page.locator('#features')
    await featuresSection.scrollIntoViewIfNeeded()
    await expect(featuresSection).toBeVisible()

    // Verify all feature cards are visible
    const expectedFeatureIds = ['url-shortening', 'analytics', 'link-management', 'security']
    for (const featureId of expectedFeatureIds) {
      const card = page.getByTestId(`feature-card-${featureId}`)
      await card.scrollIntoViewIfNeeded()
      await expect(card).toBeVisible()
    }

    // Scroll to how it works section
    const howItWorksSection = page.getByTestId('how-it-works-section')
    await howItWorksSection.scrollIntoViewIfNeeded()
    await expect(howItWorksSection).toBeVisible()

    // Verify all steps are visible
    for (let i = 1; i <= 3; i++) {
      const step = page.getByTestId(`step-${i}`)
      await step.scrollIntoViewIfNeeded()
      await expect(step).toBeVisible()
    }

    // Scroll to footer
    const footer = page.locator('footer')
    await footer.scrollIntoViewIfNeeded()
    await expect(footer).toBeVisible()

    // Verify footer links are accessible
    const footerLinks = page.getByTestId('footer-links')
    await expect(footerLinks).toBeVisible()
  })

  // Additional test: Mobile menu can be closed
  test('mobile menu can be closed after opening', async ({ page }) => {
    await page.goto('/')
    await page.waitForLoadState('networkidle')

    // Open mobile menu
    const hamburgerMenu = page.getByTestId('hamburger-menu')
    await hamburgerMenu.click()

    // Verify menu is open
    const mobileMenu = page.getByTestId('mobile-menu')
    await expect(mobileMenu).toBeVisible()

    // Close menu by clicking hamburger again or close button
    const closeButton = page.getByTestId('mobile-menu-close')
    if (await closeButton.isVisible()) {
      await closeButton.click()
    } else {
      await hamburgerMenu.click()
    }

    // Verify menu is closed
    await expect(mobileMenu).toBeHidden()
  })

  // Additional test: Navigation from mobile menu works
  test('navigation items in mobile menu work correctly', async ({ page }) => {
    await page.goto('/')
    await page.waitForLoadState('networkidle')

    // Open mobile menu
    const hamburgerMenu = page.getByTestId('hamburger-menu')
    await hamburgerMenu.click()

    // Click on Features navigation
    const mobileNavFeatures = page.getByTestId('mobile-nav-features')
    await mobileNavFeatures.click()

    // Verify scroll to features section
    const featuresSection = page.locator('#features')
    await expect(featuresSection).toBeInViewport()

    // Menu should close after navigation
    const mobileMenu = page.getByTestId('mobile-menu')
    await expect(mobileMenu).toBeHidden()
  })

  // Additional test: Test at 320px (very small mobile)
  test('homepage displays correctly at 320px width', async ({ page }) => {
    await page.setViewportSize({ width: 320, height: 568 })
    await page.goto('/')
    await page.waitForLoadState('networkidle')

    // Verify homepage renders
    const homePage = page.getByTestId('home-page')
    await expect(homePage).toBeVisible()

    // Verify no horizontal scrolling
    const scrollWidth = await page.evaluate(() => document.documentElement.scrollWidth)
    const clientWidth = await page.evaluate(() => document.documentElement.clientWidth)
    expect(scrollWidth).toBeLessThanOrEqual(clientWidth + 1)

    // Hamburger menu should still be visible
    const hamburgerMenu = page.getByTestId('hamburger-menu')
    await expect(hamburgerMenu).toBeVisible()
  })
})
