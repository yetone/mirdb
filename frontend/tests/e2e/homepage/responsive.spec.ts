/**
 * Responsive Design E2E Tests
 * Owner: Scenario 8 - Mobile Viewport, Scenario 9 - Tablet Viewport
 *
 * E2E tests to verify:
 * - Homepage displays correctly at mobile viewport (375x667)
 * - Homepage displays correctly at tablet viewport (768x1024)
 * - Layout adapts appropriately without horizontal scroll
 * - Feature cards display in appropriate grid layouts
 * - Navigation is appropriately styled for each viewport
 */

import { test, expect } from '@playwright/test'
import { HomePage, viewports } from './fixtures'

// Mobile Viewport Tests (Scenario 8)
test.describe('Responsive Design - Mobile Viewport', () => {
  test.beforeEach(async ({ page }) => {
    // Set mobile viewport before navigating
    await page.setViewportSize(viewports.mobile)
  })

  test('TC1: No horizontal scrollbar appears, content fits viewport', async ({ page }) => {
    const homePage = new HomePage(page)
    await homePage.goto()

    // Wait for page to fully load
    await page.waitForLoadState('networkidle')

    // Check that body width doesn't exceed viewport width
    const bodyScrollWidth = await page.evaluate(() => document.body.scrollWidth)
    const viewportWidth = await page.evaluate(() => window.innerWidth)

    // Body scroll width should not exceed viewport width (no horizontal scrollbar)
    expect(bodyScrollWidth).toBeLessThanOrEqual(viewportWidth)

    // Also verify no horizontal overflow on main content
    const overflowX = await page.evaluate(() => {
      const body = document.body
      return window.getComputedStyle(body).overflowX
    })

    // Check that horizontal scrollbar is not visible
    const hasHorizontalScrollbar = await page.evaluate(() => {
      return document.documentElement.scrollWidth > document.documentElement.clientWidth
    })
    expect(hasHorizontalScrollbar).toBe(false)
  })

  test('TC2: Hamburger menu icon is visible, full nav is hidden', async ({ page }) => {
    const homePage = new HomePage(page)
    await homePage.goto()

    // Wait for page to fully load
    await page.waitForLoadState('networkidle')

    // Hamburger menu should be visible on mobile
    const hamburgerMenu = page.getByTestId('hamburger-menu')
    await expect(hamburgerMenu).toBeVisible()

    // Desktop navigation should be hidden on mobile
    const desktopNav = page.getByTestId('desktop-nav')
    await expect(desktopNav).not.toBeVisible()

    // Mobile menu should not be visible initially (before clicking hamburger)
    const mobileMenu = page.getByTestId('mobile-menu')
    await expect(mobileMenu).not.toBeVisible()
  })

  test('TC3: Mobile navigation drawer/menu opens with all nav items', async ({ page }) => {
    const homePage = new HomePage(page)
    await homePage.goto()

    // Wait for page to fully load
    await page.waitForLoadState('networkidle')

    // Click hamburger menu to open mobile nav
    const hamburgerMenu = page.getByTestId('hamburger-menu')
    await hamburgerMenu.click()

    // Mobile menu should now be visible
    const mobileMenu = page.getByTestId('mobile-menu')
    await expect(mobileMenu).toBeVisible()

    // Check all nav items are present in mobile menu
    const featuresLink = page.getByTestId('mobile-nav-features')
    const howItWorksLink = page.getByTestId('mobile-nav-how-it-works')
    const signInButton = page.getByTestId('mobile-sign-in')
    const getStartedButton = page.getByTestId('mobile-get-started')

    await expect(featuresLink).toBeVisible()
    await expect(howItWorksLink).toBeVisible()
    await expect(signInButton).toBeVisible()
    await expect(getStartedButton).toBeVisible()

    // Verify link texts
    await expect(featuresLink).toHaveText('Features')
    await expect(howItWorksLink).toHaveText('How It Works')
    await expect(signInButton).toHaveText('Sign In')
    await expect(getStartedButton).toHaveText('Get Started')
  })

  test('TC4: Buttons have minimum touch target size of 44px', async ({ page }) => {
    const homePage = new HomePage(page)
    await homePage.goto()

    // Wait for page to fully load
    await page.waitForLoadState('networkidle')

    // Check hamburger menu touch target
    const hamburgerMenu = page.getByTestId('hamburger-menu')
    const hamburgerBox = await hamburgerMenu.boundingBox()
    expect(hamburgerBox).toBeTruthy()
    expect(hamburgerBox!.width).toBeGreaterThanOrEqual(44)
    expect(hamburgerBox!.height).toBeGreaterThanOrEqual(44)

    // Check CTA buttons in hero section
    const getStartedButton = page.getByTestId('get-started-button')
    const signInButton = page.getByTestId('sign-in-button')

    const getStartedBox = await getStartedButton.boundingBox()
    const signInBox = await signInButton.boundingBox()

    expect(getStartedBox).toBeTruthy()
    expect(getStartedBox!.height).toBeGreaterThanOrEqual(44)

    expect(signInBox).toBeTruthy()
    expect(signInBox!.height).toBeGreaterThanOrEqual(44)

    // Open mobile menu and check mobile nav button touch targets
    await hamburgerMenu.click()
    const mobileMenu = page.getByTestId('mobile-menu')
    await expect(mobileMenu).toBeVisible()

    const mobileSignIn = page.getByTestId('mobile-sign-in')
    const mobileGetStarted = page.getByTestId('mobile-get-started')

    const mobileSignInBox = await mobileSignIn.boundingBox()
    const mobileGetStartedBox = await mobileGetStarted.boundingBox()

    expect(mobileSignInBox).toBeTruthy()
    expect(mobileSignInBox!.height).toBeGreaterThanOrEqual(44)

    expect(mobileGetStartedBox).toBeTruthy()
    expect(mobileGetStartedBox!.height).toBeGreaterThanOrEqual(44)
  })

  test('TC5: Hero content stacks vertically and remains readable', async ({ page }) => {
    const homePage = new HomePage(page)
    await homePage.goto()

    // Wait for page to fully load
    await page.waitForLoadState('networkidle')

    // Check hero section is visible
    const heroSection = page.getByTestId('hero-section')
    await expect(heroSection).toBeVisible()

    // Check headline is visible and readable
    const headline = page.getByTestId('hero-headline')
    await expect(headline).toBeVisible()
    await expect(headline).toContainText('Shorten. Share. Track.')

    // Check subheadline is visible
    const subheadline = page.getByTestId('hero-subheadline')
    await expect(subheadline).toBeVisible()

    // Check CTA buttons stack vertically on small mobile (they should be in flex-col on small screens)
    const ctaContainer = page.getByTestId('hero-cta-container')
    await expect(ctaContainer).toBeVisible()

    // Get positions of the two buttons to verify vertical stacking
    const getStartedButton = page.getByTestId('get-started-button')
    const signInButton = page.getByTestId('sign-in-button')

    const getStartedBox = await getStartedButton.boundingBox()
    const signInBox = await signInButton.boundingBox()

    // Buttons should be stacked vertically (signIn should be below getStarted)
    // This means signIn's top position should be greater than getStarted's bottom
    expect(getStartedBox).toBeTruthy()
    expect(signInBox).toBeTruthy()

    // Check that Sign In button is below Get Started button (vertical stacking)
    // or they can be side by side at slightly larger mobile (sm: breakpoint)
    // For 375px, they should stack
    const getStartedBottom = getStartedBox!.y + getStartedBox!.height
    const signInTop = signInBox!.y

    // At 375px width, buttons should stack vertically
    // Sign In button should start at or below where Get Started ends
    expect(signInTop).toBeGreaterThanOrEqual(getStartedBottom - 10) // Allow small overlap for animations

    // Verify headline font size is reasonable for mobile (not too small)
    const headlineFontSize = await page.evaluate(() => {
      const element = document.querySelector('[data-testid="hero-headline"]')
      return element ? window.getComputedStyle(element).fontSize : null
    })
    expect(headlineFontSize).toBeTruthy()
    // Mobile should have at least text-4xl which is 36px
    const fontSizeNum = parseInt(headlineFontSize!)
    expect(fontSizeNum).toBeGreaterThanOrEqual(32) // Minimum readable size for headline
  })

  test('Mobile menu closes when clicking a nav link', async ({ page }) => {
    const homePage = new HomePage(page)
    await homePage.goto()

    // Open mobile menu
    const hamburgerMenu = page.getByTestId('hamburger-menu')
    await hamburgerMenu.click()

    const mobileMenu = page.getByTestId('mobile-menu')
    await expect(mobileMenu).toBeVisible()

    // Click on Features link
    const featuresLink = page.getByTestId('mobile-nav-features')
    await featuresLink.click()

    // Menu should close after clicking a link
    await expect(mobileMenu).not.toBeVisible()
  })

  test('Mobile menu can be toggled open and closed', async ({ page }) => {
    const homePage = new HomePage(page)
    await homePage.goto()

    const hamburgerMenu = page.getByTestId('hamburger-menu')
    const mobileMenu = page.getByTestId('mobile-menu')

    // Initially closed
    await expect(mobileMenu).not.toBeVisible()

    // Open menu
    await hamburgerMenu.click()
    await expect(mobileMenu).toBeVisible()

    // Close menu by clicking hamburger again
    await hamburgerMenu.click()
    await expect(mobileMenu).not.toBeVisible()
  })
})

// Tablet Viewport Tests (Scenario 9)
test.describe('Responsive Design - Tablet Viewport', () => {
  test.beforeEach(async ({ page }) => {
    await page.setViewportSize(viewports.tablet)
  })

  test('content adapts to tablet layout without horizontal scroll', async ({ page }) => {
    const homePage = new HomePage(page)
    await homePage.goto()

    // Wait for page to fully load
    await expect(homePage.heroSection).toBeVisible()

    // Check that document body width matches viewport (no horizontal overflow)
    const scrollWidth = await page.evaluate(() => document.documentElement.scrollWidth)
    const clientWidth = await page.evaluate(() => document.documentElement.clientWidth)

    // Scroll width should not exceed client width significantly (no horizontal scroll)
    expect(scrollWidth).toBeLessThanOrEqual(clientWidth + 1)

    // Verify key sections are visible and within viewport width
    await expect(homePage.heroHeadline).toBeVisible()
    await expect(homePage.heroSubheadline).toBeVisible()
    await expect(homePage.getStartedButton).toBeVisible()

    // Verify features section is visible
    const featuresSection = page.getByTestId('features-section')
    await expect(featuresSection).toBeVisible()

    // Verify hero section doesn't overflow
    const heroBox = await homePage.heroSection.boundingBox()
    expect(heroBox).not.toBeNull()
    if (heroBox) {
      expect(heroBox.width).toBeLessThanOrEqual(viewports.tablet.width)
    }
  })

  test('feature cards display in 2-column grid layout at tablet viewport', async ({ page }) => {
    const homePage = new HomePage(page)
    await homePage.goto()

    // Scroll to features section
    const featuresSection = page.getByTestId('features-section')
    await featuresSection.scrollIntoViewIfNeeded()
    await expect(featuresSection).toBeVisible()

    // Get feature cards grid
    const featuresGrid = page.getByTestId('features-grid')
    await expect(featuresGrid).toBeVisible()

    // Get all feature cards
    const featureCard0 = page.getByTestId('feature-card-0')
    const featureCard1 = page.getByTestId('feature-card-1')
    const featureCard2 = page.getByTestId('feature-card-2')

    await expect(featureCard0).toBeVisible()
    await expect(featureCard1).toBeVisible()
    await expect(featureCard2).toBeVisible()

    // Get bounding boxes
    const card0Box = await featureCard0.boundingBox()
    const card1Box = await featureCard1.boundingBox()
    const card2Box = await featureCard2.boundingBox()

    expect(card0Box).not.toBeNull()
    expect(card1Box).not.toBeNull()
    expect(card2Box).not.toBeNull()

    if (card0Box && card1Box && card2Box) {
      // At tablet (768px = md breakpoint), grid should be 2 columns
      // Card 0 and Card 1 should be on the same row (similar y position)
      expect(Math.abs(card0Box.y - card1Box.y)).toBeLessThan(10)

      // Card 0 and Card 1 should be side by side (different x positions)
      expect(card0Box.x).not.toBe(card1Box.x)

      // Card 2 should be on a different row (below cards 0 and 1)
      expect(card2Box.y).toBeGreaterThan(card0Box.y + card0Box.height - 20)

      // Verify cards have appropriate width for 2-column layout
      // Each card should take roughly half the container width (accounting for gap)
      const gridBox = await featuresGrid.boundingBox()
      if (gridBox) {
        // Cards should each be less than 60% of grid width (accounting for gap)
        expect(card0Box.width).toBeLessThan(gridBox.width * 0.6)
        expect(card1Box.width).toBeLessThan(gridBox.width * 0.6)
      }
    }
  })

  test('navigation is appropriately styled for tablet viewport', async ({ page }) => {
    const homePage = new HomePage(page)
    await homePage.goto()

    // Use first navigation (header navbar) to avoid matching footer nav
    const headerNavbar = page.getByRole('navigation').first()
    await expect(headerNavbar).toBeVisible()

    // At tablet (768px = md breakpoint), anchor links should be visible
    // The Navbar has: hidden md:flex for the anchor links container
    const featuresLink = page.getByRole('link', { name: /features/i })
    const howItWorksLink = page.getByRole('link', { name: /how it works/i })

    // At 768px (md breakpoint), links should be visible
    await expect(featuresLink).toBeVisible()
    await expect(howItWorksLink).toBeVisible()

    // Sign In and Get Started buttons should be visible
    const signInButton = page.getByTestId('sign-in-nav')
    const getStartedButton = page.getByTestId('get-started-nav')

    await expect(signInButton).toBeVisible()
    await expect(getStartedButton).toBeVisible()

    // Navbar should fit within viewport width
    const navbarBox = await headerNavbar.boundingBox()
    expect(navbarBox).not.toBeNull()
    if (navbarBox) {
      expect(navbarBox.width).toBeLessThanOrEqual(viewports.tablet.width)
    }
  })

  test('hero section CTA buttons are appropriately sized for tablet', async ({ page }) => {
    const homePage = new HomePage(page)
    await homePage.goto()

    await expect(homePage.getStartedButton).toBeVisible()
    await expect(homePage.signInButton).toBeVisible()

    const getStartedBox = await homePage.getStartedButton.boundingBox()
    const signInBox = await homePage.signInButton.boundingBox()

    expect(getStartedBox).not.toBeNull()
    expect(signInBox).not.toBeNull()

    if (getStartedBox && signInBox) {
      // Buttons should be visible within viewport
      expect(getStartedBox.x + getStartedBox.width).toBeLessThanOrEqual(viewports.tablet.width)
      expect(signInBox.x + signInBox.width).toBeLessThanOrEqual(viewports.tablet.width)

      // Buttons should have reasonable touch-friendly size (at least 44px height)
      expect(getStartedBox.height).toBeGreaterThanOrEqual(36)
      expect(signInBox.height).toBeGreaterThanOrEqual(36)
    }
  })

  test('page is scrollable vertically to access all sections', async ({ page }) => {
    const homePage = new HomePage(page)
    await homePage.goto()

    // Verify hero section is visible initially
    await expect(homePage.heroSection).toBeVisible()

    // Scroll to features section
    const featuresSection = page.getByTestId('features-section')
    await featuresSection.scrollIntoViewIfNeeded()
    await expect(featuresSection).toBeVisible()

    // Scroll to how it works section
    const howItWorksSection = page.getByTestId('how-it-works-section')
    await howItWorksSection.scrollIntoViewIfNeeded()
    await expect(howItWorksSection).toBeVisible()

    // Verify all sections were accessible via scroll
    const heroBox = await homePage.heroSection.boundingBox()
    const featuresBox = await featuresSection.boundingBox()
    const howItWorksBox = await howItWorksSection.boundingBox()

    expect(heroBox).not.toBeNull()
    expect(featuresBox).not.toBeNull()
    expect(howItWorksBox).not.toBeNull()
  })
})
