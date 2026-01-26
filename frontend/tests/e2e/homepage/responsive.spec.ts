/**
 * Responsive Design E2E Tests - Mobile Viewport
 * Owner: Scenario 8 - Responsive Design - Mobile Viewport
 *
 * Tests verify the homepage displays correctly on mobile devices
 * with collapsed navigation and touch-friendly elements.
 */

import { test, expect } from '@playwright/test'
import { HomePage, viewports } from './fixtures'

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
